"""Public force-stop behavior against a real Dask nanny and the fake LSF farm."""
from contextlib import contextmanager
import asyncio
import json
import inspect
import os
from pathlib import Path
import shutil
import sys
import time

import pytest

from hedloom import RunHistory, Site, file, operation, parameter, pooled, runtime, shell, study
from hedloom_exec.journal import AttemptJournal

pytest.importorskip('distributed')
pytest.importorskip('dask_jobqueue')
pytestmark = pytest.mark.skipif(sys.platform != 'linux', reason='Linux command parent-death binding')

FARM = Path(__file__).resolve().parents[1] / 'exec' / 'tests' / 'fakefarm'
COMMAND = """
import json, os, pathlib, signal, sys, time
markers = pathlib.Path(sys.argv[1])
signal.signal(signal.SIGTERM, signal.SIG_IGN)
entry = {'command_pid': os.getpid(), 'worker_pid': os.getppid()}
with (markers / 'entries').open('a') as stream:
    stream.write(json.dumps(entry) + '\\n')
    stream.flush()
(markers / 'started').write_text(json.dumps(entry))
deadline = time.monotonic() + 90
while not (markers / 'release').exists():
    if time.monotonic() > deadline:
        raise RuntimeError('test release did not arrive')
    time.sleep(0.02)
pathlib.Path(sys.argv[2]).write_text('completed')
(markers / 'completed').touch()
"""


@operation(config={'markers': parameter(str)}, outputs={'note': file('note.txt', kind='force-stop-test')})
def held_command(out, markers):
    return shell(sys.executable, '-c', COMMAND, markers, str(out.note))


@study(default_policy=pooled())
def pooled_command(markers):
    return held_command.named('work')(markers=markers)


@pytest.fixture
def farm(tmp_path, monkeypatch):
    monkeypatch.setenv('PATH', str(FARM) + os.pathsep + os.environ['PATH'])
    monkeypatch.setenv('FAKE_LSF_STATE', str(tmp_path / 'farm'))
    for command in ('bsub', 'bjobs', 'bkill'):
        assert Path(shutil.which(command)).resolve().parent == FARM.resolve()
    return tmp_path / 'farm'


def wait_for(predicate, *, timeout=30):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise AssertionError('force-stop condition timed out')
        time.sleep(0.02)


def entry(directory):
    try:
        return json.loads((directory / 'started').read_text())
    except (OSError, ValueError):
        return None


def alive(pid):
    try:
        # A reaped-or-zombie command cannot continue executing its payload.
        return Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[0] != 'Z'
    except FileNotFoundError:
        return False


def allocation(farm):
    records = [json.loads(path.read_text()) for path in farm.glob('*.json')]
    batches = [record for record in records if record.get('kind') == 'batch']
    assert len(batches) == 1, batches
    batch = batches[0]
    assert batch['state'] == 'RUN', batch
    return batch['id'], batch['supervisor_pid']


@contextmanager
def owner(root, *, max_jobs=1):
    site = Site(records_dir=str(root / 'records'), work_dir=str(root / 'work'),
                runs_dir=str(root / 'history'), threads=2, dashboard='none',
                placements={'pool': {'kind': 'lsf-pooled', 'queue': 'normal',
                    'cores': 1, 'memory_mb': 1000, 'walltime': '0:30',
                    'workers': 1, 'max_jobs': max_jobs}})
    live = runtime(site)
    try:
        yield live, site
    finally:
        # Assertion failures must release any command this test still owns.
        for directory in root.glob('markers-*'):
            (directory / 'release').touch()
        live.close(timeout=45)


def markers(root, name, *, released=False):
    directory = root / ('markers-' + name)
    directory.mkdir()
    if released:
        (directory / 'release').touch()
    return directory


def selected(history, run_id):
    row = history.invocation(run_id, 'work')
    return row if row.record is not None and row.try_number is not None else None


def assert_cancelled(site, result, before):
    assert result.state == 'STOPPED', result.summary()
    assert result.history.status == 'complete', result.history
    outcome = result['work']
    assert outcome.outcome == 'cancelled'
    assert (outcome.record, outcome.try_number) == (before.record, before.try_number)
    manifest = AttemptJournal(site.records_dir, outcome.record).read_manifest(outcome.try_number)
    assert manifest['outcome'] == 'cancelled'
    assert (manifest['attempt'], manifest['try']) == (before.record, before.try_number)
    row = RunHistory(site.runs_dir).invocation(result.run_id, 'work')
    assert row.run_reported_outcome == row.selected_execution_state == 'cancelled'
    assert (row.record, row.try_number, row.execution_id) == (
        before.record, before.try_number, before.execution_id)


@pytest.mark.parametrize('close, expected', [(False, False), (True, False), (True, True)],
                         ids=['lost-connection', 'closed-worker', 'expected-removal'])
def test_assignment_removal_does_not_certify_a_surviving_command_as_cancelled(tmp_path, farm, close, expected):
    held = markers(tmp_path, 'held')
    with owner(tmp_path) as (live, site):
        receipt = live.submit(pooled_command(str(held)), name='removed-worker-force')
        process = wait_for(lambda: entry(held))
        history = RunHistory(site.runs_dir)
        before = wait_for(lambda: selected(history, receipt.run_id))

        async def remove_assignment():
            scheduler = live._pools['pool'].scheduler
            task = scheduler.tasks[f'pooled-{before.record}-{before.try_number}']
            address = task.processing_on.address
            # distributed 2023 names the expected-removal flag "safe".
            from distributed import Scheduler
            flag = 'expected' if 'expected' in inspect.signature(Scheduler.remove_worker).parameters else 'safe'
            await scheduler.remove_worker(address, close=close, **{flag: expected},
                                          stimulus_id='test-removal-with-live-command')
            # Request via the public receipt before any new assignment can hide
            # the lost old copy. Scheduler bookkeeping cannot prove its death.
            assert receipt.stop(force=True)

        asyncio.run_coroutine_threadsafe(remove_assignment(), live._loop).result(timeout=10)
        result = receipt.wait(timeout=15)
        assert result.state == 'STOPPED', result.summary()
        assert result.history.status == 'complete'
        outcome = result['work']
        assert outcome.outcome != 'cancelled'
        assert (outcome.record, outcome.try_number) == (before.record, before.try_number)
        assert 'assignment loss is unconfirmed' in (outcome.error or ''), outcome
        journal = AttemptJournal(site.records_dir, before.record)
        manifest = journal.read_manifest(before.try_number)
        assert manifest is None or manifest.get('outcome') != 'cancelled'
        row = history.invocation(result.run_id, 'work')
        assert row.run_reported_outcome != 'cancelled'
        assert row.selected_execution_state != 'cancelled'
        # This is the dangerous situation: an ignored-TERM payload survives
        # the bookkeeping removal. The result must state uncertainty honestly.
        assert alive(process['command_pid'])
        assert not (held / 'completed').exists()
        (held / 'release').touch()


@pytest.mark.parametrize('escalate', [False, True], ids=['force', 'ordinary-then-force'])
def test_force_stops_sole_command_and_restores_same_allocation(tmp_path, farm, escalate):
    held = markers(tmp_path, 'held')
    next_command = markers(tmp_path, 'next', released=True)
    with owner(tmp_path) as (live, site):
        receipt = live.submit(pooled_command(str(held)), name='sole')
        process = wait_for(lambda: entry(held))
        history = RunHistory(site.runs_dir)
        before = wait_for(lambda: selected(history, receipt.run_id))
        batch = allocation(farm)
        assert alive(process['command_pid'])
        if escalate:
            assert receipt.stop()
            assert not receipt.done()
            assert receipt.snapshot().stop_requested
            assert not receipt.snapshot().force_requested
        assert receipt.stop(force=True)
        assert receipt.snapshot().force_requested
        stopped = receipt.wait(timeout=30)
        assert_cancelled(site, stopped, before)
        wait_for(lambda: not alive(process['command_pid']))
        assert not (held / 'completed').exists()
        assert len((held / 'entries').read_text().splitlines()) == 1
        assert allocation(farm) == batch
        later = live.submit(pooled_command(str(next_command)), name='after-force').wait(timeout=30)
        assert later.succeeded, later.summary()
        assert entry(next_command)['worker_pid'] != process['worker_pid']
        assert allocation(farm) == batch
        assert len((held / 'entries').read_text().splitlines()) == 1
        assert not receipt.stop(force=True)
        cancelled_manifest = AttemptJournal(site.records_dir, before.record).read_manifest(before.try_number)
        (held / 'release').touch()
        retried = live.submit(pooled_command(str(held)), name='retry-cancelled').wait(timeout=30)
        assert retried.succeeded, retried.summary()
        assert not retried['work'].reused
        assert retried['work'].record == before.record
        assert retried['work'].try_number == before.try_number + 1
        assert len((held / 'entries').read_text().splitlines()) == 2
        assert AttemptJournal(site.records_dir, before.record).read_manifest(before.try_number) == cancelled_manifest
        reused = live.submit(pooled_command(str(held)), name='reuse-success').wait(timeout=30)
        assert reused.succeeded, reused.summary()
        assert reused['work'].reused
        assert (reused['work'].record, reused['work'].try_number) == (
            retried['work'].record, retried['work'].try_number)
        assert len((held / 'entries').read_text().splitlines()) == 2
        assert allocation(farm) == batch


def test_force_withdraws_shared_consumer_without_restarting_worker(tmp_path, farm):
    held = markers(tmp_path, 'shared')
    next_command = markers(tmp_path, 'next', released=True)
    with owner(tmp_path) as (live, site):
        first = live.submit(pooled_command(str(held)), name='first')
        process = wait_for(lambda: entry(held))
        history = RunHistory(site.runs_dir)
        before = wait_for(lambda: selected(history, first.run_id))
        second = live.submit(pooled_command(str(held)), name='second')
        second.accepted(timeout=15)
        attached = wait_for(lambda: selected(history, second.run_id))
        assert attached.execution_id == before.execution_id
        batch = allocation(farm)
        assert first.stop(force=True)
        withdrawn = first.wait(timeout=15)
        assert withdrawn.state == 'STOPPED'
        assert withdrawn['work'].outcome == 'cancelled'
        assert withdrawn['work'].disposition == 'withdrawn'
        assert withdrawn.history.status == 'complete'
        assert alive(process['command_pid'])
        assert not second.done()
        assert not (held / 'completed').exists()
        assert allocation(farm) == batch
        (held / 'release').touch()
        survivor = second.wait(timeout=30)
        assert survivor.succeeded, survivor.summary()
        assert (survivor['work'].record, survivor['work'].try_number) == (
            before.record, before.try_number)
        assert history.invocation(withdrawn.run_id, 'work').run_reported_outcome == 'cancelled'
        assert history.invocation(survivor.run_id, 'work').selected_execution_state == 'succeeded'
        assert len((held / 'entries').read_text().splitlines()) == 1
        later = live.submit(pooled_command(str(next_command)), name='after-withdrawal').wait(timeout=30)
        assert later.succeeded, later.summary()
        assert entry(next_command)['worker_pid'] == process['worker_pid']
        assert allocation(farm) == batch


def test_force_cancels_queued_command_without_killing_other_active_work(tmp_path, farm):
    held = markers(tmp_path, 'active')
    queued = markers(tmp_path, 'queued')
    next_command = markers(tmp_path, 'next', released=True)
    with owner(tmp_path, max_jobs=2) as (live, site):
        active = live.submit(pooled_command(str(held)), name='active')
        process = wait_for(lambda: entry(held))
        batch = allocation(farm)
        waiting = live.submit(pooled_command(str(queued)), name='queued')
        waiting.accepted(timeout=15)
        history = RunHistory(site.runs_dir)
        before = wait_for(lambda: selected(history, waiting.run_id))
        assert not (queued / 'started').exists()
        assert waiting.stop(force=True)
        cancelled = waiting.wait(timeout=30)
        assert_cancelled(site, cancelled, before)
        assert alive(process['command_pid'])
        assert not active.done()
        assert not (held / 'completed').exists()
        assert not (queued / 'entries').exists()
        assert allocation(farm) == batch
        (held / 'release').touch()
        result = active.wait(timeout=30)
        assert result.succeeded, result.summary()
        later = live.submit(pooled_command(str(next_command)), name='after-queued-stop').wait(timeout=30)
        assert later.succeeded, later.summary()
        assert entry(next_command)['worker_pid'] == process['worker_pid']
        assert allocation(farm) == batch
        assert not (queued / 'entries').exists()


def test_force_stops_active_command_and_preserves_unrelated_queued_work(tmp_path, farm):
    held = markers(tmp_path, 'active')
    queued = markers(tmp_path, 'queued', released=True)
    with owner(tmp_path, max_jobs=2) as (live, site):
        active = live.submit(pooled_command(str(held)), name='force-active')
        process = wait_for(lambda: entry(held))
        history = RunHistory(site.runs_dir)
        before = wait_for(lambda: selected(history, active.run_id))
        batch = allocation(farm)
        waiting = live.submit(pooled_command(str(queued)), name='survive-queued')
        waiting.accepted(timeout=15)
        wait_for(lambda: selected(history, waiting.run_id))
        assert not (queued / 'started').exists()
        assert active.stop(force=True)
        stopped = active.wait(timeout=30)
        assert_cancelled(site, stopped, before)
        wait_for(lambda: not alive(process['command_pid']))
        assert not (held / 'completed').exists()
        assert len((held / 'entries').read_text().splitlines()) == 1
        survivor = waiting.wait(timeout=30)
        assert survivor.succeeded, survivor.summary()
        assert entry(queued)['worker_pid'] != process['worker_pid']
        assert len((queued / 'entries').read_text().splitlines()) == 1
        assert allocation(farm) == batch
        assert len((held / 'entries').read_text().splitlines()) == 1


def test_runtime_force_stop_cancels_active_and_queued_runs_together(tmp_path, farm, monkeypatch):
    import hedloom_run.pooled as executor
    original_pause = executor._pause_command
    first_pause = str(tmp_path / 'first-pause')
    second_check = str(tmp_path / 'second-pause-check')
    release_pause = str(tmp_path / 'release-pause')

    async def pause_checkpoint(key, dask_worker=None):
        # Client.run awaits this on the actual Worker loop. Yielding after
        # its first pause lets the other force operation inspect that pause.
        try:
            snapshot = original_pause(key, dask_worker=dask_worker)
        except RuntimeError:
            Path(second_check).touch()
            raise
        if snapshot['state'] == 'paused':
            # The real memory monitor tries to resume a low-memory paused worker.
            # Exercise that competing owner deterministically during the fence.
            dask_worker.memory_manager._maybe_pause_or_unpause(dask_worker, 0)
        if snapshot['state'] == 'paused' and not Path(first_pause).exists():
            Path(first_pause).touch()
            while not Path(release_pause).exists():
                await asyncio.sleep(.02)
        elif snapshot['state'] in {'paused', 'busy'}:
            Path(second_check).touch()
        return snapshot

    monkeypatch.setattr(executor, '_pause_command', pause_checkpoint)
    held = markers(tmp_path, 'active')
    queued = markers(tmp_path, 'queued')
    next_command = markers(tmp_path, 'next', released=True)
    with owner(tmp_path, max_jobs=2) as (live, site):
        active = live.submit(pooled_command(str(held)), name='bulk-active')
        process = wait_for(lambda: entry(held))
        history = RunHistory(site.runs_dir)
        active_before = wait_for(lambda: selected(history, active.run_id))
        batch = allocation(farm)
        waiting = live.submit(pooled_command(str(queued)), name='bulk-queued')
        waiting.accepted(timeout=15)
        queued_before = wait_for(lambda: selected(history, waiting.run_id))
        assert not (queued / 'started').exists()
        try:
            live.stop(force=True)
            wait_for(lambda: Path(first_pause).exists())
            wait_for(lambda: Path(second_check).exists())
            assert active.snapshot().force_requested
            assert waiting.snapshot().force_requested
        finally:
            Path(release_pause).touch()
        assert_cancelled(site, active.wait(timeout=30), active_before)
        assert_cancelled(site, waiting.wait(timeout=30), queued_before)
        wait_for(lambda: not alive(process['command_pid']))
        assert not (held / 'completed').exists()
        assert len((held / 'entries').read_text().splitlines()) == 1
        assert not (queued / 'entries').exists()
        assert allocation(farm) == batch
        later = live.submit(pooled_command(str(next_command)), name='after-bulk-stop').wait(timeout=30)
        assert later.succeeded, later.summary()
        assert entry(next_command)['worker_pid'] != process['worker_pid']
        assert allocation(farm) == batch
        assert len((held / 'entries').read_text().splitlines()) == 1
        assert not (queued / 'entries').exists()
