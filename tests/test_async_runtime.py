"""Public async lifecycle and automatic ownership, using real recorded work."""
import asyncio
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from threading import Event, enumerate as threads
import time

import pytest

from hedloom import (AcceptanceError, Reproducibility, RunFailed, RunHistory,
                     Runtime, RuntimeClosed, Site, operation, parameter,
                     returned, runtime, shell, study)

DISABLED = Reproducibility(enabled=False)


@operation(config={'value': parameter(int)}, outputs={'value': returned()})
def quick(*, value):
    return {'value': value}


@operation(config={'marker': parameter(str), 'value': parameter(int)},
           outputs={'value': returned()})
def held(*, marker, value):
    directory = Path(marker)
    (directory / 'entered').write_text('entered')
    deadline = time.monotonic() + 15
    while not (directory / 'release').exists():
        if time.monotonic() > deadline:
            raise TimeoutError('test body release')
        time.sleep(.01)
    return {'value': value}


@study
def simple(value=1):
    return quick.named('work')(value=value)


@study
def holding(marker, value=1):
    return held.named('work')(marker=marker, value=value)


def site(tmp_path, capacity=1):
    return Site(records_dir=str(tmp_path/'records'), runs_dir=str(tmp_path/'runs'),
                work_dir=str(tmp_path/'work'), placements={'local': {'max_jobs': capacity}})


def until(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError('test observation timed out')
        time.sleep(.01)


def test_receipt_precedes_durable_acceptance_and_snapshot_is_in_memory(tmp_path, monkeypatch):
    import hedloom.runtime as module
    # The package exports runtime(), so load the module explicitly.
    import importlib
    module = importlib.import_module('hedloom.runtime')
    entered, release = Event(), Event()
    original = module.HistoryWriter
    def allocate(*args, **kwargs):
        entered.set()
        assert release.wait(10)
        return original(*args, **kwargs)
    monkeypatch.setattr(module, 'HistoryWriter', allocate)
    owner = runtime(site(tmp_path))
    try:
        receipt = owner.submit(simple(), name='acceptance', reproducibility=DISABLED)
        assert receipt.submission_id and receipt.run_id is None
        assert entered.wait(10)
        assert receipt.snapshot().state == 'ACCEPTING'
        assert not receipt.done()
        with pytest.raises(TimeoutError):
            receipt.accepted(.01)
        with monkeypatch.context() as memory_only:
            memory_only.setattr(Path, 'read_text', lambda *a, **k: (_ for _ in ()).throw(AssertionError('snapshot read storage')))
            assert receipt.snapshot().run_id is None
            assert receipt.result_if_done() is None
        release.set()
        reference = receipt.accepted(10)
        assert (Path(reference.location)/'run.json').is_file()
        result = receipt.result(10)
        assert result.outputs['output'].value == 1
        assert receipt.done() and receipt.result_if_done() is result
        assert RunHistory(owner.site.runs_dir).read_run(reference.run_id).run_reported_outcome == 'succeeded'
    finally:
        release.set()
        owner.close(15)


def test_many_waiting_runs_share_one_owner_without_a_32_run_ceiling(tmp_path):
    gate = tmp_path/'gate'
    gate.mkdir()
    owner = runtime(site(tmp_path))
    try:
        first = owner.submit(holding(str(gate)), name='held', reproducibility=DISABLED)
        until(lambda: (gate/'entered').exists())
        receipts = [owner.submit(simple(value), name=f'waiting-{value}',
                                 reproducibility=DISABLED) for value in range(36)]
        assert len(receipts) == 36 and not first.done()
        assert len([thread for thread in threads() if thread is owner._thread]) == 1
        first.stop()
        assert not first.done()  # A solely owned entered body drains.
        (gate/'release').touch()
        assert first.wait(15).state == 'STOPPED'
        assert all(receipt.result(20).succeeded for receipt in receipts)
    finally:
        (gate/'release').touch()
        owner.close(25)


def test_stopping_during_preparation_executes_nothing_and_records_all_nodes(tmp_path, monkeypatch):
    entered, release = Event(), Event()
    original = Runtime._prepare
    def prepare(self, *args):
        entered.set()
        assert release.wait(10)
        return original(self, *args)
    monkeypatch.setattr(Runtime, '_prepare', prepare)
    owner = runtime(site(tmp_path))
    try:
        receipt = owner.submit(simple(), name='stopped', reproducibility=DISABLED)
        receipt.accepted(10)
        assert entered.wait(10)
        assert receipt.stop()
        assert not receipt.stop()
        release.set()
        result = receipt.wait(15)
        assert result.state == 'STOPPED'
        assert all(outcome.record is None for outcome in result.report.outcomes)
        saved = RunHistory(owner.site.runs_dir).read_run(result.run_id)
        assert saved.run_reported_outcome == 'stopped' and saved.history_status == 'complete'
        assert len(saved.invocations) == 1
        assert RunHistory(owner.site.runs_dir).reproducibility(result.run_id)['status'] in {'disabled', 'unavailable'}
    finally:
        release.set()
        owner.close(15)


def test_registered_run_keeps_selected_body_and_same_module_helper_code(tmp_path, monkeypatch):
    namespace = {'__name__': 'receipt_body_capture', 'operation': operation,
                 'returned': returned, 'study': study}
    exec('''
def helper(): return 'before'
@operation(outputs={'value': returned()})
def value(): return {'value': helper()}
@study
def build(): return value()
''', namespace)
    entered, release = Event(), Event()
    original = Runtime._prepare
    def prepare(self, *args):
        entered.set()
        assert release.wait(10)
        return original(self, *args)
    monkeypatch.setattr(Runtime, '_prepare', prepare)
    owner = runtime(site(tmp_path))
    try:
        receipt = owner.submit(namespace['build'](), name='capture', reproducibility=DISABLED)
        assert entered.wait(10)
        # Interactive rebinding happens after registration, before serialization.
        exec("def helper(): return 'after'", namespace)
        assert namespace['helper']() == 'after'
        release.set()
        assert receipt.result(15).outputs['output'].value == 'before'
    finally:
        release.set()
        owner.close(15)


def test_preparation_failure_is_terminal_and_inspectable_after_acceptance(tmp_path):
    owner = runtime(site(tmp_path))
    try:
        receipt = owner.submit(simple(), name='bad-evidence',
            reproducibility=Reproducibility(files=(tmp_path/'missing',)))
        reference = receipt.accepted(10)
        result = receipt.wait(15)
        assert result.state == 'FAILED' and 'cannot capture' in result.error
        assert not list((tmp_path/'records').glob('*/record.json'))
        with pytest.raises(RunFailed) as failure:
            receipt.result()
        assert failure.value.result is result
        saved = RunHistory(owner.site.runs_dir).read_run(reference.run_id)
        assert saved.run_reported_outcome == 'failed'
        assert all(row.record is None for row in saved.invocations)
        assert RunHistory(owner.site.runs_dir).reproducibility(reference.run_id)['status'] == 'unavailable'
        command = subprocess.run([sys.executable, '-m', 'hedloom.cli', 'runs', 'show',
                                  reference.run_id, '--runs-dir', owner.site.runs_dir, '--json'],
                                 capture_output=True, text=True, timeout=10)
        assert command.returncode == 0, command.stderr
    finally:
        owner.close(15)


def test_rejected_acceptance_retains_a_receipt(tmp_path):
    owner = runtime(Site(records_dir=str(tmp_path/'records')))
    try:
        receipt = owner.submit(simple(), name='no-history', reproducibility=DISABLED)
        result = receipt.wait(15)
        assert result.state == 'REJECTED' and result.run_id is None
        assert 'runs_dir' in result.error
        with pytest.raises(AcceptanceError):
            receipt.accepted()
        with pytest.raises(RunFailed):
            receipt.result()
    finally:
        owner.close(15)


def test_late_history_failure_preserves_computation_and_terminal_error(tmp_path, monkeypatch):
    from hedloom.history import HistoryWriter
    original = HistoryWriter.observe
    def observe(self, outcome):
        if outcome.invocation_id in self.outcomes:
            raise OSError('controlled final history observation failure')
        return original(self, outcome)
    monkeypatch.setattr(HistoryWriter, 'observe', observe)
    with runtime(site(tmp_path)) as owner:
        receipt = owner.submit(simple(), name='late-storage', reproducibility=DISABLED)
        with pytest.warns(RuntimeWarning, match='controlled final history'):
            result = receipt.result(10)
        assert result.succeeded and result.error is None
        assert result.history.status == 'degraded'
        assert any('controlled final history' in error for error in result.history.errors)
        saved = RunHistory(owner.site.runs_dir).read_run(result.run_id)
        assert saved.run_reported_outcome == 'succeeded'
        assert saved.history_status == 'degraded'


def test_close_timeout_before_loop_publication_keeps_shutdown_request(tmp_path, monkeypatch):
    entered, release = Event(), Event()
    original = Runtime._serve
    def delayed(self):
        entered.set()
        assert release.wait(10)
        return original(self)
    monkeypatch.setattr(Runtime, '_serve', delayed)
    owner = runtime(site(tmp_path))
    try:
        assert entered.wait(10)
        with pytest.raises(TimeoutError):
            owner.close(.01)
        with pytest.raises(RuntimeClosed):
            owner.submit(simple(), name='late')
        release.set()
        owner.close(15)
        owner.close(15)
        assert not owner._thread.is_alive()
    finally:
        release.set()
        owner.close(15)


def test_cancelled_async_observer_does_not_withdraw_owned_work(tmp_path):
    gate = tmp_path/'gate'
    gate.mkdir()
    owner = runtime(site(tmp_path))
    try:
        receipt = owner.submit(holding(str(gate)), name='observed', reproducibility=DISABLED)
        until(lambda: (gate/'entered').exists())
        async def observation():
            async def wait():
                return await receipt
            task = asyncio.create_task(wait())
            await asyncio.sleep(.01)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not receipt.snapshot().stop_requested
            (gate/'release').touch()
            return await receipt
        assert asyncio.run(observation()).succeeded
    finally:
        (gate/'release').touch()
        owner.close(15)


def test_repeated_close_releases_runtime_owned_threads(tmp_path):
    for number in range(3):
        owner = runtime(site(tmp_path/f'owner-{number}'))
        owner.submit(simple(), name='one', reproducibility=DISABLED).result(15)
        owner.close(15)
        assert not owner._thread.is_alive()
        assert not any(thread.is_alive() for lane in
                       (owner._work, owner._storage, owner._observation)
                       for thread in lane.threads)


def test_startup_failure_rejects_receipts_and_releases_partial_resources(tmp_path, monkeypatch):
    from hedloom_run import cluster
    entered, release = Event(), Event()
    async def broken(site):
        entered.set()
        while not release.is_set():
            await asyncio.sleep(.01)
        raise OSError('controlled startup failure')
    monkeypatch.setattr(cluster, 'async_cluster_for', broken)
    owner = runtime(site(tmp_path))
    try:
        assert entered.wait(10)
        receipt = owner.submit(simple(), name='startup', reproducibility=DISABLED)
        release.set()
        result = receipt.wait(10)
        assert result.state == 'REJECTED' and 'startup failure' in result.error
        with pytest.raises(AcceptanceError):
            receipt.accepted()
        owner.close(10)
        assert not owner._thread.is_alive()
        assert not any(thread.is_alive() for lane in
                       (owner._work, owner._storage, owner._observation)
                       for thread in lane.threads)
    finally:
        release.set()
        owner.close(10)


def test_stop_cannot_overwrite_a_published_terminal_snapshot(tmp_path, monkeypatch):
    from hedloom.run import Run
    observed = []
    original = Run._update
    def update(self, **changes):
        original(self, **changes)
        if changes.get('state') == 'SUCCEEDED':
            observed.append(self.stop())
    monkeypatch.setattr(Run, '_update', update)
    with runtime(site(tmp_path)) as owner:
        receipt = owner.submit(simple(), name='finish-race', reproducibility=DISABLED)
        assert receipt.result(10).succeeded
        assert receipt.snapshot().state == 'SUCCEEDED'
        assert observed == [False]


_CHILD_SCRIPT = '''
import json, sys, time
from pathlib import Path
from hedloom import operation, parameter, returned, runtime, shell, study, Site, Reproducibility
root = Path(sys.argv[1])
@operation(config={'marker':parameter(str)}, outputs={'value':returned()})
def command(*, marker):
    return shell(sys.executable, '-c', 'import os,time;from pathlib import Path;Path(' + repr(marker) + ').write_text(str(os.getpid()));time.sleep(60)')
@study
def work(): return command(marker=str(root/'child'))
owner=runtime(Site(records_dir=str(root/'records'),runs_dir=str(root/'runs'),work_dir=str(root/'work')))
receipt=owner.submit(work(),name='owner',reproducibility=Reproducibility(enabled=False))
deadline=time.monotonic()+10
while not (root/'child').exists():
    if time.monotonic()>deadline: raise TimeoutError('child entry')
    time.sleep(.01)
(root/'ready').write_text('ready')
if sys.argv[2]!='exit': time.sleep(60)
'''


@pytest.mark.parametrize('mode', ['exit', 'term', 'kill'])
def test_owner_exit_reclaims_local_command_without_explicit_close(tmp_path, mode):
    process = subprocess.Popen([sys.executable, '-c', _CHILD_SCRIPT, str(tmp_path), mode],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        until(lambda: (tmp_path/'ready').exists(), timeout=15)
        child = int((tmp_path/'child').read_text())
        if mode != 'exit':
            process.send_signal(signal.SIGTERM if mode == 'term' else signal.SIGKILL)
        _, stderr = process.communicate(timeout=15)
        assert process.returncode == (0 if mode == 'exit' else -signal.SIGTERM if mode == 'term' else -signal.SIGKILL), stderr
        def stopped():
            status = Path(f'/proc/{child}/stat')
            try:
                return status.read_text().split()[2] == 'Z'
            except (FileNotFoundError, ProcessLookupError):
                return True
        until(stopped, timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)


def test_cold_pooled_runtime_preserves_signals_and_releases_workers(tmp_path):
    from importlib.util import find_spec
    if find_spec('dask_jobqueue') is None:
        pytest.skip('pooled placement needs dask-jobqueue')
    farm = Path(__file__).resolve().parents[1] / 'exec/tests/fakefarm'
    environment = dict(os.environ, PATH=str(farm) + os.pathsep + os.environ['PATH'],
                       FAKE_LSF_STATE=str(tmp_path/'farm'))
    script = '''
import signal, sys
from pathlib import Path
from hedloom import Site, operation, file, pooled, shell, study, runtime, Reproducibility
root = Path(sys.argv[1])
def interrupt(*args): pass
signal.signal(signal.SIGINT, interrupt)
assert 'dask_jobqueue' not in sys.modules
@operation(outputs={'value':file('value.txt')}, policy=pooled())
def command(out): return shell('sh', '-c', 'printf 42 > "$1"', 'write', out.value)
@study
def work(): return command()
site = Site(records_dir=str(root/'records'), runs_dir=str(root/'runs'),
    work_dir=str(root/'work'), dashboard='none', placements={'pool':{
    'kind':'lsf-pooled', 'queue':'normal', 'cores':1, 'memory_mb':1000,
    'walltime':'00:30', 'workers':1, 'max_jobs':1}})
with runtime(site) as owner:
    result = owner.submit(work(), name='cold', reproducibility=Reproducibility(enabled=False)).result(30)
    assert Path(result.outputs['output'].value).read_text() == '42'
assert signal.getsignal(signal.SIGINT) is interrupt
assert not owner._thread.is_alive()
'''
    command = subprocess.run([sys.executable, '-c', script, str(tmp_path)],
                             env=environment, capture_output=True, text=True, timeout=45)
    assert command.returncode == 0, command.stderr
    jobs = list((tmp_path/'farm').glob('*.json'))
    assert jobs, 'the cold Runtime must exercise actual fake-farm workers'
    until(lambda: all(json.loads(path.read_text())['state'] in {'DONE', 'EXIT'}
                      for path in jobs))
