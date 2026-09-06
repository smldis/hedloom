"""Exact live attribution through the real facade, graph kernel and Exec."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
import json
import subprocess
import sys
import time

import pytest

from hedloom import Site, RunHistory, artifact, file, operation, parameter, returned, session, study
from hedloom.history import HistoryWriter, read_json, slot
from hedloom_run.execution import ExecutionError, ExecutionHandle, read_document

pytest.importorskip('distributed')


def wait_for(predicate):
    deadline = time.monotonic() + 15
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError('barrier timed out')
        time.sleep(0.01)


@operation(config={'markers': parameter(str), 'fail': parameter(bool)},
           outputs={'partial': file('partial.txt', kind='shared-probe')})
def held_file(out, markers, fail):
    directory = Path(markers)
    with (directory / 'calls').open('a') as stream:
        stream.write('call\n')
    out.partial.write_text('visible before return')
    (directory / 'started').touch()
    wait_for(lambda: (directory / 'release').exists())
    if fail:
        raise ValueError('deliberate failure')


@operation(inputs={'source': artifact('shared-probe')},
           outputs={'size': returned()})
def read_held(source):
    return {'size': len(Path(source).read_text())}


@operation(outputs={'value': returned()})
def immediate():
    return {'value': 1}


@operation(config={'markers': parameter(str)}, outputs={'value': returned()})
def fail_when_released(markers):
    directory = Path(markers)
    (directory / 'failure-started').touch()
    wait_for(lambda: (directory / 'fail-now').exists())
    raise ValueError('controlled branch failure')


@study
def failure_branches(markers):
    fail_when_released.named('bad')(markers=markers)
    return held_file.named('work')(markers=markers, fail=False)


@operation(inputs={'source': artifact('shared-probe')},
           config={'markers': parameter(str)}, outputs={'copy': file('copy.txt', kind='shared-probe')})
def held_copy(source, markers, out):
    directory = Path(markers)
    with (directory / 'calls').open('a') as stream:
        stream.write('call\n')
    out.copy.write_text(Path(source).read_text())
    (directory / 'started').touch()
    wait_for(lambda: (directory / 'release').exists())


@study
def staged_plan(first, second):
    return held_copy(held_file(markers=first, fail=False), markers=second)


@study
def withdrawal_plan(markers):
    immediate.named('done')()
    return held_file.named('work')(markers=markers, fail=False)


@study
def shared_plan(markers, fail=False, label='work'):
    return read_held.named('read-' + label)(held_file.named(label)(markers=markers, fail=fail))


def site_for(root, threads=2):
    return Site(root=str(root / 'records'), workspace_root=str(root / 'work'),
                history_root=str(root / 'history'), threads=threads)


def bound(root, name):
    return list((root / 'history' / 'runs' / (name + '.1') / 'selections').glob('*/execution.json'))


@pytest.mark.parametrize('fail', [False, True])
def test_staggered_consumers_share_live_paths_and_keep_their_names(tmp_path, fail):
    site = site_for(tmp_path)
    profile = tmp_path / 'site.toml'
    profile.write_text('[study]\nroot="records"\nworkspace_root="work"\nhistory_root="history"\n')
    history = RunHistory(site.history_root)
    with session(site) as live, ThreadPoolExecutor(2) as threads:
        first = threads.submit(live.submit, shared_plan(str(tmp_path), fail, 'alpha'),
                               name='first', stop_on_failure=False)
        try:
            wait_for(lambda: (tmp_path / 'started').exists())
            second = threads.submit(live.submit, shared_plan(str(tmp_path), fail, 'beta'),
                                    name='second', stop_on_failure=False)
            wait_for(lambda: len(bound(tmp_path, 'second')) == 1)
            paths = []
            for name, invocation in [('first.1', 'alpha'), ('second.1', 'beta')]:
                result = subprocess.run([sys.executable, '-m', 'hedloom.cli', 'runs', 'path',
                    '--site', str(profile), name, '--invocation', invocation, '--workspace'],
                    capture_output=True, text=True, check=True, timeout=15)
                paths.append(result.stdout.strip())
                assert (Path(paths[-1]) / 'partial.txt').read_text() == 'visible before return'
            assert paths[0] == paths[1]
            assert not first.done() and not second.done()
        finally:
            (tmp_path / 'release').touch()
        a, b = first.result(timeout=15), second.result(timeout=15)
        assert a.succeeded == b.succeeded == (not fail)
        assert a.history.status == b.history.status == 'complete'
        assert (tmp_path / 'calls').read_text().splitlines() == ['call']
        assert a['alpha'].record == b['beta'].record
        assert a['alpha'].try_number == b['beta'].try_number
        assert b['read-beta'].outcome == ('blocked' if fail else 'succeeded')
        old_id = history.invocation(a.run_id, 'alpha').execution_id
        later = live.submit(shared_plan(str(tmp_path), fail), name='later', stop_on_failure=False)
        later_row = history.invocation(later.run_id, 'work')
        assert later_row.execution_id != old_id
        assert later_row.try_number == a['alpha'].try_number + int(fail)
        assert history.invocation(a.run_id, 'alpha').execution_id == old_id
        assert history.invocation(a.run_id, 'alpha').try_number == a['alpha'].try_number


def test_binding_failure_prevents_that_invocation_admission(tmp_path, monkeypatch):
    (tmp_path / "release").touch()
    original = HistoryWriter.bind_execution
    calls = []
    def broken(self, identifier, handle, inputs=None):
        calls.append(identifier)
        if len(calls) == 2:
            raise OSError('second binding failed')
        return original(self, identifier, handle, inputs)
    monkeypatch.setattr(HistoryWriter, 'bind_execution', broken)
    with pytest.warns(RuntimeWarning), pytest.raises(OSError, match='second binding'):
        shared_plan(str(tmp_path)).submit(site=site_for(tmp_path), name='broken')
    assert (tmp_path / 'calls').read_text().splitlines() == ['call']
    assert len(list((tmp_path / 'records').glob('*/events.jsonl'))) == 1


def test_worker_reentry_cannot_select_a_second_try(tmp_path):
    site = site_for(tmp_path)
    with session(site) as live, ThreadPoolExecutor(1) as threads:
        first = threads.submit(live.submit, shared_plan(str(tmp_path), True), name='first', stop_on_failure=False)
        try:
            wait_for(lambda: (tmp_path / 'started').exists())
            with live._execution_owner.lock:
                entry = next(iter(live._execution_owner.groups.values()))
            selection = read_document(Path(entry.handle.location) / 'selection.json')
        finally:
            (tmp_path / 'release').touch()
        run = first.result(timeout=15)
        assert not run.succeeded
        live.client.retry([entry.future])
        with pytest.raises(ExecutionError, match='replay refused'):
            entry.future.result(timeout=10)
        assert read_document(Path(entry.handle.location) / 'selection.json') == selection
        assert (tmp_path / 'calls').read_text().splitlines() == ['call']


def test_entry_and_cancellation_have_one_durable_winner(tmp_path):
    for index in range(20):
        handle = ExecutionHandle.create(tmp_path / str(index))
        with ThreadPoolExecutor(2) as threads:
            enter = threads.submit(handle.enter)
            cancel = threads.submit(handle.cancel_before_start)
            assert enter.result() != cancel.result()
        if handle.state() == 'entered':
            with pytest.raises(ExecutionError, match='replay refused'):
                handle.enter()
        else:
            assert handle.state() == 'cancelled'
            assert handle.enter() is False


def test_late_arrival_keeps_finished_predecessor_and_active_successor(tmp_path):
    first_dir, second_dir = tmp_path / 'first', tmp_path / 'second'
    first_dir.mkdir()
    second_dir.mkdir()
    (first_dir / 'release').touch()
    site = site_for(tmp_path)
    subject = staged_plan(str(first_dir), str(second_dir))
    with session(site) as live, ThreadPoolExecutor(2) as threads:
        a = threads.submit(live.submit, subject, name='a')
        try:
            wait_for(lambda: (second_dir / 'started').exists())
            b = threads.submit(live.submit, subject, name='b')
            wait_for(lambda: len(bound(tmp_path, 'b')) == 2)
            history = RunHistory(site.history_root)
            assert history.invocation('a.1', 'held_file.1').execution_id != history.invocation('b.1', 'held_file.1').execution_id
            assert history.invocation('a.1', 'held_copy.1').execution_id == history.invocation('b.1', 'held_copy.1').execution_id
            for invocation in ('held_file.1', 'held_copy.1'):
                assert history.resolve_path('a.1', invocation, workspace=True) == history.resolve_path('b.1', invocation, workspace=True)
        finally:
            (second_dir / 'release').touch()
        assert a.result(timeout=15).succeeded and b.result(timeout=15).succeeded
    assert (first_dir / 'calls').read_text().splitlines() == ['call']
    assert (second_dir / 'calls').read_text().splitlines() == ['call']


def test_callback_withdrawal_leaves_other_consumer_running(tmp_path):
    site = site_for(tmp_path)
    def callback(outcome):
        if outcome.authored_key == 'done':
            (tmp_path / 'callback-ready').touch()
            wait_for(lambda: (tmp_path / 'withdraw').exists())
            raise RuntimeError('consumer A stopped')

    with session(site) as live, ThreadPoolExecutor(2) as threads:
        with pytest.warns(RuntimeWarning, match='history persistence degraded'):
            a = threads.submit(live.submit, withdrawal_plan(str(tmp_path)), name='a', on_event=callback)
            try:
                wait_for(lambda: (tmp_path / 'callback-ready').exists() and (tmp_path / 'started').exists())
                b = threads.submit(live.submit, withdrawal_plan(str(tmp_path)), name='b')
                wait_for(lambda: len(bound(tmp_path, 'b')) == 2)
                (tmp_path / 'withdraw').touch()
                with pytest.raises(RuntimeError, match='consumer A stopped') as caught:
                    a.result(timeout=10)
                outcomes = {row.authored_key: row for row in caught.value.report.outcomes}
                assert outcomes['work'].outcome == 'cancelled'
                assert outcomes['work'].disposition == 'withdrawn'
                assert not b.done()
            finally:
                (tmp_path / 'withdraw').touch()
                (tmp_path / 'release').touch()
            assert b.result(timeout=15).succeeded
    assert (tmp_path / 'calls').read_text().splitlines() == ['call']


def test_last_consumer_waits_for_entered_work_instead_of_misreporting_blocked(tmp_path):
    site = site_for(tmp_path)
    def callback(outcome):
        if outcome.authored_key == 'done':
            wait_for(lambda: (tmp_path / 'started').exists())
            (tmp_path / 'callback-raised').touch()
            raise RuntimeError('last consumer stopped')
    with session(site) as live, ThreadPoolExecutor(1) as threads:
        with pytest.warns(RuntimeWarning, match='history persistence degraded'):
            a = threads.submit(live.submit, withdrawal_plan(str(tmp_path)), name='last', on_event=callback)
            try:
                wait_for(lambda: (tmp_path / 'callback-raised').exists())
                assert not a.done()
            finally:
                (tmp_path / 'release').touch()
            with pytest.raises(RuntimeError, match='last consumer stopped') as caught:
                a.result(timeout=15)
    outcomes = {row.authored_key: row for row in caught.value.report.outcomes}
    assert outcomes['work'].outcome == 'succeeded'
    assert outcomes['work'].record is not None


def test_last_consumer_cancels_before_entry_even_when_dask_started_the_wrapper(tmp_path, monkeypatch):
    import hedloom_run.graph as graph
    original = graph._run_handle
    def delayed(handle, item, available, config, *upstream):
        if item.authored_key == 'work':
            (tmp_path / 'before-entry').touch()
            wait_for(lambda: (tmp_path / 'allow-entry').exists())
        return original(handle, item, available, config, *upstream)
    monkeypatch.setattr(graph, '_run_handle', delayed)
    def callback(outcome):
        if outcome.authored_key == 'done':
            wait_for(lambda: (tmp_path / 'before-entry').exists())
            raise RuntimeError('cancel before entry')
    with session(site_for(tmp_path)) as live, ThreadPoolExecutor(1) as threads:
        def no_cancel(*args, **kwargs):
            raise AssertionError('consumer must not cancel a shared Dask key')
        monkeypatch.setattr(live.client, 'cancel', no_cancel)
        with pytest.warns(RuntimeWarning, match='history persistence degraded'):
            a = threads.submit(live.submit, withdrawal_plan(str(tmp_path)), name='cancel', on_event=callback)
            try:
                with pytest.raises(RuntimeError, match='cancel before entry') as caught:
                    a.result(timeout=15)
                outcomes = {row.authored_key: row for row in caught.value.report.outcomes}
                assert outcomes['work'].outcome == 'blocked'
                assert outcomes['work'].record is None
                assert not (tmp_path / 'calls').exists()
            finally:
                (tmp_path / 'allow-entry').touch()
                (tmp_path / 'release').touch()
    assert not (tmp_path / 'calls').exists()


def test_same_owner_does_not_share_different_record_and_workspace_bindings(tmp_path):
    site = site_for(tmp_path)
    other = replace(site, root=str(tmp_path / 'other-records'),
                    workspace_root=str(tmp_path / 'other-work'))
    subject = shared_plan(str(tmp_path))
    with session(site) as live, ThreadPoolExecutor(2) as threads:
        a = threads.submit(live.submit, subject, name='a')
        try:
            wait_for(lambda: (tmp_path / 'started').exists())
            # The explicit Run owner is also usable by an advanced caller;
            # its compatibility check must include the complete Site binding.
            b = threads.submit(subject._run, site=other, client=live.client,
                               execution_owner=live._execution_owner, name='b')
            wait_for(lambda: len((tmp_path / 'calls').read_text().splitlines()) == 2)
            history = RunHistory(site.history_root)
            row_a = history.invocation('a.1', 'work')
            row_b = history.invocation('b.1', 'work')
            assert row_a.execution_id != row_b.execution_id
            assert row_a.journal_dir != row_b.journal_dir
            assert history.resolve_path('a.1', 'work', workspace=True) != history.resolve_path('b.1', 'work', workspace=True)
        finally:
            (tmp_path / 'release').touch()
        assert a.result(timeout=15).succeeded and b.result(timeout=15).succeeded


def test_consumers_can_choose_different_stop_policies(tmp_path):
    site = site_for(tmp_path)
    with session(site) as live, ThreadPoolExecutor(2) as threads:
        a = threads.submit(live.submit, failure_branches(str(tmp_path)), name='a', stop_on_failure=True)
        try:
            wait_for(lambda: (tmp_path / 'failure-started').exists() and (tmp_path / 'started').exists())
            b = threads.submit(live.submit, failure_branches(str(tmp_path)), name='b', stop_on_failure=False)
            wait_for(lambda: len(bound(tmp_path, 'b')) == 2)
            (tmp_path / 'fail-now').touch()
            stopped = a.result(timeout=15)
            assert stopped['bad'].outcome == 'failed'
            assert stopped['work'].disposition == 'withdrawn'
            assert stopped['work'].outcome == 'cancelled'
            assert stopped.history.status == 'complete'
            assert not b.done()
        finally:
            (tmp_path / 'fail-now').touch()
            (tmp_path / 'release').touch()
        continued = b.result(timeout=15)
        assert continued['bad'].outcome == 'failed'
        assert continued['work'].outcome == 'succeeded'
        assert continued.history.status == 'complete'
    assert (tmp_path / 'calls').read_text().splitlines() == ['call']
