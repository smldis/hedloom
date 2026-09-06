from dataclasses import replace
from pathlib import Path
import json
import multiprocessing
import os
import subprocess
import sys
import pytest

from hedloom import Site, operation, study, returned, file, parameter, RunHistory
from hedloom.history import HistoryError, HistoryWriter, read_events, publish, parse_run_id


@operation(outputs={'answer': returned()})
def answer():
    return None


@study(name='definition')
def subject():
    return answer()


def site_for(path):
    return Site(root=str(path / 'records'), history_root=str(path / 'history'))


def allocate(root, queue):
    writer = HistoryWriter(site_for(Path(root)), 'experiment.2', 'empty',
                           {'invocations': [], 'boundaries': []}, {})
    queue.put(writer.run_id)


def test_allocator_processes_reserved_gaps_and_partial_preparation(tmp_path):
    context = multiprocessing.get_context('spawn')
    queue = context.Queue()
    processes = [context.Process(target=allocate, args=(str(tmp_path), queue)) for _ in range(4)]
    for process in processes: process.start()
    try:
        names = {queue.get(timeout=20) for _ in processes}
        assert names == {f'experiment.2.{i}' for i in range(1, 5)}
    finally:
        for process in processes:
            process.join(20)
            if process.is_alive(): process.kill(); process.join()
            assert process.exitcode == 0
    reserved = tmp_path / 'history' / 'allocations' / 'experiment.2' / '5'
    reserved.mkdir()
    writer = HistoryWriter(site_for(tmp_path), 'experiment.2', 'empty', {'invocations': []}, {})
    assert writer.run_id == 'experiment.2.6'
    assert RunHistory(tmp_path / 'history').preparations() == (str(reserved),)
    assert parse_run_id('experiment.2.6') == ('experiment.2', 6)


@pytest.mark.parametrize('name', ['', ' has-space', '../escape', 'a/b', '_private'])
def test_invalid_names_refuse(tmp_path, name):
    with pytest.raises(HistoryError):
        subject().submit(site=site_for(tmp_path), name=name, sequential=True)
    assert not (tmp_path / 'records').exists()


def test_required_configuration_and_overlap(tmp_path):
    with pytest.raises(TypeError):
        subject().submit(site=site_for(tmp_path), sequential=True)
    with pytest.raises(HistoryError, match='history_root'):
        subject().submit(site=Site(root=str(tmp_path / 'records')), name='missing', sequential=True)
    with pytest.raises(HistoryError, match='overlap'):
        subject().submit(site=Site(root=str(tmp_path), history_root=str(tmp_path / 'nested')), name='overlap', sequential=True)
    assert not (tmp_path / 'records').exists()


def test_separate_histories_reuse_exact_reference_and_none(tmp_path):
    site = site_for(tmp_path)
    first = subject().submit(site=site, name='chosen', sequential=True)
    @study(name='other-definition')
    def other(): return answer.named('renamed')()
    second = other().submit(site=site, name='chosen', sequential=True)
    assert (first.run_id, second.run_id) == ('chosen.1', 'chosen.2')
    assert first['answer.1'].record == second['renamed'].record
    assert second['renamed'].reused
    history = RunHistory(site.history_root)
    assert [row.run_id for row in history.list_runs(name='chosen')] == ['chosen.2', 'chosen.1']
    assert history.outputs(first.run_id) == {'output': {'available': True, 'value': None}}
    assert first.history.status == second.history.status == 'complete'
    before = {str(path): path.stat().st_mtime_ns for path in tmp_path.rglob('*')}
    history.read_run(first.run_id)
    history.resolve_path(first.run_id, 'answer.1', journal_dir=True)
    from hedloom.discovery import list_attempts
    assert len(list_attempts(site.root)) == 1
    after = {str(path): path.stat().st_mtime_ns for path in tmp_path.rglob('*')}
    assert before == after


def test_torn_tail_corruption_and_unknown_schema(tmp_path):
    run = subject().submit(site=site_for(tmp_path), name='tail', sequential=True)
    location = Path(run.history.location)
    log = location / 'events.jsonl'
    original = log.read_bytes()
    log.write_bytes(original + b'{"seq":')
    snapshot = RunHistory(tmp_path / 'history').read_run(run.run_id)
    assert snapshot.history_status == 'degraded'
    assert 'incomplete event tail' in snapshot.diagnostics
    log.write_bytes(original + b'{}\n')
    with pytest.raises(HistoryError, match='corrupt'):
        read_events(log)
    log.write_bytes(original)
    header = json.loads((location / 'run.json').read_text())
    header['schema_version'] = 99
    (location / 'run.json').write_text(json.dumps(header))
    with pytest.raises(HistoryError, match='incompatible'):
        RunHistory(tmp_path / 'history').read_run(run.run_id)


def test_selection_write_failure_continues_and_returns_diagnostics(tmp_path, monkeypatch):
    import hedloom_run.execution as execution
    real = execution.write_document
    def fail(path, data, **kwargs):
        if Path(path).name == 'selection.json':
            raise OSError('injected selection failure')
        return real(path, data, **kwargs)
    monkeypatch.setattr(execution, 'write_document', fail)
    with pytest.warns(RuntimeWarning, match='history persistence degraded'):
        run = subject().submit(site=site_for(tmp_path), name='degraded', sequential=True)
    assert run.succeeded and run.history.status == 'degraded'
    assert run.report.outcomes[0].observation_errors
    snapshot = RunHistory(tmp_path / 'history').read_run(run.run_id)
    assert snapshot.invocations[0].record == run.report.outcomes[0].record
    assert snapshot.history_status == 'degraded'


def test_initial_failure_executes_nothing(tmp_path, monkeypatch):
    import hedloom.history as history
    def fail(*args, **kwargs): raise OSError('initial failure')
    monkeypatch.setattr(history, 'publish', fail)
    with pytest.raises(OSError, match='initial failure'):
        subject().submit(site=site_for(tmp_path), name='fail', sequential=True)
    assert not (tmp_path / 'records').exists()


def test_append_failure_stops_appending_and_keeps_outcomes(tmp_path, monkeypatch):
    real = HistoryWriter._append
    calls = []
    def append(self, event, data):
        calls.append(event)
        if event == 'invocation_outcome':
            with (self.location / 'events.jsonl').open('ab') as handle: handle.write(b'{')
            raise OSError('torn append')
        return real(self, event, data)
    monkeypatch.setattr(HistoryWriter, '_append', append)
    with pytest.warns(RuntimeWarning):
        run = subject().submit(site=site_for(tmp_path), name='append', sequential=True)
    assert run.succeeded and run.history.status == 'degraded'
    assert calls == ['run_started', 'invocation_outcome']
    assert RunHistory(tmp_path / 'history').read_run(run.run_id).run_reported_outcome == 'unreported'


def test_worker_visibility_failure_refuses_before_compute(tmp_path):
    class Invisible:
        def run(self, *args): raise OSError('worker cannot see history')
    with pytest.raises(OSError, match='worker cannot see history'):
        subject().submit(site=site_for(tmp_path), name='invisible', client=Invisible())
    assert not (tmp_path / 'records').exists()


def test_immutable_publication_conflicts(tmp_path):
    path = tmp_path / 'selection.json'
    publish(path, {'record': 'a', 'at': 'first'}, immutable=True)
    publish(path, {'record': 'a', 'at': 'later'}, immutable=True)
    with pytest.raises(HistoryError, match='conflicting'):
        publish(path, {'record': 'b'}, immutable=True)


@pytest.mark.parametrize('kernel', ['sequential', 'graph'])
@pytest.mark.parametrize('terminate', [False, True])
def test_live_discovery_from_separate_process_before_body_returns(tmp_path, kernel, terminate):
    """A pipe barrier, not a sleep, proves discovery happens before completion."""
    script = tmp_path / 'producer.py'
    script.write_text('''
import os, sys
from pathlib import Path
from hedloom import Site, operation, study, file
ready, release, root, kernel = sys.argv[1:]
@operation(outputs={"partial": file("partial.txt")})
def waiting(out):
    out.partial.write_text("visible before return")
    with open(ready, 'w') as signal: signal.write('ready\\n')
    with open(release) as barrier:
        if barrier.read(1) != 'x': raise RuntimeError('barrier failed')
@study(name="live-definition")
def build(): return waiting()
site=Site(root=str(Path(root)/'records'), workspace_root=str(Path(root)/'work'), history_root=str(Path(root)/'history'))
run=build().submit(site=site,name='live-inspection',sequential=kernel=='sequential')
assert run.succeeded and run.history.status=='complete', run.history
''')
    ready, release = tmp_path / 'ready', tmp_path / 'release'
    os.mkfifo(ready); os.mkfifo(release)
    # Nonblocking reader lets the parent impose a bounded deadline on readiness.
    import select
    fd = os.open(ready, os.O_RDONLY | os.O_NONBLOCK)
    process = subprocess.Popen([sys.executable, str(script), str(ready), str(release), str(tmp_path), kernel], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert select.select([fd], [], [], 30)[0], 'producer readiness timed out'
        assert os.read(fd, 100) == b'ready\n'
        profile = tmp_path / 'site.toml'
        profile.write_text('[study]\nroot="records"\nworkspace_root="work"\nhistory_root="history"\n')
        command = [sys.executable, '-m', 'hedloom.cli', 'runs']
        listing = subprocess.run([*command, 'list', '--site', str(profile), '--json'], capture_output=True, text=True, timeout=15, check=True)
        assert json.loads(listing.stdout)['runs'][0]['run_id'] == 'live-inspection.1'
        path = subprocess.run([*command, 'path', '--site', str(profile), 'live-inspection.1', '--invocation', 'waiting.1', '--workspace'], capture_output=True, text=True, timeout=15, check=True)
        assert path.stdout == str(tmp_path / 'work' / Path(path.stdout.strip()).name) + '\n'
        assert (Path(path.stdout.strip()) / 'partial.txt').read_text() == 'visible before return'
        row = RunHistory(tmp_path / 'history').read_run('live-inspection.1')
        assert row.run_reported_outcome == 'unreported'
        assert row.invocations[0].run_reported_outcome == 'unreported'
        assert process.poll() is None
        if terminate:
            process.kill()
            process.communicate(timeout=10)
            interrupted = RunHistory(tmp_path / 'history').read_run('live-inspection.1')
            assert interrupted.history_status == 'incomplete'
            # A later external recovery can publish terminal Exec evidence, but
            # cannot invent a completion report from the deceased consumer.
            from hedloom_exec.journal import AttemptJournal
            selected = interrupted.invocations[0]
            journal = AttemptJournal(tmp_path / 'records', selected.record)
            with journal.claim():
                journal.publish_terminal(try_number=selected.try_number,
                                         outcome='succeeded', manifest={})
            recovered = RunHistory(tmp_path / 'history').read_run('live-inspection.1')
            assert recovered.run_reported_outcome == 'unreported'
            assert recovered.invocations[0].selected_execution_state == 'succeeded'
            return
        with release.open('w') as barrier: barrier.write('x')
        stdout, stderr = process.communicate(timeout=30)
        assert process.returncode == 0, stderr.decode()
        assert RunHistory(tmp_path / 'history').read_run('live-inspection.1').history_status == 'complete'
    finally:
        os.close(fd)
        if process.poll() is None:
            process.kill(); process.communicate(timeout=10)


def test_addresses_ambiguity_slash_and_preflight(tmp_path):
    from hedloom import flow
    @flow
    def scope(): return answer.named('leaf')()
    @study(name='addresses')
    def build():
        scope.named('left')()
        scope.named('right')()
        answer.named('a/b')()
    run = build().submit(site=site_for(tmp_path), name='addresses', sequential=True)
    with pytest.raises(KeyError, match='left/leaf.*right/leaf'):
        run['leaf']
    assert run['left/leaf'].outcome == 'succeeded'
    assert run['a%2Fb'].outcome == 'succeeded'
    with pytest.raises(KeyError, match='only %2F'):
        run['a%252Fb']


@pytest.mark.parametrize('sequential', [True, False])
def test_postselection_refusal_keeps_reference(tmp_path, sequential):
    from hedloom_exec.transport import SubmissionRefused
    from hedloom_run.driver import run_plan
    from hedloom_run.graph import run_plan_graph
    from hedloom_run.cluster import cluster_for
    from distributed import Client
    class Refusing:
        name = 'refusing'
        def submit(self, identity, bundle): raise SubmissionRefused('after allocation')
    common = dict(root=str(tmp_path / 'records'), transports={'local': Refusing()})
    if sequential:
        report = run_plan(subject().document, **common)
    else:
        with cluster_for(site_for(tmp_path)) as cluster, Client(cluster) as client:
            report = run_plan_graph(subject().document, client=client, **common)
    row = report.outcomes[0]
    assert row.outcome == 'failed' and row.record and row.try_number == 0


def test_callback_exception_preserves_history_and_finalization_precedes_retention(tmp_path, monkeypatch):
    from importlib import import_module
    module = import_module('hedloom.study')
    def fail(outcome): raise RuntimeError('user callback')
    with pytest.warns(RuntimeWarning), pytest.raises(RuntimeError, match='user callback') as caught:
        subject().submit(site=site_for(tmp_path), name='callback', sequential=True, on_event=fail)
    assert caught.value.history.status == 'degraded'
    assert RunHistory(tmp_path / 'history').read_run('callback.1').invocations[0].run_reported_outcome == 'succeeded'
    def retention(site):
        assert RunHistory(site.history_root).read_run('retention.1').history_status == 'complete'
    monkeypatch.setattr(module, '_apply_automatic_retention', retention)
    subject().submit(site=site_for(tmp_path), name='retention', sequential=True)


def test_old_run_never_redirects_when_standing_changes(tmp_path):
    from hedloom_exec.journal import AttemptJournal
    run = subject().submit(site=site_for(tmp_path), name='original', sequential=True)
    outcome = run['answer.1']
    journal = AttemptJournal(tmp_path / 'records', outcome.record)
    with journal.claim():
        number = journal.begin_try()
        journal.publish_terminal(try_number=number, outcome='succeeded', manifest={'value': 99})
    history = RunHistory(tmp_path / 'history')
    assert history.read_run(run.run_id).invocations[0].try_number == outcome.try_number
    assert history.outputs(run.run_id)['output']['value'] is None
    from hedloom.discovery import list_attempts
    assert {row['try_number'] for row in list_attempts(tmp_path / 'records')} == {0, 1}


def test_absent_vs_reclaimed_workspace_and_path_stdout(tmp_path, capsys):
    from hedloom_exec.journal import AttemptJournal
    from hedloom.cli import main
    run = subject().submit(site=site_for(tmp_path), name='payload', sequential=True)
    history = RunHistory(tmp_path / 'history')
    row = history.read_run(run.run_id).invocations[0]
    workspace = Path(row.workspace)
    # Value-only output has no files in this workspace.
    import shutil
    shutil.rmtree(workspace)
    assert history.read_run(run.run_id).invocations[0].workspace_status == 'missing'
    journal = AttemptJournal(tmp_path / 'records', row.record)
    with journal.claim():
        journal.append('workspace_removed', **{'try': row.try_number, 'workspace': str(workspace)})
    assert history.read_run(run.run_id).invocations[0].workspace_status == 'reclaimed'
    profile = tmp_path / 'site.toml'
    profile.write_text('[study]\nroot="records"\nhistory_root="history"\n')
    assert main(['runs', 'path', '--site', str(profile), run.run_id, '--invocation', 'answer.1', '--workspace']) == 2
    captured = capsys.readouterr()
    assert captured.out == '' and 'reclaimed' in captured.err


def test_no_workspace_is_explicit(tmp_path):
    @operation
    def nothing(): pass
    @study(name='nothing-definition')
    def build(): nothing()
    run = build().submit(site=site_for(tmp_path), name='nothing', sequential=True)
    row = RunHistory(tmp_path / 'history').read_run(run.run_id).invocations[0]
    assert row.workspace_status == 'no-workspace' and row.workspace is None


def test_site_transformations_keep_history_root(tmp_path):
    profile = tmp_path / 'site.toml'
    profile.write_text('[study]\nroot="records"\nworkspace_root="work"\nhistory_root="history"\n')
    site = Site.from_file(profile)
    assert site.history_root == str(tmp_path / 'history')
    assert site.overridden({'kernel': {'threads': 2}}).history_root == site.history_root
    assert site.served_in_process().history_root == site.history_root
    assert site.with_transports().history_root == site.history_root


def test_preselection_refusal_publishes_no_link(tmp_path):
    from hedloom import named_policy
    @operation(policy=named_policy('unavailable')())
    def missing(): pass
    @study(name='refused-definition')
    def build(): missing()
    run = build().submit(site=site_for(tmp_path), name='refused', sequential=True)
    row = RunHistory(tmp_path / 'history').read_run(run.run_id).invocations[0]
    assert row.run_reported_outcome == 'failed' and row.record is None


def test_reused_workspace_comes_from_receipt_not_new_site_root(tmp_path):
    first_site = replace(site_for(tmp_path), workspace_root=str(tmp_path / 'original-work'))
    first = subject().submit(site=first_site, name='first', sequential=True)
    second_site = replace(first_site, workspace_root=str(tmp_path / 'new-work'))
    second = subject().submit(site=second_site, name='second', sequential=True)
    history = RunHistory(first_site.history_root)
    assert second['answer.1'].reused
    assert history.resolve_path(first.run_id, 'answer.1', workspace=True) == history.resolve_path(second.run_id, 'answer.1', workspace=True)
    assert not (tmp_path / 'new-work').exists()
