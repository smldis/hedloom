import base64
import json
import subprocess

import pytest

from hedloom import Reproducibility, RunHistory, session, submit, capture_environment
from hedloom.reproducibility import capture
from test_discovery import site_for, subject


def git(path, *args):
    return subprocess.check_output(['git', '-C', str(path), *args]).decode().strip()


def test_editable_revision_and_file_bytes_survive_edits(tmp_path, monkeypatch):
    repo = tmp_path / 'editable project'
    repo.mkdir()
    git(repo, 'init', '-q')
    (repo / 'pyproject.toml').write_text('[project]\nname="example"\nversion="1"\n')
    (repo / 'uv.lock').write_bytes(b'version = 1\n')
    git(repo, 'add', '.')
    git(repo, '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-qm', 'initial')
    commit = git(repo, 'rev-parse', 'HEAD')
    class Distribution:
        metadata = {'Name': 'example'}
        version = '1'
        def read_text(self, name):
            return json.dumps({'url': repo.as_uri(), 'dir_info': {'editable': True}})
    monkeypatch.setattr('hedloom.reproducibility.metadata.distributions', lambda: [Distribution()])
    monkeypatch.chdir(repo)
    config = tmp_path / 'setup.sh'
    config.write_bytes(b'export TOOL_VERSION=2\n')
    (repo / 'uv.lock').write_bytes(b'version = 2\n')
    evidence = capture(Reproducibility(text='load toolchain 2', files=(config,)))
    assert evidence['environment']['packages'][0]['direct_url']['url'] == repo.as_uri()
    assert evidence['environment']['repositories'][str(repo)]['commit'] == commit
    assert evidence['environment']['repositories'][str(repo)]['dirty']
    assert b'+version = 2' in base64.b64decode(evidence['environment']['repositories'][str(repo)]['tracked_patch_base64'])
    config.unlink()
    assert base64.b64decode(evidence['files'][str(config)]['content']) == b'export TOOL_VERSION=2\n'
    assert evidence['text'] == 'load toolchain 2'


@pytest.mark.parametrize("sequential", [True, False])
def test_saved_before_execution_and_distinct_for_reuse(tmp_path, monkeypatch, sequential):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('hedloom.reproducibility.metadata.distributions', lambda: [])
    site = site_for(tmp_path)
    history = RunHistory(site.history_root)
    config = tmp_path / 'setup.sh'
    config.write_text('first')
    seen = []
    first = submit(subject(), site=site, name='environment', sequential=sequential,
                   on_started=lambda ref: seen.append(history.reproducibility(ref.run_id)),
                   reproducibility=Reproducibility(files=(config,)))
    config.write_text('second')
    with session(site, sequential=sequential) as live:
        second = live.submit(subject(), name='environment', reproducibility=Reproducibility(text='second', files=(config,)))
        disabled = live.submit_all({'disabled': subject()}, reproducibility=Reproducibility(enabled=False))['disabled']
    assert second['answer.1'].reused
    assert base64.b64decode(seen[0]['files'][str(config)]['content']) == b'first'
    assert base64.b64decode(history.reproducibility(second.run_id)['files'][str(config)]['content']) == b'second'
    assert history.reproducibility(first.run_id) == seen[0]
    assert history.reproducibility(disabled.run_id)['status'] == 'disabled'


def test_missing_attachment_refuses_before_execution(tmp_path):
    with pytest.raises(ValueError, match='cannot capture reproducibility file'):
        subject().submit(site=site_for(tmp_path), name='missing', sequential=True,
                         reproducibility=Reproducibility(files=(tmp_path / 'missing',)))
    assert not (tmp_path / 'records').exists()


def test_git_unavailable_is_explicit(tmp_path, monkeypatch):
    monkeypatch.setattr('hedloom.reproducibility.metadata.distributions', lambda: [])
    monkeypatch.setattr('hedloom.reproducibility._git', lambda *args: (_ for _ in ()).throw(FileNotFoundError()))
    evidence = capture(Reproducibility(project_root=tmp_path))
    assert evidence['status'] == 'partial'
    assert any('Git revision unavailable' in gap for gap in evidence['gaps'])


def test_older_history_and_missing_new_record(tmp_path, monkeypatch):
    from hedloom.history import HistoryError, HistoryWriter
    writer = HistoryWriter(site_for(tmp_path), 'older', 'empty',
                           {'invocations': [], 'boundaries': []}, {})
    history = RunHistory(tmp_path / 'history')
    assert history.reproducibility(writer.run_id) is None
    monkeypatch.setattr('hedloom.reproducibility.metadata.distributions', lambda: [])
    run = subject().submit(site=site_for(tmp_path), name='newer', sequential=True)
    (tmp_path / 'history' / 'runs' / run.run_id / 'reproducibility.json').unlink()
    with pytest.raises(HistoryError, match='cannot read'):
        history.reproducibility(run.run_id)


def test_reproducibility_persistence_failure_refuses_execution(tmp_path, monkeypatch):
    import hedloom.history as history
    original = history.publish
    def fail(path, *args, **kwargs):
        if path.name == 'reproducibility.json':
            raise OSError('injected reproducibility failure')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(history, 'publish', fail)
    with pytest.raises(OSError, match='injected reproducibility failure'):
        subject().submit(site=site_for(tmp_path), name='failure', sequential=True)
    assert not (tmp_path / 'records').exists()


def test_cli_exposes_saved_environment(tmp_path, monkeypatch, capsys):
    from hedloom.cli import main
    monkeypatch.setattr('hedloom.reproducibility.metadata.distributions', lambda: [])
    run = subject().submit(site=site_for(tmp_path), name='inspect', sequential=True,
                           reproducibility=Reproducibility(text='recorded setup'))
    profile = tmp_path / 'site.toml'
    profile.write_text('[study]\nroot="records"\nhistory_root="history"\n')
    assert main(['runs', 'show', '--site', str(profile), run.run_id, '--json']) == 0
    data = json.loads(capsys.readouterr().out)
    assert data['reproducibility']['text'] == 'recorded setup'
    assert main(['runs', 'show', '--site', str(profile), run.run_id]) == 0
    assert 'reproducibility: partial' in capsys.readouterr().out


def test_compact_environment_omits_lockfiles_and_extra_ancestors(tmp_path, monkeypatch):
    project = tmp_path / 'project'
    project.mkdir()
    (tmp_path / 'pyproject.toml').write_text('parent')
    manifest = project / 'pyproject.toml'
    manifest.write_text('project')
    (project / 'uv.lock').write_text('large lock')
    monkeypatch.setattr('hedloom.reproducibility.metadata.distributions', lambda: [])
    from pathlib import Path
    original = Path.is_file
    def no_lock_stat(path):
        assert path.name != 'uv.lock', 'lockfile discovery is unnecessary work'
        return original(path)
    monkeypatch.setattr(Path, 'is_file', no_lock_stat)
    data = capture_environment(project_root=project).to_data()
    assert set(data['files']) == {str(manifest)}
    assert 'python_version' in data
    assert 'platform' not in data and 'python' not in data


def test_session_cache_refresh_and_fresh_attachments(tmp_path, monkeypatch):
    from pathlib import Path
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('hedloom.reproducibility.metadata.distributions', lambda: [])
    manifest = tmp_path / 'pyproject.toml'
    manifest.write_text('first environment')
    config = tmp_path / 'setup.sh'
    config.write_text('first config')
    site = site_for(tmp_path)
    history = RunHistory(site.history_root)
    with session(site, sequential=True) as live:
        first = live.submit(subject(), name='first', reproducibility=Reproducibility(files=(config,)))
        manifest.write_text('second environment')
        config.write_text('second config')
        with monkeypatch.context() as guarded:
            def refuse(*args, **kwargs):
                pytest.fail('cached submission rediscovered dependencies')
            guarded.setattr('hedloom.reproducibility.metadata.distributions', refuse)
            guarded.setattr('hedloom.reproducibility.capture_environment', refuse)
            original = Path.read_bytes
            def read(path):
                assert path != manifest, 'cached manifest was reread'
                return original(path)
            guarded.setattr(Path, 'read_bytes', read)
            second = live.submit(subject(), name='second', reproducibility=Reproducibility(files=(config,)))
        refreshed = live.refresh_environment()
        third = live.submit(subject(), name='third')
    a, b, c = [history.reproducibility(run.run_id) for run in (first, second, third)]
    assert a['environment'] == b['environment']
    assert base64.b64decode(b['files'][str(config)]['content']) == b'second config'
    assert base64.b64decode(a['environment']['files'][str(manifest)]['content']) == b'first environment'
    assert base64.b64decode(c['environment']['files'][str(manifest)]['content']) == b'second environment'
    assert c['environment'] == refreshed.to_data()
    assert c['environment']['captured_at'] != a['environment']['captured_at']
    detached = refreshed.to_data()
    detached['files'].clear()
    assert refreshed.to_data()['files']


def test_explicit_environment_reusable_across_standalone_submissions(tmp_path, monkeypatch):
    monkeypatch.setattr('hedloom.reproducibility.metadata.distributions', lambda: [])
    environment = capture_environment(project_root=tmp_path)
    def refuse(*args, **kwargs):
        pytest.fail('explicit environment was recaptured')
    monkeypatch.setattr('hedloom.reproducibility.metadata.distributions', refuse)
    monkeypatch.setattr('hedloom.reproducibility.capture_environment', refuse)
    site = site_for(tmp_path)
    for name in ('first', 'second'):
        run = submit(subject(), site=site, name=name, sequential=True, environment=environment)
        assert RunHistory(site.history_root).reproducibility(run.run_id)['environment'] == environment.to_data()


def test_concurrent_submissions_discover_once_with_slow_metadata(tmp_path, monkeypatch):
    import time
    from threading import Lock
    monkeypatch.chdir(tmp_path)
    calls = []
    lock = Lock()
    def slow_distributions():
        with lock:
            calls.append(1)
        time.sleep(0.05)  # Model slow discovery; correctness is checked by count.
        return []
    monkeypatch.setattr('hedloom.reproducibility.metadata.distributions', slow_distributions)
    site = site_for(tmp_path)
    with session(site) as live:
        runs = live.submit_all({'first': subject(), 'second': subject()})
    assert len(calls) == 1
    history = RunHistory(site.history_root)
    assert history.reproducibility(runs['first'].run_id)['environment'] == history.reproducibility(runs['second'].run_id)['environment']


def test_disabled_capture_does_not_discover_environment(tmp_path, monkeypatch):
    def refuse(*args, **kwargs):
        pytest.fail('disabled reproducibility performed discovery')
    monkeypatch.setattr('hedloom.reproducibility.metadata.distributions', refuse)
    monkeypatch.setattr('hedloom.reproducibility._git', refuse)
    run = subject().submit(site=site_for(tmp_path), name='disabled', sequential=True,
                           reproducibility=Reproducibility(enabled=False))
    assert RunHistory(tmp_path / 'history').reproducibility(run.run_id)['status'] == 'disabled'


def test_failed_refresh_preserves_previous_snapshot(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('hedloom.reproducibility.metadata.distributions', lambda: [])
    with session(site_for(tmp_path), sequential=True) as live:
        before = live.refresh_environment()
        def broken(*args, **kwargs):
            raise OSError('environment temporarily inaccessible')
        from importlib import import_module
        monkeypatch.setattr(import_module('hedloom.session'), 'capture_environment', broken)
        with pytest.raises(OSError, match='temporarily inaccessible'):
            live.refresh_environment()
        run = live.submit(subject(), name='cached')
        assert RunHistory(tmp_path / 'history').reproducibility(run.run_id)['environment'] == before.to_data()


def test_project_roots_have_separate_caches(tmp_path, monkeypatch):
    monkeypatch.setattr('hedloom.reproducibility.metadata.distributions', lambda: [])
    first, second = tmp_path / 'first', tmp_path / 'second'
    first.mkdir()
    second.mkdir()
    (first / 'pyproject.toml').write_text('first')
    (second / 'pyproject.toml').write_text('second')
    site = site_for(tmp_path)
    with session(site, sequential=True) as live:
        a = live.submit(subject(), name='first', reproducibility=Reproducibility(project_root=first))
        b = live.submit(subject(), name='second', reproducibility=Reproducibility(project_root=second))
    history = RunHistory(site.history_root)
    assert history.reproducibility(a.run_id)['environment']['project_root'] == str(first)
    assert history.reproducibility(b.run_id)['environment']['project_root'] == str(second)


def committed_study(tmp_path):
    import runpy
    repo = tmp_path / 'studies'
    repo.mkdir()
    script = repo / 'my study.py'
    script.write_text('def work():\n    return 1\n')
    git(repo, 'init', '-q')
    git(repo, 'add', '.')
    git(repo, '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-qm', 'study')
    return repo, script, runpy.run_path(str(script))['work']


def test_clean_study_uses_recoverable_git_reference_each_run(tmp_path, monkeypatch):
    monkeypatch.setattr('hedloom.reproducibility.metadata.distributions', lambda: [])
    repo, script, body = committed_study(tmp_path)
    environment = capture_environment(project_root=tmp_path)
    first = capture(implementations=(body,), environment=environment)
    ref = first['sources'][str(script)]
    assert str(script) not in first['files']
    assert ref['path'] == 'my study.py'
    assert ref['repository'] == str(repo)
    assert git(repo, 'show', ref['commit'] + ':' + ref['path']) == script.read_text().strip()
    script.write_text('def work():\n    return 2\n')
    git(repo, 'add', '.')
    git(repo, '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-qm', 'change')
    second = capture(implementations=(body,), environment=environment)
    assert second['environment'] == first['environment']
    assert second['sources'][str(script)]['commit'] != ref['commit']
    assert first['sources'][str(script)] == ref


@pytest.mark.parametrize('change', ['unstaged', 'staged', 'untracked', 'assume-unchanged'])
def test_dirty_study_preserves_bytes_instead_of_claiming_clean_revision(tmp_path, monkeypatch, change):
    monkeypatch.setattr('hedloom.reproducibility.metadata.distributions', lambda: [])
    repo, script, body = committed_study(tmp_path)
    environment = capture_environment(project_root=tmp_path)
    if change == 'untracked':
        (repo / 'helper.py').write_text('value = 2\n')
    else:
        if change == 'assume-unchanged':
            git(repo, 'update-index', '--assume-unchanged', script.name)
        script.write_text('def work():\n    return 2\n')
        if change == 'staged':
            git(repo, 'add', script.name)
    record = capture(implementations=(body,), environment=environment)
    assert str(script) not in record['sources']
    assert base64.b64decode(record['files'][str(script)]['content']) == script.read_bytes()


def test_no_git_and_explicit_attachments_always_preserve_bytes(tmp_path, monkeypatch):
    monkeypatch.setattr('hedloom.reproducibility.metadata.distributions', lambda: [])
    repo, script, body = committed_study(tmp_path)
    environment = capture_environment(project_root=tmp_path)
    attached = capture(Reproducibility(files=(script,)), (body,), environment=environment)
    assert str(script) in attached['files'] and str(script) not in attached['sources']
    def unavailable(*args):
        raise FileNotFoundError('no git')
    monkeypatch.setattr('hedloom.reproducibility._git', unavailable)
    fallback = capture(implementations=(body,), environment=environment)
    assert str(script) in fallback['files']
    assert not fallback['sources']


def test_reader_accepts_old_record_filename(tmp_path):
    from hedloom.history import HistoryWriter, publish
    writer = HistoryWriter(site_for(tmp_path), 'old', 'empty', {'invocations': []}, {})
    location = tmp_path / 'history' / 'runs' / writer.run_id
    header_path = location / 'run.json'
    header = json.loads(header_path.read_text())
    header.pop('reproducibility', None)
    header['provenance'] = 'provenance.json'
    publish(header_path, header)
    publish(location / 'provenance.json', {'status': 'disabled'})
    assert RunHistory(tmp_path / 'history').reproducibility(writer.run_id)['status'] == 'disabled'
