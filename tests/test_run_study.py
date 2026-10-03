"""One-shot execution through the public facade, including owner cleanup."""
import importlib

import pytest

from hedloom import RunFailed, RunHistory, operation, returned, run_study, study
from test_async_runtime import DISABLED, simple, site


@operation(outputs={'value': returned()})
def fail_one_shot():
    raise ValueError('one-shot computation failed')


@study
def failing_one_shot():
    return fail_one_shot.named('work')()


@pytest.fixture
def owners(monkeypatch):
    module = importlib.import_module('hedloom.runtime')
    original = module.runtime
    opened = []

    def tracked(*args, **kwargs):
        owner = original(*args, **kwargs)
        opened.append(owner)
        return owner

    monkeypatch.setattr(module, 'runtime', tracked)
    return opened


def assert_closed(owners):
    assert owners
    for owner in owners:
        assert owner._closed.result(timeout=0) is None
        assert not owner._thread.is_alive()


def test_public_one_shot_returns_saved_outputs_and_reuses_across_owners(tmp_path, owners):
    configured = site(tmp_path)
    first = run_study(simple(7), site=configured, name='first', priority=3,
                      override={'kernel': {'threads': 1}}, reproducibility=DISABLED)
    assert first.succeeded and first.outputs['output'].value == 7
    assert_closed(owners)
    second = run_study(simple(7), site=configured, name='again', reproducibility=DISABLED)
    assert second.succeeded and second['work'].disposition == 'reused'
    assert first.run_id != second.run_id
    assert RunHistory(configured.runs_dir).read_run(second.run_id).run_reported_outcome == 'succeeded'
    assert len(owners) == 2 and owners[0] is not owners[1]
    assert_closed(owners)


@pytest.mark.parametrize('require_success', [False, True])
def test_failed_result_is_inspectable_after_cleanup(tmp_path, owners, require_success):
    options = dict(site=site(tmp_path), name='failure', require_success=require_success,
                   reproducibility=DISABLED, stop_on_failure=False)
    if require_success:
        with pytest.raises(RunFailed) as error:
            run_study(failing_one_shot(), **options)
        result = error.value.result
    else:
        result = run_study(failing_one_shot(), **options)
    assert result.state == 'FAILED' and not result.succeeded
    assert 'one-shot computation failed' in result['work'].error
    assert result.history.status == 'complete'
    assert_closed(owners)


def test_invalid_submission_closes_the_one_shot_owner(tmp_path, owners):
    with pytest.raises(TypeError, match='authored Study'):
        run_study(object(), site=site(tmp_path), name='invalid', reproducibility=DISABLED)
    assert_closed(owners)


def test_startup_failure_closes_the_one_shot_owner(tmp_path, owners, monkeypatch):
    import hedloom_run.cluster as cluster

    async def refused(*args, **kwargs):
        raise OSError('one-shot startup failed')

    monkeypatch.setattr(cluster, 'async_cluster_for', refused)
    with pytest.raises(OSError, match='one-shot startup failed'):
        run_study(simple(), site=site(tmp_path), name='startup', reproducibility=DISABLED)
    assert_closed(owners)
