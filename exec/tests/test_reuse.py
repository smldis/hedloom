"""Reuse must mean "already done with these inputs", or it is a lie."""

import pytest

from hedloom_exec.durability import Durability, execute
from hedloom_exec.identity import attempt_identity
from hedloom_exec.reuse import input_digest, scan_attempts
from hedloom_exec.transport import InProcessTransport

BUNDLE = {
    "operation": "simulate",
    "command": ["awk", "-f", "rule.awk", "tt.in"],
    "inputs": {"model": "sha256:aaa"},
}


def transport(runs=None):
    def simulate(**kwargs):
        if runs is not None:
            runs.append(kwargs)
        return "ok"

    return InProcessTransport({"simulate": simulate})


def test_digest_is_stable_across_key_order():
    first = input_digest({"operation": "a", "inputs": {"x": 1, "y": 2}})
    second = input_digest({"inputs": {"y": 2, "x": 1}, "operation": "a"})
    assert first == second


def test_changed_inputs_change_the_digest():
    changed = dict(BUNDLE, inputs={"model": "sha256:bbb"})
    assert input_digest(BUNDLE) != input_digest(changed)


def test_placement_does_not_participate_in_identity():
    """Changing where work runs must not invalidate what it produced."""

    relocated = dict(BUNDLE, queue="bigmem", walltime="120", cores=16, env={"X": "1"})
    assert input_digest(relocated) == input_digest(BUNDLE)


def test_nominated_environment_does_participate():
    one_toolchain = dict(BUNDLE, identity_env={"TOOL_ROOT": "/toolchain/a"})
    another = dict(BUNDLE, identity_env={"TOOL_ROOT": "/toolchain/b"})
    assert input_digest(one_toolchain) != input_digest(another)
    assert input_digest(one_toolchain) != input_digest(BUNDLE)


def test_unserializable_inputs_are_refused_with_an_explanation():
    with pytest.raises(ValueError, match="JSON-serializable"):
        input_digest({"operation": "a", "inputs": {"handle": object()}})


def test_identity_is_the_declared_computation_and_nothing_else():
    with_inputs = attempt_identity(computation_digest="abc")
    other_inputs = attempt_identity(computation_digest="def")

    assert with_inputs.rendered != other_inputs.rendered
    assert with_inputs.computation_digest == "abc"
    assert attempt_identity(computation_digest="abc") == with_inputs


def test_unchanged_inputs_reuse_the_published_result(tmp_path):
    runs = []
    shared = transport(runs)
    common = {
        "durability": Durability.RECORDED,
        "root": str(tmp_path),


    }

    first = execute(shared, BUNDLE, **common)
    second = execute(shared, BUNDLE, **common)

    assert first.disposition == "claimed"
    assert second.disposition == "completed"
    assert len(runs) == 1


def test_changed_inputs_do_not_reuse_the_old_result(tmp_path):
    """The defect this module exists to remove."""

    runs = []
    shared = transport(runs)
    common = {
        "durability": Durability.RECORDED,
        "root": str(tmp_path),


    }

    execute(shared, BUNDLE, **common)
    changed = dict(BUNDLE, inputs={"model": "sha256:bbb"})
    second = execute(shared, changed, **common)

    assert second.disposition == "claimed"
    assert len(runs) == 2
    assert len(scan_attempts(tmp_path)) == 2


def test_scanning_an_absent_root_is_empty_not_an_error(tmp_path):
    assert scan_attempts(tmp_path / "nothing-here") == ()


def test_recorded_execution_needs_only_a_bundle_and_a_root(tmp_path):
    """No study, no Plan ID, no invocation ID, no authored key."""

    import inspect

    parameters = inspect.signature(execute).parameters
    assert set(parameters) == {
        "transport", "bundle", "durability", "root", "workspace_root"
    }

    result = execute(
        transport(), BUNDLE, durability=Durability.RECORDED, root=str(tmp_path)
    )
    assert result.outcome == "succeeded"
    assert result.record == scan_attempts(tmp_path)[0].identity
    assert result.try_number == 0


def test_recorded_execution_still_requires_a_root(tmp_path):
    with pytest.raises(ValueError, match="requires a root"):
        execute(transport(), BUNDLE, durability=Durability.RECORDED)


def test_ephemeral_execution_selects_no_record_or_try():
    result = execute(transport(), BUNDLE)

    assert result.outcome == "succeeded"
    assert result.record is None and result.try_number is None


def test_a_prior_failure_gets_a_new_try_in_the_same_shared_record(tmp_path):
    """A retry is a numbered try, and the result names which one it selected.

    Failure is not reusable evidence, so the next request for that declaration
    runs again -- as try 1 under the record try 0 already failed in, whoever
    asks. Reuse then selects try 1, and says so.
    """

    def explode(**kwargs):
        raise RuntimeError("no")

    common = {"durability": Durability.RECORDED, "root": str(tmp_path)}
    failed = execute(
        InProcessTransport({"simulate": explode}),
        BUNDLE,
        **common,
    )
    assert failed.outcome == "failed"

    runs: list[str] = []
    retried = execute(
        transport(runs),
        BUNDLE,
        **common,
    )

    assert retried.outcome == "succeeded"
    assert retried.record == failed.record
    assert (failed.try_number, retried.try_number) == (0, 1)
    assert len(scan_attempts(tmp_path)) == 1
    assert len(runs) == 1
    assert sorted(
        int(path.stem) for path in (retried.journal.directory / "manifest").glob("*.json")
    ) == [0, 1]

    # And the succeeded evidence is now what a third requester reuses.
    third = execute(transport(runs), BUNDLE, **common)
    assert third.disposition == "completed"
    assert third.try_number == 1, "reuse selects the try that actually succeeded"
    assert len(runs) == 1
