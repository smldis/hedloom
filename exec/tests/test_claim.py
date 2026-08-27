import pytest

import hedloom_exec.attempt as attempt_module
from hedloom_exec.attempt import StaleIdentity, accept_for_reuse, launch_or_attach, reconcile
from hedloom_exec.identity import attempt_identity
from hedloom_exec.journal import AttemptJournal, ConcurrentClaim
from hedloom_exec.transport import InProcessTransport


IDENTITY = attempt_identity(plan_id="claim", invocation_id="one").rendered


def test_the_claim_covers_every_try_at_one_input_set(tmp_path):
    first = AttemptJournal(tmp_path, IDENTITY)
    second = AttemptJournal(tmp_path, IDENTITY)
    with first.claim():
        first.begin_try()
        with pytest.raises(ConcurrentClaim):
            with second.claim():
                second.begin_try()


def test_a_second_controller_cannot_start_a_try_while_one_is_claimed(tmp_path):
    first = AttemptJournal(tmp_path, IDENTITY)
    with first.claim():
        with pytest.raises(ConcurrentClaim):
            launch_or_attach(
                AttemptJournal(tmp_path, IDENTITY),
                InProcessTransport({"work": lambda: 1}),
                {"operation": "work"},
            )


def test_a_crash_between_tries_leaves_the_record_readable(tmp_path):
    journal = AttemptJournal(tmp_path, IDENTITY)
    with journal.claim():
        zero = journal.begin_try()
        journal.publish_terminal(try_number=zero, outcome="failed", manifest={})
    state = AttemptJournal(tmp_path, IDENTITY).fold()
    assert state.current_try == 0
    assert state.outcome == "failed"


def test_stale_identity_still_refuses_a_record_created_from_other_inputs(tmp_path):
    transport = InProcessTransport({"work": lambda: 1})
    journal = AttemptJournal(tmp_path, IDENTITY)
    launch_or_attach(journal, transport, {"operation": "work", "inputs": {"a": 1}})
    with pytest.raises(StaleIdentity):
        launch_or_attach(
            AttemptJournal(tmp_path, IDENTITY),
            transport,
            {"operation": "work", "inputs": {"a": 2}},
        )


def test_accept_for_reuse_holds_the_claim(tmp_path, monkeypatch):
    transport = InProcessTransport({"work": lambda: (_ for _ in ()).throw(ValueError("x"))})
    journal = AttemptJournal(tmp_path, IDENTITY)
    launch_or_attach(journal, transport, {"operation": "work"})
    reconcile(journal, transport)
    original = journal.append

    def checked(event, **data):
        assert journal._claim_held
        return original(event, **data)

    monkeypatch.setattr(journal, "append", checked)
    accept_for_reuse(journal, reason="inspected")


def test_an_unwritable_workspace_does_not_lose_terminal_publication(tmp_path, monkeypatch):
    transport = InProcessTransport({"work": lambda: 7})
    journal = AttemptJournal(tmp_path, IDENTITY)
    launch_or_attach(journal, transport, {"operation": "work"})

    def refuse(*_args, **_kwargs):
        raise PermissionError("workspace is read-only")

    monkeypatch.setattr(attempt_module, "write_diagnostics", refuse)
    state = reconcile(journal, transport)
    assert state.outcome == "succeeded"
    assert journal.read_manifest()["result"]["diagnostics_error"].startswith(
        "PermissionError:"
    )
