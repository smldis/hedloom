import os
import threading
from pathlib import Path

import pytest

import hedloom_exec.attempt as attempt_module
import hedloom_exec.journal as journal_module
from hedloom_exec.attempt import StaleIdentity, accept_for_reuse, launch_or_attach, reconcile
from hedloom_exec.identity import attempt_identity
from hedloom_exec.journal import AttemptJournal, ConcurrentClaim, JournalError
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


def test_a_new_record_becomes_visible_with_its_layout_already_in_it(
    tmp_path, monkeypatch
):
    """The record is published in one step, so it is never half-built.

    Creating the directory and then writing `layout` were two visible steps,
    and a caller arriving between them saw a record that existed and declared
    nothing -- which is exactly what a foreign directory looks like. It then
    either refused a record that was merely being built or adopted one that
    was not Hedloom's, decided by a check made before the claim was held. This
    watches the moment the directory becomes visible and asserts the layout is
    already inside it, so there is no such moment to arrive in.
    """

    published = {}
    rename = os.rename

    def watched(source, destination):
        published["staged"] = sorted(item.name for item in Path(source).iterdir())
        return rename(source, destination)

    monkeypatch.setattr(journal_module.os, "rename", watched)
    identity = attempt_identity(plan_id="p", invocation_id="atomic").rendered
    journal = AttemptJournal(tmp_path, identity)

    with journal.claim():
        pass

    assert published["staged"] == ["layout"]
    assert journal.layout_path.read_text(encoding="utf-8").strip() == "1"
    assert not list(tmp_path.glob(".*partial*")), "staging must not be left behind"


def test_concurrent_callers_are_refused_by_name_not_by_a_missing_layout(tmp_path):
    """Every loser meets a claimed record, never one still being built."""

    identity = attempt_identity(plan_id="p", invocation_id="racing").rendered
    entered = []
    refusals = []

    def contend():
        try:
            with AttemptJournal(tmp_path, identity).claim():
                entered.append(1)
        except Exception as error:
            refusals.append(error)

    threads = [threading.Thread(target=contend) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert entered, "at least one caller must reach the record"
    assert all(isinstance(item, ConcurrentClaim) for item in refusals), (
        f"unexpected refusals: {sorted({type(i).__name__ for i in refusals})}"
    )
    assert (tmp_path / identity / "layout").is_file()


def test_a_record_with_a_journal_and_no_layout_is_still_refused(tmp_path):
    """The refusal that matters is kept: a pre-layout-1 root stays unreadable."""

    identity = attempt_identity(plan_id="p", invocation_id="ancient").rendered
    journal = AttemptJournal(tmp_path, identity)
    journal.directory.mkdir(parents=True)
    journal.log_path.write_text('{"seq": 0, "event": "created", "data": {}}\n')

    with pytest.raises(JournalError, match="no recognised layout"):
        with journal.claim():
            pass
