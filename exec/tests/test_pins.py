from __future__ import annotations

import pytest

from hedloom_exec.attempt import accept_for_reuse
from hedloom_exec.identity import attempt_identity, try_name
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.pins import (
    PinError, PinSelectionError, is_pinned, pin as make_pin, pins_of,
    resolve_selector, unpin,
)
from hedloom_exec.prune import RetentionPolicy, RetentionRule, survey


def _record(tmp_path, label="point", outcomes=("failed",)):
    identity = attempt_identity(computation_digest=f"plan/{label}").rendered
    journal = AttemptJournal(tmp_path / "records", identity)
    workspaces = []
    for outcome in outcomes:
        with journal.claim():
            number = journal.begin_try()
            if not any(event.event == "created" for event in journal.events()):
                journal.append(
                    "created",
                    **{"try": number, "operation": "work",
                       "input_digest": label},
                )
            journal.publish_terminal(try_number=number, outcome=outcome, manifest={})
        workspace = tmp_path / "work" / try_name(identity, number)
        workspace.mkdir(parents=True)
        (workspace / "result.bin").write_bytes(f"result-{number}".encode())
        workspaces.append(workspace)
    return journal, workspaces


def _policy():
    return RetentionPolicy((RetentionRule(
        "spent", outcome=("failed",), keep_latest=0, keep_logs=False,
    ),), floor="0s")


def test_a_pinned_try_is_never_a_candidate(tmp_path):
    journal, workspaces = _record(tmp_path)
    make_pin(journal, try_number=0, workspace_root=tmp_path / "work",
             reason="report", freeze=False)
    found = survey(journal.directory.parent, _policy(), workspace_root=tmp_path / "work")
    assert found.candidates == ()
    assert found.skipped[0].reason == "pinned"
    assert workspaces[0].exists()


def test_a_pinned_try_is_skipped_while_its_siblings_are_pruned(tmp_path):
    journal, workspaces = _record(tmp_path, outcomes=("failed", "failed"))
    make_pin(journal, try_number=0, workspace_root=tmp_path / "work",
             reason="keep", freeze=False)
    report = survey(journal.directory.parent, _policy(),
                    workspace_root=tmp_path / "work").apply()
    assert workspaces[0].exists()
    assert not workspaces[1].exists()
    assert [item.try_number for item in report.removed] == [1]


def test_a_pin_survives_a_rerun_of_the_same_record(tmp_path):
    journal, _workspaces = _record(tmp_path)
    made = make_pin(journal, try_number=0, workspace_root=tmp_path / "work",
                    reason="keep", freeze=False)
    with journal.claim():
        journal.publish_terminal(try_number=journal.begin_try(), outcome="failed",
                                 manifest={})
    assert pins_of(journal.fold()) == (made,)


def test_a_pin_is_attributable_to_a_reason_and_an_actor(tmp_path):
    journal, _ = _record(tmp_path)
    made = make_pin(journal, try_number=0, workspace_root=tmp_path / "work",
                    reason="reference", actor="engineer", freeze=False)
    assert made.reason == "reference"
    assert made.actor == "engineer"


def test_unpin_targets_one_pin_by_id(tmp_path):
    journal, _ = _record(tmp_path)
    made = make_pin(journal, try_number=0, workspace_root=tmp_path / "work",
                    reason="keep", freeze=False)
    released = unpin(journal, pin_id=made.pin_id, reason="done", actor="engineer")
    assert not released.is_active
    assert released.released_reason == "done"
    assert released.released_by == "engineer"


def test_unpin_appends_rather_than_erasing(tmp_path):
    journal, _ = _record(tmp_path)
    made = make_pin(journal, try_number=0, workspace_root=tmp_path / "work",
                    reason="keep", freeze=False)
    unpin(journal, pin_id=made.pin_id, reason="done", thaw=False)
    assert [event.event for event in journal.events()].count("pinned") == 1
    assert [event.event for event in journal.events()].count("unpinned") == 1


def test_the_record_shows_a_pin_that_was_later_released(tmp_path):
    journal, _ = _record(tmp_path)
    made = make_pin(journal, try_number=0, workspace_root=tmp_path / "work",
                    reason="keep", freeze=False)
    unpin(journal, pin_id=made.pin_id, reason="done", thaw=False)
    assert len(pins_of(journal.fold(), active_only=False)) == 1
    assert pins_of(journal.fold()) == ()


def test_two_pins_on_one_try_release_independently(tmp_path):
    journal, _ = _record(tmp_path)
    first = make_pin(journal, try_number=0, workspace_root=tmp_path / "work",
                     reason="first", freeze=False)
    unpin(journal, pin_id=first.pin_id, reason="replace", thaw=False)
    second = make_pin(journal, try_number=0, workspace_root=tmp_path / "work",
                      reason="second", freeze=False)
    unpin(journal, pin_id=second.pin_id, reason="done", thaw=False)
    assert {item.pin_id for item in pins_of(journal.fold(), active_only=False)} == {
        first.pin_id, second.pin_id
    }


def test_pinning_a_non_terminal_try_is_refused(tmp_path):
    identity = attempt_identity(computation_digest="plan/point").rendered
    journal = AttemptJournal(tmp_path / "records", identity)
    with journal.claim():
        number = journal.begin_try()
    with pytest.raises(PinError, match="not terminal"):
        make_pin(journal, try_number=number, workspace_root=tmp_path / "work",
                 reason="too early")


def test_pinning_an_already_pinned_try_is_refused(tmp_path):
    journal, _ = _record(tmp_path)
    make_pin(journal, try_number=0, workspace_root=tmp_path / "work",
             reason="first", freeze=False)
    with pytest.raises(PinError, match="already pinned"):
        make_pin(journal, try_number=0, workspace_root=tmp_path / "work",
                 reason="second", freeze=False)


def test_an_ambiguous_identity_prefix_is_refused_with_candidates(tmp_path):
    first, _ = _record(tmp_path, "one")
    second, _ = _record(tmp_path, "two")
    prefix = "hedloom-"
    with pytest.raises(PinSelectionError) as caught:
        resolve_selector(first.directory.parent, prefix)
    assert first.identity in str(caught.value)
    assert second.identity in str(caught.value)


def test_a_name_shaped_selector_is_refused(tmp_path):
    """There is no `<study>:<key>` address: a record belongs to no study."""

    _record(tmp_path, "point")
    with pytest.raises(PinSelectionError, match="no record matches"):
        resolve_selector(tmp_path / "records", "plan:point")


def test_a_selector_that_matches_no_record_is_refused(tmp_path):
    with pytest.raises(PinSelectionError, match="no record matches"):
        resolve_selector(tmp_path, "hedloom-000000000000000000")


def test_an_empty_selector_is_refused(tmp_path):
    with pytest.raises(PinSelectionError, match="record identity or prefix"):
        resolve_selector(tmp_path, "#0")


def test_accept_for_reuse_does_not_create_a_pin(tmp_path):
    journal, _ = _record(tmp_path)
    accept_for_reuse(journal, reason="known failure")
    assert journal.fold().pins == ()


def test_an_accepted_failure_is_skipped_as_reusable_not_as_pinned(tmp_path):
    journal, _ = _record(tmp_path)
    accept_for_reuse(journal, reason="known failure")
    found = survey(journal.directory.parent, _policy(), workspace_root=tmp_path / "work")
    assert found.skipped[0].reason == "reusable"


def test_an_accepted_failure_becomes_prunable_once_a_later_try_stands(tmp_path):
    journal, workspaces = _record(tmp_path)
    accept_for_reuse(journal, reason="known failure")
    with journal.claim():
        number = journal.begin_try()
        journal.publish_terminal(try_number=number, outcome="succeeded", manifest={})
    current = tmp_path / "work" / try_name(journal.identity, number)
    current.mkdir()
    found = survey(journal.directory.parent, _policy(), workspace_root=tmp_path / "work")
    assert [item.workspace for item in found.candidates] == [workspaces[0]]


def test_a_pin_is_never_written_into_the_workspace(tmp_path):
    journal, workspaces = _record(tmp_path)
    before = {item.name for item in workspaces[0].iterdir()}
    make_pin(journal, try_number=0, workspace_root=tmp_path / "work",
             reason="keep", freeze=False)
    assert {item.name for item in workspaces[0].iterdir()} == before


def test_pruning_a_workspace_cannot_delete_its_own_pin(tmp_path):
    journal, _ = _record(tmp_path)
    made = make_pin(journal, try_number=0, workspace_root=tmp_path / "work",
                    reason="keep", freeze=False)
    survey(journal.directory.parent, _policy(), workspace_root=tmp_path / "work").apply()
    assert pins_of(journal.fold()) == (made,)
    assert journal.directory.exists()


def test_a_try_selector_names_exactly_one_try(tmp_path):
    journal, _ = _record(tmp_path, outcomes=("failed", "failed"))
    _record_value, tries = resolve_selector(
        journal.directory.parent, f"{journal.identity}#1"
    )
    assert [item.number for item in tries] == [1]


def test_a_removal_recorded_before_a_crash_cannot_later_be_pinned(tmp_path):
    journal, _ = _record(tmp_path)
    with journal.claim():
        journal.append("workspace_removed", **{"try": 0, "workspace": "pending"})
    with pytest.raises(PinError, match="recorded workspace removal"):
        make_pin(journal, try_number=0, workspace_root=tmp_path / "work",
                 reason="too late", freeze=False)
