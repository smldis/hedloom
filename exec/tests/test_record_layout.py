from hedloom_exec.identity import attempt_identity
from hedloom_exec.journal import AttemptJournal, JournalError, LAYOUT_VERSION
from hedloom_exec.pins import pin as make_pin, verify

import pytest


def test_a_pin_records_the_layout_it_was_made_under(tmp_path):
    identity = attempt_identity(plan_id="plan", invocation_id="point").rendered
    journal = AttemptJournal(tmp_path / "records", identity)
    with journal.claim():
        number = journal.begin_try()
        journal.publish_terminal(try_number=number, outcome="failed", manifest={})
    workspace = tmp_path / "work" / f"{identity}-{number}"
    workspace.mkdir(parents=True)
    made = make_pin(journal, try_number=number, workspace_root=tmp_path / "work",
                    reason="keep", freeze=False)
    assert made.layout == LAYOUT_VERSION


def test_verify_reports_layout_changed_rather_than_drift(tmp_path):
    identity = attempt_identity(plan_id="plan", invocation_id="point").rendered
    journal = AttemptJournal(tmp_path / "records", identity)
    with journal.claim():
        number = journal.begin_try()
        journal.publish_terminal(try_number=number, outcome="failed", manifest={})
    workspace = tmp_path / "work" / f"{identity}-{number}"
    workspace.mkdir(parents=True)
    made = make_pin(journal, try_number=number, workspace_root=tmp_path / "work",
                    reason="keep", freeze=False)
    (workspace / "drift").write_text("changed")
    checked = verify(made, layout=LAYOUT_VERSION + 1)
    assert checked.outcome == "layout-changed"
    assert checked.drifted == ()


def test_a_record_with_no_layout_file_is_treated_as_foreign(tmp_path):
    directory = tmp_path / "records" / ("hedloom-" + "0" * 20)
    directory.mkdir(parents=True)
    (directory / "events.jsonl").write_text("")
    with pytest.raises(JournalError, match="no recognised layout"):
        AttemptJournal(directory.parent, directory.name).fold()
