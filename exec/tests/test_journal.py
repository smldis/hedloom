import json

import pytest

from hedloom_exec.identity import attempt_identity
from hedloom_exec.journal import AttemptJournal, JournalError, LAYOUT_VERSION


IDENTITY = attempt_identity(plan_id="journal", invocation_id="test").rendered


def journal(tmp_path):
    return AttemptJournal(tmp_path, IDENTITY)


def test_fresh_record_folds_to_unsubmitted(tmp_path):
    state = journal(tmp_path).fold()
    assert state.phase == "unsubmitted"
    assert state.current_try is None
    assert state.events == ()


def test_a_new_record_writes_layout_one(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        assert log.layout_path.read_text() == f"{LAYOUT_VERSION}\n"


def test_a_recognised_record_reads(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        log.begin_try()
    assert log.fold().current_try == 0


def test_a_preexisting_empty_directory_without_a_layout_is_foreign(tmp_path):
    (tmp_path / IDENTITY).mkdir()
    with pytest.raises(JournalError, match="layout"):
        with journal(tmp_path).claim():
            pass


@pytest.mark.parametrize("layout", [None, "2\n", "not-an-integer\n"])
def test_a_missing_or_unknown_layout_is_refused_without_legacy_fallback(tmp_path, layout):
    directory = tmp_path / IDENTITY
    directory.mkdir()
    (directory / "events.jsonl").write_text("{}\n")
    if layout is not None:
        (directory / "layout").write_text(layout)
    with pytest.raises(JournalError, match="layout"):
        journal(tmp_path).fold()


def test_events_are_append_only_and_sequenced_across_tries(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        zero = log.begin_try()
        log.append("created", **{"try": zero, "operation": "simulate"})
        log.append("submit_intent", **{"try": zero, "transport": "fake"})
        log.publish_terminal(try_number=zero, outcome="failed", manifest={})
        one = log.begin_try()
        log.append("submit_intent", **{"try": one, "transport": "fake"})
    assert [event.seq for event in log.events()] == list(range(len(log.events())))
    assert [item.number for item in log.fold().tries] == [0, 1]


def test_fold_reports_the_latest_trys_phase_and_outcome(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        zero = log.begin_try()
        log.publish_terminal(try_number=zero, outcome="failed", manifest={})
        one = log.begin_try()
        log.append("submit_intent", **{"try": one, "transport": "fake"})
    state = log.fold()
    assert state.current_try == 1
    assert state.phase == "intended"
    assert state.outcome is None
    assert state.tries[0].outcome == "failed"


def test_fold_attributes_every_event_to_its_try(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        zero = log.begin_try()
        log.append("observed", **{"try": zero, "state": "running"})
        log.publish_terminal(try_number=zero, outcome="failed", manifest={})
        one = log.begin_try()
        log.append("observed", **{"try": one, "state": "pending"})
    assert log.fold().tries[0].observations == ({"state": "running"},)
    assert log.fold().tries[1].observations == ({"state": "pending"},)


def test_fold_of_a_single_try_record_projects_current_state(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        number = log.begin_try()
        log.append("submit_intent", **{"try": number, "transport": "fake"})
        log.append("submit_receipt", **{"try": number, "handle": {"job_id": "42"}})
    state = log.fold()
    assert state.phase == "submitted"
    assert state.handle == {"job_id": "42"}


def test_a_new_trys_intent_resets_the_phase(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        zero = log.begin_try()
        log.publish_terminal(try_number=zero, outcome="failed", manifest={})
        one = log.begin_try()
        log.append("submit_intent", **{"try": one, "transport": "fake"})
    assert log.fold().phase == "intended"


def test_manifest_becomes_visible_before_the_terminal_record(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        number = log.begin_try()
        log.publish_terminal(try_number=number, outcome="succeeded", manifest={"value": 3})
    document = json.loads(log.manifest_path(0).read_text())
    assert document["outcome"] == "succeeded"
    assert document["result"] == {"value": 3}
    assert log.read_manifest() == document
    assert log.events()[-1].event == "terminal"


def test_publication_leaves_no_partial_file(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        number = log.begin_try()
        log.publish_terminal(try_number=number, outcome="failed", manifest={"error": "boom"})
    assert not tuple(log.directory.rglob("*.partial"))


def test_unknown_events_and_outcomes_are_refused(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        number = log.begin_try()
        with pytest.raises(JournalError):
            log.append("invented_event", **{"try": number})
        with pytest.raises(JournalError):
            log.publish_terminal(try_number=number, outcome="mostly_fine", manifest={})


def test_malformed_journal_lines_are_reported_not_ignored(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        log.begin_try()
    with open(log.log_path, "a", encoding="utf-8") as handle:
        handle.write("{not json\n")
    with pytest.raises(JournalError):
        log.fold()
