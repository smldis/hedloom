from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os

import pytest

from hedloom_exec.alias import point_alias
from hedloom_exec.identity import attempt_identity, try_name
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.prune import (
    RetentionError, RetentionPolicy, RetentionRule, survey,
)


def _journal(tmp_path, label="point"):
    identity = attempt_identity(plan_id="plan", invocation_id=label).rendered
    return AttemptJournal(tmp_path / "records", identity)


def _terminal(journal, outcome="failed", *, result=None):
    with journal.claim():
        number = journal.begin_try()
        if not any(event.event == "created" for event in journal.fold().events):
            journal.append(
                "created", **{"try": number, "plan": "plan",
                "invocation": "invoke:point", "operation": "work",
                "input_digest": "digest", "authored_key": "point"},
            )
        journal.publish_terminal(
            try_number=number, outcome=outcome, manifest=result or {},
        )
    return number


def _workspace(tmp_path, journal, number, content=b"payload"):
    path = tmp_path / "work" / try_name(journal.identity, number)
    path.mkdir(parents=True)
    (path / "result.bin").write_bytes(content)
    return path


def _policy(*rules, floor="0s"):
    return RetentionPolicy(tuple(rules), floor=floor)


def _failed(**changes):
    values = dict(name="failures", outcome=("failed",), keep_latest=0)
    values.update(changes)
    return RetentionRule(**values)


def test_a_survey_removes_nothing(tmp_path):
    journal = _journal(tmp_path)
    number = _terminal(journal)
    workspace = _workspace(tmp_path, journal, number)
    found = survey(journal.directory.parent, _policy(_failed()),
                   workspace_root=tmp_path / "work")
    assert len(found.candidates) == 1
    assert (workspace / "result.bin").read_bytes() == b"payload"
    assert not any(event.event == "workspace_removed" for event in journal.events())


def test_a_survey_creates_no_directory_it_inspects(tmp_path):
    root = tmp_path / "absent-records"
    work = tmp_path / "absent-work"
    assert survey(root, _policy(_failed()), workspace_root=work).candidates == ()
    assert not root.exists()
    assert not work.exists()


def test_a_survey_reports_the_bytes_it_would_free(tmp_path):
    journal = _journal(tmp_path)
    number = _terminal(journal)
    _workspace(tmp_path, journal, number, b"12345")
    found = survey(journal.directory.parent, _policy(_failed()),
                   workspace_root=tmp_path / "work")
    assert found.freed_bytes == 5
    assert found.as_data()["freed_bytes"] == 5


def test_conditions_within_a_rule_are_anded(tmp_path):
    journal = _journal(tmp_path)
    number = _terminal(journal)
    _workspace(tmp_path, journal, number, b"123")
    rule = _failed(older_than="0s", larger_than="4B")
    found = survey(journal.directory.parent, _policy(rule),
                   workspace_root=tmp_path / "work")
    assert found.candidates == ()
    assert found.skipped[0].reason == "no-rule"


def test_rules_are_ored_with_each_other(tmp_path):
    journal = _journal(tmp_path)
    number = _terminal(journal)
    _workspace(tmp_path, journal, number, b"123")
    too_large = _failed(name="large", larger_than="4B")
    old = _failed(name="old", older_than="0s")
    found = survey(journal.directory.parent, _policy(too_large, old),
                   workspace_root=tmp_path / "work")
    assert found.candidates[0].rule == "old"


def test_the_floor_overrides_every_rule(tmp_path):
    journal = _journal(tmp_path)
    number = _terminal(journal)
    _workspace(tmp_path, journal, number)
    found = survey(journal.directory.parent, _policy(_failed(), floor="1w"),
                   workspace_root=tmp_path / "work")
    assert found.skipped[0].reason == "floor"


def test_unreconciled_is_never_selected(tmp_path):
    journal = _journal(tmp_path)
    number = _terminal(journal, "unreconciled")
    _workspace(tmp_path, journal, number)
    rule = RetentionRule("old", older_than="0s", keep_latest=0)
    found = survey(journal.directory.parent, _policy(rule),
                   workspace_root=tmp_path / "work")
    assert found.skipped[0].reason == "unreconciled"


def test_a_rule_that_would_select_unreconciled_is_refused():
    with pytest.raises(RetentionError, match="never selectable"):
        RetentionRule("wrong", outcome=("unreconciled",))


def test_a_rule_with_no_condition_is_refused():
    with pytest.raises(RetentionError, match="no selection condition"):
        RetentionRule("everything")


def test_an_unknown_policy_key_is_refused():
    with pytest.raises(RetentionError, match="unknown key"):
        RetentionPolicy.from_toml({"rule": [], "mystery": True})


def test_a_malformed_duration_is_refused_not_coerced():
    with pytest.raises(RetentionError, match="malformed duration"):
        RetentionRule("bad", older_than="two weeks")


def test_a_single_outcome_string_is_refused_rather_than_split_into_letters():
    with pytest.raises(RetentionError, match="not the single string 'failed'"):
        RetentionRule("careless", outcome="failed")


def test_an_outcome_that_is_not_a_sequence_is_refused():
    with pytest.raises(RetentionError, match="sequence of outcome names"):
        RetentionRule("careless", outcome=7)


def test_keep_latest_spares_the_newest_tries_of_each_record(tmp_path):
    journal = _journal(tmp_path)
    first = _terminal(journal)
    second = _terminal(journal)
    _workspace(tmp_path, journal, first)
    _workspace(tmp_path, journal, second)
    found = survey(journal.directory.parent, _policy(_failed(keep_latest=1)),
                   workspace_root=tmp_path / "work")
    assert [item.try_number for item in found.candidates] == [first]
    assert any(item.try_number == second and item.reason == "no-rule"
               for item in found.skipped)


def test_keep_logs_spares_stdout_and_stderr(tmp_path):
    journal = _journal(tmp_path)
    number = _terminal(journal)
    workspace = _workspace(tmp_path, journal, number, b"123")
    (workspace / "stdout.log").write_bytes(b"stdout")
    (workspace / "stderr.log").write_bytes(b"stderr")
    found = survey(journal.directory.parent, _policy(_failed(keep_logs=True)),
                   workspace_root=tmp_path / "work")
    assert found.freed_bytes == 3


def test_the_reusable_result_is_never_a_candidate(tmp_path):
    journal = _journal(tmp_path)
    number = _terminal(journal, "succeeded")
    _workspace(tmp_path, journal, number)
    rule = RetentionRule("success", outcome=("succeeded",), keep_latest=0)
    found = survey(journal.directory.parent, _policy(rule),
                   workspace_root=tmp_path / "work")
    assert found.skipped[0].reason == "reusable"


def test_a_non_terminal_try_is_never_a_candidate(tmp_path):
    journal = _journal(tmp_path)
    with journal.claim():
        number = journal.begin_try()
    found = survey(journal.directory.parent, _policy(_failed()),
                   workspace_root=tmp_path / "work")
    assert found.skipped[0].reason == "non-terminal"
    assert found.skipped[0].try_number == number


def test_a_workspace_outside_the_roots_is_refused(tmp_path):
    journal = _journal(tmp_path)
    number = _terminal(journal)
    work = tmp_path / "work"
    work.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    os.symlink(outside, work / try_name(journal.identity, number))
    found = survey(journal.directory.parent, _policy(_failed()), workspace_root=work)
    assert found.skipped[0].reason == "outside-roots"


def test_the_survey_explains_every_skip_with_a_reason(tmp_path):
    journal = _journal(tmp_path)
    number = _terminal(journal)
    _workspace(tmp_path, journal, number)
    found = survey(journal.directory.parent, _policy(_failed(), floor="1w"),
                   workspace_root=tmp_path / "work")
    assert all(item.reason for item in found.skipped)
    assert "skipped" in found.summary()


def test_larger_than_measures_the_workspace_not_the_record(tmp_path):
    journal = _journal(tmp_path)
    number = _terminal(journal)
    _workspace(tmp_path, journal, number, b"12345")
    (journal.directory / "large-record-evidence").write_bytes(b"x" * 100)
    found = survey(
        journal.directory.parent,
        _policy(_failed(larger_than="6B")), workspace_root=tmp_path / "work",
    )
    assert found.candidates == ()


def test_larger_than_walks_directory_outputs(tmp_path):
    journal = _journal(tmp_path)
    result = {"artifacts": [{"name": "tree", "kind": "directory",
                              "address": "ignored", "size": 0}]}
    number = _terminal(journal, result=result)
    workspace = _workspace(tmp_path, journal, number, b"")
    tree = workspace / "tree"
    tree.mkdir()
    (tree / "nested.bin").write_bytes(b"12345")
    found = survey(
        journal.directory.parent,
        _policy(_failed(larger_than="5B")), workspace_root=tmp_path / "work",
    )
    assert found.candidates[0].bytes == 5


def test_larger_than_refuses_an_artifact_whose_kind_is_unknown(tmp_path):
    journal = _journal(tmp_path)
    number = _terminal(journal, result={"artifacts": [{"kind": "mystery"}]})
    _workspace(tmp_path, journal, number)
    with pytest.raises(RetentionError, match="cannot determine"):
        survey(journal.directory.parent, _policy(_failed(larger_than="1B")),
               workspace_root=tmp_path / "work")


def test_older_than_measures_publication_not_file_mtime(tmp_path):
    journal = _journal(tmp_path)
    number = _terminal(journal)
    workspace = _workspace(tmp_path, journal, number)
    old = (datetime.now(timezone.utc) - timedelta(days=30)).timestamp()
    os.utime(workspace / "result.bin", (old, old))
    found = survey(
        journal.directory.parent,
        _policy(_failed(older_than="7d")), workspace_root=tmp_path / "work",
    )
    assert found.candidates == ()


def test_an_aliased_workspace_is_never_a_candidate(tmp_path):
    journal = _journal(tmp_path)
    number = _terminal(journal)
    workspace = _workspace(tmp_path, journal, number)
    point_alias(journal.directory.parent, plan_id="plan", authored_key="point",
                output="result", target=workspace / "result.bin")
    found = survey(journal.directory.parent, _policy(_failed()),
                   workspace_root=tmp_path / "work")
    assert found.skipped[0].reason == "aliased"


def test_a_survey_of_an_empty_root_is_not_an_error(tmp_path):
    found = survey(tmp_path / "empty", _policy(_failed()),
                   workspace_root=tmp_path / "work")
    assert found.candidates == ()
    assert found.skipped == ()
