from __future__ import annotations

import pytest

from hedloom_exec.durability import Durability, execute
from hedloom_exec.identity import attempt_identity, try_name
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.prune import RetentionPolicy, RetentionRule, survey
from hedloom_exec.transport import Observation


def _record(tmp_path, label="point", *, content=b"payload", outcome="failed"):
    identity = attempt_identity(computation_digest=f"plan/{label}").rendered
    journal = AttemptJournal(tmp_path / "records", identity)
    with journal.claim():
        number = journal.begin_try()
        journal.append(
            "created",
            **{"try": number, "operation": "work", "input_digest": label},
        )
        journal.publish_terminal(try_number=number, outcome=outcome, manifest={})
    workspace = tmp_path / "work" / try_name(identity, number)
    workspace.mkdir(parents=True)
    (workspace / "result.bin").write_bytes(content)
    return journal, number, workspace


def _policy(*, keep_logs=False):
    return RetentionPolicy((RetentionRule(
        "spent", outcome=("failed",), keep_latest=0, keep_logs=keep_logs,
    ),), floor="0s")


def _survey(tmp_path, policy=None):
    return survey(tmp_path / "records", policy or _policy(),
                  workspace_root=tmp_path / "work")


def test_apply_removes_exactly_what_the_survey_named(tmp_path):
    _journal, _number, workspace = _record(tmp_path)
    untouched = tmp_path / "work" / "ordinary-directory"
    untouched.mkdir()
    report = _survey(tmp_path).apply()
    assert [item.workspace for item in report.removed] == [workspace]
    assert not workspace.exists()
    assert untouched.exists()


def test_apply_records_workspace_removed_in_the_record(tmp_path):
    journal, number, _workspace = _record(tmp_path)
    _survey(tmp_path).apply(actor="operator")
    event = next(item for item in journal.events() if item.event == "workspace_removed")
    assert event.data["try"] == number
    assert event.data["actor"] == "operator"
    assert event.data["rule"] == "spent"


def test_apply_leaves_the_record_directory_intact(tmp_path):
    journal, _number, _workspace = _record(tmp_path)
    _survey(tmp_path).apply()
    assert journal.directory.is_dir()
    assert journal.log_path.is_file()
    assert journal.layout_path.is_file()


def test_apply_rechecks_preconditions_under_the_claim(tmp_path):
    """A protection acquired after the survey still stops the removal."""

    from hedloom_exec.pins import pin

    journal, number, workspace = _record(tmp_path)
    proposal = _survey(tmp_path)
    pin(journal, try_number=number, workspace_root=tmp_path / "work",
        reason="wanted after all", actor="tester", freeze=False)
    report = proposal.apply()
    assert report.removed == ()
    assert report.skipped[0].try_number == number
    assert report.skipped[0].reason == "pinned"
    assert workspace.exists()


def test_apply_skips_a_contended_record_rather_than_waiting(tmp_path):
    journal, _number, workspace = _record(tmp_path)
    proposal = _survey(tmp_path)
    with journal.claim():
        report = proposal.apply()
    assert report.skipped[0].reason == "contended"
    assert workspace.exists()


def test_the_removal_event_is_written_before_the_bytes_go(tmp_path, monkeypatch):
    journal, _number, workspace = _record(tmp_path)
    import hedloom_exec.prune as prune_module

    original = prune_module._remove_workspace

    def observed(path, *, keep_logs):
        assert path == workspace
        assert any(item.event == "workspace_removed" for item in journal.events())
        original(path, keep_logs=keep_logs)

    monkeypatch.setattr(prune_module, "_remove_workspace", observed)
    _survey(tmp_path).apply()


def test_a_crash_after_the_event_and_before_the_unlink_self_heals(
    tmp_path, monkeypatch
):
    journal, _number, workspace = _record(tmp_path)
    proposal = _survey(tmp_path)
    import hedloom_exec.prune as prune_module

    original = prune_module._remove_workspace

    def crash(_path, *, keep_logs):
        raise RuntimeError("injected crash")

    monkeypatch.setattr(prune_module, "_remove_workspace", crash)
    with pytest.raises(RuntimeError, match="injected"):
        proposal.apply()
    assert workspace.exists()
    assert len([e for e in journal.events() if e.event == "workspace_removed"]) == 1
    monkeypatch.setattr(prune_module, "_remove_workspace", original)
    proposal.apply()
    assert not workspace.exists()
    assert len([e for e in journal.events() if e.event == "workspace_removed"]) == 1


def test_apply_stops_at_the_byte_limit(tmp_path):
    _record(tmp_path, "one", content=b"123")
    _record(tmp_path, "two", content=b"456")
    report = _survey(tmp_path).apply(limit_bytes=3)
    assert len(report.removed) == 1
    assert report.freed_bytes == 3
    assert report.stopped_at_limit


def test_pruning_a_try_does_not_change_the_next_try_number(tmp_path):
    journal, number, _workspace = _record(tmp_path)
    _survey(tmp_path).apply()
    with journal.claim():
        next_number = journal.begin_try()
    assert next_number == number + 1


def test_pruning_a_try_does_not_change_what_reuse_returns(tmp_path):
    journal, _first, old_workspace = _record(tmp_path)
    with journal.claim():
        second = journal.begin_try()
        journal.publish_terminal(try_number=second, outcome="succeeded", manifest={})
    current_workspace = tmp_path / "work" / try_name(journal.identity, second)
    current_workspace.mkdir()
    (current_workspace / "result.bin").write_bytes(b"current")
    _survey(tmp_path).apply()
    assert not old_workspace.exists()
    assert journal.read_manifest()["try"] == second
    assert current_workspace.exists()


def test_keep_logs_preserves_diagnostics_while_removing_payload(tmp_path):
    _journal, _number, workspace = _record(tmp_path)
    (workspace / "stdout.log").write_text("out")
    (workspace / "stderr.log").write_text("err")
    report = _survey(tmp_path, _policy(keep_logs=True)).apply()
    assert report.freed_bytes == len(b"payload")
    assert sorted(item.name for item in workspace.iterdir()) == [
        "stderr.log", "stdout.log"
    ]


def test_a_run_after_pruning_behaves_as_if_nothing_was_pruned(tmp_path):
    class FailedWork:
        name = "failed-work"
        discovery_is_authoritative = True

        def __init__(self):
            self.results = {}

        def submit(self, name, bundle):
            workdir = bundle["workdir"]
            from pathlib import Path

            Path(workdir, "evidence").write_text(name)
            self.results[name] = Observation("failed", {"expected": True})
            return {"identity": name, "workdir": workdir}

        def discover(self, name):
            return self.results.get(name)

        def poll(self, handle):
            return self.results[handle["identity"]]

        def cancel(self, handle):
            return None

    transport = FailedWork()
    options = {
        "durability": Durability.RECORDED,
        "root": str(tmp_path / "records"),
        "workspace_root": str(tmp_path / "work"),


    }
    first = execute(transport, {"operation": "work"}, **options)
    survey(tmp_path / "records", _policy(), workspace_root=tmp_path / "work").apply()
    second = execute(transport, {"operation": "work"}, **options)
    assert first.outcome == second.outcome == "failed"
    assert [item.number for item in second.journal.fold().tries] == [0, 1]
    assert (tmp_path / "work" / try_name(second.journal.identity, 1)).is_dir()
