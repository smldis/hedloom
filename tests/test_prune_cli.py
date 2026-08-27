import json

from hedloom.cli import main
from hedloom_exec.identity import attempt_identity, try_name
from hedloom_exec.journal import AttemptJournal


def _site_and_record(tmp_path):
    root = tmp_path / "records"
    work = tmp_path / "work"
    identity = attempt_identity(plan_id="plan", invocation_id="point").rendered
    journal = AttemptJournal(root, identity)
    with journal.claim():
        number = journal.begin_try()
        journal.append(
            "created", **{"try": number, "plan": "plan", "invocation": "point-id",
                          "operation": "work", "input_digest": "digest",
                          "authored_key": "point"},
        )
        journal.publish_terminal(try_number=number, outcome="failed", manifest={})
    workspace = work / try_name(identity, number)
    workspace.mkdir(parents=True)
    (workspace / "result").write_bytes(b"12345")
    profile = tmp_path / "site.toml"
    profile.write_text(
        f"""
[study]
root = "{root}"
workspace_root = "{work}"

[retention]
floor = "0s"

[[retention.rule]]
name = "failures"
outcome = ["failed"]
keep_latest = 0
keep_logs = false
"""
    )
    return profile, journal, workspace


def test_prune_is_a_dry_run_without_apply(tmp_path, capsys):
    profile, _journal, workspace = _site_and_record(tmp_path)
    assert main(["prune", "--site", str(profile)]) == 0
    assert workspace.exists()
    assert "1 candidate" in capsys.readouterr().out


def test_prune_apply_removes_the_surveyed_workspace(tmp_path):
    profile, journal, workspace = _site_and_record(tmp_path)
    assert main(["prune", "--site", str(profile), "--apply"]) == 0
    assert not workspace.exists()
    assert any(event.event == "workspace_removed" for event in journal.events())


def test_prune_json_is_machine_readable(tmp_path, capsys):
    profile, journal, _workspace = _site_and_record(tmp_path)
    assert main(["prune", "--site", str(profile), "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert data["candidates"][0]["identity"] == journal.identity


def test_cli_selection_overrides_site_rules(tmp_path, capsys):
    profile, _journal, workspace = _site_and_record(tmp_path)
    assert main(["prune", "--site", str(profile),
                 "--outcome", "succeeded"]) == 0
    assert workspace.exists()
    assert "0 candidate" in capsys.readouterr().out


def test_cli_can_restrict_by_plan_and_authored_invocation(tmp_path, capsys):
    profile, _journal, workspace = _site_and_record(tmp_path)
    assert main(["prune", "--site", str(profile), "--plan", "other"]) == 0
    assert "0 candidate" in capsys.readouterr().out
    assert main(["prune", "--site", str(profile), "--invocation", "point"]) == 0
    assert "1 candidate" in capsys.readouterr().out
    assert workspace.exists()
