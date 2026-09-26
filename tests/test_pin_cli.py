from hedloom.cli import main
from hedloom_exec.identity import attempt_identity, try_name
from hedloom_exec.journal import AttemptJournal
import pytest


def _record(tmp_path):
    identity = attempt_identity(computation_digest="plan/point").rendered
    root = tmp_path / "records"
    work = tmp_path / "work"
    journal = AttemptJournal(root, identity)
    with journal.claim():
        number = journal.begin_try()
        journal.append(
            "created",
            **{"try": number, "operation": "work", "input_digest": "digest"},
        )
        journal.publish_terminal(try_number=number, outcome="failed", manifest={})
    workspace = work / try_name(identity, number)
    workspace.mkdir(parents=True)
    (workspace / "result").write_text("value")
    return journal, root, work


def test_pin_cli_requires_both_explicit_roots(tmp_path, capsys):
    _journal, root, _work = _record(tmp_path)
    status = main(["pin", "--records-dir", str(root), "plan:point",
                   "--reason", "report", "--no-freeze"])
    assert status == 2
    assert "both --records-dir and --work-dir" in capsys.readouterr().err


@pytest.mark.parametrize("old,new", [
    ("--root", "--records-dir"),
    ("--workspace-root", "--work-dir"),
    ("--history-root", "--runs-dir"),
])
def test_old_storage_flags_refuse_with_replacement(old, new, capsys):
    with pytest.raises(SystemExit) as error:
        main(["pin", f"{old}=stale"])
    assert error.value.code == 2
    assert f"{old} was renamed to {new}" in capsys.readouterr().err


def test_pin_cli_resolves_a_record_prefix(tmp_path, capsys):
    journal, root, work = _record(tmp_path)
    status = main(["pin", "--records-dir", str(root), "--work-dir", str(work),
                   journal.identity[:16], "--reason", "report",
                   "--actor", "engineer", "--no-freeze"])
    assert status == 0
    made = journal.fold().pins[0]
    assert made.pin_id in capsys.readouterr().out
    assert made.actor == "engineer"


def test_pins_cli_lists_active_pins(tmp_path, capsys):
    journal, root, work = _record(tmp_path)
    main(["pin", "--records-dir", str(root), "--work-dir", str(work),
          journal.identity, "--reason", "report", "--no-freeze"])
    capsys.readouterr()
    status = main(["pins", "--records-dir", str(root), "--work-dir", str(work)])
    assert status == 0
    output = capsys.readouterr().out
    assert journal.fold().pins[0].pin_id in output
    assert "report" in output


def test_unpin_cli_targets_a_pin_id_prefix(tmp_path, capsys):
    journal, root, work = _record(tmp_path)
    main(["pin", "--records-dir", str(root), "--work-dir", str(work),
          journal.identity, "--reason", "report", "--no-freeze"])
    made = journal.fold().pins[0]
    capsys.readouterr()
    status = main(["unpin", "--records-dir", str(root), "--work-dir", str(work),
                   made.pin_id[:12], "--reason", "done", "--no-thaw"])
    assert status == 0
    assert not journal.fold().pins[0].is_active
    assert "released" in capsys.readouterr().out


def test_pin_cli_uses_both_roots_from_a_site(tmp_path):
    journal, root, work = _record(tmp_path)
    profile = tmp_path / "site.toml"
    profile.write_text(
        f'[study]\nrecords_dir = "{root}"\nwork_dir = "{work}"\n'
    )
    assert main(["pin", "--site", str(profile), journal.identity,
                 "--reason", "report", "--no-freeze"]) == 0
    assert journal.fold().pins[0].is_active


def test_pin_cli_refuses_a_name_shaped_selector(tmp_path, capsys):
    """A record belongs to no study, so `<study>:<key>` addresses nothing."""

    _journal, root, work = _record(tmp_path)
    status = main(["pin", "--records-dir", str(root), "--work-dir", str(work),
                   "plan:point", "--reason", "report", "--no-freeze"])
    assert status == 2
    assert "no record matches" in capsys.readouterr().err
