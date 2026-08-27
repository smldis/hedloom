from hedloom.cli import main
from hedloom_exec.identity import attempt_identity, try_name
from hedloom_exec.journal import AttemptJournal


def _record(tmp_path):
    identity = attempt_identity(plan_id="plan", invocation_id="point").rendered
    root = tmp_path / "records"
    work = tmp_path / "work"
    journal = AttemptJournal(root, identity)
    with journal.claim():
        number = journal.begin_try()
        journal.append(
            "created", **{"try": number, "plan": "plan", "invocation": "point",
                          "operation": "work", "input_digest": "digest",
                          "authored_key": "point"},
        )
        journal.publish_terminal(try_number=number, outcome="failed", manifest={})
    workspace = work / try_name(identity, number)
    workspace.mkdir(parents=True)
    (workspace / "result").write_text("value")
    return journal, root, work


def test_pin_cli_requires_both_explicit_roots(tmp_path, capsys):
    _journal, root, _work = _record(tmp_path)
    status = main(["pin", "--root", str(root), "plan:point",
                   "--reason", "report", "--no-freeze"])
    assert status == 2
    assert "both --root and --workspace-root" in capsys.readouterr().err


def test_pin_cli_resolves_a_human_selector(tmp_path, capsys):
    journal, root, work = _record(tmp_path)
    status = main(["pin", "--root", str(root), "--workspace-root", str(work),
                   "plan:point", "--reason", "report", "--actor", "engineer",
                   "--no-freeze"])
    assert status == 0
    made = journal.fold().pins[0]
    assert made.pin_id in capsys.readouterr().out
    assert made.actor == "engineer"


def test_pins_cli_lists_active_pins(tmp_path, capsys):
    journal, root, work = _record(tmp_path)
    main(["pin", "--root", str(root), "--workspace-root", str(work),
          journal.identity, "--reason", "report", "--no-freeze"])
    capsys.readouterr()
    status = main(["pins", "--root", str(root), "--workspace-root", str(work)])
    assert status == 0
    output = capsys.readouterr().out
    assert journal.fold().pins[0].pin_id in output
    assert "report" in output


def test_unpin_cli_targets_a_pin_id_prefix(tmp_path, capsys):
    journal, root, work = _record(tmp_path)
    main(["pin", "--root", str(root), "--workspace-root", str(work),
          journal.identity, "--reason", "report", "--no-freeze"])
    made = journal.fold().pins[0]
    capsys.readouterr()
    status = main(["unpin", "--root", str(root), "--workspace-root", str(work),
                   made.pin_id[:12], "--reason", "done", "--no-thaw"])
    assert status == 0
    assert not journal.fold().pins[0].is_active
    assert "released" in capsys.readouterr().out


def test_pin_cli_uses_both_roots_from_a_site(tmp_path):
    journal, root, work = _record(tmp_path)
    profile = tmp_path / "site.toml"
    profile.write_text(
        f'[study]\nroot = "{root}"\nworkspace_root = "{work}"\n'
    )
    assert main(["pin", "--site", str(profile), journal.identity,
                 "--reason", "report", "--no-freeze"]) == 0
    assert journal.fold().pins[0].is_active
