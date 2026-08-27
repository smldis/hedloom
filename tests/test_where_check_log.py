"""The operator surface resolves, checks, and explains recorded iterations."""

from pathlib import Path

from hedloom import Site, local, operation, parameter, returned, study
from hedloom.cli import main
from hedloom_exec.alias import alias_path
from hedloom_exec.durability import Durability, execute
from hedloom_exec.transport import Observation


class FileTransport:
    name = "files"
    discovery_is_authoritative = True

    def __init__(self):
        self.handles = {}

    def submit(self, identity, bundle):
        workdir = Path(bundle["workdir"])
        (workdir / "result.txt").write_text("result", encoding="utf-8")
        handle = {"identity": identity, "workdir": str(workdir)}
        self.handles[identity] = handle
        return handle

    def discover(self, identity):
        return self.handles.get(identity)

    def poll(self, _handle):
        return Observation("succeeded", {})

    def cancel(self, _handle):
        return None


def run(root, work, value="one"):
    return execute(
        FileTransport(),
        {
            "operation": "write",
            "inputs": {"source": value},
            "outputs": {"result": {"path": "result.txt"}},
        },
        durability=Durability.RECORDED,
        root=str(root),
        workspace_root=str(work),
        plan_id="study",
        invocation_id="invoke:key:point",
        authored_key="point:write",
    )


def site_file(tmp_path, root):
    profile = tmp_path / "site.toml"
    profile.write_text(f'[study]\nroot = "{root}"\n', encoding="utf-8")
    return profile


def test_where_resolves_a_selector_to_an_output_path(tmp_path, capsys):
    root = tmp_path / "attempts"
    result = run(root, tmp_path / "work")

    status = main(
        [
            "where",
            "--site",
            str(site_file(tmp_path, root)),
            "study:point:write",
            "--output",
            "result",
        ]
    )

    assert status == 0
    assert Path(capsys.readouterr().out.strip()) == (
        tmp_path / "work" / result.journal.identity / "result.txt"
    )


def test_where_refuses_a_selector_that_matches_nothing(tmp_path, capsys):
    status = main(
        ["where", "--root", str(tmp_path), "study:missing", "--output", "result"]
    )

    assert status == 2
    assert "no attempt matches" in capsys.readouterr().err


def test_check_exits_zero_for_a_current_path(tmp_path, capsys):
    root = tmp_path / "attempts"
    current = run(root, tmp_path / "work")
    path = tmp_path / "work" / current.journal.identity / "result.txt"

    assert main(["check", "--root", str(root), str(path)]) == 0
    assert "current" in capsys.readouterr().out


def test_check_refuses_to_call_a_path_current_without_an_alias(tmp_path, capsys):
    root = tmp_path / "attempts"
    result = run(root, tmp_path / "work")
    path = tmp_path / "work" / result.journal.identity / "result.txt"
    alias_path(
        root, plan_id="study", authored_key="point:write", output="result"
    ).unlink()

    assert main(["check", "--root", str(root), str(path)]) == 2
    assert "no current output alias" in capsys.readouterr().err


def test_check_exits_non_zero_and_explains_for_a_superseded_path(tmp_path, capsys):
    root = tmp_path / "attempts"
    stale = run(root, tmp_path / "work", "one")
    current = run(root, tmp_path / "work", "two")
    path = tmp_path / "work" / stale.journal.identity / "result.txt"

    assert main(["check", "--root", str(root), str(path)]) == 1
    output = capsys.readouterr().out
    assert stale.journal.identity in output
    assert current.journal.identity in output
    assert "inputs changed" in output


def test_log_lists_iterations_with_the_reason_each_reran(tmp_path, capsys):
    root = tmp_path / "attempts"
    first = run(root, tmp_path / "work", "one")
    second = run(root, tmp_path / "work", "two")

    assert main(["log", "--root", str(root), "study:point:write"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert second.journal.identity in lines[0] and "inputs" in lines[0]
    assert first.journal.identity in lines[1] and "first" in lines[1]


@operation(config={"value": parameter(int)}, outputs={"value": returned()})
def configured_value(*, value):
    return value


@study(default_policy=local())
def configured_study(value):
    return {"value": configured_value.named("point")(value=value).value}


def test_a_run_prints_the_rerun_reason_per_invocation(tmp_path, capsys):
    site = Site(root=str(tmp_path / "attempts"))
    configured_study(1).submit(site=site, sequential=True)
    capsys.readouterr()

    configured_study(2).submit(site=site, sequential=True, watch=True)

    output = capsys.readouterr().out
    assert "point" in output
    assert "rerun: arguments changed" in output


def test_a_reused_invocation_prints_reused_not_a_reason(tmp_path, capsys):
    site = Site(root=str(tmp_path / "attempts"))
    configured_study(1).submit(site=site, sequential=True)
    capsys.readouterr()

    configured_study(1).submit(site=site, sequential=True, watch=True)

    output = capsys.readouterr().out
    assert "reused" in output
    assert "rerun" not in output
