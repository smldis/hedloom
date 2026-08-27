"""The stable view follows work without becoming its identity."""

from pathlib import Path
import os

from hedloom_exec.alias import (
    ALIAS_DIR,
    alias_path,
    alias_root,
    aliases_into,
    point_alias,
)
from hedloom_exec.durability import Durability, execute
from hedloom_exec.identity import try_name
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.reuse import scan_attempts
from hedloom_exec.transport import Observation
from hedloom_exec.watch import live_attempts


class FileTransport:
    name = "files"
    discovery_is_authoritative = True

    def __init__(self, *, outcome="succeeded", write=True, before=None):
        self.outcome = outcome
        self.write = write
        self.before = before
        self.handles = {}

    def submit(self, identity, bundle):
        workdir = Path(bundle["workdir"])
        if self.before:
            self.before(identity, bundle)
        if self.write:
            (workdir / "result.txt").write_text("first", encoding="utf-8")
        handle = {"identity": identity, "workdir": str(workdir)}
        self.handles[identity] = handle
        return handle

    def discover(self, identity):
        return self.handles.get(identity)

    def poll(self, _handle):
        return Observation(self.outcome, {})

    def cancel(self, _handle):
        return None


def run(root, workspace, *, inputs="one", transport=None):
    return execute(
        transport or FileTransport(),
        {
            "operation": "write",
            "inputs": {"source": inputs},
            "outputs": {"result": {"path": "result.txt"}},
        },
        durability=Durability.RECORDED,
        root=str(root),
        workspace_root=str(workspace),
        plan_id="study",
        invocation_id="invoke:key:point",
        authored_key="point:write",
    )


def test_an_alias_is_created_before_the_command_launches(tmp_path):
    root = tmp_path / "attempts"
    workspace = tmp_path / "work"

    def before(_identity, bundle):
        published = alias_path(
            root, plan_id="study", authored_key="point:write", output="result"
        )
        assert published.is_symlink()
        assert published.resolve(strict=False) == Path(bundle["workdir"]) / "result.txt"

    run(root, workspace, transport=FileTransport(before=before))


def test_an_alias_resolves_to_the_current_trys_workspace(tmp_path):
    root = tmp_path / "attempts"
    result = run(root, tmp_path / "work")
    published = alias_path(
        root, plan_id="study", authored_key="point:write", output="result"
    )

    assert published.resolve() == (
        tmp_path / "work" / try_name(result.journal.identity, 0) / "result.txt"
    )


def test_repointing_an_alias_is_atomic(tmp_path, monkeypatch):
    root = tmp_path / "attempts"
    first = tmp_path / "work" / "first" / "result.txt"
    second = tmp_path / "work" / "second" / "result.txt"
    published = point_alias(
        root,
        plan_id="study",
        authored_key="point:write",
        output="result",
        target=first,
    )
    replace = os.replace

    def observe(source, destination):
        assert Path(destination) == published
        assert published.resolve(strict=False) == first
        replace(source, destination)

    monkeypatch.setattr(os, "replace", observe)
    point_alias(
        root,
        plan_id="study",
        authored_key="point:write",
        output="result",
        target=second,
    )
    assert published.resolve(strict=False) == second


def test_a_reader_following_the_alias_sees_a_growing_file(tmp_path):
    target = tmp_path / "work" / "attempt" / "result.txt"
    published = point_alias(
        tmp_path / "attempts",
        plan_id="study",
        authored_key="point:write",
        output="result",
        target=target,
    )
    target.parent.mkdir(parents=True)
    target.write_text("one", encoding="utf-8")
    assert published.read_text(encoding="utf-8") == "one"
    target.write_text("one two", encoding="utf-8")
    assert published.read_text(encoding="utf-8") == "one two"


def test_a_new_try_repoints_the_alias(tmp_path):
    root = tmp_path / "attempts"
    work = tmp_path / "work"
    first = run(root, work, transport=FileTransport(outcome="failed", write=False))
    first_target = alias_path(
        root, plan_id="study", authored_key="point:write", output="result"
    ).resolve(strict=False)
    second = run(root, work, transport=FileTransport(outcome="failed", write=False))

    assert first.journal.identity == second.journal.identity
    assert first_target.parent.name == try_name(first.journal.identity, 0)
    assert alias_path(
        root, plan_id="study", authored_key="point:write", output="result"
    ).resolve(strict=False).parent.name == try_name(second.journal.identity, 1)


def test_a_new_record_repoints_the_alias(tmp_path):
    root = tmp_path / "attempts"
    work = tmp_path / "work"
    first = run(root, work, inputs="one")
    second = run(root, work, inputs="two")

    assert first.journal.identity != second.journal.identity
    assert alias_path(
        root, plan_id="study", authored_key="point:write", output="result"
    ).resolve().parent.name == try_name(second.journal.identity, 0)


def test_an_alias_to_an_unwritten_output_dangles_rather_than_lying(tmp_path):
    target = tmp_path / "work" / "attempt" / "result.txt"
    published = point_alias(
        tmp_path / "attempts",
        plan_id="study",
        authored_key="point:write",
        output="result",
        target=target,
    )

    assert published.is_symlink()
    assert not published.exists()


def test_hedloom_never_pre_touches_a_declared_output_to_close_the_window(tmp_path):
    root = tmp_path / "attempts"

    def before(_identity, bundle):
        assert not (Path(bundle["workdir"]) / "result.txt").exists()

    result = run(
        root,
        tmp_path / "work",
        transport=FileTransport(write=False, before=before),
    )
    assert result.outcome == "failed"
    assert "was not produced" in result.detail["error"]


def test_an_alias_is_never_created_inside_a_workspace(tmp_path):
    root = tmp_path / "attempts"
    result = run(root, tmp_path / "work")
    published = alias_path(
        root, plan_id="study", authored_key="point:write", output="result"
    )

    assert result.journal.identity not in published.parts
    assert published.parent not in published.resolve().parents


def test_aliases_into_finds_every_alias_for_a_workspace(tmp_path):
    root = tmp_path / "attempts"
    workspace = tmp_path / "work" / "attempt"
    first = point_alias(
        root, plan_id="study", authored_key="point", output="one", target=workspace / "one"
    )
    second = point_alias(
        root, plan_id="study", authored_key="point", output="two", target=workspace / "two"
    )
    point_alias(
        root,
        plan_id="study",
        authored_key="other",
        output="one",
        target=tmp_path / "work" / "other" / "one",
    )

    assert aliases_into(root, workspace) == (first, second)


def test_the_alias_root_is_not_hidden(tmp_path):
    assert ALIAS_DIR == "latest"
    assert alias_root(tmp_path).name == "latest"


def test_the_alias_root_is_derived_from_the_attempts_root(tmp_path):
    assert alias_root(tmp_path / "attempts") == tmp_path / "attempts" / "latest"


def test_aliases_are_built_by_default_with_nothing_configured(tmp_path):
    root = tmp_path / "attempts"
    run(root, tmp_path / "work")

    assert alias_root(root).is_dir()


def test_scan_attempts_skips_the_alias_directory(tmp_path):
    root = tmp_path / "attempts"
    run(root, tmp_path / "work")

    assert all(record.identity != ALIAS_DIR for record in scan_attempts(root))


def test_live_attempts_skips_the_alias_directory(tmp_path):
    root = tmp_path / "attempts"
    point_alias(
        root,
        plan_id="study",
        authored_key="point",
        output="result",
        target=tmp_path / "missing",
    )

    assert live_attempts(root) == ()


def test_a_directory_without_a_journal_is_never_read_as_an_attempt(tmp_path):
    root = tmp_path / "attempts"
    (root / "ordinary-directory").mkdir(parents=True)

    assert scan_attempts(root) == ()
    assert live_attempts(root) == ()


def test_the_pruner_never_treats_the_alias_directory_as_a_record(tmp_path):
    """The future pruner consumes this guarded record scan."""

    root = tmp_path / "attempts"
    (root / ALIAS_DIR).mkdir(parents=True)
    assert scan_attempts(root) == ()
