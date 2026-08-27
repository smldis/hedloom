"""Iteration history keeps creation order separate from the live result."""

from pathlib import Path

from hedloom_exec.alias import alias_path
from hedloom_exec.durability import Durability, execute
from hedloom_exec.lineage import is_behind, lineage, why_reran
from hedloom_exec.reuse import input_digests
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


def bundle(value="one", *, implementation="body-one"):
    return {
        "operation": "write",
        "implementation": {"fingerprint": implementation},
        "inputs": {"source": value},
        "outputs": {"result": {"path": "result.txt"}},
    }


def run(root, work, value="one", *, implementation="body-one"):
    return execute(
        FileTransport(),
        bundle(value, implementation=implementation),
        durability=Durability.RECORDED,
        root=str(root),
        workspace_root=str(work),
        plan_id="study",
        invocation_id="invoke:key:point",
        authored_key="point:write",
    )


def history(root):
    return lineage(root, plan_id="study", authored_key="point:write")


def test_lineage_walks_supersedes_newest_first(tmp_path):
    first = run(tmp_path / "attempts", tmp_path / "work", "one")
    second = run(tmp_path / "attempts", tmp_path / "work", "two")

    iterations = history(tmp_path / "attempts")
    assert [item.identity for item in iterations] == [
        second.journal.identity,
        first.journal.identity,
    ]
    assert iterations[0].supersedes == iterations[1].identity


def test_lineage_of_a_first_record_is_one_iteration(tmp_path):
    result = run(tmp_path / "attempts", tmp_path / "work")

    assert [item.identity for item in history(tmp_path / "attempts")] == [
        result.journal.identity
    ]


def test_why_reran_names_the_single_changed_key():
    prior = input_digests(bundle())
    current = input_digests(bundle(implementation="body-two"))

    assert why_reran(prior, current) == ("implementation",)


def test_why_reran_names_implementation_when_a_body_was_edited():
    prior = input_digests(bundle(implementation="body-one"))
    current = input_digests(bundle(implementation="body-two"))

    assert why_reran(prior, current) == ("implementation",)


def test_why_reran_names_inputs_when_a_source_was_edited():
    prior = input_digests(bundle("one"))
    current = input_digests(bundle("two"))

    assert why_reran(prior, current) == ("inputs",)


def test_why_reran_does_not_claim_to_name_which_input_changed():
    prior = input_digests(bundle("one"))
    current = input_digests(bundle("two"))

    assert why_reran(prior, current) == ("inputs",)
    assert "source" not in why_reran(prior, current)


def test_why_reran_is_empty_for_identical_bundles():
    evidence = input_digests(bundle())

    assert why_reran(evidence, evidence) == ()


def test_lineage_marks_the_alias_target_as_current(tmp_path):
    root = tmp_path / "attempts"
    first = run(root, tmp_path / "work", "one")
    second = run(root, tmp_path / "work", "two")

    current = next(item for item in history(root) if item.is_current)
    assert current.identity == second.journal.identity
    assert current.identity != first.journal.identity


def test_lineage_marks_nothing_current_when_no_alias_exists_yet(tmp_path):
    root = tmp_path / "attempts"
    result = run(root, tmp_path / "work")
    alias_path(
        root, plan_id="study", authored_key="point:write", output="result"
    ).unlink()

    assert result.journal.identity
    assert not any(item.is_current for item in history(root))


def test_a_reverted_edit_is_current_even_though_it_is_not_newest(tmp_path):
    root = tmp_path / "attempts"
    original = run(root, tmp_path / "work", "one")
    edited = run(root, tmp_path / "work", "two")
    reverted = run(root, tmp_path / "work", "one")

    iterations = history(root)
    assert reverted.journal.identity == original.journal.identity
    assert iterations[0].identity == edited.journal.identity
    assert (
        next(item for item in iterations if item.is_current).identity
        == original.journal.identity
    )


def test_is_behind_returns_none_for_the_current_workspace(tmp_path):
    root = tmp_path / "attempts"
    current = run(root, tmp_path / "work")
    path = tmp_path / "work" / current.journal.identity / "result.txt"

    assert is_behind(root, path) is None


def test_is_behind_names_the_iteration_that_superseded_a_stale_path(tmp_path):
    root = tmp_path / "attempts"
    stale = run(root, tmp_path / "work", "one")
    current = run(root, tmp_path / "work", "two")
    path = tmp_path / "work" / stale.journal.identity / "result.txt"

    replacement = is_behind(root, path)
    assert replacement is not None
    assert replacement.identity == current.journal.identity


def test_a_reverted_edit_returns_to_the_original_identity(tmp_path):
    root = tmp_path / "attempts"
    original = run(root, tmp_path / "work", "one")
    edited = run(root, tmp_path / "work", "two")
    reverted = run(root, tmp_path / "work", "one")

    assert original.journal.identity != edited.journal.identity
    assert reverted.journal.identity == original.journal.identity
