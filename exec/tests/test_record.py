from pathlib import Path

from hedloom_exec.attempt import accept_for_reuse
from hedloom_exec.durability import Durability, execute
from hedloom_exec.identity import try_name
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.reuse import scan_attempts
from hedloom_exec.transport import InProcessTransport, Observation

BUNDLE = {"operation": "simulate", "inputs": {"model": "sha256:aaa"}}
COMMON = {"durability": Durability.RECORDED}


def flaky(failures):
    state = {"calls": 0}

    def simulate(**_kwargs):
        state["calls"] += 1
        if state["calls"] <= failures:
            raise MemoryError("node ran out of memory")
        return {"gain_db": 60.0}

    return InProcessTransport({"simulate": simulate}), state


class WorkspaceTransport:
    name = "workspace"
    discovery_is_authoritative = True

    def __init__(self):
        self.results = {}

    def submit(self, name, bundle):
        workdir = Path(bundle["workdir"])
        (workdir / "evidence.txt").write_text(name)
        self.results[name] = Observation("failed", {"error": "expected"})
        return {"identity": name, "workdir": str(workdir)}

    def discover(self, name):
        return {"identity": name} if name in self.results else None

    def poll(self, handle):
        return self.results[handle["identity"]]

    def cancel(self, _handle):
        return None


def test_repeated_failures_share_one_record(tmp_path):
    transport, state = flaky(3)
    results = [execute(transport, BUNDLE, root=str(tmp_path), **COMMON) for _ in range(3)]
    assert len({result.journal.identity for result in results}) == 1
    assert len(scan_attempts(tmp_path)) == 1
    assert [item.outcome for item in results[0].journal.fold().tries] == [
        "failed",
        "failed",
        "failed",
    ]
    assert state["calls"] == 3


def test_each_try_gets_its_own_workspace(tmp_path):
    transport = WorkspaceTransport()
    common = {**COMMON, "workspace_root": str(tmp_path / "work")}
    first = execute(transport, BUNDLE, root=str(tmp_path / "attempts"), **common)
    second = execute(transport, BUNDLE, root=str(tmp_path / "attempts"), **common)
    identity = first.journal.identity
    assert second.journal.identity == identity
    assert (tmp_path / "work" / try_name(identity, 0)).is_dir()
    assert (tmp_path / "work" / try_name(identity, 1)).is_dir()


def test_a_rerun_cannot_overwrite_an_earlier_trys_workspace(tmp_path):
    transport = WorkspaceTransport()
    common = {**COMMON, "workspace_root": str(tmp_path / "work")}
    first = execute(transport, BUNDLE, root=str(tmp_path / "attempts"), **common)
    original = tmp_path / "work" / try_name(first.journal.identity, 0) / "evidence.txt"
    before = original.read_text()
    execute(transport, BUNDLE, root=str(tmp_path / "attempts"), **common)
    assert original.read_text() == before


def test_a_rerun_cannot_overwrite_an_earlier_trys_manifest(tmp_path):
    transport, _ = flaky(2)
    first = execute(transport, BUNDLE, root=str(tmp_path), **COMMON)
    manifest_zero = first.journal.manifest_path(0)
    before = manifest_zero.read_text()
    execute(transport, BUNDLE, root=str(tmp_path), **COMMON)
    assert manifest_zero.read_text() == before
    assert first.journal.manifest_path(1).is_file()


def test_a_changed_input_starts_a_new_record_at_try_zero(tmp_path):
    transport, _ = flaky(1)
    first = execute(transport, BUNDLE, root=str(tmp_path), **COMMON)
    changed = {**BUNDLE, "inputs": {"model": "sha256:bbb"}}
    second = execute(transport, changed, root=str(tmp_path), **COMMON)
    assert first.journal.identity != second.journal.identity
    assert second.journal.fold().current_try == 0


def test_a_succeeded_try_ends_the_sequence_of_tries(tmp_path):
    transport, state = flaky(1)
    first = execute(transport, BUNDLE, root=str(tmp_path), **COMMON)
    second = execute(transport, BUNDLE, root=str(tmp_path), **COMMON)
    third = execute(transport, BUNDLE, root=str(tmp_path), **COMMON)
    assert first.outcome == "failed"
    assert second.outcome == "succeeded"
    assert third.disposition == "completed"
    assert state["calls"] == 2
    assert len(third.journal.fold().tries) == 2


def test_an_accepted_failure_stands_as_the_result(tmp_path):
    transport, state = flaky(1)
    first = execute(transport, BUNDLE, root=str(tmp_path), **COMMON)
    accept_for_reuse(first.journal, reason="known-bad point")
    second = execute(transport, BUNDLE, root=str(tmp_path), **COMMON)
    assert second.disposition == "completed"
    assert second.outcome == "failed"
    assert state["calls"] == 1
    assert AttemptJournal(tmp_path, first.journal.identity).fold().reuse_reason == "known-bad point"


def test_record_readers_project_standing_evidence_before_the_latest_try(tmp_path):
    transport, _ = flaky(2)
    result = execute(transport, BUNDLE, root=str(tmp_path), **COMMON)
    accept_for_reuse(result.journal, reason="selected")
    with result.journal.claim():
        assert result.journal.begin_try() == 1
    record = scan_attempts(tmp_path)[0]
    assert record.try_number == 0
    assert record.outcome == "failed"


def test_there_is_no_cap_on_retained_tries(tmp_path):
    transport, state = flaky(30)
    for _ in range(25):
        execute(transport, BUNDLE, root=str(tmp_path), **COMMON)
    records = scan_attempts(tmp_path)
    assert len(records) == 1
    assert len(AttemptJournal(tmp_path, records[0].identity).fold().tries) == 25
    assert state["calls"] == 25
