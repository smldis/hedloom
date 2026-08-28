from __future__ import annotations

import shutil
import stat
from pathlib import Path

import pytest

from hedloom_exec.journal import AttemptJournal
from hedloom_exec.pins import pins_of
from hedloom_exec.reuse import scan_attempts

from examples import retention


needs_tools = pytest.mark.skipif(
    shutil.which("awk") is None or shutil.which("sh") is None,
    reason="the example runs real awk and sh",
)


@needs_tools
def test_reclaiming_frees_payload_and_keeps_every_record(tmp_path: Path) -> None:
    """The example's arithmetic, checked against the record rather than its output.

    The example prints what it believes; this reads the journals and the
    filesystem. A defect in either the protocol or the example's accounting
    shows up as the two disagreeing about what was spent and what was kept.
    """

    assert retention.main(tmp_path) == 0

    records = tmp_path / "attempts"
    workspaces = tmp_path / "work"
    scanned = scan_attempts(records)

    # Two passes over four points: the diverging ones are corrected rather than
    # retried, so they are new records and the first pass's failures remain.
    failed = [item for item in scanned if item.outcome == "failed"]
    assert len(failed) == 2, "the first pass must leave exactly two spent tries"

    # The example reclaims with `keep_logs`, so a spent workspace is emptied of
    # payload rather than deleted: the trace goes, the diagnostics that say why
    # it diverged stay. A test that looked for a missing directory would pass
    # for the wrong reason the day that default changed.
    def payload_of(record) -> list[str]:
        workspace = workspaces / f"{record.identity}-{record.try_number}"
        return sorted(
            item.name
            for item in workspace.rglob("*")
            if item.is_file() and item.name not in {"stdout.log", "stderr.log"}
        )

    removed = [item for item in failed if not payload_of(item)]
    assert len(removed) == 1, "one spent try is pinned; only the other is reclaimable"

    reclaimed = workspaces / f"{removed[0].identity}-{removed[0].try_number}"
    assert reclaimed.is_dir(), "reclaiming payload must not remove the diagnostics"
    left = sorted(item.name for item in reclaimed.iterdir())
    assert set(left) <= {"stdout.log", "stderr.log"}, f"payload survived: {left}"
    # The tool wrote its trace to the declared file, so stdout carried nothing
    # and no stdout.log was made. What must survive is the reason it diverged.
    assert "residual did not converge" in (reclaimed / "stderr.log").read_text()

    # The removal is durable evidence, not an inference from a missing directory.
    events = AttemptJournal(records, removed[0].identity).events()
    assert [item.event for item in events].count("workspace_removed") == 1

    # The record that names the reclaimed payload is still readable, and still
    # says the try failed. Reclaiming bytes must not erase why they were spent.
    assert removed[0].outcome == "failed"
    assert (records / removed[0].identity / "events.jsonl").is_file()


@needs_tools
def test_the_pinned_workspace_survives_and_stays_frozen(tmp_path: Path) -> None:
    """A pin is a promise about a path, so the path and its bytes must both hold."""

    assert retention.main(tmp_path) == 0

    records = tmp_path / "attempts"
    workspaces = tmp_path / "work"
    pinned = [
        (item, pins_of(AttemptJournal(records, item.identity).fold()))
        for item in scan_attempts(records)
    ]
    held = [(item, found) for item, found in pinned if found]
    assert len(held) == 1, "the example pins exactly one spent try"

    record, (promise,) = held[0]
    workspace = workspaces / f"{record.identity}-{record.try_number}"
    assert workspace.is_dir(), "a pinned workspace is never a prune candidate"
    assert Path(promise.workspace) == workspace

    # Freezing is a guardrail rather than enforcement, but the guardrail must
    # actually be in place: no payload file is left writable.
    payload = [item for item in workspace.rglob("*") if item.is_file()]
    assert payload, "the pinned try wrote a trace before it diverged"
    assert not any(
        item.lstat().st_mode & stat.S_IWUSR for item in payload
    ), "pinning with freeze must remove the write bit it recorded"
