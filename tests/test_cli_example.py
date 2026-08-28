from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from hedloom.cli import main as cli
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.lineage import lineage
from hedloom_exec.reuse import scan_attempts

from examples import cli as cli_example


needs_tools = pytest.mark.skipif(
    shutil.which("awk") is None or shutil.which("sh") is None,
    reason="the example runs real awk and sh",
)


@needs_tools
def test_the_tour_runs_every_command_and_they_all_answer(tmp_path: Path) -> None:
    """The example checks each exit status itself; this checks it checked."""

    assert cli_example.main(tmp_path) == 0


@needs_tools
def test_check_exits_non_zero_for_a_path_that_was_superseded(tmp_path: Path) -> None:
    """`check` is meant to be scriptable, so `behind` has to be a status.

    The example prints what it saw; this asks the CLI directly, because a
    command that printed `behind` and still exited zero would leave every
    `if hedloom check ...` in a script silently wrong.
    """

    assert cli_example.main(tmp_path) == 0
    records = tmp_path / "attempts"
    workspaces = tmp_path / "work"

    scanned = scan_attempts(records)
    plan_id = next(item.plan_id for item in scanned)
    iterations = lineage(records, plan_id=plan_id, authored_key="narrow:tabulate")
    assert len(iterations) == 2, "the example edits one point's inputs once"

    current = next(item for item in iterations if item.is_current)
    superseded = next(item for item in iterations if not item.is_current)

    def table_of(identity: str) -> Path:
        record = next(item for item in scanned if item.identity == identity)
        return workspaces / f"{identity}-{record.try_number}" / "table.txt"

    assert cli(["check", str(table_of(current.identity)), "--root", str(records)]) == 0
    assert (
        cli(["check", str(table_of(superseded.identity)), "--root", str(records)]) == 1
    )

    # The point nobody edited stays current: `check` reports supersession at a
    # key, not "is there anything newer anywhere in this root".
    untouched = lineage(records, plan_id=plan_id, authored_key="wide:tabulate")
    assert len(untouched) == 1
    still = table_of(untouched[0].identity)
    assert cli(["check", str(still), "--root", str(records)]) == 0


@needs_tools
def test_the_pin_the_tour_takes_is_released_durably(tmp_path: Path) -> None:
    """The tour pins and then unpins, and both must be in the record."""

    assert cli_example.main(tmp_path) == 0
    records = tmp_path / "attempts"

    events = [
        event.event
        for record in scan_attempts(records)
        for event in AttemptJournal(records, record.identity).events()
    ]
    assert events.count("pinned") == 1
    assert events.count("unpinned") == 1, "a released pin keeps its history"

    active = [
        item
        for record in scan_attempts(records)
        for item in AttemptJournal(records, record.identity).fold().pins
        if item.is_active
    ]
    assert not active, "the tour releases the pin it takes"
