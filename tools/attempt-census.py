#!/usr/bin/env python3
"""Measure retry depth and storage in one records directory.

Usage:
    PYTHONPATH=exec/src python tools/attempt-census.py RECORDS_DIR [WORK_DIR]

The two locations are independent. The reader creates nothing and ignores any entry without an
``events.jsonl`` journal.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import os
from pathlib import Path
import sys
import time

from hedloom_exec.journal import AttemptJournal


def _disk_usage(path: Path) -> tuple[int, int]:
    """Return allocated bytes and inode count below ``path``."""

    total = 0
    entries = 0
    for directory, _directories, filenames in os.walk(path):
        entries += 1
        for name in filenames:
            try:
                stat = os.stat(os.path.join(directory, name))
            except OSError:
                continue
            total += stat.st_blocks * 512
            entries += 1
    return total, entries


def _percentile(values: list[int], fraction: float) -> int:
    if not values:
        return 0
    return values[min(len(values) - 1, int(len(values) * fraction))]


def census(records_dir: Path, work_dir: Path | None = None) -> str:
    """Return a reproducible, human-readable census without changing storage."""

    workspaces = work_dir or records_dir

    started = time.perf_counter()
    attempt_directories = sorted(
        path for path in records_dir.iterdir() if (path / "events.jsonl").exists()
    ) if records_dir.is_dir() else []
    directory_seconds = time.perf_counter() - started

    groups: dict[tuple[object, ...], list[tuple[int | None, str, bool, Path]]] = (
        defaultdict(list)
    )
    outcomes: Counter[str] = Counter()
    started = time.perf_counter()
    for directory in attempt_directories:
        journal = AttemptJournal(records_dir, directory.name)
        state = journal.fold()
        events = state.events
        created = next(
            (event.data for event in events if event.event == "created"),
            {},
        )
        # A record is a declaration, so the record directory is the group.
        key = (directory.name, created.get("input_digest"))
        for item in state.tries:
            outcome = item.outcome or item.phase
            outcomes[outcome] += 1
            groups[key].append(
                (item.number, outcome, item.reuse_accepted, directory)
            )
    scan_seconds = time.perf_counter() - started

    depths = sorted(len(group) for group in groups.values())
    deepest = [
        max((sequence for sequence, *_rest in group if sequence is not None), default=-1)
        for group in groups.values()
    ]
    large_groups = [group for group in groups.values() if len(group) >= 5]
    record_bytes, record_entries = _disk_usage(records_dir)
    workspace_bytes, workspace_entries = (
        _disk_usage(workspaces) if workspaces.is_dir() else (0, 0)
    )

    lines = [
        f"records_dir          {records_dir}",
        f"work_dir             {workspaces}",
        f"record directories   {len(attempt_directories)}",
        f"try outcomes         {dict(outcomes)}",
        "",
        f"records                      {len(groups)}",
        "  tries per record     "
        f"max {max(depths, default=0)}  p95 {_percentile(depths, 0.95)}  "
        f"median {_percentile(depths, 0.5)}",
        f"  deepest try in use         {max(deepest, default=-1)}",
        f"  records with >=5 tries     {len(large_groups)}",
        "",
        f"records   {record_bytes / 1048576:9.1f} MiB   {record_entries:>7} entries",
        f"workspaces{workspace_bytes / 1048576:9.1f} MiB   {workspace_entries:>7} entries",
    ]
    if record_bytes:
        lines.append(
            f"  workspace:record byte ratio  {workspace_bytes / record_bytes:.1f}x"
        )
    lines.extend(
        [
            "",
            f"readdir+sort {directory_seconds * 1000:.1f} ms   "
            f"read+fold+derive {scan_seconds * 1000:.1f} ms   "
            f"({scan_seconds / max(len(attempt_directories), 1) * 1e6:.0f} us/record)",
        ]
    )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("records_dir", type=Path)
    parser.add_argument("work_dir", nargs="?", type=Path)
    arguments = parser.parse_args(sys.argv[1:] if argv is None else argv)
    records_dir = arguments.records_dir.resolve()
    workspace = (
        arguments.work_dir.resolve() if arguments.work_dir else None
    )
    print(census(records_dir, workspace))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
