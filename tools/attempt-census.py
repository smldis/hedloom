#!/usr/bin/env python3
"""Measure retry depth and storage in one attempt root.

Usage:
    PYTHONPATH=exec/src python tools/attempt-census.py ROOT [WORKSPACE_ROOT]

``ROOT`` may be the attempts directory itself or a parent containing
``attempts/``.  The reader creates nothing and ignores entries without an
``events.jsonl`` journal, including the derived ``latest/`` alias tree.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
import os
from pathlib import Path
import sys
import time

from hedloom_exec.identity import attempt_identity


MAX_PROBE = 64


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


def census(root: Path, workspace_root: Path | None = None) -> str:
    """Return a reproducible, human-readable census without changing the root."""

    attempts = root / "attempts" if (root / "attempts").is_dir() else root
    workspaces = workspace_root or root / "work"

    started = time.perf_counter()
    attempt_directories = sorted(
        path for path in attempts.iterdir() if (path / "events.jsonl").exists()
    ) if attempts.is_dir() else []
    directory_seconds = time.perf_counter() - started

    groups: dict[tuple[object, ...], list[tuple[int | None, str, bool, Path]]] = (
        defaultdict(list)
    )
    outcomes: Counter[str] = Counter()
    unresolved = 0

    started = time.perf_counter()
    for directory in attempt_directories:
        events = [
            json.loads(line)
            for line in (directory / "events.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        created = next(
            (event.get("data") or {} for event in events if event.get("event") == "created"),
            {},
        )
        terminal = next(
            (event for event in reversed(events) if event.get("event") == "terminal"),
            None,
        )
        accepted = any(event.get("event") == "reuse_accepted" for event in events)
        outcome = (terminal or {}).get("data", {}).get("outcome") or "non-terminal"
        outcomes[outcome] += 1

        sequence = created.get("try")
        if sequence is None and created.get("plan") and created.get("invocation"):
            for candidate in range(MAX_PROBE):
                rendered = attempt_identity(
                    plan_id=created["plan"],
                    invocation_id=created["invocation"],
                    sequence=candidate,
                    input_digest=created.get("input_digest"),
                ).rendered
                if rendered == directory.name:
                    sequence = candidate
                    break
        if sequence is None:
            unresolved += 1
        key = (
            created.get("plan"),
            created.get("invocation"),
            created.get("input_digest"),
        )
        groups[key].append((sequence, outcome, accepted, directory))
    scan_seconds = time.perf_counter() - started

    depths = sorted(len(group) for group in groups.values())
    deepest = [
        max((sequence for sequence, *_rest in group if sequence is not None), default=-1)
        for group in groups.values()
    ]
    large_groups = [group for group in groups.values() if len(group) >= 5]
    record_bytes, record_entries = _disk_usage(attempts)
    workspace_bytes, workspace_entries = (
        _disk_usage(workspaces) if workspaces.is_dir() else (0, 0)
    )

    lines = [
        f"root                 {root}",
        f"attempt directories  {len(attempt_directories)}",
        f"outcomes             {dict(outcomes)}",
        f"unresolved sequence  {unresolved}",
        "",
        f"(invocation,digest) groups   {len(groups)}",
        "  attempts per group  "
        f"max {max(depths, default=0)}  p95 {_percentile(depths, 0.95)}  "
        f"median {_percentile(depths, 0.5)}",
        f"  deepest sequence in use    {max(deepest, default=-1)}",
        f"  groups with >=5 attempts   {len(large_groups)}",
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
            f"({scan_seconds / max(len(attempt_directories), 1) * 1e6:.0f} us/attempt)",
        ]
    )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("workspace_root", nargs="?", type=Path)
    arguments = parser.parse_args(sys.argv[1:] if argv is None else argv)
    root = arguments.root.resolve()
    workspace = (
        arguments.workspace_root.resolve() if arguments.workspace_root else None
    )
    print(census(root, workspace))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
