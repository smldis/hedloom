"""Read iteration order and current-result state from records and aliases."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Mapping

from hedloom_exec.alias import alias_path
from hedloom_exec.reuse import AttemptRecord, scan_attempts

__all__ = ["Iteration", "is_behind", "lineage", "why_reran"]


@dataclass(frozen=True, slots=True)
class Iteration:
    identity: str
    try_number: int
    outcome: str | None
    at: str
    supersedes: str | None
    changed_keys: tuple[str, ...]
    is_current: bool


def why_reran(
    prior: Mapping[str, str], current: Mapping[str, str]
) -> tuple[str, ...]:
    """Return only the identity-key names whose explanatory digests differ."""

    return tuple(
        key
        for key in sorted(set(prior) | set(current))
        if prior.get(key) != current.get(key)
    )


def _ordered(records: tuple[AttemptRecord, ...]) -> tuple[AttemptRecord, ...]:
    by_identity = {record.identity: record for record in records}
    superseded = {
        record.supersedes for record in records if record.supersedes in by_identity
    }
    heads = sorted(
        (record for record in records if record.identity not in superseded),
        key=lambda record: (record.created_at or "", record.identity),
        reverse=True,
    )
    ordered: list[AttemptRecord] = []
    visited: set[str] = set()
    for head in heads:
        current: AttemptRecord | None = head
        while current is not None and current.identity not in visited:
            ordered.append(current)
            visited.add(current.identity)
            current = by_identity.get(current.supersedes or "")
    ordered.extend(
        sorted(
            (record for record in records if record.identity not in visited),
            key=lambda record: (record.created_at or "", record.identity),
            reverse=True,
        )
    )
    return tuple(ordered)


def _current_identities(
    root: str | os.PathLike[str],
    *,
    plan_id: str,
    authored_key: str,
    records: tuple[AttemptRecord, ...],
) -> set[str]:
    key_directory = alias_path(
        root, plan_id=plan_id, authored_key=authored_key, output="probe"
    ).parent
    if not key_directory.is_dir():
        return set()
    identities = {record.identity for record in records}
    current: set[str] = set()
    for candidate in key_directory.iterdir():
        if not candidate.is_symlink():
            continue
        parts = candidate.resolve(strict=False).parts
        current.update(identity for identity in identities if identity in parts)
    return current


def lineage(
    root: str | os.PathLike[str], *, plan_id: str, authored_key: str
) -> tuple[Iteration, ...]:
    """Return creation order from supersedes and current state from aliases."""

    # This operator-facing query is deliberately keyed by authored identity.
    records = tuple(
        record
        for record in scan_attempts(root)
        if record.plan_id == plan_id and record.authored_key == authored_key
    )
    ordered = _ordered(records)
    current = _current_identities(
        root, plan_id=plan_id, authored_key=authored_key, records=records
    )
    by_identity = {record.identity: record for record in records}
    return tuple(
        Iteration(
            identity=record.identity,
            try_number=record.try_number if record.try_number is not None else 0,
            outcome=record.outcome,
            at=record.created_at or "",
            supersedes=record.supersedes,
            changed_keys=(
                why_reran(
                    by_identity[record.supersedes].input_digests,
                    record.input_digests,
                )
                if record.supersedes in by_identity
                else ()
            ),
            is_current=record.identity in current,
        )
        for record in ordered
    )


def is_behind(
    root: str | os.PathLike[str], path: str | os.PathLike[str]
) -> Iteration | None:
    """Return the current iteration when ``path`` belongs to an older one."""

    resolved = Path(path).resolve(strict=False)
    records = scan_attempts(root)
    stale = next(
        (record for record in records if record.identity in resolved.parts), None
    )
    if stale is None or stale.plan_id is None or stale.authored_key is None:
        return None
    iterations = lineage(
        root, plan_id=stale.plan_id, authored_key=stale.authored_key
    )
    matched = next(
        (iteration for iteration in iterations if iteration.identity == stale.identity),
        None,
    )
    if matched is None or matched.is_current:
        return None
    return next((iteration for iteration in iterations if iteration.is_current), None)
