"""Read-only retention surveys over spent try workspaces.

Selection is a property of the record and its tries: outcome, age, size, and
how many terminal tries to keep. Nothing here asks who requested a record, and
nothing exempts a try because some derived per-requester view points at it. The
protections that remain are the ones about the evidence itself — an unfinished
or unreconciled try, a contended record, the standing evidence a future reuse
would select, and a pin someone made deliberately.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping
import os
import re
import shutil

from hedloom_exec.artifacts import workspace_path
from hedloom_exec.identity import try_name
from hedloom_exec.journal import AttemptJournal, ConcurrentClaim, TERMINAL_OUTCOMES
from hedloom_exec.reuse import AttemptRecord, scan_attempts

__all__ = [
    "Candidate", "RetentionError", "RetentionPolicy", "RetentionRule",
    "PruneReport", "Skip", "SkipReason", "Survey", "survey",
]


class RetentionError(ValueError):
    """A retention declaration or surveyed record cannot be read safely."""


_DURATION = re.compile(r"^(0|[1-9][0-9]*)([smhdw])$")
_DURATION_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
_SIZE = re.compile(r"^(0|[1-9][0-9]*)(B|KiB|MiB|GiB|TiB)$")
_SIZE_BYTES = {"B": 1, "KiB": 1024, "MiB": 1024**2,
               "GiB": 1024**3, "TiB": 1024**4}
_RULE_KEYS = frozenset(
    {"name", "outcome", "older_than", "larger_than", "keep_latest", "keep_logs"}
)
_POLICY_KEYS = frozenset({"floor", "rule", "automatic"})
_SELECTABLE_OUTCOMES = TERMINAL_OUTCOMES - {"unreconciled"}
_LOG_NAMES = frozenset({"stdout.log", "stderr.log"})


def _duration(value: str, *, field: str) -> timedelta:
    if not isinstance(value, str) or (matched := _DURATION.fullmatch(value)) is None:
        raise RetentionError(f"{field} has malformed duration {value!r}")
    count, unit = matched.groups()
    return timedelta(seconds=int(count) * _DURATION_SECONDS[unit])


def _size(value: str, *, field: str) -> int:
    if not isinstance(value, str) or (matched := _SIZE.fullmatch(value)) is None:
        raise RetentionError(f"{field} has malformed size {value!r}")
    count, unit = matched.groups()
    return int(count) * _SIZE_BYTES[unit]


def _timestamp(value: Any, *, identity: str, try_number: int) -> datetime:
    if not isinstance(value, str):
        raise RetentionError(
            f"attempt {identity} try {try_number} has no publication timestamp"
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise RetentionError(
            f"attempt {identity} try {try_number} has malformed publication "
            f"timestamp {value!r}"
        ) from error
    if parsed.tzinfo is None:
        raise RetentionError(
            f"attempt {identity} try {try_number} has a timezone-free timestamp"
        )
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class RetentionRule:
    """One reviewable conjunction of selection conditions."""

    name: str
    outcome: tuple[str, ...] = ()
    older_than: str | None = None
    larger_than: str | None = None
    keep_latest: int = 1
    keep_logs: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise RetentionError("each retention rule needs a non-empty name")
        # A bare string is iterable, so tuple() would silently split it into
        # characters and refuse with a nonsense list of one-letter outcomes.
        # `from_toml` and the CLI both guard this; the constructor is public
        # too, and must say the same thing.
        if isinstance(self.outcome, str):
            raise RetentionError(
                f"rule {self.name!r} outcome must be a sequence of outcome "
                f"names, not the single string {self.outcome!r}"
            )
        try:
            outcomes = tuple(self.outcome)
        except TypeError as error:
            raise RetentionError(
                f"rule {self.name!r} outcome must be a sequence of outcome names"
            ) from error
        object.__setattr__(self, "outcome", outcomes)
        if "unreconciled" in outcomes:
            raise RetentionError("unreconciled tries are never selectable")
        unknown = sorted(set(outcomes) - _SELECTABLE_OUTCOMES)
        if unknown:
            raise RetentionError(
                f"rule {self.name!r} names unknown outcome(s): {', '.join(unknown)}"
            )
        if self.older_than is not None:
            _duration(self.older_than, field=f"rule {self.name!r} older_than")
        if self.larger_than is not None:
            _size(self.larger_than, field=f"rule {self.name!r} larger_than")
        if (not isinstance(self.keep_latest, int)
                or isinstance(self.keep_latest, bool) or self.keep_latest < 0):
            raise RetentionError(
                f"rule {self.name!r} keep_latest must be a non-negative integer"
            )
        if not isinstance(self.keep_logs, bool):
            raise RetentionError(f"rule {self.name!r} keep_logs must be true or false")
        if not outcomes and self.older_than is None and self.larger_than is None:
            raise RetentionError(
                f"rule {self.name!r} has no selection condition; refusing 'everything'"
            )


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    rules: tuple[RetentionRule, ...]
    floor: str = "7d"

    def __post_init__(self) -> None:
        object.__setattr__(self, "rules", tuple(self.rules))
        _duration(self.floor, field="retention floor")
        names = [rule.name for rule in self.rules]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise RetentionError(
                f"retention rule names must be unique: {', '.join(duplicates)}"
            )

    @classmethod
    def from_toml(cls, data: Mapping[str, Any]) -> "RetentionPolicy":
        if not isinstance(data, Mapping):
            raise RetentionError("retention policy must be a mapping")
        unknown = sorted(set(data) - _POLICY_KEYS)
        if unknown:
            raise RetentionError(
                f"retention declares unknown key(s): {', '.join(unknown)}"
            )
        declarations = data.get("rule") or ()
        if not isinstance(declarations, (list, tuple)):
            raise RetentionError("retention.rule must be an array of tables")
        rules: list[RetentionRule] = []
        for index, declaration in enumerate(declarations):
            if not isinstance(declaration, Mapping):
                raise RetentionError(f"retention rule {index} must be a mapping")
            extra = sorted(set(declaration) - _RULE_KEYS)
            if extra:
                raise RetentionError(
                    f"retention rule {index} declares unknown key(s): {', '.join(extra)}"
                )
            values = dict(declaration)
            if "outcome" in values:
                raw = values["outcome"]
                if not isinstance(raw, (list, tuple)) or isinstance(raw, str):
                    raise RetentionError(
                        f"retention rule {index} outcome must be an array"
                    )
                values["outcome"] = tuple(raw)
            try:
                rules.append(RetentionRule(**values))
            except TypeError as error:
                raise RetentionError(
                    f"retention rule {index} is incomplete: {error}"
                ) from error
        automatic = data.get("automatic")
        if automatic is not None:
            if not isinstance(automatic, Mapping):
                raise RetentionError("retention.automatic must be a mapping")
            extra = sorted(set(automatic) - {"after_run"})
            if extra:
                raise RetentionError(
                    "retention.automatic declares unknown key(s): " + ", ".join(extra)
                )
            after_run = automatic.get("after_run", ())
            if not isinstance(after_run, (list, tuple)) or isinstance(after_run, str):
                raise RetentionError("retention.automatic.after_run must be an array")
            missing = sorted(set(after_run) - {rule.name for rule in rules})
            if missing:
                raise RetentionError(
                    "retention.automatic.after_run names unknown rule(s): "
                    + ", ".join(missing)
                )
        return cls(tuple(rules), data.get("floor", "7d"))


@dataclass(frozen=True, slots=True)
class Candidate:
    identity: str
    try_number: int
    workspace: Path
    bytes: int
    outcome: str
    published_at: str
    rule: str


SkipReason = Literal[
    "pinned", "floor", "unreconciled", "contended", "reusable",
    "non-terminal", "outside-roots", "no-rule",
]


@dataclass(frozen=True, slots=True)
class Skip:
    identity: str
    try_number: int | None
    reason: SkipReason
    detail: str = ""


@dataclass(frozen=True, slots=True)
class Survey:
    root: Path
    workspace_root: Path
    policy: RetentionPolicy
    candidates: tuple[Candidate, ...]
    skipped: tuple[Skip, ...]
    surveyed_at: str

    @property
    def freed_bytes(self) -> int:
        return sum(item.bytes for item in self.candidates)

    def summary(self) -> str:
        return (
            f"{len(self.candidates)} candidate(s), {self.freed_bytes} byte(s) "
            f"reclaimable; {len(self.skipped)} skipped"
        )

    def as_data(self) -> dict[str, Any]:
        return {
            "root": str(self.root), "workspace_root": str(self.workspace_root),
            "surveyed_at": self.surveyed_at, "freed_bytes": self.freed_bytes,
            "candidates": [
                {"identity": item.identity, "try": item.try_number,
                 "workspace": str(item.workspace), "bytes": item.bytes,
                 "outcome": item.outcome, "published_at": item.published_at,
                 "rule": item.rule}
                for item in self.candidates
            ],
            "skipped": [
                {"identity": item.identity, "try": item.try_number,
                 "reason": item.reason, "detail": item.detail}
                for item in self.skipped
            ],
        }

    def apply(
        self, *, limit_bytes: int | None = None, actor: str | None = None
    ) -> "PruneReport":
        """Re-check this proposal under each record claim, then remove it."""

        if limit_bytes is not None and (
            not isinstance(limit_bytes, int)
            or isinstance(limit_bytes, bool)
            or limit_bytes < 0
        ):
            raise RetentionError("limit_bytes must be a non-negative integer")
        removed: list[Candidate] = []
        skipped: list[Skip] = []
        freed = 0
        stopped = False

        for proposed in self.candidates:
            if limit_bytes is not None and freed >= limit_bytes:
                stopped = True
                break
            journal = AttemptJournal(self.root, proposed.identity)
            try:
                with journal.claim():
                    records = tuple(
                        item for item in scan_attempts(self.root)
                        if item.identity == proposed.identity
                    )
                    refreshed = survey(
                        self.root, self.policy, workspace_root=self.workspace_root,
                        records=records,
                    )
                    current = next(
                        (item for item in refreshed.candidates
                         if item.identity == proposed.identity
                         and item.try_number == proposed.try_number),
                        None,
                    )
                    if current is None:
                        reason = next(
                            (item for item in refreshed.skipped
                             if item.identity == proposed.identity
                             and item.try_number == proposed.try_number),
                            Skip(proposed.identity, proposed.try_number, "no-rule",
                                 "candidate no longer satisfies policy"),
                        )
                        skipped.append(reason)
                        continue

                    already_recorded = any(
                        event.event == "workspace_removed"
                        and event.data.get("try") == current.try_number
                        for event in journal.events()
                    )
                    if not already_recorded:
                        data: dict[str, Any] = {
                            "try": current.try_number,
                            "workspace": str(current.workspace),
                            "bytes": current.bytes,
                            "rule": current.rule,
                        }
                        if actor is not None:
                            data["actor"] = actor
                        journal.append("workspace_removed", **data)
                    rule = next(
                        item for item in self.policy.rules if item.name == current.rule
                    )
                    _remove_workspace(current.workspace, keep_logs=rule.keep_logs)
                    removed.append(current)
                    freed += current.bytes
            except ConcurrentClaim:
                skipped.append(
                    Skip(proposed.identity, proposed.try_number, "contended")
                )

        return PruneReport(
            tuple(removed), tuple(skipped), freed, stopped,
            datetime.now(timezone.utc).isoformat(timespec="microseconds"),
        )


@dataclass(frozen=True, slots=True)
class PruneReport:
    removed: tuple[Candidate, ...]
    skipped: tuple[Skip, ...]
    freed_bytes: int
    stopped_at_limit: bool
    applied_at: str


def _within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _workspace_bytes(workspace: Path, *, keep_logs: bool) -> int:
    if not workspace.exists():
        return 0
    total = 0
    for directory, directories, filenames in os.walk(workspace, followlinks=False):
        base = Path(directory)
        for name in list(directories):
            child = base / name
            if child.is_symlink():
                total += child.lstat().st_size
                directories.remove(name)
        for name in filenames:
            child = base / name
            if keep_logs and child.parent == workspace and name in _LOG_NAMES:
                continue
            total += child.lstat().st_size
    return total


def _remove_workspace(workspace: Path, *, keep_logs: bool) -> None:
    """Remove only the surveyed try payload, optionally preserving diagnostics."""

    if not workspace.exists():
        return
    if not keep_logs:
        shutil.rmtree(workspace)
        return
    for child in workspace.iterdir():
        if child.name in _LOG_NAMES and child.is_file() and not child.is_symlink():
            continue
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink()


def _validate_artifact_kinds(
    manifest: Mapping[str, Any], *, identity: str, try_number: int
) -> None:
    result = manifest.get("result") or {}
    artifacts = result.get("artifacts") or () if isinstance(result, Mapping) else ()
    for artifact in artifacts:
        if not isinstance(artifact, Mapping) or artifact.get("kind") not in {
            "file", "directory", "stream", "value"
        }:
            kind = artifact.get("kind") if isinstance(artifact, Mapping) else None
            raise RetentionError(
                f"attempt {identity} try {try_number} has artifact kind {kind!r}; "
                "a size rule cannot determine what to measure"
            )


def _is_pinned(state: Any, try_number: int) -> bool:
    return any(
        getattr(pin, "try_number", None) == try_number
        and getattr(pin, "is_active", False)
        for pin in getattr(state, "pins", ())
    )


def survey(
    root: str | os.PathLike[str], policy: RetentionPolicy, *,
    workspace_root: str | os.PathLike[str] | None = None,
    records: Iterable[AttemptRecord] | None = None,
) -> Survey:
    """Classify every try without creating or changing anything."""

    record_root = Path(root)
    work_root = Path(workspace_root) if workspace_root is not None else record_root
    source = tuple(scan_attempts(record_root) if records is None else records)
    now = datetime.now(timezone.utc)
    floor = _duration(policy.floor, field="retention floor")
    candidates: list[Candidate] = []
    skipped: list[Skip] = []

    for record in source:
        journal = AttemptJournal(record_root, record.identity)
        state = journal.fold()
        if not state.tries:
            skipped.append(Skip(record.identity, None, "non-terminal", "no try"))
            continue
        standing = journal.read_manifest()
        standing_try = standing.get("try") if standing is not None else None
        newest = sorted(
            (item.number for item in state.tries if item.is_terminal), reverse=True
        )
        rank = {number: index for index, number in enumerate(newest)}

        for item in state.tries:
            number = item.number
            if not item.is_terminal:
                skipped.append(Skip(record.identity, number, "non-terminal"))
                continue
            if item.outcome == "unreconciled":
                skipped.append(Skip(record.identity, number, "unreconciled"))
                continue
            if number == standing_try:
                skipped.append(Skip(record.identity, number, "reusable"))
                continue
            if _is_pinned(state, number):
                skipped.append(Skip(record.identity, number, "pinned"))
                continue
            manifest = journal.read_manifest(number)
            if manifest is None:
                skipped.append(
                    Skip(record.identity, number, "no-rule", "terminal manifest missing")
                )
                continue
            published_at = manifest.get("published_at")
            published = _timestamp(
                published_at, identity=record.identity, try_number=number
            )
            if now - published < floor:
                skipped.append(Skip(record.identity, number, "floor"))
                continue

            workspace = workspace_path(work_root, try_name(record.identity, number))
            if (workspace.is_symlink()
                    or not _within(workspace.absolute(), work_root.absolute())
                    or not _within(workspace.resolve(strict=False),
                                   work_root.resolve(strict=False))):
                skipped.append(Skip(record.identity, number, "outside-roots"))
                continue
            if not workspace.is_dir():
                skipped.append(
                    Skip(record.identity, number, "no-rule", "workspace is missing")
                )
                continue
            selected: Candidate | None = None
            for rule in policy.rules:
                if rank.get(number, 0) < rule.keep_latest:
                    continue
                if rule.outcome and item.outcome not in rule.outcome:
                    continue
                if rule.older_than is not None and now - published < _duration(
                    rule.older_than, field=f"rule {rule.name!r} older_than"
                ):
                    continue
                if rule.larger_than is not None:
                    _validate_artifact_kinds(
                        manifest, identity=record.identity, try_number=number
                    )
                measured = _workspace_bytes(workspace, keep_logs=rule.keep_logs)
                if rule.larger_than is not None and measured < _size(
                    rule.larger_than, field=f"rule {rule.name!r} larger_than"
                ):
                    continue
                selected = Candidate(
                    record.identity, number, workspace, measured, item.outcome or "",
                    str(published_at), rule.name,
                )
                break
            if selected is None:
                skipped.append(Skip(record.identity, number, "no-rule"))
            else:
                candidates.append(selected)

    return Survey(
        record_root, work_root, policy, tuple(candidates), tuple(skipped),
        now.isoformat(timespec="microseconds"),
    )
