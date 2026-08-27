"""Per-try promises that a terminal workspace stays put and unchanged."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import blake2b
from pathlib import Path
from typing import Any, Literal, TYPE_CHECKING
from uuid import uuid4
import getpass
import os
import stat

from hedloom_exec.artifacts import workspace_path
from hedloom_exec.identity import try_name

if TYPE_CHECKING:  # pragma: no cover
    from hedloom_exec.journal import AttemptJournal, AttemptState

__all__ = [
    "FrozenFile", "Pin", "PinError", "PinSelectionError", "Verification",
    "VerifyOutcome", "is_pinned", "pin", "pins_of", "resolve_selector",
    "unpin", "verify",
]

_CHUNK = 1024 * 1024


class PinError(RuntimeError):
    """A pin promise cannot be made, released, or verified honestly."""


class PinSelectionError(PinError):
    """A human selector has no unique record meaning."""


@dataclass(frozen=True, slots=True)
class FrozenFile:
    relpath: str
    size: int
    modified_ns: int
    digest: str


@dataclass(frozen=True, slots=True)
class Pin:
    pin_id: str
    identity: str
    try_number: int
    workspace: str
    contents: tuple[FrozenFile, ...]
    reason: str
    actor: str
    at: str
    layout: int
    froze: bool
    released_at: str | None = None
    released_by: str | None = None
    released_reason: str | None = None

    @property
    def is_active(self) -> bool:
        return self.released_at is None


VerifyOutcome = Literal["intact", "drifted", "layout-changed", "missing"]


@dataclass(frozen=True, slots=True)
class Verification:
    outcome: VerifyOutcome
    drifted: tuple[str, ...] = ()
    detail: str = ""


def _actor(value: str | None) -> str:
    selected = value if value is not None else getpass.getuser()
    if not isinstance(selected, str) or not selected.strip():
        raise PinError("a pin actor must be a non-empty string")
    return selected


def _reason(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PinError("a pin reason must be a non-empty string")
    return value


def _digest(path: Path) -> str:
    digest = blake2b(digest_size=32)
    if path.is_symlink():
        digest.update(b"symlink\0")
        digest.update(os.readlink(path).encode())
    elif path.is_dir():
        digest.update(b"directory\0")
    else:
        digest.update(b"file\0")
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(_CHUNK), b""):
                digest.update(chunk)
    return digest.hexdigest()


def _inventory(workspace: Path) -> tuple[FrozenFile, ...]:
    found: list[FrozenFile] = []
    for path in sorted(workspace.rglob("*")):
        metadata = path.lstat()
        found.append(FrozenFile(
            relpath=str(path.relative_to(workspace)),
            size=metadata.st_size if not path.is_dir() else 0,
            modified_ns=metadata.st_mtime_ns,
            digest=_digest(path),
        ))
    return tuple(found)


def _modes(workspace: Path) -> dict[str, int]:
    paths = [workspace, *sorted(workspace.rglob("*"))]
    return {
        "." if path == workspace else str(path.relative_to(workspace)):
        stat.S_IMODE(path.lstat().st_mode)
        for path in paths if not path.is_symlink()
    }


def _freeze(workspace: Path, modes: dict[str, int]) -> bool:
    succeeded = True
    # Children first, workspace last: losing directory write permission cannot
    # prevent walking the children whose modes still need changing.
    for relative, mode in sorted(modes.items(), key=lambda item: item[0] == "."):
        path = workspace if relative == "." else workspace / relative
        try:
            path.chmod(mode & ~0o222)
        except OSError:
            succeeded = False
    return succeeded


def _thaw(workspace: Path, modes: dict[str, int]) -> bool:
    succeeded = True
    # Workspace first restores traversal/write before children.
    for relative, mode in sorted(modes.items(), key=lambda item: item[0] != "."):
        path = workspace if relative == "." else workspace / relative
        try:
            path.chmod(mode)
        except OSError:
            succeeded = False
    return succeeded


def pins_of(
    state: "AttemptState", *, try_number: int | None = None,
    active_only: bool = True,
) -> tuple[Pin, ...]:
    pins = tuple(getattr(state, "pins", ()))
    return tuple(
        item for item in pins
        if (try_number is None or item.try_number == try_number)
        and (not active_only or item.is_active)
    )


def is_pinned(state: "AttemptState", try_number: int) -> bool:
    return bool(pins_of(state, try_number=try_number, active_only=True))


def pin(
    journal: "AttemptJournal", *, try_number: int,
    workspace_root: str | os.PathLike[str], reason: str,
    actor: str | None = None, freeze: bool = True,
) -> Pin:
    """Inventory, optionally chmod, then durably promise one terminal try."""

    from hedloom_exec.journal import LAYOUT_VERSION

    reason = _reason(reason)
    actor = _actor(actor)
    with journal.claim():
        state = journal.fold()
        selected = next(
            (item for item in state.tries if item.number == try_number), None
        )
        if selected is None or not selected.is_terminal:
            raise PinError(
                f"attempt {journal.identity} try {try_number} is not terminal"
            )
        if is_pinned(state, try_number):
            raise PinError(
                f"attempt {journal.identity} try {try_number} is already pinned"
            )
        workspace = workspace_path(
            workspace_root, try_name(journal.identity, try_number)
        )
        if workspace.is_symlink() or not workspace.is_dir():
            raise PinError(
                f"attempt {journal.identity} try {try_number} has no workspace at "
                f"{workspace}"
            )
        contents = _inventory(workspace)
        modes = _modes(workspace)
        froze = _freeze(workspace, modes) if freeze else False
        pin_id = f"pin-{uuid4().hex}"
        journal.append(
            "pinned",
            **{
                "try": try_number, "pin_id": pin_id,
                "workspace": str(workspace.absolute()),
                "contents": [
                    {"relpath": item.relpath, "size": item.size,
                     "modified_ns": item.modified_ns, "digest": item.digest}
                    for item in contents
                ],
                "reason": reason, "actor": actor, "layout": LAYOUT_VERSION,
                "froze": froze, "modes": modes,
            },
        )
        return next(item for item in journal.fold().pins if item.pin_id == pin_id)


def unpin(
    journal: "AttemptJournal", *, pin_id: str, reason: str,
    actor: str | None = None, thaw: bool = True,
) -> Pin:
    """Release one pin by id, preserving its event history."""

    reason = _reason(reason)
    actor = _actor(actor)
    with journal.claim():
        state = journal.fold()
        selected = next(
            (item for item in state.pins
             if item.pin_id == pin_id and item.is_active), None
        )
        if selected is None:
            raise PinError(f"no active pin matches {pin_id!r}")
        pinned_event = next(
            event for event in state.events
            if event.event == "pinned" and event.data.get("pin_id") == pin_id
        )
        journal.append(
            "unpinned",
            **{"try": selected.try_number, "pin_id": pin_id, "reason": reason,
               "actor": actor, "thaw": thaw},
        )
        if thaw:
            _thaw(Path(selected.workspace), dict(pinned_event.data.get("modes") or {}))
        return next(item for item in journal.fold().pins if item.pin_id == pin_id)


def verify(pin: Pin, *, layout: int) -> Verification:
    if layout != pin.layout:
        return Verification(
            "layout-changed", detail=f"pin layout {pin.layout}, current layout {layout}"
        )
    workspace = Path(pin.workspace)
    if workspace.is_symlink() or not workspace.is_dir():
        return Verification("missing", detail=f"workspace missing: {workspace}")
    before = {item.relpath: item for item in pin.contents}
    after = {item.relpath: item for item in _inventory(workspace)}
    drifted = tuple(sorted(
        name for name in set(before) | set(after) if before.get(name) != after.get(name)
    ))
    return Verification("drifted", drifted) if drifted else Verification("intact")


def resolve_selector(root: str | os.PathLike[str], selector: str):
    """Resolve one record selector and optional try suffix from records alone."""

    from hedloom_exec.journal import AttemptJournal
    from hedloom_exec.reuse import scan_attempts

    records = scan_attempts(root)
    base, marker, suffix = selector.partition("#")
    requested_try: int | None = None
    if marker:
        if not suffix.isdigit() or str(int(suffix)) != suffix:
            raise PinSelectionError("try selector must end in #<non-negative integer>")
        requested_try = int(suffix)
    if base.startswith("hedloom-"):
        matches = [item for item in records if item.identity.startswith(base)]
    else:
        matches = [
            item for item in records
            if f"{item.plan_id}:{item.authored_key}" == base
        ]
    if not matches:
        raise PinSelectionError(f"no record matches {selector!r}")
    if len(matches) != 1:
        choices = ", ".join(item.identity for item in matches)
        raise PinSelectionError(f"selector {selector!r} is ambiguous: {choices}")
    record = matches[0]
    state = AttemptJournal(record.directory.parent, record.identity).fold()
    if requested_try is not None:
        selected = next(
            (item for item in state.tries if item.number == requested_try), None
        )
        if selected is None:
            raise PinSelectionError(
                f"record {record.identity} has no try {requested_try}"
            )
        return record, (selected,)
    return record, tuple(item for item in state.tries if item.is_terminal)
