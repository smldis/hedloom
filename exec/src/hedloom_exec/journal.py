"""Append-only attempt journal and atomic terminal publication.

The journal is the durable record that outlives any process, worker, executor,
or scheduler handle. Everything a restarted controller needs to reason about an
attempt is derived by folding this file; nothing is inferred from the continued
existence of an in-memory object.

Two ordering rules carry the whole recovery argument:

* ``submit_intent`` is written and flushed **before** a transport is asked to
  accept work, so an accepted submission whose receipt is lost still has a
  durable trace naming the identity to look for.
* ``terminal`` is written **after** the result manifest is atomically visible,
  so a journal that claims a terminal outcome always has readable evidence.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from contextlib import contextmanager
from typing import Any, Iterator, Mapping
import errno
import fcntl
import json
import os

from hedloom_exec.errors import AttemptError

__all__ = [
    "AttemptJournal",
    "AttemptState",
    "ClaimNotHeld",
    "ConcurrentClaim",
    "JournalError",
    "JournalEvent",
    "LAYOUT_VERSION",
    "TERMINAL_OUTCOMES",
    "TryState",
]

LAYOUT_VERSION = 1
TERMINAL_OUTCOMES = frozenset({"succeeded", "failed", "cancelled", "unreconciled"})

_EVENTS = frozenset(
    {
        "created",
        "try_started",
        "submit_intent",
        "submit_receipt",
        "submit_refused",
        "submit_indeterminate",
        "submit_lost",
        "cancel_requested",
        "observed",
        "terminal",
        "reuse_accepted",
        "placement",
    }
)


class JournalError(RuntimeError):
    """The durable record is missing, malformed, or internally contradictory."""


class ConcurrentClaim(JournalError, AttemptError):
    """Another caller holds this attempt right now.

    Reported rather than waited on: a second submission of the same attempt is
    the defect, so the honest response is to say who is already doing it.
    """


class ClaimNotHeld(JournalError):
    """A try allocation was attempted outside the record's exclusive claim."""


@dataclass(frozen=True, slots=True)
class JournalEvent:
    seq: int
    at: str
    event: str
    data: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class TryState:
    """The folded view of one execution try within a content record."""

    number: int
    phase: str = "unsubmitted"
    handle: Mapping[str, Any] | None = None
    transport: str | None = None
    substrate: str | None = None
    outcome: str | None = None
    manifest_path: str | None = None
    cancel_requested: bool = False
    cancel_reason: str | None = None
    reuse_accepted: bool = False
    reuse_reason: str | None = None
    placement: Mapping[str, Any] = field(default_factory=dict)
    observations: tuple[Mapping[str, Any], ...] = field(default_factory=tuple)
    started_at: str | None = None
    ended_at: str | None = None

    @property
    def is_terminal(self) -> bool:
        return self.phase == "terminal"


@dataclass(frozen=True, slots=True)
class AttemptState:
    """The folded view of one content record and all of its tries.

    ``phase`` is one of ``unsubmitted``, ``intended``, ``submitted``, or
    ``terminal``. ``intended`` is the crash window: durable intent exists but no
    receipt does, so whether the external system accepted the work is unknown
    from the record alone and must be settled by discovery.
    """

    identity: str
    tries: tuple[TryState, ...] = field(default_factory=tuple)
    current_try: int | None = None
    events: tuple[JournalEvent, ...] = field(default_factory=tuple)

    @property
    def current(self) -> TryState | None:
        if self.current_try is None:
            return None
        return next((item for item in self.tries if item.number == self.current_try), None)

    @property
    def phase(self) -> str:
        return self.current.phase if self.current is not None else "unsubmitted"

    @property
    def handle(self) -> Mapping[str, Any] | None:
        return self.current.handle if self.current is not None else None

    @property
    def transport(self) -> str | None:
        return self.current.transport if self.current is not None else None

    @property
    def substrate(self) -> str | None:
        return self.current.substrate if self.current is not None else None

    @property
    def outcome(self) -> str | None:
        return self.current.outcome if self.current is not None else None

    @property
    def manifest_path(self) -> str | None:
        return self.current.manifest_path if self.current is not None else None

    @property
    def cancel_requested(self) -> bool:
        return self.current.cancel_requested if self.current is not None else False

    @property
    def cancel_reason(self) -> str | None:
        return self.current.cancel_reason if self.current is not None else None

    @property
    def reuse_accepted(self) -> bool:
        return self.current.reuse_accepted if self.current is not None else False

    @property
    def reuse_reason(self) -> str | None:
        return self.current.reuse_reason if self.current is not None else None

    @property
    def placement(self) -> Mapping[str, Any]:
        return self.current.placement if self.current is not None else {}

    @property
    def observations(self) -> tuple[Mapping[str, Any], ...]:
        return self.current.observations if self.current is not None else ()

    @property
    def is_terminal(self) -> bool:
        return self.phase == "terminal"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _fsync_directory(path: Path) -> None:
    """Persist a directory entry, not just a file's contents.

    Without this, `os.replace` and the creation of `events.jsonl` are not
    crash-durable: the data survives but the name pointing at it can be lost.
    Both ordering rules above depend on the entry, not only the bytes.
    """

    try:
        descriptor = os.open(path, os.O_RDONLY)
    except (NotADirectoryError, FileNotFoundError):  # pragma: no cover
        return
    try:
        os.fsync(descriptor)
    except OSError:  # pragma: no cover - not every filesystem permits this
        pass
    finally:
        os.close(descriptor)


class AttemptJournal:
    """One record's durable directory: events, per-try manifests, and standing.

    The directory layout is deliberately plain so that an operator, a script, a
    CI job, or an agent can read the same facts without this package.
    """

    def __init__(self, root: str | os.PathLike[str], identity: str) -> None:
        self.identity = identity
        self.directory = Path(root) / identity
        self.log_path = self.directory / "events.jsonl"
        self.layout_path = self.directory / "layout"
        self.manifest_directory = self.directory / "manifest"
        self.standing_path = self.directory / "standing.json"
        self.lock_path = self.directory / "claim.lock"
        self._next_seq: int | None = None
        self._claim_held = False

    def exists(self) -> bool:
        return self.log_path.exists()

    def manifest_path(self, try_number: int) -> Path:
        """Return the immutable manifest address for one try."""

        if (
            not isinstance(try_number, int)
            or isinstance(try_number, bool)
            or try_number < 0
        ):
            raise JournalError("try number must be a non-negative integer")
        return self.manifest_directory / f"{try_number}.json"

    def _write_layout(self) -> None:
        temporary = self.directory / "layout.partial"
        with open(temporary, "w", encoding="utf-8") as handle:
            handle.write(f"{LAYOUT_VERSION}\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.layout_path)
        _fsync_directory(self.directory)

    def _require_layout(self, *, initialise_empty: bool = False) -> None:
        if not self.directory.exists():
            return
        if not self.layout_path.exists():
            if initialise_empty and not self.log_path.exists():
                self._write_layout()
                return
            raise JournalError(
                f"attempt {self.identity} has no recognised layout; roots "
                "written before record/workspace layout 1 are unreadable"
            )
        try:
            raw = self.layout_path.read_text(encoding="utf-8").strip()
            version = int(raw)
        except (OSError, ValueError) as error:
            raise JournalError(
                f"attempt {self.identity} has an unreadable layout declaration"
            ) from error
        if version != LAYOUT_VERSION:
            raise JournalError(
                f"attempt {self.identity} declares unsupported layout {version}; "
                f"this version reads layout {LAYOUT_VERSION} only"
            )

    @contextmanager
    def claim(self) -> Iterator[None]:
        """Hold this record exclusively while deciding what to do with it.

        Reading the record, recording intent, and submitting must be one
        indivisible step. Without it two controllers -- or two threads of one
        plan runner -- can both fold an unsubmitted state and both submit,
        producing two real jobs for one try. An advisory lock on a file in
        the attempt directory is enough: every writer goes through here.

        ==== DEVNOTE =============================================
        DEVNOTE/TODO -- REVIEW BEFORE ANY MULTI-HOST USE. Raised 2026-08-16.

        "An advisory lock on a file is enough" assumes a filesystem that
        honours the lock. **A study root on NFS may not be one, and it does
        not say so.** This is the weakest load-bearing assumption in the whole
        durability argument and it has never been tested against a real
        network filesystem.

        What can go wrong, all of it silently -- `flock()` returns success in
        every case below:

        * Linux emulates `flock` over NFS with POSIX byte-range locks, but the
          `local_lock=` mount option turns that off. Mounted `local_lock=flock`
          or `local_lock=all`, the lock becomes **node-local**: two hosts both
          acquire it and neither is told.
        * NFSv3 mounted `-o nolock` is the same story, and is common on
          clusters that had lockd trouble.
        * NFSv3 needs `rpc.statd`/`lockd` alive; NFSv4 locks are **leases**, so
          a client partitioned past its lease can have a lock revoked while it
          still believes it holds it.

        And the consequence is worse than duplicate jobs. The lock is what
        makes "exactly one writer" true, and `events.jsonl` is appended with
        `O_APPEND` -- which **NFS does not make atomic**, because the client
        resolves the offset itself. So a lock that silently degrades does not
        merely produce two `bsub` jobs for one identity: it can interleave or
        overwrite records in the journal that every recovery, reuse and
        identity decision is read back from. The durable record starts lying.

        Not at risk from *this*: per-try manifest and standing publication use
        atomic `rename()`, and publication now remains under this record claim.
        The historical TLA+ counterexample that found unlocked publication is
        documented in `docs/internals/attempt-claim-protocol.md`.

        Not reachable today, which is why this is a flag and not a fix: one
        process runs the plan, the Dask cluster is in-process, and two separate
        `open()` calls in one process do contend on `flock` correctly. The
        exposure opens with (a) two controllers against one root -- two people,
        or the same study started twice on two login hosts -- and (b) pooled
        placement, where journals would be written from farm nodes; see
        `hedloom/design/pooled-placement-plan.md` section 2, which defers that design for
        this reason.

        Options to weigh when it is picked up, cheapest first:

        1. **Detect rather than trust.** At run start, read the mount options
           for the study root from `/proc/mounts`; if it is NFS with
           `local_lock` or `nolock`, refuse or say so loudly. This is the house
           style -- refuse rather than guess -- and it is maybe thirty lines.
        2. **Replace the lock with an NFS-safe idiom.** `O_CREAT|O_EXCL` is
           atomic on NFSv4 but was not on v3; the portable one is the classic
           `link()`-then-check-`st_nlink == 2` dance, which holds on both.
        3. **Do not share a root across controllers** -- partition by run. Safe,
           but cross-run reuse is the entire point, so this costs the feature.
        4. **Ask the substrate instead** -- `bjobs -J <try-name>` before
           submitting. Racy on its own, and `discovery_is_authoritative`
           already encodes a better version of this idea.

        Do not treat the current single-host green test suite as evidence about
        any of this. It is evidence that the lock works on a local filesystem.
        ==== END DEVNOTE =========================================
        """

        new_record = not self.directory.exists()
        self.directory.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as error:
                if error.errno not in (errno.EACCES, errno.EAGAIN):
                    raise
                raise ConcurrentClaim(
                    f"attempt {self.identity} is already being launched by "
                    f"another caller"
                ) from error
            self._require_layout(initialise_empty=new_record)
            self._claim_held = True
            yield
        finally:
            self._claim_held = False
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    def begin_try(self, *, actor: str | None = None) -> int:
        """Reserve and durably record the next try while the claim is held.

        An allocated but never submitted try is resumed.  Terminal tries move
        to the next unbounded number.  The marker is fsynced before this method
        returns, so every later transport call has a recoverable job name.
        """

        if not self._claim_held:
            raise ClaimNotHeld(
                f"attempt {self.identity} cannot allocate a try without its claim"
            )
        state = self.fold()
        current = state.current
        if current is not None and current.phase == "unsubmitted":
            return current.number
        number = 0 if current is None else current.number + 1
        data: dict[str, Any] = {"try": number}
        if actor is not None:
            data["actor"] = actor
        self.append("try_started", **data)
        return number

    def append(self, event: str, /, **data: Any) -> JournalEvent:
        """Durably append one event, flushing before returning.

        The flush is the point of the method: a caller may crash immediately
        after it returns, and the record must already be on disk.
        """

        if event not in _EVENTS:
            raise JournalError(f"unknown journal event {event!r}")
        try_number = data.get("try")
        if (
            not isinstance(try_number, int)
            or isinstance(try_number, bool)
            or try_number < 0
        ):
            raise JournalError(f"journal event {event!r} requires a non-negative try")
        self.directory.mkdir(parents=True, exist_ok=True)
        self._require_layout()
        if self._next_seq is None:
            # Counted once per journal instance rather than on every append,
            # which made writing n events cost O(n^2) reads of the whole log.
            self._next_seq = sum(1 for _ in self._raw_lines())
        fresh_log = not self.log_path.exists()
        record = {
            "seq": self._next_seq,
            "at": _now(),
            "event": event,
            "data": data,
        }
        line = json.dumps(record, sort_keys=True, separators=(",", ":"))
        with open(self.log_path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        if fresh_log:
            _fsync_directory(self.directory)
        self._next_seq += 1
        return JournalEvent(record["seq"], record["at"], event, data)

    def _raw_lines(self) -> Iterator[str]:
        if not self.log_path.exists():
            return iter(())
        return (
            line
            for line in self.log_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )

    def events(self) -> tuple[JournalEvent, ...]:
        self._require_layout()
        parsed: list[JournalEvent] = []
        for index, line in enumerate(self._raw_lines()):
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise JournalError(
                    f"attempt {self.identity} has a malformed journal line {index}"
                ) from error
            if not isinstance(record, dict) or not {"seq", "at", "event"} <= set(
                record
            ):
                raise JournalError(
                    f"attempt {self.identity} has a structurally invalid journal "
                    f"line {index}: {line[:80]!r}"
                )
            parsed.append(
                JournalEvent(
                    seq=record["seq"],
                    at=record["at"],
                    event=record["event"],
                    data=record.get("data") or {},
                )
            )
        return tuple(parsed)

    def fold(self) -> AttemptState:
        """Derive current state from the durable record alone."""
        events = self.events()
        tries: dict[int, dict[str, Any]] = {}
        for item in events:
            number = item.data.get("try")
            if (
                not isinstance(number, int)
                or isinstance(number, bool)
                or number < 0
            ):
                raise JournalError(
                    f"attempt {self.identity} has event {item.event!r} without "
                    "a valid try number"
                )
            if item.event == "try_started":
                if number in tries:
                    raise JournalError(
                        f"attempt {self.identity} starts try {number} more than once"
                    )
                tries[number] = {
                    "number": number,
                    "phase": "unsubmitted",
                    "handle": None,
                    "transport": None,
                    "substrate": None,
                    "outcome": None,
                    "manifest_path": None,
                    "cancel_requested": False,
                    "cancel_reason": None,
                    "reuse_accepted": False,
                    "reuse_reason": None,
                    "placement": {},
                    "observations": [],
                    "started_at": item.at,
                    "ended_at": None,
                }
                continue
            if number not in tries:
                raise JournalError(
                    f"attempt {self.identity} records {item.event!r} for try "
                    f"{number} before try_started"
                )
            current = tries[number]
            if item.event == "submit_intent":
                current["phase"] = "intended"
                current["transport"] = item.data.get("transport")
                current["substrate"] = (
                    item.data.get("substrate") or current["transport"]
                )
            elif item.event == "submit_receipt":
                current["phase"] = "submitted"
                current["handle"] = item.data.get("handle")
            elif item.event in ("submit_refused", "submit_lost"):
                current["phase"] = "unsubmitted"
                current["handle"] = None
            elif item.event == "submit_indeterminate":
                current["phase"] = "intended"
            elif item.event == "cancel_requested":
                current["cancel_requested"] = True
                current["cancel_reason"] = item.data.get("reason")
            elif item.event == "placement":
                current["placement"] = {
                    key: value for key, value in item.data.items() if key != "try"
                }
            elif item.event == "reuse_accepted":
                current["reuse_accepted"] = True
                current["reuse_reason"] = item.data.get("reason")
            elif item.event == "observed":
                current["observations"].append(
                    {key: value for key, value in item.data.items() if key != "try"}
                )
            elif item.event == "terminal":
                current["phase"] = "terminal"
                current["outcome"] = item.data.get("outcome")
                current["manifest_path"] = item.data.get("manifest")
                current["ended_at"] = item.at

        folded: list[TryState] = []
        for number in sorted(tries):
            values = tries[number]
            outcome = values["outcome"]
            if outcome is not None and outcome not in TERMINAL_OUTCOMES:
                raise JournalError(
                    f"attempt {self.identity} records unknown outcome {outcome!r}"
                )
            values["observations"] = tuple(values["observations"])
            folded.append(TryState(**values))
        return AttemptState(
            identity=self.identity,
            tries=tuple(folded),
            current_try=folded[-1].number if folded else None,
            events=events,
        )

    def _publish_json(self, path: Path, document: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f"{path.name}.partial")
        payload = json.dumps(document, sort_keys=True, indent=2) + "\n"
        with open(temporary, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)

    def make_standing(self, try_number: int) -> Mapping[str, Any]:
        """Atomically make one published try the record's reusable result."""

        if not self._claim_held:
            raise ClaimNotHeld(
                f"attempt {self.identity} cannot change standing without its claim"
            )
        document = self.read_manifest(try_number)
        if document is None:
            raise JournalError(
                f"attempt {self.identity} try {try_number} has no manifest to stand"
            )
        self._publish_json(self.standing_path, document)
        return document

    def publish_terminal(
        self, *, try_number: int, outcome: str, manifest: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        """Publish one try's immutable manifest, then its terminal event.

        Successful evidence also becomes ``standing.json`` before the terminal
        event.  A crash after either rename leaves readable evidence from which
        reconciliation can repair the append-only record.
        """

        if not self._claim_held:
            raise ClaimNotHeld(
                f"attempt {self.identity} cannot publish without its claim"
            )
        if outcome not in TERMINAL_OUTCOMES:
            raise JournalError(f"unknown terminal outcome {outcome!r}")
        path = self.manifest_path(try_number)
        if path.exists():
            raise JournalError(
                f"attempt {self.identity} try {try_number} already has a manifest"
            )
        document = {
            "attempt": self.identity,
            "try": try_number,
            "outcome": outcome,
            "published_at": _now(),
            "result": dict(manifest),
        }
        self._publish_json(path, document)
        if outcome == "succeeded":
            self._publish_json(self.standing_path, document)
        self.append(
            "terminal",
            **{"try": try_number, "outcome": outcome, "manifest": str(path)},
        )
        return document

    def read_manifest(
        self, try_number: int | None = None
    ) -> Mapping[str, Any] | None:
        """Read one try manifest, or the record's standing result by default."""

        self._require_layout()
        path = self.standing_path if try_number is None else self.manifest_path(try_number)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            label = "standing result" if try_number is None else f"try {try_number}"
            raise JournalError(
                f"attempt {self.identity} has an unreadable {label} manifest"
            ) from error
