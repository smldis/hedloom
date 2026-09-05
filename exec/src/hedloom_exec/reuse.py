"""Input identity and sound reuse.

Reuse is only honest if "already done" means "already done *with these
inputs*". This module derives a digest over the parts of a bundle that
determine its result, so that a changed input produces a different attempt
rather than a silently reused old one. That digest is the whole of a record's
identity: nothing about who asked participates, so two requesters declaring the
same computation are asking for the same record.

What participates is a real design decision, not an implementation detail:

* The operation, its command or arguments, its working directory, its declared
  input digests, and any environment explicitly nominated as identity-bearing.
* **Not** where or how the work ran. Queue, walltime, cores, and host do not
  change what a deterministic operation produces, so changing them must not
  invalidate a result. Placement is a scheduling concern; identity is a
  semantic one, and conflating them would make every resource tweak look like
  a new experiment.

An operation whose result depends on something outside this list — wall-clock
time, a mutable network resource, an undeclared file — is not honestly
reusable, and no digest can fix that. Declaring inputs is how an author makes
that promise explicit.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import blake2b
from pathlib import Path
from typing import Any, Mapping
import json

from hedloom_exec.journal import AttemptJournal

__all__ = [
    "AttemptRecord",
    "IDENTITY_KEYS",
    "input_digest",
    "scan_attempts",
]

IDENTITY_KEYS = (
    "operation",
    "operation_version",
    "implementation",
    "command",
    "arguments",
    "cwd",
    "inputs",
    "outputs",
    "identity_env",
)
"""Bundle keys that determine the result. Everything else is execution detail.

``operation_version`` is here because a reimplemented operation may produce a
different answer from the same inputs; omitting it would reuse results across a
change in meaning. ``implementation`` carries the same argument further: a
fingerprint of the body that will run turns that from a promise an author has
to remember into something the record notices by itself.
"""


def input_digest(bundle: Mapping[str, Any]) -> str:
    """Digest the identity-bearing content of a bundle.

    Deterministic across processes: the same declared inputs always produce the
    same digest, so two runs can agree on whether work is already done.
    """

    material = {
        key: bundle[key]
        for key in IDENTITY_KEYS
        if key in bundle and bundle[key] is not None
    }
    try:
        canonical = json.dumps(material, sort_keys=True, separators=(",", ":"))
    except TypeError as error:
        raise ValueError(
            "bundle inputs must be JSON-serializable to have a stable identity; "
            "pass a digest or a declared reference rather than a live object"
        ) from error
    return blake2b(canonical.encode(), digest_size=16).hexdigest()


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    """What one record says about itself, without opening try payloads.

    A record describes a computation, not a requester. It carries no study,
    Plan ID, invocation ID or authored key: those name who asked, and asking
    twice for the same declaration is one record, so storing an owner on it
    could only ever be the first caller's name masquerading as authority.
    """

    identity: str
    input_digest: str | None
    outcome: str | None
    directory: Path
    try_number: int | None = None
    created_at: str | None = None

    @property
    def is_terminal(self) -> bool:
        return self.outcome is not None


def scan_attempts(root: str | Path) -> tuple[AttemptRecord, ...]:
    """Read every attempt under ``root``.

    A directory scan is honest for a prototype and obviously wrong at scale;
    an index belongs here only once a real workload makes the scan hurt.
    """

    base = Path(root)
    if not base.is_dir():
        return ()

    records: list[AttemptRecord] = []
    for directory in sorted(base.iterdir()):
        if not (directory / "events.jsonl").exists():
            continue
        journal = AttemptJournal(base, directory.name)
        state = journal.fold()
        standing = journal.read_manifest()
        selected_try = (
            standing.get("try") if standing is not None else state.current_try
        )
        selected_outcome = (
            standing.get("outcome") if standing is not None else state.outcome
        )
        created = next(
            (event for event in state.events if event.event == "created"), None
        )
        data = created.data if created else {}
        records.append(
            AttemptRecord(
                identity=directory.name,
                input_digest=data.get("input_digest"),
                outcome=selected_outcome,
                directory=directory,
                try_number=selected_try,
                created_at=created.at if created else None,
            )
        )
    return tuple(records)
