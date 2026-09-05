"""How much durability one invocation is worth.

Recording is a cost, and most invocations should not pay it. An ordinary
in-memory Python step that computes a number from two other numbers has nothing
worth reconstructing: if the caller dies, the step dies, and rerunning it is
cheaper than any record of it would have been.

Work that leaves the process is different. It can be expensive, it can be
observed by other people, and it can leave something behind that must be
cleaned up. That work earns a record.

The distinction is declared, never inferred from placement. An operation states
what it is; the runtime does not guess from where a call happened to land.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping
from uuid import uuid4

from hedloom_exec.attempt import (
    LaunchResult,
    launch_or_attach,
    reconcile,
)
from hedloom_exec.identity import attempt_identity
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.reuse import input_digest
from hedloom_exec.transport import Transport

__all__ = ["Durability", "ExecutionResult", "execute"]


class Durability(Enum):
    """What an invocation leaves behind."""

    EPHEMERAL = "ephemeral"
    """Nothing is written. Suitable for in-process work that dies with its
    caller and is cheaper to rerun than to record."""

    RECORDED = "recorded"
    """A full attempt directory: append-only events and a published manifest.
    Required for work that leaves the process."""


def _artifacts_of(result: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {item["name"]: item for item in result.get("artifacts", [])}


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """The outcome of one invocation, whatever its durability.

    ``record`` and ``try_number`` name the execution this call actually
    selected — the try whose evidence was published or reused. They are the
    reference a caller pins, prunes around, or reads back, and they are stated
    here rather than inferred later by scanning for a name or resolving a
    mutable pointer. A call that selected no try leaves both ``None`` rather
    than inventing one; ephemeral work has neither.
    """

    outcome: str
    value: Any = None
    detail: Mapping[str, Any] | None = None
    durability: Durability = Durability.EPHEMERAL
    disposition: str | None = None
    journal: AttemptJournal | None = None
    artifacts: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    record: str | None = None
    try_number: int | None = None

    def address(self, name: str) -> str | None:
        """Where one declared output landed, for a downstream invocation."""

        return self.artifacts.get(name, {}).get("address")


def execute(
    transport: Transport,
    bundle: Mapping[str, Any],
    *,
    durability: Durability = Durability.EPHEMERAL,
    root: str | None = None,
    workspace_root: str | None = None,
) -> ExecutionResult:
    """Run one invocation at the declared durability level.

    ``EPHEMERAL`` touches no filesystem at all: no directory is created, no
    identity is required, and nothing survives the call. ``RECORDED`` runs the
    full attempt protocol and can complete from an existing manifest without
    rerunning the payload.

    The bundle is the whole request. Its declared computation digest selects
    the record, so there is nothing to pass about who is asking: two callers
    declaring the same work reach the same record, and the second finds the
    first's evidence instead of recomputing it. Reuse cannot be stale by
    construction, because a changed declaration lands on a different record.

    The returned :class:`ExecutionResult` names the record and the try it
    selected, which is the reference to use for anything that must talk about
    *this* execution afterwards.
    """

    if durability is Durability.EPHEMERAL:
        # A per-call key. A shared constant let two concurrent ephemeral calls
        # read each other's results back out of the transport.
        key = f"ephemeral-{uuid4().hex}"
        handle = transport.submit(key, bundle)
        observation = transport.poll(handle)
        detail = dict(observation.detail or {})
        forget = getattr(transport, "forget", None)
        if forget is not None:
            forget(key)
        return ExecutionResult(
            outcome=observation.state,
            value=detail.get("value"),
            detail=detail,
            durability=durability,
        )

    if root is None:
        raise ValueError("recorded execution requires a root")

    identity = attempt_identity(computation_digest=input_digest(bundle)).rendered
    journal = AttemptJournal(root, identity)

    declared_outputs = bundle.get("outputs")
    launched: LaunchResult = launch_or_attach(
        journal,
        transport,
        bundle,
        workspace_root=workspace_root,
    )
    if launched.disposition == "completed":
        manifest = launched.manifest or {}
        result = dict(manifest.get("result", {}))
        return ExecutionResult(
            outcome=manifest.get("outcome", "unreconciled"),
            value=result.get("value"),
            detail=result,
            durability=durability,
            disposition="completed",
            journal=journal,
            artifacts=_artifacts_of(result),
            record=identity,
            try_number=manifest.get("try"),
        )

    state = reconcile(journal, transport, bundle_outputs=declared_outputs)
    published = (
        journal.read_manifest(state.current_try)
        if state.current_try is not None
        else None
    ) or {}
    result = dict(published.get("result", {}))
    return ExecutionResult(
        outcome=state.outcome or state.phase,
        value=result.get("value"),
        detail=result,
        durability=durability,
        disposition=launched.disposition,
        journal=journal,
        artifacts=_artifacts_of(result),
        record=identity,
        try_number=state.current_try,
    )
