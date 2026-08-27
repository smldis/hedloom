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
from hedloom_exec.identity import attempt_identity, try_name
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.lineage import why_reran
from hedloom_exec.reuse import attempts_for, input_digest, input_digests
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
    """The outcome of one invocation, whatever its durability."""

    outcome: str
    value: Any = None
    detail: Mapping[str, Any] | None = None
    durability: Durability = Durability.EPHEMERAL
    disposition: str | None = None
    journal: AttemptJournal | None = None
    artifacts: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    changed_keys: tuple[str, ...] = ()

    def address(self, name: str) -> str | None:
        """Where one declared output landed, for a downstream invocation."""

        return self.artifacts.get(name, {}).get("address")


def execute(
    transport: Transport,
    bundle: Mapping[str, Any],
    *,
    durability: Durability = Durability.EPHEMERAL,
    identity: str | None = None,
    root: str | None = None,
    plan_id: str | None = None,
    invocation_id: str | None = None,
    authored_key: str | None = None,
    unchecked_identity: bool = False,
    workspace_root: str | None = None,
) -> ExecutionResult:
    """Run one invocation at the declared durability level.

    ``EPHEMERAL`` touches no filesystem at all: no directory is created, no
    identity is required, and nothing survives the call. ``RECORDED`` runs the
    full attempt protocol and can complete from an existing manifest without
    rerunning the payload.

    Pass ``plan_id`` and ``invocation_id``: the identity is then derived from
    the bundle's declared inputs, and reuse cannot return a result computed
    from different ones, because different inputs land on a different identity.

    A bare ``identity`` is refused for recorded execution. Such an identity
    says nothing about what produced the result under it, so reuse against it
    can silently return stale work — the defect this argument exists to
    prevent. Tests that deliberately construct crash states may opt out with
    ``unchecked_identity=True``; production callers should not.
    """

    if durability is Durability.EPHEMERAL:
        # A per-call key. A shared constant let two concurrent ephemeral calls
        # read each other's results back out of the transport.
        key = identity or f"ephemeral-{uuid4().hex}"
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

    changed_keys: tuple[str, ...] = ()
    if plan_id and invocation_id:
        # Attribution only. Neither key participates in the input digest, so
        # recording where an attempt came from cannot change what it reuses.
        bundle = {**bundle, "plan": plan_id, "invocation": invocation_id}
        if authored_key is not None:
            bundle["authored_key"] = authored_key
        if identity is None:
            selected = attempt_identity(
                plan_id=plan_id,
                invocation_id=invocation_id,
                input_digest=input_digest(bundle),
            )
            identity = selected.rendered

            # A changed digest starts a new record. Same-digest retries are
            # tries inside that one record and never supersede one another.
            known = attempts_for(
                root, plan_id=plan_id, invocation_id=invocation_id
            )
            if not any(
                record.identity == selected.rendered for record in known
            ):
                prior = [
                    record
                    for record in known
                    if record.input_digest is not None
                    and record.input_digest != selected.input_digest
                ]
                if prior:
                    latest = max(
                        prior,
                        key=lambda record: (record.created_at or "", record.identity),
                    )
                    bundle["supersedes"] = latest.identity
                    if latest.input_digests:
                        changed_keys = why_reran(
                            latest.input_digests,
                            input_digests(bundle),
                        )

    if identity is None:
        raise ValueError(
            "recorded execution requires both plan_id and invocation_id so the "
            "identity can be derived from declared inputs"
        )
    if not (plan_id and invocation_id) and not unchecked_identity:
        raise ValueError(
            "a bare identity cannot make reuse sound: nothing ties it to the "
            "inputs a stored result was computed from. Pass plan_id and "
            "invocation_id, or unchecked_identity=True to construct a state "
            "deliberately."
        )
    # Refuse a caller-supplied record name before creating any durable state.
    try_name(identity, 0)

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
            changed_keys=changed_keys,
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
        changed_keys=changed_keys,
    )
