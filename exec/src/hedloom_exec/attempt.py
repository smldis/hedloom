"""The record/try protocol: claim, attach, reconcile, or refuse to guess.

This module encodes the ownership hypothesis under test. The durable journal
owns record identity; the transport owns try delivery; the substrate owns external
state after acceptance. No live object is treated as the authority for any of
them.

``launch_or_attach`` must resolve to exactly one of three dispositions, or fail
loudly. The failure is not a defect: an unrecoverable attempt is a real
property of a substrate that cannot answer questions about its own accepted
work, and reporting it beats acting blindly.

Under the current owner-bound lifetime decision, work is not meant to survive
its caller, so the ``attached`` disposition and ``UnrecoverableAttempt`` are
reachable only for a transport whose substrate keeps work after the submitter
dies. Nothing here does today. They are retained because the distinction they
encode — accepted, refused, or indeterminate — is exactly what an orphan-reaping
path needs in order to know whether there is anything to kill.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from hedloom_exec.artifacts import (
    MissingOutput,
    capture_outputs,
    workspace_for,
    workspace_path,
    write_diagnostics,
)
from hedloom_exec.errors import AttemptError
from hedloom_exec.identity import try_name
from hedloom_exec.journal import AttemptJournal, AttemptState, TryState
from hedloom_exec.reuse import input_digest
from hedloom_exec.transport import Observation, SubmissionRefused, Transport, substrate_of

__all__ = [
    "AttemptCancelled",
    "AttemptError",
    "StaleIdentity",
    "LaunchResult",
    "REUSABLE_OUTCOMES",
    "ReconciliationError",
    "UnrecoverableAttempt",
    "accept_for_reuse",
    "is_reusable",
    "launch_or_attach",
    "reconcile",
    "request_cancel",
]

REUSABLE_OUTCOMES = frozenset({"succeeded"})
"""Outcomes a later run may return instead of doing the work again.

Only success. A failure may have been the work's own verdict — a design that
does not converge — or something incidental to it: an out-of-memory kill, a
preempted node, a full filesystem. The record cannot tell those apart, and
guessing in either direction is worse than not guessing. Caching an infrastructure
failure would poison an invocation permanently; discarding a real negative
result would waste the run that produced it.

So a failed try is kept, not reused: rerunning is the default, the earlier try
stays on disk for inspection, and an operator who has looked at it can
mark it reusable with `accept_for_reuse`.
"""


class UnrecoverableAttempt(AttemptError):
    """Durable intent exists and the substrate cannot say whether it accepted.

    Raised only when discovery is non-authoritative. Resubmitting here could
    duplicate externally scheduled work; abandoning could lose it. Neither is
    the caller's choice to make silently.
    """


class ReconciliationError(AttemptError):
    """The durable record and the observed substrate state disagree."""


class AttemptCancelled(AttemptError):
    """Cancellation was recorded for this try; it must not be launched.

    Recorded intent outlives the process that recorded it. Submitting anyway
    would start work an operator has already stopped.
    """


class StaleIdentity(AttemptError):
    """The stored result under this identity came from different inputs.

    Only reachable when a caller supplies an identity by hand. A derived
    identity cannot produce this, because different inputs land elsewhere.
    """


def is_reusable(
    state: AttemptState | TryState, manifest: Mapping[str, Any] | None
) -> bool:
    """Whether a published result may stand in for running the work again."""

    if manifest is None:
        return False
    selected: TryState | None
    if isinstance(state, AttemptState):
        number = manifest.get("try")
        selected = next((item for item in state.tries if item.number == number), None)
    else:
        selected = state
    if selected is not None and selected.reuse_accepted:
        return True
    return manifest.get("outcome") in REUSABLE_OUTCOMES


def accept_for_reuse(journal: AttemptJournal, *, reason: str) -> AttemptState:
    """Record that a human inspected this result and chose to keep it.

    The escape hatch for a failure worth preserving — a known-bad point being
    debugged, or a negative result that should not be recomputed on every run.
    It is durable and attributable rather than a flag on a command line.
    """

    with journal.claim():
        state = journal.fold()
        current = state.current
        if current is None or not current.is_terminal:
            raise AttemptError(
                f"attempt {journal.identity} has no terminal try to accept"
            )
        published = journal.read_manifest(current.number)
        if published is None:
            raise AttemptError(
                f"attempt {journal.identity} try {current.number} has no "
                "published result to accept"
            )
        journal.append(
            "reuse_accepted",
            **{
                "try": current.number,
                "reason": reason,
                "outcome": published.get("outcome"),
            },
        )
        journal.make_standing(current.number)
        return journal.fold()


@dataclass(frozen=True, slots=True)
class LaunchResult:
    """What ``launch_or_attach`` resolved to, and the state it left behind."""

    disposition: str
    state: AttemptState
    manifest: Mapping[str, Any] | None = None


def launch_or_attach(
    journal: AttemptJournal,
    transport: Transport,
    bundle: Mapping[str, Any],
    *,
    workspace_root: str | Path | None = None,
) -> LaunchResult:
    """Resolve the current record try to one of three durable dispositions.

    ``completed`` — a manifest is already visible; the payload does not rerun.
    ``attached`` — the substrate already holds this attempt; no new submission.
    ``claimed``  — nothing was accepted before; this call submits it once.
    """

    with journal.claim():
        return _launch_or_attach_locked(
            journal, transport, bundle, workspace_root=workspace_root
        )


def _launch_or_attach_locked(
    journal: AttemptJournal,
    transport: Transport,
    bundle: Mapping[str, Any],
    *,
    workspace_root: str | Path | None = None,
) -> LaunchResult:
    state = journal.fold()
    _require_matching_inputs(journal, state, bundle)

    standing = journal.read_manifest()
    if standing is not None:
        standing_try = standing.get("try")
        selected = next(
            (item for item in state.tries if item.number == standing_try), None
        )
        if selected is None:
            raise ReconciliationError(
                f"attempt {journal.identity} has a standing result for unknown "
                f"try {standing_try!r}"
            )
        if not selected.is_terminal:
            journal.append(
                "terminal",
                **{
                    "try": selected.number,
                    "outcome": standing.get("outcome"),
                    "manifest": str(journal.manifest_path(selected.number)),
                    "repaired": True,
                },
            )
            state = journal.fold()
        if not is_reusable(state, standing):
            raise ReconciliationError(
                f"attempt {journal.identity} has a non-reusable standing result"
            )
        _bundle_for_try(
            journal,
            bundle,
            selected.number,
            workspace_root=workspace_root,
            create_workspace=False,
        )
        return LaunchResult("completed", state, standing)

    current = state.current
    if current is not None:
        published = journal.read_manifest(current.number)
        if published is not None:
            if not current.is_terminal:
                journal.append(
                    "terminal",
                    **{
                        "try": current.number,
                        "outcome": published.get("outcome"),
                        "manifest": str(journal.manifest_path(current.number)),
                        "repaired": True,
                    },
                )
                state = journal.fold()
                current = state.current
            assert current is not None
            if is_reusable(current, published):
                journal.make_standing(current.number)
                _bundle_for_try(
                    journal,
                    bundle,
                    current.number,
                    workspace_root=workspace_root,
                    create_workspace=False,
                )
                return LaunchResult("completed", state, published)
            # A retained, non-reusable terminal try is followed by a new one.
        elif current.is_terminal:
            raise ReconciliationError(
                f"attempt {journal.identity} try {current.number} claims a "
                f"terminal outcome but no manifest is visible at "
                f"{journal.manifest_path(current.number)}"
            )
        elif current.cancel_requested:
            raise AttemptCancelled(
                f"attempt {journal.identity} try {current.number} has a recorded "
                f"cancellation ({current.cancel_reason!r}) and will not be launched"
            )
        elif current.phase == "submitted":
            return LaunchResult("attached", state)
        elif current.phase == "intended":
            job_name = try_name(journal.identity, current.number)
            handle = transport.discover(job_name)
            if handle is not None:
                journal.append(
                    "submit_receipt",
                    **{
                        "try": current.number,
                        "handle": dict(handle),
                        "recovered": True,
                    },
                )
                return LaunchResult("attached", journal.fold())
            if not transport.discovery_is_authoritative:
                raise UnrecoverableAttempt(
                    f"attempt {journal.identity} try {current.number} recorded "
                    f"submission intent to transport {transport.name!r}, which "
                    "cannot authoritatively confirm or deny acceptance; "
                    "recoverable execution is unsupported here"
                )
            journal.append(
                "submit_lost",
                **{
                    "try": current.number,
                    "transport": transport.name,
                    "substrate": substrate_of(transport),
                },
            )

    number = journal.begin_try()
    state = journal.fold()
    job_name = try_name(journal.identity, number)

    if not any(event.event == "created" for event in state.events):
        journal.append(
            "created",
            **{
                "try": number,
                "operation": bundle.get("operation"),
                "input_digest": input_digest(bundle),
            },
        )

    submitted_bundle = _bundle_for_try(
        journal,
        bundle,
        number,
        workspace_root=workspace_root,
        create_workspace=True,
    )

    placement = bundle.get("placement")
    if placement:
        journal.append("placement", **{"try": number, **placement})

    journal.append(
        "submit_intent",
        **{
            "try": number,
            "transport": transport.name,
            "substrate": substrate_of(transport),
        },
    )
    try:
        handle = transport.submit(job_name, submitted_bundle)
    except SubmissionRefused as error:
        journal.append(
            "submit_refused",
            **{"try": number, "error": f"{type(error).__name__}: {error}"},
        )
        raise
    except Exception as error:
        journal.append(
            "submit_indeterminate",
            **{"try": number, "error": f"{type(error).__name__}: {error}"},
        )
        raise
    journal.append("submit_receipt", **{"try": number, "handle": dict(handle)})
    return LaunchResult("claimed", journal.fold())


def _bundle_for_try(
    journal: AttemptJournal,
    bundle: Mapping[str, Any],
    number: int,
    *,
    workspace_root: str | Path | None,
    create_workspace: bool,
) -> Mapping[str, Any]:
    """Bind one try's workspace, so declared outputs have somewhere to land.

    The workspace is named by the try, and that name is the only stable
    reference to it. There is no derived per-requester view: a record belongs
    to a computation rather than to a study, so a name-shaped alias could only
    have pointed one requester's spelling at another's work.
    """

    prepared: Mapping[str, Any] = {**bundle, "try": number}
    declared_outputs = bundle.get("outputs")
    if not declared_outputs and workspace_root is None:
        return prepared
    root = workspace_root or journal.directory.parent
    name = try_name(journal.identity, number)
    workdir = (
        workspace_for(root, name)
        if create_workspace
        else workspace_path(root, name)
    )
    return {**prepared, "workdir": str(workdir)}


def _require_matching_inputs(
    journal: AttemptJournal,
    state: AttemptState,
    bundle: Mapping[str, Any],
) -> None:
    """Refuse to reuse a record that was created from different inputs.

    The guard lives here, where identity meets the journal, rather than only in
    `execute`. Both entry points pass through this function, so a hand-supplied
    identity cannot route around it.
    """

    created = next(
        (event for event in state.events if event.event == "created"), None
    )
    if created is None:
        return
    recorded = created.data.get("input_digest")
    if recorded is None or not bundle:
        return
    current = input_digest(bundle)
    if recorded != current:
        raise StaleIdentity(
            f"attempt {journal.identity} was created from inputs {recorded} "
            f"but this bundle digests to {current}. A record is selected by "
            f"the declared computation, so a changed declaration must be given "
            f"its own record rather than written into this one."
        )


def request_cancel(
    journal: AttemptJournal, transport: Transport, *, reason: str
) -> AttemptState:
    """Record cancellation intent durably, then ask the substrate to stop.

    Intent is recorded first and unconditionally. A lost acknowledgement cannot
    establish whether the substrate acted, so cancellation is an intent to be
    reconciled later, never a fact established by this call returning.
    """

    with journal.claim():
        state = journal.fold()
        current = state.current
        if current is None:
            raise AttemptError(f"attempt {journal.identity} has no try to cancel")
        journal.append(
            "cancel_requested", **{"try": current.number, "reason": reason}
        )
        state = journal.fold()
        if state.phase == "submitted" and state.handle is not None:
            transport.cancel(state.handle)
        return journal.fold()


def reconcile(
    journal: AttemptJournal,
    transport: Transport,
    *,
    bundle_outputs: Mapping[str, Mapping[str, Any]] | None = None,
) -> AttemptState:
    """Observe the substrate and publish a terminal manifest when one is due.

    Success requires both an acceptable external state and an atomically
    published manifest. A disagreement between the record and the substrate is
    published as ``unreconciled`` rather than normalized into either outcome.
    """

    with journal.claim():
        return _reconcile_locked(
            journal, transport, bundle_outputs=bundle_outputs
        )


def _reconcile_locked(
    journal: AttemptJournal,
    transport: Transport,
    *,
    bundle_outputs: Mapping[str, Mapping[str, Any]] | None = None,
) -> AttemptState:
    state = journal.fold()
    current = state.current
    if current is None:
        raise ReconciliationError(
            f"attempt {journal.identity} has no allocated try to reconcile"
        )
    published = journal.read_manifest(current.number)
    if published is not None:
        if not current.is_terminal:
            journal.append(
                "terminal",
                **{
                    "try": current.number,
                    "outcome": published.get("outcome"),
                    "manifest": str(journal.manifest_path(current.number)),
                    "repaired": True,
                },
            )
            if published.get("outcome") == "succeeded":
                journal.make_standing(current.number)
        return journal.fold()
    if current.is_terminal:
        return state

    if state.phase != "submitted" or state.handle is None:
        raise ReconciliationError(
            f"attempt {journal.identity} cannot be reconciled from phase "
            f"{state.phase!r}"
        )

    observation: Observation = transport.poll(state.handle)
    journal.append(
        "observed",
        **{
            "try": current.number,
            "state": observation.state,
            "detail": dict(observation.detail or {}),
        },
    )

    if not observation.is_terminal:
        if observation.state == "absent":
            journal.publish_terminal(
                try_number=current.number,
                outcome="unreconciled",
                manifest={
                    "reason": "substrate reports no such accepted work",
                    "handle": dict(state.handle),
                },
            )
            return journal.fold()
        return journal.fold()

    outcome = observation.state
    if state.cancel_requested and outcome == "succeeded":
        # Cancellation was requested and the work finished anyway. That is a
        # real, reportable disagreement rather than a plain success.
        journal.publish_terminal(
            try_number=current.number,
            outcome="unreconciled",
            manifest={
                "reason": "cancellation was requested but the work succeeded",
                "cancel_reason": state.cancel_reason,
                "observed": dict(observation.detail or {}),
            },
        )
        return journal.fold()

    detail = dict(observation.detail or {})
    location = state.handle.get("workdir")
    workdir = Path(location) if location else None
    try:
        write_diagnostics(
            workdir, detail.get("stdout", ""), detail.get("stderr", "")
        )
    except OSError as error:
        # Diagnostics are evidence, but failure to write them must not erase a
        # completed substrate outcome or prevent terminal publication.
        detail["diagnostics_error"] = f"{type(error).__name__}: {error}"

    if outcome == "succeeded":
        try:
            produced = capture_outputs(
                bundle_outputs,
                workdir=workdir,
                stdout=detail.get("stdout", ""),
                stderr=detail.get("stderr", ""),
                value=detail.get("value"),
            )
        except MissingOutput as error:
            # The work reported success but did not produce what it promised.
            # That is a failed invocation, not a successful one with a gap.
            journal.publish_terminal(
                try_number=current.number,
                outcome="failed",
                manifest={**detail, "error": str(error)},
            )
            return journal.fold()
        if produced:
            detail["artifacts"] = [item.as_data() for item in produced]

    if state.placement:
        detail["placement"] = {
            **state.placement,
            "observed": {
                "transport": state.transport,
                "handle": {
                    key: value
                    for key, value in (state.handle or {}).items()
                    # `settings` is what the substrate was actually asked for,
                    # which only exists once a transport has resolved the
                    # request; it is evidence, not the plan's declaration.
                    if key in ("job_id", "identity", "settings", "transport", "workdir")
                },
            },
        }

    journal.publish_terminal(
        try_number=current.number, outcome=outcome, manifest=detail
    )
    return journal.fold()
