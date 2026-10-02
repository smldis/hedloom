"""Worker execution helpers for the async ready-only controller.

Static dependency admission lives in controller.py; Dask executes ready recorded
work. The durable handle refuses replay before Exec is entered.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from typing import Any, Mapping

from hedloom_exec.attempt import AttemptError
from hedloom_exec.durability import Durability, execute
from hedloom_exec.planned import PlannedInvocation
from hedloom_exec.transport import Transport, TransportError

from hedloom_run.binding import (
    UnsupportedPlacement,
    available_transports,
    build_bundle,
    produced_by,
    select_transport,
)
from hedloom_run.driver import InvocationOutcome, RunReport
from hedloom_run.site import PLACEMENT_RESOURCE

__all__ = ["nested_submission_context"]


_OCCUPANCY = threading.local()


@contextmanager
def _occupying(placement: str):
    previous = getattr(_OCCUPANCY, "placement", None)
    _OCCUPANCY.placement = placement
    try:
        yield
    finally:
        _OCCUPANCY.placement = previous


def nested_submission_context() -> bool:
    """Whether submission is being attempted from an executing body."""
    return getattr(_OCCUPANCY, "placement", None) is not None


@dataclass(frozen=True, slots=True)
class _RunConfig:
    """Where the durable record and the workspaces live, for one run."""

    records_dir: str
    work_dir: str | None = None
    publish_selection: Any = None
    execution_handle: Any = None
    outputs: Mapping[str, Mapping[str, Mapping[str, Any]]] | None = None
    priority: int = 0
    sources: Mapping[str, str] = field(default_factory=dict)
    """Declared sources, already located, keyed as input bindings name them.

    Travels to every task because any invocation may declare one, and a task
    reads it exactly as it reads an upstream output.
    """


@dataclass(frozen=True, slots=True)
class _Step:
    """One task's return: what happened, and what downstream tasks may read.

    The controller delivers selected outputs before admitting dependent work;
    a task receives exactly the inputs its invocation declared.
    """

    outcome: InvocationOutcome
    produced: Mapping[Any, Any] = field(default_factory=dict)


def _outcome(
    item: PlannedInvocation,
    *,
    disposition: str,
    outcome: str,
    placement: str | None = None,
    error: str | None = None,
    **observation,
) -> InvocationOutcome:
    return InvocationOutcome(
        invocation_id=item.invocation_id,
        authored_key=item.authored_key,
        operation=item.operation,
        input_digest=item.input_digest,
        disposition=disposition,
        outcome=outcome,
        placement=placement,
        error=error,
        **observation,
    )


def _run_one(
    item: PlannedInvocation,
    transports: Mapping[str, Transport],
    config: _RunConfig,
    *upstream: _Step,
) -> _Step:
    """Execute a ready invocation, marking body-held submission unsupported."""

    with _occupying(_placement_of(item)):
        return _run_one_here(item, transports, config, *upstream)


def _run_one_here(
    item: PlannedInvocation,
    transports: Mapping[str, Transport],
    config: _RunConfig,
    *upstream: _Step,
) -> _Step:
    """Execute one invocation once every input it named has landed.

    Runs on a worker. Everything it needs arrives as an argument: there is no
    shared state between tasks, which is what lets the same function serve a
    thread, a process, or eventually a pooled worker.
    """

    unmet = [step for step in upstream if step.outcome.outcome != "succeeded"]
    if unmet:
        # Deliberately not an exception. A dependent of failed work has not
        # failed; it never ran, and the report should say so. Independent
        # branches of the plan are unaffected.
        return _Step(_outcome(item, disposition="skipped", outcome="blocked",
            block_reason="dependency failure: " + ", ".join(step.outcome.invocation_id for step in unmet)))

    # Sources first: they are produced before anything runs, so they are the
    # floor every upstream output is laid on top of.
    produced: dict[str, Any] = dict(config.sources)
    for step in upstream:
        produced.update(step.produced)

    try:
        placement_name, chosen = select_transport(item, transports)
    except UnsupportedPlacement as error:
        return _Step(
            _outcome(
                item,
                disposition="refused",
                outcome="failed",
                placement=(item.policy or {}).get("name"),
                error=str(error),
            )
        )

    bundle = dict(item.bundle) if "resolved_inputs" in item.bundle else build_bundle(
        item,
        produced=produced,
        placement_name=placement_name,
        transport=chosen,
        outputs=config.outputs,
    )

    bundle["scheduling"] = {"priority": config.priority}
    if config.execution_handle is not None:
        # Runtime control only: Exec identity and journal select explicit fields.
        # The command-only farm task never receives this local gate address.
        bundle["_execution_handle"] = config.execution_handle
    try:
        result = execute(
            chosen,
            bundle,
            durability=Durability.RECORDED,
            records_dir=config.records_dir,
            work_dir=config.work_dir,
            publish_selection=config.publish_selection,
        )
    except (AttemptError, TransportError) as error:
        return _Step(
            _outcome(
                item,
                disposition="refused",
                outcome="failed",
                placement=placement_name,
                error=f"{type(error).__name__}: {error}",
                record=error.selection.record if error.selection else None,
                try_number=error.selection.try_number if error.selection else None,
                observation_errors=error.publication_errors,
            )
        )

    contributed = produced_by(item, result, records_dir=config.records_dir) if result.outcome == "succeeded" else {}
    return _Step(
        InvocationOutcome(
            invocation_id=item.invocation_id,
            authored_key=item.authored_key,
            operation=item.operation,
            input_digest=item.input_digest,
            disposition="reused" if result.disposition == "completed" else result.disposition or "ran",
            outcome=result.outcome,
            placement=placement_name,
            value=result.value,
            artifacts=dict(result.artifacts),
            record=result.record,
            try_number=result.try_number,
            error=(result.detail or {}).get("error"),
            observation_errors=result.publication_errors,
        ),
        contributed,
    )


def _placement_of(item: PlannedInvocation) -> str:
    """The placement this invocation resolved to, at authoring time.

    Never absent: an operation that declares no policy gets `local` when the
    Plan is built, so there is no such thing as an unplaced invocation. This
    reads the same field `select_transport` reads, so the worker a task runs on
    and the transport it runs through can never disagree.
    """

    return (item.policy or {}).get("name") or "local"


def _admission(
    item: PlannedInvocation, transports: Mapping[str, Transport]
) -> dict[str, float]:
    """The capacity this task must be admitted against before it may run.

    One unit of its own placement, including `local`. Leaving local work
    unannotated is what lets it be scheduled onto — and later stolen onto — the
    worker whose threads are the farm's budget.

    Unsupported placements are refused during controller preparation, before
    executor submission. Return no annotation for that defensive helper case;
    requiring an undeclared resource would leave a task unrunnable forever.
    """

    name = _placement_of(item)
    if transports.get(name) is None:
        return {}
    return {f"{PLACEMENT_RESOURCE}{name}": 1}


def _require_shippable(transports: Mapping[str, Transport]) -> None:
    """Refuse a transport that cannot reach a worker, before anything runs.

    Dask serializes every task, so each transport is copied to the worker that
    runs the invocation. Left to Dask, a transport holding a lock, a socket, or
    a live client fails deep inside the protocol with a message about graph
    expressions, naming neither the placement nor the cause.
    """

    import cloudpickle

    for name, transport in transports.items():
        try:
            cloudpickle.dumps(transport)
        except Exception as error:  # deliberate: any failure to ship qualifies
            raise TypeError(
                f"the transport for placement {name!r} cannot be sent to a "
                f"worker ({type(error).__name__}: {error}). Dask copies a "
                "transport to the worker that runs the invocation, so it must "
                "be serializable; a transport that must stay a singleton needs "
                "to be built on the worker instead of passed to it."
            ) from error


def _task_key(item: PlannedInvocation) -> str:
    """A key an operator can recognise, and one Dask can learn from.

    The operation comes first because Dask groups tasks by everything before the
    first `-` and keeps a rolling average duration per group. Keyed by point,
    every task was its own group, nothing was ever learned, and every task fell
    back to a flat 500 ms estimate — the number the scheduler then used to decide
    which worker was least busy and what was worth stealing. Keyed by operation,
    the average becomes real after the first few points finish.

    The authored key stays, because the point of watching a sweep is still
    knowing which *point* is running. The digest suffix keeps it unique when the
    same key is planned twice.
    """

    name = item.authored_key or item.invocation_id
    return f"{item.operation}-{name}-{item.input_digest[:8]}"


def _blocked(item: PlannedInvocation) -> _Step:
    return _Step(_outcome(item, disposition="skipped", outcome="blocked",
        block_reason="stopped before admission"))


def _abnormal(item: PlannedInvocation, error: BaseException) -> _Step:
    """Name a broken task in a partial report without swallowing its error."""

    return _Step(
        _outcome(
            item,
            disposition="refused",
            outcome="failed",
            placement=_placement_of(item),
            error=f"{type(error).__name__}: {error}",
        )
    )


def _report(
    items: Sequence[PlannedInvocation], completed: Mapping[str, _Step]
) -> RunReport:
    return RunReport(tuple(completed[item.invocation_id].outcome for item in items))


def _run_handle(handle, item, available, config, *upstream):
    if not handle.enter():
        return _blocked(item)
    try:
        step = _run_one(item, available, replace(config,
            publish_selection=handle.publish_selection, execution_handle=handle), *upstream)
    except BaseException:
        # An entered handle cannot replay, even when the task has no result.
        # Keep the original exception if storage is also failing.
        try:
            handle.finish()
        except Exception:
            pass
        raise
    try:
        handle.finish()
    except Exception as error:
        step = replace(step, outcome=replace(step.outcome,
            observation_errors=(*step.outcome.observation_errors,
                                f'execution accounting failed: {error}')))
    return step


def _execution_contract(item, available, config):
    """Binding compatibility is stronger than declared computation equivalence."""
    import cloudpickle
    from hashlib import sha256
    from pathlib import Path
    _, transport = select_transport(item, available)
    binding = transport.execution_contract(item.operation) if hasattr(transport, 'execution_contract') else transport
    return sha256(cloudpickle.dumps((item.input_digest, binding, dict(item.policy),
        str(Path(config.records_dir).resolve()), str(Path(config.work_dir).resolve()) if config.work_dir else None,
        config.outputs))).digest()
