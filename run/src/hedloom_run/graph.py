"""Admit ready invocations and execute them under declared placement capacity.

Run resolves static dependencies on the controller and asks Exec to finalize
identity before admitting each invocation. The Session owner joins compatible
active work after that resolution. Waiting controllers occupy no worker slot.
Dask executes ready tasks, each requesting its declared placement resource;
sequential execution uses the same owner protocol without a scheduler.

A handle gates cancellation and entry and can enter Exec only once. Consumer
reports project shared execution evidence onto their own authored invocations.
Result-dependent branching, retries and fallback are not introduced here.
"""

from __future__ import annotations

import sys
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Mapping, Sequence

from hedloom_exec.attempt import AttemptError
from hedloom_exec.durability import Durability, execute
from hedloom_exec.planned import PlannedInvocation, prepare_invocations, finalize_invocation
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

__all__ = ["NestedCapacityExhausted", "run_plan_graph"]


class NestedCapacityExhausted(RuntimeError):
    """A nested run cannot be admitted, because its waiters hold every unit.

    Raised before the inner plan spends anything. The alternative is the worst
    outcome this kernel can produce: every unit of a placement held by an
    invocation that is blocked waiting for work which needs that same unit, so
    nothing is ever admitted, nothing fails, and the run hangs against a
    cluster whose workers all read as busy.
    """


# An invocation body may author and submit a further Plan. When it does, it is
# blocked for the whole of that inner run while still holding one unit of its
# own placement — that is not a leak, it is what "this invocation is still
# running" means. Two facts follow, and together they make the deadlock
# decidable rather than merely likely:
#
#   * every running task holds a unit of its placement (`_admission`), so
#   * if the number of *blocked* holders reaches a placement's capacity, every
#     unit is held by something that cannot proceed, and no task of that
#     placement will ever be admitted again.
#
# `_OCCUPANCY` is what lets a nested submission know which unit its caller is
# holding; `_BLOCKED_UNITS` counts the holders that are waiting. Both are
# process-global on purpose: this kernel's workers are threads in the
# submitting process, so a sibling waiter is as much a claim on capacity as an
# ancestor is.
_OCCUPANCY = threading.local()
_BLOCKED_UNITS: dict[str, int] = {}
_BLOCKED_LOCK = threading.Lock()


@contextmanager
def _occupying(placement: str):
    """Record which placement's unit the calling thread is holding."""

    previous = getattr(_OCCUPANCY, "placement", None)
    _OCCUPANCY.placement = placement
    try:
        yield
    finally:
        _OCCUPANCY.placement = previous


@contextmanager
def _waiting_on_nested_run():
    """Count this thread's held unit as blocked, for as long as it is.

    A no-op outside an invocation: a run submitted from the driver holds no
    unit of anything, and its capacity is whatever the cluster declares.
    """

    placement = getattr(_OCCUPANCY, "placement", None)
    if placement is None:
        yield
        return
    with _BLOCKED_LOCK:
        _BLOCKED_UNITS[placement] = _BLOCKED_UNITS.get(placement, 0) + 1
    try:
        yield
    finally:
        with _BLOCKED_LOCK:
            remaining = _BLOCKED_UNITS.get(placement, 0) - 1
            if remaining > 0:
                _BLOCKED_UNITS[placement] = remaining
            else:
                _BLOCKED_UNITS.pop(placement, None)


@dataclass(frozen=True, slots=True)
class _RunConfig:
    """Where the durable record and the workspaces live, for one run."""

    root: str
    workspace_root: str | None = None
    publish_selection: Any = None
    outputs: Mapping[str, Mapping[str, Mapping[str, Any]]] | None = None
    sources: Mapping[str, str] = field(default_factory=dict)
    """Declared sources, already located, keyed as input bindings name them.

    Travels to every task because any invocation may declare one, and a task
    reads it exactly as it reads an upstream output.
    """


@dataclass(frozen=True, slots=True)
class _Step:
    """One task's return: what happened, and what downstream tasks may read.

    The produced map travels along the graph edge rather than through shared
    state, so a task depends on exactly the outputs it declared inputs from.
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
    """Execute one invocation, recording which placement's unit it holds.

    The unit is the fact a nested submission needs: a body that authors and
    submits a further Plan is blocked for that whole run while still holding
    it, and `_require_nesting_headroom` can only decide whether that is
    survivable if it knows which placement is being held.
    """

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

    try:
        result = execute(
            chosen,
            bundle,
            durability=Durability.RECORDED,
            root=config.root,
            workspace_root=config.workspace_root,
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

    contributed = produced_by(item, result, root=config.root) if result.outcome == "succeeded" else {}
    return _Step(
        InvocationOutcome(
            invocation_id=item.invocation_id,
            authored_key=item.authored_key,
            operation=item.operation,
            input_digest=item.input_digest,
            disposition=result.disposition or "ran",
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

    The exception is a placement this run cannot serve at all. That invocation
    is refused by `select_transport` the moment it starts, exactly as it is
    under the sequential kernel, and refusing one branch must not abandon the
    others. Annotating it would instead hold it unrunnable forever against a
    capacity nobody declares, which would make the two kernels disagree about a
    plan — the one thing this module may not do. It reaches no transport, so
    the thread it occupies is measured in microseconds and there is nothing for
    a budget to protect.
    """

    name = _placement_of(item)
    if transports.get(name) is None:
        return {}
    return {f"{PLACEMENT_RESOURCE}{name}": 1}


def _declared_placements(client: Any) -> dict[str, float]:
    """Every placement capacity this cluster offers, summed over its workers."""

    offered: dict[str, float] = {}
    info = client.scheduler_info() or {}
    for worker in (info.get("workers") or {}).values():
        for name, amount in (worker.get("resources") or {}).items():
            offered[name] = offered.get(name, 0) + amount
    return offered


def _require_admission(
    client: Any,
    items: Sequence[PlannedInvocation],
    transports: Mapping[str, Transport],
) -> None:
    """Refuse a cluster that cannot admit this plan, before anything runs.

    A task asking for capacity no worker declares is not slow — it is never
    scheduled. Dask holds it unrunnable with no exception, no log line at the
    client, and an idle-looking cluster, which is the worst failure this design
    can produce: a sweep that appears to be waiting on the farm while the farm
    has never been asked for anything. Cheaper to refuse here, in the same
    spirit as `_require_shippable`.
    """

    offered = _declared_placements(client)
    # Only placements this run can actually serve. One it cannot is a
    # per-invocation refusal that both kernels already agree on; see
    # `_admission`.
    wanted = sorted(
        {
            name
            for name in (_placement_of(item) for item in items)
            if transports.get(name) is not None
        }
    )
    missing = [
        name for name in wanted if f"{PLACEMENT_RESOURCE}{name}" not in offered
    ]
    if not missing:
        return
    declared = sorted(
        name[len(PLACEMENT_RESOURCE):]
        for name in offered
        if name.startswith(PLACEMENT_RESOURCE)
    )
    raise UnsupportedPlacement(
        f"this cluster declares no capacity for placement "
        f"{', '.join(repr(name) for name in missing)}, which this plan uses. "
        f"It offers: {', '.join(declared) or 'no placements at all'}. Every task "
        "carries the placement it resolved to, so one the cluster does not offer "
        "is held unrunnable forever rather than failing — the run would appear to "
        "hang against an idle cluster. Build the cluster with "
        "hedloom_run.cluster.cluster_for(site), which derives its workers from the "
        "same profile the placements come from."
    )


def _require_nesting_headroom(
    client: Any,
    items: Sequence[PlannedInvocation],
    transports: Mapping[str, Transport],
) -> None:
    """Refuse a nested run whose waiters already hold every unit it needs.

    Sound rather than cautious, and the argument is short. Every running task
    holds one unit of its placement, so the units held by *blocked* waiters
    cannot be released until the work they are waiting for runs. If those
    waiters account for a placement's whole capacity, no task of that placement
    can be admitted, by anyone, ever — including the one this run is about to
    submit. That is a deadlock rather than a delay, and it is knowable here,
    before the inner plan spends anything.

    A run that is not nested reads an empty `_BLOCKED_UNITS` and returns.
    """

    with _BLOCKED_LOCK:
        blocked = dict(_BLOCKED_UNITS)
    if not blocked:
        return

    offered = _declared_placements(client)
    wanted = sorted(
        {
            name
            for name in (_placement_of(item) for item in items)
            if transports.get(name) is not None
        }
    )
    for name in wanted:
        capacity = offered.get(f"{PLACEMENT_RESOURCE}{name}", 0)
        held = blocked.get(name, 0)
        if capacity - held > 0:
            continue
        raise NestedCapacityExhausted(
            f"this plan needs placement {name!r}, whose declared capacity is "
            f"{capacity:g}, and {held:g} of those units are held by "
            "invocations that are themselves waiting for a nested run. Every "
            "unit is therefore held by something that cannot finish until this "
            "plan does, so nothing here would ever be admitted and the run "
            "would hang against workers that all read as busy. Give the "
            "operation that submits a nested plan a placement of its own, so "
            "its waiting does not consume the capacity the inner plan needs, "
            f"or declare more {name!r} capacity than there are invocations "
            "that nest."
        )


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
        step = _run_one(item, available, replace(config, publish_selection=handle.publish_selection), *upstream)
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
        str(Path(config.root).resolve()), str(Path(config.workspace_root).resolve()) if config.workspace_root else None,
        config.outputs))).digest()


def _run_ready(items, available, config, client, owner, bind, on_event, stop_on_failure):
    """Admit static dependencies from the controller after their identities resolve.

    Waiting/joining consumes no placement slot. Both kernels use this owner
    protocol; only the graph kernel submits ready executions to Dask.
    """
    from concurrent.futures import Future
    from pathlib import Path
    from uuid import uuid4
    from time import sleep
    from hedloom_run.execution import ExecutionOwner, ExecutionHandle

    owner = owner or ExecutionOwner(Path(config.root).resolve() / '.executions')
    token = uuid4().hex
    completed, active = {}, {}
    produced = dict(config.sources)
    remaining = list(items)
    stopped = False
    in_flight = []

    def save(item, step, notify=True):
        outcome = replace(step.outcome, invocation_id=item.invocation_id,
                          authored_key=item.authored_key, operation=item.operation)
        contributed = produced_by(item, outcome, root=config.root) if outcome.outcome == 'succeeded' else {}
        completed[item.invocation_id] = _Step(outcome, contributed)
        produced.update(contributed)
        if notify and on_event:
            on_event(outcome)

    def collect(identifier, notify=True):
        entry, item, consumer = active.pop(identifier)
        try:
            try:
                step = entry.future.result()
            except BaseException as error:
                save(item, _abnormal(item, error), notify)
                raise
            save(item, step, notify)
        finally:
            owner.release(entry, consumer)

    def withdraw(notify):
        preserved = []
        for identifier, (entry, item, consumer) in list(active.items()):
            decision = owner.withdraw(entry, consumer)
            if decision == 'preserve':
                if not entry.future.done():
                    in_flight.append(item.authored_key)
                preserved.append(identifier)
            else:
                active.pop(identifier)
                step = (_Step(_outcome(item, disposition='withdrawn', outcome='cancelled',
                        block_reason='consumer withdrew; shared execution belongs to remaining consumers'))
                        if decision == 'withdrawn' else _blocked(item))
                try:
                    save(item, step, notify)
                finally:
                    owner.release(entry, consumer)
        for item in remaining:
            save(item, _blocked(item), notify)
        remaining.clear()
        # Gate every abandoned pending execution before waiting on entered work.
        errors = []
        for identifier in preserved:
            try:
                collect(identifier, notify)
            except BaseException as error:
                errors.append(error)
        if errors:
            raise errors[0]

    try:
        while remaining or active:
            for identifier, (entry, item, consumer) in list(active.items()):
                if entry.future.done():
                    collect(identifier)
                    if completed[identifier].outcome.outcome != 'succeeded':
                        stopped = stopped or stop_on_failure
            if stopped:
                withdraw(True)
                break
            for spec in list(remaining):
                if client is None and active:
                    break
                if not all(dep in completed for dep in spec.depends_on):
                    continue
                remaining.remove(spec)
                if any(completed[dep].outcome.outcome != 'succeeded' for dep in spec.depends_on):
                    save(spec, _Step(_outcome(spec, disposition='skipped', outcome='blocked', block_reason='dependency failure')))
                    continue
                fresh_handle = ExecutionHandle.create(owner.root) if spec.execution == 'each_submission' else None
                item = finalize_invocation(spec, produced, fresh_handle.execution_id if fresh_handle else None)
                try:
                    placement, chosen = select_transport(item, available)
                except UnsupportedPlacement as error:
                    if fresh_handle is not None:
                        fresh_handle.cancel_before_start()
                    save(item, _Step(_outcome(item, disposition='refused', outcome='failed', error=str(error))))
                    stopped = stop_on_failure
                    if stopped:
                        break
                    continue
                bundle = build_bundle(item, produced=produced, placement_name=placement,
                                      transport=chosen, outputs=config.outputs)
                item = replace(item, bundle=bundle)
                consumer = f'{token}:{spec.invocation_id}'
                leader = False
                with owner.lock:
                    entry = owner.entry(_execution_contract(item, available, config), client, fresh_handle)
                    try:
                        if bind:
                            bind(spec.invocation_id, entry.handle, bundle['input_evidence'])
                    except BaseException:
                        if not entry.consumers:
                            entry.handle.cancel_before_start()
                            owner.release(entry, consumer)
                        raise
                    entry.consumers.add(consumer)
                    active[item.invocation_id] = (entry, item, consumer)
                    if entry.future is None:
                        if client is None:
                            entry.future = Future()
                            leader = True
                        else:
                            entry.future = client.submit(_run_handle, entry.handle, item, available, config,
                                key=f'{_task_key(item)}-{entry.handle.execution_id}',
                                resources=_admission(item, available), pure=False, retries=0)
                if leader:
                    try:
                        entry.future.set_result(_run_handle(entry.handle, item, available, config))
                    except BaseException as error:
                        entry.future.set_exception(error)
                if client is None:
                    collect(item.invocation_id)
                    stopped = stop_on_failure and completed[item.invocation_id].outcome.outcome != 'succeeded'
                    break
            if active:
                sleep(0.01)
    except BaseException as error:
        try:
            withdraw(False)
        except BaseException as cleanup_error:
            error.cleanup_error = cleanup_error
        for item in items:
            completed.setdefault(item.invocation_id, _blocked(item))
        error.report = _report(items, completed)
        error.in_flight = tuple(in_flight)
        raise
    return _report(items, completed)


def run_plan_graph(
    document: Mapping[str, Any],
    transport: Transport | None = None,
    **options: Any,
) -> RunReport:
    """Execute a Plan as a Dask graph — see `_run_plan_graph` for the arguments.

    This wrapper exists for one fact that cannot be observed from inside the
    run: whether the caller is an invocation that will be blocked, holding a
    unit of its own placement, for as long as this run takes. Counting that
    here is what lets a nested run be refused instead of deadlocking, and it
    costs a run submitted from the driver nothing — it holds no unit, so the
    claim is a no-op.
    """

    with _waiting_on_nested_run():
        return _run_plan_graph(document, transport, **options)


def _run_plan_graph(
    document: Mapping[str, Any],
    transport: Transport | None = None,
    *,
    client: Any,
    transports: Mapping[str, Transport] | None = None,
    root: str,
    workspace_root: str | None = None,
    commands: Mapping[str, Sequence[str]] | None = None,
    outputs: Mapping[str, Mapping[str, Mapping[str, Any]]] | None = None,
    identity_env: Mapping[str, str] | None = None,
    source_fingerprints: Mapping[str, str] | None = None,
    source_addresses: Mapping[str, str] | None = None,
    stop_on_failure: bool = True,
    on_event: Callable[[InvocationOutcome], None] | None = None,
    execution_owner: Any = None,
    on_execution: Any = None,
) -> RunReport:
    """Execute a Plan as a Dask graph and report in the Plan's own order.

    ``client`` is a `distributed.Client`. It is required rather than created
    here: the cluster's shape is an operational decision — how many concurrent
    jobs the site tolerates, whether a dashboard is served — and a library that
    silently started one would be choosing it for the operator.

    ``source_addresses`` locates each declared source and travels to every
    task, so an operation naming an external file as an input receives it
    wherever it lands. The path is resolved on this machine, which is a claim
    about the site: it must mean the same thing on whatever host runs the work.

    ``on_event`` fires as tasks complete, in completion order, so a long sweep
    is observable while it runs. The returned report stays in plan order, so a
    run remains comparable with any other run of the same plan.

    With ``stop_on_failure`` (the default), the first failed outcome stops work
    that has no live worker stack and waits for work already executing. An
    exception outside an invocation outcome is re-raised with ``report`` and
    ``in_flight`` attributes, so cleanup does not erase what the run had done.
    """

    from distributed import as_completed

    available = available_transports(transport, transports)
    _require_shippable(
        dict(transports) if transports is not None else {"*": transport}
    )
    config = _RunConfig(
        root=root,
        workspace_root=workspace_root,
        outputs=outputs,
        sources=dict(source_addresses or {}),
    )

    items = prepare_invocations(
        document,
        commands=commands,
        outputs=outputs,
        identity_env=identity_env,
        source_fingerprints=source_fingerprints,
    )
    _require_admission(client, items, available)
    _require_nesting_headroom(client, items, available)
    return _run_ready(items, available, config, client, execution_owner,
                      on_execution, on_event, stop_on_failure)
