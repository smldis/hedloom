"""Readiness owned by Dask; placement still owned by the Plan.

One task per invocation, edges where one invocation's output feeds another's
input. Dask decides what is ready and how much runs at once; it does not decide
where anything runs, what an attempt's identity is, or whether work may be
reused — those stay with the Plan and with `hedloom_exec`, exactly as they were
under the sequential driver.

The invariant this kernel must not break:

    Changing which kernel decides readiness changes how long a plan takes and
    nothing else — the same results, under the same identities.

The binding rules it obeys are therefore imported from `hedloom_run.binding` rather
than restated here.

**Cluster shape matters, and the recommended one is unusual.** Build it from
the site profile, which is the only shape this kernel accepts:

    from hedloom_run.cluster import cluster_for
    cluster = cluster_for(site)

That gives one in-process worker per placement, each holding threads that
belong to that placement alone. Every task submitted here is annotated with the
placement its invocation already resolved to, so a worker's threads can only be
spent on the work they were sized for.

The annotation is not a refinement, it is the mechanism. A task that requests no
resource is legal on *every* worker, and Dask will place it on whichever looks
least busy and later **steal** it onto whichever falls idle — always the worker
sized for a large farm cap. A local invocation then holds a thread meant for a
`bsub -I`, and a farm job waits behind a python function with its capacity
unused. Annotating only the farm tasks does not prevent this; annotating all of
them makes it unrepresentable.

Three measured reasons for the rest of the shape, recorded in
`docs/vision/open-concepts.md`:

* An invocation waiting on `bsub -I` costs about 16 KiB of thread and one
  client process. A worker's thread count is therefore not a statement about
  this host's CPUs but about how many jobs may be in flight, which is why it is
  *derived* from the placement's declared cap rather than configured beside it:
  the two are independent gates and the smaller binds silently.
* Nothing secedes. A worker holding live `bsub -I` clients should read as
  running, and `secede()` would report it idle by excluding the task from the
  parallelism count.
* Threads avoid supervision and duplication, not serialization. A nanny that
  restarts a worker under memory pressure would take that worker's blocked
  clients with it — and, under owner-bound lifetime, that many running farm
  jobs. Measured, though, and worth knowing: Dask serializes every task even on
  an in-process cluster, so a **transport always travels as a copy**, never as
  a shared live object. Ours are effectively stateless per submission, so a
  copy is correct. A transport that must be a singleton — a pooled one holding
  a client to a second cluster — cannot be passed this way and will need a
  factory constructed on the worker.

A failed invocation blocks its dependents by returning a blocked outcome, not
by raising. Independent branches continue: one corner failing does not abandon
the other forty-nine, which is what a sweep wants and what the sequential
driver could not offer.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

from hedloom_exec.attempt import AttemptError
from hedloom_exec.durability import Durability, execute
from hedloom_exec.planned import PlannedInvocation, plan_bundles
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

__all__ = ["run_plan_graph"]


@dataclass(frozen=True, slots=True)
class _RunConfig:
    """Where the durable record and the workspaces live, for one run."""

    plan_id: str
    root: str
    workspace_root: str | None = None
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
    produced: Mapping[str, Any] = field(default_factory=dict)


def _outcome(
    item: PlannedInvocation,
    *,
    disposition: str,
    outcome: str,
    placement: str | None = None,
    error: str | None = None,
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
    )


def _run_one(
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
        return _Step(_outcome(item, disposition="skipped", outcome="blocked"))

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

    bundle = build_bundle(
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
            plan_id=config.plan_id,
            invocation_id=item.invocation_id,
        )
    except (AttemptError, TransportError) as error:
        return _Step(
            _outcome(
                item,
                disposition="refused",
                outcome="failed",
                placement=placement_name,
                error=f"{type(error).__name__}: {error}",
            )
        )

    contributed = produced_by(item, result) if result.outcome == "succeeded" else {}
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
            error=(result.detail or {}).get("error"),
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
    first `-` and keeps a rolling average duration per group. Keyed by corner,
    every task was its own group, nothing was ever learned, and every task fell
    back to a flat 500 ms estimate — the number the scheduler then used to decide
    which worker was least busy and what was worth stealing. Keyed by operation,
    the average becomes real after the first few corners finish.

    The authored key stays, because the point of watching a sweep is still
    knowing which *corner* is running. The digest suffix keeps it unique when the
    same key is planned twice.
    """

    name = item.authored_key or item.invocation_id
    return f"{item.operation}-{name}-{item.input_digest[:8]}"


@dataclass(frozen=True, slots=True)
class _Stop:
    """The futures whose real outcomes survive a stop-admission decision."""

    preserved: tuple[Any, ...]
    in_flight: tuple[str | None, ...]


def _blocked(item: PlannedInvocation) -> _Step:
    return _Step(_outcome(item, disposition="skipped", outcome="blocked"))


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


def _executing_keys(client: Any) -> set[str]:
    """Task keys with live Python stacks, not merely assigned to a worker."""

    return {
        key
        for tasks in (client.call_stack() or {}).values()
        for key in tasks
    }


def _stop_admitting(
    *,
    client: Any,
    items: Sequence[PlannedInvocation],
    futures: Mapping[str, Any],
    completed: dict[str, _Step],
    on_event: Callable[[InvocationOutcome], None] | None,
    notify: bool,
) -> _Stop:
    """Cancel work without a live stack and preserve work already executing."""

    by_id = {item.invocation_id: item for item in items}
    executing = _executing_keys(client)
    outstanding = {
        invocation_id: future
        for invocation_id, future in futures.items()
        if invocation_id not in completed
    }
    in_flight = {
        invocation_id: future
        for invocation_id, future in outstanding.items()
        if future.key in executing
    }
    finished = {
        invocation_id: future
        for invocation_id, future in outstanding.items()
        if future.done() and invocation_id not in in_flight
    }
    cancelled = {
        invocation_id: future
        for invocation_id, future in outstanding.items()
        if invocation_id not in in_flight and invocation_id not in finished
    }

    if cancelled:
        # A task can acquire a thread after the stack snapshot but before this
        # call. Dask cannot interrupt that Python thread: its `bsub -I` runs to
        # completion and its journal is published normally. The bounded loss is
        # only this run report's line for it, which is marked blocked below.
        client.cancel(list(cancelled.values()), force=False)
        for item in items:
            if item.invocation_id not in cancelled:
                continue
            step = _blocked(item)
            completed[item.invocation_id] = step
            if notify and on_event:
                on_event(step.outcome)

    return _Stop(
        preserved=tuple([*finished.values(), *in_flight.values()]),
        in_flight=tuple(
            by_id[invocation_id].authored_key for invocation_id in in_flight
        ),
    )


def _collect_preserved(
    futures: Sequence[Any],
    *,
    by_future_key: Mapping[str, PlannedInvocation],
    completed: dict[str, _Step],
    on_event: Callable[[InvocationOutcome], None] | None,
    notify: bool,
    suppress_errors: bool,
) -> None:
    """Wait for admitted tasks; cleanup may record but never replace an error."""

    from distributed import as_completed

    deferred: BaseException | None = None
    for future in as_completed(list(futures)):
        item = by_future_key[future.key]
        try:
            step = future.result()
        except BaseException as error:
            step = _abnormal(item, error)
            deferred = deferred or error
        completed[item.invocation_id] = step
        if notify and on_event:
            on_event(step.outcome)
    if deferred is not None and not suppress_errors:
        raise deferred


def run_plan_graph(
    document: Mapping[str, Any],
    transport: Transport | None = None,
    *,
    client: Any,
    transports: Mapping[str, Transport] | None = None,
    plan_id: str,
    root: str,
    workspace_root: str | None = None,
    commands: Mapping[str, Sequence[str]] | None = None,
    outputs: Mapping[str, Mapping[str, Mapping[str, Any]]] | None = None,
    identity_env: Mapping[str, str] | None = None,
    source_fingerprints: Mapping[str, str] | None = None,
    source_addresses: Mapping[str, str] | None = None,
    stop_on_failure: bool = True,
    on_event: Callable[[InvocationOutcome], None] | None = None,
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
        plan_id=plan_id,
        root=root,
        workspace_root=workspace_root,
        outputs=outputs,
        sources=dict(source_addresses or {}),
    )

    items = plan_bundles(
        document,
        commands=commands,
        identity_env=identity_env,
        source_fingerprints=source_fingerprints,
    )
    _require_admission(client, items, available)
    futures: dict[str, Any] = {}
    taken: set[str] = set()
    for item in items:
        # Two tasks sharing a key would be one task to Dask, and one of the two
        # invocations would never run. Readable first, unique always.
        key = _task_key(item)
        while key in taken:
            key = f"{key}.{len(taken)}"
        taken.add(key)
        futures[item.invocation_id] = client.submit(
            _run_one,
            item,
            available,
            config,
            *(futures[dependency] for dependency in item.depends_on),
            key=key,
            # The worker this may run on, which is the worker whose threads
            # belong to this invocation's placement. Not a hint: an
            # unannotated task is legal everywhere and gets moved.
            resources=_admission(item, available),
            # Side-effecting work with a durable record of its own. Dask must
            # not decide two invocations are the same call and run one; reuse
            # is `hedloom_exec`'s decision, made against declared inputs.
            pure=False,
        )

    by_future_key = {
        future.key: item
        for item in items
        for future in (futures[item.invocation_id],)
    }
    completed: dict[str, _Step] = {}
    known_in_flight: tuple[str | None, ...] = ()
    normal_exit = False
    try:
        for future in as_completed(list(futures.values())):
            step = future.result()
            completed[step.outcome.invocation_id] = step
            if on_event:
                on_event(step.outcome)
            if stop_on_failure and step.outcome.outcome != "succeeded":
                stopped = _stop_admitting(
                    client=client,
                    items=items,
                    futures=futures,
                    completed=completed,
                    on_event=on_event,
                    notify=True,
                )
                known_in_flight = stopped.in_flight
                _collect_preserved(
                    stopped.preserved,
                    by_future_key=by_future_key,
                    completed=completed,
                    on_event=on_event,
                    notify=True,
                    suppress_errors=False,
                )
                break
        normal_exit = True
    finally:
        if not normal_exit:
            error = sys.exc_info()[1]
            try:
                stopped = _stop_admitting(
                    client=client,
                    items=items,
                    futures=futures,
                    completed=completed,
                    on_event=on_event,
                    notify=False,
                )
                known_in_flight = tuple(
                    dict.fromkeys([*known_in_flight, *stopped.in_flight])
                )
                _collect_preserved(
                    stopped.preserved,
                    by_future_key=by_future_key,
                    completed=completed,
                    on_event=on_event,
                    notify=False,
                    suppress_errors=True,
                )
            except BaseException as cleanup_error:
                if error is None:
                    raise
                error.cleanup_error = cleanup_error

            for item in items:
                completed.setdefault(item.invocation_id, _blocked(item))
            if error is not None:
                error.report = _report(items, completed)
                error.in_flight = known_in_flight

    return _report(items, completed)
