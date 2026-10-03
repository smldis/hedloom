"""Pooled LSF placement: many invocations over a few reusable farm workers.

The direct placement submits one `bsub -I` per invocation, which is what makes
an invocation individually visible, individually cancellable, and individually
accounted. It costs one queue dispatch and one live `bsub` client *process* on
the submit host per invocation in flight, and
`design/concurrency-two-workers-2026-08-15.md` §9 found that process count, not
threads, is the ceiling that actually binds.

Pooled placement pays the dispatch once per *worker* instead. The workers are
LSF jobs held open by `dask_jobqueue.LSFCluster`; an invocation routed to a
pool ships its argv to one of them and waits on a future rather than on a
subprocess.

Note what it does *not* remove: the thread. A readiness worker thread is held
for the whole wait, exactly as a `bsub -I` waiter holds one. Pooled is a
process-count fix, not a thread-count fix, and that is deliberate — it is the
one of those two that addresses the constraint that was found to bind.

## What runs where, and why the line is drawn here

Design (i) of `design/pooled-placement-plan.md` §2: **the command only**. Identity,
the journal, the workspace and `execute()` all stay on the submit host, exactly
where they are for a direct placement. Only argv crosses to the pooled worker.

The alternative — moving the whole of `_run_one` — would put journal writes on
farm nodes, and the claim protocol takes `fcntl.flock` on `events.jsonl`. Every
other invariant in this system is host-agnostic; that one is not, and over NFS
from many hosts it is the piece of the durability argument that does not
obviously survive. It is rejected until somebody measures that contention. Not
an argument — a test.

## Why this transport holds no client

Dask copies a transport to the worker that runs the invocation, so a transport
holding a live `Client` cannot ship — `hedloom_run.graph._require_shippable`
refuses it up front, by placement name. That refusal is not an obstacle to work
around; it is the guard that forces the design below.

So the transport carries only *data*: which pool to use and what that pool was
asked for. The live client is built on the worker by `PooledClientPlugin` and
found through `distributed.get_worker()` at submit time. `WorkerPlugin.setup()`
runs in the worker's main thread, which is where a non-serializable singleton
belongs.

Two Dask facts this design is built on, both measured against
`distributed==2026.7.1` rather than assumed:

* Constructing a second `Client` **silently takes the process default**. The
  pooled client is therefore always built with `set_as_default=False`, and
  nothing here ever reaches for the ambient one.
* `get_client()` inside a task returns the *worker's own* cluster — the
  readiness cluster, never the pool. Wrong answer, no error. The pool must be
  handed over explicitly, which is what the plugin does.

## What pooling gives up, and why it is not the default

Per-invocation `-R rusage[...]`, per-invocation `bkill`, per-invocation
accounting and per-invocation licence arbitration all live in the one-job-per-
invocation shape and are lost inside a pool: the farm sees N workers, not N
invocations. The `bjobs` watcher can still see the pool's *workers*, but it can
no longer tell you that a particular invocation is PEND — because that
invocation never was a job.

Route an operation to a pool when its median queue wait is a significant
fraction of its median runtime — roughly a third, as a starting rule — and its
invocations are uniform enough to share one worker shape. Below that, one job
per invocation is the better deal. That is a per-operation judgement, which is
exactly why placement is authored per operation and not per study.
"""

from __future__ import annotations

import logging
import shlex
import asyncio
import signal
import sys
import time
from typing import Any, Mapping

from hedloom_exec.lsf import SubprocessRunner
from hedloom_exec.transport import (
    Observation, SubmissionRefused, TransportError, placement_options,
)

__all__ = [
    "POOL_ATTRIBUTE",
    "POOL_OPTIONS",
    "LSFPooledTransport",
    "PooledClientPlugin",
    "install_pools",
    "remove_pools",
    "open_pools",
    "attach_pools",
    "close_pools",
    "open_pools_async",
    "attach_pools_async",
    "close_pools_async",
    "run_command",
]

POOL_ATTRIBUTE = "hedloom_pools"
"""Where a readiness worker keeps its pooled clients, keyed by placement name.

An attribute on the worker rather than a module global, because a worker is the
thing whose lifetime the client shares: `teardown` runs when the worker goes,
and nothing is left behind pointing at a scheduler that has closed.
"""

COMMAND_RESOURCE = "hedloom-command"
"""One whole-worker command reservation, independent of allocated cores."""

_INTERRUPT_TIMEOUT = 40.0


def _force_support():
    from hedloom_exec.lsf import _LIBC
    return sys.platform.startswith("linux") and _LIBC is not None


def _locate_interrupt(key, client_id, previous_worker=None, dask_scheduler=None):
    task = dask_scheduler.tasks.get(key)
    if task is None:
        location = {"state": "unregistered"}
    elif task.state in {"memory", "erred"}:
        location = {"state": "completed"}
    else:
        owner = dask_scheduler.clients.get(client_id)
        if owner is None or task.who_wants != {owner} or task.dependents:
            raise RuntimeError("pooled cancellation refused: unrelated scheduling consumers")
        location = {"state": "located", "worker":
                    task.processing_on.address if task.processing_on else None}
    if previous_worker is not None:
        location["previous_worker_present"] = previous_worker in dask_scheduler.workers
    return location


def _pause_command(key, dask_worker=None):
    """Freeze actual worker admission before distinguishing queued from entered."""
    from distributed.core import Status
    worker = dask_worker
    task = worker.state.tasks.get(key)
    if task is None:
        return {"state": "unregistered"}
    if task.state in {"memory", "error"}:
        return {"state": "completed"}
    if getattr(worker, "_hedloom_interrupt_key", None):
        # Another waiter owns this temporary admission fence. Re-locate after
        # it cancels/resumes or restarts; never overwrite its ownership marker.
        return {"state": "busy"}
    if worker.status != Status.running:
        raise RuntimeError("pooled cancellation refused: worker is already unavailable")
    worker._hedloom_interrupt_key = key
    worker._hedloom_interrupt_memory_pause_fraction = worker.memory_manager.memory_pause_fraction
    # Dask's memory monitor resumes any paused worker when RSS is low. Keep
    # this owned admission fence until withdrawal/restart is acknowledged;
    # spilling and Nanny memory termination remain active.
    worker.memory_manager.memory_pause_fraction = False
    try:
        worker.status = Status.paused  # setter delivers WorkerState PauseEvent
    except BaseException:
        worker.memory_manager.memory_pause_fraction = worker._hedloom_interrupt_memory_pause_fraction
        del worker._hedloom_interrupt_memory_pause_fraction
        del worker._hedloom_interrupt_key
        raise
    executing = {task.key for task in worker.state.executing | worker.state.long_running}
    return {"state": "paused", "worker": worker.address,
            "entered": key in executing, "executing": sorted(executing),
            "supported": _force_support()}


def _resume_command_worker(key, dask_worker=None):
    from distributed.core import Status
    if getattr(dask_worker, "_hedloom_interrupt_key", None) != key:
        return False
    dask_worker.memory_manager.memory_pause_fraction = dask_worker._hedloom_interrupt_memory_pause_fraction
    del dask_worker._hedloom_interrupt_memory_pause_fraction
    del dask_worker._hedloom_interrupt_key
    if dask_worker.status == Status.paused:
        dask_worker.status = Status.running
    return True


def _worker_released(key, dask_worker=None):
    return key not in dask_worker.state.tasks


def _resume_interrupted_worker(worker, dask_scheduler=None):
    state = dask_scheduler.workers.get(worker)
    if state is not None and state.status.name == "paused":
        dask_scheduler.handle_worker_status_change(
            status="running", worker=worker, stimulus_id="hedloom-interrupt-aborted"
        )


def _prepare_interrupt(key, client_id, worker_snapshot=None,
                       dask_scheduler=None):
    """Inspect, fence and withdraw scheduling interest in one scheduler turn.

    The scheduler handlers used here also back Dask's public cancellation and
    worker status protocol (verified on distributed 2023.9.2 and 2026.7.1).
    Doing these together prevents queued work entering between inspection and
    cancellation, or unrelated work entering the worker before its restart.
    """
    scheduler = dask_scheduler
    task = scheduler.tasks.get(key)
    if task is None:
        return {"state": "unregistered"}
    if task.state in {"memory", "erred"}:
        return {"state": "completed"}
    owner = scheduler.clients.get(client_id)
    if owner is None or task.who_wants != {owner} or task.dependents:
        raise RuntimeError("pooled cancellation refused: unrelated scheduling consumers")
    worker = task.processing_on
    address = None
    if worker is not None:
        if worker_snapshot is None:
            # Assignment may change between the location and withdrawal RPCs.
            # Retry the actual-worker checkpoint before withdrawing anything.
            return {"state": "moved"}
        if worker_snapshot["worker"] != worker.address:
            raise RuntimeError("pooled command moved after worker admission was paused")
        entered = worker_snapshot["entered"]
        supported = worker_snapshot["supported"]
        executing = set(worker_snapshot["executing"])
        if entered:
            if not supported:
                raise RuntimeError("pooled force cancellation needs verified Linux parent-death binding")
            if not worker.nanny:
                raise RuntimeError("pooled force cancellation requires a nanny-managed worker")
            if executing != {key}:
                raise RuntimeError("pooled cancellation refused: worker carries unrelated executing tasks")
        if worker.status.name not in {"running", "paused"}:
            raise RuntimeError("pooled cancellation refused: worker is already unavailable")
        address = worker.address
        scheduler.handle_worker_status_change(
            status="paused", worker=address, stimulus_id=f"hedloom-interrupt-{key}"
        )
    try:
        scheduler.client_releases_keys(
            keys=[key], client=client_id, stimulus_id=f"hedloom-interrupt-{key}"
        )
    except BaseException:
        if address:
            scheduler.handle_worker_status_change(
                status="running", worker=address, stimulus_id=f"hedloom-interrupt-aborted-{key}"
            )
        raise
    return {"state": "withdrawn", "worker": address}


def _interrupt_ack(key, worker, dask_scheduler=None):
    if key in dask_scheduler.tasks:
        return False
    if worker:
        state = dask_scheduler.workers.get(worker)
        if state is None or state.status.name != "paused":
            raise RuntimeError("pooled cancellation lost its exclusive paused worker")
    return True


async def _interrupt_command(client, future):
    """Run on the existing pooled-client loop; return only confirmed outcomes."""
    from distributed.comm.core import CommClosedError
    worker = None
    restart_started = False

    async def interrupt():
        nonlocal worker, restart_started
        while True:
            if future.done():
                return None
            # A busy/unregistered checkpoint belongs to the previous location;
            # a restart may leave this command temporarily unassigned.
            snapshot = None
            location = await client.run_on_scheduler(
                _locate_interrupt, key=future.key, client_id=client.id
            )
            if location["state"] == "completed":
                return None
            if location["state"] != "unregistered":
                worker = location["worker"]
                if worker:
                    try:
                        snapshots = await client.run(_pause_command, key=future.key,
                                                     workers=[worker])
                    except CommClosedError:
                        relocated = await client.run_on_scheduler(
                            _locate_interrupt, key=future.key, client_id=client.id,
                            previous_worker=worker,
                        )
                        if relocated["previous_worker_present"]:
                            # A lost reply may have installed our fence. Keep
                            # its address for matching-key cleanup, and retain
                            # the error instead of guessing at worker state.
                            raise
                        # Another force operation can retire this worker after
                        # location but before the checkpoint RPC. Nothing has
                        # been withdrawn here; re-inspect ownership/assignment.
                        worker = None
                        await asyncio.sleep(.02)
                        continue
                    snapshot = snapshots[worker]
                    if snapshot["state"] == "completed":
                        return None
                    if snapshot["state"] != "paused":
                        await asyncio.sleep(.02)
                        continue
                decision = await client.run_on_scheduler(
                    _prepare_interrupt, key=future.key, client_id=client.id,
                    worker_snapshot=snapshot
                )
                if decision["state"] == "completed":
                    return None
                if decision["state"] == "moved":
                    await asyncio.sleep(.02)
                    continue
                if decision["state"] != "unregistered":
                    break
                raise RuntimeError("pooled command disappeared before cancellation acknowledgement")
            # submit uses an asynchronous scheduler stream. Absence before
            # registration is not evidence that the command was cancelled.
            await asyncio.sleep(.02)
        worker = decision["worker"]
        await client.cancel(future, force=True)
        while not await client.run_on_scheduler(
            _interrupt_ack, key=future.key, worker=worker
        ):
            await asyncio.sleep(.02)
        entered = snapshot is not None and snapshot["entered"]
        if worker and not entered:
            # Scheduler assignment includes resource-constrained queued tasks.
            # Keep worker admission frozen until its free-key message arrives.
            while not (await client.run(_worker_released, key=future.key,
                                       workers=[worker]))[worker]:
                await asyncio.sleep(.02)
        if worker and entered:
            restart_started = True
            restarted = await client.restart_workers([worker], timeout=30,
                                                     raise_for_error=False)
            if restarted.get(worker) != "OK":
                raise RuntimeError(f"pooled worker restart not confirmed: {restarted!r}")
        return {"reason": "force stop requested", "worker": worker,
                "worker_restarted": bool(worker and entered)}

    try:
        return await asyncio.wait_for(interrupt(), timeout=_INTERRUPT_TIMEOUT)
    finally:
        if worker and not restart_started:
            # No intentional worker loss occurred. Worker-side resource
            # reservations still prevent overlapping commands while it drains.
            # An uncertain restart instead leaves the old worker quarantined;
            # resuming it could overlap a replacement with surviving work.
            try:
                resumed = await asyncio.wait_for(client.run(
                    _resume_command_worker, key=future.key, workers=[worker]), timeout=2)
                if resumed.get(worker):
                    await asyncio.wait_for(client.run_on_scheduler(
                        _resume_interrupted_worker, worker=worker), timeout=2)
            except Exception:
                logging.getLogger(__name__).exception(
                    "could not restore pooled worker after unconfirmed interruption"
                )

POOL_OPTIONS = (
    "cores",
    "memory_mb",
    "project",
    "queue",
    "walltime",
    "workers",
)
"""The vocabulary a pooled placement can express, closed deliberately.

Narrower than `hedloom_exec.lsf.PLACEMENT_OPTIONS`, and the omissions are the
point. `licences` and `resources` describe what *one invocation* needs, and a
pool cannot honour them: its workers are claimed once, before any invocation is
routed to them, so a per-invocation licence request would be silently ignored.
An
option this cannot express is refused rather than dropped.

`workers` is how many LSF jobs the pool holds open. It is not `max_jobs`, which
is how many invocations may be in flight against the pool at once — usually the
same number, but they are different facts and conflating them would hide a pool
that was quietly half-idle.
"""


def run_command(
    argv: list[str],
    cwd: str | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Run one command on a pooled worker and report what happened.

    This is the whole of what crosses to the farm. It is a module-level
    function on purpose: cloudpickle sends an importable function *by
    reference*, and `hedloom_run` is installed in the pooled worker's
    environment because that worker is started by the same interpreter. A
    closure would be sent by value and would work too, but by reference is
    honest about the dependency — if `hedloom-run` is not installed on the farm
    node, that should fail loudly at once rather than appear to work.

    Commands use Exec's subprocess runner, including its environment merge and
    Linux SIGKILL parent-death binding of the immediate child to this worker.
    That binding does not bind the
    batch worker to the Runtime: orderly pool close remains necessary, and
    abrupt submit-host loss has different guarantees.

    Failure is a recordable outcome, not an exception: a non-zero status is
    what this returns, and the caller decides what it means. Raising here would
    turn a failed piece of work into a failed *transport*, which is a different
    thing and reconciles differently.
    """

    completed = SubprocessRunner()(
        list(argv),
        cwd=cwd,
        env=dict(env) if env else None,
        parent_death_signal=signal.SIGKILL,
    )
    return {
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


async def install_pools(worker: Any, addresses: Mapping[str, str],
                        securities: Mapping[str, Any] | None = None) -> None:
    """Give one worker a client into each pool. Runs on the worker."""

    from distributed import Client

    clients = {}
    setattr(worker, POOL_ATTRIBUTE, clients)
    try:
        for pool, address in addresses.items():
            clients[pool] = Client(
                address, asynchronous=True, set_as_default=False,
                security=(securities or {}).get(pool),
            )
            await clients[pool]
    except BaseException:
        await remove_pools(worker)
        raise


async def remove_pools(worker: Any) -> None:
    """Give the clients back when the worker goes. Runs on the worker."""

    for client in (getattr(worker, POOL_ATTRIBUTE, None) or {}).values():
        try:
            await client.close()
        except Exception:  # pragma: no cover - teardown must not mask a result
            pass
    setattr(worker, POOL_ATTRIBUTE, {})


def PooledClientPlugin(pools: Mapping[str, str],
                       securities: Mapping[str, Any] | None = None) -> Any:
    """A `WorkerPlugin` that builds one client per pool on every worker.

    A factory rather than a class, because the base class cannot be named until
    `distributed` is imported and this module must import without it — that is
    what lets `hedloom_run.site` build a pooled transport from a profile on a
    machine that will never run one, and it is why the two hooks above are
    module functions rather than method bodies.

    Subclassing is not optional decoration: `Client.register_plugin` refuses a
    duck-typed plugin outright ("Registering duck-typed plugins is not
    allowed"), so a structural stand-in fails at registration.

    `setup` runs in the worker's main thread, which is the documented place for
    a singleton that cannot be pickled — and the reason the transport itself
    can stay plain data.
    """

    from distributed import WorkerPlugin

    class _PooledClientPlugin(WorkerPlugin):
        # Stable, so re-registering replaces rather than accumulates.
        name = "hedloom-pools"

        def __init__(self, addresses: Mapping[str, str]) -> None:
            # Addresses, not clients: this object is itself shipped to every
            # worker, and a client cannot survive that.
            self._addresses = dict(addresses)
            # Only the in-process readiness workers receive these credentials.
            # They never become a transport, command bundle or saved setting.
            self._securities = dict(securities or {})

        async def setup(self, worker: Any) -> None:
            await install_pools(worker, self._addresses, self._securities)

        async def teardown(self, worker: Any) -> None:
            await remove_pools(worker)

    return _PooledClientPlugin(pools)


class LSFPooledTransport:
    """Ship one invocation's argv to a pool and wait for what it did.

    Plain data, so it ships to a readiness worker like any other transport. The
    live client is found on the worker; see this module's docstring for why it
    cannot be held here.
    """

    name = "lsf-pooled"

    discovery_is_authoritative = False
    """A pooled attempt cannot be found again, and this says so.

    A direct `bsub -I` job carries the record's try name as its job name, so
    `bjobs -J` gives a trustworthy negative: LSF is a durable third party that
    outlives the submitter. A pooled invocation is a Dask future on a scheduler
    this process owns. If the submitter dies, the future dies with it and
    nothing on the farm remembers the invocation — the workers are still there,
    but they were never told which attempt they were serving.

    So a `None` from `discover` means "cannot ask", not "nothing was accepted",
    and `hedloom_exec.attempt` must refuse to guess rather than risk running
    the same work twice. That is a real cost of pooling and it is recorded here
    rather than papered over.

    A TLA+ check of the attempt protocol on this substrate (`MCPooled.cfg`,
    2026-08-17) says that refusal is not a formality: with it, every invariant
    holds; deny it and the same configuration reproduces `MCDetached`, where a
    caller crashing mid-submission leaves its work running and the next caller
    starts a second copy. Neither discovery nor owner-bound lifetime is
    protecting a pooled attempt — **refusing to guess is the only thing that
    is**. Declaring this `True` to make recovery smoother would not degrade the
    guarantee, it would remove it.

    The price is recoverability, and it is worth saying plainly: a pooled
    invocation caught in the crash window is *permanently* unrecoverable. Its
    phase stays `intended`, so every later attempt raises again and no rerun
    gets past it without someone editing the journal. A direct placement
    recovers there, because its job died with its client and "not found" is the
    truth. Pooling buys throughput with recoverability.
    """

    def __init__(self, pool: str, *, settings: Mapping[str, Any] | None = None) -> None:
        self.pool = pool
        self.settings = dict(settings or {})

    def _client(self) -> Any:
        """The pooled client this worker was given, or a refusal that explains.

        Every failure here is established before anything could have been
        accepted, so all of them are `SubmissionRefused` rather than an
        indeterminate `TransportError`: holding an attempt open in the crash
        window over a missing plugin would be wrong.
        """

        try:
            from distributed import get_worker
        except ImportError as error:  # pragma: no cover - guarded by the extra
            raise SubmissionRefused(
                "pooled placement needs distributed; install hedloom-run[pooled]"
            ) from error

        try:
            worker = get_worker()
        except ValueError as error:
            raise SubmissionRefused(
                f"placement {self.pool!r} is pooled, and pooled work runs only "
                "on a Runtime placement worker: it reaches its pool through "
                "that worker's client, and there is no worker here. Submit "
                "through hedloom.runtime(...), or use a direct placement."
            ) from error

        client = (getattr(worker, POOL_ATTRIBUTE, None) or {}).get(self.pool)
        if client is None:
            raise SubmissionRefused(
                f"this worker holds no client for pool {self.pool!r}. The pool "
                "is opened beside the readiness cluster and handed to every "
                "worker by PooledClientPlugin; a run that built its own cluster "
                "has the workers but not the plugin. Build the cluster with "
                "hedloom.runtime(...), which opens both."
            )
        return client

    def submit(self, identity: str, bundle: Mapping[str, Any]) -> Mapping[str, Any]:
        """Send the command to the pool and block until it is done.

        Blocking is the design, not an omission. A pooled invocation holds a
        readiness thread for its whole wait, exactly as a `bsub -I` waiter
        does; what it no longer holds is a *process*. Returning early would
        mean the attempt record could not say what happened, and reconciling it
        later would need a durable handle the pool cannot give.
        """

        command = bundle.get("command")
        if not command:
            raise SubmissionRefused(
                "a pooled bundle needs a 'command' list; external work is a "
                "command line, not an in-process callable"
            )

        self._validate_requirements(bundle)
        client = self._client()
        workdir = bundle.get("workdir") or bundle.get("cwd")
        future = client.submit(
            run_command,
            list(command),
            cwd=workdir,
            env=bundle.get("env"),
            # Never deduplicated by Dask. Two invocations with identical argv
            # are still two attempts with two records, and letting the
            # scheduler collapse them would make one record describe work it
            # did not cause. Reuse is hedloom's decision, taken on the input
            # digest, and it has already been taken by the time we are here.
            pure=False,
            key=f"pooled-{identity}",
            retries=0,
            resources={COMMAND_RESOURCE: 1},
            priority=(bundle.get("scheduling") or {}).get("priority", 0),
            fifo_timeout="0 ms",
        )
        try:
            execution = bundle.get("_execution_handle")
            cancelled = None
            if execution is None:
                result = future.result()
            else:
                while True:
                    if not future.done() and execution.interrupt_requested():
                        cancelled = client.sync(_interrupt_command, client, future,
                                                callback_timeout=_INTERRUPT_TIMEOUT + 5)
                        if cancelled is not None:
                            break
                    if future.done():
                        result = future.result()
                        break
                    time.sleep(.1)
        except Exception as error:
            # The pool could not be reached or the worker died: indeterminate,
            # not a refusal. The command may or may not have run.
            raise TransportError(
                f"pooled execution on {self.pool!r} did not return a result "
                f"({type(error).__name__}: {error})"
            ) from error

        completed = {
            "transport": self.name,
            "identity": identity,
            "kind": "completed",
            "pool": self.pool,
            "workdir": workdir,
            # What the pool was asked for, as data. A pooled invocation cannot say
            # what *it* asked LSF for — it asked for nothing, the pool did — so
            # the record carries the pool's shape instead of an invented one.
            "settings": dict(self.settings),
            "command": shlex.join(list(command)),
        }
        if cancelled is not None:
            completed["cancellation"] = cancelled
        else:
            completed.update(result)
        return completed

    def _validate_requirements(self, bundle: Mapping[str, Any]) -> None:
        """Refuse demands the already allocated worker cannot express.

        A command reserves a whole worker; this bounds aggregate CPU/memory
        requests without introducing an invocation-level licence arbiter.
        These are scheduling reservations, not OS resource enforcement.
        """
        for name, value in placement_options(bundle).items():
            if name in ("cores", "memory_mb"):
                capacity = int(self.settings.get(name) or (1 if name == "cores" else 1000))
                if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                    raise SubmissionRefused(f"pooled {name} must be a positive integer")
                if value <= capacity:
                    continue
                raise SubmissionRefused(
                    f"pool {self.pool!r} allocates {name}={capacity} per worker; "
                    f"this invocation requests {value}. Use a larger pool or direct placement."
                )
            if name in ("project", "queue", "walltime") and value == self.settings.get(name):
                continue
            raise SubmissionRefused(
                f"pool {self.pool!r} cannot honour per-invocation {name}={value!r}; "
                "workers are allocated before commands. Use a matching pool or direct placement."
            )

    def discover(self, identity: str) -> Mapping[str, Any] | None:
        """Always `None`, which callers may not read as "nothing was accepted".

        See `discovery_is_authoritative`. Answering anything else would be an
        invention: there is nothing to ask.
        """

        return None

    def poll(self, handle: Mapping[str, Any]) -> Observation:
        """Read a completed handle. There is no other kind."""

        if "cancellation" in handle:
            return Observation("cancelled", dict(handle["cancellation"]))
        if "returncode" not in handle:
            return Observation("absent")
        returncode = handle["returncode"]
        if returncode == 0:
            return Observation("succeeded", {"stdout": handle.get("stdout", "")})
        return Observation(
            "failed",
            {
                "returncode": returncode,
                "stdout": handle.get("stdout", ""),
                "stderr": handle.get("stderr", ""),
                "error": f"pooled command exited with status {returncode}",
            },
        )

    def cancel(self, handle: Mapping[str, Any]) -> None:
        """Nothing to cancel: `submit` only returns once the work is over.

        Runtime force-stop intent is read by the submit-host waiter while its
        Future is live. Confirmed cancellation is returned as a completed
        handle and reconciled normally; there is nothing left to interrupt at
        this terminal transport boundary.
        """

        return None


def _scheduler_exposure(dashboard: str) -> dict[str, Any]:
    """Diagnostic HTTP exposure is independent of authenticated pool RPC.

    Disabling Bokeh alone still leaves unauthenticated HTTP routes, including
    logs, information and proxy routes. The accompanying scheduler class below
    suppresses that listener entirely for ``none``. Loopback/network explicitly
    expose diagnostics; Jupyter execution is disabled for every owned scheduler.
    """

    if dashboard == "none":
        return {"dashboard": False, "dashboard_address": None}
    if dashboard == "loopback":
        return {"dashboard_address": "127.0.0.1:0"}
    return {}


def _pool_scheduler_class(dashboard: str) -> dict[str, Any]:
    if dashboard == "none":
        from distributed import Scheduler
        from .cluster import _silent
        return {"scheduler_cls": _silent(Scheduler)}
    return {}


def open_pools(site: Any) -> dict[str, Any]:
    """Start one `LSFCluster` for each pooled placement this site declares.

    Returns `{}` when the site declares none, so a caller can always call it
    and a study that pools nothing pays nothing — including not importing
    `dask_jobqueue`.

    One cluster per pool rather than one for all of them, because an
    `LSFCluster` has exactly one worker shape. Two operations that need
    different memory are two pools, which is the register's mixed-topology row
    and the reason "not wholly pooled" is a design constraint rather than a
    preference.
    """

    pooled = {
        name: options
        for name, options in site.placements.items()
        if isinstance(options, Mapping) and options.get("kind") == "lsf-pooled"
    }
    if not pooled:
        return {}

    try:
        from dask_jobqueue import LSFCluster
    except ImportError as error:
        raise TransportError(
            f"placement {', '.join(sorted(pooled))} is pooled, which needs "
            "dask-jobqueue: install hedloom-run[pooled]"
        ) from error

    from ._pool_security import PoolCredentials, owned_cluster_type

    # Farm workers need a reachable network channel. TLS authenticates it by
    # default; explicit authentication=none selects unprotected TCP. Diagnostic
    # HTTP policy remains separate from this command channel.
    clusters: dict[str, Any] = {}
    scheduler_options = _scheduler_exposure(getattr(site, "dashboard", "none"))
    try:
        for name, options in pooled.items():
            cores = int(options.get("cores") or 1)
            memory_mb = int(options.get("memory_mb") or 1000)
            credentials = PoolCredentials(site.records_dir, options.get("authentication", "tls"))
            try:
                cluster = owned_cluster_type(LSFCluster, credentials)(
                    security=credentials.security,
                    protocol=credentials.protocol,
                    shared_temp_directory=str(credentials.directory) if credentials.directory else None,
                    queue=options.get("queue"),
                    project=options.get("project"),
                    cores=cores,
                    # dask-jobqueue takes a size, and the profile speaks megabytes
                    # because that is what a site's LSF limits are written in.
                    memory=f"{memory_mb}MB",
                    walltime=str(options.get("walltime") or "1:00"),
                    processes=1,
                    worker_extra_args=["--resources", f"{COMMAND_RESOURCE}=1"],
                    n_workers=0,
                    # Louder than dask-jobqueue's default, not quieter. It silences
                    # a `JobQueueCluster` at ERROR, which takes the pool's warnings
                    # with it — and a farm worker that dies, a job that is killed
                    # for memory, or a scheduler that loses a comm are all reported
                    # at WARNING. A pool has no other way to say those things.
                    #
                    # This is the same level `spec_cluster` asks for, so a study
                    # that spans both hears both on the same terms rather than
                    # having one half quietly hold things back.
                    silence_logs=logging.WARNING,
                    scheduler_options={**scheduler_options, "jupyter": False},
                    **credentials.worker_options(),
                    **_pool_scheduler_class(getattr(site, "dashboard", "none")),
                )
            except BaseException:
                credentials.close()
                raise
            clusters[name] = cluster
            cluster.scale(jobs=int(options.get("workers", options["max_jobs"])))
    except BaseException:
        # A pool that half-started still holds farm jobs. Give them back before
        # the exception leaves, or a failed run leaves workers on the queue.
        close_pools(clusters)
        raise
    return clusters


def attach_pools(client: Any, pools: Mapping[str, Any]) -> None:
    """Give every readiness worker a client into each pool.

    Registered on the *readiness* client, because that is where the invocations
    run. `register_plugin` also applies to workers that join later, so this is
    safe to call once at the start of a session.
    """

    if not pools:
        return
    client.register_plugin(
        PooledClientPlugin(
            {name: cluster.scheduler_address for name, cluster in pools.items()},
            {name: cluster.security for name, cluster in pools.items()},
        )
    )


def close_pools(pools: Mapping[str, Any]) -> None:
    """Close every pool, and let no single failure strand the others.

    Order matters at the call site rather than here: the readiness cluster must
    close *first*, because its workers hold clients into these pools. Closing a
    pool out from under a live client leaves that client reconnecting to a dead
    scheduler, which fills the log with cancellations that read as a failure
    and are not one.
    """

    for cluster in list(pools.values()):
        try:
            cluster.close()
        except Exception:  # pragma: no cover - teardown must not mask a result
            pass


async def open_pools_async(site: Any) -> dict[str, Any]:
    """Start pooled schedulers on this loop, with one command per farm worker.

    Scaling submits batch workers asynchronously. No wait-for-workers barrier
    holds up local placements while a farm queue is pending.
    """
    pooled = {
        name: options for name, options in site.placements.items()
        if isinstance(options, Mapping) and options.get("kind") == "lsf-pooled"
    }
    if not pooled:
        return {}
    try:
        from dask_jobqueue import LSFCluster
    except ImportError as error:
        raise TransportError(
            "pooled placement needs dask-jobqueue: install hedloom-run[pooled]"
        ) from error
    from ._pool_security import PoolCredentials, owned_cluster_type
    clusters: dict[str, Any] = {}
    try:
        for name, options in pooled.items():
            credentials = PoolCredentials(site.records_dir, options.get("authentication", "tls"))
            try:
                cluster = owned_cluster_type(LSFCluster, credentials)(
                    security=credentials.security,
                    protocol=credentials.protocol,
                    shared_temp_directory=str(credentials.directory) if credentials.directory else None,
                    asynchronous=True,
                    queue=options.get("queue"),
                    project=options.get("project"),
                    cores=int(options.get("cores") or 1),
                    memory=f"{int(options.get('memory_mb') or 1000)}MB",
                    walltime=str(options.get("walltime") or "1:00"),
                    processes=1,
                    n_workers=0,
                    worker_extra_args=["--resources", f"{COMMAND_RESOURCE}=1"],
                    silence_logs=logging.WARNING,
                    scheduler_options={**_scheduler_exposure(getattr(site, "dashboard", "none")),
                                       "jupyter": False},
                    **credentials.worker_options(),
                    **_pool_scheduler_class(getattr(site, "dashboard", "none")),
                )
            except BaseException:
                credentials.close()
                raise
            clusters[name] = cluster
            await cluster
            cluster.scale(jobs=int(options.get("workers", options["max_jobs"])))
    except BaseException:
        try:
            await close_pools_async(clusters)
        except Exception:
            logging.getLogger(__name__).warning("pool startup cleanup failed", exc_info=True)
        raise
    return clusters


async def attach_pools_async(client: Any, pools: Mapping[str, Any]) -> None:
    """Install pool clients on placement workers without blocking their loop."""
    if pools:
        await client.register_plugin(PooledClientPlugin({
            name: cluster.scheduler_address for name, cluster in pools.items()
        }, {name: cluster.security for name, cluster in pools.items()}))


async def close_pools_async(pools: Mapping[str, Any]) -> None:
    """Close each owned pool after local worker plugins have released clients."""
    errors = []
    for cluster in list(pools.values()):
        try:
            await cluster.close()
        except Exception as error:
            errors.append(error)
    if errors:
        raise TransportError(f"could not close {len(errors)} pool(s): {errors[0]}") from errors[0]
