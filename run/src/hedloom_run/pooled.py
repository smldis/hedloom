"""Pooled LSF placement: many invocations over a few reusable farm workers.

The direct placement submits one `bsub -I` per invocation, which is what makes
a corner individually visible, individually cancellable, and individually
accounted. It costs one queue dispatch and one live `bsub` client *process* on
the submit host per invocation in flight, and
`docs/concurrency-two-workers-2026-08-15.md` §9 found that process count, not
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

Design (i) of `docs/pooled-placement-plan.md` §2: **the command only**. Identity,
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

Per-corner `-R rusage[...]`, per-corner `bkill`, per-corner accounting and
per-corner licence arbitration all live in the one-job-per-corner shape and are
lost inside a pool: the farm sees N workers, not N invocations. The `bjobs`
watcher can still see the pool's *workers*, but it can no longer tell you that
a particular corner is PEND — because that corner never was a job.

Route an operation to a pool when its median queue wait is a significant
fraction of its median runtime — roughly a third, as a starting rule — and its
corners are uniform enough to share one worker shape. Below that, one job per
corner is the better deal. That is a per-operation judgement, which is exactly
why placement is authored per operation and not per study.
"""

from __future__ import annotations

import shlex
import subprocess
from typing import Any, Mapping

from hedloom_exec.transport import Observation, SubmissionRefused, TransportError

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
    "run_command",
]

POOL_ATTRIBUTE = "hedloom_pools"
"""Where a readiness worker keeps its pooled clients, keyed by placement name.

An attribute on the worker rather than a module global, because a worker is the
thing whose lifetime the client shares: `teardown` runs when the worker goes,
and nothing is left behind pointing at a scheduler that has closed.
"""

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
routed to them, so a per-corner licence request would be silently ignored. An
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

    Failure is a recordable outcome, not an exception: a non-zero status is
    what this returns, and the caller decides what it means. Raising here would
    turn a failed simulation into a failed *transport*, which is a different
    thing and reconciles differently.
    """

    completed = subprocess.run(
        list(argv),
        cwd=cwd,
        env=dict(env) if env else None,
        capture_output=True,
        text=True,
    )
    return {
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def install_pools(worker: Any, addresses: Mapping[str, str]) -> None:
    """Give one worker a client into each pool. Runs on the worker."""

    from distributed import Client

    clients = {}
    for pool, address in addresses.items():
        # Never the process default: a second default silently displaces the
        # first, and this worker already holds one for its own cluster.
        clients[pool] = Client(address, set_as_default=False)
    setattr(worker, POOL_ATTRIBUTE, clients)


def remove_pools(worker: Any) -> None:
    """Give the clients back when the worker goes. Runs on the worker."""

    for client in (getattr(worker, POOL_ATTRIBUTE, None) or {}).values():
        try:
            client.close()
        except Exception:  # pragma: no cover - teardown must not mask a result
            pass
    setattr(worker, POOL_ATTRIBUTE, {})


def PooledClientPlugin(pools: Mapping[str, str]) -> Any:
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

        def setup(self, worker: Any) -> None:
            install_pools(worker, self._addresses)

        def teardown(self, worker: Any) -> None:
            remove_pools(worker)

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

    A direct `bsub -I` job carries the attempt identity as its job name, so
    `bjobs -J` gives a trustworthy negative: LSF is a durable third party that
    outlives the submitter. A pooled invocation is a Dask future on a scheduler
    this process owns. If the submitter dies, the future dies with it and
    nothing on the farm remembers the invocation — the workers are still there,
    but they were never told which attempt they were serving.

    So a `None` from `discover` means "cannot ask", not "nothing was accepted",
    and `hedloom_exec.attempt` must refuse to guess rather than risk running
    the same work twice. That is a real cost of pooling and it is recorded here
    rather than papered over.
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
                "on the graph kernel: it reaches its pool through a client that "
                "a Dask worker holds, and there is no worker here. The "
                "sequential kernel walks the plan in this thread and has no "
                "pool to reach, so this invocation cannot run there. Use "
                "hedloom.session(...) without sequential=True, or give this "
                "operation a direct placement."
            ) from error

        client = (getattr(worker, POOL_ATTRIBUTE, None) or {}).get(self.pool)
        if client is None:
            raise SubmissionRefused(
                f"this worker holds no client for pool {self.pool!r}. The pool "
                "is opened beside the readiness cluster and handed to every "
                "worker by PooledClientPlugin; a run that built its own cluster "
                "has the workers but not the plugin. Build the cluster with "
                "hedloom.session(...), which opens both."
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
        )
        try:
            result = future.result()
        except Exception as error:
            # The pool could not be reached or the worker died: indeterminate,
            # not a refusal. The command may or may not have run.
            raise TransportError(
                f"pooled execution on {self.pool!r} did not return a result "
                f"({type(error).__name__}: {error})"
            ) from error

        return {
            "transport": self.name,
            "identity": identity,
            "kind": "completed",
            "pool": self.pool,
            "workdir": workdir,
            # What the pool was asked for, as data. A pooled corner cannot say
            # what *it* asked LSF for — it asked for nothing, the pool did — so
            # the record carries the pool's shape instead of an invented one.
            "settings": dict(self.settings),
            "command": shlex.join(list(command)),
            "returncode": result["returncode"],
            "stdout": result["stdout"],
            "stderr": result["stderr"],
        }

    def discover(self, identity: str) -> Mapping[str, Any] | None:
        """Always `None`, which callers may not read as "nothing was accepted".

        See `discovery_is_authoritative`. Answering anything else would be an
        invention: there is nothing to ask.
        """

        return None

    def poll(self, handle: Mapping[str, Any]) -> Observation:
        """Read a completed handle. There is no other kind."""

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

        Recording the intent remains the caller's job, as it is for any
        transport whose work is already terminal by the time anyone could ask.
        Cancelling a pool's *workers* is a different act — it stops every
        invocation in flight, not this one — and belongs to whoever opened the
        pool.
        """

        return None


def _scheduler_exposure(dashboard: str) -> dict[str, Any]:
    """How much of a pool's scheduler this site is willing to publish.

    Only the diagnostic HTTP server. A pool's *comm* address must stay
    network-reachable whatever this says, because that is how its workers get
    home; closing the dashboard cannot strand them, and no farm worker ever
    connects to it.

    `"none"` matters for more than exposure, and it is worth being exact about
    what it does. A `Scheduler` starts an HTTP server unconditionally, so this
    does **not** leave a pool silent the way `hedloom_run.cluster`'s `_silent`
    subclass leaves the readiness cluster silent — the listener remains, serving
    `/health` and `/metrics`. What `dashboard: False` skips is installing the
    *bokeh* routes.

    That is the part that matters here. Loading them needs bokeh, and an
    installation whose bokeh is missing or mismatched fails inside
    `distributed.dashboard.scheduler` with an `AttributeError` naming neither
    bokeh nor the dashboard. A site that already chose `dashboard = "none"` for
    its readiness cluster — often for exactly that reason — would otherwise meet
    the same failure again the moment it declared a pool, raised by a second
    scheduler it never asked for and cannot see.

    Closing the pool's listener outright would mean substituting the scheduler
    class, which `dask_jobqueue` does not take as data the way `SpecCluster`
    does. It is left open deliberately: a pool must be reachable from the farm
    in any case, so the socket is not the exposure the readiness cluster's is.

    `"network"` passes nothing, so the pool is indistinguishable from a plain
    `LSFCluster`. Note that Dask's default dashboard port is 8787 for every
    scheduler, so a site with several pools will see it taken and moved, with a
    warning — harmless, and not worth choosing a port on the site's behalf.
    """

    if dashboard == "none":
        return {"dashboard": False, "dashboard_address": None}
    if dashboard == "loopback":
        return {"dashboard_address": "127.0.0.1:0"}
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

    # Deliberately no `protocol=` and no loopback *comm* binding, and this is
    # the one place where copying the readiness cluster would be actively
    # wrong. That cluster is `inproc` because its scheduler and workers are
    # objects in one process. A pool's workers are on farm nodes: they reach
    # this scheduler over the network, so it must listen on an address they can
    # reach. TCP (or TLS) is the only correct answer here.
    #
    # The *dashboard* is a different listener and does transfer, which is why
    # `dashboard` is read below: no farm worker ever connects to it, so
    # restricting or closing it cannot strand a pool. That distinction is the
    # whole of §4's dashboard row — the comm channel must be reachable, the
    # diagnostic HTTP server need not be.
    clusters: dict[str, Any] = {}
    scheduler_options = _scheduler_exposure(getattr(site, "dashboard", "network"))
    try:
        for name, options in pooled.items():
            cores = int(options.get("cores") or 1)
            memory_mb = int(options.get("memory_mb") or 1000)
            cluster = LSFCluster(
                queue=options.get("queue"),
                project=options.get("project"),
                cores=cores,
                # dask-jobqueue takes a size, and the profile speaks megabytes
                # because that is what a site's LSF limits are written in.
                memory=f"{memory_mb}MB",
                walltime=str(options.get("walltime") or "1:00"),
                processes=1,
                n_workers=0,
                scheduler_options=dict(scheduler_options),
            )
            clusters[name] = cluster
            cluster.scale(jobs=int(options.get("workers") or options["max_jobs"]))
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
            {name: cluster.scheduler_address for name, cluster in pools.items()}
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
