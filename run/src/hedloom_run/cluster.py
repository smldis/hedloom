"""The cluster a site is willing to run, including what it will not expose.

`run_plan_graph` takes a client and refuses to build one, because how much a
site tolerates running at once is an operational decision. That stays true.
What was missing is the other half of the same decision: a Dask cluster opens
listening sockets, and on a shared submit host those are not private.

Two facts make this an option rather than a footnote. Both were measured
against `distributed==2026.7.1`, the version `hedloom-run[dask]` pins:

* A `Scheduler` **always** starts an HTTP server. `Scheduler.__init__` calls
  `start_http_server(...)` unconditionally; `dashboard=False` only skips
  installing the bokeh routes, leaving a listener on `:8787` still serving
  `/health`, `/metrics` and `/api`. Every `Worker` starts one of its own.
* Both bind every interface. A study on a login host therefore publishes its
  corner names, workspace paths and profiler to anything that can reach the
  host — and loopback does not help, because loopback is per host, not per
  user.

So the exposure is declared, in the profile, beside the concurrency:

    [kernel]
    threads = 32
    dashboard = "network"     # "network" | "loopback" | "none"

`"network"` is the default and is exactly what Dask does unaided: that branch
passes no address at all, so an installation that adopts this module without
declaring anything is indistinguishable from one that never used it.

The invariant:

    Exposure changes how a run can be watched and nothing about what it
    computes.

No identity, no reuse, no Plan content is touched — the same promise the choice
of kernel already makes.

**Why "none" needs a private seam.** There is no supported way to ask
`distributed` for a scheduler that does not listen, so this suppresses
`ServerNode.start_http_server` while the cluster is built. Both the scheduler
and the workers create their servers there, so one override silences all of
them, and `ServerNode` being their shared base is what makes it a single seam
rather than a sweep.

Depending on a private method is defensible only if the *behaviour* is what is
tested. `tests/test_cluster.py` asserts that a silent cluster holds no HTTP
server and that work still runs, so an upgrade that moves the seam fails on the
symptom rather than on an import.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, Mapping

from hedloom_run.site import EXPOSURES, PLACEMENT_RESOURCE, Site, SiteError

__all__ = ["EXPOSURES", "cluster_for", "local_cluster", "spec_cluster"]


def _no_http_server(
    self: Any,
    routes: Any,
    dashboard_address: Any,
    default_port: int = 0,
    ssl_options: Any = None,
) -> None:
    """Stand in for `ServerNode.start_http_server`, which always binds."""

    return None


@contextmanager
def _without_http_servers() -> Iterator[None]:
    """Hold the seam open only while a cluster is being built.

    Scoped in time rather than in space: the patch is global, but a scheduler
    and its workers create their servers during construction, so covering the
    construction covers their whole lives. Anything built afterwards — by this
    process, or by a library sharing it — listens normally.
    """

    from distributed.node import ServerNode

    original = ServerNode.start_http_server
    ServerNode.start_http_server = _no_http_server
    try:
        yield
    finally:
        ServerNode.start_http_server = original


def local_cluster(
    *,
    threads: int | None = None,
    dashboard: str = "network",
    processes: bool = False,
    placements: Mapping[str, int] | None = None,
) -> Any:
    """Build the local, threaded cluster this kernel documents.

    One worker with many threads, for the reasons `hedloom_run.graph` sets out: an
    invocation waiting on `bsub -I` costs a thread rather than anything scarce,
    nothing secedes, and no nanny may restart a worker holding live farm
    clients.

    ``threads`` left as None gives Dask's own sizing, which is this host's CPU
    count — right for a local study and wrong for a farm sweep, where the
    number is the share of the farm this study may spend and belongs in the
    profile as that placement's `max_jobs`.

    ``placements`` declares this one worker's capacity for each placement. The
    graph kernel annotates every task with the placement it resolved to and
    refuses a cluster that offers none, so a cluster built here for a study that
    runs anywhere but `local` needs the mapping. When a site has more than one
    placement, `spec_cluster` is the shape that separates them — one worker
    cannot hold two budgets apart, since its threads are a single pool.
    """

    from distributed import LocalCluster

    if dashboard not in EXPOSURES:
        raise SiteError(
            f"unknown dashboard exposure {dashboard!r}; a site may declare "
            f"{', '.join(repr(item) for item in EXPOSURES)}"
        )
    if dashboard == "none" and processes:
        # Refused rather than quietly downgraded. Workers in their own
        # processes reach the scheduler over TCP, so a silent multi-process
        # cluster cannot exist; binding anyway under a name that promises
        # silence would be the kind of plausible falsehood this project treats
        # as a defect.
        raise SiteError(
            "a multi-process cluster cannot be silent: its workers connect to "
            "the scheduler over TCP, which needs a listening socket. Run "
            "processes=False, which is the shape this kernel documents, or "
            "declare dashboard = 'loopback'"
        )

    options: dict[str, Any] = {"processes": processes, "n_workers": 1}
    if threads is not None:
        options["threads_per_worker"] = threads
    if placements:
        options["resources"] = {
            f"{PLACEMENT_RESOURCE}{name}": cap for name, cap in placements.items()
        }

    if dashboard == "network":
        # Deliberately passes no address: naming Dask's default here would
        # copy a value that is theirs to change, and this branch promises to
        # be indistinguishable from not using this module at all.
        return LocalCluster(**options)

    if dashboard == "loopback":
        # Both servers. The worker's is a second listener and defaults to every
        # interface exactly as the scheduler's does, so binding only the
        # scheduler would leave the study half exposed and look closed.
        return LocalCluster(
            **options,
            dashboard_address="127.0.0.1:0",
            worker_dashboard_address="127.0.0.1:0",
        )

    with _without_http_servers():
        # `dashboard_address=None` as well as the suppression: it is what makes
        # the scheduler skip connecting bokeh routes to an application the
        # patch never built.
        return LocalCluster(**options, dashboard_address=None)


def spec_cluster(
    workers: Mapping[str, Mapping[str, Any]], *, dashboard: str = "network"
) -> Any:
    """One worker per placement, each with its own threads and its own capacity.

    `LocalCluster` applies a single recipe to every worker, so it cannot express
    this: two workers that differ in how many threads they hold and which
    placement those threads belong to. `SpecCluster` takes a specification per
    worker instead, and `LocalCluster` is itself a subclass of it, so this is
    the same machinery written out rather than a different one.

    Why the separation is the point, and not tidiness: a task that asks for no
    resource is legal on *every* worker, and Dask will both place it and later
    steal it onto whichever worker looks least busy — which is always the one
    sized for a large farm cap. A local invocation then holds a thread that
    exists to hold a `bsub -I`, and a farm job waits behind it with its capacity
    unused. Giving every placement its own worker is what makes that
    unrepresentable, because there is no unrestricted task left to misplace.

    `cls=Worker` is deliberate: in-process, no nanny. A nanny restarting a
    worker under memory pressure would take that worker's live `bsub` clients
    with it, and a farm job dies with the thread that spawned it.
    """

    from distributed import Scheduler, SpecCluster, Worker

    if dashboard not in EXPOSURES:
        raise SiteError(
            f"unknown dashboard exposure {dashboard!r}; a site may declare "
            f"{', '.join(repr(item) for item in EXPOSURES)}"
        )
    if not workers:
        raise SiteError(
            "a cluster needs at least one placement to build a worker for"
        )

    # A bare `Scheduler` defaults to LSF-free port 8786 and would collide with
    # anything already there; `LocalCluster` passes 0 for the same reason.
    scheduler: dict[str, Any] = {"port": 0}
    worker_common: dict[str, Any] = {}

    if dashboard == "network":
        # `LocalCluster`'s own default, restated because a bare `Scheduler`
        # serves no dashboard at all unless told to. This is the one value here
        # copied from Dask rather than derived, and the behavioural test in
        # `tests/test_cluster.py` is what would notice if it drifted.
        scheduler.update(dashboard=True, dashboard_address=":8787")
    elif dashboard == "loopback":
        scheduler.update(dashboard=True, dashboard_address="127.0.0.1:0")
        # The worker's server is a second listener and binds every interface
        # exactly as the scheduler's does, so binding only the scheduler would
        # leave the study half exposed and look closed.
        worker_common.update(dashboard=True, dashboard_address="127.0.0.1:0")
    else:
        scheduler.update(dashboard=False, dashboard_address=None)

    spec = {
        name: {"cls": Worker, "options": {**worker_common, **dict(options)}}
        for name, options in workers.items()
    }
    build = lambda: SpecCluster(  # noqa: E731 - one expression, used twice
        scheduler={"cls": Scheduler, "options": scheduler},
        workers=spec,
    )

    if dashboard == "none":
        # `SpecCluster` starts the scheduler and every worker inside
        # `__init__`, so covering construction covers all of them.
        with _without_http_servers():
            return build()
    return build()


def cluster_for(site: Site, *, dashboard: str | None = None) -> Any:
    """Build the cluster this site's profile describes, and only this.

    The single supported way to get a cluster the graph kernel will accept. The
    annotation each task carries and the capacity each worker declares are both
    derived from `Site.placements` — this function is where that stops being a
    convention and becomes structural, because there is no second place to
    write either number.

    A caller who builds their own `LocalCluster` and passes it to a study is not
    wrong so much as unadmitted: their cluster declares no placement capacity,
    every task asks for some, and Dask holds all of them unscheduled forever. The
    kernel refuses that up front rather than hanging, and points back here.
    """

    return spec_cluster(
        site.cluster_spec(), dashboard=dashboard or site.dashboard
    )
