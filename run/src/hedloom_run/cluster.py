"""The cluster a site is willing to run, including what it will not expose.

`run_plan_graph` takes a client and refuses to build one, because how much a
site tolerates running at once is an operational decision. That stays true.
What was missing is the other half of the same decision: a Dask cluster opens
listening sockets, and on a shared submit host those are not private.

Two facts make this an option rather than a footnote. Both were measured
against `distributed==2026.7.1`. That was once the version `hedloom-run[dask]`
pinned; the extra now takes a floor (`>=2023.9.2`) instead, because a site does
not always get to choose its `distributed` and a hard pin turns "a version
behind" into "cannot install". The measurements below are therefore evidence
from one version rather than a guarantee about every version, and
`tests/test_cluster.py` is what holds them: it asserts what a cluster *does*,
so a release that moves this seam fails on the symptom.

* A `Scheduler` **always** starts an HTTP server. `Scheduler.__init__` calls
  `start_http_server(...)` unconditionally; `dashboard=False` only skips
  installing the bokeh routes, leaving a listener on `:8787` still serving
  `/health`, `/metrics` and `/api`. Every `Worker` starts one of its own.
* Both bind every interface. A study on a login host therefore publishes its
  invocation names, workspace paths and profiler to anything that can reach the
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

**How "none" is built.** There is no argument that asks `distributed` for a
scheduler which does not listen, but there is no need for one: `SpecCluster`
takes the scheduler and worker classes as data, so a subclass overriding
`start_http_server` gives silence that belongs to *this* cluster. Both the
scheduler and the workers bind there, so one override covers both.

This replaced suppressing that method on the shared `ServerNode` base while a
cluster was built. A patch has to be undone, which meant process-wide state, a
lock and a depth count to survive two constructions at once, and a window in
which any cluster built concurrently by anything else in the process was
silenced without asking. It was found by two silent clusters in two threads
leaving the seam open for the rest of the process. Substituting a class has none
of those properties and needs no state at all.

Overriding a method Dask does not document as an extension point is still
defensible only if the *behaviour* is what is tested. `tests/test_cluster.py`
asserts that a silent cluster holds no HTTP server, that a normal one built
beside it does, and that work still runs — so an upgrade that moves the seam
fails on the symptom rather than on an import.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping

from hedloom_run.site import EXPOSURES, PLACEMENT_RESOURCE, Site, SiteError

__all__ = ["EXPOSURES", "cluster_for", "local_cluster", "spec_cluster"]


_DASHBOARD_IMPORT = "distributed.dashboard"


def _blames_the_dashboard(error: BaseException) -> bool:
    """Whether this failure, or anything under it, is the dashboard import.

    The chain has to be walked rather than the message read, because the two
    halves of a cluster report the same cause differently. The scheduler's
    surfaces as `Cluster failed to start: module 'distributed.dashboard' has no
    attribute 'scheduler'`, which says so. A worker's surfaces as `Worker failed
    to start.` — nothing at all — with the `AttributeError` about
    `distributed.dashboard.worker` only as its `__cause__`.

    Matching on the top-level message would translate one and let the other
    through under a name that explains nothing.
    """

    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if _DASHBOARD_IMPORT in str(current):
            return True
        current = current.__cause__ or current.__context__
    return False


def _built(build: Callable[[], Any], dashboard: str) -> Any:
    """Build a cluster, and translate the one failure nobody can read.

    A scheduler that serves a dashboard imports `distributed.dashboard.scheduler`
    lazily, and that import needs bokeh. Without it — or with a bokeh that does
    not match this `distributed` — the failure surfaces as

        AttributeError: module 'distributed.dashboard' has no attribute 'scheduler'

    which names neither bokeh nor the dashboard, and arrives from a cluster the
    caller never asked to have a dashboard at all, because `"network"` is the
    default. Worse, it is *intermittent*: the import is lazy, so two clusters
    built at once can have one succeed and one fail. A study that ran yesterday
    fails today with a message about an attribute.

    Newer `distributed` degrades to a reduced status page instead of raising,
    which is why this is not caught by pinning one version and is exactly the
    kind of thing a floor rather than a pin lets a site meet. So it is
    translated here rather than left to be recognised.
    """

    try:
        return build()
    except Exception as error:
        if not _blames_the_dashboard(error):
            raise
        raise SiteError(
            f"this cluster asked for dashboard = {dashboard!r} and Dask could "
            "not build one: its dashboard needs bokeh, which is either missing "
            "or does not match the installed distributed. Dask reports this as "
            f"an attribute error on {_DASHBOARD_IMPORT}, which names neither. "
            "Either install bokeh, or declare dashboard = 'none' in the "
            "profile's [kernel] table — the choice changes how a run can be "
            "watched and nothing about what it computes."
        ) from error


def _silent(base: Any) -> Any:
    """A subclass of `base` that never starts its HTTP server.

    `ServerNode.start_http_server` is where a scheduler and every worker bind,
    so overriding it in a subclass silences both — and silences *only these*
    clusters. That is the whole difference from suppressing it on the shared
    base class, which was the first thing tried here: a patch has to be undone,
    which means process-wide state, a lock and a depth count to survive two
    constructions at once, and a window in which a cluster built concurrently by
    anything else in the process is silenced without asking.

    None of which is needed, because `SpecCluster` takes the scheduler and
    worker classes as data. Substituting a class is a supported extension point;
    the patch was a workaround for not having looked for one.
    """

    def start_http_server(self: Any, *arguments: Any, **options: Any) -> None:
        return None

    return type(f"Silent{base.__name__}", (base,), {
        "start_http_server": start_http_server,
    })


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
        return _built(lambda: LocalCluster(**options), dashboard)

    if dashboard == "loopback":
        # Both servers. The worker's is a second listener and defaults to every
        # interface exactly as the scheduler's does, so binding only the
        # scheduler would leave the study half exposed and look closed.
        return _built(
            lambda: LocalCluster(
                **options,
                dashboard_address="127.0.0.1:0",
                worker_dashboard_address="127.0.0.1:0",
            ),
            dashboard,
        )

    # `LocalCluster` hardcodes its scheduler class (`deploy/local.py`), so it
    # cannot be told to be silent. It is a `SpecCluster` underneath and this
    # branch is one worker with no processes — which `spec_cluster` expresses
    # exactly — so the silent shape is built there rather than by reaching into
    # Dask's internals to reproduce it here.
    return spec_cluster(
        {
            "local": {
                "nthreads": threads,
                "resources": (options.get("resources") or None),
            }
        },
        dashboard="none",
    )


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

    # Scheduler and workers are objects in this process, so their comm channel
    # should be too. A bare `Scheduler` otherwise defaults to TCP even when its
    # workers are in-process, opening one scheduler socket and one per worker.
    # `LocalCluster(processes=False)` selects `inproc` for this same shape.
    scheduler: dict[str, Any] = {"protocol": "inproc", "port": 0}
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

    # Silence is a property of these two classes, not of this process. See
    # `_silent`: it is why nothing here has to be undone afterwards.
    scheduler_cls = _silent(Scheduler) if dashboard == "none" else Scheduler
    worker_cls = _silent(Worker) if dashboard == "none" else Worker
    spec = {
        name: {"cls": worker_cls, "options": {**worker_common, **dict(options)}}
        for name, options in workers.items()
    }
    return _built(
        lambda: SpecCluster(
            scheduler={"cls": scheduler_cls, "options": scheduler},
            workers=spec,
        ),
        dashboard,
    )


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
