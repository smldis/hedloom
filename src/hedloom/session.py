"""One live connection to a site's compute, for as long as runs need it.

A study author should not have to hold a cluster, a client, a scheduler address
and a close order in their head to run a sweep. Before this they did: build the
cluster with `cluster_for` and no other way, wrap a `Client` around it, submit
inside that, and unwind both in the right order — five of the six concepts in
the call belonging to Dask, each with a failure mode that looks like something
else. A cluster of the wrong shape does not raise; it waits forever against an
idle farm.

What is deliberately *not* hidden is the lifetime. Leaving the block ends the
runs inside it, and under owner-bound lifetime that takes their farm jobs with
them. That is a real fact about running work here, so it keeps a real shape:

    with session(site) as farm:
        first = farm.submit(subject)

Everything else the block owns — the cluster, the client, one status watcher for
the root rather than one per run — is machinery, and machinery is what a front
door is for hiding.

A session does not choose what a run *means*. It may spend less of the farm,
reach another queue, expose no dashboard, or run the whole thing in this process
for debugging, and an attempt identity is the same under all of them; see
`Site.overridden`. Which kernel decides readiness is likewise not a semantic
choice — `hedloom_run.binding` states that as the invariant both kernels are
tested against — so `sequential=True` is a statement about how much runs at
once, not about what comes back.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping

from hedloom_run.driver import InvocationOutcome
from hedloom_run.site import Site, SiteError

__all__ = ["Session", "session"]

_WATCH_JOIN_SECONDS = 1.0


def _printer(label: str | None) -> Callable[[InvocationOutcome], None]:
    """Report outcomes as they land, named so concurrent runs stay legible."""

    prefix = f"[{label}] " if label else ""

    def report(outcome: InvocationOutcome) -> None:
        name = outcome.authored_key or outcome.invocation_id
        disposition = "reused" if outcome.reused else outcome.disposition
        detail = f"  {outcome.error}" if outcome.error else ""
        print(f"{prefix}[{disposition:>9}] {name:<34}{outcome.outcome}{detail}")

    return report


def _client_class(site: Site) -> Any:
    """Dask's client, or a refusal that names both ways out.

    `distributed` is an optional extra, and staying optional is the point: a
    site that never runs more than one invocation at a time has no use for a
    scheduler. What it must not do is quietly downgrade — a sweep that declared
    eight concurrent jobs and silently ran them one at a time would look like a
    slow farm rather than a missing dependency.
    """

    try:
        from distributed import Client
    except ModuleNotFoundError as error:  # pragma: no cover - needs the extra gone
        declared = max(site.capacity.values(), default=1)
        raise SiteError(
            f"this session needs a scheduler: the site declares capacity for "
            f"{declared} concurrent invocation(s) and `distributed` is not "
            f"installed. Either install it (`pip install hedloom-run[dask]`), or "
            f"ask for one at a time with session(site, sequential=True) — which "
            f"costs the concurrency and nothing else, since both kernels produce "
            f"the same results under the same identities."
        ) from error
    return Client


class Session:
    """The cluster, the client and the watcher a set of runs shares.

    Built by `session(...)`; not useful outside a `with` block, because what it
    owns has to be released and the release is visible on purpose.
    """

    def __init__(
        self,
        site: Site,
        *,
        sequential: bool = False,
        watch: bool = False,
        as_default: bool = False,
        watch_reader: Any = None,
    ) -> None:
        self.site = site
        self.sequential = sequential
        self.watch = watch
        self.as_default = as_default
        # Keeps the scheduler boundary explicit in tests, as it does in
        # `start_watcher` itself; nothing in a real run supplies it.
        self.watch_reader = watch_reader
        self._cluster: Any = None
        self._client: Any = None
        self._pools: dict[str, Any] = {}
        self._watcher: Any = None

    @property
    def client(self) -> Any:
        """The Dask client this session holds, or None when sequential.

        Present for the run that genuinely needs to reach past this seam. Using
        it is not the intended path and nothing here depends on it.
        """

        return self._client

    def __enter__(self) -> "Session":
        from hedloom.study import start_watcher

        try:
            if self.watch:
                # One watcher for the root, not one per run: two concurrent runs
                # sharing a root would otherwise report every attempt twice.
                self._watcher = start_watcher(self.site.root, self.watch_reader)
            if not self.sequential:
                from hedloom_run.cluster import cluster_for

                client = _client_class(self.site)
                self._cluster = cluster_for(self.site)
                # Not the process default. A `Client` that takes that role
                # rewrites `dask.config`'s scheduler for everything in this
                # process and restores it on close, which two clients whose
                # lifetimes interleave get wrong — leaving a later bare
                # `dask.compute` dialling a scheduler that has gone. hedloom's
                # cluster is hedloom's; a caller's own `dask.compute` should
                # keep meaning what it meant before they imported this.
                self._client = client(
                    self._cluster.scheduler_address, set_as_default=self.as_default
                )
                # A pooled placement needs a second cluster, whose workers are
                # LSF jobs, and a client into it on every readiness worker. Both
                # are opened here rather than by the kernel, for the same reason
                # the readiness cluster is: how much farm a study may hold open
                # is an operational decision with a lifetime, and this block is
                # what owns lifetimes. A site declaring no pool opens none and
                # does not import dask-jobqueue.
                from hedloom_run.pooled import attach_pools, open_pools

                self._pools = open_pools(self.site)
                attach_pools(self._client, self._pools)
        except BaseException:
            self._release()
            raise
        return self

    def __exit__(self, *exception: Any) -> bool:
        self._release()
        return False

    def _release(self) -> None:
        """Give back everything held, in the order that makes each safe."""

        try:
            if self._client is not None:
                self._client.close()
                self._client = None
            if self._cluster is not None:
                self._cluster.close()
                self._cluster = None
            if self._pools:
                # Last, and the order is load-bearing rather than tidy. Every
                # readiness worker holds a client into these pools; closing a
                # pool while one is live leaves it reconnecting to a scheduler
                # that has gone, which fills the log with cancellations that
                # read as a failure and are not one. The readiness cluster
                # above takes those clients with it when it closes.
                from hedloom_run.pooled import close_pools

                close_pools(self._pools)
                self._pools = {}
        finally:
            if self._watcher is not None:
                stop, thread = self._watcher
                stop.set()
                # Status is evidence about a run, never part of one: a wedged
                # scheduler query cannot keep the caller here.
                thread.join(timeout=_WATCH_JOIN_SECONDS)
                self._watcher = None

    def submit(
        self,
        study: Any,
        *,
        stop_on_failure: bool = True,
        on_event: Callable[[InvocationOutcome], None] | None = None,
        label: str | None = None,
    ) -> Any:
        """Run one study on this session's compute."""

        return study._run(
            site=self.site,
            client=self._client,
            stop_on_failure=stop_on_failure,
            on_event=on_event or (_printer(label) if self.watch else None),
        )

    def submit_all(
        self,
        studies: Mapping[str, Any],
        *,
        stop_on_failure: bool = True,
        on_event: Callable[[InvocationOutcome], None] | None = None,
    ) -> dict[str, Any]:
        """Run several studies against one cluster, and therefore one budget.

        This is the shape that makes the shared budget structural. Each study is
        its own graph with its own report, but the placement capacity they draw
        on belongs to the session's workers, so two studies cannot between them
        put more on the farm than the site declared.

        Sequentially when this session is sequential — one at a time is what was
        asked for, and running the studies in threads instead would put as many
        invocations in flight as there are studies, quietly exceeding the
        capacity that `sequential=True` promised.

        Returns a report per label. An exception from any study is raised once
        the others have finished, carrying `runs` with whatever did complete,
        for the same reason the graph kernel attaches a partial report.
        """

        runs: dict[str, Any] = {}
        if self.sequential:
            for label, subject in studies.items():
                runs[label] = self.submit(
                    subject,
                    stop_on_failure=stop_on_failure,
                    on_event=on_event,
                    label=label,
                )
            return runs

        from threading import Thread

        failures: dict[str, BaseException] = {}

        def one(label: str) -> None:
            try:
                runs[label] = self.submit(
                    studies[label],
                    stop_on_failure=stop_on_failure,
                    on_event=on_event,
                    label=label,
                )
            except BaseException as error:  # noqa: BLE001 - re-raised below
                failures[label] = error

        threads = [Thread(target=one, args=(label,)) for label in studies]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        if failures:
            label, error = next(iter(failures.items()))
            error.runs = dict(runs)
            error.failures = dict(failures)
            raise error
        return runs


def session(
    site: Site,
    override: Mapping[str, Mapping[str, Any]] | None = None,
    *,
    sequential: bool = False,
    locally: bool = False,
    watch: bool = False,
    as_default: bool = False,
    _watch_reader: Any = None,
) -> Session:
    """Open a site's compute for the duration of a `with` block.

        with session(site) as farm:                    # as the site declares
            run = farm.submit(subject)

        with session(site, {"placement": {"lsf": {"max_jobs": 1}}}) as farm:
            run = farm.submit(subject)                 # this run spends less

    ``override`` speaks the profile's own vocabulary and applies to this session
    only, so a site needs one declaration rather than one per way of running it.
    It can change how work is executed and never what it means; `Site.overridden`
    holds that argument and the refusals.

    ``sequential`` runs one invocation at a time and builds no cluster, which is
    what makes `distributed` optional. Say it rather than leave it to be inferred
    from a missing argument: a site declaring `max_jobs = 8` and quietly running
    one at a time is indistinguishable from a busy farm.

    ``locally`` is the debugging pair — every placement served by its authored
    body in this process, one at a time, no farm and no scheduler. Sugar for
    `sequential=True` with `Site.served_in_process()`, and both halves stay
    reachable on their own.

    ``watch`` polls the queue for the whole block and prints transitions once,
    however many runs happen inside it.

    ``as_default`` makes this session's client Dask's process-wide default, for a
    caller who wants their own `dask.compute` to use this cluster too. Off
    because claiming a process-wide default is the caller's decision.
    """

    if override:
        site = site.overridden(override)
    if locally:
        site = site.served_in_process()
        sequential = True
    return Session(
        site,
        sequential=sequential,
        watch=watch,
        as_default=as_default,
        watch_reader=_watch_reader,
    )
