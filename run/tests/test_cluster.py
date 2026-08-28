"""What a site exposes, with silence as the default.

Two tests carry this module. `test_a_silent_cluster_holds_no_http_server` is
the one the feature exists for, and it asserts a behaviour rather than an
internal: if a future `distributed` moves the seam this module suppresses, the
failure names a cluster that opened a server, not a missing attribute.

`test_the_network_exposure_asks_dask_for_nothing` protects the explicit opt-in.
`"network"` must pass no address at all, because the moment it names one it has
taken over a default that belongs to Dask.
"""

import threading
import pytest

from hedloom_run.cluster import cluster_for, local_cluster, spec_cluster
from hedloom_run.site import Site, SiteError

distributed = pytest.importorskip("distributed")


class Recorder:
    """Stands in for `LocalCluster` to capture what it was asked for."""

    def __init__(self):
        self.kwargs = None

    def __call__(self, **kwargs):
        self.kwargs = kwargs
        return "cluster"


@pytest.fixture
def recorded(monkeypatch):
    """Intercept cluster construction: these tests are about the request."""

    recorder = Recorder()
    monkeypatch.setattr(distributed, "LocalCluster", recorder)
    return recorder


@pytest.fixture
def spec_recorded(monkeypatch):
    """The same, for the heterogeneous shape a site actually gets.

    `cluster_for` builds a `SpecCluster` because a site has one worker per
    placement and `LocalCluster` applies a single recipe to all of them.
    """

    recorder = Recorder()
    monkeypatch.setattr(distributed, "SpecCluster", recorder)
    return recorder


def test_the_network_exposure_asks_dask_for_nothing(recorded):
    local_cluster(threads=4, dashboard="network")

    assert "dashboard_address" not in recorded.kwargs
    assert "worker_dashboard_address" not in recorded.kwargs
    assert recorded.kwargs == {
        "processes": False,
        "n_workers": 1,
        "threads_per_worker": 4,
    }


def test_silence_is_what_a_site_gets_without_declaring_anything(
    tmp_path, spec_recorded
):
    site = Site(root=str(tmp_path))

    assert site.dashboard == "none"

    cluster_for(site)
    scheduler = spec_recorded.kwargs["scheduler"]["options"]
    assert scheduler["dashboard"] is False
    assert scheduler["dashboard_address"] is None
    # These workers are objects in this process. TCP would add a scheduler
    # listener and one worker listener per placement for no benefit.
    assert scheduler["protocol"] == "inproc"
    assert scheduler["port"] == 0


def test_low_level_cluster_construction_is_silent_by_default(spec_recorded):
    local_cluster()

    scheduler = spec_recorded.kwargs["scheduler"]["options"]
    assert scheduler["dashboard"] is False
    assert scheduler["dashboard_address"] is None


def test_unset_threads_leave_the_sizing_to_dask(recorded):
    local_cluster(dashboard="network")

    assert "threads_per_worker" not in recorded.kwargs


def test_loopback_binds_the_worker_too(recorded):
    local_cluster(threads=2, dashboard="loopback")

    # Two servers, so binding only the scheduler would look closed and leave
    # the study half exposed.
    assert recorded.kwargs["dashboard_address"] == "127.0.0.1:0"
    assert recorded.kwargs["worker_dashboard_address"] == "127.0.0.1:0"


def test_a_site_carries_its_exposure_to_the_cluster(tmp_path, spec_recorded):
    site = Site(root=str(tmp_path), threads=7, dashboard="loopback")

    cluster_for(site)

    assert spec_recorded.kwargs["scheduler"]["options"][
        "dashboard_address"
    ] == "127.0.0.1:0"
    worker = spec_recorded.kwargs["workers"]["local"]["options"]
    # Two servers, so binding only the scheduler would look closed.
    assert worker["dashboard_address"] == "127.0.0.1:0"
    assert worker["nthreads"] == 7


def test_an_override_beats_the_profile(tmp_path, spec_recorded):
    site = Site(root=str(tmp_path), dashboard="loopback")

    cluster_for(site, dashboard="network")

    assert spec_recorded.kwargs["scheduler"]["options"][
        "dashboard_address"
    ] == ":8787"


def test_every_placement_gets_a_worker_that_holds_only_its_own_work(
    tmp_path, spec_recorded
):
    """The point of the shape, asserted rather than assumed.

    An unannotated task is legal on every worker and gets both placed and
    stolen onto whichever looks idle — always the one sized for a big farm cap.
    Separate workers, each declaring only its own placement, is what leaves no
    unrestricted task to misplace.
    """

    site = Site(root=str(tmp_path), placements={"local": 2, "lsf": 200})

    cluster_for(site)

    workers = spec_recorded.kwargs["workers"]
    assert set(workers) == {"local", "lsf"}
    assert workers["lsf"]["options"]["resources"] == {"placement:lsf": 200}
    assert workers["local"]["options"]["resources"] == {"placement:local": 2}
    # Derived, not configured: a smaller thread count would bind first, cap the
    # farm below what the profile declared, and report nothing.
    assert workers["lsf"]["options"]["nthreads"] == 200


def test_a_multi_process_cluster_cannot_be_silent():
    # Workers in their own processes dial the scheduler over TCP, so the
    # request is impossible rather than merely awkward, and is refused.
    with pytest.raises(SiteError, match="cannot be silent"):
        local_cluster(dashboard="none", processes=True)


def test_an_unknown_exposure_is_refused():
    with pytest.raises(SiteError, match="unknown dashboard exposure"):
        local_cluster(dashboard="private")


def test_a_profile_naming_an_unknown_exposure_is_refused_at_construction(tmp_path):
    with pytest.raises(SiteError, match="not one of"):
        Site(root=str(tmp_path), dashboard="off")


def test_a_silent_cluster_holds_no_http_server():
    """The feature: no listener, and the work still runs.

    Asserted through the objects rather than through `ss`, so the test says
    what it means on any host — a scheduler and every worker without an HTTP
    server is exactly the condition that leaves nothing bound.
    """

    cluster = local_cluster(threads=2, dashboard="none")
    try:
        assert cluster.scheduler_address.startswith("inproc://")
        assert all(
            worker.address.startswith("inproc://")
            for worker in cluster.workers.values()
        )
        assert getattr(cluster.scheduler, "http_server", None) is None
        for worker in cluster.workers.values():
            assert getattr(worker, "http_server", None) is None

        with distributed.Client(cluster) as client:
            assert client.submit(lambda: 6 * 7).result() == 42
    finally:
        cluster.close()


def test_a_silent_cluster_is_still_silent_when_it_has_two_workers():
    """The seam is scoped to construction, and SpecCluster builds inside it.

    `SpecCluster.__init__` starts the scheduler and every worker before it
    returns, so suppressing the HTTP server for the duration of construction
    covers all of them. Asserted as behaviour, so an upgrade that moves the
    seam fails here rather than on an import.
    """

    cluster = spec_cluster(
        {
            "local": {"nthreads": 1, "resources": {"placement:local": 1}},
            "farm": {"nthreads": 2, "resources": {"placement:lsf": 2}},
        },
        dashboard="none",
    )
    try:
        assert cluster.scheduler_address.startswith("inproc://")
        assert all(
            worker.address.startswith("inproc://")
            for worker in cluster.workers.values()
        )
        assert getattr(cluster.scheduler, "http_server", None) is None
        for worker in cluster.workers.values():
            assert getattr(worker, "http_server", None) is None

        with distributed.Client(cluster) as client:
            assert client.submit(lambda: 6 * 7, resources={"placement:lsf": 1}).result() == 42
    finally:
        cluster.close()


def test_the_seam_is_restored_for_whatever_is_built_next():
    """Scoped in time: silencing one cluster must not silence the process."""

    silent = local_cluster(threads=1, dashboard="none")
    silent.close()

    after = local_cluster(threads=1, dashboard="loopback")
    try:
        assert after.scheduler.http_server is not None
    finally:
        after.close()


def test_silence_belongs_to_the_cluster_that_asked_for_it(tmp_path):
    """Silence must not leak to clusters built beside a silent one.

    This is what suppressing `ServerNode.start_http_server` process-wide could
    not give. Two silent constructions at once each restored the other's patch
    and left the seam open, so every later cluster was silenced without asking —
    found as an unrelated test, asserting a normal cluster has an HTTP server,
    beginning to fail. Substituting a subclass has no such window, and this
    holds it by building both kinds at the same time.
    """

    # Half of what this asserts is that a cluster built beside a silent one
    # *still listens*, and a listening cluster cannot be built at all without
    # bokeh — Dask's dashboard import fails, and under this test's concurrency
    # it fails for some of the threads and not others. Skipping is honest;
    # failing would report a missing optional dependency as a broken seam.
    pytest.importorskip("bokeh", reason="a listening cluster needs a dashboard")

    from distributed.node import ServerNode

    untouched = ServerNode.start_http_server
    site = Site(root=str(tmp_path), placements={"local": 1})
    built: list[tuple[str, object]] = []

    def build(exposure: str) -> None:
        built.append(
            (exposure, spec_cluster(site.cluster_spec(), dashboard=exposure))
        )

    threads = [
        threading.Thread(target=build, args=(exposure,))
        for exposure in ("none", "none", "loopback", "none", "loopback")
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    try:
        silent = [c for exposure, c in built if exposure == "none"]
        listening = [c for exposure, c in built if exposure == "loopback"]
        assert len(silent) == 3 and len(listening) == 2
        for cluster in silent:
            assert not hasattr(cluster.scheduler, "http_server")
        for cluster in listening:
            assert cluster.scheduler.http_server is not None, (
                "a cluster built beside a silent one must still listen"
            )
    finally:
        for _, cluster in built:
            cluster.close()

    assert ServerNode.start_http_server is untouched, (
        "silence is a subclass, so the shared base is never modified at all"
    )


def test_a_missing_bokeh_is_reported_as_a_missing_bokeh():
    """The dashboard's real failure mode, said in words that name it.

    A scheduler serving a dashboard imports `distributed.dashboard.scheduler`
    lazily, and that needs bokeh. Without it the failure arrives as an
    `AttributeError` about a missing attribute on a module the caller has never
    heard of. It cost a farm run to recognise once.

    Both halves are covered because they do not report it the same way: the
    scheduler's message says `distributed.dashboard`, and a worker's says only
    "Worker failed to start." with the cause underneath.
    """

    from hedloom_run.cluster import _blames_the_dashboard

    scheduler_side = RuntimeError(
        "Cluster failed to start: module 'distributed.dashboard' has no "
        "attribute 'scheduler'"
    )
    assert _blames_the_dashboard(scheduler_side)

    worker_side = RuntimeError("Worker failed to start.")
    worker_side.__cause__ = AttributeError(
        "module 'distributed.dashboard' has no attribute 'worker'"
    )
    assert _blames_the_dashboard(worker_side), (
        "a worker reports this only in its cause; matching the message alone "
        "would let it through under a name that explains nothing"
    )

    unrelated = RuntimeError("Worker failed to start.")
    unrelated.__cause__ = OSError("address already in use")
    assert not _blames_the_dashboard(unrelated), (
        "an unrelated startup failure must not be blamed on bokeh"
    )


def test_the_bokeh_diagnosis_survives_a_cycle_in_the_cause_chain():
    """Walking a chain must terminate even when the chain does not.

    `__context__` can point back at an exception already seen, and this walk
    runs while a cluster is failing to start — the worst moment to hang.
    """

    from hedloom_run.cluster import _blames_the_dashboard

    first = RuntimeError("one")
    second = RuntimeError("two")
    first.__cause__ = second
    second.__cause__ = first

    assert not _blames_the_dashboard(first)
