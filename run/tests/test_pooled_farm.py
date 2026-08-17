"""A pooled LSF cluster, driven against the fake farm.

`docs/pooled-placement-plan.md` sets out a four-step spike and says each step
is falsifiable on its own. Steps 1 and 2 are here — two clusters in one
process, and a `WorkerPlugin` that builds the pooled client where it cannot be
shipped — together with the thing that had blocked step 3: a substrate to run
them against.

Nothing here needs LSF. `dask_jobqueue.LSFCluster` submits a job script with
`bsub`, and `exec/tests/fakefarm` now answers that call shape, so the whole
pooled path is exercised locally. What is still deferred to a real farm is
what the fake says it cannot reproduce: contention, fair share, and a queue
that pends because the farm is busy.

These are the slowest tests in this unit — they start real worker processes —
and they are worth it, because the alternative is a design whose only evidence
is that it ought to work.
"""

import os
import time

import pytest

from hedloom_run.cluster import spec_cluster
from hedloom_run.graph import _require_shippable

distributed = pytest.importorskip("distributed")
dask_jobqueue = pytest.importorskip("dask_jobqueue")

from distributed import Client, WorkerPlugin, get_worker  # noqa: E402

FARM = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "exec",
    "tests",
    "fakefarm",
)


@pytest.fixture
def farm(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", FARM + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("FAKE_LSF_STATE", str(tmp_path / "farm"))
    return tmp_path / "farm"


@pytest.fixture
def pool(farm):
    """One `LSFCluster` whose workers are fake-farm batch jobs."""

    cluster = dask_jobqueue.LSFCluster(
        queue="normal", cores=1, memory="1GB", walltime="00:30",
        processes=1, n_workers=0, death_timeout=60,
    )
    try:
        yield cluster
    finally:
        cluster.close()


# Every payload below is defined *inside* its test, and that is not a style
# choice. These workers are separate processes started by the fake farm, with
# no test module on their path. cloudpickle sends a module-level function to a
# worker **by reference**, so the worker tries to import `test_pooled_farm`,
# fails, and the task simply never completes — no exception at the client, just
# a gather that waits forever. A locally defined function is sent by value and
# needs nothing on the far side. The same rule governs the `WorkerPlugin`.


def test_a_pooled_cluster_runs_work_on_farm_submitted_workers(pool, farm):
    """Step 3's substrate: a pool whose workers arrived through `bsub`."""

    def double(value):
        return value * 2

    client = Client(pool)
    try:
        pool.scale(jobs=2)
        client.wait_for_workers(2, timeout=120)
        assert client.gather([client.submit(double, i, pure=False)
                              for i in range(6)]) == [0, 2, 4, 6, 8, 10]
    finally:
        # Client before cluster. A client outliving its scheduler spends the
        # teardown reconnecting to nothing, which reads as a failure and is not
        # one; it is the same ordering the pooled `WorkerPlugin` needs below.
        client.close()

    submitted = list(farm.glob("*.json"))
    assert len(submitted) == 2, "each worker should be one visible farm job"


def test_closing_the_pool_leaves_no_farm_job_behind(pool, farm):
    """The lifetime argument pooled placement has to make.

    A direct placement is owner-bound: `bsub -I` dies with its client and
    `hedloom_exec`'s whole crash-window argument rests on it. A pooled worker
    has no such binding — it is a batch job that outlives whatever submitted it
    — so the guarantee has to come from somewhere else, and this is it:
    `LSFCluster.close()` cancels every job it opened. If that ever stops being
    true, a study would leave workers running on the farm after it finished.
    """

    import json

    client = Client(pool)
    pool.scale(jobs=1)
    client.wait_for_workers(1, timeout=120)
    record = json.loads(next(iter(farm.glob("*.json"))).read_text())
    supervisor = record["supervisor_pid"]
    assert record["state"] == "RUN"

    client.close()
    pool.close()

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        record = json.loads(next(iter(farm.glob("*.json"))).read_text())
        if record["state"] not in ("PEND", "RUN"):
            break
        time.sleep(0.1)
    assert record["state"] == "EXIT", "the pool did not cancel its own job"

    with pytest.raises(ProcessLookupError):
        os.kill(supervisor, 0)


def test_a_readiness_worker_reaches_the_pool_through_a_plugin(pool, farm):
    """Steps 1 and 2, against a pool that is really made of farm jobs.

    The register recorded "two clusters in one process composes cleanly" as
    assumed rather than demonstrated. This is the demonstration, and it is the
    shape pooled placement would take: the in-process `SpecCluster` this kernel
    already builds for readiness, holding a client to a second cluster whose
    workers are LSF jobs.
    """

    class PooledClientPlugin(WorkerPlugin):
        """Build the pooled client on the worker, the only place it fits.

        `graph.py` notes that a transport which must be a singleton "will need
        a factory constructed on the worker". This is that factory: `setup()`
        runs in the worker's main thread, so the client is built where it is
        used and never has to survive being pickled.
        """

        name = "pooled-client"

        def __init__(self, address):
            self._address = address

        def setup(self, worker):
            # Never the process default: a second default silently displaces
            # the first, and anything reaching for the ambient client
            # afterwards gets the wrong cluster with no error.
            worker.pooled_client = Client(self._address, set_as_default=False)

        def teardown(self, worker):
            client = getattr(worker, "pooled_client", None)
            if client is not None:
                client.close()

    def through_the_pool(value):
        """Runs on the readiness cluster; does its work on the pool."""

        return get_worker().pooled_client.submit(
            lambda inner: inner * 2, value, pure=False
        ).result()

    readiness = spec_cluster({"lsf": {"nthreads": 2}}, dashboard="none")
    client = Client(readiness)
    pooled = None
    try:
        pool.scale(jobs=1)
        pooled = Client(pool, set_as_default=False)
        pooled.wait_for_workers(1, timeout=120)

        client.register_plugin(PooledClientPlugin(pool.scheduler_address))
        assert client.gather([client.submit(through_the_pool, i, pure=False)
                              for i in range(4)]) == [0, 2, 4, 6]
    finally:
        if pooled is not None:
            pooled.close()
        client.close()
        readiness.close()


def test_a_transport_holding_a_pooled_client_is_still_refused(pool):
    """The guard that keeps the plugin honest.

    A pooled transport must be built on the worker, and the thing that makes
    that a rule rather than a habit is `_require_shippable` refusing the naked
    one up front — naming the placement, rather than failing deep inside Dask's
    serialization with a message about graph expressions.
    """

    client = Client(pool, set_as_default=False)

    class NakedPooledTransport:
        name = "lsf-pooled"
        discovery_is_authoritative = False

        def __init__(self, pooled):
            self._pooled = pooled

    try:
        with pytest.raises(TypeError, match="cannot be sent to a worker"):
            _require_shippable({"pool": NakedPooledTransport(client)})
    finally:
        client.close()
