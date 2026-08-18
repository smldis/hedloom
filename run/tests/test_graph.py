"""The Dask kernel, held to the same meaning as the sequential one.

The load-bearing test here is not that a plan runs — it is
`test_the_graph_kernel_produces_the_same_identities_as_the_loop`. If the two
kernels can disagree about identity, a study means something different
depending on how it was run, and reuse across kernels becomes unsound.

The cluster is local and threaded, which is the shape this kernel documents:
no pickling of live transports, no nanny, and one thread per waiting
invocation.
"""

import importlib
import threading
import time
from pathlib import Path

import pytest

from hedloom_exec.journal import ConcurrentClaim
from hedloom_exec.planned import plan_bundles
from hedloom_exec.transport import InProcessTransport
from hedloom_run.driver import run_plan
from hedloom_run.binding import UnsupportedPlacement
from hedloom_run.graph import _task_key, run_plan_graph

distributed = pytest.importorskip("distributed")

from hedloom_run.cluster import local_cluster  # noqa: E402


@pytest.fixture(scope="module")
def client():
    # Silent, because `dashboard_address=None` is not: a scheduler starts its
    # HTTP server regardless and this suite used to bind :8787 on every run,
    # colliding with whatever the developer was already watching.
    # Declares the capacity every task now asks for. The kernel annotates each
    # task with the placement its invocation resolved to and refuses a cluster
    # that offers none, because an unadmitted task is never scheduled and never
    # reported — the run would hang against an idle cluster.
    cluster = local_cluster(threads=4, dashboard="none", placements={"local": 4})
    with distributed.Client(cluster) as connected:
        yield connected
    cluster.close()


def double(value=None, **kwargs):
    return (value or 1) * 2


def scale(factor=1, source=None, **kwargs):
    return (source or 1) * factor


def explode(**kwargs):
    raise RuntimeError("this point does not converge")


def controlled(marker_dir, role, **kwargs):
    """A task whose start and finish the client-side test controls by files."""

    directory = Path(marker_dir)
    (directory / f"started-{role}").write_text("started")
    if role == "fail":
        while not (directory / "fail-now").exists():
            time.sleep(0.01)
        raise RuntimeError("controlled failure")
    while not (directory / f"release-{role}").exists():
        time.sleep(0.01)
    return role


class ContendedTransport(InProcessTransport):
    """Raise the journal's ordinary claim conflict for one operation."""

    def submit(self, identity, bundle):
        if bundle.get("operation") == "explode":
            raise ConcurrentClaim("another caller owns this attempt")
        return super().submit(identity, bundle)


class RecordingClient:
    """Keep the real futures passed to cancellation inspectable by the test."""

    def __init__(self, client):
        self.client = client
        self.cancelled = []

    def __getattr__(self, name):
        return getattr(self.client, name)

    def cancel(self, futures, **kwargs):
        self.cancelled.extend(futures)
        return self.client.cancel(futures, **kwargs)


def transports():
    return {
        "local": InProcessTransport(
            {"double": double, "scale": scale, "explode": explode}
        )
    }


def invocation(key, operation, *, config=None, source=None):
    item = {
        "id": f"invoke:{key}",
        "authored_key": key,
        "operation": {"name": operation, "version": "1"},
        "config": config or [],
        "inputs": [],
        "policy": {"name": "local", "options": {}},
    }
    if source is not None:
        item["inputs"] = [
            {
                "cardinality": "scalar",
                "name": "source",
                "reference": {
                    "type": "output",
                    "invocation_id": f"invoke:{source}",
                    "output_name": "out",
                },
            }
        ]
    return item


def document(invocations):
    names = sorted({item["operation"]["name"] for item in invocations})
    return {
        "schema_version": 2,
        "sources": [],
        "operations": [
            {"identity": {"name": name, "version": "1"},
             "outputs": [{"name": "out"}]}
            for name in names
        ],
        "invocations": invocations,
    }


def chain():
    """seed -> consumer, plus an unrelated sibling that shares nothing."""

    return document(
        [
            invocation("seed", "double", config=[{"name": "value", "value": 3}]),
            invocation(
                "consumer", "scale", config=[{"name": "factor", "value": 10}],
                source="seed",
            ),
            invocation("sibling", "double", config=[{"name": "value", "value": 5}]),
        ]
    )


def test_a_cluster_that_cannot_admit_the_plan_is_refused(tmp_path):
    """The failure this refusal exists to prevent is silence.

    Every task asks for the capacity of the placement it resolved to. A cluster
    declaring none holds all of them unrunnable — no exception, no log line at
    the client, an idle cluster and an empty farm. Refused before anything runs
    instead, in the same spirit as the shippability check.
    """

    cluster = local_cluster(threads=2, dashboard="none")
    try:
        with distributed.Client(cluster) as bare:
            with pytest.raises(UnsupportedPlacement) as raised:
                run_plan_graph(
                    chain(),
                    client=bare,
                    transports=transports(),
                    plan_id="study",
                    root=str(tmp_path),
                )
    finally:
        cluster.close()

    assert "local" in str(raised.value)
    assert "cluster_for" in str(raised.value), "say how to build one that works"


def test_a_plan_runs_and_reports_in_plan_order(client, tmp_path):
    report = run_plan_graph(
        chain(),
        client=client,
        transports=transports(),
        plan_id="study",
        root=str(tmp_path),
    )

    assert report.succeeded, report.summary()
    assert [item.authored_key for item in report.outcomes] == [
        "seed",
        "consumer",
        "sibling",
    ]


def test_an_upstream_value_reaches_its_consumer(client, tmp_path):
    report = run_plan_graph(
        chain(),
        client=client,
        transports=transports(),
        plan_id="study",
        root=str(tmp_path),
    )

    by_key = {item.authored_key: item for item in report.outcomes}
    assert by_key["seed"].value == 6
    assert by_key["consumer"].value == 60, "the edge must carry the value"


def test_the_graph_kernel_produces_the_same_identities_as_the_loop(client, tmp_path):
    """Which kernel decides readiness must not change what a plan means."""

    sequential = run_plan(
        chain(),
        transports=transports(),
        plan_id="study",
        root=str(tmp_path / "loop"),
    )
    graphed = run_plan_graph(
        chain(),
        client=client,
        transports=transports(),
        plan_id="study",
        root=str(tmp_path / "graph"),
    )

    assert [item.input_digest for item in sequential.outcomes] == [
        item.input_digest for item in graphed.outcomes
    ]
    assert [item.value for item in sequential.outcomes] == [
        item.value for item in graphed.outcomes
    ]


def test_a_result_recorded_by_one_kernel_is_reused_by_the_other(client, tmp_path):
    """The corollary: identity is shared, so the record is too."""

    run_plan(
        chain(),
        transports=transports(),
        plan_id="study",
        root=str(tmp_path),
    )
    graphed = run_plan_graph(
        chain(),
        client=client,
        transports=transports(),
        plan_id="study",
        root=str(tmp_path),
    )

    assert all(item.reused for item in graphed.outcomes), graphed.summary()


def test_stopped_reports_have_the_same_shape_in_both_kernels(client, tmp_path):
    """A blocked invocation needs no kernel-specific report vocabulary."""

    plan = document(
        [
            invocation("first", "double"),
            invocation("bad", "explode", source="first"),
            invocation("third", "scale", source="bad"),
            invocation("fourth", "scale", source="third"),
        ]
    )
    sequential = run_plan(
        plan,
        transports=transports(),
        plan_id="study",
        root=str(tmp_path / "loop"),
    )
    graphed = run_plan_graph(
        plan,
        client=client,
        transports=transports(),
        plan_id="study",
        root=str(tmp_path / "graph"),
    )

    def shape(report):
        return [
            (
                item.authored_key,
                item.disposition,
                item.outcome,
                item.placement,
            )
            for item in report.outcomes
        ]

    assert shape(graphed) == shape(sequential)


def test_a_failure_blocks_its_dependent_and_spares_the_others(client, tmp_path):
    """One point failing must not abandon the rest of a sweep."""

    report = run_plan_graph(
        document(
            [
                invocation("bad", "explode"),
                invocation("downstream", "scale", source="bad"),
                invocation("unrelated", "double",
                           config=[{"name": "value", "value": 7}]),
            ]
        ),
        client=client,
        transports=transports(),
        plan_id="study",
        root=str(tmp_path),
        stop_on_failure=False,
    )

    by_key = {item.authored_key: item for item in report.outcomes}
    assert by_key["bad"].outcome == "failed"
    assert by_key["downstream"].outcome == "blocked"
    assert by_key["downstream"].disposition == "skipped"
    assert by_key["unrelated"].outcome == "succeeded", (
        "an independent branch must still run"
    )


def _wait_until(predicate, message, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.01)
    raise AssertionError(message)


def _call_stack_keys(client):
    return {
        key
        for tasks in (client.call_stack() or {}).values()
        for key in tasks
    }


def _controlled_document(marker_dir):
    return document(
        [
            invocation(
                "first",
                "controlled",
                config=[
                    {"name": "marker_dir", "value": str(marker_dir)},
                    {"name": "role", "value": "first"},
                ],
            ),
            invocation(
                "bad",
                "controlled",
                config=[
                    {"name": "marker_dir", "value": str(marker_dir)},
                    {"name": "role", "value": "fail"},
                ],
            ),
            invocation(
                "third",
                "controlled",
                config=[
                    {"name": "marker_dir", "value": str(marker_dir)},
                    {"name": "role", "value": "third"},
                ],
            ),
            invocation(
                "fourth",
                "controlled",
                config=[
                    {"name": "marker_dir", "value": str(marker_dir)},
                    {"name": "role", "value": "fourth"},
                ],
            ),
        ]
    )


def test_stopping_cancels_the_unstarted_and_waits_for_the_in_flight(tmp_path):
    """Cancellation is asserted from future state, never inferred from sleep."""

    cluster = local_cluster(threads=2, dashboard="none", placements={"local": 2})
    marker_dir = tmp_path / "markers"
    marker_dir.mkdir()
    plan = _controlled_document(marker_dir)
    keys = {item.authored_key: _task_key(item) for item in plan_bundles(plan)}
    result = {}
    errors = []

    try:
        with distributed.Client(cluster) as connected:
            recording = RecordingClient(connected)

            def run():
                try:
                    result["report"] = run_plan_graph(
                        plan,
                        client=recording,
                        transports={
                            "local": InProcessTransport({"controlled": controlled})
                        },
                        plan_id="study",
                        root=str(tmp_path / "attempts"),
                    )
                except BaseException as error:
                    errors.append(error)

            thread = threading.Thread(target=run)
            thread.start()
            _wait_until(
                lambda: {keys["first"], keys["bad"]} <= _call_stack_keys(connected),
                "the first two invocations did not acquire the two worker threads",
            )
            (marker_dir / "fail-now").write_text("fail")
            cancelled_future = _wait_until(
                lambda: next(
                    (
                        future
                        for future in recording.cancelled
                        if future.status == "cancelled"
                    ),
                    None,
                ),
                "no unstarted future reached Dask's cancelled state",
            )
            cancelled_key = cancelled_future.key
            assert thread.is_alive(), "the run returned before admitted work finished"

            for role in ("first", "third", "fourth"):
                (marker_dir / f"release-{role}").write_text("release")
            thread.join(timeout=5)
            assert not thread.is_alive()
            assert not errors

            report = result["report"]
            by_key = {item.authored_key: item for item in report.outcomes}
            cancelled_name = next(
                name for name, key in keys.items() if key == cancelled_key
            )
            assert len(report.outcomes) == 4
            assert by_key["bad"].outcome == "failed"
            assert by_key["first"].outcome == "succeeded"
            assert by_key[cancelled_name].disposition == "skipped"
            assert by_key[cancelled_name].outcome == "blocked"
            for role in ("third", "fourth"):
                if (marker_dir / f"started-{role}").exists():
                    assert by_key[role].outcome == "succeeded"
    finally:
        (marker_dir / "fail-now").touch()
        for role in ("first", "third", "fourth"):
            (marker_dir / f"release-{role}").touch()
        if "thread" in locals():
            thread.join(timeout=5)
        cluster.close()


def test_disabling_the_stop_runs_every_independent_branch(tmp_path):
    marker_dir = tmp_path / "markers"
    marker_dir.mkdir()
    plan = _controlled_document(marker_dir)
    cluster = local_cluster(threads=2, dashboard="none", placements={"local": 2})
    result = {}

    try:
        with distributed.Client(cluster) as connected:

            def run():
                result["report"] = run_plan_graph(
                    plan,
                    client=connected,
                    transports={
                        "local": InProcessTransport({"controlled": controlled})
                    },
                    plan_id="study",
                    root=str(tmp_path / "attempts"),
                    stop_on_failure=False,
                )

            thread = threading.Thread(target=run)
            thread.start()
            _wait_until(
                lambda: (marker_dir / "started-fail").exists(),
                "the failing invocation did not start",
            )
            (marker_dir / "fail-now").write_text("fail")
            for role in ("first", "third", "fourth"):
                (marker_dir / f"release-{role}").write_text("release")
            thread.join(timeout=5)
            assert not thread.is_alive()

            by_key = {
                item.authored_key: item for item in result["report"].outcomes
            }
            assert by_key["bad"].outcome == "failed"
            assert all(
                by_key[name].outcome == "succeeded"
                for name in ("first", "third", "fourth")
            )
    finally:
        (marker_dir / "fail-now").touch()
        for role in ("first", "third", "fourth"):
            (marker_dir / f"release-{role}").touch()
        if "thread" in locals():
            thread.join(timeout=5)
        cluster.close()


def test_disabling_the_stop_never_enters_stop_admission(
    client, tmp_path, monkeypatch
):
    graph_module = importlib.import_module("hedloom_run.graph")

    def unexpected_stop(**kwargs):
        raise AssertionError("stop admission was entered while disabled")

    monkeypatch.setattr(graph_module, "_stop_admitting", unexpected_stop)
    report = run_plan_graph(
        document(
            [
                invocation("bad", "explode"),
                invocation("unrelated", "double"),
            ]
        ),
        client=client,
        transports=transports(),
        plan_id="study",
        root=str(tmp_path),
        stop_on_failure=False,
    )

    assert {item.authored_key for item in report.outcomes} == {
        "bad",
        "unrelated",
    }


def test_a_concurrent_claim_is_reported_without_losing_other_outcomes(
    client, tmp_path
):
    report = run_plan_graph(
        document(
            [
                invocation("before", "double"),
                invocation("contended", "explode"),
                invocation("after", "double"),
            ]
        ),
        client=client,
        transports={
            "local": ContendedTransport({"double": double, "explode": explode})
        },
        plan_id="study",
        root=str(tmp_path),
        stop_on_failure=False,
    )

    by_key = {item.authored_key: item for item in report.outcomes}
    assert len(report.outcomes) == 3
    assert by_key["before"].outcome == "succeeded"
    assert by_key["contended"].disposition == "refused"
    assert "ConcurrentClaim" in by_key["contended"].error
    assert by_key["after"].outcome == "succeeded"


def test_an_escaping_exception_cancels_before_it_propagates(tmp_path):
    """Even a broken observer must leave no unstarted work behind."""

    cluster = local_cluster(threads=2, dashboard="none", placements={"local": 2})
    marker_dir = tmp_path / "markers"
    marker_dir.mkdir()
    root = invocation("root", "double")
    children = [
        invocation(
            name,
            "controlled",
            config=[
                {"name": "marker_dir", "value": str(marker_dir)},
                {"name": "role", "value": name},
            ],
            source="root",
        )
        for name in ("one", "two", "three")
    ]
    plan = document([root, *children])
    keys = {item.authored_key: _task_key(item) for item in plan_bundles(plan)}
    caught = {}

    try:
        with distributed.Client(cluster) as connected:
            recording = RecordingClient(connected)

            def broken_observer(outcome):
                if outcome.authored_key == "root":
                    _wait_until(
                        lambda: any(
                            keys[name] in _call_stack_keys(connected)
                            for name in ("one", "two", "three")
                        ),
                        "no dependent acquired a worker before observer failure",
                    )
                    raise RuntimeError("observer broke")

            def run():
                try:
                    run_plan_graph(
                        plan,
                        client=recording,
                        transports={
                            "local": InProcessTransport(
                                {"double": double, "controlled": controlled}
                            )
                        },
                        plan_id="study",
                        root=str(tmp_path / "attempts"),
                        on_event=broken_observer,
                    )
                except BaseException as error:
                    caught["error"] = error

            thread = threading.Thread(target=run)
            thread.start()
            cancelled_future = _wait_until(
                lambda: next(
                    (
                        future
                        for future in recording.cancelled
                        if future.status == "cancelled"
                    ),
                    None,
                ),
                "the escaping exception did not cancel an unstarted future",
            )
            cancelled_key = cancelled_future.key
            assert thread.is_alive(), "cleanup did not wait for admitted work"
            for name in ("one", "two", "three"):
                (marker_dir / f"release-{name}").write_text("release")
            thread.join(timeout=5)
            assert not thread.is_alive()

            error = caught["error"]
            assert str(error) == "observer broke"
            assert len(error.report.outcomes) == 4
            assert error.in_flight
            cancelled_name = next(
                name for name, key in keys.items() if key == cancelled_key
            )
            by_key = {item.authored_key: item for item in error.report.outcomes}
            assert by_key[cancelled_name].outcome == "blocked"
            assert all(
                by_key[name].outcome == "succeeded" for name in error.in_flight
            )
    finally:
        for name in ("one", "two", "three"):
            (marker_dir / f"release-{name}").touch()
        if "thread" in locals():
            thread.join(timeout=5)
        cluster.close()


def test_a_placement_nobody_provides_fails_rather_than_falling_back(
    client, tmp_path
):
    report = run_plan_graph(
        document(
            [
                {
                    **invocation("heavy", "double"),
                    "policy": {"name": "lsf-pool", "options": {}},
                }
            ]
        ),
        client=client,
        transports=transports(),
        plan_id="study",
        root=str(tmp_path),
    )

    outcome = report.outcomes[0]
    assert outcome.disposition == "refused"
    assert "lsf-pool" in outcome.error


def test_events_report_while_the_sweep_is_still_running(client, tmp_path):
    seen = []
    report = run_plan_graph(
        chain(),
        client=client,
        transports=transports(),
        plan_id="study",
        root=str(tmp_path),
        on_event=seen.append,
    )

    assert len(seen) == len(report.outcomes)
    assert {item.authored_key for item in seen} == {"seed", "consumer", "sibling"}


def test_a_transport_that_cannot_reach_a_worker_is_refused_by_name(client, tmp_path):
    """Dask's own failure names a graph expression, not the placement."""

    import threading

    unshippable = InProcessTransport({"double": double})
    unshippable.lock = threading.Lock()

    with pytest.raises(TypeError) as raised:
        run_plan_graph(
            chain(),
            client=client,
            transports={"local": unshippable},
            plan_id="study",
            root=str(tmp_path),
        )

    assert "local" in str(raised.value)


def test_a_task_is_named_after_the_point_it_runs():
    """The point of watching a sweep is knowing which point is running."""

    from hedloom_exec.planned import plan_bundles

    items = plan_bundles(chain())
    keys = [_task_key(item) for item in items]

    # The operation comes first: Dask groups tasks by everything before the
    # first "-" and learns a duration average per group. Keyed by point, every
    # task was its own group and every estimate fell back to a flat 500 ms.
    assert keys[0].startswith("double-")
    assert "seed" in keys[0], "the point must still be readable in a dashboard"
    assert len(set(keys)) == len(keys)
