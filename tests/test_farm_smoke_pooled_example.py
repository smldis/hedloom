"""The pooled farm smoke example itself, against the fake farm.

`tests/test_pooled_placement.py` covers pooled placement with operations it
writes for the purpose. This runs the example a reader is pointed at, so its
two distinct claims are checked rather than assumed: that one plan reaches both
substrates in one run, and that moving an invocation from the pool to a direct
placement reuses its result instead of recomputing it.

No LSF is needed — `exec/tests/fakefarm` answers both call shapes — but
`dask_jobqueue` is, because that is what builds the pool.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from hedloom import Site

pytest.importorskip("distributed")
pytest.importorskip("dask_jobqueue")

from examples import farm_smoke_pooled  # noqa: E402


FARM = str(Path(__file__).resolve().parents[1] / "exec" / "tests" / "fakefarm")


@pytest.fixture
def farm(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", FARM + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("FAKE_LSF_STATE", str(tmp_path / "farm"))
    return tmp_path / "farm"


def site_with_pool(tmp_path: Path) -> Site:
    return Site(
        root=str(tmp_path / "attempts"),
        workspace_root=str(tmp_path / "work"),
        dashboard="none",
        placements={
            "pool": {
                "kind": "lsf-pooled",
                "queue": "normal",
                "cores": 1,
                "memory_mb": 1000,
                "walltime": "0:30",
                "workers": 2,
                "max_jobs": 2,
            },
            "lsf": {
                "kind": "lsf-interactive",
                "queue": "normal",
                "walltime": "5",
                "max_jobs": 2,
            },
        },
    )


def test_the_mixed_plan_reaches_both_substrates_in_one_run(tmp_path, farm) -> None:
    """A pooled producer feeding a direct consumer, for every point."""

    run = farm_smoke_pooled.placement_sweep(
        farm_smoke_pooled.pooled_sweep
    ).submit(site=site_with_pool(tmp_path))

    assert run.succeeded, run.summary()
    placements = {item.placement for item in run.report.outcomes}
    assert placements == {"pool", "lsf"}, (
        f"the example exists to cross two substrates; it used {placements}"
    )
    assert list(farm.glob("*.json")), "the pool's workers are farm jobs"


def test_moving_a_point_off_the_pool_reuses_rather_than_reruns(tmp_path, farm) -> None:
    """The kernel invariant, on the example a reader is actually shown.

    `direct_sweep` is `pooled_sweep` with one operation's placement changed and
    nothing else. Queue, cores and host are excluded from `IDENTITY_KEYS` on
    purpose, so the moved run must land on the same identities and reuse every
    result. If it ever reruns, a study recomputes everything the day someone
    edits a placement.
    """

    site = site_with_pool(tmp_path)

    first = farm_smoke_pooled.placement_sweep(
        farm_smoke_pooled.pooled_sweep
    ).submit(site=site)
    assert first.succeeded, first.summary()

    moved = farm_smoke_pooled.placement_sweep(
        farm_smoke_pooled.direct_sweep
    ).submit(site=site)
    assert moved.succeeded, moved.summary()
    assert len(moved.report.reused) == len(moved.report.outcomes), (
        "changing where work runs must change how long it takes and nothing else"
    )
    assert moved.value == first.value
