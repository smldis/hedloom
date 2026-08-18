"""Routing an operation to a pool, from the façade, against the fake farm.

Step 4 of `design/pooled-placement-plan.md`'s spike sequence, and the first point
at which the design pays: one plan, some points on their own `bsub -I` job and
some on a shared pool of reusable workers, in one run.

The kernel invariant applies here too and is what the last test asserts:

    Changing where work runs changes how long a plan takes and nothing else.

An invocation moved to a pool must land on the same attempt identity and reuse
the same result as one placed directly, because queue, cores and host are
excluded from `IDENTITY_KEYS` on purpose. If that ever stops being true, a study
would silently recompute everything the day someone changed a placement.

No LSF is needed: `exec/tests/fakefarm` answers both the interactive and the
batch call shapes, and `dask_jobqueue.LSFCluster` builds the pool out of the
latter.
"""

import os
from pathlib import Path

import pytest

from hedloom import Site, file, flow, local, operation, parameter, pooled, shell, study, sweep

pytest.importorskip("distributed")
pytest.importorskip("dask_jobqueue")

FARM = str(Path(__file__).resolve().parents[1] / "exec" / "tests" / "fakefarm")


@operation(
    config={"word": parameter(str)},
    outputs={"note": file("note.txt", kind="text-file")},
)
def write_note(out, *, word: str):
    """A command, not a callable: only a command can leave this process."""

    return shell("sh", "-c", f"printf %s {word}{word} > {out.note}")


@flow
def notes(words):
    written = [write_note(word=word) for word in sweep(words, key=lambda item: item)]
    return {"last": written[-1]}


@study(default_policy=local())
def pooled_study(words=("ab", "cd")):
    return notes.named("notes")(words)


@study(default_policy=pooled())
def all_pooled(words=("ab", "cd")):
    return notes.named("notes")(words)


@pytest.fixture
def farm(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", FARM + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("FAKE_LSF_STATE", str(tmp_path / "farm"))
    return tmp_path / "farm"


def site_with_pool(tmp_path, **extra):
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
            **extra,
        },
    )


def test_a_study_runs_entirely_on_a_pool(tmp_path, farm):
    """The plain case: every point routed to `pool`, one run, real workers."""

    run = all_pooled().submit(site=site_with_pool(tmp_path))

    assert run.succeeded, run.summary()
    assert {item.placement for item in run.report.outcomes} == {"pool"}
    assert (tmp_path / "work").exists()

    # The pool's workers are farm jobs, and the fake farm saw them as such.
    submitted = list(farm.glob("*.json"))
    assert submitted, "the pool should have submitted LSF jobs for its workers"


def test_a_mixed_plan_places_some_corners_directly_and_some_on_the_pool(
    tmp_path, farm
):
    """Spike step 4. Two substrates, one plan, one run, one report."""

    site = site_with_pool(tmp_path)

    # Placement is authored, per call, with `.options(policy=...)`. It is not
    # an override on the site: which substrate a point belongs on is a
    # property of the work, and the Plan records it before anything is spent.
    @flow
    def mixed(words):
        direct = [
            write_note.options(policy=local())(word=word)
            for word in sweep(words, key=lambda item: f"local-{item}")
        ]
        on_pool = [
            write_note.options(policy=pooled())(word=word)
            for word in sweep(words, key=lambda item: f"pool-{item}")
        ]
        return {"direct": direct[-1], "pooled": on_pool[-1]}

    @study(default_policy=local())
    def build(words=("ab", "cd")):
        return mixed.named("mixed")(words)

    run = build().submit(site=site)

    assert run.succeeded, run.summary()
    placements = {item.authored_key: item.placement for item in run.report.outcomes}
    assert placements["pool-ab:write_note"] == "pool"
    assert placements["pool-cd:write_note"] == "pool"
    assert placements["local-ab:write_note"] == "local"
    assert set(placements.values()) == {"pool", "local"}, (
        "one plan should have reached both substrates in one run"
    )


def test_moving_an_operation_to_a_pool_reuses_what_it_already_produced(
    tmp_path, farm
):
    """The kernel invariant, stated as reuse.

    Queue, cores and host are excluded from `IDENTITY_KEYS` deliberately: they
    do not change what a deterministic operation produces, so changing them
    must not invalidate a result. Pooling is the largest such change there is —
    a different substrate entirely — so if any placement were going to leak
    into identity, this is where it would show.
    """

    site = site_with_pool(tmp_path)

    first = all_pooled(("ab",)).submit(site=site)
    assert first.succeeded, first.summary()

    # Same study, same inputs, now placed directly on its own LSF job.
    @study(default_policy=local())
    def directly(words=("ab",)):
        return notes.named("notes")(words)

    second = directly().submit(site=site)

    assert second.succeeded, second.summary()
    assert all(item.reused for item in second.report.outcomes), (
        "a point moved off the pool must reuse the result the pool produced; "
        "placement is not identity-bearing"
    )


def test_a_pooled_placement_refuses_the_sequential_kernel_by_name(tmp_path, farm):
    """It cannot work there, so it says so instead of appearing to.

    The sequential kernel walks the plan in this thread. A pooled invocation
    reaches its pool through a client a Dask worker holds, and there is no
    worker — so this is a genuine divergence between the kernels, and the only
    honest thing to do is name it. Silently running the work here instead would
    be worse: it would succeed, publish an attempt record, and teach the author
    that `sequential=True` means what it does not.
    """

    run = all_pooled(("ab",)).submit(site=site_with_pool(tmp_path), sequential=True)

    assert not run.succeeded
    failure = run.report.outcomes[0]
    assert failure.outcome == "failed"
    assert "pooled" in (failure.error or "") and "sequential" in (failure.error or "")
