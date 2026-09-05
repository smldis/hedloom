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


def test_a_mixed_plan_places_some_points_directly_and_some_on_the_pool(
    tmp_path, farm
):
    """Spike step 4. Two substrates, one plan, one run, one report.

    The two arms compute *different* words on purpose. A record is selected by
    the declared computation and placement is not part of it, so writing the
    same word on both arms would be one shared record asked for twice at once
    — which this pass leaves as a claim refusal rather than coalescing. What
    this test is about is that one plan reaches two substrates in one run;
    placement-independence of identity is asserted by
    `test_placement_does_not_reach_the_attempt_identity` instead.
    """

    site = site_with_pool(tmp_path)

    # Placement is authored, per call, with `.options(policy=...)`. It is not
    # an override on the site: which substrate a point belongs on is a
    # property of the work, and the Plan records it before anything is spent.
    @flow
    def mixed(words):
        direct = [
            write_note.options(policy=local())(word=f"{word}-local")
            for word in sweep(words, key=lambda item: f"local-{item}")
        ]
        on_pool = [
            write_note.options(policy=pooled())(word=f"{word}-pool")
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


def test_placement_does_not_reach_the_attempt_identity(tmp_path, farm):
    """The kernel invariant, stated where it actually lives: the digest.

    Queue, cores and host are excluded from `IDENTITY_KEYS` deliberately: they
    do not change what a deterministic operation produces, so changing them
    must not invalidate a result. Pooling is the largest such change there is —
    a different substrate entirely — so if any placement were going to leak
    into identity, this is where it would show.

    Asserted on both halves. A record is now selected by the declared
    computation alone (`exec/src/hedloom_exec/identity.py`), so two
    *differently named* studies declaring the same work reach the same record:
    the second run must therefore both carry the same digest and reuse the
    first run's evidence rather than recompute it. These two submits are
    sequential, so nothing here depends on the deferred question of what two
    simultaneous requesters should do.
    """

    site = site_with_pool(tmp_path)

    on_pool = all_pooled(("ab",)).submit(site=site)
    assert on_pool.succeeded, on_pool.summary()

    # Same work, same inputs, placed directly instead of on the pool.
    @study(default_policy=local())
    def directly(words=("ab",)):
        return notes.named("notes")(words)

    placed_directly = directly().submit(site=site)
    assert placed_directly.succeeded, placed_directly.summary()

    # Guards the comparison below: two empty lists are equal for the wrong
    # reason, and this plan has exactly one point.
    assert len(on_pool.report.outcomes) == 1
    assert len(placed_directly.report.outcomes) == 1
    assert {item.placement for item in on_pool.report.outcomes} == {"pool"}
    assert {item.placement for item in placed_directly.report.outcomes} == {"local"}
    assert [item.input_digest for item in on_pool.report.outcomes] == [
        item.input_digest for item in placed_directly.report.outcomes
    ], (
        "a point moved off the pool must keep the identity it had on it; "
        "placement is not identity-bearing"
    )
    assert all(item.reused for item in placed_directly.report.outcomes), (
        "the same declared computation in another study must select the same "
        "record and reuse its evidence, not recompute it under a new name"
    )


def test_an_overridden_run_reuses_what_the_plain_run_produced(tmp_path, farm):
    """The other half: same namespace, different substrate, nothing recomputed.

    `Site.with_override` promises exactly this — "an overridden run lands on the
    same attempt identities as a plain one, and the two reuse each other's
    work". An override is how a placement is *reached*, which is why it may
    carry `placement` and `kernel` and refuses everything the Plan owns.
    """

    site = site_with_pool(tmp_path)

    first = all_pooled(("ab",)).submit(site=site)
    assert first.succeeded, first.summary()

    second = all_pooled(("ab",)).submit(
        site=site,
        override={"placement": {"pool": {"cores": 2, "walltime": "1:00"}}},
    )

    assert second.succeeded, second.summary()
    # `all` over nothing is True, so the count is part of the claim.
    assert len(second.report.outcomes) == len(first.report.outcomes) == 1
    assert all(item.reused for item in second.report.outcomes), (
        "changing how a placement is reached must not invalidate a result"
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
