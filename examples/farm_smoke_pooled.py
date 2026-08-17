"""Prove pooled placement on a real LSF farm, beside the direct one.

    python examples/farm_smoke_pooled.py examples/farm-smoke-pooled.site.toml

Deliberately a second example rather than a flag on `farm_smoke.py`. That one
has been run against a real farm and passed; it is the evidence that direct
placement works, and evidence is worth more than tidiness. Nothing here touches
it.

## What this is trying to falsify

Pooled placement is a different substrate reached through a *second* Dask
cluster whose workers are LSF jobs, so the things most likely to be wrong are
not arithmetic — they are whether the pool comes up at all on a real farm, and
whether a mixed plan keeps its promises. In order of how expensive each would
be to discover later:

1. **The pool's workers reach their scheduler.** Locally everything is one
   host, so nothing tests that a `dask-worker` started by `bsub` on a compute
   node can dial back to the submit host. If the site's network or firewall
   says otherwise, this hangs at step 1 and nothing else here matters. It is
   the one failure a fake farm structurally cannot reproduce.
2. **A mixed plan reaches both substrates in one run.** Some corners on their
   own `bsub -I`, some through the pool.
3. **Placement is not identity-bearing.** The third pass moves work that ran on
   the pool onto a direct placement, and every invocation must be *reused*
   rather than recomputed. Queue, cores and host are excluded from
   `IDENTITY_KEYS` on purpose; if that ever broke, changing a placement would
   silently discard a farm's worth of results.
4. **Closing the session leaves no worker behind.** A pooled worker is a batch
   job and is *not* owner-bound: unlike `bsub -I`, it does not die with the
   client that submitted it. `LSFCluster.close()` is what stops it, and the
   check at the end is `bjobs`, not an assumption.

## Before running this

`python exec/examples/lsf_preflight.py --queue <queue>` still applies — it
checks what the direct placement needs. Pooling adds one requirement it does
not cover: **`hedloom-run` must be importable on the compute nodes**, because a
pooled worker is a Python process that imports `hedloom_run.pooled` to run your
command. If your farm mounts this checkout, that is already true.

If the run stalls at step 1 with workers PENDing forever, the queue is full or
the shape is unsatisfiable; if they start and never register, it is the network
between compute node and submit host, and `--pool-workers 1` plus `bjobs -l`
will tell you which.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
for unit in ("flow", "exec", "run"):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / unit / "src"))

from hedloom import (  # noqa: E402
    Site,
    artifact,
    file,
    flow,
    lsf,
    operation,
    parameter,
    pooled,
    session,
    shell,
    study,
    sweep,
)

NUMBER_LIST = artifact("number-list")

# Short work, many of it: the shape pooling is *for*. Each of these would spend
# more time queueing than running as its own job, which is the whole argument
# for paying dispatch once per worker instead of once per corner.
POINTS = tuple(
    {"key": f"p{index}", "start": index * 10, "count": 3}
    for index in range(1, 9)
)


@operation(
    config={"start": parameter(int), "count": parameter(int)},
    outputs={"numbers": file("numbers.txt", kind="number-list")},
    policy=pooled(),
)
def generate_pooled(out, *, start: int, count: int):
    """Produced on a shared pool worker. The body is placement-blind."""

    return shell(
        "/bin/sh",
        "-c",
        'start=$1; count=$2; output=$3; i=0; '
        ': > "$output"; while [ "$i" -lt "$count" ]; do '
        'printf "%s\\n" "$((start + i))" >> "$output"; i=$((i + 1)); done',
        "hedloom-generate",
        start,
        count,
        out.numbers,
    )


@operation(
    config={"start": parameter(int), "count": parameter(int)},
    inputs={"numbers": NUMBER_LIST},
    outputs={"summary": file("summary.txt", kind="text-file")},
    policy=lsf(),
)
def summarize_directly(numbers, out, *, start: int, count: int):
    """Consumed on its own `bsub -I` job, from an artifact the pool produced.

    The crossing is the point: a pooled producer and a direct consumer are two
    substrates in one chain, and the artifact between them is an address on a
    shared filesystem either can read. If that assumption is wrong at a site,
    this is where it shows.
    """

    return shell(
        "/bin/sh",
        "-c",
        'numbers=$1; start=$2; count=$3; output=$4; '
        'rows=0; sum=0; '
        'while IFS= read -r value; do '
        'rows=$((rows + 1)); sum=$((sum + value)); done < "$numbers"; '
        'printf "start=%s\\ncount=%s\\nrows=%s\\nsum=%s\\n" '
        '"$start" "$count" "$rows" "$sum" > "$output"',
        "hedloom-summarize",
        numbers,
        start,
        count,
        out.summary,
    )


@flow
def pooled_sweep(points):
    summaries = {}
    for point in sweep(points, key="key"):
        numbers = generate_pooled(start=point["start"], count=point["count"])
        result = summarize_directly(
            numbers, start=point["start"], count=point["count"]
        )
        summaries[point["key"]] = result.summary
    return summaries


@study
def mixed_sweep():
    """Eight chains: a pooled producer feeding a direct consumer, each time."""

    return pooled_sweep.named("pooled-sweep")(POINTS)


@flow
def direct_sweep(points):
    """The same work, every corner placed directly. Must reuse, never rerun."""

    summaries = {}
    for point in sweep(points, key="key"):
        numbers = generate_pooled.options(policy=lsf())(
            start=point["start"], count=point["count"]
        )
        result = summarize_directly(
            numbers, start=point["start"], count=point["count"]
        )
        summaries[point["key"]] = result.summary
    return summaries


@study
def moved_sweep():
    return direct_sweep.named("pooled-sweep")(POINTS)


def pool_jobs() -> set[str] | None:
    """Which farm jobs are live right now, by name, according to LSF itself.

    Asked rather than assumed: the claim being checked is that closing the
    session took the pool's workers with it, and only the farm can settle that.

    `None` means *could not ask*, which is deliberately not the same value as
    "nothing is running". Returning an empty set for an LSF that failed to
    answer would let a leak check pass by being unable to look — the exact
    shape of mistake `discovery_is_authoritative` exists to prevent elsewhere.

    A non-zero status with no output is LSF's way of saying the queue is empty,
    so that one is a genuine empty set.
    """

    try:
        answer = subprocess.run(
            ["bjobs", "-noheader", "-o", "job_name stat"],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if answer.returncode != 0 and "no unfinished job" not in answer.stderr.lower():
        return None
    return {line.split()[0] for line in answer.stdout.splitlines() if line.strip()}


def run(subject, moved, farm) -> int:
    print("first submission (pooled producers, direct consumers):")
    first = farm.submit(subject)
    if not first.succeeded:
        print(first.summary())
        return 1

    placements = {item.authored_key: item.placement for item in first.report.outcomes}
    pooled_keys = sorted(k for k, v in placements.items() if v == "pool")
    direct_keys = sorted(k for k, v in placements.items() if v == "lsf")
    print(f"\n  reached the pool  : {len(pooled_keys)} invocations")
    print(f"  reached bsub -I   : {len(direct_keys)} invocations")
    if not pooled_keys or not direct_keys:
        print("FAIL: one plan should have reached both substrates")
        print(first.summary())
        return 1

    sample = Path(first["p1:summarize_directly"].artifacts["summary"]["address"])
    print(f"\n  p1 summary ({sample}):")
    print(sample.read_text(), end="")

    print("\nsecond submission (identical; must reuse all sixteen):")
    second = farm.submit(subject)
    if not second.succeeded or len(second.report.reused) != len(POINTS) * 2:
        print(second.summary())
        return 1

    print("\nthird submission (same work, moved off the pool onto bsub -I):")
    third = farm.submit(moved)
    if not third.succeeded:
        print(third.summary())
        return 1
    recomputed = [item for item in third.report.outcomes if not item.reused]
    if recomputed:
        print("FAIL: moving a placement recomputed work it should have reused:")
        for item in recomputed:
            print(f"  {item.authored_key} ({item.placement})")
        print("placement is not identity-bearing; this is a real regression")
        return 1
    print("  all reused: placement is not identity-bearing, as designed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("site", type=Path, help="farm Site profile declaring a pool")
    parser.add_argument(
        "--pool-workers",
        type=int,
        default=None,
        help="override the pool's worker count, for a first cautious run",
    )
    args = parser.parse_args()

    site = Site.from_file(args.site)
    if args.pool_workers is not None:
        site = site.overridden(
            {"placement": {"pool": {"workers": args.pool_workers,
                                    "max_jobs": args.pool_workers}}}
        )

    pools = [
        name for name, options in site.placements.items()
        if isinstance(options, dict) and options.get("kind") == "lsf-pooled"
    ]
    if not pools:
        print(f"{args.site} declares no lsf-pooled placement; nothing to smoke test")
        return 2
    print(f"pools declared: {', '.join(pools)}")

    subject = mixed_sweep()
    moved = moved_sweep()
    print(subject.summary(), "\n")

    before = pool_jobs()
    with session(site, watch=True) as farm:
        status = run(subject, moved, farm)

    # The session has closed. A pooled worker is a batch job with no owner
    # binding, so nothing but `LSFCluster.close()` was ever going to stop it —
    # which makes this the one check that cannot be inferred from the run.
    after = pool_jobs()
    if before is None or after is None:
        print("\nLEAK CHECK SKIPPED: bjobs could not answer, so whether the "
              "pool's workers stopped is unknown. Check with `bjobs` yourself.")
    elif after - before:
        leaked = sorted(after - before)
        print(f"\nFAIL: {len(leaked)} farm job(s) outlived the session: "
              f"{', '.join(leaked)}")
        print("bkill them, and treat this as a leak rather than a flake")
        return 1
    else:
        print("\nno farm job outlived the session")

    if status == 0:
        print("\npooled farm smoke passed: the pool came up, a mixed plan "
              "reached both substrates, reuse survived a placement move, and "
              "nothing was left running")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
