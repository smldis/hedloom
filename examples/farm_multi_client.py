"""Concurrent studies against one farm budget, and one study root.

    python examples/farm_multi_client.py --queue reg --max-jobs 2

`examples/farm_smoke.py` answers "does one study reach the farm and come back".
This one answers what is shared when more than one run is in flight — a
colleague on the same submit host, a second study, or the same study started
twice. It reuses that example's operations, so the two agree about what a farm
job is by construction rather than by copy.

Its site is built in Python rather than read from a profile, which is the other
half of `examples/farm_smoke.py`. A profile is right there: a queue, a walltime
and a farm share belong to an installation and get copied and edited per site.
Here the site *is* the experiment — three arrangements that differ in one
declared number — so writing it as arguments is both shorter and more honest
than three TOML files.

Three arrangements, each answering a question by measurement:

  one session, two studies    the placement budget is shared        (8 jobs)
  one session, one study twice the Dask key namespace is shared     (4 jobs)
  two sessions, one study     only the study root is shared         (4 jobs)

A fourth pass resubmits all of it and must spend nothing.

Concurrency is measured from the journals, not from this process: every attempt
records `submit_intent` before the transport is touched and `submit_receipt`
when it returns, and `bsub -I` returns only when the job is over. The interval
between them is the window that job held a share of the farm, on any substrate,
whether or not the run that started it is still alive.
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
for unit in ("flow", "exec", "run"):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / unit / "src"))

from hedloom import Site, session, study  # noqa: E402
from hedloom_exec.journal import AttemptJournal  # noqa: E402

# The operations under test are the farm smoke test's, imported rather than
# copied: an edit to one is an edit to both, which is what keeps them agreeing.
# It also means editing that file reruns this example's attempts, because an
# operation's source is part of what its identity is derived from.
from farm_smoke import range_sweep  # noqa: E402

# A different base on every run, so a rerun measures fresh farm work rather
# than reusing the last run's and reporting that it spent nothing. Reuse is
# proved deliberately, in the fourth pass, and never by accident.
RUN = int(time.time()) % 10_000
BASES = {
    name: RUN * 100 + index * 20
    for index, name in enumerate(("north", "south", "shared", "contended"))
}


def points(prefix: str, base: int) -> tuple[dict[str, Any], ...]:
    """Two independent chains, so a study is four farm jobs and no more."""

    # Underscores, not hyphens: a flow's outputs are named by these keys and a
    # flow output name has to be a Python identifier.
    return (
        {"key": f"{prefix}_slow", "start": base, "count": 3, "delay_s": 0.8},
        {"key": f"{prefix}_fast", "start": base + 10, "count": 2, "delay_s": 0.1},
    )


@study
def sweep_for(name: str):
    """One study per name. A different base is different inputs, so new attempts.

    `@study` makes the function a family: calling it plans, and what comes back
    is inspectable before anything is spent. Nothing in here runs — the flow
    call records a scope and the operations inside it record invocations.
    """

    return range_sweep.named(f"multi-{name}")(points(name, BASES[name]))


def site_for(root: Path, *, queue: str, cap: int) -> Site:
    """The installation, as Python rather than as a file.

    One declaration per placement: the budget and the substrate together, in the
    same vocabulary a profile's `[placement.lsf]` table uses. Declaring them
    apart — a transport here, a capacity there — is what used to let a typo
    produce a placement with a budget and no way to reach it.
    """

    return Site(
        root=str(root / "attempts"),
        workspace_root=str(root / "work"),
        placements={
            "lsf": {
                "kind": "lsf-interactive",
                "queue": queue,
                "walltime": "1",
                "cores": 1,
                "timeout": 300,
                "max_jobs": cap,
            },
        },
        # Local concurrency on the submit host, which is a different machine's
        # problem from the number above. Loopback because this holds two
        # clusters at once and a dashboard on a fixed port would collide.
        threads=2,
        dashboard="loopback",
    )


# ---------------------------------------------------------------------------
# Measurement: what the durable record says happened, not what this run thinks.


def _at(record: dict[str, Any]) -> float:
    return datetime.fromisoformat(record["at"]).timestamp()


def farm_spans(root: str, since: float) -> list[tuple[float, float]]:
    """Every farm job started after `since`, as the window it held the farm.

    A reused attempt has no `submit_intent` of its own and contributes nothing,
    so the length of this list is exactly what a pass spent.
    """

    spans: list[tuple[float, float]] = []
    for events in sorted(Path(root).rglob("events.jsonl")):
        intent: float | None = None
        receipt: float | None = None
        for line in events.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            if record["event"] == "submit_intent":
                intent, receipt = _at(record), None
            elif record["event"] == "submit_receipt" and intent is not None:
                receipt = _at(record)
        if intent is not None and intent >= since:
            # An attempt whose submission never returned is still holding the
            # farm as far as anyone can tell, so it runs to the end of the pass.
            spans.append((intent, receipt if receipt is not None else time.time()))
    return spans


def peak_overlap(spans: Sequence[tuple[float, float]]) -> int:
    """The most farm jobs that were ever in flight at the same moment."""

    edges = sorted([(start, 1) for start, _ in spans] + [(end, -1) for _, end in spans])
    peak = running = 0
    for _, delta in edges:
        running += delta
        peak = max(peak, running)
    return peak


def disagreements(root: str) -> list[str]:
    """Attempts whose journal and whose manifest name different outcomes.

    `execute` reports the outcome from the folded journal and the artifacts from
    the published manifest. They are written by `publish_terminal`, which runs
    outside the attempt claim, so two callers reconciling one attempt can
    interleave their two writes — see `docs/attempt-claim-protocol.md`, where a
    TLA+ model reaches that state in eighteen steps. Nothing below should ever
    make it happen; this is here so that if it does, the run says so instead of
    returning a result under someone else's verdict.
    """

    broken = []
    for directory in sorted(Path(root).iterdir()):
        if not (directory / "events.jsonl").exists():
            continue
        journal = AttemptJournal(root, directory.name)
        published = journal.read_manifest()
        recorded = journal.fold().outcome
        if recorded is None and published is None:
            continue
        visible = published.get("outcome") if published else None
        if recorded != visible:
            broken.append(f"{directory.name}: journal {recorded!r}, manifest {visible!r}")
    return broken


def announce(label: str | None = None) -> Callable[[Any], None]:
    prefix = f"[{label}] " if label else "  "

    def report(outcome: Any) -> None:
        name = outcome.authored_key or outcome.invocation_id
        detail = f"  {outcome.error}" if outcome.error else ""
        print(f"{prefix}{outcome.disposition:>9} {name:<34}{outcome.outcome}{detail}")

    return report


def failures(run: Any) -> list[str]:
    return [
        f"{outcome.authored_key}: {outcome.outcome}"
        for outcome in run.report.outcomes
        if outcome.outcome != "succeeded"
    ]


# ---------------------------------------------------------------------------
# The three arrangements.


def shared_budget(site: Site, cap: int) -> bool:
    """One session, two studies.

    The question an operator actually has: if a second study starts while mine
    is running, do we each get the farm share the site declares, or do we share
    one? A session is one cluster, and a placement's budget belongs to that
    cluster's workers, so `submit_all` cannot put more on the farm than the site
    declared however many studies it is given.
    """

    print("\n=== one session, two studies")
    print(f"    eight jobs wanted, {cap} may be in flight")
    since = time.time()
    with session(site) as farm:
        runs = farm.submit_all(
            {name: sweep_for(name) for name in ("north", "south")},
            on_event=announce(),
        )

    spans = farm_spans(site.root, since)
    peak = peak_overlap(spans)
    print(f"    farm jobs: {len(spans)}   peak in flight: {peak}   declared cap: {cap}")
    for label, run in runs.items():
        for problem in failures(run):
            print(f"    {label} failed: {problem}")
    if any(failures(run) for run in runs.values()):
        return False
    if len(spans) != 8:
        print(f"    FAILED: expected eight farm jobs, the record shows {len(spans)}")
        return False
    if peak > cap:
        print(f"    FAILED: {peak} jobs were in flight at once, over a cap of {cap}")
        return False
    if peak <= 1:
        # Not a correctness failure, but nothing was concurrent, so the pass
        # proves nothing about a shared budget.
        print("    INCONCLUSIVE: never more than one job in flight; the farm was "
              "not busy enough for the two studies to overlap")
        return False
    print(f"    both studies finished, and {peak} in flight never exceeded {cap}:")
    print("    one session is one budget, however many studies draw on it")
    return True


def same_work_twice(site: Site) -> bool:
    """One session, the same study submitted twice at once.

    Dask's key namespace belongs to the scheduler, not to a submission. Both
    graphs carry identical task keys, so the second resolves to the first's
    tasks: the work runs once and both reports describe it. Worth knowing
    because it means the attempt claim is never even consulted here — there is
    only ever one caller — so this is Dask's idempotence, not hedloom's.
    """

    print("\n=== one session, the same study twice")
    print("    four jobs wanted, submitted twice")
    since = time.time()
    subject = sweep_for("shared")
    with session(site) as farm:
        runs = farm.submit_all(
            {"first": subject, "second": subject}, on_event=announce()
        )

    spans = farm_spans(site.root, since)
    print(f"    farm jobs: {len(spans)} (eight would mean the work ran twice)")
    for label, run in runs.items():
        for problem in failures(run):
            print(f"    {label} failed: {problem}")
    if any(failures(run) for run in runs.values()):
        return False
    if len(spans) != 4:
        print(f"    FAILED: expected four farm jobs, the record shows {len(spans)}")
        return False
    print("    four jobs for two submissions: identical task keys are one task.")
    print("    Note both reports say 'claimed' — a submission is told the outcome")
    print("    of the work, not whether it was the one that caused it to run.")
    return True


def two_controllers(site: Site) -> bool:
    """Two sessions, the same study, one study root.

    Two people on two login hosts, or one study started twice. Nothing is shared
    but the root, so the Dask keys are in different namespaces and both callers
    really do reach the attempt protocol for the same identity. What prevents
    the duplicate here is the claim in the journal, and it refuses rather than
    waits.

    This is the one arrangement that still needs threads, and that is correct:
    it models two independent processes, and it should not read as routine. Note
    what is *not* shared — each session has its own cluster and therefore its own
    budget, so two controllers can put twice the declared cap on the farm.
    """

    print("\n=== two sessions, the same study")
    print("    four jobs wanted; the loser of each claim must not resubmit")
    since = time.time()
    subject = sweep_for("contended")
    runs: dict[str, Any] = {}

    def controller(label: str) -> None:
        with session(site) as farm:
            runs[label] = farm.submit(
                subject,
                # Both reports are wanted whole. Stopping at the first refusal is
                # the right default for a study that has gone wrong; here the
                # refusals are the evidence.
                stop_on_failure=False,
                on_event=announce(label),
            )

    threads = [threading.Thread(target=controller, args=(name,))
               for name in ("host-a", "host-b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    spans = farm_spans(site.root, since)
    print(f"    farm jobs: {len(spans)} (eight would mean the claim did not hold)")
    # Both reports first, because the accounting below is only meaningful when
    # every controller finished and can be asked what it saw.
    if len(runs) != 2:
        print(f"    FAILED: only {len(runs)} of two controllers produced a report")
        return False

    succeeded: set[str] = set()
    refused = 0
    for run in runs.values():
        for outcome in run.report.outcomes:
            key = outcome.authored_key or outcome.invocation_id
            if outcome.outcome == "succeeded":
                succeeded.add(key)
            elif "ConcurrentClaim" in (outcome.error or ""):
                refused += 1
    expected = {
        outcome.authored_key or outcome.invocation_id
        for run in runs.values()
        for outcome in run.report.outcomes
    }
    # Conservation of work is the property the protocol actually promises, so
    # it is checked before the job count. A refused caller is a legal ending --
    # the claim refuses rather than waits -- and the defect it could hide is an
    # invocation that no controller resolved at all. Checking the count first
    # would return before this ran, leaving a short count unable to say whether
    # work was lost or merely spent differently.
    missing = sorted(expected - succeeded)
    if missing:
        print(f"    FAILED: no controller produced a result for {', '.join(missing)}")
        print(f"            {refused} claim(s) refused; {len(spans)} farm job(s)")
        return False
    if len(spans) != 4:
        print(f"    FAILED: expected four farm jobs, the record shows {len(spans)}")
        print(f"            every invocation has a result and {refused} claim(s) "
              f"were refused, so no work was lost")
        return False
    print(f"    every invocation succeeded for exactly one controller; "
          f"{refused} claim(s) refused by name")
    print("    the journal claim is the backstop Dask cannot be: it works across")
    print("    processes, and it reports the second caller rather than waiting")
    return True


def all_reused(site: Site) -> bool:
    """One session, every study again. A concurrent mess must still leave a
    record that reuses cleanly, or none of the above was worth doing."""

    print("\n=== one session, all four studies again")
    since = time.time()
    with session(site) as farm:
        runs = farm.submit_all(
            {name: sweep_for(name) for name in BASES},
            on_event=lambda outcome: None,
        )

    spans = farm_spans(site.root, since)
    reused = sum(
        1
        for run in runs.values()
        for outcome in run.report.outcomes
        if outcome.disposition == "completed"
    )
    print(f"    farm jobs spent: {len(spans)}   invocations reused: {reused}/16")
    if any(failures(run) for run in runs.values()):
        for run in runs.values():
            for problem in failures(run):
                print(f"    failed: {problem}")
        return False
    if spans:
        print(f"    FAILED: {len(spans)} new farm job(s); everything should reuse")
        return False
    if reused != 16:
        print(f"    FAILED: {reused} of 16 invocations reused")
        return False
    print("    nothing spent, everything reused")
    return True


def main() -> int:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", default="reg", help="LSF queue to submit to")
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=2,
        help=(
            "farm jobs in flight per session. Deliberately smaller than this "
            "example can keep busy: four jobs are ready at once, so a cap of "
            "two is a cap that binds, and a cap of four would be satisfied "
            "without ever being tested"
        ),
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=here / "_runs" / "farm-multi-client",
        help="where attempts and workspaces go",
    )
    args = parser.parse_args()

    site = site_for(args.root, queue=args.queue, cap=args.max_jobs)
    print(f"study root: {site.root}")
    print(f"placement 'lsf': {site.capacity['lsf']} job(s) in flight per session")

    for act in (
        lambda: shared_budget(site, args.max_jobs),
        lambda: same_work_twice(site),
        lambda: two_controllers(site),
        lambda: all_reused(site),
    ):
        if not act():
            return 1

    broken = disagreements(site.root)
    if broken:
        print("\nFAILED: a journal and its manifest disagree:")
        for line in broken:
            print(f"    {line}")
        return 1

    print("\nmulti-client sweep passed:")
    print("  the placement budget is the session's, shared by every study on it")
    print("  identical work submitted twice to one session runs once")
    print("  identical work submitted from two sessions is refused, not duplicated")
    print("  and every attempt's record agrees with its published result")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
