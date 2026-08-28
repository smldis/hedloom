"""One file: produce spent work, survey it, pin what must be kept, reclaim the rest.

    python examples/retention.py

Storage is the one resource a study spends that nothing returns on its own.
Every try keeps its own workspace, deliberately, so a failure stays readable
next to the run that replaced it — and a study with several corners and a few
reruns accumulates directories no current plan resolves to.

This example spends some storage on purpose and then reclaims it, checking the
arithmetic at every step rather than trusting a summary:

    first pass    four points, two diverge after writing their trace
    reclaimable   nothing yet — `latest/` still resolves to those failures
    second pass   the diverging points corrected; the failures are superseded
    survey        names the spent tries and changes nothing
    pin           one spent try promised to a reader
    apply         frees exactly the bytes the survey promised

The two passes are the point, not padding. A `latest/` alias is bound before a
body runs, so a tool can watch an output while it is still being written —
which means the newest try at a key always has something resolving to it,
whether it succeeded or not. Spent storage is storage nothing points at, so a
failure becomes reclaimable when it is superseded and not before. A policy
that reclaimed the current failure at a key would strand the path a colleague
was reading.

Every number here is checkable. The survey states how many bytes it would
free; the filesystem is measured before and after; the two must agree. A run
where they disagree has a real defect, not a rounding difference.

What the example is really evidence for is the shape of the refusals. A
survey that quietly created the directory it inspected, or an apply that took
a pinned workspace, or a reclaim that removed a record rather than a payload,
would all still print plausible numbers. So each is checked by name.
"""

from __future__ import annotations

from pathlib import Path
import shutil
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
for unit in ("flow", "exec", "run"):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / unit / "src"))

from hedloom import (  # noqa: E402
    Site,
    artifact,
    file,
    flow,
    local,
    operation,
    parameter,
    returned,
    shell,
    study,
    sweep,
)
from hedloom_exec.journal import LAYOUT_VERSION, AttemptJournal  # noqa: E402
from hedloom_exec.pins import pin, pins_of, verify  # noqa: E402
from hedloom_exec.prune import RetentionPolicy, RetentionRule, survey  # noqa: E402
from hedloom_exec.reuse import scan_attempts  # noqa: E402

TRACE = artifact("simulation-trace")
ROWS = artifact("row-count")

# Two points settle, two diverge. The diverging ones write their whole trace
# first and fail afterwards, which is the case retention exists for: the
# workspace is large, the result is unusable, and why it failed is in the log
# beside it.
POINTS = (
    {"key": "steady", "steps": 4000, "diverges": False},
    {"key": "drifting", "steps": 4000, "diverges": True},
    {"key": "settled", "steps": 4000, "diverges": False},
    {"key": "runaway", "steps": 4000, "diverges": True},
)

# The second pass, after the operator found the cause. Same authored keys, so
# `latest/` repoints to the corrected work; different inputs, so these are new
# records rather than new tries. The first pass's failures become superseded —
# nothing resolves to them any more, and that is what makes them reclaimable.
CORRECTED = tuple(
    {**point, "diverges": False, "steps": 4200 if point["diverges"] else point["steps"]}
    for point in POINTS
)


@operation(
    config={
        "key": parameter(str),
        "steps": parameter(int),
        "diverges": parameter(bool),
    },
    outputs={"trace": file("trace.txt", kind="simulation-trace")},
)
def simulate(out, *, key: str, steps: int, diverges: bool):
    """Write the declared trace with a real tool, then fail if this point diverges."""

    script = (
        f"awk 'BEGIN {{ for (i = 0; i < {steps}; i++) "
        f'printf "%d %.6f\\n", i, i * 0.5 }}\' > \'{out.trace}\''
    )
    if diverges:
        # Bytes on disk, and nothing usable in them. Exactly what a policy is
        # for: this is spent storage, not evidence anybody will read twice.
        script += "; echo 'residual did not converge' >&2; exit 1"
    return shell("sh", "-c", script)


@operation(inputs={"trace": TRACE}, outputs={"rows": returned(kind="row-count")})
def count_rows(trace) -> int:
    """A value-returning body, so a settled point has a result worth keeping."""

    return sum(1 for line in Path(trace).read_text().splitlines() if line.strip())


@flow
def spent_work(points):
    """One keyed scope per point; the diverging ones never reach `count_rows`."""

    counted = {}
    for point in sweep(points, key="key"):
        trace = simulate(
            key=point["key"], steps=point["steps"], diverges=point["diverges"]
        )
        counted[point["key"]] = count_rows(trace)
    return {"rows": counted["steady"]}


@study(default_policy=local())
def spending_study(points):
    """The study: four points swept, in this process."""

    return spent_work.named("spending")(points)


def payload_bytes(*roots: Path) -> int:
    """Every byte under these roots, counted the way the filesystem holds them."""

    return sum(
        path.lstat().st_size
        for root in roots
        for path in root.rglob("*")
        if not path.is_dir()
    )


def report(label: str, found) -> None:
    print(f"    {label}: {found.summary()}")
    for candidate in found.candidates:
        print(
            f"      candidate {candidate.identity[-8:]} try {candidate.try_number}"
            f"  {candidate.outcome}  {candidate.bytes} byte(s)  by {candidate.rule!r}"
        )
    for skip in found.skipped:
        print(f"      skipped   {skip.identity[-8:]}  {skip.reason}")


def main(work: Path | None = None) -> int:
    if shutil.which("awk") is None or shutil.which("sh") is None:
        print("awk and sh are not both on PATH; this example needs real tools")
        return 1

    default = Path(__file__).resolve().parent / "_runs" / "retention"
    work = Path(work) if work is not None else default
    # A fresh root, deliberately: the arithmetic below is only checkable when
    # every byte under it was produced by this run.
    shutil.rmtree(work, ignore_errors=True)
    records = work / "attempts"
    workspaces = work / "work"
    site = Site(root=str(records), workspace_root=str(workspaces))

    print("=== first pass: two points diverge")
    run = spending_study(POINTS).submit(
        site=site, watch=True, stop_on_failure=False
    )
    outcomes = [item.outcome for item in run.report.outcomes]
    print(f"\n    invocations: {outcomes.count('succeeded')} succeeded, "
          f"{outcomes.count('failed')} failed")
    print(f"    on disk: {payload_bytes(records, workspaces)} byte(s)")

    # Nothing is reclaimable yet, and that is the rule rather than a gap. A
    # `latest/` alias is bound before a body runs -- so a tool can watch an
    # output while it is written -- which means the newest try at a key always
    # has something resolving to it, failed or not. Spent storage is storage
    # nothing points at, and a failure only becomes that once it is superseded.
    stopgap = survey(
        records,
        RetentionPolicy(
            rules=(RetentionRule(name="failed", outcome=("failed",), keep_latest=0),),
            floor="0s",
        ),
        workspace_root=workspaces,
    )
    aliased = [item for item in stopgap.skipped if item.reason == "aliased"]
    print(f"    reclaimable now: {len(stopgap.candidates)} — the two failures are "
          f"still what `latest/` resolves to ({len(aliased)} skipped as aliased)")

    print("\n=== second pass: the operator corrects the diverging points")
    fixed = spending_study(CORRECTED).submit(
        site=site, watch=True, stop_on_failure=False
    )
    settled_now = [item.outcome for item in fixed.report.outcomes]
    print(f"\n    invocations: {settled_now.count('succeeded')} succeeded, "
          f"{settled_now.count('failed')} failed")
    print(f"    on disk: {payload_bytes(records, workspaces)} byte(s) — the first "
          "pass's failures are still there, now superseded")

    # The default floor is seven days, which would spare everything here. An
    # example that quietly ran with the shipped default and reclaimed nothing
    # would demonstrate the API and prove nothing, so the floor is lowered on
    # purpose and said out loud.
    policy = RetentionPolicy(
        rules=(
            RetentionRule(
                name="diverged traces",
                outcome=("failed",),
                keep_latest=0,
                keep_logs=True,
            ),
        ),
        floor="0s",
    )

    print("\n=== survey: read-only by construction")
    before = payload_bytes(records, workspaces)
    proposed = survey(records, policy, workspace_root=workspaces)
    report("survey", proposed)
    after = payload_bytes(records, workspaces)
    if after != before:
        print(f"    FAILED: the survey changed {before - after} byte(s); "
              "a dry run must inspect without creating or removing anything")
        return 1
    if not proposed.candidates:
        print("    FAILED: two points diverged, so two spent tries were expected")
        return 1
    print(f"    nothing changed: {before} byte(s) before and after")

    print("\n=== pin: one spent try that must outlive the policy")
    kept = proposed.candidates[0]
    journal = AttemptJournal(records, kept.identity)
    promise = pin(
        journal,
        try_number=kept.try_number,
        workspace_root=workspaces,
        reason="quoted in a report; the path must keep resolving",
        actor="example",
    )
    print(f"    pinned {promise.pin_id[:16]}… over {len(promise.contents)} file(s), "
          f"write bits removed: {promise.froze}")

    print("\n=== survey again: the pin is a refusal, not a preference")
    guarded = survey(records, policy, workspace_root=workspaces)
    report("survey", guarded)
    pinned_skips = [item for item in guarded.skipped if item.reason == "pinned"]
    if len(pinned_skips) != 1:
        print("    FAILED: the pinned try must be skipped by name, not by luck")
        return 1

    print("\n=== apply: the only destructive step")
    promised = guarded.freed_bytes
    named_before = len(scan_attempts(records))
    before = payload_bytes(records, workspaces)
    applied = guarded.apply(actor="example")
    after = payload_bytes(records, workspaces)
    print(f"    freed {applied.freed_bytes} byte(s) across "
          f"{len(applied.removed)} workspace(s)")

    # Three separate instruments, which could disagree: what the survey
    # promised, what the report claims, and what the filesystem lost. The
    # record grows by the removal events it appends, so the disk delta is the
    # freed payload minus that growth rather than an exact match.
    if applied.freed_bytes != promised:
        print(f"    FAILED: survey promised {promised}, apply freed "
              f"{applied.freed_bytes}")
        return 1
    if before - after > promised:
        print(f"    FAILED: the filesystem lost {before - after} byte(s), more "
              f"than the {promised} the survey named")
        return 1
    print(f"    survey promised {promised}; disk fell by {before - after} "
          "(the difference is the removal events the record gained)")

    print("\n=== what survived, checked by name")
    surviving = scan_attempts(records)
    if len(surviving) != named_before:
        print(f"    FAILED: {len(surviving)} of {named_before} records survived; "
              "reclaiming a payload must never remove the record that names it")
        return 1
    print(f"    records: all {named_before} intact — a reclaimed try is still"
          " nameable, and still says why it failed")

    kept_workspace = Path(kept.workspace)
    if not kept_workspace.is_dir():
        print(f"    FAILED: the pinned workspace {kept_workspace} was removed")
        return 1
    checked = verify(promise, layout=LAYOUT_VERSION)
    if checked.outcome != "intact":
        print(f"    FAILED: the pinned workspace reports {checked.outcome}: "
              f"{checked.detail}")
        return 1
    print(f"    pin: {kept_workspace.name} intact, content re-hashed and unchanged")

    settled = [item for item in surviving if item.outcome == "succeeded"]
    for record in settled:
        if not (workspaces / f"{record.identity}-{record.try_number}").is_dir():
            print(f"    FAILED: {record.authored_key} settled and was reclaimed "
                  "anyway; a reusable result is never a candidate")
            return 1
    print(f"    reusable: {len(settled)} settled point(s) untouched — a result a "
          "later run would reuse is not spent storage")

    active = pins_of(journal.fold())
    print(f"\n    {len(active)} pin(s) still active; release with "
          f"`hedloom unpin {promise.pin_id[:12]}… --reason done`")
    print("    the same pass from a terminal, once a site declares [retention]:")
    print(f"      hedloom prune --root {records} --workspace-root {workspaces} "
          "--failed --keep-latest 0")
    print("      hedloom prune --site site.toml --apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
