"""Author a flow, plan it, execute it, edit one input, and rerun.

This is the first end-to-end slice across both units: Hedloom Flow authors and
normalizes a static Plan, Hedloom Exec derives content-addressed bundles from
that Plan document and executes them with durable records.

The point of the demonstration is the third run. Refine one point's grid and
rerun: that point and the reduction downstream of it recompute, the untouched
points are reused from their published manifests, and the superseded result is
still on disk and nameable rather than overwritten. Nothing is asked to declare
that it changed — the digest of what went in is what decides.

The work is the trapezoid rule over a definite integral whose value is
analytic, so a reused result and a recomputed one can be checked against each
other rather than trusted. `../../examples/grid_refinement.py` is the same
study one layer up, where the integration is done by real `awk` on whatever
placement the site names; it gets the same numbers.

Run it from the unit directory with both source trees on the path:

    PYTHONPATH=src:../flow/src python examples/planned_refinement.py
"""

from __future__ import annotations

import math
import shutil
import sys
import tempfile

try:
    from hedloom_flow import (
        address,
        artifact,
        artifacts,
        codec,
        flow,
        input_artifact,
        materialization,
        operation,
        parameter,
        planned,
    )
    from hedloom_flow.authoring import returned
except ModuleNotFoundError:  # pragma: no cover - guidance, not logic
    sys.exit(
        "hedloom_flow is required for this example.\n"
        "Run: PYTHONPATH=src:../flow/src python "
        "examples/planned_refinement.py"
    )

from hedloom_exec.durability import Durability, execute
from hedloom_exec.planned import plan_bundles
from hedloom_exec.reuse import describe_staleness, scan_attempts, stale_attempts
from hedloom_exec.transport import InProcessTransport

PLAN_ID = "refinement"
POINTS = {"coarse": 8, "medium": 32, "fine": 128}

# The integral is exp(-x) over [0, 1], whose exact value is 1 - 1/e. Declared
# here rather than measured so the estimates below have something to be wrong
# against.
LOWER = 0.0
UPPER = 1.0
EXACT = math.exp(-LOWER) - math.exp(-UPPER)


@operation(
    inputs={"grid": artifact("grid-declaration")},
    config={"point": parameter(str), "steps": parameter(int)},
    outputs={"result": returned(kind="quadrature-result")},
)
def integrate(grid, *, point, steps):
    raise AssertionError("operation bodies do not run during planning")


@operation(
    inputs={"results": artifacts("quadrature-result")},
    outputs={"verdict": returned(kind="refinement-verdict")},
)
def compare(results):
    raise AssertionError("operation bodies do not run during planning")


@flow
def refine(grid, *, points):
    results = [
        integrate.named(f"point-{name}")(grid, point=name, steps=steps)
        for name, steps in points.items()
    ]
    return compare.named("compare")(results)


@planned
def refinement(points):
    """The plan. Calling this builds one; nothing inside it runs."""

    grid = input_artifact(
        address("repository-relative", "inputs/grid.json"),
        artifact=artifact("grid-declaration"),
        materialized_as=materialization(
            address_space="repository-relative",
            codec=codec("json", encoding="utf-8"),
            access_scope="repository-checkout",
        ),
    )
    return {"verdict": refine(grid, points=points)}


def build_plan(points):
    """The document, which is what this example's executor consumes."""

    return refinement(points).to_data()


# Implementations. Deliberately arithmetic rather than a tool: the slice under
# demonstration is identity and reuse, not what the number means.
def integrate_impl(*, point, steps, grid=None):
    width = (UPPER - LOWER) / steps
    total = (math.exp(-LOWER) + math.exp(-UPPER)) / 2.0
    for index in range(1, steps):
        total += math.exp(-(LOWER + index * width))
    return {"point": point, "steps": steps, "estimate": total * width}


def compare_impl(*, results=None):
    estimates = [item["estimate"] for item in (results or [])]
    return {
        "points": len(estimates),
        "worst_error": max((abs(value - EXACT) for value in estimates), default=None),
    }


def run(document, root, label):
    transport = InProcessTransport(
        {"__main__.integrate": integrate_impl, "__main__.compare": compare_impl}
    )
    values: dict[str, object] = {}
    print(f"\n{label}")

    for item in plan_bundles(document):
        bundle = dict(item.bundle)
        resolved: dict[str, object] = {}
        for name, reference in item.bundle["inputs"].items():
            if isinstance(reference, list):
                resolved[name] = [values.get(entry) for entry in reference]
            elif reference.startswith("output:"):
                resolved[name] = values.get(reference)
        bundle["resolved_inputs"] = resolved

        result = execute(
            transport,
            bundle,
            durability=Durability.RECORDED,
            root=root,
            plan_id=PLAN_ID,
            invocation_id=item.invocation_id,
        )
        verb = "reused " if result.disposition == "completed" else "ran    "
        print(f"  {verb} {item.authored_key:<14} {item.input_digest[:12]}")

        for output in ("result", "verdict"):
            values[f"output:{item.input_digest}:{output}"] = result.value

    return values


def main():
    root = tempfile.mkdtemp(prefix="hedloom-exec-example-")
    try:
        first = build_plan(POINTS)
        run(first, root, "First run — nothing is published yet")
        run(first, root, "Second run — unchanged inputs, nothing recomputes")

        edited = dict(POINTS, fine=512)
        second = build_plan(edited)
        values = run(second, root, "Third run — the fine grid refined to 512 steps")

        verdicts = [value for key, value in values.items() if "verdict" in key]
        print(f"\n  final verdict: {verdicts[-1] if verdicts else None}")

        known = scan_attempts(root)
        superseded = [
            record
            for item in plan_bundles(second)
            for record in stale_attempts(
                root,
                plan_id=PLAN_ID,
                invocation_id=item.invocation_id,
                current_digest=item.input_digest,
                records=known,
            )
        ]
        print("\n  superseded but retained:")
        print("   " + describe_staleness(superseded).replace("\n", "\n   "))
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
