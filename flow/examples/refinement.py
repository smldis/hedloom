"""Author a refinement study as an inspectable Plan, and run none of it.

    PYTHONPATH=src python examples/refinement.py | python -m json.tool

The same study as [`../../examples/grid_refinement.py`](../../examples/grid_refinement.py)
— the trapezoid rule over a definite integral, at three grid densities — stopped
one layer earlier. This unit authors and normalizes; it has no executor and
never calls a body. So what the example prints is the Plan document, and every
body below refuses to be called: reaching a valid Plan is the evidence that
authoring recorded the calls instead of running them.

What the document shows, and what to look for in it:

* **Every invocation is keyed.** `refine-grid`, `point-coarse`, `integrate-point`
  — authored names, not positions, so inserting a point does not renumber the
  others and cost their reuse downstream.
* **Outputs are declared where the operation is.** `integrate` says it writes
  `quadrature.txt`; `compare` says its result *is* the return value. The layer
  above reads those same declarations to know where to look, which is why a
  body and its executor cannot drift apart.
* **Fan-in is ordered.** `compare` takes `artifacts(...)`, and the Plan records
  one positioned edge per member, so `coarse, medium, fine` is a fact of the
  document rather than of the dict that happened to build it.
* **The external input is an address, not a path.** Nothing here resolves it —
  this unit does no I/O — so the site that runs the Plan says what
  `repository-relative` means.

Ordinary Python decides the shape: `include_refinements` selects one point or
three, and the difference is visible in the Plan before anything is spent.
"""

from __future__ import annotations

from hedloom_flow import (
    address,
    artifact,
    artifacts,
    flow,
    input_artifact,
    operation,
    parameter,
    planned,
)
from hedloom_flow.authoring import file, returned


POINTS = artifact("refinement-points")
QUADRATURE = artifact("quadrature-result")
VERDICT = artifact("refinement-verdict")

# The refinements, coarsest first. Each is four times the last, which is what
# makes the answer checkable: the trapezoid rule is second order, so every
# refinement by four should shrink the error by sixteen.
COARSE_STEPS = 8
REFINEMENT_STEPS = {"medium": 32, "fine": 128}


@operation(
    name="example.integrate",
    inputs={"points": POINTS},
    config={"point": parameter(str), "steps": parameter(int)},
    outputs={"result": file("quadrature.txt", kind="quadrature-result")},
)
def integrate(points, *, point, steps):
    """Declare a body that writes a file; planning must never call it.

    The runnable form of this operation is `integrate` in
    `../../examples/grid_refinement.py`, where it returns `shell("awk", ...)`
    and the declared `quadrature.txt` really lands.
    """

    raise AssertionError("operation bodies must not execute while planning")


@operation(
    name="example.compare",
    inputs={"results": artifacts("quadrature-result")},
    outputs={"verdict": returned(kind="refinement-verdict")},
)
def compare(results):
    """Declare an ordered fan-in whose result is the return value itself."""

    raise AssertionError("operation bodies must not execute while planning")


@flow(name="example.refine_one_point")
def refine_one_point(points, *, point, steps):
    """One operation behind a visible per-point boundary.

    A flow this small is still worth authoring: it gives each point its own
    boundary in the document, which is what lets one point be discussed,
    rerun, or placed without naming the invocation inside it.
    """

    return integrate.named("integrate-point")(points, point=point, steps=steps)


@flow(name="example.refine_grid")
def refine_grid(points, *, include_refinements):
    """Use ordinary Python to select a static graph, then reduce its results.

    The loop is Python's, and it runs while authoring — so the graph is settled
    before anything is spent, and no result decides what runs next. Keys are
    written out here rather than swept; `sweep(...)` is the shorthand for the
    same thing, and `../../examples/grid_refinement.py` uses it.
    """

    grids = {}
    results = []

    coarse = refine_one_point.named("point-coarse")(
        points,
        point="coarse",
        steps=COARSE_STEPS,
    )
    grids["coarse"] = coarse
    results.append(coarse)

    if include_refinements:
        for name, steps in REFINEMENT_STEPS.items():
            refined = refine_one_point.named(f"point-{name}")(
                points,
                point=name,
                steps=steps,
            )
            grids[name] = refined
            results.append(refined)

    verdict = compare.named("compare-refinements")(results)
    return {"points": grids, "verdict": verdict}


@planned
def build_refinement_plan(*, include_refinements: bool = True):
    """One validated Plan containing the complete authored graph.

    `@planned` makes calling this build one: the body records rather than runs,
    and what it returns names the plan's outputs.
    """

    points = input_artifact(
        address("repository-relative", "inputs/refinement-points.json"),
        artifact=POINTS,
    )
    return refine_grid.named("refine-grid")(
        points,
        include_refinements=include_refinements,
    )


def main() -> None:
    print(build_refinement_plan().to_json())


if __name__ == "__main__":
    main()
