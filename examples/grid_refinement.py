"""One file: author a grid-refinement sweep, inspect it, run it on real awk.

    python examples/grid_refinement.py

Small deliberately — the trapezoid rule applied to a definite integral whose
value is analytic, so the result can be checked by hand rather than believed.
What it demonstrates is the whole path in one place: operation bodies that
really run, declared outputs that really land, a placement per invocation,
content-addressed reuse on a second run, and one edited point rerunning only
itself.

    first run   3 grids integrated
    second run  3 grids reused, nothing recomputed

The answer is checkable twice over. The integral of exp(-x) over [0, 1] is
1 - 1/e, and the trapezoid rule's error is h^2/12 * (f'(b) - f'(a)) — so each
refinement by four should shrink the error by sixteen. A run that does not
show that has a real defect, not a tolerance to widen.
"""

from __future__ import annotations

from pathlib import Path
import math
import shutil
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
for unit in ("flow", "exec", "run"):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / unit / "src"))

from hedloom import (  # noqa: E402
    Site,
    artifact,
    artifacts,
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

GRID = artifact("grid-declaration")
QUADRATURE = artifact("quadrature-result")
ESTIMATE = artifact("integral-estimate")
VERDICT = artifact("refinement-verdict")

POINTS = ({"key": "coarse", "steps": 8}, {"key": "medium", "steps": 32},
          {"key": "fine", "steps": 128})

LOWER = 0.0
UPPER = 1.0

RULE = Path(__file__).resolve().parent / "trapezoid.awk"

GRID_TEMPLATE = """# trapezoid grid, {key}
a={lower}
b={upper}
steps={steps}
"""


@operation(config={"key": parameter(str), "steps": parameter(int)},
           outputs={"grid": file("grid.txt", kind="grid-declaration")})
def write_grid(out, *, key: str, steps: int) -> None:
    """A body that writes a declared file. `out.grid` is this attempt's own."""

    out.grid.write_text(
        GRID_TEMPLATE.format(key=key, lower=LOWER, upper=UPPER, steps=steps)
    )


@operation(inputs={"grid": GRID},
           outputs={"result": file("quadrature.txt", kind="quadrature-result")})
def integrate(grid, out):
    """A launcher. The command runs at this invocation's placement."""

    # The tool writes the declared file itself rather than printing: the point
    # of a declared artifact is that another operator can read it without our
    # code, and stdout is diagnostics until an operation says otherwise.
    return shell("awk", "-f", RULE, "-v", f"out={out.result}", grid)


@operation(inputs={"result": QUADRATURE},
           outputs={"estimate": returned(kind="integral-estimate")})
def estimate(result) -> float:
    """A value-returning body: nothing is written, the number is the result."""

    text = Path(result).read_text().strip()
    if not text:
        raise ValueError(f"{result} carries no quadrature result to read")
    return float(text)


@operation(inputs={"estimates": artifacts("integral-estimate")},
           outputs={"verdict": returned(kind="refinement-verdict")})
def compare(estimates: list) -> dict:
    """The study's conclusion, computed rather than transcribed."""

    exact = math.exp(-LOWER) - math.exp(-UPPER)
    errors = [abs(value - exact) for value in estimates]
    return {
        "exact": exact,
        "estimates": list(estimates),
        "worst_error_pct": max(error / exact * 100.0 for error in errors),
        # Second order: refining by four should shrink the error by sixteen.
        "order_ratios": [
            previous / current
            for previous, current in zip(errors, errors[1:])
            if current > 0.0
        ],
    }


@flow
def refinement_sweep(points):
    """One keyed scope per point; every call inside gets a stable identity."""

    measured = []
    for point in sweep(points, key="key"):
        grid = write_grid(key=point["key"], steps=point["steps"])
        measured.append(estimate(integrate(grid)))
    return {"verdict": compare.named("compare")(measured).verdict}


@study(default_policy=local())
def grid_refinement():
    """The study: every point swept, in this process."""

    return refinement_sweep.named("refinement")(POINTS)


def main() -> int:
    if shutil.which("awk") is None:
        print("awk is not on PATH; this example needs a real external tool")
        return 1

    here = Path(__file__).resolve().parent
    work = here / "_runs"
    site = Site(
        root=str(work / "attempts"),
        workspace_root=str(work / "work"),
        address_spaces={"repository-relative": str(here)},
    )

    subject = grid_refinement()
    print(subject.summary(), "\n")

    run = subject.submit(site=site, watch=True)
    print("\nconclusion:", run.value)
    print("coarse grid estimated", run["coarse:estimate"].value)
    return 0 if run.succeeded else 1


if __name__ == "__main__":
    raise SystemExit(main())
