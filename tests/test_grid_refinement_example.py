from __future__ import annotations

import math
import shutil
from pathlib import Path

import pytest

from hedloom import Site

from examples import grid_refinement


ROOT = Path(__file__).resolve().parents[1]

needs_awk = pytest.mark.skipif(
    shutil.which("awk") is None, reason="the example runs real awk"
)


def site_for(tmp_path: Path) -> Site:
    return Site(
        root=str(tmp_path / "attempts"),
        workspace_root=str(tmp_path / "work"),
        address_spaces={"repository-relative": str(ROOT / "examples")},
    )


@needs_awk
def test_the_integral_is_right_and_converges_at_second_order(tmp_path: Path) -> None:
    """The example's claim, checked against the closed form rather than itself.

    `grid_refinement.py` says the trapezoid error falls by sixteen for each
    refinement by four, and that the answer is analytic. Both are checkable
    without running anything twice, so a run that agrees with its own arithmetic
    but disagrees with calculus still fails here.
    """

    run = grid_refinement.grid_refinement().submit(site=site_for(tmp_path))
    assert run.succeeded, run.summary()

    verdict = run.outputs["verdict"].value
    exact = math.exp(-0.0) - math.exp(-1.0)
    assert verdict["exact"] == pytest.approx(exact)

    estimates = verdict["estimates"]
    assert len(estimates) == len(grid_refinement.POINTS)

    # The trapezoid rule overestimates a convex integrand, and refining must
    # move each estimate towards the exact value rather than merely change it.
    errors = [abs(value - exact) for value in estimates]
    assert errors == sorted(errors, reverse=True), (
        f"refinement did not converge: {errors}"
    )

    # Second order: four times the steps, a sixteenth of the error. The example
    # calls a run that misses this a real defect rather than a loose tolerance,
    # so the band here is narrow on purpose.
    assert verdict["order_ratios"] == [pytest.approx(16.0, rel=0.05)] * len(
        verdict["order_ratios"]
    )
    assert len(verdict["order_ratios"]) == len(grid_refinement.POINTS) - 1


@needs_awk
def test_a_second_submission_recomputes_nothing(tmp_path: Path) -> None:
    """Content-addressed reuse, measured from the report rather than the clock."""

    site = site_for(tmp_path)
    subject = grid_refinement.grid_refinement()

    first = subject.submit(site=site)
    assert first.succeeded, first.summary()
    assert not first.report.reused, "nothing can be reused on a first run"

    second = subject.submit(site=site)
    assert second.succeeded, second.summary()
    assert len(second.report.reused) == len(second.report.outcomes)
    assert second.outputs["verdict"].value == first.outputs["verdict"].value
