"""Compute the public refinement Plan through the local Dask experiment.

    PYTHONPATH=src:. python examples/local_dask_refinement.py

The Plan from `refinement.py` is a document, not a program: it names operations
and says nothing about what they compute. This binds each authored identity to
a callable and lowers the whole graph into Dask delayeds, which is the smallest
honest demonstration that the document is enough to drive an executor.

Two things are deliberately explicit. The operations are supplied by identity
rather than discovered, so nothing here can run a body the Plan did not name.
And the external source is supplied as an already-decoded value, so this unit
still resolves no address and opens no file — the point of `input_artifact`
being an address rather than a path.

The arithmetic is real. `../../examples/grid_refinement.py` computes the same
three estimates by handing the same three grids to real `awk`, and gets the
same numbers, because the trapezoid rule does not care who evaluates it.
"""

from __future__ import annotations

import json
import math

import dask
import refinement

from hedloom_flow.experimental.local_dask import lower_delayed


# The decoded source, injected rather than read. It is what makes the answer
# checkable: the integral of exp(-x) over [0, 1] is 1 - 1/e exactly.
DECODED_POINTS = {
    "integrand": "exp(-x)",
    "lower": 0.0,
    "upper": 1.0,
}


def _trapezoid(lower: float, upper: float, steps: int) -> float:
    """The rule itself, in one place, so both bindings below agree by construction."""

    width = (upper - lower) / steps
    total = (math.exp(-lower) + math.exp(-upper)) / 2.0
    for index in range(1, steps):
        total += math.exp(-(lower + index * width))
    return total * width


def _integrate_identity(points, *, point: str, steps: int):
    """The runtime binding for the authored `example.integrate` identity."""

    estimate = _trapezoid(points["lower"], points["upper"], steps)
    return {
        "result": {
            "point": point,
            "steps": steps,
            "estimate": round(estimate, 12),
        }
    }


def _compare_identity(*, results):
    """Preserve the Plan's significant coarse/medium/fine collection order."""

    exact = math.exp(-DECODED_POINTS["lower"]) - math.exp(-DECODED_POINTS["upper"])
    errors = [abs(item["estimate"] - exact) for item in results]
    return {
        "verdict": {
            "point_order": [item["point"] for item in results],
            "estimates": [item["estimate"] for item in results],
            # Second order: refining by four should shrink the error by sixteen.
            "order_ratios": [
                round(previous / current, 6)
                for previous, current in zip(errors, errors[1:])
                if current > 0.0
            ],
        }
    }


def build_result():
    """Lower and explicitly compute one deterministic semantic result record."""

    normalized = refinement.build_refinement_plan()
    source_id = normalized.sources[0].id
    integrate_identity = refinement.integrate.identity
    compare_identity = refinement.compare.identity
    lowered = lower_delayed(
        normalized,
        operations={
            integrate_identity: _integrate_identity,
            compare_identity: _compare_identity,
        },
        sources={source_id: DECODED_POINTS},
    )

    output_names = tuple(output.name for output in normalized.outputs)
    computed = dask.compute(
        *(lowered.outputs[name] for name in output_names),
        scheduler="synchronous",
        optimize_graph=False,
    )
    return {
        "plan": {
            "counts": {
                "boundaries": len(normalized.boundaries),
                "edges": len(normalized.edges),
                "flows": len(normalized.flows),
                "invocations": len(normalized.invocations),
                "operations": len(normalized.operations),
                "outputs": len(normalized.outputs),
                "sources": len(normalized.sources),
            },
            "ids": {
                "boundaries": [item.id for item in normalized.boundaries],
                "edges": [item.id for item in normalized.edges],
                "invocations": [item.id for item in normalized.invocations],
                "sources": [item.id for item in normalized.sources],
            },
            "schema_version": normalized.schema_version,
        },
        "results": dict(zip(output_names, computed, strict=True)),
    }


def main() -> None:
    print(json.dumps(build_result(), sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
