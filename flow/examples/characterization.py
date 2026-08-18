"""Build an inspectable analog characterization plan without executing work."""

from __future__ import annotations

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


DESIGN = artifact("analog-design-description")
CORNER_METRICS = artifact("point-metrics")
SUMMARY = artifact("characterization-summary")
JSON_V1 = codec("json", version="1", encoding="utf-8")
REPOSITORY_JSON = materialization(
    codec=JSON_V1,
    address_space="repository-relative",
    access_scope="repository-checkout",
)


@operation(
    name="example.estimate_corner_metrics",
    inputs={"design": DESIGN},
    config={"point": parameter(str), "temperature_c": parameter(int)},
    outputs={"metrics": CORNER_METRICS},
)
def estimate_corner_metrics(design, *, point, temperature_c):
    """Describe analytical work; planning must never call this body."""

    raise AssertionError("operation bodies must not execute while planning")


@operation(
    name="example.reduce_characterization",
    inputs={"measurements": artifacts("point-metrics")},
    outputs={"summary": SUMMARY},
)
def reduce_characterization(measurements):
    """Describe ordered fan-in over the planned point artifacts."""

    raise AssertionError("operation bodies must not execute while planning")


@flow(name="example.characterize_one_corner")
def characterize_one_corner(design, *, point, temperature_c):
    """Reuse one operation declaration behind a visible per-invocation boundary."""

    return estimate_corner_metrics.named("estimate-point-metrics")(
        design,
        point=point,
        temperature_c=temperature_c,
    )


@flow(name="example.characterize_design")
def characterize_design(design, *, include_extremes):
    """Use ordinary Python to select a static graph, then reduce its results."""

    points = {}
    measurements = []

    nominal = characterize_one_corner.named("point-tt")(
        design,
        point="tt",
        temperature_c=27,
    )
    points["tt"] = nominal
    measurements.append(nominal)

    if include_extremes:
        slow = characterize_one_corner.named("point-ss")(
            design,
            point="ss",
            temperature_c=125,
        )
        fast = characterize_one_corner.named("point-ff")(
            design,
            point="ff",
            temperature_c=-40,
        )
        points["ss"] = slow
        points["ff"] = fast
        measurements.extend((slow, fast))

    summary = reduce_characterization.named("reduce-characterization")(
        measurements
    )
    return {"points": points, "summary": summary}


@planned
def build_characterization_plan(*, include_extremes: bool = True):
    """One validated plan containing the complete authored graph.

    `@planned` makes calling this build one: the body records rather than runs,
    and what it returns names the plan's outputs.
    """

    design = input_artifact(
        address("repository-relative", "inputs/two-stage-opamp.json"),
        artifact=DESIGN,
        materialized_as=REPOSITORY_JSON,
    )
    return characterize_design.named("characterize-design")(
        design,
        include_extremes=include_extremes,
    )


def main() -> None:
    print(build_characterization_plan().to_json())


if __name__ == "__main__":
    main()
