import json

import pytest

import hedloom_flow
from hedloom_flow import (
    AuthoringError,
    BindingError,
    CollectionInputBinding,
    PlanningScopeError,
    address,
    artifact,
    flow,
    input_artifact,
    operation,
    parameter,
    plan,
    submit,
)
from examples import refinement


GRID = artifact("grid-declaration")
QUADRATURE = artifact("quadrature-result")
VERDICT = artifact("refinement-verdict")


def _source(locator, artifact_contract):
    return input_artifact(
        address("repository-relative", locator),
        artifact=artifact_contract,
    )


@pytest.mark.parametrize(
    ("include_refinements", "expected_points"),
    [(False, ("coarse",)), (True, ("coarse", "medium", "fine"))],
)
def test_refinement_collection_fan_in_is_ordered_and_fully_keyed(
    include_refinements, expected_points
):
    normalized = refinement.build_refinement_plan(
        include_refinements=include_refinements
    )

    assert normalized.validate() is normalized
    assert normalized.schema_version == 3
    assert len(normalized.sources) == 1
    assert len(normalized.invocations) == len(expected_points) + 1
    assert len(normalized.edges) == len(expected_points)
    assert len(normalized.boundaries) == len(expected_points) + 1

    roots = [
        boundary for boundary in normalized.boundaries if boundary.parent_id is None
    ]
    assert len(roots) == 1
    root = roots[0]
    assert root.authored_key == "refine-grid"
    assert {
        boundary.parent_id
        for boundary in normalized.boundaries
        if boundary.parent_id is not None
    } == {root.id}
    assert {boundary.authored_key for boundary in normalized.boundaries} == {
        "refine-grid",
        *(f"point-{point}" for point in expected_points),
    }

    summary = next(
        invocation
        for invocation in normalized.invocations
        if invocation.authored_key == "compare-refinements"
    )
    binding = summary.inputs[0]
    assert isinstance(binding, CollectionInputBinding)
    assert binding.cardinality == "collection"

    invocations_by_id = {item.id: item for item in normalized.invocations}
    bound_points = []
    producer_boundary_ids = []
    for reference in binding.references:
        producer = invocations_by_id[reference.invocation_id]
        bound_points.append(
            next(item.value for item in producer.config if item.name == "point")
        )
        assert producer.authored_key == "integrate-point"
        producer_boundary_ids.append(producer.boundary_id)
    assert tuple(bound_points) == expected_points
    assert len(set(producer_boundary_ids)) == len(expected_points)
    assert set(producer_boundary_ids) == {
        boundary.id
        for boundary in normalized.boundaries
        if boundary.authored_key != "refine-grid"
    }

    positioned_edges = sorted(
        normalized.edges, key=lambda edge: edge.target_member_index
    )
    assert [edge.target_member_index for edge in positioned_edges] == list(
        range(len(expected_points))
    )
    assert [edge.source for edge in positioned_edges] == list(binding.references)
    assert all(edge.target_invocation_id == summary.id for edge in positioned_edges)
    assert all(edge.id.startswith("edge:key:") for edge in positioned_edges)
    assert all(item.authored_key is not None for item in normalized.invocations)
    assert all(item.authored_key is not None for item in normalized.boundaries)
    assert normalized.sources[0].address.address_space == "repository-relative"
    assert normalized.sources[0].artifact.kind == "refinement-points"
    assert all(reference.value_class == "ephemeral" for reference in binding.references)
    assert all(edge.source.value_class == "ephemeral" for edge in positioned_edges)

    assert {output.name for output in normalized.outputs} == {
        *(f"points__{point}" for point in expected_points),
        "verdict",
    }
    assert json.loads(normalized.to_json()) == normalized.to_data()


def test_refinement_example_prints_canonical_json(capsys):
    normalized = refinement.build_refinement_plan()

    refinement.main()
    printed = capsys.readouterr().out.strip()
    assert printed == normalized.to_json()


@pytest.mark.parametrize("include_refinements", [False, True])
def test_same_example_inputs_reconstruct_identical_data_and_ids(include_refinements):
    first = refinement.build_refinement_plan(
        include_refinements=include_refinements
    )
    second = refinement.build_refinement_plan(
        include_refinements=include_refinements
    )

    assert first.to_data() == second.to_data()
    assert first.to_json() == second.to_json()
    for field in ("sources", "invocations", "edges", "boundaries"):
        assert [item.id for item in getattr(first, field)] == [
            item.id for item in getattr(second, field)
        ]


def test_example_operation_bodies_are_never_executed():
    # Both public operation bodies raise unconditionally. Reaching a valid plan
    # is direct evidence that flow authoring recorded calls without running them.
    full = refinement.build_refinement_plan()
    nominal_only = refinement.build_refinement_plan(
        include_refinements=False
    )

    assert len(full.invocations) == 4
    assert len(nominal_only.invocations) == 2


def test_nested_flow_failure_rolls_back_graph_and_all_id_counters():
    @operation(
        name="acceptance.estimate",
        inputs={"design": GRID},
        outputs={"metrics": QUADRATURE},
    )
    def estimate(design):
        raise AssertionError("must not run")

    @operation(
        name="acceptance.summarize",
        inputs={"metrics": QUADRATURE},
        outputs={"summary": VERDICT},
    )
    def summarize(metrics):
        raise AssertionError("must not run")

    @flow(name="acceptance.failing_nested")
    def failing_nested(design):
        summarize(estimate(design))
        raise RuntimeError("authored nested-flow failure")

    @flow(name="acceptance.successful_nested")
    def successful_nested(design):
        return summarize(estimate(design))

    @flow(name="acceptance.rollback_study")
    def rollback_study(design):
        with pytest.raises(RuntimeError, match="nested-flow failure"):
            failing_nested(design)
        return successful_nested(design)

    with plan() as draft:
        result = rollback_study(
            _source("inputs/grid.json", GRID)
        )
    normalized = draft.finish(outputs={"summary": result})

    assert [item.id for item in normalized.invocations] == [
        "invoke:0001",
        "invoke:0002",
    ]
    assert [item.id for item in normalized.edges] == ["edge:0001"]
    assert {item.id for item in normalized.boundaries} == {
        "flow:0001",
        "flow:0002",
    }
    assert {item.identity.name for item in normalized.flows} == {
        "acceptance.rollback_study",
        "acceptance.successful_nested",
    }
    assert normalized.validate() is normalized


def test_foreign_source_only_and_incompatible_values_fail_before_finish_returns():
    @operation(
        inputs={"design": GRID},
        outputs={"metrics": QUADRATURE},
    )
    def estimate(design):
        raise AssertionError("must not run")

    @operation(inputs={"metrics": QUADRATURE}, outputs={"summary": VERDICT})
    def summarize(metrics):
        raise AssertionError("must not run")

    with plan() as foreign_draft:
        foreign_result = estimate(
            _source("inputs/foreign.json", GRID)
        )
    foreign_draft.finish(outputs={"metrics": foreign_result})

    with plan() as local_draft:
        local_source = _source("inputs/local.json", GRID)
        with pytest.raises(BindingError, match="different plan"):
            summarize(foreign_result)
        with pytest.raises(BindingError, match="expects artifact kind"):
            summarize(local_source)
        with pytest.raises(BindingError, match="artifact inputs must be"):
            summarize("results/point.json")
        local_result = estimate(local_source)

    with pytest.raises(BindingError, match="different plan"):
        local_draft.finish(outputs={"metrics": foreign_result})
    with pytest.raises(AuthoringError, match="not an input source"):
        local_draft.finish(outputs={"design": local_source})

    normalized = local_draft.finish(outputs={"metrics": local_result})
    assert len(normalized.invocations) == 1
    assert normalized.invocations[0].id == "invoke:0001"


def test_no_run_or_ambient_execution_surface_and_submit_is_explicit():
    assert not hasattr(hedloom_flow, "run")
    assert not hasattr(refinement.refine_grid, "run")
    assert not hasattr(refinement.integrate, "run")

    with pytest.raises(PlanningScopeError, match="active plan"):
        refinement.refine_grid(object(), include_refinements=True)
    with pytest.raises(PlanningScopeError, match="active plan"):
        _source("inputs/grid.json", GRID)
    with pytest.raises(NotImplementedError, match="outside this planning spike"):
        submit(refinement.build_refinement_plan())


def _mapping_order_plan(*, reverse_inputs):
    @operation(
        name="acceptance.mapping_order.estimate",
        inputs={"design": GRID},
        config={"point": parameter(str)},
        outputs={"metrics": QUADRATURE},
    )
    def estimate(design, *, point):
        raise AssertionError("must not run")

    reducer_inputs = (
        {"right": QUADRATURE, "left": QUADRATURE}
        if reverse_inputs
        else {"left": QUADRATURE, "right": QUADRATURE}
    )

    @operation(
        name="acceptance.mapping_order.reduce",
        inputs=reducer_inputs,
        outputs={"summary": VERDICT},
    )
    def reduce(left, right):
        raise AssertionError("must not run")

    with plan() as draft:
        design = _source("inputs/grid.json", GRID)
        left = estimate(design, point="medium")
        right = estimate(design, point="fine")
        summary = reduce(left=left, right=right)
    return draft.finish(outputs={"summary": summary})


def test_name_keyed_declaration_order_is_semantically_irrelevant():
    assert _mapping_order_plan(reverse_inputs=False).to_data() == _mapping_order_plan(
        reverse_inputs=True
    ).to_data()
