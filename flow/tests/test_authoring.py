from dataclasses import FrozenInstanceError, replace
import json

import pytest

from hedloom_flow import (
    AuthoringError,
    BindingError,
    CollectionInputBinding,
    FlowCall,
    HandleUsedAsValue,
    InputBinding,
    PlanningScopeError,
    PlanValidationError,
    address,
    artifact,
    artifacts,
    flow,
    input_artifact,
    local,
    named_policy,
    operation,
    parameter,
    plan,
    plain_data,
    planned,
    submit,
)
from hedloom_flow.authoring import directory, file


MODEL = artifact("model-input")
RAW = artifact("simulation-raw")
REPORT = artifact("measurement-report")
MEASUREMENT = artifact("measurement")


def _source(locator, artifact_contract):
    return input_artifact(
        address("test-address-space", locator),
        artifact=artifact_contract,
    )


def test_calls_outside_scope_are_actionable_and_operation_body_does_not_run():
    calls = []

    @operation(inputs={"model": MODEL}, outputs={"raw": RAW})
    def simulate(model):
        calls.append(model)

    @flow
    def study(model):
        return simulate(model)

    with pytest.raises(PlanningScopeError, match="with plan"):
        simulate(object())
    with pytest.raises(PlanningScopeError, match="with plan"):
        study(object())

    with plan() as draft:
        result = study(_source("input.in", MODEL))
    normalized = draft.finish(outputs={"raw": result})

    assert calls == []
    assert len(normalized.invocations) == 1
    assert len(normalized.boundaries) == 1


def test_external_sources_require_the_strict_structured_authoring_surface():
    with plan() as draft:
        with pytest.raises(TypeError, match="positional"):
            input_artifact("legacy-uri", "model-input")
        with pytest.raises(AuthoringError, match=r"address\(\.\.\.\)"):
            input_artifact(
                "legacy-uri",
                artifact=MODEL,
            )
        model = _source("input.in", MODEL)
    normalized = draft.finish(outputs={})

    assert model.reference.value_class == "artifact"
    assert normalized.sources[0].address.locator == "input.in"


def test_source_deduplication_uses_the_complete_immutable_declaration():
    """A source is its address and its contract, so those decide sameness.

    Declaring the same external artifact twice must be one source: two would
    give one file two identities, and reuse would split between them for no
    reason an author could see.
    """

    shared_address = address("test-address-space", "shared.data")

    with plan() as draft:
        first = input_artifact(shared_address, artifact=MODEL)
        repeated = input_artifact(shared_address, artifact=MODEL)
        different_kind = input_artifact(shared_address, artifact=RAW)
        different_address = _source("elsewhere.data", MODEL)
    normalized = draft.finish(outputs={})

    assert first is repeated
    assert len(normalized.sources) == 3
    assert different_kind.reference != first.reference
    assert different_address.reference != first.reference


def test_a_refused_source_declaration_does_not_consume_an_id():
    """Rejection must leave no trace, or IDs drift on a caught mistake."""

    with plan() as draft:
        with pytest.raises(AuthoringError, match=r"artifact\(\.\.\.\)"):
            input_artifact(
                address("test-address-space", "opaque/../locator"),
                artifact="model-input",
            )
        accepted = _source("opaque/../locator", MODEL)
    normalized = draft.finish(outputs={})

    assert accepted.reference.source_id == "source:0001"
    assert normalized.sources[0].address.locator == "opaque/../locator"


def test_output_only_declarations_are_rejected_for_operation_inputs():
    with pytest.raises(AuthoringError, match=r"artifact\(\.\.\.\) or artifacts"):
        operation(inputs={"model": file("model.in")})


def test_directory_declares_filesystem_shape_separately_from_artifact_kind():
    @operation(outputs={"bundle": directory("bundle", kind="report-bundle")})
    def collect():
        raise AssertionError("must not run")

    @planned
    def build():
        return collect.named("collect")()

    (output,) = build().operations[0].outputs
    assert output.artifact.kind == "report-bundle"
    assert plain_data(output.binding) == {
        "path": "bundle",
        "filesystem_kind": "directory",
    }


def test_options_are_immutable_and_policy_precedence_is_explicit():
    lsf = named_policy("lsf")
    operation_default = lsf(queue="operation")
    plan_default = lsf(queue="plan")
    override = lsf(queue="call")

    @operation(
        inputs={"model": MODEL},
        outputs={"raw": RAW},
        default_policy=operation_default,
    )
    def simulate(model):
        raise AssertionError("must not run")

    @operation(inputs={"model": MODEL}, outputs={"raw": RAW})
    def inherited(model):
        raise AssertionError("must not run")

    call_view = simulate.options(policy=override)
    assert call_view is not simulate
    assert simulate.definition.default_policy is operation_default
    with pytest.raises(FrozenInstanceError):
        call_view.policy = plan_default

    with plan(default_policy=plan_default) as draft:
        model = _source("input.in", MODEL)
        call_view(model)
        simulate(model)
        inherited(model)
    normalized = draft.finish(outputs={})

    assert [item.policy for item in normalized.invocations] == [
        override,
        operation_default,
        plan_default,
    ]

    with plan() as local_draft:
        model = _source("input.in", MODEL)
        inherited(model)
    local_plan = local_draft.finish(outputs={})
    assert local_plan.invocations[0].policy == local()


def test_repeated_planning_has_stable_source_invocation_edge_and_boundary_ids():
    @operation(inputs={"model": MODEL}, outputs={"raw": RAW})
    def simulate(model):
        raise AssertionError("must not run")

    @operation(inputs={"raw": RAW}, outputs={"report": REPORT})
    def measure(raw):
        raise AssertionError("must not run")

    @flow
    def study(model):
        return measure(simulate(model))

    def build():
        with plan() as draft:
            result = study(_source("input.in", MODEL))
        return draft.finish(outputs={"report": result})

    first = build()
    second = build()

    assert first.to_data() == second.to_data()
    assert [item.id for item in first.sources] == ["source:0001"]
    assert [item.id for item in first.invocations] == [
        "invoke:0001",
        "invoke:0002",
    ]
    assert [item.id for item in first.edges] == ["edge:0001"]
    assert [item.id for item in first.boundaries] == ["flow:0001"]
    assert isinstance(first.invocations[1].inputs[0], InputBinding)
    assert first.invocations[1].inputs[0].cardinality == "scalar"
    assert first.edges[0].target_member_index is None
    assert all(
        item["authored_key"] is None for item in first.to_data()["invocations"]
    )
    assert first.to_data()["boundaries"][0]["authored_key"] is None


def test_collection_input_preserves_member_order_in_bindings_edges_and_json():
    @operation(
        name="authoring.collections.measure",
        inputs={"model": MODEL},
        config={"label": parameter(str)},
        outputs={"measurement": MEASUREMENT},
    )
    def measure(model, *, label):
        raise AssertionError("must not run")

    @operation(
        name="authoring.collections.summarize",
        inputs={"measurements": artifacts("measurement")},
        outputs={"report": REPORT},
    )
    def summarize(measurements):
        raise AssertionError("must not run")

    def build():
        with plan() as draft:
            model = _source("input.in", MODEL)
            measurements = [
                measure(model, label=label) for label in ("ss", "tt", "ff")
            ]
            report = summarize(measurements)
        return draft.finish(outputs={"report": report})

    first = build()
    second = build()
    summary = first.invocations[-1]
    binding = summary.inputs[0]

    assert first.to_data() == second.to_data()
    assert first.to_json() == second.to_json()
    assert first.operations[-1].inputs[0].required is True
    assert first.operations[-1].inputs[0].cardinality == "collection"
    assert isinstance(binding, CollectionInputBinding)
    assert binding.cardinality == "collection"
    assert [reference.invocation_id for reference in binding.references] == [
        "invoke:0001",
        "invoke:0002",
        "invoke:0003",
    ]
    assert [edge.source.invocation_id for edge in first.edges] == [
        "invoke:0001",
        "invoke:0002",
        "invoke:0003",
    ]
    assert [edge.target_member_index for edge in first.edges] == [0, 1, 2]

    data = json.loads(first.to_json())
    serialized_binding = data["invocations"][-1]["inputs"][0]
    assert serialized_binding["cardinality"] == "collection"
    assert [
        reference["invocation_id"]
        for reference in serialized_binding["references"]
    ] == ["invoke:0001", "invoke:0002", "invoke:0003"]
    assert [edge["target_member_index"] for edge in data["edges"]] == [0, 1, 2]
    assert "value" not in serialized_binding


def test_external_source_collection_member_gets_an_edge_without_scalar_regression():
    @operation(
        name="authoring.source_collections.measure",
        inputs={"model": MODEL},
        outputs={"measurement": MEASUREMENT},
    )
    def measure(model):
        raise AssertionError("must not run")

    @operation(
        name="authoring.source_collections.summarize",
        inputs={"measurements": artifacts("measurement")},
        outputs={"report": REPORT},
    )
    def summarize(measurements):
        raise AssertionError("must not run")

    def build():
        with plan() as draft:
            model = _source("input.in", MODEL)
            external = _source("existing-measurement.json", MEASUREMENT)
            produced = measure(model)
            report = summarize([external, produced])
        return draft.finish(outputs={"report": report})

    first = build()
    second = build()
    data = json.loads(first.to_json())
    binding = data["invocations"][-1]["inputs"][0]

    assert first.to_data() == second.to_data()
    assert first.to_json() == second.to_json()
    assert len(first.edges) == 2
    assert [reference["type"] for reference in binding["references"]] == [
        "source",
        "output",
    ]
    assert [reference["value_class"] for reference in binding["references"]] == [
        "artifact",
        "ephemeral",
    ]
    assert [edge["source"]["type"] for edge in data["edges"]] == [
        "source",
        "output",
    ]
    assert [edge["source"]["value_class"] for edge in data["edges"]] == [
        "artifact",
        "ephemeral",
    ]
    assert [edge["target_member_index"] for edge in data["edges"]] == [0, 1]


def test_collection_inputs_reject_invalid_authored_values_early():
    @operation(inputs={"model": MODEL}, outputs={"measurement": MEASUREMENT})
    def measure(model):
        raise AssertionError("must not run")

    @operation(
        inputs={"model": MODEL},
        outputs={"left": MEASUREMENT, "right": MEASUREMENT},
    )
    def split(model):
        raise AssertionError("must not run")

    @operation(
        inputs={"measurements": artifacts("measurement")},
        outputs={"report": REPORT},
    )
    def summarize(measurements):
        raise AssertionError("must not run")

    with plan() as foreign_draft:
        foreign = measure(_source("foreign.in", MODEL))
    foreign_draft.finish(outputs={"measurement": foreign})

    with plan() as draft:
        model = _source("input.in", MODEL)
        existing_measurement = _source(
            "existing-measurement.json", MEASUREMENT
        )
        measurement = measure(model)
        multiple = split(model)
        for invalid in (model, "measurement.json", b"measurement", {"one": measurement}):
            with pytest.raises(BindingError, match="non-string sequence"):
                summarize(invalid)
        with pytest.raises(BindingError, match="non-string sequence"):
            summarize(member for member in [measurement])
        with pytest.raises(BindingError, match="must not be empty"):
            summarize([])
        with pytest.raises(BindingError, match="expects artifact kind"):
            summarize([measurement, model])
        with pytest.raises(BindingError, match="different plan"):
            summarize([foreign])
        with pytest.raises(BindingError, match="select one explicitly"):
            summarize([multiple])
        report = summarize((measurement, existing_measurement))
    normalized = draft.finish(outputs={"report": report})

    assert len(normalized.invocations) == 3
    assert [edge.target_member_index for edge in normalized.edges] == [0, 1]
    assert normalized.to_data()["edges"][1]["source"]["type"] == "source"


def test_name_keyed_declaration_order_does_not_change_normalized_plan():
    def build(*, reversed_declarations):
        @operation(
            name="authoring.mapping_order.produce",
            inputs={"model": MODEL},
            config={"point": parameter(str)},
            outputs={"raw": RAW},
        )
        def produce(model, *, point):
            raise AssertionError("must not run")

        input_items = [("left", RAW), ("right", RAW)]
        config_items = [("label", str), ("point", str)]
        output_items = [("raw", RAW), ("report", REPORT)]
        if reversed_declarations:
            input_items.reverse()
            config_items.reverse()
            output_items.reverse()

        @operation(
            name="authoring.mapping_order.combine",
            inputs=dict(input_items),
            config={name: parameter(value_type) for name, value_type in config_items},
            outputs=dict(output_items),
        )
        def combine(left, right, *, label, point):
            raise AssertionError("must not run")

        with plan() as draft:
            model = _source("input.in", MODEL)
            left = produce(model, point="ss")
            right = produce(model, point="ff")
            combined = combine(
                right=right,
                left=left,
                label="comparison",
                point="all",
            )
        return draft.finish(outputs={"raw": combined.raw, "report": combined.report})

    forward = build(reversed_declarations=False)
    reverse = build(reversed_declarations=True)

    assert forward.to_data() == reverse.to_data()
    assert [edge.id for edge in forward.edges] == ["edge:0001", "edge:0002"]
    combine = next(
        definition
        for definition in forward.operations
        if definition.identity.name == "authoring.mapping_order.combine"
    )
    assert [contract.name for contract in combine.inputs] == ["left", "right"]
    assert [contract.name for contract in combine.config] == ["label", "point"]
    assert [contract.name for contract in combine.outputs] == ["raw", "report"]


def test_nested_static_branch_and_fan_in_normalize_to_one_plan():
    @operation(
        inputs={"model": MODEL},
        config={"point": parameter(str)},
        outputs={"raw": RAW},
    )
    def simulate(model, *, point):
        raise AssertionError("must not run")

    @operation(
        inputs={"left": RAW, "right": RAW}, outputs={"report": REPORT}
    )
    def compare(left, right):
        raise AssertionError("must not run")

    @flow
    def characterize(model, *, points):
        return {point: simulate(model, point=point) for point in points}

    @flow
    def study(model, *, include_slow):
        points = ["tt"]
        if include_slow:
            points.append("ss")
        branches = characterize(model, points=points)
        return compare(branches["tt"], branches["ss"])

    with plan() as draft:
        result = study(
            _source("amplifier.in", MODEL), include_slow=True
        )
    normalized = draft.finish(outputs={"report": result})

    assert len(normalized.invocations) == 3
    assert len(normalized.edges) == 2
    assert len(normalized.boundaries) == 2
    study_boundary = next(item for item in normalized.boundaries if item.parent_id is None)
    nested = next(item for item in normalized.boundaries if item.parent_id is not None)
    assert nested.parent_id == study_boundary.id
    assert {item.name for item in nested.outputs} == {"ss", "tt"}
    assert normalized.validate() is normalized


def test_multiple_outputs_require_explicit_selection_but_are_inspectable():
    @operation(inputs={"model": MODEL}, outputs={"raw": RAW, "report": REPORT})
    def split(model):
        raise AssertionError("must not run")

    @operation(inputs={"raw": RAW}, outputs={"report": REPORT})
    def measure(raw):
        raise AssertionError("must not run")

    with plan() as draft:
        model = _source("input.in", MODEL)
        result = split(model)
        assert result.declared_outputs == ("raw", "report")
        assert result.outputs["raw"] is result.raw
        with pytest.raises(BindingError, match="select one explicitly"):
            measure(result)
        report = measure(result.raw)
    normalized = draft.finish(outputs={"report": report})
    assert len(normalized.edges) == 1


def test_invalid_bindings_and_flow_outputs_fail_during_planning():
    @operation(
        inputs={"model": MODEL},
        config={"point": parameter(str)},
        outputs={"raw": RAW},
    )
    def simulate(model, *, point):
        raise AssertionError("must not run")

    @operation(inputs={"raw": RAW}, outputs={"report": REPORT})
    def measure(raw):
        raise AssertionError("must not run")

    @flow
    def invalid_flow(model):
        simulate(model, point="tt")
        return "not an artifact"

    with plan() as draft:
        model = _source("input.in", MODEL)
        with pytest.raises(BindingError, match="missing config"):
            simulate(model)
        with pytest.raises(BindingError, match="unexpected bindings"):
            simulate(model, point="tt", extra=True)
        with pytest.raises(BindingError, match="expects str"):
            simulate(model, point=3)
        with pytest.raises(BindingError, match="expects artifact kind"):
            measure(model)
        with pytest.raises(AuthoringError, match="must be an operation output"):
            invalid_flow(model)
        good = simulate(model, point="tt")
    normalized = draft.finish(outputs={"raw": good})
    assert len(normalized.invocations) == 1
    assert normalized.invocations[0].id == "invoke:0001"


def test_foreign_references_and_finished_or_reused_sessions_are_rejected():
    @operation(inputs={"model": MODEL}, outputs={"raw": RAW})
    def simulate(model):
        raise AssertionError("must not run")

    first_draft = plan()
    with first_draft:
        foreign = simulate(_source("one.in", MODEL))
    first_draft.finish(outputs={"raw": foreign})

    with plan() as second_draft:
        with pytest.raises(BindingError, match="different plan"):
            simulate(foreign)
    second_draft.finish(outputs={})

    with pytest.raises(AuthoringError, match="already been finished"):
        first_draft.finish(outputs={})
    with pytest.raises(PlanningScopeError, match="cannot be reused"):
        with first_draft:
            pass


def test_definition_declarations_and_submit_boundary_fail_early():
    with pytest.raises(AuthoringError, match="declarations absent from signature"):

        @operation(inputs={"model": MODEL}, outputs={"raw": RAW})
        def invalid(other):
            pass

    with pytest.raises(AuthoringError, match="must use Parameter"):
        operation(config={"point": str})

    with pytest.raises(NotImplementedError, match="outside this planning spike"):
        submit(object())


def test_a_policy_and_a_name_compose_immutably_in_either_order():
    override = named_policy("lsf")(queue="short")

    @operation(inputs={"model": MODEL}, outputs={"raw": RAW})
    def simulate(model):
        raise AssertionError("must not run")

    policy_view = simulate.options(policy=override)
    policy_then_key = policy_view.named("policy-first")
    key_view = simulate.named("key-first")
    key_then_policy = key_view.options(policy=override)

    assert policy_view.policy is override
    assert policy_view.key is None
    assert key_view.policy is None
    assert key_view.key == "key-first"
    assert policy_then_key.policy is override
    assert policy_then_key.key == "policy-first"
    assert key_then_policy.policy is override
    assert key_then_policy.key == "key-first"
    with pytest.raises(FrozenInstanceError):
        policy_then_key.key = "changed"

    with plan() as draft:
        model = _source("input.in", MODEL)
        policy_then_key(model)
        key_then_policy(model)
    normalized = draft.finish(outputs={})

    assert [item.authored_key for item in normalized.invocations] == [
        "policy-first",
        "key-first",
    ]
    assert [item.policy for item in normalized.invocations] == [override, override]
    assert all("authored_key" in item for item in normalized.to_data()["invocations"])


def test_keyed_nested_flows_repeat_and_survive_an_unkeyed_sibling_insertion():
    @operation(
        name="authoring.identities.produce",
        inputs={"model": MODEL},
        outputs={"raw": RAW},
    )
    def produce(model):
        raise AssertionError("must not run")

    @operation(
        name="authoring.identities.consume",
        inputs={"raw": artifacts("simulation-raw")},
        outputs={"report": REPORT},
    )
    def consume(raw):
        raise AssertionError("must not run")

    @operation(
        name="authoring.identities.noise",
        inputs={"model": MODEL},
        outputs={"raw": RAW},
    )
    def noise(model):
        raise AssertionError("must not run")

    @flow(name="authoring.identities.inner")
    def inner(raw):
        return consume.named("consumer")(raw)

    @flow(name="authoring.identities.outer")
    def outer(model):
        raw = [
            produce.named("producer-left")(model),
            produce.named("producer-right")(model),
        ]
        return inner.named("inner")(raw)

    @flow(name="authoring.identities.noise_flow")
    def noise_flow(model):
        return noise(model)

    def build(*, insert_sibling):
        with plan() as draft:
            model = _source("input.in", MODEL)
            if insert_sibling:
                noise_flow(model)
            report = outer.named("outer")(model)
        return draft.finish(outputs={"report": report})

    baseline = build(insert_sibling=False)
    repeated = build(insert_sibling=False)
    inserted = build(insert_sibling=True)

    outer_view = outer.named("original")
    changed_outer_view = outer_view.named("changed")
    assert isinstance(outer_view, FlowCall)
    assert outer_view.key == "original"
    assert changed_outer_view.key == "changed"
    with pytest.raises(FrozenInstanceError):
        outer_view.key = "mutated"
    # A flow has no policy to take, because it is a scope rather than work.
    with pytest.raises(AttributeError, match="options"):
        outer.options(policy=local())

    assert baseline.to_data() == repeated.to_data()
    assert baseline.to_json() == repeated.to_json()
    assert {
        item.authored_key: item.id
        for item in baseline.invocations
        if item.authored_key is not None
    } == {
        item.authored_key: item.id
        for item in inserted.invocations
        if item.authored_key is not None
    }
    assert {
        item.authored_key: item.id
        for item in baseline.boundaries
        if item.authored_key is not None
    } == {
        item.authored_key: item.id
        for item in inserted.boundaries
        if item.authored_key is not None
    }
    assert [item.id for item in baseline.edges] == [item.id for item in inserted.edges]
    assert all(item.id.startswith("edge:key:") for item in baseline.edges)
    assert [item.target_member_index for item in baseline.edges] == [0, 1]
    assert inserted.invocations[0].id == "invoke:0001"
    assert inserted.boundaries[0].id == "flow:0001"
    assert {
        item["authored_key"] for item in baseline.to_data()["boundaries"]
    } == {"outer", "inner"}

    malformed = replace(
        baseline,
        edges=(replace(baseline.edges[0], id="edge:wrong"), baseline.edges[1]),
    )
    with pytest.raises(PlanValidationError) as caught:
        malformed.validate()
    assert "stable_edge_id_mismatch" in {
        issue.code for issue in caught.value.issues
    }


def test_duplicate_keys_share_one_operation_and_flow_namespace_per_scope():
    calls = []

    @operation(inputs={"model": MODEL}, outputs={"raw": RAW})
    def produce(model):
        raise AssertionError("must not run")

    @flow
    def leaf(model):
        calls.append("flow body ran")
        return produce(model)

    @flow
    def keyed_scope(model):
        return produce.named("reused")(model)

    with plan() as draft:
        model = _source("input.in", MODEL)
        produce.named("op-duplicate")(model)
        with pytest.raises(AuthoringError, match="already used"):
            produce.named("op-duplicate")(model)

        leaf.named("flow-duplicate")(model)
        with pytest.raises(AuthoringError, match="already used"):
            leaf.named("flow-duplicate")(model)

        produce.named("cross-kind")(model)
        with pytest.raises(AuthoringError, match="already used"):
            leaf.named("cross-kind")(model)

        keyed_scope.named("scope-a")(model)
        keyed_scope.named("scope-b")(model)
        leaf(model)
    normalized = draft.finish(outputs={})

    assert calls == ["flow body ran", "flow body ran"]
    reused = [
        invocation
        for invocation in normalized.invocations
        if invocation.authored_key == "reused"
    ]
    assert len(reused) == 2
    assert reused[0].boundary_id != reused[1].boundary_id
    assert reused[0].id != reused[1].id
    assert [
        boundary.id
        for boundary in normalized.boundaries
        if boundary.authored_key is None
    ] == ["flow:0001"]
    assert normalized.validate() is normalized


def test_keyed_calls_and_edges_do_not_consume_unkeyed_counters():
    @operation(inputs={"model": MODEL}, outputs={"raw": RAW})
    def produce(model):
        raise AssertionError("must not run")

    @operation(inputs={"raw": RAW}, outputs={"report": REPORT})
    def consume(raw):
        raise AssertionError("must not run")

    with plan() as draft:
        model = _source("input.in", MODEL)
        keyed_raw = produce.named("keyed-producer")(model)
        consume.named("keyed-consumer")(keyed_raw)
        unkeyed_raw = produce(model)
        unkeyed_report = consume(unkeyed_raw)
    normalized = draft.finish(outputs={"report": unkeyed_report})

    unkeyed_invocation_ids = [
        item.id for item in normalized.invocations if item.authored_key is None
    ]
    assert unkeyed_invocation_ids == [
        "invoke:0001",
        "invoke:0002",
    ]
    stable_edges = [
        item for item in normalized.edges if item.id.startswith("edge:key:")
    ]
    counter_edges = [
        item.id for item in normalized.edges if item.id.startswith("edge:0")
    ]
    assert len(stable_edges) == 1
    assert counter_edges == ["edge:0001"]


def test_duplicate_operation_rollback_does_not_leak_or_consume_counters():
    @operation(
        name="authoring.rollback.accepted",
        inputs={"model": MODEL},
        outputs={"raw": RAW},
    )
    def accepted(model):
        raise AssertionError("must not run")

    @operation(
        name="authoring.rollback.rejected",
        inputs={"model": MODEL},
        outputs={"raw": RAW},
    )
    def rejected(model):
        raise AssertionError("must not run")

    with plan() as draft:
        model = _source("input.in", MODEL)
        accepted(model)
        accepted.named("reserved")(model)
        with pytest.raises(AuthoringError, match="already used"):
            rejected.named("reserved")(model)
        accepted(model)
    normalized = draft.finish(outputs={})

    unkeyed_invocation_ids = [
        item.id for item in normalized.invocations if item.authored_key is None
    ]
    assert unkeyed_invocation_ids == [
        "invoke:0001",
        "invoke:0002",
    ]
    assert {item.identity.name for item in normalized.operations} == {
        "authoring.rollback.accepted"
    }
    assert len(normalized.invocations) == 3


def test_failing_keyed_flow_restores_keys_graph_and_every_unkeyed_counter():
    @operation(inputs={"model": MODEL}, outputs={"raw": RAW})
    def produce(model):
        raise AssertionError("must not run")

    @operation(inputs={"raw": RAW}, outputs={"report": REPORT})
    def consume(raw):
        raise AssertionError("must not run")

    @flow(name="authoring.rollback.failing")
    def failing(model):
        raw = produce.named("step")(model)
        consume(raw)
        raise RuntimeError("planned failure")

    @flow(name="authoring.rollback.success")
    def success(model):
        return produce.named("step")(model)

    @flow(name="authoring.rollback.unkeyed")
    def unkeyed(model):
        return produce(model)

    with plan() as draft:
        model = _source("input.in", MODEL)
        with pytest.raises(RuntimeError, match="planned failure"):
            failing.named("boundary")(model)
        keyed = success.named("boundary")(model)
        raw = produce(model)
        report = consume(raw)
        later = unkeyed(model)
    normalized = draft.finish(
        outputs={"keyed": keyed, "report": report, "later": later}
    )

    unkeyed_invocation_ids = [
        item.id for item in normalized.invocations if item.authored_key is None
    ]
    assert unkeyed_invocation_ids == [
        "invoke:0001",
        "invoke:0002",
        "invoke:0003",
    ]
    assert [item.id for item in normalized.edges] == ["edge:0001"]
    assert [item.id for item in normalized.boundaries if item.authored_key is None] == [
        "flow:0001"
    ]
    assert {item.identity.name for item in normalized.flows} == {
        "authoring.rollback.success",
        "authoring.rollback.unkeyed",
    }
    assert {item.authored_key for item in normalized.invocations} == {None, "step"}


@pytest.mark.parametrize(
    "invalid_key",
    ["", " leading", "trailing ", "_leading", "bad key", "é"],
)
def test_invalid_authored_key_syntax_fails_before_graph_mutation(invalid_key):
    @operation(inputs={"model": MODEL}, outputs={"raw": RAW})
    def produce(model):
        raise AssertionError("must not run")

    @flow
    def wrapper(model):
        return produce(model)

    with plan() as draft:
        model = _source("input.in", MODEL)
        with pytest.raises(AuthoringError, match="authored key"):
            produce.named(invalid_key)(model)
        with pytest.raises(AuthoringError, match="authored key"):
            wrapper.named(invalid_key)(model)
        result = produce(model)
    normalized = draft.finish(outputs={"raw": result})

    assert [item.id for item in normalized.invocations] == ["invoke:0001"]
    assert normalized.boundaries == ()


def test_a_handle_refuses_to_answer_about_a_value_it_does_not_have():
    """The failure mode was silence, not error.

    Every other way of reading a handle already raised, but `if raw:` answered
    True and `raw == 0` answered False — both about the reference, while
    looking like answers about the result. That is the one thing a planning
    handle must never do, because it is how result-dependent control arrives
    without anyone deciding to add it.
    """

    @operation(inputs={"model": MODEL}, outputs={"raw": RAW})
    def simulate(model):
        raise AssertionError("must not run")

    with plan() as draft:
        result = simulate(_source("input.in", MODEL))
        handle = result.raw

        for candidate in (result, handle):
            with pytest.raises(HandleUsedAsValue, match="no value"):
                bool(candidate)
            with pytest.raises(HandleUsedAsValue, match="no value"):
                _ = candidate == 0
            with pytest.raises(HandleUsedAsValue, match="no value"):
                _ = candidate != 0

        # Ordering already refused, and still does: Python has no answer for
        # it either, so there was nothing here to fix.
        with pytest.raises(TypeError):
            _ = handle > 0

        # The refusal says which call it is about, so an author can find it.
        with pytest.raises(HandleUsedAsValue, match="'raw'.*'simulation-raw'"):
            bool(handle)

        # Still a usable key: a handle is its own identity, and dict lookup
        # settles on identity before it would ever ask about equality.
        assert {handle: "kept"}[handle] == "kept"

    normalized = draft.finish(outputs={"raw": result})
    assert len(normalized.invocations) == 1


def test_reading_a_handle_is_refused_as_both_kinds_of_mistake():
    """It is an authoring error in this system, and a TypeError in Python.

    An author reaching for either name should catch it, so the refusal answers
    to both rather than making them guess which vocabulary applies.
    """

    assert issubclass(HandleUsedAsValue, AuthoringError)
    assert issubclass(HandleUsedAsValue, TypeError)


def test_planned_builds_exactly_what_the_draft_form_builds():
    """The claim that licenses `@planned`: it is the same plan, spelled shorter.

    If these two ever diverge, the decorator has become a second way to mean
    something slightly different — which is the thing it exists to remove.
    """

    @operation(inputs={"model": MODEL}, outputs={"raw": RAW})
    def simulate(model):
        raise AssertionError("must not run")

    @planned
    def decorated(locator):
        model = input_artifact(
            address("test-address-space", locator),
            artifact=MODEL,
        )
        return {"raw": simulate.named("sim")(model).raw}

    with plan() as draft:
        model = input_artifact(
            address("test-address-space", "point.cir"),
            artifact=MODEL,
        )
        outputs = {"raw": simulate.named("sim")(model).raw}
    explicit = draft.finish(outputs=outputs)

    assert decorated("point.cir").to_data() == explicit.to_data()


def test_a_planned_family_takes_arguments_and_can_be_called_again():
    """One decorated function, many plans — and no draft to have used up."""

    @operation(config={"key": parameter(str)}, outputs={"raw": RAW})
    def produce(*, key):
        raise AssertionError("must not run")

    @planned
    def build(key):
        return produce.named(key)(key=key)

    first = build("left")
    second = build("right")
    again = build("left")

    assert first.to_data() == again.to_data(), "the same arguments are the same plan"
    assert first.to_data() != second.to_data()
    assert build.__name__ == "build"


def test_a_planned_body_that_raises_leaves_nothing_behind():
    """A failed plan must not poison the next one authored in this process."""

    @operation(outputs={"raw": RAW})
    def produce():
        raise AssertionError("must not run")

    @planned
    def broken():
        produce.named("recorded")()
        raise ValueError("the author's own mistake")

    @planned
    def sound():
        return produce.named("recorded")()

    with pytest.raises(ValueError, match="the author's own mistake"):
        broken()

    # The scope was closed on the way out, so this records from a clean draft
    # rather than joining a half-authored one.
    assert len(sound().invocations) == 1


def test_a_planned_result_that_is_not_a_mapping_is_named_output():
    """The same rule a flow's return value follows, at the root."""

    @operation(outputs={"raw": RAW})
    def produce():
        raise AssertionError("must not run")

    @planned
    def single():
        return produce.named("only")()

    assert [item.name for item in single().outputs] == ["output"]


def test_a_planned_default_policy_reaches_every_call_inside_it():
    @operation(outputs={"raw": RAW})
    def produce():
        raise AssertionError("must not run")

    declared = named_policy("lsf")(queue="short")

    @planned(default_policy=declared)
    def build():
        return produce.named("only")()

    (invocation,) = build().invocations
    assert invocation.policy == declared


def test_one_plan_at_a_time_still_holds_when_the_scope_is_a_decorator():
    """Hiding the `with` must not quietly permit what it refused."""

    @operation(outputs={"raw": RAW})
    def produce():
        raise AssertionError("must not run")

    @planned
    def inner():
        return produce.named("inner")()

    @planned
    def outer():
        inner()
        return produce.named("outer")()

    with pytest.raises(PlanningScopeError, match="nested plan contexts"):
        outer()
