import inspect

import pytest

from hedloom_exec.identity import (
    IdentityError,
    attempt_identity,
    parse_try_name,
    try_name,
)


def test_a_record_identity_ignores_the_try_number():
    identity = attempt_identity(plan_id="plan-1", invocation_id="inv-a")
    assert try_name(identity.rendered, 0).startswith(identity.rendered)
    assert try_name(identity.rendered, 9).startswith(identity.rendered)


def test_attempt_identity_no_longer_accepts_a_sequence():
    assert "sequence" not in inspect.signature(attempt_identity).parameters
    with pytest.raises(TypeError):
        attempt_identity(plan_id="p", invocation_id="i", sequence=0)


def test_the_same_inputs_always_render_the_same_record():
    first = attempt_identity(plan_id="plan-1", invocation_id="inv-a", input_digest="a")
    second = attempt_identity(plan_id="plan-1", invocation_id="inv-a", input_digest="a")
    assert first == second


def test_changed_inputs_render_a_different_record():
    first = attempt_identity(plan_id="plan-1", invocation_id="inv-a", input_digest="a")
    second = attempt_identity(plan_id="plan-1", invocation_id="inv-a", input_digest="b")
    assert first.rendered != second.rendered


def test_a_try_name_is_the_record_identity_and_its_number():
    identity = attempt_identity(plan_id="plan-1", invocation_id="inv-a").rendered
    assert try_name(identity, 12) == f"{identity}-12"


def test_a_try_name_is_usable_as_a_batch_job_name():
    identity = attempt_identity(plan_id="plan-1", invocation_id="inv-a").rendered
    rendered = try_name(identity, 3)
    assert rendered.replace("-", "").isalnum()
    assert len(rendered) <= 60


def test_parse_try_name_round_trips_every_rendered_identity():
    for plan, invocation, number in (("p", "i", 0), ("ab", "c", 417)):
        identity = attempt_identity(plan_id=plan, invocation_id=invocation).rendered
        assert parse_try_name(try_name(identity, number)) == (identity, number)


@pytest.mark.parametrize(
    "name",
    ["hedloom-record-0", "hedloom-ABCDEF0123456789abcd-0", "x" * 20 + "-0"],
)
def test_parse_try_name_refuses_a_base_that_is_not_a_rendered_identity(name):
    with pytest.raises(IdentityError):
        parse_try_name(name)


@pytest.mark.parametrize("suffix", ["-1", "00", "01", "+1", "1.0", ""])
def test_parse_try_name_refuses_a_negative_or_padded_number(suffix):
    identity = attempt_identity(plan_id="p", invocation_id="i").rendered
    with pytest.raises(IdentityError):
        parse_try_name(f"{identity}-{suffix}")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"plan_id": "", "invocation_id": "inv"},
        {"plan_id": "plan", "invocation_id": ""},
        {"plan_id": "plan\x1fsplit", "invocation_id": "inv"},
        {"plan_id": "plan", "invocation_id": "inv\nnewline"},
    ],
)
def test_ambiguous_components_are_refused(kwargs):
    with pytest.raises(IdentityError):
        attempt_identity(**kwargs)


def test_components_cannot_be_confused_by_reassociation():
    first = attempt_identity(plan_id="ab", invocation_id="c")
    second = attempt_identity(plan_id="a", invocation_id="bc")
    assert first.rendered != second.rendered
