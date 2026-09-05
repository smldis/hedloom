import inspect

import pytest

from hedloom_exec.identity import (
    IdentityError,
    attempt_identity,
    parse_try_name,
    try_name,
)


def test_a_record_identity_ignores_the_try_number():
    identity = attempt_identity(computation_digest="plan-1/inv-a")
    assert try_name(identity.rendered, 0).startswith(identity.rendered)
    assert try_name(identity.rendered, 9).startswith(identity.rendered)


def test_attempt_identity_no_longer_accepts_a_sequence():
    assert "sequence" not in inspect.signature(attempt_identity).parameters
    with pytest.raises(TypeError):
        attempt_identity(computation_digest="i", sequence=0)


def test_the_record_is_selected_by_the_declared_computation_alone():
    parameters = inspect.signature(attempt_identity).parameters
    assert list(parameters) == ["computation_digest"]
    for absent in ("plan_id", "invocation_id", "authored_key", "study"):
        assert absent not in parameters


@pytest.mark.parametrize("requester", ["plan_id", "invocation_id", "authored_key"])
def test_requester_metadata_is_not_accepted_at_all(requester):
    with pytest.raises(TypeError):
        attempt_identity(computation_digest="a", **{requester: "anything"})


def test_the_same_declaration_always_renders_the_same_record():
    first = attempt_identity(computation_digest="a")
    second = attempt_identity(computation_digest="a")
    assert first == second
    assert first.rendered == second.rendered


def test_a_changed_declaration_renders_a_different_record():
    first = attempt_identity(computation_digest="a")
    second = attempt_identity(computation_digest="b")
    assert first.rendered != second.rendered
    assert first != second


def test_identity_equality_is_record_equality():
    """Nothing about a requester can make one shared record compare as two."""

    assert attempt_identity(computation_digest="a") == attempt_identity(
        computation_digest="a"
    )


def test_a_try_name_is_the_record_identity_and_its_number():
    identity = attempt_identity(computation_digest="plan-1/inv-a").rendered
    assert try_name(identity, 12) == f"{identity}-12"


def test_a_try_name_is_usable_as_a_batch_job_name():
    identity = attempt_identity(computation_digest="plan-1/inv-a").rendered
    rendered = try_name(identity, 3)
    assert rendered.replace("-", "").isalnum()
    assert len(rendered) <= 60


def test_parse_try_name_round_trips_every_rendered_identity():
    for declaration, number in (("p", 0), ("ab", 417)):
        identity = attempt_identity(computation_digest=declaration).rendered
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
    identity = attempt_identity(computation_digest="p").rendered
    with pytest.raises(IdentityError):
        parse_try_name(f"{identity}-{suffix}")


@pytest.mark.parametrize(
    "digest",
    ["", "declaration\x1fsplit", "declaration\nnewline"],
)
def test_an_ambiguous_declaration_is_refused(digest):
    with pytest.raises(IdentityError):
        attempt_identity(computation_digest=digest)


def test_a_missing_declaration_is_refused_rather_than_shared():
    """No digest must not collapse every request onto one record."""

    with pytest.raises(IdentityError, match="non-empty"):
        attempt_identity(computation_digest=None)
    with pytest.raises(TypeError):
        attempt_identity()
