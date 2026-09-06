import pytest
from hedloom_flow import operation, flow, plan
from hedloom_flow.authoring import AuthoringError


@operation()
def prepare(): pass


@operation()
def other(): pass


def build(extra=False):
    with plan() as draft:
        prepare()
        if extra: other()
        prepare()
    return draft.finish(outputs={})


def test_unrelated_insertion_and_explicit_calls_do_not_renumber():
    first, extended = build(), build(True)
    assert [row.authored_key for row in first.invocations] == ['prepare.1', 'prepare.2']
    assert [row.id for row in first.invocations] == [row.id for row in extended.invocations if row.operation == prepare.identity]
    with plan() as draft:
        prepare.named('explicit')()
        prepare()
    assert [row.authored_key for row in draft.finish(outputs={}).invocations] == ['explicit', 'prepare.1']


@pytest.mark.parametrize('explicit_first', [False, True])
def test_explicit_generated_collision_in_either_order(explicit_first):
    with plan() as draft:
        if explicit_first: prepare.named('prepare.1')()
        else: prepare()
        with pytest.raises(AuthoringError, match='already used'):
            if explicit_first: prepare()
            else: prepare.named('prepare.1')()
    assert len(draft.finish(outputs={}).invocations) == 1


def test_different_definitions_with_same_short_name_require_explicit_keys():
    @operation(name='another.prepare')
    def prepare(): pass
    original = globals()['prepare']
    with plan() as draft:
        original()
        with pytest.raises(AuthoringError, match='distinct definitions'):
            prepare()
        prepare.named('distinct')()
    assert len(draft.finish(outputs={}).invocations) == 2


def test_failed_nested_call_restores_automatic_namespace_ownership():
    should_fail = [True]
    @flow
    def nested():
        prepare()
        if should_fail[0]: raise ValueError('nested failure')
    with plan() as draft:
        with pytest.raises(ValueError): nested()
        should_fail[0] = False
        nested()
        nested()
    document = draft.finish(outputs={})
    assert [row.authored_key for row in document.boundaries] == ['nested.1', 'nested.2']
    assert [row.authored_key for row in document.invocations] == ['prepare.1', 'prepare.1']


def test_occurrences_beyond_four_digits():
    with plan() as draft:
        for _ in range(10001): prepare()
    result = draft.finish(outputs={})
    assert result.invocations[-1].authored_key == 'prepare.10001'
