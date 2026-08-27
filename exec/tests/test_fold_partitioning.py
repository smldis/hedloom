from hedloom_exec.attempt import is_reusable
from hedloom_exec.identity import attempt_identity
from hedloom_exec.journal import AttemptJournal


IDENTITY = attempt_identity(plan_id="fold", invocation_id="sticky").rendered


def journal(tmp_path):
    return AttemptJournal(tmp_path, IDENTITY)


def test_a_cancellation_of_one_try_does_not_block_the_next(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        zero = log.begin_try()
        log.append("cancel_requested", **{"try": zero, "reason": "stop"})
        log.publish_terminal(try_number=zero, outcome="cancelled", manifest={})
        one = log.begin_try()
    state = log.fold()
    assert state.current_try == one
    assert state.cancel_requested is False
    assert state.tries[0].cancel_reason == "stop"


def test_accepting_one_trys_failure_does_not_make_a_later_try_reusable(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        zero = log.begin_try()
        first = log.publish_terminal(try_number=zero, outcome="failed", manifest={})
        log.append("reuse_accepted", **{"try": zero, "reason": "inspected"})
        log.make_standing(zero)
        one = log.begin_try()
        second = log.publish_terminal(try_number=one, outcome="failed", manifest={})
    state = log.fold()
    assert is_reusable(state.tries[0], first)
    assert not is_reusable(state.tries[1], second)
    assert state.reuse_accepted is False


def test_observations_are_attributed_to_the_try_they_describe(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        zero = log.begin_try()
        log.append("observed", **{"try": zero, "state": "running"})
        log.publish_terminal(try_number=zero, outcome="failed", manifest={})
        one = log.begin_try()
        log.append("observed", **{"try": one, "state": "pending"})
    state = log.fold()
    assert state.tries[0].observations == ({"state": "running"},)
    assert state.observations == ({"state": "pending"},)


def test_sticky_state_from_try_zero_is_not_reported_as_try_threes(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        for number in range(4):
            assert log.begin_try() == number
            if number == 0:
                log.append("cancel_requested", **{"try": number, "reason": "old"})
                log.append("reuse_accepted", **{"try": number, "reason": "old"})
                log.append("observed", **{"try": number, "state": "running"})
            if number < 3:
                log.publish_terminal(try_number=number, outcome="failed", manifest={})
    state = log.fold()
    assert state.current_try == 3
    assert not state.cancel_requested
    assert not state.reuse_accepted
    assert state.observations == ()
