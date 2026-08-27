import pytest

from hedloom_exec.attempt import launch_or_attach
from hedloom_exec.identity import attempt_identity, try_name
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.lsf import LSFInteractiveTransport
from hedloom_exec.transport import Observation, TransportError


IDENTITY = attempt_identity(plan_id="recover", invocation_id="one").rendered
BUNDLE = {"operation": "work", "command": ["true"]}


class RecoveringTransport:
    name = "recovering"
    discovery_is_authoritative = True

    def __init__(self, *, found=True):
        self.found = found
        self.discovered = []
        self.submitted = []

    def discover(self, name):
        self.discovered.append(name)
        return {"identity": name} if self.found else None

    def submit(self, name, _bundle):
        self.submitted.append(name)
        return {"identity": name}

    def poll(self, _handle):
        return Observation("running")

    def cancel(self, _handle):
        return None


def intended(tmp_path):
    log = AttemptJournal(tmp_path, IDENTITY)
    with log.claim():
        number = log.begin_try()
        log.append(
            "submit_intent",
            **{"try": number, "transport": "recovering", "substrate": "recovering"},
        )
    return log


def test_discovery_after_a_lost_receipt_asks_for_the_try_name(tmp_path):
    transport = RecoveringTransport()
    launch_or_attach(intended(tmp_path), transport, BUNDLE)
    assert transport.discovered == [try_name(IDENTITY, 0)]


def test_a_lost_receipt_does_not_cause_a_second_submission(tmp_path):
    transport = RecoveringTransport()
    result = launch_or_attach(intended(tmp_path), transport, BUNDLE)
    assert result.disposition == "attached"
    assert transport.submitted == []


def test_discovery_given_the_record_name_is_refused_not_answered_negatively():
    calls = []

    def runner(argv, **_kwargs):
        calls.append(argv)
        raise AssertionError("LSF must not be asked about a bare record")

    transport = LSFInteractiveTransport(defaults={"walltime": "1"}, runner=runner)
    with pytest.raises(TransportError, match="record-local try"):
        transport.discover(IDENTITY)
    assert calls == []


def test_an_authoritative_negative_discovery_still_means_never_accepted(tmp_path):
    transport = RecoveringTransport(found=False)
    result = launch_or_attach(intended(tmp_path), transport, BUNDLE)
    expected = try_name(IDENTITY, 0)
    assert result.disposition == "claimed"
    assert transport.discovered == [expected]
    assert transport.submitted == [expected]
