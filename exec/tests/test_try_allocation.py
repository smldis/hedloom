import pytest

from hedloom_exec.attempt import launch_or_attach
from hedloom_exec.identity import attempt_identity, try_name
from hedloom_exec.journal import AttemptJournal, ClaimNotHeld, ConcurrentClaim
from hedloom_exec.transport import Observation, TransportError


IDENTITY = attempt_identity(computation_digest="allocation/one").rendered
BUNDLE = {"operation": "work"}


def journal(tmp_path):
    return AttemptJournal(tmp_path, IDENTITY)


def test_begin_try_requires_the_claim_to_be_held(tmp_path):
    with pytest.raises(ClaimNotHeld):
        journal(tmp_path).begin_try()


def test_begin_try_records_the_number_before_it_returns(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        assert log.begin_try() == 0
        assert log.events()[-1].event == "try_started"
        assert log.events()[-1].data["try"] == 0


class InspectingTransport:
    name = "inspect"
    discovery_is_authoritative = True

    def __init__(self, log, *, lose_receipt=False):
        self.log = log
        self.lose_receipt = lose_receipt
        self.submissions = []
        self.results = {}

    def submit(self, name, _bundle):
        events = self.log.events()
        assert events[-2].event == "try_started" or any(
            event.event == "try_started" and event.data["try"] == 0
            for event in events
        )
        assert events[-1].event == "submit_intent"
        self.submissions.append(name)
        self.results[name] = Observation("succeeded", {"value": 1})
        if self.lose_receipt:
            raise TransportError("receipt lost")
        return {"identity": name}

    def discover(self, name):
        return {"identity": name} if name in self.results else None

    def poll(self, handle):
        return self.results[handle["identity"]]

    def cancel(self, _handle):
        return None


def test_the_try_number_is_durable_before_any_transport_call(tmp_path):
    log = journal(tmp_path)
    transport = InspectingTransport(log)
    launch_or_attach(log, transport, BUNDLE)
    assert transport.submissions == [try_name(IDENTITY, 0)]


def test_a_crash_after_acceptance_leaves_the_job_discoverable(tmp_path):
    log = journal(tmp_path)
    transport = InspectingTransport(log, lose_receipt=True)
    with pytest.raises(TransportError):
        launch_or_attach(log, transport, BUNDLE)
    transport.lose_receipt = False
    resumed = launch_or_attach(AttemptJournal(tmp_path, IDENTITY), transport, BUNDLE)
    assert resumed.disposition == "attached"
    assert transport.submissions == [try_name(IDENTITY, 0)]


def test_a_try_allocated_but_never_submitted_is_resumed_not_abandoned(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        assert log.begin_try() == 0
    with log.claim():
        assert log.begin_try() == 0
    assert len([event for event in log.events() if event.event == "try_started"]) == 1


def test_an_interrupted_run_does_not_burn_try_numbers(tmp_path):
    log = journal(tmp_path)
    with log.claim():
        log.begin_try()
    transport = InspectingTransport(log)
    launch_or_attach(log, transport, BUNDLE)
    assert log.fold().current_try == 0


def test_two_claimants_cannot_receive_the_same_try_number(tmp_path):
    first = journal(tmp_path)
    second = journal(tmp_path)
    with first.claim():
        assert first.begin_try() == 0
        with pytest.raises(ConcurrentClaim):
            with second.claim():
                second.begin_try()
