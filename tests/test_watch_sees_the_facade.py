"""The observer must recognise work the façade submitted.

`hedloom_exec.watch` asks LSF about attempts it believes are live, and picks them
by substrate. The façade never hands the executor a bare queue: it wraps every
site transport in a `BoundTransport`, so the transport that submits is named for
the wrapper and the queue is underneath it.

Nothing here tested that seam. `exec/tests/test_watch.py` writes its journals
directly, so it could only ever assert what it had just written, and the
observer read an empty farm for every study the façade ran — silently, because
finding no live jobs is what a finished sweep also looks like. The number lost
with it is queue latency, which is the evidence the pooled-versus-direct
question has always been waiting for.
"""

from pathlib import Path

from hedloom_exec.durability import Durability, execute
from hedloom_exec.identity import try_name
from hedloom_exec.transport import Observation, substrate_of
from hedloom_exec.watch import live_attempts, observe, status_of

from hedloom.binding import BoundTransport, Shell


class FakeQueue:
    """Stands in for the real queue: named like it, accepts, never finishes."""

    name = "lsf-interactive"
    discovery_is_authoritative = True

    def __init__(self) -> None:
        self.submitted: list[str] = []

    def submit(self, identity, bundle):
        self.submitted.append(identity)
        return {"transport": self.name, "identity": identity, "job": identity}

    def discover(self, identity):
        return None

    def poll(self, handle):
        return Observation("running", {})

    def cancel(self, handle):
        return None


class FakeReader:
    """One `bjobs` answer, keyed by job name exactly as the real one is."""

    def __init__(self, states):
        self._states = states

    def states(self):
        return dict(self._states)


def simulate(**arguments):
    return Shell(["true"])


def run_one(root: Path) -> str:
    transport = BoundTransport({"simulate": simulate}, FakeQueue())
    execute(
        transport,
        {"operation": "simulate", "arguments": {}},
        durability=Durability.RECORDED,
        root=str(root),
        plan_id="study",
        invocation_id="invoke:point",
    )
    live = live_attempts(root)
    assert len(live) == 1, "one attempt was submitted and none concluded"
    return live[0].identity


def test_the_wrapper_declares_the_substrate_underneath_it():
    bound = BoundTransport({}, FakeQueue())

    assert bound.name == "bound:lsf-interactive", "the wrapper is still named"
    assert substrate_of(bound) == "lsf-interactive", "and it says where work lands"


def test_a_bare_transport_is_its_own_substrate():
    assert substrate_of(FakeQueue()) == "lsf-interactive"
    assert substrate_of(BoundTransport({})) == "in-process"


def test_an_attempt_the_facade_submitted_names_the_queue_not_the_wrapper(tmp_path):
    identity = run_one(tmp_path)

    status = status_of(tmp_path, identity)

    assert status.transport == "bound:lsf-interactive", "what submitted it"
    assert status.substrate == "lsf-interactive", "where it actually landed"


def test_the_observer_asks_the_farm_about_work_the_facade_submitted(tmp_path):
    """The whole point: this used to return an empty sweep."""

    identity = run_one(tmp_path)

    rows = observe(tmp_path, FakeReader({try_name(identity, 0): "pending"}))

    assert [row.observed for row in rows] == ["pending"]


def test_queue_latency_becomes_computable(tmp_path):
    identity = run_one(tmp_path)

    observe(tmp_path, FakeReader({try_name(identity, 0): "pending"}))
    observe(tmp_path, FakeReader({try_name(identity, 0): "running"}))

    status = status_of(tmp_path, identity)
    assert status.queue_seconds is not None, (
        "the gap between submit_intent and the first running observation is the "
        "per-job dispatch cost, and it is the number a max_jobs is chosen from"
    )
    assert status.queue_seconds >= 0
