"""Watching a sweep without owning it.

The tests that matter here are the ones about *not* interfering: an observer
writes its own file, records only transitions, and cannot change what an
attempt concludes. The rest is parsing, which is where the silent-wrongness
risk lives — a `PEND` read as `RUN` would be a lie about the one field anyone
is watching.
"""

import pytest

from hedloom_exec.durability import Durability, execute
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.identity import attempt_identity, try_name
from hedloom_exec.lsf import CommandResult
from hedloom_exec.transport import InProcessTransport, TransportError
from hedloom_exec.watch import (
    LSFStatusReader,
    ObservationLog,
    live_attempts,
    observe,
    render,
    status_of,
)


class FakeBjobs:
    """Replays one `bjobs -o` answer and records how often it was asked."""

    def __init__(self, rows=(), returncode=0, stderr=""):
        self.rows = list(rows)
        self.returncode = returncode
        self.stderr = stderr
        self.calls = []

    def __call__(self, argv, *, cwd=None, env=None, timeout=None):
        self.calls.append(list(argv))
        return CommandResult(
            returncode=self.returncode,
            stdout="".join(f"{name} {state}\n" for name, state in self.rows),
            stderr=self.stderr,
        )


def record_identity(label):
    return attempt_identity(plan_id="watch", invocation_id=label).rendered


def submitted_attempt(root, label="abc", transport="lsf-interactive"):
    """An attempt that has been submitted and has not concluded."""

    identity = record_identity(label)
    journal = AttemptJournal(root, identity)
    with journal.claim():
        number = journal.begin_try()
        job = try_name(identity, number)
        journal.append(
            "created",
            **{
                "try": number,
                "plan": "study",
                "invocation": "invoke:point-tt",
                "operation": "simulate",
                "input_digest": "d" * 32,
            },
        )
        journal.append(
            "submit_intent",
            **{"try": number, "transport": transport, "substrate": transport},
        )
        journal.append(
            "submit_receipt", **{"try": number, "handle": {"identity": job}}
        )
    return journal


def test_a_submitted_attempt_is_live_and_a_finished_one_is_not(tmp_path):
    live_record = submitted_attempt(tmp_path, "live")
    done = submitted_attempt(tmp_path, "done")
    with done.claim():
        done.publish_terminal(try_number=0, outcome="succeeded", manifest={"value": 1})

    live = live_attempts(tmp_path)

    assert [item.identity for item in live] == [live_record.identity]
    assert status_of(tmp_path, done.identity).outcome == "succeeded"


def test_one_call_answers_for_every_job(tmp_path):
    """A process per point per refresh would cost more than the work."""

    for index in range(5):
        submitted_attempt(tmp_path, str(index))
    runner = FakeBjobs(
        [(try_name(record_identity(str(index)), 0), "RUN") for index in range(5)]
    )

    observe(tmp_path, LSFStatusReader(runner))

    assert len(runner.calls) == 1
    assert runner.calls[0][:2] == ["bjobs", "-noheader"]


def test_the_farm_state_reaches_the_row(tmp_path):
    journal = submitted_attempt(tmp_path)
    rows = observe(
        tmp_path, LSFStatusReader(FakeBjobs([(try_name(journal.identity, 0), "PEND")]))
    )

    assert rows[0].observed == "pending"


def test_only_transitions_are_recorded(tmp_path):
    """Watching every ten seconds must not write six lines a minute per job."""

    journal = submitted_attempt(tmp_path)
    job = try_name(journal.identity, 0)
    pending = LSFStatusReader(FakeBjobs([(job, "PEND")]))
    running = LSFStatusReader(FakeBjobs([(job, "RUN")]))

    observe(tmp_path, pending)
    observe(tmp_path, pending)
    observe(tmp_path, pending)
    observe(tmp_path, running)

    log = ObservationLog(tmp_path, journal.identity)
    states = [entry["state"] for entry in log.entries()]
    assert states == ["pending", "running"]


def test_queue_latency_is_what_the_transition_measures(tmp_path):
    """The number the pooled-versus-direct question has always lacked."""

    journal = submitted_attempt(tmp_path)
    job = try_name(journal.identity, 0)
    observe(tmp_path, LSFStatusReader(FakeBjobs([(job, "PEND")])))
    rows = observe(tmp_path, LSFStatusReader(FakeBjobs([(job, "RUN")])))

    assert rows[0].queue_seconds is not None
    assert rows[0].queue_seconds >= 0


def test_an_observer_writes_beside_the_record_and_never_into_it(tmp_path):
    """The invariant: evidence about an attempt, not a transition of it."""

    journal = submitted_attempt(tmp_path)
    before = [event.event for event in journal.events()]

    observe(
        tmp_path,
        LSFStatusReader(FakeBjobs([(try_name(journal.identity, 0), "RUN")])),
    )

    after = [event.event for event in AttemptJournal(tmp_path, journal.identity).events()]
    assert after == before, "the owner's log must be untouched"
    assert (tmp_path / journal.identity / "observations.jsonl").exists()


def test_observing_cannot_change_what_an_attempt_concludes(tmp_path):
    """A watcher may be wrong, restarted, or malicious; results are not its call."""

    transport = InProcessTransport({"work": lambda **kwargs: 41})
    common = {
        "durability": Durability.RECORDED,
        "root": str(tmp_path),
        "plan_id": "study",
        "invocation_id": "invoke:a",
    }
    first = execute(transport, {"operation": "work"}, **common)
    for identity in (path.name for path in tmp_path.iterdir()):
        if (tmp_path / identity / "events.jsonl").exists():
            ObservationLog(tmp_path, identity).record(0, "failed", note="a lie")

    second = execute(transport, {"operation": "work"}, **common)

    assert first.outcome == "succeeded"
    assert second.disposition == "completed", "reuse must ignore observations"
    assert second.value == 41


def test_a_record_written_before_substrates_were_told_apart_is_still_watched(
    tmp_path,
):
    """`submit_intent` used to carry one name for two facts.

    Folding falls back to it, so a journal written by an older run is still
    asked about rather than quietly dropped from every sweep.
    """

    identity = record_identity("old")
    journal = AttemptJournal(tmp_path, identity)
    with journal.claim():
        number = journal.begin_try()
        journal.append(
            "created", **{"try": number, "invocation": "invoke:old", "operation": "simulate"}
        )
        journal.append(
            "submit_intent", **{"try": number, "transport": "lsf-interactive"}
        )

    status = status_of(tmp_path, identity)

    assert status.substrate == "lsf-interactive"
    assert observe(
        tmp_path, LSFStatusReader(FakeBjobs([(try_name(identity, 0), "PEND")]))
    )[0].observed == "pending"


def test_a_wrapped_transport_is_watched_by_what_holds_the_job(tmp_path):
    """The failure this file could not previously see.

    A caller may wrap the queue in something that runs an authored body first.
    The wrapper is what submitted the attempt; the queue is what holds the job,
    and it is the queue a farm can be asked about. Matching the wrapper's name
    returned an empty sweep, which is indistinguishable from a finished one.
    """

    identity = record_identity("wrapped")
    journal = AttemptJournal(tmp_path, identity)
    with journal.claim():
        number = journal.begin_try()
        journal.append(
            "created",
            **{"try": number, "invocation": "invoke:wrapped", "operation": "simulate"},
        )
        journal.append(
            "submit_intent",
            **{
                "try": number,
                "transport": "bound:lsf-interactive",
                "substrate": "lsf-interactive",
            },
        )

    rows = observe(
        tmp_path, LSFStatusReader(FakeBjobs([(try_name(identity, 0), "RUN")]))
    )

    assert [row.observed for row in rows] == ["running"]


def test_an_attempt_on_another_substrate_is_not_invented(tmp_path):
    """An in-process invocation has no job to ask about."""

    submitted_attempt(tmp_path, "local", transport="in-process")
    runner = FakeBjobs()

    rows = observe(tmp_path, LSFStatusReader(runner))

    assert runner.calls == [], "nothing to ask LSF about"
    assert rows[0].observed is None


def test_a_job_absent_from_lsf_is_left_to_reconciliation(tmp_path):
    """It may have just finished. Deciding that is the owner's job, not ours."""

    journal = submitted_attempt(tmp_path)
    rows = observe(tmp_path, LSFStatusReader(FakeBjobs([])))

    assert rows[0].observed is None
    assert not (tmp_path / journal.identity / "observations.jsonl").exists()


def test_an_lsf_too_old_for_the_stable_format_refuses(tmp_path):
    """Parsing default bjobs columns would read PEND as RUN on some rows."""

    submitted_attempt(tmp_path)
    reader = LSFStatusReader(
        FakeBjobs(returncode=255, stderr="bjobs: Illegal option -- o")
    )

    with pytest.raises(TransportError) as raised:
        observe(tmp_path, reader)

    assert "-o" in str(raised.value)


def test_a_corrupt_observation_file_cannot_hide_a_result(tmp_path):
    journal = submitted_attempt(tmp_path)
    (tmp_path / journal.identity / "observations.jsonl").write_text("{not json\n")

    assert status_of(tmp_path, journal.identity).observed is None


def test_the_view_names_the_invocation_rather_than_the_digest(tmp_path):
    journal = submitted_attempt(tmp_path)
    rows = observe(
        tmp_path, LSFStatusReader(FakeBjobs([(try_name(journal.identity, 0), "RUN")]))
    )

    text = render(rows)
    assert "invoke:point-tt" in text
    assert "running" in text
