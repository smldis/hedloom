"""``watch=True`` reports the farm while preserving the run it observes.

The executor's observer already owns LSF parsing and durable observations. The
façade only has to keep that observer alive beside a submission, print its
transitions, and get out of the way if status cannot be read.
"""

import importlib
from pathlib import Path
import time
from threading import Event, enumerate as threads

import pytest

from hedloom import Site, local, operation, returned, study
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.identity import attempt_identity, try_name
from hedloom_exec.transport import TransportError
from hedloom_exec.watch import status_of
from hedloom.study import _WATCH_THREAD_NAME, _watch


class ReplayReader:
    """Return a sequence of complete farm readings without patching LSF."""

    def __init__(self, *states):
        self._states = list(states)
        self.calls = 0

    def states(self):
        self.calls += 1
        state = self._states.pop(0)
        if isinstance(state, Exception):
            raise state
        return dict(state)


class StopAfter:
    """Give the synchronous poll loop a fixed number of refreshes."""

    def __init__(self, refreshes):
        self._remaining = refreshes

    def wait(self, timeout):
        self._remaining -= 1
        return self._remaining == 0


def submitted_attempt(root: Path, label: str = "point") -> AttemptJournal:
    identity = attempt_identity(plan_id="watch-submit", invocation_id=label).rendered
    journal = AttemptJournal(root, identity)
    with journal.claim():
        number = journal.begin_try()
        job = try_name(identity, number)
        journal.append(
            "created",
            **{
                "try": number,
                "plan": "study",
                "invocation": "point",
                "operation": "simulate",
                "input_digest": "d" * 32,
            },
        )
        journal.append(
            "submit_intent",
            **{
                "try": number,
                "transport": "bound:lsf-interactive",
                "substrate": "lsf-interactive",
            },
        )
        journal.append(
            "submit_receipt", **{"try": number, "handle": {"identity": job}}
        )
    return journal


def test_the_poller_prints_each_transition_once_and_queue_time_on_running(
    tmp_path, capsys
):
    """Repeated refreshes are silence; state changes are the useful evidence."""

    journal = submitted_attempt(tmp_path)
    job = try_name(journal.identity, 0)
    reader = ReplayReader(
        {job: "pending"},
        {job: "pending"},
        {job: "running"},
    )

    _watch(tmp_path, reader, StopAfter(3))

    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == "[watch] point → pending"
    assert lines[1].startswith("[watch] point pending → running (")
    assert lines[1].endswith("s queued)")
    assert len(lines) == 2
    assert status_of(tmp_path, journal.identity).queue_seconds is not None


def test_a_job_first_seen_running_still_prints_a_queue_measurement(tmp_path, capsys):
    """Missing one PEND sample must not discard submission-to-RUN latency."""

    journal = submitted_attempt(tmp_path)
    job = try_name(journal.identity, 0)

    _watch(
        tmp_path,
        ReplayReader({job: "running"}),
        StopAfter(1),
    )

    line = capsys.readouterr().out.strip()
    assert line.startswith("[watch] point → running (")
    assert line.endswith("s queued)")
    assert status_of(tmp_path, journal.identity).queue_seconds is not None


@operation(outputs={"value": returned()})
def local_value():
    return 7


@study(default_policy=local())
def local_study():
    return {"value": local_value.named("point")().value}


def test_a_local_study_never_calls_the_status_reader_and_keeps_completion_output(
    tmp_path, capsys
):
    """A local run has no scheduler to ask and must retain today's reporter."""

    reader = ReplayReader(AssertionError("local work called bjobs"))
    site = Site(root=str(tmp_path / "attempts"))

    run = local_study().submit(site=site, watch=True, _watch_reader=reader)

    output = capsys.readouterr().out
    assert run.succeeded
    assert run.value == 7
    assert reader.calls == 0
    assert "[watch]" not in output
    assert "point" in output and "succeeded" in output
    assert not any(
        thread.name == _WATCH_THREAD_NAME and thread.is_alive()
        for thread in threads()
    )


class WedgedReader:
    def __init__(self, job):
        self.job = job
        self.entered = Event()
        self.release = Event()

    def states(self):
        self.entered.set()
        self.release.wait()
        return {self.job: "pending"}


def test_a_wedged_reader_leaves_only_a_daemon_and_cannot_hold_submit(tmp_path):
    """A scheduler command that never returns must not own process lifetime."""

    root = tmp_path / "attempts"
    journal = submitted_attempt(root, "existing-farm-job")
    reader = WedgedReader(try_name(journal.identity, 0))
    started = time.monotonic()

    run = local_study().submit(
        site=Site(root=str(root)),
        watch=True,
        _watch_reader=reader,
    )

    elapsed = time.monotonic() - started
    lingering = [
        thread
        for thread in threads()
        if thread.name == _WATCH_THREAD_NAME and thread.is_alive()
    ]
    assert run.succeeded
    assert reader.entered.is_set()
    assert elapsed < 2
    assert lingering and all(thread.daemon for thread in lingering)

    reader.release.set()
    for thread in lingering:
        thread.join(timeout=1)


_WATCHER_REACHED_READER = False


@operation(outputs={"value": returned()})
def wait_for_watcher():
    deadline = time.monotonic() + 2
    while not _WATCHER_REACHED_READER:
        if time.monotonic() >= deadline:
            raise RuntimeError("the watcher never reached its injected reader")
        time.sleep(0.001)
    return 41


@study(default_policy=local())
def waiting_study():
    return {"value": wait_for_watcher.named("point")().value}


class RefusingReader:
    def __init__(self):
        self.calls = 0

    def states(self):
        global _WATCHER_REACHED_READER

        self.calls += 1
        _WATCHER_REACHED_READER = True
        raise TransportError("bjobs is too old for -o")


def test_a_status_reader_failure_prints_once_and_cannot_fail_the_run(
    tmp_path, capsys
):
    """Unsupported LSF reporting is evidence failure, not execution failure."""

    global _WATCHER_REACHED_READER

    _WATCHER_REACHED_READER = False
    root = tmp_path / "attempts"
    submitted_attempt(root, "existing-farm-job")
    reader = RefusingReader()

    run = waiting_study().submit(
        site=Site(root=str(root)),
        watch=True,
        _watch_reader=reader,
    )

    output = capsys.readouterr().out
    assert run.succeeded, run.summary()
    assert run.value == 41
    assert reader.calls == 1
    assert output.count("[watch disabled]") == 1
    assert "bjobs is too old for -o" in output
    assert "point" in output and "succeeded" in output


def test_a_raised_run_still_stops_and_joins_its_poller(tmp_path, monkeypatch):
    """The poller is scoped to submit even when the kernel has no report."""

    study_module = importlib.import_module("hedloom.study")

    def fail_run(document, **kwargs):
        raise RuntimeError("kernel escaped")

    monkeypatch.setattr(study_module, "run_plan", fail_run)

    with pytest.raises(RuntimeError, match="kernel escaped"):
        local_study().submit(
            site=Site(root=str(tmp_path / "attempts")),
            sequential=True,
            watch=True,
            _watch_reader=ReplayReader(AssertionError("local work called bjobs")),
        )

    assert not any(
        thread.name == _WATCH_THREAD_NAME and thread.is_alive()
        for thread in threads()
    )
