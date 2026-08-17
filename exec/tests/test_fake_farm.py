"""End-to-end submission through the real subprocess layer.

These tests use a fake `bsub`/`bjobs`/`bkill` on PATH rather than the injected
runner, so everything below the transport is genuine: argument construction,
child binding, exit-status propagation, and output capture.

The lifetime guarantee is genuine too, and that is the point of the last three.
`bsub -I` promises the job dies with its client; the whole crash-window argument
in `hedloom_exec.attempt` rests on it, and a TLA+ model of that protocol
(`docs/attempt-claim-protocol.md`) found it is what closes the window rather than
`discovery_is_authoritative`. So it is tested by killing a submitter for real and
asking the farm what happened, through the same chain a farm run uses: this
process binds its `bsub` client with `PR_SET_PDEATHSIG`, and the client binds the
command the same way.

What remains unreproducible is LSF's own scheduling — contention, fair share, and
a queue that pends because the farm is busy rather than because
`FAKE_LSF_PEND_SECONDS` said so.
"""

import json
import os
import signal
import subprocess
import sys
import time

import pytest

from hedloom_exec.attempt import launch_or_attach, reconcile
from hedloom_exec.durability import Durability, execute
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.lsf import LSFInteractiveTransport

FARM = os.path.join(os.path.dirname(__file__), "fakefarm")
EXEC_SRC = os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="the fake farm uses executable scripts"
)


@pytest.fixture
def farm(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", FARM + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("FAKE_LSF_STATE", str(tmp_path / "farm"))
    return LSFInteractiveTransport(defaults={"walltime": "5", "queue": "normal"})


def test_a_real_submission_runs_the_command_and_records_success(farm, tmp_path):
    journal = AttemptJournal(tmp_path, "hedloom-farm-ok")
    bundle = {"command": [sys.executable, "-c", "print('simulated')"]}

    launch_or_attach(journal, farm, bundle)
    state = reconcile(journal, farm)

    assert state.outcome == "succeeded"
    assert "simulated" in journal.read_manifest()["result"]["stdout"]


def test_a_failing_command_propagates_its_exit_status(farm, tmp_path):
    journal = AttemptJournal(tmp_path, "hedloom-farm-fail")
    bundle = {
        "command": [
            sys.executable,
            "-c",
            "print('failure detail'); raise SystemExit(3)",
        ]
    }

    launch_or_attach(journal, farm, bundle)
    state = reconcile(journal, farm)

    assert state.outcome == "failed"
    result = journal.read_manifest()["result"]
    assert result["returncode"] == 3
    assert result["stdout"] == "failure detail\n"
    assert result["error"] == "bsub -I exited with status 3"


def test_the_submission_reaches_bsub_with_its_declared_shape(farm, tmp_path):
    import json

    result = execute(
        farm,
        {"command": [sys.executable, "-c", "pass"]},
        durability=Durability.RECORDED,
        root=str(tmp_path),
        plan_id="plan-1",
        invocation_id="inv-shape",
    )

    identity = result.journal.identity
    recorded = json.loads((tmp_path / "farm" / f"{identity}.json").read_text())
    assert recorded["options"]["-J"] == identity
    assert recorded["options"]["-W"] == "5"
    assert recorded["options"]["-q"] == "normal"


def test_a_finished_job_is_not_discovered(farm, tmp_path):
    """`bjobs` without `-a` reports the active queue, and finished work has left.

    This once answered "still there" for a job that had ended, which hid the
    whole `submit_lost` branch: an attempt in the crash window would attach to a
    job that was already over instead of establishing nothing was accepted.
    """

    result = execute(
        farm,
        {"command": [sys.executable, "-c", "pass"]},
        durability=Durability.RECORDED,
        root=str(tmp_path),
        plan_id="plan-1",
        invocation_id="inv-done",
    )

    assert result.outcome == "succeeded"
    assert farm.discover(result.journal.identity) is None


def test_discovery_and_cancellation_reach_the_real_commands(farm, tmp_path):
    """The case `discover` exists for: something a previous run left behind.

    Planted rather than submitted, because a job this process submits is over by
    the time `submit` returns — with owner-bound lifetime a live match means
    someone else's job, which is exactly what the record cannot rule out.
    """

    state = tmp_path / "farm"
    state.mkdir(parents=True, exist_ok=True)
    identity = "hedloom-left-behind"
    # No `owner_pid`: a leftover whose client this fake never saw, which is the
    # one thing it cannot prove is gone.
    (state / f"{identity}.json").write_text(
        json.dumps({"name": identity, "state": "RUN", "options": {"-q": "normal"}}),
        encoding="utf-8",
    )

    found = farm.discover(identity)
    assert found is not None and found["kind"] == "live"
    farm.cancel({"identity": identity})
    assert farm.discover(identity) is None


def test_an_unknown_job_name_is_not_discovered(farm):
    assert farm.discover("hedloom-never-submitted") is None


def submitter(tmp_path, root, identity, command, farm_state):
    """A separate process that submits and blocks in `bsub -I`.

    Separate because the thing under test is what happens when the submitter
    *dies*, and a test cannot kill itself. It goes through `launch_or_attach`, so
    the durable record it leaves behind is the real one.
    """

    script = tmp_path / f"submit-{identity}.py"
    script.write_text(
        "import sys\n"
        f"sys.path.insert(0, {EXEC_SRC!r})\n"
        "from hedloom_exec.attempt import launch_or_attach\n"
        "from hedloom_exec.journal import AttemptJournal\n"
        "from hedloom_exec.lsf import LSFInteractiveTransport\n"
        f"journal = AttemptJournal({str(root)!r}, {identity!r})\n"
        "transport = LSFInteractiveTransport("
        'defaults={"walltime": "5", "queue": "normal"})\n'
        f"launch_or_attach(journal, transport, {{'command': {command!r}}})\n",
        encoding="utf-8",
    )
    return subprocess.Popen(
        [sys.executable, str(script)],
        env={
            **os.environ,
            "PATH": FARM + os.pathsep + os.environ["PATH"],
            "FAKE_LSF_STATE": str(farm_state),
        },
    )


def _alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def test_a_job_dies_with_the_client_that_submitted_it(tmp_path, monkeypatch):
    """Owner-bound lifetime, end to end, through both bindings.

    This process binds its `bsub` client, the client binds the command, and the
    farm stops reporting a job whose client is gone. The whole crash-window
    argument depends on it: if the job survived, a later run establishing "not
    accepted" and resubmitting would duplicate real work.
    """

    root = tmp_path / "attempts"
    state = tmp_path / "farm"
    marker = tmp_path / "ran.txt"
    identity = "hedloom-owner-bound"
    command = [
        "/bin/sh",
        "-c",
        f"printf started >> {marker}; sleep 1.5; printf finished >> {marker}",
    ]

    child = submitter(tmp_path, root, identity, command, state)
    try:
        assert wait_for(lambda: marker.exists() and marker.read_text() == "started"), (
            "the farm job should have started"
        )
        record = state / f"{identity}.json"
        assert wait_for(lambda: json.loads(record.read_text())["state"] == "RUN")
        child.kill()
        child.wait(timeout=10)
    finally:
        if child.poll() is None:  # pragma: no cover - only on an unexpected hang
            child.kill()
            child.wait()

    monkeypatch.setenv("PATH", FARM + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("FAKE_LSF_STATE", str(state))
    transport = LSFInteractiveTransport(defaults={"walltime": "5", "queue": "normal"})

    assert wait_for(lambda: transport.discover(identity) is None), (
        "a job whose client was killed must leave the queue"
    )
    # The work itself is gone, not merely unreported: ask the kernel. The window
    # is deliberately far shorter than the command's remaining sleep, because a
    # generous one passes when the job merely *finished* — which is how this
    # assertion first passed against a fake with no binding at all.
    job_pid = json.loads(record.read_text())["job_pid"]
    assert wait_for(lambda: not _alive(job_pid), timeout=0.5), (
        "the command outlived its client"
    )
    # And it never reached the write that follows its sleep, so nothing was
    # half-done behind hedloom's back.
    time.sleep(1.6)
    assert marker.read_text() == "started"


def test_the_crash_window_resubmits_instead_of_attaching(tmp_path, monkeypatch):
    """Durable intent with no receipt, resolved by asking the farm.

    The path a TLA+ model of this protocol names as load-bearing, and the one the
    fake could not reach while it reported finished jobs as running: intent is on
    disk, the submitter is gone, and discovery has to establish that nothing is
    accepted before this attempt may be launched again.
    """

    root = tmp_path / "attempts"
    state = tmp_path / "farm"
    identity = "hedloom-crash-window"
    command = ["/bin/sh", "-c", "sleep 1"]

    child = submitter(tmp_path, root, identity, command, state)
    journal = AttemptJournal(root, identity)
    record = state / f"{identity}.json"
    try:
        assert wait_for(
            lambda: journal.exists()
            and any(item.event == "submit_intent" for item in journal.events())
        ), "the submitter should have recorded its intent"
        # Wait for the job to be *running* before killing the client, not merely
        # for intent to be on disk. Killing earlier tests a different case — a
        # crash before the substrate was reached at all — and leaves discovery
        # with nothing it could have found either way, so the interesting path
        # would only sometimes be the one exercised.
        assert wait_for(
            lambda: record.exists()
            and json.loads(record.read_text()).get("state") == "RUN"
        ), "the job should have started before its client dies"
        child.kill()
        child.wait(timeout=10)
    finally:
        if child.poll() is None:  # pragma: no cover
            child.kill()
            child.wait()

    # The crash window itself: intent is durable, no receipt followed.
    state_before = AttemptJournal(root, identity).fold()
    assert state_before.phase == "intended"

    monkeypatch.setenv("PATH", FARM + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("FAKE_LSF_STATE", str(state))
    transport = LSFInteractiveTransport(defaults={"walltime": "5", "queue": "normal"})
    resumed = AttemptJournal(root, identity)
    launched = launch_or_attach(resumed, transport, {"command": command})

    events = [item.event for item in resumed.events()]
    assert launched.disposition == "claimed", (
        "nothing was accepted, so this call owns the attempt"
    )
    assert "submit_lost" in events, events
    # Recorded in order: the lost submission is followed by a fresh one, so the
    # record explains the duplicate intent rather than merely holding two.
    assert events.index("submit_lost") < events.index("submit_receipt")
    assert reconcile(resumed, transport).outcome == "succeeded"


def test_the_watcher_sees_a_job_pend_and_then_run(tmp_path, monkeypatch):
    """`PEND -> RUN`, through the reader the watcher actually uses.

    `LSFStatusReader` asks `bjobs -noheader -o "job_name stat"`, which this fake
    did not implement — so `states()` read the non-zero exit as "no unfinished
    job found" and the watcher reported nothing, for every run. It is the one
    part of watching that no injected reader can stand in for.
    """

    from hedloom_exec.watch import observe

    root = tmp_path / "attempts"
    state = tmp_path / "farm"
    identity = "hedloom-pending"
    monkeypatch.setenv("FAKE_LSF_PEND_SECONDS", "0.8")
    monkeypatch.setenv("PATH", FARM + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("FAKE_LSF_STATE", str(state))

    child = submitter(tmp_path, root, identity, ["/bin/sh", "-c", "sleep 2"], state)
    journal = AttemptJournal(root, identity)
    try:
        assert wait_for(
            lambda: journal.exists()
            and any(item.event == "submit_intent" for item in journal.events())
        )
        assert wait_for(
            lambda: any(row.observed == "pending" for row in observe(root))
        ), "a queued job must read as pending"
        assert wait_for(
            lambda: any(row.observed == "running" for row in observe(root)),
            timeout=15.0,
        ), "and must be seen to start"
    finally:
        child.kill()
        child.wait(timeout=10)
