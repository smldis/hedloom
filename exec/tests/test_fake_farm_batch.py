"""The fake farm's batch mode: `bsub` that accepts and returns.

Separate from `test_fake_farm.py` because it tests the *opposite* lifetime
promise. That file exists to prove an interactive job dies with its client;
this one exists to prove a batch job does not, which is the property
`dask_jobqueue.LSFCluster` is built on and the reason pooled placement needs a
different argument for how a worker ever stops.

Nothing here imports `dask_jobqueue`, or Dask at all. The fake is exercised
through the commands themselves, so these tests say whether the substrate is
faithful independently of whether anything is yet built on it — and they are
the tests that make "no LSF on this host" stop being a reason not to work on
pooled placement.

What remains unreproducible is LSF's own scheduling: contention, fair share,
and a queue that pends because the farm is busy rather than because
`FAKE_LSF_PEND_SECONDS` said so.
"""

import json
import os
import re
import subprocess
import sys
import time

import pytest

FARM = os.path.join(os.path.dirname(__file__), "fakefarm")

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="the fake farm uses executable scripts"
)


@pytest.fixture
def farm(tmp_path, monkeypatch):
    """PATH and state, the two things every call to the fake needs."""

    state = tmp_path / "farm"
    monkeypatch.setenv("PATH", FARM + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("FAKE_LSF_STATE", str(state))
    return state


def script(tmp_path, body, **directives):
    """A job script in the shape `dask_jobqueue` emits one."""

    lines = ["#!/usr/bin/env bash", ""]
    lines += [f"#BSUB {flag} {value}" for flag, value in directives.items()]
    lines += ["", body, ""]
    path = tmp_path / "job.sh"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def submit(path, *, stdin):
    """Both submission shapes, because `use-stdin` decides which one is used.

    `dask_jobqueue`'s LSF default is `use-stdin: true`, which reaches the fake
    as `bsub< script`. The argument form is the fallback its own docs point at
    when a site's `bsub` rejects the pipe, so a fake that served only one of
    them would work until someone flipped that setting.
    """

    if stdin:
        with open(path) as handle:
            return subprocess.run(
                ["bsub"], stdin=handle, capture_output=True, text=True
            )
    return subprocess.run(["bsub", str(path)], capture_output=True, text=True)


def job_id(result):
    """Parsed the way `dask_jobqueue` parses it: `(?P<job_id>\\d+)`."""

    assert result.returncode == 0, result.stderr
    match = re.search(r"(?P<job_id>\d+)", result.stdout)
    assert match is not None, f"unparseable submission output {result.stdout!r}"
    return match.group("job_id")


def record(farm, identifier):
    return json.loads((farm / f"{identifier}.json").read_text())


def wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


@pytest.mark.parametrize("stdin", [True, False], ids=["stdin", "argument"])
def test_a_batch_submission_returns_a_parseable_job_id(farm, tmp_path, stdin):
    marker = tmp_path / "ran.txt"
    result = submit(
        script(tmp_path, f"printf ran > {marker}", **{"-q": "normal", "-J": "w"}),
        stdin=stdin,
    )

    identifier = job_id(result)
    assert wait_for(lambda: marker.exists() and marker.read_text() == "ran")
    assert wait_for(lambda: record(farm, identifier)["state"] == "DONE")


def test_the_script_directives_are_what_bjobs_reports(farm, tmp_path):
    """`#BSUB` lines are the only place a pooled job says what it asked for.

    `LSFCluster` puts the queue, core count and memory in the script rather
    than on the command line, so a fake that read only argv would report every
    pooled worker as being on the default queue.
    """

    result = submit(
        script(
            tmp_path,
            "sleep 5",
            **{"-J": "dask-worker", "-q": "bigmem", "-n": "4", "-W": "00:30"},
        ),
        stdin=True,
    )
    identifier = job_id(result)

    assert wait_for(lambda: record(farm, identifier)["state"] == "RUN")
    options = record(farm, identifier)["options"]
    assert options["-q"] == "bigmem"
    assert options["-n"] == "4"

    listed = subprocess.run(
        ["bjobs", identifier, "-noheader"], capture_output=True, text=True
    )
    assert listed.returncode == 0
    assert listed.stdout.split() == [identifier, os.environ.get("USER", "user"),
                                     "RUN", "bigmem", "dask-worker"]

    subprocess.run(["bkill", identifier], capture_output=True, text=True)


def test_two_submissions_get_two_ids_though_they_share_a_name(farm, tmp_path):
    """`LSFCluster` names every worker `dask-worker`; only the id separates them.

    Keying records by name — which is right for an interactive job, whose name
    *is* the attempt identity — would have made the second pooled worker
    overwrite the first, and every later `bkill` reach whichever was written
    last.
    """

    job = script(tmp_path, "sleep 5", **{"-J": "dask-worker"})
    first = job_id(submit(job, stdin=True))
    second = job_id(submit(job, stdin=True))

    assert first != second
    assert record(farm, first)["name"] == record(farm, second)["name"] == "dask-worker"

    listing = subprocess.run(
        ["bjobs", "-noheader", "-o", "job_name stat"], capture_output=True, text=True
    )
    assert listing.stdout.count("dask-worker") == 2

    subprocess.run(["bkill", first, second], capture_output=True, text=True)


def test_a_batch_job_outlives_the_client_that_submitted_it(farm, tmp_path):
    """The whole reason batch mode exists here.

    An interactive job dies with its client and `test_fake_farm.py` proves it.
    A batch job must not: a pooled worker is submitted by a `bsub` that has
    long since exited, and if the fake bound it the same way, every pooled
    worker would vanish the moment the cluster finished scaling. Then
    `death_timeout` and `bkill`-on-close — the two things that actually stop a
    pooled worker — would never be exercised at all.
    """

    marker = tmp_path / "ran.txt"
    body = f"printf started >> {marker}; sleep 1.5; printf finished >> {marker}"
    # Submitted by a *separate* process which is then gone, so nothing this
    # test holds could be keeping the job alive.
    client = subprocess.run(
        [sys.executable, "-c",
         "import subprocess, sys;"
         f"print(subprocess.run(['bsub', {str(script(tmp_path, body))!r}],"
         "capture_output=True, text=True).stdout, end='')"],
        capture_output=True, text=True,
    )
    identifier = job_id(client)

    assert wait_for(lambda: marker.exists() and marker.read_text() == "started")
    # The submitting client has already exited; the job is still running.
    assert client.returncode == 0
    assert record(farm, identifier)["state"] == "RUN"

    assert wait_for(lambda: record(farm, identifier)["state"] == "DONE", timeout=10)
    assert marker.read_text() == "startedfinished", (
        "a batch job must run to completion with no client left to hold it"
    )


def test_bkill_by_id_stops_the_job_and_its_children(farm, tmp_path):
    """How a pooled worker actually stops, and why the signal goes to the group.

    `LSFCluster.close()` cancels by id, and a pooled worker is a `dask-worker`
    with a nanny below it. Signalling only the process the fake launched would
    leave the real worker running and reporting itself healthy to a scheduler
    that had already gone.
    """

    marker = tmp_path / "ran.txt"
    child_marker = tmp_path / "child.txt"
    body = (
        f"( sleep 30; printf leaked > {child_marker} ) &\n"
        f"printf started > {marker}\n"
        "wait\n"
    )
    identifier = job_id(submit(script(tmp_path, body), stdin=True))

    assert wait_for(lambda: marker.exists())
    assert wait_for(lambda: "job_pid" in record(farm, identifier))
    supervisor = record(farm, identifier)["supervisor_pid"]
    job = record(farm, identifier)["job_pid"]

    killed = subprocess.run(["bkill", identifier], capture_output=True, text=True)
    assert killed.returncode == 0
    assert f"Job <{identifier}> is being terminated" in killed.stdout

    assert wait_for(lambda: not alive(supervisor)), "the supervisor outlived bkill"
    assert wait_for(lambda: not alive(job)), "the job outlived bkill"
    assert record(farm, identifier)["state"] == "EXIT"

    # And it is gone from the queue, which is what a cluster asks next.
    assert subprocess.run(
        ["bjobs", identifier, "-noheader"], capture_output=True, text=True
    ).returncode == 255

    # The backgrounded child never reached its write: nothing leaked past the
    # job the cluster believed it had cancelled.
    time.sleep(0.5)
    assert not child_marker.exists()


def test_a_batch_job_whose_supervisor_is_gone_leaves_the_queue(farm, tmp_path):
    """Liveness stays derived, in batch mode too.

    `bjobs` reads the supervisor rather than the recorded state, so a crash
    that leaves `RUN` on disk cannot report a job that is not there — the same
    rule interactive mode applies to the `bsub` client.
    """

    identifier = job_id(submit(script(tmp_path, "sleep 30"), stdin=True))
    assert wait_for(lambda: record(farm, identifier)["state"] == "RUN")

    supervisor = record(farm, identifier)["supervisor_pid"]
    os.killpg(os.getpgid(supervisor), 9)

    assert wait_for(lambda: not alive(supervisor))
    # State on disk still says RUN, because nothing got to write otherwise.
    assert record(farm, identifier)["state"] == "RUN"
    assert subprocess.run(
        ["bjobs", identifier, "-noheader"], capture_output=True, text=True
    ).returncode == 255


def test_a_pending_batch_job_reads_as_pending(farm, tmp_path, monkeypatch):
    """`FAKE_LSF_PEND_SECONDS` reaches batch mode too, so a watcher has a
    transition to see on a pooled job exactly as it does on a direct one."""

    monkeypatch.setenv("FAKE_LSF_PEND_SECONDS", "0.8")
    identifier = job_id(submit(script(tmp_path, "sleep 2"), stdin=True))

    assert record(farm, identifier)["state"] == "PEND"
    listing = subprocess.run(
        ["bjobs", "-noheader", "-o", "job_name stat"], capture_output=True, text=True
    )
    assert listing.stdout.strip().endswith("PEND")
    assert wait_for(lambda: record(farm, identifier)["state"] == "RUN", timeout=10)

    subprocess.run(["bkill", identifier], capture_output=True, text=True)
