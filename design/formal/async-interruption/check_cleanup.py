"""Replay CleanupOverlap's trace against actual helpers; no cluster or jobs.

From ASS root: .venv/bin/python hedloom/design/formal/async-interruption/check_cleanup.py
Historical mode replays pinned 683a88f source from Git, verified by SHA-256.
Fixed mode imports the actual current API; both modes run by default.
Scheduler/worker objects are minimal fixtures, not a network integration test.
"""
import argparse
import hashlib
from pathlib import Path
import subprocess
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from distributed.core import Status


def snapshot_helpers():
    # The parent began fixing these findings during the check. Keep this dated
    # reproduction stable without changing/importing its newer pooled module.
    hedloom_root = Path(__file__).resolve().parents[3]
    source = subprocess.check_output([
        "git", "-C", str(hedloom_root), "show",
        "683a88f:run/src/hedloom_run/pooled.py",
    ])
    expected = "13749aabf6739be56755d34bb8d2bfaaec022d08700da7c42120f3ee42af5f97"
    assert hashlib.sha256(source).hexdigest() == expected
    module = ModuleType("hedloom_run._formal_683a88f_pooled")
    module.__package__ = "hedloom_run"
    sys.modules[module.__name__] = module
    exec(compile(source, "683a88f:run/src/hedloom_run/pooled.py", "exec"), module.__dict__)
    return module


def historical():
    helpers = snapshot_helpers()
    class ActiveA:
        key = "A"

    owner = object()
    worker = SimpleNamespace(
        address="worker",
        status=Status.paused,
        _hedloom_interrupt_key="A",
        _hedloom_interrupt_memory_pause_fraction=.8,
        memory_manager=SimpleNamespace(memory_pause_fraction=False),
        state=SimpleNamespace(
            tasks={"B": SimpleNamespace(state="constrained")},
            executing={ActiveA()}, long_running=set(),
        ),
    )
    scheduler_worker = SimpleNamespace(
        address="worker", status=Status.paused, nanny="nanny",
    )
    scheduler = SimpleNamespace(
        workers={"worker": scheduler_worker}, clients={"owner": owner},
        tasks={"B": SimpleNamespace(
            state="processing", who_wants={owner}, dependents=set(),
            processing_on=scheduler_worker,
        )},
    )

    def status_change(*, status, worker, stimulus_id):
        scheduler.workers[worker].status = Status[status]

    def release(*, keys, client, stimulus_id):
        for key in keys:
            scheduler.tasks.pop(key)

    scheduler.handle_worker_status_change = status_change
    scheduler.client_releases_keys = release

    # A's worker cleanup replied True; its scheduler cleanup is delayed.
    assert helpers._resume_command_worker("A", worker) is True
    snapshot = helpers._pause_command("B", worker)
    assert snapshot["state"] == "paused" and not snapshot["entered"]
    assert helpers._prepare_interrupt("B", "owner", snapshot, scheduler)["state"] == "withdrawn"
    helpers._resume_interrupted_worker("worker", scheduler)  # A's delayed second RPC
    assert worker.status == Status.paused
    assert worker._hedloom_interrupt_key == "B"
    assert scheduler_worker.status == Status.running
    try:
        helpers._interrupt_ack("B", "worker", scheduler)
    except RuntimeError as error:
        assert str(error) == "pooled cancellation lost its exclusive paused worker"
        print("REPRODUCED at 683a88f: A's delayed scheduler cleanup makes B's acknowledgement fail; B's worker fence remains owned and paused.")
    else:
        raise AssertionError("Expected the recorded acknowledgement failure")


def fixed():
    from distributed import Scheduler
    from hedloom_run import pooled as helpers
    from hedloom_run import _pool_scheduler as owned

    class ActiveA:
        key = "A"

    owner = object()
    worker = SimpleNamespace(
        address="worker", status=Status.paused,
        _hedloom_interrupt_key="A",
        _hedloom_interrupt_memory_pause_fraction=.8,
        memory_manager=SimpleNamespace(memory_pause_fraction=False),
        state=SimpleNamespace(tasks={"B": SimpleNamespace(state="constrained")},
                              executing={ActiveA()}, long_running=set()),
    )
    scheduler_worker = SimpleNamespace(address="worker", status=Status.paused,
                                       nanny="nanny", extra={owned.FENCE: "A"})

    class Fixture(owned.OwnedPoolScheduler):
        # No Scheduler initialization: no sockets, processes or event loop.
        def __init__(self):
            self.workers = {"worker": scheduler_worker}
            self.clients = {"owner": owner}
            self.tasks = {"B": SimpleNamespace(
                state="processing", who_wants={owner}, dependents=set(),
                processing_on=scheduler_worker, metadata={},
            )}

        def client_releases_keys(self, *, keys, client, stimulus_id):
            for key in keys:
                self.tasks.pop(key)

    scheduler = Fixture()
    delivered = []

    def base_status(self, status, worker, stimulus_id):
        target = self.workers[worker] if isinstance(worker, str) else worker
        target.status = Status[status] if isinstance(status, str) else status
        delivered.append((status, stimulus_id))

    # Execute the actual override; stub only Dask's wider state-machine base.
    with patch.object(Scheduler, "handle_worker_status_change", base_status):
        assert helpers._resume_interrupted_worker("worker", "A", scheduler)
        assert helpers._pause_command("B", worker)["state"] == "busy"
        assert helpers._resume_command_worker("A", worker)
        snapshot = helpers._pause_command("B", worker)
        assert snapshot["state"] == "paused" and not snapshot["entered"]
        assert helpers._prepare_interrupt("B", "owner", snapshot, scheduler)["state"] == "withdrawn"

        assert not helpers._resume_interrupted_worker("worker", "A", scheduler)
        assert not helpers._resume_interrupted_worker("missing-worker", "B", scheduler)
        assert not helpers._resume_command_worker("A", worker)
        before = len(delivered)
        for status in ("running", Status.running):
            for target in ("worker", scheduler_worker):
                scheduler.handle_worker_status_change(status, target, "stale-message")
        assert len(delivered) == before
        assert scheduler_worker.status == Status.paused
        assert scheduler_worker.extra[owned.FENCE] == "B"
        assert worker.status == Status.paused and worker._hedloom_interrupt_key == "B"
        assert worker.memory_manager.memory_pause_fraction is False
        assert helpers._interrupt_ack("B", "worker", scheduler)

        assert helpers._resume_interrupted_worker("worker", "B", scheduler)
        assert scheduler_worker.status == Status.running and owned.FENCE not in scheduler_worker.extra
        assert worker.status == Status.paused  # Worker release is still separate.
        assert helpers._resume_command_worker("B", worker)
        assert worker.memory_manager.memory_pause_fraction == .8
        before = len(delivered)
        scheduler.handle_worker_status_change("running", "worker", "unfenced-message")
        assert len(delivered) == before + 1

    print("PASS fixed working-tree API: owner checks, stale running-message suppression, scheduler-first cleanup, and B acknowledgement.")
    for module in (helpers, owned):
        path = Path(module.__file__)
        print(f"{path.name} SHA-256 {hashlib.sha256(path.read_bytes()).hexdigest()}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("historical", "fixed", "both"), default="both")
    args = parser.parse_args()
    if args.mode in {"historical", "both"}:
        historical()
    if args.mode in {"fixed", "both"}:
        fixed()


if __name__ == "__main__":
    main()
