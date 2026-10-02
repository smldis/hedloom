"""The cancellation fence must preserve completion and unrelated work."""
from types import SimpleNamespace
import asyncio

import pytest

from hedloom_run.pooled import _prepare_interrupt, _interrupt_ack
from hedloom_run import pooled


def scheduler(state="processing", *, nanny="nanny", unrelated=False):
    owner = object()
    worker = SimpleNamespace(address="worker", nanny=nanny,
                             status=SimpleNamespace(name="running"), processing=[])
    task = SimpleNamespace(key="command", state=state, who_wants={owner},
                           dependents=set(), processing_on=worker if state == "processing" else None)
    worker.processing = [task]
    if unrelated:
        worker.processing.append(SimpleNamespace(key="other"))
    events = []
    fixture = SimpleNamespace(tasks={task.key: task}, clients={"owner": owner},
                              workers={"worker": worker}, events=events)

    def pause(**kwargs):
        events.append("pause" if kwargs["status"] == "paused" else "resume")
        worker.status.name = kwargs["status"]

    def release(**kwargs):
        events.append("release")
        assert task.processing_on is None or worker.status.name == "paused"
        fixture.tasks.pop(task.key)
        worker.processing = []

    fixture.handle_worker_status_change = pause
    fixture.client_releases_keys = release
    return fixture


def test_running_cancellation_fences_worker_before_releasing_interest():
    fixture = scheduler()
    snapshot = {"worker": "worker", "entered": True,
                "executing": ["command"], "supported": True}
    decision = _prepare_interrupt("command", "owner", snapshot, dask_scheduler=fixture)
    assert decision == {"state": "withdrawn", "worker": "worker"}
    assert fixture.events == ["pause", "release"]
    assert _interrupt_ack("command", "worker", fixture)


@pytest.mark.parametrize("entered, executing", [
    (True, ["command"]), (False, ["other"]),
])
def test_worker_snapshot_distinguishes_unrelated_queued_assignments_from_actual_execution(entered, executing):
    fixture = scheduler(unrelated=True)
    snapshot = {"worker": "worker", "entered": entered,
                "executing": executing, "supported": True}
    decision = _prepare_interrupt("command", "owner", worker_snapshot=snapshot,
                                  dask_scheduler=fixture)
    assert decision == {"state": "withdrawn", "worker": "worker"}
    assert fixture.events == ["pause", "release"]


def test_actual_unrelated_execution_refuses_restart_even_with_worker_snapshot():
    fixture = scheduler(unrelated=True)
    snapshot = {"worker": "worker", "entered": True,
                "executing": ["command", "other"], "supported": True}
    with pytest.raises(RuntimeError, match="unrelated executing"):
        _prepare_interrupt("command", "owner", worker_snapshot=snapshot,
                           dask_scheduler=fixture)
    assert fixture.events == []


def test_queued_cancellation_is_atomic_without_worker_restart():
    fixture = scheduler("no-worker")
    assert _prepare_interrupt("command", "owner", dask_scheduler=fixture) == {
        "state": "withdrawn", "worker": None}
    assert fixture.events == ["release"]
    assert _interrupt_ack("command", None, fixture)


@pytest.mark.parametrize("options, supported", [
    ({"nanny": None}, ["worker"]),
    ({"unrelated": True}, ["worker"]),
    ({}, []),
])
def test_unsupported_restart_refuses_without_cancelling_or_pausing(options, supported):
    fixture = scheduler(**options)
    snapshot = {"worker": "worker", "entered": True,
                "executing": [task.key for task in fixture.workers["worker"].processing],
                "supported": "worker" in supported}
    with pytest.raises(RuntimeError):
        _prepare_interrupt("command", "owner", snapshot, dask_scheduler=fixture)
    assert fixture.events == []
    assert "command" in fixture.tasks


@pytest.mark.parametrize("state", ["memory", "erred"])
def test_completed_work_preserves_actual_result(state):
    fixture = scheduler(state)
    assert _prepare_interrupt("command", "owner", dask_scheduler=fixture) == {"state": "completed"}
    assert fixture.events == []


def test_shared_dask_interest_refuses_before_changing_scheduler():
    fixture = scheduler()
    fixture.tasks["command"].who_wants.add(object())
    with pytest.raises(RuntimeError, match="unrelated scheduling consumers"):
        _prepare_interrupt("command", "owner", dask_scheduler=fixture)
    assert fixture.events == []


def test_absence_before_submit_stream_registration_is_not_cancellation():
    fixture = scheduler()
    fixture.tasks.clear()
    assert _prepare_interrupt("command", "owner", dask_scheduler=fixture) == {"state": "unregistered"}
    assert fixture.events == []


def test_assignment_during_location_rpc_requires_worker_checkpoint_before_cancellation():
    fixture = scheduler()
    assert _prepare_interrupt("command", "owner", dask_scheduler=fixture) == {"state": "moved"}
    assert fixture.events == []
    assert "command" in fixture.tasks


@pytest.mark.parametrize("status", ["paused", "closing"])
def test_another_owned_force_pause_waits_without_overwriting_its_owner(status):
    from distributed.core import Status
    worker = SimpleNamespace(status=Status[status], _hedloom_interrupt_key="other",
                             state=SimpleNamespace(tasks={"command": SimpleNamespace(state="constrained")}))
    assert pooled._pause_command("command", worker) == {"state": "busy"}
    assert worker.status == Status[status]
    assert worker._hedloom_interrupt_key == "other"


def test_external_pause_still_refuses_instead_of_claiming_ownership():
    from distributed.core import Status
    worker = SimpleNamespace(status=Status.paused,
                             state=SimpleNamespace(tasks={"command": SimpleNamespace(state="constrained")}))
    with pytest.raises(RuntimeError, match="already unavailable"):
        pooled._pause_command("command", worker)
    assert not hasattr(worker, "_hedloom_interrupt_key")


def test_wait_for_another_owned_force_pause_is_bounded_and_preserves_interest(monkeypatch):
    monkeypatch.setattr(pooled, "_INTERRUPT_TIMEOUT", .03)
    fixture = scheduler()

    class Client:
        id = "owner"

        async def run(self, function, **kwargs):
            if function is pooled._pause_command:
                return {"worker": {"state": "busy"}}
            # Cleanup must not unpause a fence owned by another key.
            return {"worker": False}

        async def run_on_scheduler(self, function, **kwargs):
            return function(dask_scheduler=fixture, **kwargs)

    future = SimpleNamespace(key="command", done=lambda: False)
    with pytest.raises(TimeoutError):
        asyncio.run(pooled._interrupt_command(Client(), future))
    assert fixture.events == []
    assert "command" in fixture.tasks


def test_busy_worker_restart_can_leave_waiting_command_unassigned_before_cancellation():
    fixture = scheduler()

    class Client:
        id = "owner"
        checkpoints = 0

        async def run(self, function, **kwargs):
            if function is pooled._pause_command:
                self.checkpoints += 1
                fixture.tasks["command"].processing_on = None
                return {"worker": {"state": "busy"}}
            return {"worker": False}

        async def run_on_scheduler(self, function, **kwargs):
            return function(dask_scheduler=fixture, **kwargs)

        async def cancel(self, *args, **kwargs):
            pass

    client = Client()
    future = SimpleNamespace(key="command", done=lambda: False)
    result = asyncio.run(pooled._interrupt_command(client, future))
    assert result["worker"] is None and not result["worker_restarted"]
    assert client.checkpoints == 1
    assert fixture.events == ["release"]


@pytest.mark.parametrize("failure, expected_status", [
    ("cancel", "running"), ("restart", "paused"),
])
def test_interruption_failure_never_claims_cancelled_and_restores_only_before_restart(failure, expected_status):
    fixture = scheduler()

    class Client:
        id = "owner"

        async def run(self, function, **kwargs):
            if function is pooled._pause_command:
                return {"worker": {"state": "paused", "worker": "worker", "entered": True,
                                   "executing": ["command"], "supported": True}}
            return {"worker": True}

        async def run_on_scheduler(self, function, **kwargs):
            return function(dask_scheduler=fixture, **kwargs)

        async def cancel(self, future, **kwargs):
            if failure == "cancel":
                raise OSError("cancel acknowledgement lost")

        async def restart_workers(self, *args, **kwargs):
            raise OSError("restart acknowledgement lost")

    future = SimpleNamespace(key="command", done=lambda: False)
    with pytest.raises(OSError, match="acknowledgement lost"):
        asyncio.run(pooled._interrupt_command(Client(), future))
    assert fixture.workers["worker"].status.name == expected_status


def test_registration_wait_is_bounded(monkeypatch):
    monkeypatch.setattr(pooled, "_INTERRUPT_TIMEOUT", .03)
    fixture = scheduler()
    fixture.tasks.clear()

    class Client:
        id = "owner"

        async def run(self, function, **kwargs):
            if function is pooled._pause_command:
                return {"worker": {"state": "paused", "worker": "worker", "entered": True,
                                   "executing": ["command"], "supported": True}}
            return {"worker": True}

        async def run_on_scheduler(self, function, **kwargs):
            return function(dask_scheduler=fixture, **kwargs)

    future = SimpleNamespace(key="command", done=lambda: False)
    with pytest.raises(TimeoutError):
        asyncio.run(pooled._interrupt_command(Client(), future))
    assert fixture.events == []
