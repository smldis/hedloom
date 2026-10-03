"""The cancellation fence must preserve completion and unrelated work."""
from types import SimpleNamespace
import asyncio

import pytest

from hedloom_run.pooled import _prepare_interrupt, _interrupt_ack
from hedloom_run import pooled


def scheduler(state="processing", *, nanny="nanny", unrelated=False):
    owner = object()
    worker = SimpleNamespace(address="worker", nanny=nanny,
                             status=SimpleNamespace(name="running"), processing=[], extra={})
    task = SimpleNamespace(key="command", state=state, who_wants={owner},
                           dependents=set(), metadata=None,
                           processing_on=worker if state == "processing" else None)
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


@pytest.mark.parametrize("status", ["running", "paused", "closing"])
def test_another_owned_force_pause_waits_without_overwriting_its_owner(status):
    from distributed.core import Status
    worker = SimpleNamespace(status=Status[status], _hedloom_interrupt_key="other",
                             state=SimpleNamespace(tasks={"command": SimpleNamespace(state="constrained")}))
    assert pooled._pause_command("command", worker) == {"state": "busy"}
    assert worker.status == Status[status]
    assert worker._hedloom_interrupt_key == "other"


@pytest.mark.parametrize("original_fraction", [False, .8])
def test_owned_pause_prevents_actual_memory_monitor_resume_and_restores_policy(original_fraction):
    from distributed.core import Status
    from distributed.worker_memory import WorkerMemoryManager
    manager = SimpleNamespace(memory_pause_fraction=original_fraction, memory_limit=1000)
    worker = SimpleNamespace(status=Status.running, address="worker", memory_manager=manager,
        state=SimpleNamespace(tasks={"command": SimpleNamespace(state="ready")},
                              executing=set(), long_running=set()))
    decision = pooled._pause_command("command", worker)
    assert decision["state"] == "paused"
    assert manager.memory_pause_fraction is False
    WorkerMemoryManager._maybe_pause_or_unpause(manager, worker, 0)
    assert worker.status == Status.paused
    assert not pooled._resume_command_worker("other", worker)
    assert worker.status == Status.paused and manager.memory_pause_fraction is False
    assert pooled._resume_command_worker("command", worker)
    assert worker.status == Status.running
    assert manager.memory_pause_fraction == original_fraction
    assert not hasattr(worker, "_hedloom_interrupt_key")
    assert not hasattr(worker, "_hedloom_interrupt_memory_pause_fraction")
    # The original automatic memory resume policy applies again afterwards.
    worker.status = Status.paused
    WorkerMemoryManager._maybe_pause_or_unpause(manager, worker, 0)
    assert worker.status == (Status.paused if original_fraction is False else Status.running)


def test_failed_owned_pause_restores_memory_policy_and_releases_marker():
    from distributed.core import Status
    class Worker:
        address = "worker"
        memory_manager = SimpleNamespace(memory_pause_fraction=.8)
        state = SimpleNamespace(tasks={"command": SimpleNamespace(state="ready")})

        @property
        def status(self):
            return Status.running

        @status.setter
        def status(self, value):
            raise RuntimeError("pause delivery failed")

    worker = Worker()
    with pytest.raises(RuntimeError, match="pause delivery failed"):
        pooled._pause_command("command", worker)
    assert worker.memory_manager.memory_pause_fraction == .8
    assert not hasattr(worker, "_hedloom_interrupt_key")
    assert not hasattr(worker, "_hedloom_interrupt_memory_pause_fraction")


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
                from hedloom_run._pool_scheduler import assignment_loss, RESTART
                task = fixture.tasks["command"]
                old = fixture.workers.pop("worker")
                old.extra[RESTART] = "acknowledged"
                evidence = {"worker_identity": id(old), "tasks": [(task.key, id(task))]}
                assignment_loss(task, old)
                task.processing_on = None
                pooled._certify_owned_restart("worker", "acknowledged", evidence, fixture)
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


@pytest.mark.parametrize("change", ["removed", "unknown", "live", "shared", "missing"])
def test_checkpoint_connection_loss_retries_only_after_authoritative_worker_removal(change, monkeypatch):
    from distributed.comm.core import CommClosedError
    if change == "missing":
        monkeypatch.setattr(pooled, "_INTERRUPT_TIMEOUT", .03)
    fixture = scheduler()
    checks = []
    cancellations = []

    class Client:
        id = "owner"

        async def run(self, function, **kwargs):
            if function is pooled._pause_command:
                if change != "live":
                    old = fixture.workers.pop("worker")
                    from hedloom_run._pool_scheduler import assignment_loss, RESTART
                    # Removal alone is insufficient. Only an acknowledged
                    # owned restart permits the retry to withdraw safely.
                    evidence = {"worker_identity": id(old),
                                "tasks": [("command", id(fixture.tasks["command"]))]}
                    if change == "removed":
                        old.extra[RESTART] = "acknowledged"
                    assignment_loss(fixture.tasks["command"], old)
                    if change == "removed":
                        pooled._certify_owned_restart("worker", "acknowledged", evidence, fixture)
                    fixture.tasks["command"].processing_on = None
                if change == "shared":
                    fixture.tasks["command"].who_wants.add(object())
                if change == "missing":
                    fixture.tasks.pop("command")
                raise CommClosedError("Address removed or reply lost")
            return {"worker": False}

        async def run_on_scheduler(self, function, **kwargs):
            checks.append((function, dict(kwargs)))
            return function(dask_scheduler=fixture, **kwargs)

        async def cancel(self, *args, **kwargs):
            cancellations.append("cancel")

    future = SimpleNamespace(key="command", done=lambda: False)
    if change == "removed":
        result = asyncio.run(pooled._interrupt_command(Client(), future))
        assert result["worker"] is None and not result["worker_restarted"]
        assert fixture.events == ["release"] and cancellations == ["cancel"]
    else:
        expected = {"unknown": RuntimeError, "live": CommClosedError,
                    "shared": RuntimeError, "missing": TimeoutError}[change]
        with pytest.raises(expected):
            asyncio.run(pooled._interrupt_command(Client(), future))
        assert fixture.events == [] and cancellations == []
        if change != "missing":
            assert "command" in fixture.tasks
    assert any(function is pooled._locate_interrupt and options.get("previous_worker") == "worker"
               for function, options in checks)


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


@pytest.mark.parametrize("checkpoint", ["locate", "prepare"])
def test_unknown_assignment_loss_refuses_before_withdrawing_interest(checkpoint):
    from hedloom_run._pool_scheduler import assignment_loss
    fixture = scheduler()
    task = fixture.tasks["command"]
    assignment_loss(task, fixture.workers["worker"])
    task.processing_on = None
    function = pooled._locate_interrupt if checkpoint == "locate" else _prepare_interrupt
    with pytest.raises(RuntimeError, match="assignment loss is unconfirmed"):
        function("command", "owner", dask_scheduler=fixture)
    assert fixture.events == []
    assert "command" in fixture.tasks


def test_loss_between_location_and_prepare_cannot_be_mistaken_for_unentered_work():
    from hedloom_run._pool_scheduler import assignment_loss
    fixture = scheduler()
    assert pooled._locate_interrupt("command", "owner", dask_scheduler=fixture)["worker"] == "worker"
    task = fixture.tasks["command"]
    assignment_loss(task, fixture.workers.pop("worker"))
    task.processing_on = None
    with pytest.raises(RuntimeError, match="assignment loss is unconfirmed"):
        _prepare_interrupt("command", "owner", dask_scheduler=fixture)
    assert fixture.events == []


def test_owned_restart_waits_for_exact_confirmation_and_second_loss_stays_unknown():
    from hedloom_run._pool_scheduler import assignment_loss, loss_record, FENCE
    fixture = scheduler()
    task = fixture.tasks["command"]
    worker = fixture.workers["worker"]
    worker.status.name = "paused"
    worker.extra[FENCE] = "other"
    evidence = pooled._begin_owned_restart("other", "worker", "restart", fixture)
    assignment_loss(task, worker)
    task.processing_on = None
    assert pooled._locate_interrupt("command", "owner", dask_scheduler=fixture)["state"] == "restart-pending"
    assert _prepare_interrupt("command", "owner", dask_scheduler=fixture) == {"state": "restart-pending"}
    # A second old copy remains uncertain even after the first is certified.
    second_worker = SimpleNamespace(address="new-worker", extra={})
    assignment_loss(task, second_worker)
    pooled._certify_owned_restart("worker", "restart", evidence, fixture)
    assert loss_record(task) == {"unknown": True, "pending": None}
    with pytest.raises(RuntimeError, match="assignment loss is unconfirmed"):
        pooled._locate_interrupt("command", "owner", dask_scheduler=fixture)
    assert fixture.events == []


@pytest.mark.parametrize("mismatch", ["token", "worker", "worker_identity", "task_identity"])
def test_restart_acknowledgement_certifies_only_exact_task_and_worker(mismatch):
    from hedloom_run._pool_scheduler import assignment_loss, loss_record, FENCE
    fixture = scheduler()
    task = fixture.tasks["command"]
    worker = fixture.workers["worker"]
    worker.status.name = "paused"
    worker.extra[FENCE] = "other"
    evidence = pooled._begin_owned_restart("other", "worker", "restart", fixture)
    assignment_loss(task, worker)
    task.processing_on = None
    token, address = "restart", "worker"
    if mismatch == "token":
        token = "different"
    elif mismatch == "worker":
        address = "different"
    elif mismatch == "worker_identity":
        evidence["worker_identity"] += 1
    else:
        evidence["tasks"] = [(task.key, id(task) + 1)]
    pooled._certify_owned_restart(address, token, evidence, fixture)
    assert loss_record(task)["pending"] is not None
    assert _prepare_interrupt("command", "owner", dask_scheduler=fixture) == {"state": "restart-pending"}
    assert fixture.events == []


def test_acknowledged_owned_loss_permits_unassigned_queued_cancellation():
    from hedloom_run._pool_scheduler import assignment_loss, FENCE
    fixture = scheduler()
    task = fixture.tasks["command"]
    worker = fixture.workers["worker"]
    worker.status.name = "paused"
    worker.extra[FENCE] = "other"
    evidence = pooled._begin_owned_restart("other", "worker", "restart", fixture)
    assignment_loss(task, worker)
    task.processing_on = None
    pooled._certify_owned_restart("worker", "restart", evidence, fixture)
    assert _prepare_interrupt("command", "owner", dask_scheduler=fixture) == {"state": "withdrawn", "worker": None}
    assert fixture.events == ["release"]


def test_unconfirmed_restart_is_bounded_without_withdrawal(monkeypatch):
    from hedloom_run._pool_scheduler import assignment_loss, RESTART
    monkeypatch.setattr(pooled, "_INTERRUPT_TIMEOUT", .03)
    fixture = scheduler()
    task = fixture.tasks["command"]
    worker = fixture.workers["worker"]
    worker.extra[RESTART] = "unacknowledged"
    assignment_loss(task, worker)
    task.processing_on = None

    class Client:
        id = "owner"

        async def run_on_scheduler(self, function, **kwargs):
            return function(dask_scheduler=fixture, **kwargs)

    with pytest.raises(TimeoutError):
        asyncio.run(pooled._interrupt_command(Client(), SimpleNamespace(key="command", done=lambda: False)))
    assert fixture.events == []
    assert "command" in fixture.tasks


@pytest.mark.parametrize("acknowledgement", ["timed out", "lost"])
def test_failed_owned_restart_never_certifies_other_pending_commands(acknowledgement):
    from hedloom_run._pool_scheduler import assignment_loss, loss_record
    fixture = scheduler()
    worker = fixture.workers["worker"]
    queued = SimpleNamespace(key="queued", metadata=None, processing_on=worker)
    fixture.tasks[queued.key] = queued
    original_release = fixture.client_releases_keys

    def release(**kwargs):
        original_release(**kwargs)
        worker.processing = [queued]

    fixture.client_releases_keys = release

    class Client:
        id = "owner"

        async def run(self, function, **kwargs):
            assert function is pooled._pause_command
            return {"worker": {"state": "paused", "worker": "worker", "entered": True,
                               "executing": ["command"], "supported": True}}

        async def run_on_scheduler(self, function, **kwargs):
            return function(dask_scheduler=fixture, **kwargs)

        async def cancel(self, *args, **kwargs):
            pass

        async def restart_workers(self, *args, **kwargs):
            assignment_loss(queued, worker)
            if acknowledgement == "lost":
                raise OSError("restart acknowledgement lost")
            return {"worker": "timed out"}

    expected = OSError if acknowledgement == "lost" else RuntimeError
    with pytest.raises(expected):
        asyncio.run(pooled._interrupt_command(Client(), SimpleNamespace(key="command", done=lambda: False)))
    assert loss_record(queued)["pending"] is not None
    assert worker.status.name == "paused"


def test_late_scheduler_cleanup_preserves_newer_owner_pause():
    from hedloom_run._pool_scheduler import FENCE
    fixture = scheduler()
    worker = fixture.workers["worker"]
    worker.status.name = "paused"
    worker.extra[FENCE] = "new-owner"
    fixture.tasks.clear()
    assert not pooled._resume_interrupted_worker("worker", "old-owner", fixture)
    assert worker.status.name == "paused" and worker.extra[FENCE] == "new-owner"
    assert pooled._interrupt_ack("new-owner", "worker", fixture)
    assert pooled._resume_interrupted_worker("worker", "new-owner", fixture)
    assert fixture.events == ["resume"]


@pytest.mark.parametrize("status", ["running", "enum"])
def test_batched_old_running_message_cannot_release_current_scheduler_fence(monkeypatch, status):
    from distributed import Scheduler
    from distributed.core import Status
    from hedloom_run._pool_scheduler import OwnedPoolScheduler, FENCE

    class Fixture(OwnedPoolScheduler):
        def __init__(self):
            self.workers = scheduler().workers

    fixture = Fixture()
    worker = fixture.workers["worker"]
    worker.status.name = "paused"
    worker.extra[FENCE] = "new-owner"
    delivered = []
    monkeypatch.setattr(Scheduler, "handle_worker_status_change",
                        lambda self, *args: delivered.append(args))
    message = Status.running if status == "enum" else "running"
    OwnedPoolScheduler.handle_worker_status_change(fixture, message, "worker", "old-message")
    assert delivered == [] and worker.extra[FENCE] == "new-owner"
    del worker.extra[FENCE]
    OwnedPoolScheduler.handle_worker_status_change(fixture, message, "worker", "current-message")
    assert len(delivered) == 1


@pytest.mark.parametrize("expected", [False, True])
def test_owned_scheduler_records_all_removals_before_base_clears_assignment(monkeypatch, expected):
    import inspect
    from distributed import Scheduler
    from hedloom_run._pool_scheduler import OwnedPoolScheduler, loss_record

    class Fixture(OwnedPoolScheduler):
        def __init__(self):
            fixture = scheduler()
            self.workers = fixture.workers
            self.tasks = fixture.tasks

        def coerce_address(self, address):
            return address

    fixture = Fixture()
    task = fixture.tasks["command"]
    # 2023 calls this flag safe; both forms still require lost-copy evidence.
    flag = "expected" if "expected" in inspect.signature(Scheduler.remove_worker).parameters else "safe"

    async def base_remove(self, address, **kwargs):
        assert kwargs[flag] is expected
        assert loss_record(task)["unknown"]
        task.processing_on = None
        self.workers.pop(address)
        return "OK"

    monkeypatch.setattr(Scheduler, "remove_worker", base_remove)
    assert asyncio.run(fixture.remove_worker("worker", **{flag: expected}, close=False,
                                            stimulus_id="test-removal")) == "OK"
    assert task.processing_on is None and loss_record(task)["unknown"]


@pytest.mark.parametrize("hold", ["force", "restart"])
def test_owned_admission_holds_no_worker_assignment_without_changing_resources(monkeypatch, hold):
    from distributed import Scheduler
    from hedloom_run._pool_scheduler import OwnedPoolScheduler, ADMISSION, LOSS

    class Fixture(OwnedPoolScheduler):
        def __init__(self):
            pass

    task = SimpleNamespace(metadata={}, resource_restrictions={pooled.COMMAND_RESOURCE: 1})
    marker = ADMISSION if hold == "force" else LOSS
    task.metadata[marker] = {"token": "force"} if hold == "force" else {"pending": {"token": "restart"}}
    fallback = object()
    monkeypatch.setattr(Scheduler, "valid_workers", lambda self, task: fallback)
    fixture = Fixture()
    assert fixture.valid_workers(task) == set()
    assert task.resource_restrictions == {pooled.COMMAND_RESOURCE: 1}
    del task.metadata[marker]
    assert fixture.valid_workers(task) is fallback


def test_force_admission_owner_is_exact_and_failure_cleanup_reconsiders_queued_task():
    from hedloom_run._pool_scheduler import ADMISSION
    fixture = scheduler("no-worker")
    task = fixture.tasks["command"]
    transitions = []
    fixture.transitions = lambda recommendations, **kwargs: transitions.append(recommendations)
    assert pooled._locate_interrupt("command", "owner", request_token="first", dask_scheduler=fixture)["state"] == "located"
    first = dict(task.metadata[ADMISSION])
    assert pooled._locate_interrupt("command", "owner", request_token="second", dask_scheduler=fixture)["state"] == "admission-busy"
    assert task.metadata[ADMISSION] == first
    pooled._release_interrupt_admission("command", "second", fixture)
    assert task.metadata[ADMISSION] == first and transitions == []
    pooled._release_interrupt_admission("command", "first", fixture)
    assert ADMISSION not in task.metadata and transitions == [{"command": "processing"}]


def test_stale_cleanup_cannot_clear_admission_on_replaced_task_state():
    from hedloom_run._pool_scheduler import ADMISSION
    fixture = scheduler("no-worker")
    old = fixture.tasks["command"]
    pooled._locate_interrupt("command", "owner", request_token="first", dask_scheduler=fixture)
    replacement = SimpleNamespace(key="command", state="no-worker", metadata=dict(old.metadata))
    fixture.tasks["command"] = replacement
    pooled._release_interrupt_admission("command", "first", fixture)
    assert ADMISSION in replacement.metadata


def test_restart_confirmation_reconsiders_unrelated_work_but_force_task_remains_held():
    from hedloom_run._pool_scheduler import assignment_loss, FENCE, ADMISSION, LOSS
    fixture = scheduler()
    task = fixture.tasks["command"]
    worker = fixture.workers["worker"]
    worker.status.name = "paused"
    worker.extra[FENCE] = "other"
    pooled._locate_interrupt("command", "owner", request_token="force", dask_scheduler=fixture)
    evidence = pooled._begin_owned_restart("other", "worker", "restart", fixture)
    assignment_loss(task, worker)
    task.processing_on = None
    task.state = "no-worker"
    transitions = []
    fixture.transitions = lambda recommendations, **kwargs: transitions.append(recommendations)
    pooled._certify_owned_restart("worker", "restart", evidence, fixture)
    assert task.metadata[LOSS]["pending"] is None
    assert task.metadata[ADMISSION] == {"token": "force", "identity": id(task)}
    assert transitions == [{"command": "processing"}]
