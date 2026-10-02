"""Loop-owned static readiness, sharing, and consumer withdrawal.

Only ready, durably bound executions reach Dask. Placement capacity bounds the
outstanding executor window; submitted Runs may wait without reserving threads.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any
from uuid import uuid4

from hedloom_exec.planned import finalize_invocation
from hedloom_run.binding import UnsupportedPlacement, build_bundle, produced_by, select_transport
from hedloom_run.execution import ExecutionHandle
from hedloom_run.graph import (
    _Step, _abnormal, _admission, _blocked, _execution_contract, _outcome,
    _placement_of, _report, _run_handle, _task_key,
)


@dataclass(eq=False)
class _Entry:
    key: bytes
    handle_task: Any
    item: Any
    available: Any
    config: Any
    consumers: dict = field(default_factory=dict)
    bindings: set = field(default_factory=set)
    future: Any = None
    task: Any = None
    abandoned: bool = False
    closing: bool = False
    settled: Any = field(default_factory=asyncio.Event)
    step: Any = None

    @property
    def priority(self):
        return max((run.priority for run, _ in self.consumers.values()), default=0)


class ControlRun:
    """One loop-owned submission; cancellation of a waiter leaves work owned."""

    def __init__(self, controller, items, available, config, bind, on_event,
                 stop_on_failure, priority):
        self.controller = controller
        self.items = tuple(items)
        self.available = available
        self.config = config
        self.bind = bind
        self.on_event = on_event
        self.stop_on_failure = stop_on_failure
        self.priority = priority
        self.token = uuid4().hex
        self.remaining = list(items)
        self.completed = {}
        self.delivered = set()
        self.active = {}
        self.produced = dict(config.sources)
        self.pending = set()
        self.stopped = False
        self.force_requested = False
        self.error = None
        self.draining = set()
        self._stop_signal = asyncio.get_running_loop().create_future()
        self._result = asyncio.get_running_loop().create_future()

    @property
    def done(self):
        return self._result.done()

    def stop(self, *, force=False):
        """Withdraw; force may interrupt a solely owned pooled command."""
        if force and not self.force_requested:
            self.force_requested = True
            # An ordinary withdrawal may already have inspected an entered
            # gate. Revisit those executions when the operator escalates.
            self.draining.clear()
        self.stopped = True
        if not self._stop_signal.done():
            self._stop_signal.set_result(None)
        self.controller._wake.set()

    async def wait(self):
        report = await asyncio.shield(self._result)
        if self.error is not None:
            self.error.report = report
            raise self.error
        return report


class Controller:
    """One owner on one asyncio loop; no per-Run coordination threads."""

    def __init__(self, client, capacities, offload, *, executions_dir=None):
        if any(type(capacity) is not int or capacity < 1 for capacity in capacities.values()):
            raise ValueError("placement capacities must be positive integers")
        self.client = client
        self.capacities = dict(capacities)
        self.offload = offload
        self.executions_dir = executions_dir
        self.runs = []
        self.entries = {}
        self.outstanding = dict.fromkeys(capacities, 0)
        self._wake = asyncio.Event()
        self._closed = False
        self._last_run = None
        self._preparing = 0
        self._preparation_slots = asyncio.Semaphore(2)
        self._executions = set()
        self._driver = asyncio.create_task(self._drive())

    def submit(self, items, available, config, *, bind=None, on_event=None,
               stop_on_failure=True, priority=0):
        if self._closed:
            raise RuntimeError("controller is closed")
        if type(priority) is not int:
            raise TypeError("priority must be an integer")
        for item in items:
            name = _placement_of(item)
            if available.get(name) is not None and name not in self.capacities:
                raise UnsupportedPlacement(f"no executor capacity for placement {name!r}")
        run = ControlRun(self, items, available, config, bind, on_event,
                         stop_on_failure, priority)
        self.runs.append(run)
        self._wake.set()
        return run

    def _spawn(self, run, coroutine):
        task = asyncio.create_task(coroutine)
        run.pending.add(task)
        def finished(done):
            run.pending.discard(done)
            if not done.cancelled() and done.exception() is not None:
                run.error = run.error or done.exception()
                run.stop()
            self._wake.set()
        task.add_done_callback(finished)
        return task

    async def _save(self, run, item, step):
        outcome = replace(step.outcome, invocation_id=item.invocation_id,
                          authored_key=item.authored_key, operation=item.operation)
        # The execution result is evidence even if delivering its selected
        # outputs to this consumer fails. Keep its exact reference first.
        run.completed[item.invocation_id] = _Step(outcome)
        contributed = {}
        try:
            if outcome.outcome == 'succeeded':
                contributed = await self.offload(
                    lambda: produced_by(item, outcome, records_dir=run.config.records_dir))
        except Exception as error:
            run.error = run.error or error
            run.stop()
            outcome = replace(outcome, observation_errors=(*outcome.observation_errors,
                f'output delivery failed: {type(error).__name__}: {error}'))
        else:
            run.produced.update(contributed)
            run.delivered.add(item.invocation_id)
        run.completed[item.invocation_id] = _Step(outcome, contributed)
        if outcome.outcome != 'succeeded' and run.stop_on_failure:
            run.stop()
        if run.on_event:
            await run.on_event(outcome)
        self._wake.set()

    def _prepare_item(self, spec, produced, available, config):
        root = self.executions_dir or Path(config.records_dir).resolve() / '.executions'
        fresh = ExecutionHandle.create(root) if spec.execution == 'each_submission' else None
        try:
            item = finalize_invocation(spec, produced, fresh.execution_id if fresh else None)
            placement, chosen = select_transport(item, available)
            bundle = build_bundle(item, produced=produced, placement_name=placement,
                                  transport=chosen, outputs=config.outputs)
            item = replace(item, bundle=bundle)
            return item, _execution_contract(item, available, config), fresh, root
        except BaseException:
            if fresh:
                fresh.cancel_before_start()
            raise

    async def _prepare(self, run, spec):
        await self._preparation_slots.acquire()
        preparing = True
        entry = None
        consumer = (run.token, spec.invocation_id)
        try:
            item, key, fresh, root = await self.offload(
                self._prepare_item, spec, dict(run.produced), run.available, run.config)
            entry = self.entries.get(key)
            while entry is not None and entry.closing:
                # A sole withdrawal has sealed attachment before inspecting
                # its durable gate. Do not attach after an interrupt decision
                # or dispatch a replacement against its still-active Exec claim.
                # Waiting for an existing execution consumes no preparation
                # lane. Independent Runs can still acquire inputs and bind.
                self._preparation_slots.release()
                self._preparing -= 1
                preparing = False
                self._wake.set()
                settled = asyncio.create_task(entry.settled.wait())
                try:
                    await asyncio.wait((settled, run._stop_signal),
                                       return_when=asyncio.FIRST_COMPLETED)
                finally:
                    settled.cancel()
                    await asyncio.gather(settled, return_exceptions=True)
                if run.stopped:
                    # No handle has been acquired or durably bound for this
                    # occurrence; it need not wait for another Run's execution.
                    entry = None
                    await self._save(run, item, _blocked(item))
                    return
                await self._preparation_slots.acquire()
                self._preparing += 1
                preparing = True
                entry = self.entries.get(key)
            if entry is None or entry.abandoned or (entry.task and entry.task.done()):
                handle_task = asyncio.create_task(self.offload(
                    lambda: fresh or ExecutionHandle.create(root)))
                entry = _Entry(key, handle_task, item, run.available, run.config)
                self.entries[key] = entry
            # Reserve ownership before awaiting handle creation, so withdrawal
            # sees every consumer that can subsequently publish this handle.
            entry.bindings.add(consumer)
            handle = await entry.handle_task
            # Persist each consumer link before giving it any dispatch authority.
            if run.bind:
                await run.bind(spec.invocation_id, handle, item.bundle['input_evidence'])
            entry.bindings.discard(consumer)
            if run.stopped:
                if not entry.consumers and not entry.bindings:
                    entry.abandoned = True
                    await self.offload(handle.cancel_before_start)
                    if entry.task is None and self.entries.get(key) is entry:
                        self.entries.pop(key, None)
                        entry.settled.set()
                await self._save(run, item, _blocked(item))
                return
            if entry.step is not None:
                await self._save(run, item, entry.step)
                return
            entry.consumers[consumer] = (run, item)
            run.active[item.invocation_id] = entry
        except BaseException as error:
            if entry:
                entry.bindings.discard(consumer)
            if entry and not entry.consumers and not entry.bindings:
                entry.abandoned = True
                try:
                    handle = await entry.handle_task
                    await self.offload(handle.cancel_before_start)
                    if entry.task is None and self.entries.get(entry.key) is entry:
                        self.entries.pop(entry.key, None)
                        entry.settled.set()
                except Exception:
                    pass
            await self._save(run, spec, _abnormal(spec, error))
            if not isinstance(error, UnsupportedPlacement):
                run.error = run.error or error
                run.stop()
        finally:
            if preparing:
                self._preparing -= 1
                self._preparation_slots.release()
            self._wake.set()

    async def _execute(self, entry):
        name = _placement_of(entry.item)
        try:
            handle = await entry.handle_task
            # Serialization inside Client.submit is synchronous, even for an
            # asynchronous Client. Keep that work in the owned offload lane.
            priority = entry.priority
            entry.future = await self.offload(
                lambda: self.client.submit(
                    _run_handle, handle, entry.item, entry.available,
                    replace(entry.config, priority=priority),
                    key=f'{_task_key(entry.item)}-{handle.execution_id}',
                    resources=_admission(entry.item, entry.available),
                    priority=priority, fifo_timeout='0ms', pure=False, retries=0))
            step = await entry.future
        except BaseException as error:
            step = _abnormal(entry.item, error)
            for run, _ in entry.consumers.values():
                run.error = run.error or error
                run.stop()
        finally:
            self.outstanding[name] -= 1
        entry.step = step
        for consumer, (run, item) in list(entry.consumers.items()):
            run.active.pop(item.invocation_id, None)
            entry.consumers.pop(consumer, None)
            self._spawn(run, self._save(run, item, step))
        if self.entries.get(entry.key) is entry:
            self.entries.pop(entry.key, None)
        entry.settled.set()
        self._wake.set()

    async def _withdraw(self, run, identifier, entry):
        consumer = (run.token, identifier)
        pair = entry.consumers.get(consumer)
        if pair is None:
            return
        _, item = pair
        handle = await entry.handle_task
        if run.active.get(identifier) is not entry:
            return
        shared = len(entry.consumers) + len(entry.bindings) > 1
        if not shared:
            # Seal on the loop before releasing it for filesystem work. A
            # pending durable binding already counts as another owner.
            entry.closing = True
            entry.settled.clear()
            cancelled = await self.offload(handle.cancel_before_start)
            # Completion may have projected the result while the gate was read.
            if run.active.get(identifier) is not entry:
                return
            shared = len(entry.consumers) + len(entry.bindings) > 1
            if shared:
                entry.closing = False
                entry.settled.set()
            elif not cancelled:
                run.draining.add(identifier)
                transport = entry.available.get(_placement_of(item))
                if run.force_requested and getattr(transport, 'name', None) in {
                    'lsf-pooled', 'bound:lsf-pooled'
                }:
                    # Persist intent only after the sole-ownership recheck.
                    # Keep the outer Future alive so Exec can reconcile the
                    # confirmed interruption into its actual record and try.
                    await self.offload(handle.request_interrupt)
                else:
                    # Ordinary withdrawal still permits later consumers to
                    # join entered work. Escalation will inspect ownership anew.
                    entry.closing = False
                    entry.settled.set()
                return
        if shared:
            entry.consumers.pop(consumer, None)
            step = _Step(_outcome(item, disposition='withdrawn', outcome='cancelled',
                block_reason='consumer withdrew; shared execution belongs to remaining consumers'))
        else:
            entry.consumers.pop(consumer, None)
            entry.abandoned = True
            if entry.task is None and self.entries.get(entry.key) is entry:
                self.entries.pop(entry.key, None)
                entry.settled.set()
            step = _blocked(item)
        run.active.pop(identifier, None)
        await self._save(run, item, step)

    async def _block_remaining(self, run, remaining):
        # Withdrawal projects every occurrence but needs only one cooperative
        # coroutine, even for a large Plan. Storage backpressure is respected.
        for index, item in enumerate(remaining):
            if index % 64 == 0:
                await asyncio.sleep(0)
            try:
                await self._save(run, item, _blocked(item))
            except Exception as error:
                run.completed.setdefault(item.invocation_id, _abnormal(item, error))
                run.error = run.error or error

    def _ready_specs(self, run):
        return [item for item in run.remaining
                if all(dep in run.delivered for dep in item.depends_on)]

    def _ordered_runs(self):
        # Equal-priority runs rotate at every preparation/dispatch opportunity.
        runs = [run for run in self.runs if not run.done and not run.stopped]
        if self._last_run in runs:
            index = runs.index(self._last_run) + 1
            runs = runs[index:] + runs[:index]
        return sorted(runs, key=lambda run: -run.priority)

    async def _drive(self):
        try:
            while True:
                await self._wake.wait()
                self._wake.clear()
                for run in self.runs:
                    if run.stopped:
                        if run.remaining:
                            remaining, run.remaining = run.remaining, []
                            self._spawn(run, self._block_remaining(run, remaining))
                        for identifier, entry in list(run.active.items()):
                            if identifier not in run.draining and not any(getattr(task, '_withdraw_id', None) == identifier for task in run.pending):
                                task = self._spawn(run, self._withdraw(run, identifier, entry))
                                task._withdraw_id = identifier
                # At most two preparation operations enter offload concurrently.
                # A slow source/binding leaves another opportunity for progress.
                for run in self._ordered_runs():
                    if self._preparing >= 2:
                        break
                    # Do not eagerly materialize every node of a large Plan.
                    # Each Run keeps at most one placement-window of prepared
                    # or settling work, without limiting waiting receipts.
                    if len(run.active) + len(run.pending) >= max(1, sum(self.capacities.values())):
                        continue
                    ready = self._ready_specs(run)
                    if not ready:
                        continue
                    item = ready[0]
                    run.remaining.remove(item)
                    if any(run.completed[dep].outcome.outcome != 'succeeded' for dep in item.depends_on):
                        self._spawn(run, self._save(run, item, _Step(_outcome(
                            item, disposition='skipped', outcome='blocked', block_reason='dependency failure'))))
                    else:
                        self._preparing += 1
                        self._spawn(run, self._prepare(run, item))
                        self._last_run = run
                    self._wake.set()
                candidates = [entry for entry in self.entries.values()
                              if entry.consumers and entry.task is None
                              and not entry.abandoned and not entry.closing]
                # Prepared execution nominations follow Run priority then rotation.
                order = {run: index for index, run in enumerate(self._ordered_runs())}
                candidates.sort(key=lambda entry: (-entry.priority, min(
                    (order.get(run, len(order)) for run, _ in entry.consumers.values()), default=len(order))))
                for entry in candidates:
                    name = _placement_of(entry.item)
                    if self.outstanding[name] >= self.capacities[name]:
                        continue
                    self.outstanding[name] += 1
                    self._last_run = max((run for run, _ in entry.consumers.values()), key=lambda run: run.priority)
                    entry.task = asyncio.create_task(self._execute(entry))
                    self._executions.add(entry.task)
                    def executed(task):
                        self._executions.discard(task)
                        self._wake.set()
                    entry.task.add_done_callback(executed)
                for run in self.runs:
                    if not run.done and not run.remaining and not run.active and not run.pending:
                        run._result.set_result(_report(run.items, run.completed))
                self.runs[:] = [run for run in self.runs if not run.done]
                if self._last_run is not None and self._last_run.done:
                    self._last_run = None
                if self._closed and all(run.done for run in self.runs):
                    if not self._executions:
                        return
        except BaseException as error:
            # A broken engine has lost admission authority, not ownership of
            # entered work. Stop new dispatch, withdraw and drain before any
            # receipt becomes terminal. Execution tasks retain actual evidence.
            self._closed = True
            runs = tuple(self.runs)
            for run in runs:
                run.error = run.error or error
                run.stop()
                if run.remaining:
                    remaining, run.remaining = run.remaining, []
                    self._spawn(run, self._block_remaining(run, remaining))
            while True:
                for run in runs:
                    for identifier, entry in list(run.active.items()):
                        if identifier not in run.draining and not any(
                            getattr(task, '_withdraw_id', None) == identifier for task in run.pending):
                            task = self._spawn(run, self._withdraw(run, identifier, entry))
                            task._withdraw_id = identifier
                tasks = [*self._executions, *(task for run in runs for task in run.pending)]
                if not tasks:
                    break
                await asyncio.gather(*tasks, return_exceptions=True)
            for run in runs:
                if not run.done:
                    for item in run.items:
                        # Missing occurrences had no entered execution left
                        # after drain; actual results were stored by _save.
                        run.completed.setdefault(item.invocation_id, _blocked(item))
                    run._result.set_result(_report(run.items, run.completed))
            self.runs.clear()

    async def close(self):
        self._closed = True
        for run in self.runs:
            if not run.done:
                run.stop()
        self._wake.set()
        try:
            await self._driver
        finally:
            if self._executions:
                await asyncio.gather(*tuple(self._executions), return_exceptions=True)
