"""A thread-safe submission receipt, independent of the caller's event loop."""
from __future__ import annotations

import asyncio
from concurrent.futures import Future
from dataclasses import dataclass, replace
from threading import Lock
from uuid import uuid4
import weakref


class AcceptanceError(RuntimeError):
    """The receipt could not become a durably accepted submission."""


class RunFailed(RuntimeError):
    def __init__(self, result):
        self.result = result
        super().__init__(result.error or f'Run {result.run_id or "receipt"} {result.state.lower()}')


class RuntimeClosed(RuntimeError):
    """This owner is closing and cannot accept more work."""


@dataclass(frozen=True, slots=True)
class RunSnapshot:
    submission_id: str
    state: str
    run_id: str | None = None
    completed: int = 0
    total: int = 0
    stop_requested: bool = False
    error: str | None = None
    force_requested: bool = False


class Run:
    """Immediate receipt; acceptance and completion happen independently.

    ``wait`` returns every terminal result. ``result`` additionally requires
    success. Interrupting either wait does not withdraw the submission.
    ``stop`` explicitly withdraws this consumer, preserving other consumers.
    ``stop(force=True)`` also requests interruption of solely owned pooled
    commands; other entered work drains. A pending ordinary stop can escalate.
    """
    def __init__(self, runtime):
        self.submission_id = uuid4().hex
        self._runtime = weakref.ref(runtime)
        self._lock = Lock()
        self._snapshot = RunSnapshot(self.submission_id, 'REGISTERED')
        self._acceptance = Future()
        self._completion = Future()
        self._reference = None

    @property
    def run_id(self):
        return self.snapshot().run_id

    @property
    def reference(self):
        with self._lock:
            return self._reference

    def snapshot(self):
        """Read immutable in-memory state; never touch storage or Dask."""
        with self._lock:
            return self._snapshot

    def done(self):
        return self._completion.done()

    def accepted(self, timeout=None):
        """Wait only for durable acceptance and return its saved address."""
        return self._acceptance.result(timeout)

    def wait(self, timeout=None):
        return self._completion.result(timeout)

    def result(self, timeout=None):
        result = self.wait(timeout)
        if not result.succeeded:
            raise RunFailed(result)
        return result

    def result_if_done(self):
        return self.wait() if self.done() else None

    def stop(self, *, force=False):
        """Withdraw; optionally interrupt solely owned pooled commands.

        Returns whether the request changed. Force is a request, not proof of
        interruption; inspect the terminal result for the actual outcome.
        """
        if type(force) is not bool:
            raise TypeError('force must be a boolean')
        with self._lock:
            if self._completion.done() or self._snapshot.state in {
                    'SUCCEEDED', 'FAILED', 'STOPPED', 'REJECTED'}:
                return False
            if self._snapshot.stop_requested and (not force or self._snapshot.force_requested):
                return False
            self._snapshot = replace(self._snapshot, stop_requested=True,
                                     force_requested=force or self._snapshot.force_requested,
                                     state='STOPPING')
        runtime = self._runtime()
        if runtime is not None:
            runtime._request_stop(self.submission_id)
        return True

    def __await__(self):
        async def observe():
            return await asyncio.shield(asyncio.wrap_future(self._completion))
        return observe().__await__()

    def _update(self, **changes):
        with self._lock:
            # An in-progress phase must not hide a pending withdrawal.
            if self._snapshot.stop_requested and changes.get('state') in {
                    'REGISTERED', 'ACCEPTING', 'PREPARING', 'RUNNING'}:
                changes['state'] = 'STOPPING'
            self._snapshot = replace(self._snapshot, **changes)

    def _accept(self, reference):
        with self._lock:
            self._reference = reference
            self._snapshot = replace(self._snapshot, run_id=reference.run_id)
        self._acceptance.set_result(reference)

    def _finish(self, result):
        self._update(state=result.state, error=result.error,
                     completed=len(result.report.outcomes))
        if not self._acceptance.done():
            self._acceptance.set_exception(AcceptanceError(result.error or 'stopped before acceptance'))
        self._completion.set_result(result)
