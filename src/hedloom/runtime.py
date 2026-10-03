"""Runtime-owned async coordination; callers need no running event loop."""
from __future__ import annotations

import asyncio
import atexit
from concurrent.futures import Future
from dataclasses import dataclass, replace
import hashlib
import marshal
import os
from pathlib import Path
import sys
from threading import Lock, Thread, current_thread, main_thread
from types import FunctionType, CodeType
from uuid import uuid4
import weakref

from hedloom.binding import BoundTransport
from hedloom.history import HistoryPersistence, HistoryWriter, publish, validate_name
from hedloom.reproducibility import (EnvironmentSnapshot, Reproducibility,
                                    _options, _project_root, capture, capture_environment)
from hedloom.run import Run, RuntimeClosed
from hedloom.study import RunResult, Study, _apply_automatic_retention, start_watcher
from hedloom_run.driver import InvocationOutcome, RunReport
from hedloom_run.site import Site

_LIVE = weakref.WeakSet()


def _bootstrap_pools(site):
    """Jobqueue 0.9 imports a runner that installs SIGINT on import.

    Load the optional dependency on the caller's main thread and restore its
    handler. Cluster creation/scaling remains entirely on the controller.
    """
    if not any(options.get('kind') == 'lsf-pooled' for options in site.placements.values()):
        return
    if 'dask_jobqueue' in sys.modules:
        return
    if current_thread() is not main_thread():
        raise RuntimeError('a cold pooled Runtime must be created on the main thread; '
                           'dask-jobqueue installs signal handlers during import')
    import signal
    previous = signal.getsignal(signal.SIGINT)
    try:
        import dask_jobqueue  # noqa: F401 - bootstrap without submitting jobs
    except ImportError as error:
        raise RuntimeError('pooled placement needs dask-jobqueue; install hedloom-run[pooled]') from error
    finally:
        signal.signal(signal.SIGINT, previous)


def _complete_unentered(document, report, reason):
    """Account for work never entered, without inventing finalized identity."""
    outcomes = {item.invocation_id: item for item in report.outcomes}
    missing = [item for item in document.get('invocations', ()) if item['id'] not in outcomes]
    if not missing:
        return report
    return RunReport((*report.outcomes, *(InvocationOutcome(
        invocation_id=item['id'], authored_key=item.get('authored_key') or item['id'],
        operation=item['operation']['name'], input_digest=None, disposition='skipped',
        outcome='blocked', block_reason=reason)
        for item in missing)))


def _global_names(code):
    yield from code.co_names
    for value in code.co_consts:
        if isinstance(value, CodeType):
            yield from _global_names(value)


def _pin_bodies(implementations):
    """Retain selected code and shallow globals without serializing on submit.

    Same-module helpers referenced by that code are pinned too. Closures,
    mutable objects and external modules remain author-owned dependencies.
    """
    cache = {}
    def pin(body):
        if not isinstance(body, FunctionType):
            return body
        if id(body) in cache:
            return cache[id(body)]
        namespace = dict(body.__globals__)
        pinned = FunctionType(body.__code__, namespace, body.__name__,
                              body.__defaults__, body.__closure__)
        cache[id(body)] = pinned
        pinned.__kwdefaults__ = dict(body.__kwdefaults__) if body.__kwdefaults__ else None
        pinned.__annotations__ = dict(body.__annotations__)
        pinned.__dict__.update(body.__dict__)
        pinned.__module__, pinned.__qualname__ = body.__module__, body.__qualname__
        for name in set(_global_names(body.__code__)):
            value = namespace.get(name)
            if isinstance(value, FunctionType) and value.__module__ == body.__module__:
                namespace[name] = pin(value)
        return pinned
    return {name: pin(body) for name, body in implementations.items()}


@dataclass
class _Submission:
    receipt: Run
    plan: object
    implementations: dict
    study_name: str
    name: str
    priority: int
    stop_on_failure: bool
    reproducibility: Reproducibility
    environment: EnvironmentSnapshot | Future | None
    cwd: str
    argv: tuple
    task: object = None
    control: object = None


class Runtime:
    """One process-owned controller and a site's shared execution capacity.

    Construction and submission do not wait for cluster startup. ``ready`` is
    an optional explicit startup observation. A context releases ownership
    automatically; outside a context the process owns the Runtime until exit.
    """
    def __init__(self, site: Site, *, watch=False, watch_reader=None):
        if not isinstance(site, Site):
            raise TypeError('runtime needs a Site')
        from hedloom_run.graph import nested_submission_context
        if nested_submission_context():
            raise RuntimeError('nested Runtime inside an execution body is unsupported; stage Runs at the caller')
        self.site = site
        self.runtime_id = uuid4().hex
        self.watch = watch
        self._watch_reader = watch_reader
        self._lock = Lock()
        self._loop = None
        self._closing = False
        self._close_task = None
        self._records = {}
        self._ready = Future()
        self._closed = Future()
        self._cluster = self._client = self._controller = None
        self._pools = {}
        self._watcher = None
        self._environments = {}
        self._environment_lock = Lock()
        self._dashboard_link = None
        self._bootstrap_error = None
        try:
            _bootstrap_pools(site)
        except Exception as error:
            self._bootstrap_error = error
        self._thread = Thread(target=self._serve, name=f'hedloom-controller-{self.runtime_id[:8]}', daemon=True)
        _LIVE.add(self)
        self._thread.start()

    @property
    def dashboard_link(self):
        return self._dashboard_link

    def ready(self, timeout=None):
        self._ready.result(timeout)
        return self

    def __enter__(self):
        return self

    def __exit__(self, kind, error, traceback):
        if kind is not None:
            self.stop()
        try:
            self.close()
        except Exception:
            if error is None:
                raise
        return False

    def submit(self, subject: Study, *, name: str, priority=0,
               reproducibility=None, environment=None, stop_on_failure=True):
        """Register a waiting Run immediately, without storage or cluster RPC."""
        from hedloom_run.graph import nested_submission_context
        if nested_submission_context():
            raise RuntimeError('nested submission inside an execution body is unsupported; stage Runs at the caller')
        if not isinstance(subject, Study):
            raise TypeError('submit needs an authored Study')
        validate_name(name)
        if type(priority) is not int:
            raise TypeError('priority must be an integer')
        options = _options(reproducibility)
        if environment is not None and not isinstance(environment, EnvironmentSnapshot):
            raise TypeError('environment must be an EnvironmentSnapshot or None')
        receipt = Run(self)
        cwd = os.getcwd()
        options = replace(options,
                          project_root=os.path.abspath(options.project_root or cwd),
                          files=tuple(os.path.abspath(path) for path in options.files))
        if environment is None and options.enabled:
            with self._environment_lock:
                environment = self._environments.get(str(options.project_root))
        entry = _Submission(receipt, subject.plan, _pin_bodies(dict(subject.implementations)),
                            subject.name, name, priority, stop_on_failure, options,
                            environment, cwd, tuple(sys.argv))
        with self._lock:
            if self._closing:
                raise RuntimeClosed('runtime is closed to new submissions')
            self._records[receipt.submission_id] = entry
            if self._loop is not None:
                self._loop.call_soon_threadsafe(self._launch, entry)
        return receipt

    def stop(self, *, force=False):
        """Withdraw all owned Runs, optionally interrupting pooled commands."""
        if type(force) is not bool:
            raise TypeError('force must be a boolean')
        with self._lock:
            receipts = tuple(entry.receipt for entry in self._records.values())
        for receipt in receipts:
            receipt.stop(force=force)

    def _request_stop(self, identifier):
        with self._lock:
            entry = self._records.get(identifier)
            loop = self._loop
        if entry is not None and loop is not None:
            loop.call_soon_threadsafe(lambda: entry.control.stop(
                force=entry.receipt.snapshot().force_requested) if entry.control else None)

    def refresh_environment(self, *, project_root=None):
        """Explicitly recapture dependencies; queued Runs keep their evidence."""
        key = str(_project_root(project_root))
        snapshot = capture_environment(project_root=key)
        with self._environment_lock:
            completed = Future()
            completed.set_result(snapshot)
            self._environments[key] = completed
        return snapshot

    def _environment(self, entry):
        if not entry.reproducibility.enabled:
            return None
        if entry.environment is not None:
            return entry.environment.result() if isinstance(entry.environment, Future) else entry.environment
        key = str(entry.reproducibility.project_root)
        with self._environment_lock:
            pending = self._environments.get(key)
            leader = pending is None
            if leader:
                pending = Future()
                self._environments[key] = pending
        if leader:
            try:
                pending.set_result(capture_environment(project_root=key))
            except BaseException as error:
                pending.set_exception(error)
                with self._environment_lock:
                    if self._environments.get(key) is pending:
                        self._environments.pop(key)
                raise
        return pending.result()

    def _serve(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        with self._lock:
            self._loop = loop
            pending = tuple(self._records.values())
            closing = self._closing
        loop.create_task(self._initialize())
        for entry in pending:
            loop.call_soon(self._launch, entry)
        if closing:
            loop.call_soon(self._ensure_close)
        try:
            loop.run_forever()
        finally:
            loop.close()
            _LIVE.discard(self)

    async def _initialize(self):
        from hedloom._offload import Offload
        from hedloom_run.cluster import async_cluster_for
        from hedloom_run.controller import Controller
        from hedloom_run.pooled import open_pools_async, attach_pools_async
        try:
            if self._bootstrap_error is not None:
                raise self._bootstrap_error
            self._work = Offload(2, 'prepare')
            self._storage = Offload(1, 'storage')
            self._observation = Offload(1, 'observe')
            self._cluster = await async_cluster_for(self.site)
            from distributed import Client
            self._client = await Client(self._cluster, asynchronous=True, set_as_default=False)
            self._pools = await open_pools_async(self.site)
            await attach_pools_async(self._client, self._pools)
            self._dashboard_link = self._cluster.dashboard_link
            self._controller = Controller(self._client, self.site.capacity, self._work,
                executions_dir=Path(self.site.runs_dir or self.site.records_dir) / '_meta' / 'executions')
            if self.watch:
                self._watcher = start_watcher(self.site.records_dir, self._watch_reader)
            self._ready.set_result(None)
        except BaseException as error:
            self._ready.set_exception(error)
            with self._lock:
                self._closing = True
            self._ensure_close()

    def _launch(self, entry):
        if entry.task is None:
            entry.task = asyncio.create_task(self._run(entry))

    def _prepare(self, entry, document):
        import cloudpickle
        from hedloom_exec.planned import prepare_invocations
        from hedloom_run.graph import _RunConfig, _require_shippable
        implementations = cloudpickle.loads(cloudpickle.dumps(entry.implementations))
        transports = {name: BoundTransport(implementations, delegate)
                      for name, delegate in self.site.transports.items()}
        for name in self.site.placements:
            transports.setdefault(name, BoundTransport(implementations))
        used = {(item.get('policy') or {}).get('name', 'local')
                for item in document.get('invocations', ())}
        _require_shippable({name: transport for name, transport in transports.items() if name in used})
        fingerprints = self.site.fingerprints(document)
        addresses = self.site.source_addresses(document, fingerprints)
        items = prepare_invocations(document, source_fingerprints=fingerprints)
        config = _RunConfig(records_dir=self.site.records_dir, work_dir=self.site.work_dir,
                            sources=addresses, priority=entry.priority)
        evidence = capture(entry.reproducibility, implementations.values(),
                           environment=self._environment(entry))
        evidence['cwd'], evidence['argv'] = entry.cwd, list(entry.argv)
        bindings = {name: {'code_sha256': hashlib.sha256(marshal.dumps(body.__code__)).hexdigest()}
                    for name, body in implementations.items() if isinstance(body, FunctionType)}
        return items, transports, config, evidence, bindings

    async def _run(self, entry):
        receipt = entry.receipt
        writer = None
        document = {}
        report = RunReport(())
        error = None
        state = 'REJECTED'
        try:
            await asyncio.shield(asyncio.wrap_future(self._ready))
            if receipt.snapshot().stop_requested:
                state = 'STOPPED'
                return
            receipt._update(state='ACCEPTING')
            document = await self._work.prioritized(entry.priority, entry.plan.to_data)
            receipt._update(total=len(document.get('invocations', ())))
            writer = await self._storage(HistoryWriter, self.site, entry.name, entry.study_name,
                document, {'stop_on_failure':entry.stop_on_failure, 'kernel':'async',
                           'priority':entry.priority, 'placements':dict(self.site.placements)},
                self._client, submission_id=receipt.submission_id, runtime_id=self.runtime_id,
                preparation_pending=True)
            receipt._accept(writer.reference)
            if self.watch:
                await self._observation(print, f'run {writer.run_id}', file=sys.stderr)
            receipt._update(state='PREPARING')
            if receipt.snapshot().stop_requested:
                state = 'STOPPED'
            else:
                items, transports, config, evidence, bindings = await self._work.prioritized(entry.priority, self._prepare, entry, document)
                await self._storage(writer.prepared, evidence, bindings)
                async def bind(identifier, handle, inputs):
                    await self._storage(writer.bind_execution, identifier, handle, inputs)
                async def observe(outcome):
                    try:
                        await self._storage(writer.observe, outcome)
                    except Exception as observation_error:
                        await self._storage(writer.degrade, observation_error)
                    receipt._update(completed=receipt.snapshot().completed + 1)
                    if self.watch:
                        await self._observation(print, f'[{entry.name}] {outcome.disposition} {outcome.authored_key}: {outcome.outcome}')
                entry.control = self._controller.submit(items, transports, config, bind=bind,
                    on_event=observe, stop_on_failure=entry.stop_on_failure, priority=entry.priority)
                if receipt.snapshot().stop_requested:
                    entry.control.stop(force=receipt.snapshot().force_requested)
                receipt._update(state='RUNNING')
                report = await entry.control.wait()
                state = 'STOPPED' if receipt.snapshot().stop_requested else 'SUCCEEDED' if report.succeeded else 'FAILED'
            report = _complete_unentered(document, report, 'stopped before execution')
            await self._storage(writer.finish, report, state=state)
        except BaseException as caught:
            error = f'{type(caught).__name__}: {caught}'
            report = getattr(caught, 'report', report)
            report = _complete_unentered(document, report, 'submission preparation or engine failure')
            state = 'FAILED' if writer is not None else 'REJECTED'
            if writer is not None:
                try:
                    await self._storage(writer.failed, report, error, state=state)
                except Exception as persistence_error:
                    writer.errors.append(f'completion unrecorded: {persistence_error}')
        finally:
            history = writer.descriptor if writer is not None else HistoryPersistence('', 'unavailable', (error,) if error else ())
            result = RunResult(report, document, entry.study_name,
                               writer.run_id if writer else None, history, state, error)
            receipt._finish(result)
            try:
                if writer is not None and state == 'SUCCEEDED':
                    await self._observation(_apply_automatic_retention, self.site)
            finally:
                with self._lock:
                    self._records.pop(receipt.submission_id, None)

    def close(self, timeout=None):
        """Stop accepting submissions and drain already owned Runs."""
        with self._lock:
            self._closing = True
            loop = self._loop
        if loop is not None and not loop.is_closed():
            loop.call_soon_threadsafe(self._ensure_close)
        self._closed.result(timeout)
        self._thread.join(timeout)

    def _ensure_close(self):
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())

    async def _close(self):
        from hedloom_run.pooled import close_pools_async
        errors = []
        try:
            try:
                await asyncio.shield(asyncio.wrap_future(self._ready))
            except BaseException:
                pass  # Startup failure still requires partial-resource cleanup.
            with self._lock:
                entries = tuple(self._records.values())
            for entry in entries:
                self._launch(entry)
            if entries:
                await asyncio.gather(*(entry.task for entry in entries), return_exceptions=True)
            for component in (self._controller, self._client, self._cluster):
                if component is not None:
                    try:
                        await component.close()
                    except BaseException as caught:
                        errors.append(caught)
            try:
                await close_pools_async(self._pools)
            except BaseException as caught:
                errors.append(caught)
            if self._watcher is not None:
                self._watcher[0].set()
                await self._observation(self._watcher[1].join, timeout=1)
            for name in ('_work', '_storage', '_observation'):
                lane = getattr(self, name, None)
                if lane is not None:
                    await lane.close()
        except BaseException as caught:
            errors.append(caught)
        finally:
            if not errors:
                self._closed.set_result(None)
            else:
                self._closed.set_exception(errors[0])
            asyncio.get_running_loop().call_soon(asyncio.get_running_loop().stop)

    def _exit_owner(self):
        """Bounded process-exit cleanup, without waiting for arbitrary bodies."""
        self.stop()
        with self._lock:
            self._closing = True
            loop = self._loop
        if loop is None or loop.is_closed():
            return
        async def reclaim_pools():
            from hedloom_run.pooled import close_pools_async
            await close_pools_async(self._pools)
        try:
            asyncio.run_coroutine_threadsafe(reclaim_pools(), loop).result(timeout=3)
        except Exception:
            pass  # Owner death still closes direct children/connectivity.


def runtime(site, override=None, *, locally=False, watch=False, _watch_reader=None):
    if override:
        site = site.overridden(override)
    if locally:
        site = site.served_in_process()
    return Runtime(site, watch=watch, watch_reader=_watch_reader)


def run_study(subject: Study, *, site: Site, name: str, override=None,
              locally=False, watch=False, priority=0, reproducibility=None,
              environment=None, stop_on_failure=True, require_success=False,
              _watch_reader=None) -> RunResult:
    """Run one Study to completion and release its owned Runtime.

    This blocking convenience uses the same async execution as ``Runtime``.
    It returns failed terminal results too; ``require_success=True`` raises
    ``RunFailed`` with that result. Startup and invalid-argument errors raise
    directly. Resources are closed before returning or propagating an error.
    Repeated calls create separate runtimes; retain a Runtime to share pools
    and capacity across submissions. Interrupted waits use normal context
    withdrawal and drain semantics, rather than forced termination.
    """
    with runtime(site, override=override, locally=locally, watch=watch,
                 _watch_reader=_watch_reader) as live:
        live.ready()
        receipt = live.submit(subject, name=name, priority=priority,
                              reproducibility=reproducibility, environment=environment,
                              stop_on_failure=stop_on_failure)
        return receipt.result() if require_success else receipt.wait()


@atexit.register
def _reclaim_runtime_owners():
    for owner in tuple(_LIVE):
        owner._exit_owner()
