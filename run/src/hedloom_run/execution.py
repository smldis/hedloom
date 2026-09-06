"""Session-owned dispatch references, independent of Exec computation identity.

One handle may enter Exec at most once. Cancellation and entry use the same
durable gate; observers never grant permission to execute or to replay work.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import uuid4
import fcntl
import json
import os
import tempfile


class ExecutionError(RuntimeError):
    """A dispatch reference cannot be used safely."""


def _sync(directory):
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def make_directory(path):
    path = Path(path)
    if path.is_dir():
        return
    make_directory(path.parent)
    try:
        path.mkdir()
    except FileExistsError:
        if not path.is_dir():
            raise
    _sync(path.parent)


def read_document(path):
    try:
        data = json.loads(Path(path).read_text())
        if type(data.get('schema_version')) is not int or data['schema_version'] != 1:
            raise ValueError('unsupported execution schema')
        return data
    except (OSError, ValueError, AttributeError) as error:
        raise ExecutionError(f'cannot read execution document {path}: {error}') from error


def write_document(path, data, *, immutable=False):
    path = Path(path)
    document = {'schema_version': 1, **data}
    fd, temporary = tempfile.mkstemp(prefix='.execution-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(document, stream, sort_keys=True)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        if immutable:
            try:
                os.link(temporary, path)
            except FileExistsError:
                previous = read_document(path)
                if {k: v for k, v in previous.items() if k != 'at'} != {
                    k: v for k, v in document.items() if k != 'at'
                }:
                    raise ExecutionError(f'conflicting execution reference: {path}')
        else:
            os.replace(temporary, path)
        _sync(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@dataclass(frozen=True)
class ExecutionHandle:
    """A serializable dispatch address; it does not reserve an Exec try."""

    location: str
    execution_id: str

    @classmethod
    def create(cls, root):
        identifier = uuid4().hex
        directory = Path(root).resolve() / identifier
        make_directory(directory)
        write_document(directory / 'execution.json', {'execution_id': identifier}, immutable=True)
        with (directory / 'gate.lock').open('xb') as stream:
            stream.flush()
            os.fsync(stream.fileno())
        _sync(directory)
        write_document(directory / 'state.json', {'state': 'ready'})
        return cls(str(directory), identifier)

    @contextmanager
    def _gate(self):
        # The owner is one local Session. Worker/controller use separate file
        # descriptions even with threaded workers. No lock spans a body call.
        with (Path(self.location) / 'gate.lock').open('r+b') as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream, fcntl.LOCK_UN)

    def state(self):
        state = read_document(Path(self.location) / 'state.json').get('state')
        if state not in {'ready', 'entered', 'cancelled', 'finished'}:
            raise ExecutionError('invalid execution admission state')
        return state

    def enter(self):
        with self._gate():
            state = self.state()
            if state == 'cancelled':
                return False
            if state != 'ready':
                raise ExecutionError('execution replay refused before entering Exec')
            write_document(Path(self.location) / 'state.json', {'state': 'entered'})
            return True

    def cancel_before_start(self):
        with self._gate():
            state = self.state()
            if state == 'ready':
                write_document(Path(self.location) / 'state.json', {'state': 'cancelled'})
                return True
            return state == 'cancelled'

    def finish(self):
        with self._gate():
            if self.state() == 'entered':
                write_document(Path(self.location) / 'state.json', {'state': 'finished'})

    def publish_selection(self, selection):
        """Publish Exec-owned evidence at this shared execution address."""
        common = dict(execution_id=self.execution_id, record=selection.record,
                      try_number=selection.try_number,
                      at=datetime.now(timezone.utc).isoformat())
        write_document(Path(self.location) / 'selection.json',
                       {**common, 'disposition': selection.disposition}, immutable=True)
        if selection.workspace_known:
            write_document(Path(self.location) / 'workspace.json',
                           {**common, 'workspace': selection.workspace}, immutable=True)


@dataclass
class OwnedExecution:
    handle: ExecutionHandle
    future: Any = None
    consumers: set[str] = field(default_factory=set)


class ExecutionOwner:
    """Session table of active invocations with compatible finalized bindings.

    Completed entries can be replaced. Consumers retain their exact entry until
    release, which checks object identity before removing a table generation.
    """

    def __init__(self, root):
        self.root = Path(root).resolve() / uuid4().hex
        self.lock = RLock()
        self.groups = {}
        self.client = None

    def entry(self, key, client=None, handle=None):
        """Look up one ready compatible invocation. Caller holds the owner lock."""
        if client is not None:
            if self.client is not None and self.client is not client:
                raise ExecutionError('an execution owner cannot span Dask clients')
            self.client = client
        entry = self.groups.get(key)
        if entry is None or (entry.future is not None and entry.future.done()) or entry.handle.state() == 'cancelled':
            entry = OwnedExecution(handle or ExecutionHandle.create(self.root))
            self.groups[key] = entry
        return entry

    def withdraw(self, entry, consumer):
        with self.lock:
            entry.consumers.discard(consumer)
            if entry.future is not None and entry.future.done():
                return 'preserve'
            if entry.consumers:
                return 'withdrawn'
            if entry.handle.cancel_before_start():
                return 'blocked'
            return 'preserve'

    def release(self, entry, consumer):
        with self.lock:
            entry.consumers.discard(consumer)
            # An old consumer can never remove a replacement generation.
            for key, existing in list(self.groups.items()):
                if existing is entry and not entry.consumers:
                    self.groups.pop(key)

    def close(self):
        with self.lock:
            self.groups.clear()
            self.client = None
