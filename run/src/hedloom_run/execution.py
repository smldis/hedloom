"""Runtime-owned dispatch references, independent of Exec computation identity.

One handle may enter Exec at most once. Cancellation and entry use the same
durable gate; observers never grant permission to execute or to replay work.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
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
        # The owner is one local Runtime. Worker/controller use separate file
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

    def request_interrupt(self) -> bool:
        """Persist force-stop intent without claiming that execution has stopped."""
        with self._gate():
            if self.state() != 'entered':
                return False
            write_document(Path(self.location) / 'interrupt.json',
                           {'requested': True}, immutable=True)
            return True

    def interrupt_requested(self) -> bool:
        path = Path(self.location) / 'interrupt.json'
        return path.exists() and read_document(path).get('requested') is True

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
