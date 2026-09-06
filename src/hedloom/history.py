"""Durable consumer history. Computation evidence remains in Exec."""
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import json
import os
import re
import tempfile
import uuid
import warnings

from hedloom.addresses import invocation_addresses

SCHEMA = 1


class HistoryError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class RunReference:
    run_id: str
    location: str


@dataclass(frozen=True, slots=True)
class HistoryPersistence:
    location: str
    status: str
    errors: tuple[str, ...] = ()


def now():
    return datetime.now(timezone.utc).isoformat()


def validate_name(name):
    if not isinstance(name, str) or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]*', name) is None:
        raise HistoryError('submission name must match [A-Za-z0-9][A-Za-z0-9._-]*')
    return name


def parse_run_id(value):
    name, dot, suffix = value.rpartition('.')
    validate_name(name)
    if not dot or re.fullmatch(r'[1-9][0-9]*', suffix) is None:
        raise HistoryError('a complete run address requires a positive canonical occurrence, e.g. experiment.1')
    return name, int(suffix)


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def mkdir(path):
    path = Path(path)
    if path.is_dir():
        return
    mkdir(path.parent)
    try:
        path.mkdir()
    except FileExistsError:
        if not path.is_dir():
            raise
    fsync_dir(path.parent)


def read_json(path):
    try:
        data = json.loads(Path(path).read_text())
    except (OSError, ValueError) as error:
        raise HistoryError(f'cannot read {path}: {error}') from error
    if not isinstance(data, dict) or type(data.get('schema_version')) is not int or data.get('schema_version') != SCHEMA:
        raise HistoryError(f'incompatible or malformed history document: {path}')
    return data


def read_plan(path):
    try:
        data = json.loads(Path(path).read_text())
    except (OSError, ValueError) as error:
        raise HistoryError(f'cannot read saved Plan {path}: {error}') from error
    if not isinstance(data, dict) or data.get('schema_version') not in (2, 3):
        raise HistoryError(f'incompatible saved Plan: {path}')
    return data


def publish(path, data, *, immutable=False, exact=False):
    path = Path(path)
    document = data if exact else {'schema_version': SCHEMA, **data}
    encoded = (json.dumps(document, sort_keys=True) + '\n').encode()
    fd, temporary = tempfile.mkstemp(prefix='.publish-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        if immutable:
            try:
                os.link(temporary, path)
            except FileExistsError:
                old = read_json(path)
                new = json.loads(encoded)
                old.pop('at', None)
                new.pop('at', None)
                if old != new:
                    raise HistoryError(f'conflicting immutable history reference at {path}')
        else:
            os.replace(temporary, path)
        fsync_dir(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def slot(invocation_id):
    return sha256(invocation_id.encode()).hexdigest()


def selection_documents(directory, identifier):
    """Read Exec evidence through the consumer's durable execution binding."""
    from hedloom_run.execution import ExecutionError, read_document

    directory = Path(directory) / 'selections' / slot(identifier)
    link_path = directory / 'execution.json'
    link = read_json(link_path) if link_path.exists() else {}
    if any((directory / filename).exists() for filename in ('selection.json', 'workspace.json')):
        raise HistoryError('selection evidence requires an execution binding')
    if link:
        if link.get('invocation_id') != identifier:
            raise HistoryError('execution binding invocation mismatch')
        location = Path(link['location'])
        if not location.is_absolute() or location.name != link.get('execution_id'):
            raise HistoryError('invalid execution binding location')
        try:
            header = read_document(location / 'execution.json')
            if header.get('execution_id') != link['execution_id']:
                raise HistoryError('execution header identity mismatch')
            documents = []
            for filename in ('selection.json', 'workspace.json'):
                path = location / filename
                data = read_document(path) if path.exists() else {}
                if data:
                    if data.get('execution_id') != link['execution_id']:
                        raise HistoryError('execution selection identity mismatch')
                    data = {**data, 'invocation_id': identifier}
                documents.append(data)
            if documents[1] and not documents[0]:
                data = read_document(location / 'selection.json')
                if data.get('execution_id') != link['execution_id']:
                    raise HistoryError('execution selection identity mismatch')
                documents[0] = {**data, 'invocation_id': identifier}
            return (*documents, link)
        except ExecutionError as error:
            raise HistoryError(str(error)) from error
    return {}, {}, {}


def _worker_handshake(location, token, dask_worker=None):
    directory = Path(location) / 'preflight'
    if (directory / 'challenge').read_text() != token:
        raise HistoryError('history worker challenge mismatch')
    worker = dask_worker.address
    publish(directory / slot(worker), {'token': token, 'worker': worker}, immutable=True)
    return worker


def verify_workers(client, location):
    directory = Path(location) / 'preflight'
    mkdir(directory)
    token = uuid.uuid4().hex
    challenge = directory / 'challenge'
    with challenge.open('w') as handle:
        handle.write(token)
        handle.flush()
        os.fsync(handle.fileno())
    fsync_dir(directory)
    workers = client.run(_worker_handshake, str(location), token)
    for worker in workers.values():
        acknowledgement = read_json(directory / slot(worker))
        if acknowledgement != {'schema_version': SCHEMA, 'token': token, 'worker': worker}:
            raise HistoryError(f'history worker acknowledgement mismatch: {worker}')
    for child in directory.iterdir():
        child.unlink()
    directory.rmdir()
    fsync_dir(directory.parent)


class HistoryWriter:
    def __init__(self, site, name, study_name, document, options, client=None):
        validate_name(name)
        addresses = invocation_addresses(document)
        if site.history_root is None:
            raise HistoryError('Hedloom submission requires Site.history_root / [study] history_root')
        root = Path(site.history_root).resolve()
        for other in (site.root, site.workspace_root):
            if other is not None:
                other = Path(other).resolve()
                if root == other or root in other.parents or other in root.parents:
                    raise HistoryError('history_root must not overlap record or workspace roots')
        mkdir(root)
        limit = os.pathconf(root, 'PC_NAME_MAX')
        if len(os.fsencode(name + '.1')) > limit:
            raise HistoryError('submission name and occurrence exceed filesystem component length')
        reservations = root / 'allocations' / name
        mkdir(reservations)
        number = 1
        while True:
            if len(os.fsencode(f'{name}.{number}')) > limit:
                raise HistoryError('submission name and occurrence exceed filesystem component length')
            try:
                (reservations / str(number)).mkdir()
                fsync_dir(reservations)
                break
            except FileExistsError:
                number += 1
        self.run_id = f'{name}.{number}'
        self.location = root / 'runs' / self.run_id
        mkdir(self.location)
        self.errors = []
        self.seq = 0
        self.append_failed = False
        self.outcomes = {}
        self.addresses = addresses
        for identifier in addresses:
            mkdir(self.location / 'selections' / slot(identifier))
        # Establish no-overwrite publication capability before any body runs.
        probe = self.location / '.capability'
        publish(probe, {'probe': True}, immutable=True)
        probe.unlink()
        fsync_dir(self.location)
        publish(self.location / 'plan.json', document, exact=True)
        publish(self.location / 'addresses.json', {'addresses': addresses})
        if client is not None:
            verify_workers(client, self.location)
        self._append('run_started', {})
        publish(self.location / 'run.json', dict(run_id=self.run_id, name=name,
                occurrence=number, study_name=study_name, at=now(),
                record_root=str(Path(site.root).resolve()), workspace_root=site.workspace_root,
                options=options), immutable=True)

    @property
    def reference(self):
        return RunReference(self.run_id, str(self.location))

    @property
    def descriptor(self):
        return HistoryPersistence(str(self.location), 'degraded' if self.errors else 'complete', tuple(self.errors))

    def degrade(self, error):
        message = str(error)
        if message not in self.errors:
            self.errors.append(message)
            try:
                publish(self.location / 'persistence.json',
                        {'status': 'degraded', 'errors': self.errors, 'at': now()})
            except Exception:
                pass  # Available in memory even when all storage is unavailable.
            try:
                warnings.warn(f'history persistence degraded for {self.run_id}: {message}', RuntimeWarning)
            except Exception:
                pass

    def bind_execution(self, invocation_id, handle):
        if invocation_id not in self.addresses:
            raise HistoryError('cannot bind an unknown invocation')
        publish(self.location / 'selections' / slot(invocation_id) / 'execution.json',
                dict(invocation_id=invocation_id, execution_id=handle.execution_id,
                     location=handle.location, at=now()), immutable=True)

    def _append(self, event, data):
        row = dict(schema_version=SCHEMA, seq=self.seq + 1, at=now(), event=event, data=data)
        with (self.location / 'events.jsonl').open('ab') as handle:
            handle.write((json.dumps(row, sort_keys=True) + '\n').encode())
            handle.flush()
            os.fsync(handle.fileno())
        fsync_dir(self.location)
        self.seq += 1

    def append(self, event, data):
        if self.append_failed:
            return
        try:
            self._append(event, data)
        except Exception as error:
            self.append_failed = True
            self.degrade(error)

    def observe(self, outcome):
        data = {key: getattr(outcome, key) for key in ('invocation_id', 'operation',
                'input_digest', 'placement', 'disposition', 'outcome', 'record', 'try_number', 'error', 'block_reason')}
        for error in outcome.observation_errors:
            self.degrade(error)
        data['observation_errors'] = list(outcome.observation_errors)
        previous = self.outcomes.get(outcome.invocation_id)
        if previous is not None:
            if {key: value for key, value in previous.items() if key != "reference_provenance"} != data:
                self.degrade(f'conflicting outcomes for {outcome.invocation_id}')
            return
        if outcome.record is not None:
            selected, _, _ = selection_documents(self.location, outcome.invocation_id)
            if selected:
                try:
                    if (selected['record'], selected['try_number']) != (outcome.record, outcome.try_number):
                        raise HistoryError(f'conflicting selected reference for {outcome.invocation_id}')
                    data['reference_provenance'] = 'live-selection'
                except Exception as error:
                    self.degrade(error)
            else:
                data['reference_provenance'] = 'late-outcome'
                self.degrade(f'live selection unavailable for {outcome.invocation_id}')
        self.outcomes[outcome.invocation_id] = data
        self.append('invocation_outcome', data)

    def finish(self, report):
        for outcome in report.outcomes:
            try:
                self.observe(outcome)
            except Exception as error:
                self.degrade(error)
        missing = set(self.addresses) - self.outcomes.keys()
        if missing:
            self.degrade(f'unreported invocations: {sorted(missing)}')
        if not missing:
            self.append('run_finished', dict(succeeded=report.succeeded,
                history_status='degraded' if self.errors else 'complete', errors=self.errors))

    def interrupted(self, error):
        partial = getattr(error, 'report', None)
        if partial:
            for outcome in partial.outcomes:
                try:
                    self.observe(outcome)
                except Exception as persistence_error:
                    self.degrade(persistence_error)
        self.degrade(f'completion unrecorded: {type(error).__name__}: {error}')
        self.append('submission_exception', {'error': str(error), 'errors': self.errors})


def read_events(path):
    try:
        raw = Path(path).read_bytes()
    except OSError as error:
        raise HistoryError(f'missing history events: {path}') from error
    incomplete = bool(raw and not raw.endswith(b'\n'))
    lines = raw.split(b'\n')[:-1]
    result = []
    for seq, line in enumerate(lines, 1):
        try:
            row = json.loads(line)
            if type(row['schema_version']) is not int or row['schema_version'] != SCHEMA or type(row['seq']) is not int or row['seq'] != seq:
                raise ValueError('unknown schema or sequence gap')
            if not isinstance(row['data'], dict) or not isinstance(row['at'], str):
                raise ValueError('invalid event envelope')
            if row['event'] not in {'run_started', 'invocation_outcome', 'run_finished', 'submission_exception'}:
                raise ValueError('unknown event')
        except (ValueError, KeyError, TypeError) as error:
            raise HistoryError(f'corrupt history event {seq} in {path}: {error}') from error
        result.append(row)
    return tuple(result), ('incomplete event tail',) if incomplete else ()
