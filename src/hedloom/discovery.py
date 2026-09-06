"""Read-only inspection of consumer history and exact computation evidence."""
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import os

from hedloom.addresses import display_address, invocation_addresses, resolve_invocation
from hedloom.history import HistoryError, parse_run_id, read_events, read_json, read_plan, selection_documents, validate_name
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.identity import try_name
from hedloom_exec.artifacts import workspace_path
from hedloom_run.binding import output_value


@dataclass(frozen=True, slots=True)
class InvocationSnapshot:
    invocation_id: str
    address: str
    components: tuple[str, ...]
    operation: str
    run_reported_outcome: str
    selected_execution_state: str | None
    record: str | None
    try_number: int | None
    workspace: str | None
    workspace_status: str
    journal_dir: str | None
    error: str | None
    blockers: tuple[str, ...] = ()
    diagnostics: tuple[str, ...] = ()
    block_reason: str | None = None
    execution_id: str | None = None


@dataclass(frozen=True, slots=True)
class RunSnapshot:
    run_id: str
    name: str
    occurrence: int
    study_name: str
    at: str
    last_observation: str
    history_status: str
    run_reported_outcome: str
    invocations: tuple[InvocationSnapshot, ...]
    diagnostics: tuple[str, ...]


def since_time(value):
    if value is None:
        return None
    if len(value) == 10:
        return datetime.strptime(value, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('--since timestamps require an explicit timezone')
    return result


def _workspace_status(location, removed):
    if location is None:
        return 'no-workspace'
    if removed:
        return 'reclaimed'
    if not Path(location).is_dir():
        return 'missing'
    if not os.access(location, os.R_OK | os.X_OK):
        return 'inaccessible'
    return 'available'


class RunHistory:
    def __init__(self, history_root):
        if history_root is None:
            raise HistoryError('site declares no history_root')
        self.root = Path(history_root).resolve()

    def read_run(self, run_id):
        name, occurrence = parse_run_id(run_id)
        directory = self.root / 'runs' / run_id
        header = read_json(directory / 'run.json')
        if (header.get('name'), header.get('occurrence'), header.get('run_id')) != (name, occurrence, run_id):
            raise HistoryError(f'run header address mismatch: {run_id}')
        document = read_plan(directory / 'plan.json')
        addresses = read_json(directory / 'addresses.json')['addresses']
        if {key: tuple(parts) for key, parts in addresses.items()} != invocation_addresses(document):
            raise HistoryError('address table does not match the saved Plan')
        events, diagnostics = read_events(directory / 'events.jsonl')
        diagnostics = list(diagnostics)
        persistence_path = directory / 'persistence.json'
        if persistence_path.exists():
            persistence = read_json(persistence_path)
            diagnostics.extend(persistence['errors'])
        outcomes = {}
        final = None
        last = header['at']
        for row in events:
            last = max(last, row['at'])
            data = row['data']
            if row['event'] == 'invocation_outcome':
                identifier = data['invocation_id']
                if identifier not in addresses or identifier in outcomes and outcomes[identifier] != data:
                    raise HistoryError('unknown or conflicting invocation outcome')
                outcomes[identifier] = data
                diagnostics.extend(data.get('observation_errors', []))
            elif row['event'] == 'run_finished':
                if final is not None and final != data:
                    raise HistoryError('conflicting final events')
                final = data
                diagnostics.extend(data.get('errors', []))
            elif row['event'] == 'submission_exception':
                diagnostics.extend(data.get('errors', []))
        if final is not None and set(outcomes) != set(addresses):
            raise HistoryError('run_finished lacks complete invocation accounting')
        rows = []
        for invocation in document['invocations']:
            identifier = invocation['id']
            outcome = outcomes.get(identifier, {})
            selection, binding, execution = selection_documents(directory, identifier)
            for saved in (selection, binding):
                if saved and saved.get('invocation_id') != identifier:
                    raise HistoryError('selection invocation mismatch')
                last = max(last, saved.get('at', last))
            reference = (selection.get('record'), selection.get('try_number'))
            reported = (outcome.get('record'), outcome.get('try_number'))
            if reference[0] is not None and reported[0] is not None and reference != reported:
                raise HistoryError('live and final references conflict')
            if reference[0] is None:
                reference = reported
            if binding and (binding.get('record'), binding.get('try_number')) != reference:
                raise HistoryError('workspace binding reference conflict')
            record, number = reference
            if (record is None) != (number is None) or (number is not None and (type(number) is not int or number < 0)):
                raise HistoryError('invalid selected reference')
            if selection and record is None:
                raise HistoryError('selection without an actual reference')
            state_name = None
            workspace = binding.get('workspace')
            workspace_status = 'binding-unreported' if record else 'no-selection'
            journal_dir = None
            issues = []
            if record:
                if not isinstance(record, str) or Path(record).name != record or record in ('.', '..'):
                    raise HistoryError('invalid selected record')
                journal = AttemptJournal(header['record_root'], record)
                journal_dir = str(journal.directory.resolve())
                if not journal.directory.is_dir():
                    issues.append(f'missing recorded journal: {journal_dir}')
                else:
                    state, partial = journal.snapshot()
                    issues.extend(partial)
                    selected = next((item for item in state.tries if item.number == number), None)
                    if selected is None:
                        issues.append('selected try unavailable in journal snapshot')
                    else:
                        state_name = selected.outcome or selected.phase
                        if selected.is_terminal and journal.read_manifest(number) is None:
                            issues.append('missing selected manifest')
                        removed = any(event.event == 'workspace_removed' and event.data.get('try') == number for event in state.events)
                        if binding:
                            workspace_status = _workspace_status(workspace, removed)
                        elif outcome and outcome.get('reference_provenance') == 'late-outcome':
                            issues.append('live workspace binding unavailable; inspecting recorded receipt')
                            if selected.handle is not None and 'workdir' in selected.handle:
                                workspace = selected.handle['workdir']
                                workspace_status = _workspace_status(workspace, removed)
                if binding and workspace is None:
                    workspace_status = 'no-workspace'
            blockers = tuple(edge['source']['invocation_id'] for edge in document.get('edges', [])
                             if edge.get('target_invocation_id') == identifier and edge.get('source', {}).get('invocation_id') in outcomes
                             and outcomes[edge['source']['invocation_id']]['outcome'] != 'succeeded')
            rows.append(InvocationSnapshot(identifier, display_address(addresses[identifier]),
                tuple(addresses[identifier]), invocation['operation']['name'],
                outcome.get('outcome', 'unreported'), state_name, record, number,
                workspace, workspace_status, journal_dir, outcome.get('error'), blockers, tuple(issues),
                outcome.get('block_reason'), execution.get('execution_id')))
        status = 'incomplete' if final is None else final['history_status']
        if diagnostics and status == 'complete':
            status = 'degraded'
        return RunSnapshot(run_id, name, occurrence, header['study_name'], header['at'], last,
            status, 'unreported' if final is None else 'succeeded' if final['succeeded'] else 'failed',
            tuple(sorted(rows, key=lambda row: row.address)), tuple(dict.fromkeys(diagnostics)))

    def list_runs(self, *, name=None, study=None, since=None):
        if name is not None:
            validate_name(name)
        cutoff = since_time(since)
        rows = []
        for path in (self.root / 'runs').glob('*/run.json'):
            header = read_json(path)
            if name is not None and header['name'] != name or study is not None and header['study_name'] != study:
                continue
            if cutoff and datetime.fromisoformat(header['at']) < cutoff:
                continue
            rows.append(self.read_run(path.parent.name))
        return tuple(sorted(rows, key=lambda row: (row.at, row.run_id), reverse=True))

    def preparations(self):
        return tuple(sorted(str(path) for path in (self.root / 'allocations').glob('*/*')
                     if path.is_dir() and not (self.root / 'runs' / f'{path.parent.name}.{path.name}' / 'run.json').exists()))

    def list_invocations(self, run_id):
        return self.read_run(run_id).invocations

    def invocation(self, run_id, invocation):
        rows = self.list_invocations(run_id)
        identifier = resolve_invocation({row.invocation_id: row.components for row in rows}, invocation)
        return next(row for row in rows if row.invocation_id == identifier)

    def resolve_path(self, run_id, invocation, *, workspace=False, journal_dir=False):
        if workspace == journal_dir:
            raise ValueError('choose exactly one of workspace or journal_dir')
        row = self.invocation(run_id, invocation)
        if not row.record:
            raise HistoryError('invocation has no selected execution')
        path = row.workspace if workspace else row.journal_dir
        if workspace and row.workspace_status != 'available':
            raise HistoryError(f'workspace: {row.workspace_status}')
        if path is None or not Path(path).is_dir():
            raise HistoryError(f'directory missing or inaccessible: {path}')
        if not os.access(path, os.R_OK | os.X_OK):
            raise HistoryError(f'directory inaccessible: {path}')
        return str(Path(path).resolve())

    def outputs(self, run_id):
        """Resolve saved named exports from exact manifests, including None."""
        snapshot = self.read_run(run_id)
        directory = self.root / 'runs' / run_id
        header = read_json(directory / 'run.json')
        document = read_plan(directory / 'plan.json')
        rows = {row.invocation_id: row for row in snapshot.invocations}
        result = {}
        for output in document.get('outputs', []):
            reference = output['reference']
            row = rows.get(reference.get('invocation_id'))
            available = row is not None and row.run_reported_outcome == 'succeeded' and row.record is not None
            value = None
            if available:
                manifest = AttemptJournal(header['record_root'], row.record).read_manifest(row.try_number)
                available = manifest is not None
                if available:
                    evidence = manifest.get('result', {})
                    artifacts = {item['name']: item for item in evidence.get('artifacts', [])}
                    value = output_value(artifacts, evidence.get('value'), reference.get('output_name'))
            result[output['name']] = {'available': available, 'value': value}
        return result


def list_attempts(root, *, workspace_root=None, since=None, outcome=None, record=None, try_number=None):
    cutoff = since_time(since)
    rows = []
    directories = [Path(root) / record] if record else Path(root).glob('*')
    if record and (Path(record).name != record or record in ('.', '..')):
        raise ValueError('record must be one directory name')
    for directory in directories:
        if not (directory / 'layout').is_file():
            continue
        journal = AttemptJournal(root, directory.name)
        state, diagnostics = journal.snapshot()
        standing = journal.read_manifest()
        created = next((event for event in state.events if event.event == 'created'), None)
        for item in state.tries:
            if try_number is not None and item.number != try_number or outcome is not None and item.outcome != outcome:
                continue
            times = [event.at for event in state.events if event.data.get('try') == item.number]
            if cutoff and (not times or datetime.fromisoformat(times[0]) < cutoff):
                continue
            recorded = (item.handle or {})
            workspace = (Path(recorded['workdir']) if recorded.get('workdir') else None) if 'workdir' in recorded else workspace_path(workspace_root or root, try_name(directory.name, item.number))
            removed = any(event.event == 'workspace_removed' and event.data.get('try') == item.number for event in state.events)
            rows.append(dict(record=directory.name, try_number=item.number,
                operation=created.data.get('operation') if created else None,
                at=times[0] if times else None, last_observation=times[-1] if times else None,
                state=item.outcome or item.phase, standing=standing is not None and standing.get('try') == item.number,
                pins=[pin.pin_id for pin in state.pins if pin.is_active and pin.try_number == item.number],
                workspace=str(workspace.resolve()) if workspace is not None else None, payload='no-workspace' if workspace is None else 'reclaimed' if removed else 'available' if workspace.is_dir() else 'absent',
                diagnostics=list(diagnostics)))
    return tuple(sorted(rows, key=lambda row: row['at'] or '', reverse=True))
