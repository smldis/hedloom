"""Architecture probe, not a Hedloom implementation or production benchmark.

Run from Hedloom with PYTHONPATH=src:flow/src:exec/src:run/src.
Uses real Dask and real Exec; controller bindings/receipts are experimental.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

from distributed import Client, LocalCluster

from hedloom.history import publish, read_json
from hedloom_exec.durability import Durability, execute
from hedloom_exec.transport import InProcessTransport


def allocate_reference():
    return uuid.uuid4().hex


def wait_for(path):
    deadline = time.monotonic() + 15
    while not Path(path).exists():
        if time.monotonic() >= deadline:
            raise TimeoutError(str(path))
        time.sleep(0.01)


def run_execution(reference, directory, *, fail=False, one_shot=False):
    root = Path(directory)
    receipt = root / 'executions' / reference
    receipt.mkdir(parents=True, exist_ok=True)
    if one_shot:
        # Probe guard only: production publication also needs crash-durable ordering.
        # No automatic replay/recovery is promised by this deliberately bounded mode.
        try:
            with (receipt / 'entered').open('x') as stream:
                stream.write(reference)
        except FileExistsError:
            raise RuntimeError('execution replay refused before entering Exec') from None

    def selection_sink(selection):
        publish(receipt / 'selection.json', {
            'record': selection.record, 'try_number': selection.try_number,
        }, immutable=True)
        if selection.workspace_known:
            publish(receipt / 'workspace.json', {
                'record': selection.record, 'try_number': selection.try_number,
                'workspace': selection.workspace,
            }, immutable=True)

    def body():
        with (root / 'body-calls').open('a') as stream:
            stream.write('call\n')
        wait_for(root / 'release')
        if fail:
            raise ValueError('deliberate probe failure')
        return 42

    result = execute(
        InProcessTransport({'probe': body}),
        {'operation': 'probe', 'arguments': {}, 'outputs': {'value': {'value': True}}},
        root=str(root / 'records'), workspace_root=str(root / 'work'),
        durability=Durability.RECORDED, on_selection=selection_sink,
    )
    publish(receipt / 'terminal.json', {'outcome': result.outcome}, immutable=True)
    return result


class SessionOwner:
    """Probe of one controller owning requests; no distributed registration.

    Narrow compatibility key is sufficient ONLY for this fixed-function probe.
    Production must include the entire bound execution contract and dependencies.
    """

    def __init__(self, client):
        self.client = client
        self.lock = threading.Lock()
        self.active = {}

    def acquire(self, root, consumer, *, fail=False):
        key = (str(root), fail)
        with self.lock:
            old = self.active.get(key)
            if old is not None:
                reference, future, consumers = old
                terminal = root / 'executions' / reference / 'terminal.json'
                if future.done() or terminal.exists():
                    old = None
            if old is None:
                reference = uuid.uuid4().hex
                # Bind the initial consumer before scheduling any execution.
                binding = bind(root, consumer, reference)
                future = self.client.submit(run_execution, reference, str(root),
                    fail=fail, one_shot=True, key='owned-' + reference, pure=False)
                consumers = set()
                self.active[key] = reference, future, consumers
            else:
                binding = bind(root, consumer, reference)
            consumers.add(consumer)
            return future, binding

    def withdraw(self, root, consumer, *, fail=False):
        with self.lock:
            reference, future, consumers = self.active[(str(root), fail)]
            consumers.remove(consumer)
            # Last-consumer cancellation is deliberately NOT implemented in this probe.
            return len(consumers)


def bind(root, consumer, reference):
    path = root / 'runs' / consumer / 'binding.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    publish(path, {'execution': str(root / 'executions' / reference)}, immutable=True)
    return path


def inspect_in_fresh_process(binding):
    # Deliberately no Client, scheduler connection, imported callbacks, or history scan.
    result = subprocess.run([sys.executable, '-c', '''
import json, sys
from pathlib import Path
binding = json.loads(Path(sys.argv[1]).read_text())
print((Path(binding['execution']) / 'selection.json').read_text())
''', str(binding)], check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def task_absent(key, dask_scheduler):
    return key not in dask_scheduler.tasks


def await_forgotten(client, keys):
    deadline = time.monotonic() + 5
    while not all(client.run_on_scheduler(task_absent, key=key) for key in keys):
        if time.monotonic() > deadline:
            raise TimeoutError('scheduler did not forget released probe tasks')
        time.sleep(0.01)


def main():
    observations = {}
    with tempfile.TemporaryDirectory(prefix='discovery-reference-') as tmp:
        root = Path(tmp)
        with LocalCluster(n_workers=1, threads_per_worker=2, processes=False,
                          dashboard_address=None) as cluster, Client(cluster) as client:
            first_ticket = client.submit(allocate_reference, key='ticket', pure=False)
            reference = first_ticket.result()
            a = bind(root, 'A', reference)
            first = client.submit(run_execution, first_ticket, str(root), key='execution', pure=False)
            wait_for(root / 'executions' / reference / 'selection.json')

            # B arrives only AFTER selection while the body is still blocked.
            second_ticket = client.submit(allocate_reference, key='ticket', pure=False)
            assert second_ticket.result() == reference
            b = bind(root, 'B', second_ticket.result())
            second = client.submit(run_execution, second_ticket, str(root), key='execution', pure=False)
            a_selection, b_selection = inspect_in_fresh_process(a), inspect_in_fresh_process(b)
            assert a_selection == b_selection
            assert not first.done() and not second.done()
            observations['staggered_live_consumers'] = 'same exact try readable from fresh process before completion'
            (root / 'release').touch()
            one, two = first.result(), second.result()
            assert one.value == two.value == 42
            assert (one.record, one.try_number) == (a_selection['record'], a_selection['try_number'])
            assert (root / 'body-calls').read_text().splitlines() == ['call']
            observations['shared_execution'] = 'one actual Exec body call; both results match early selection'

            # A stale receipt must not be inferred from a freshly recreated Dask key.
            first.release()
            second.release()
            first_ticket.release()
            second_ticket.release()
            await_forgotten(client, ['execution', 'ticket'])
            new_ticket = client.submit(allocate_reference, key='ticket', pure=False)
            new_reference = new_ticket.result()
            assert new_reference != reference
            c = bind(root, 'C', new_reference)
            third = client.submit(run_execution, new_ticket, str(root), key='execution', pure=False)
            three = third.result()
            assert three.disposition == 'completed'
            assert inspect_in_fresh_process(c)['try_number'] == one.try_number
            observations['released_graph_then_resubmission'] = 'fresh receipt identity; Exec reuses completed try'

            # Counterexample: a lost/recomputed allocation task is NOT a stable identity.
            client.retry([new_ticket])
            regenerated = new_ticket.result()
            assert regenerated != new_reference
            observations['allocation_recomputation_counterexample'] = 'same allocation task key can produce a different reference; must be fenced or made durable'

            # Counterexample: cancel is per Client/key, not per Python Future object.
            cancel_root = root / 'cancellation'
            cancel_root.mkdir()
            ca = client.submit(run_execution, 'cancel-reference', str(cancel_root), key='cancel-execution', pure=False)
            wait_for(cancel_root / 'executions' / 'cancel-reference' / 'selection.json')
            cb = client.submit(run_execution, 'cancel-reference', str(cancel_root), key='cancel-execution', pure=False)
            client.cancel([ca], force=False)
            assert cb.cancelled()
            (cancel_root / 'release').touch()
            observations['same_client_cancellation_counterexample'] = 'cancelling A also cancels B future; consumer ownership needs explicit policy'

            # Counterexample: a failed execution is legitimately followed by a new Exec try.
            # Reusing a receipt for a task rerun must not silently leave its first selection.
            fail_root = root / 'failure'
            fail_root.mkdir()
            (fail_root / 'release').touch()
            failure = client.submit(run_execution, 'failure-reference', str(fail_root), fail=True,
                                    key='failed-execution', pure=False)
            failed_one = failure.result()
            client.retry([failure])
            failed_two = failure.result()
            selection = read_json(fail_root / 'executions' / 'failure-reference' / 'selection.json')
            assert failed_two.try_number != failed_one.try_number
            assert selection['try_number'] == failed_one.try_number
            observations['execution_recomputation_counterexample'] = 'observer-only immutable receipt retains old try while reexecuted task returns a new try; cannot ship without a guard'

            # A bounded alternative: Session already owns the Client and now also
            # owns active execution handles. No extra Dask allocation tasks.
            owned_root = root / 'owned'
            owner = SessionOwner(client)
            fa, ba = owner.acquire(owned_root, 'A')
            wait_for(Path(read_json(ba)['execution']) / 'selection.json')
            fb, bb = owner.acquire(owned_root, 'B')
            assert fa.key == fb.key
            assert inspect_in_fresh_process(ba) == inspect_in_fresh_process(bb)
            assert owner.withdraw(owned_root, 'A') == 1
            assert not fb.cancelled()
            (owned_root / 'release').touch()
            assert fb.result().value == 42
            assert (owned_root / 'body-calls').read_text().splitlines() == ['call']
            observations['owned_handle_staggered_consumers'] = 'exact live reference, one body, no extra Dask task; withdrawing A leaves B intact'

            old_selection = inspect_in_fresh_process(bb)
            client.retry([fb])
            try:
                fb.result()
            except RuntimeError as error:
                assert 'replay refused' in str(error)
            else:
                raise AssertionError('guard allowed a replay')
            assert inspect_in_fresh_process(bb) == old_selection
            assert (owned_root / 'body-calls').read_text().splitlines() == ['call']
            observations['owned_handle_replay_guard'] = 'replay refused before Exec; old exact reference unchanged; no second body'

            fc, bc = owner.acquire(owned_root, 'C')
            assert fc.key != fa.key
            assert fc.result().disposition == 'completed'
            assert inspect_in_fresh_process(bc) == old_selection
            observations['owned_handle_later_success'] = 'new dispatch after completion; Exec reuses the original successful try'

            owned_failure_root = root / 'owned-failure'
            owned_failure_root.mkdir()
            (owned_failure_root / 'release').touch()
            f1, b1 = owner.acquire(owned_failure_root, 'first', fail=True)
            r1 = f1.result()
            f2, b2 = owner.acquire(owned_failure_root, 'second', fail=True)
            r2 = f2.result()
            assert r2.try_number == r1.try_number + 1
            assert inspect_in_fresh_process(b1)['try_number'] == r1.try_number
            assert inspect_in_fresh_process(b2)['try_number'] == r2.try_number
            observations['owned_handle_later_failure'] = 'new submission after failure gets a new try; both historical selections stay exact'

    print(json.dumps(observations, indent=2))


if __name__ == '__main__':
    main()
