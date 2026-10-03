"""Ready-only controller evidence with one local placement slot."""
import asyncio
from pathlib import Path
import time

import pytest

from hedloom_exec.planned import prepare_invocations
from hedloom_exec.transport import InProcessTransport
from hedloom_run.controller import Controller
from hedloom_run.graph import _RunConfig

pytest.importorskip('distributed')
from distributed import Client, Scheduler, SpecCluster, Worker
from hedloom_run.cluster import _silent


def operation(*, marker=None, role='x', source=None, fail=False, **kwargs):
    if marker:
        root = Path(marker)
        (root / f'started-{role}').write_text(role)
        while not (root / f'release-{role}').exists():
            time.sleep(.01)
    if fail:
        raise ValueError('controlled failure')
    return {'out': source * 2 if source is not None else role}


def document(*specs):
    invocations = []
    for key, config, source in specs:
        item = {'id': f'invoke:{key}', 'authored_key': key,
                'operation': {'name': 'operation', 'version': '1'},
                'config': [{'name': name, 'value': value} for name, value in config.items()],
                'inputs': [], 'policy': {'name': 'local', 'options': {}}}
        if source:
            item['inputs'] = [{'cardinality': 'scalar', 'name': 'source', 'reference': {
                'type': 'output', 'invocation_id': f'invoke:{source}', 'output_name': 'out'}}]
        invocations.append(item)
    return {'schema_version': 4, 'sources': [], 'operations': [{
        'identity': {'name': 'operation', 'version': '1'}, 'outputs': [{'name': 'out'}]}],
        'invocations': invocations}


async def until(predicate):
    async def poll():
        while not predicate():
            await asyncio.sleep(.01)
    await asyncio.wait_for(poll(), 10)


async def exercise(tmp_path, test, capacity=1):
    cluster = await SpecCluster(scheduler={'cls': _silent(Scheduler), 'options':
        {'protocol': 'inproc', 'dashboard': False, 'dashboard_address': None}},
        workers={'local': {'cls': _silent(Worker), 'options': {
            'nthreads': capacity, 'resources': {'placement:local': capacity}}}},
        asynchronous=True, silence_logs=50)
    client = await Client(cluster, asynchronous=True, set_as_default=False)
    async def offload(function, *args):
        return await asyncio.to_thread(function, *args)
    controller = Controller(client, {'local': capacity}, offload,
                            executions_dir=tmp_path/'executions')
    try:
        await asyncio.wait_for(test(controller), 20)
    finally:
        await controller.close()
        await client.close()
        await cluster.close()


def submit(controller, tmp_path, doc, **kwargs):
    return controller.submit(prepare_invocations(doc),
        {'local': InProcessTransport({'operation': operation})},
        _RunConfig(str(tmp_path/'records')), **kwargs)


class _PooledInProcess(InProcessTransport):
    """Controller-only pooled identity; the held body still drains normally.

    Public fake-farm tests verify actual nanny interruption and cancelled Exec
    evidence. Here a real Dask wrapper keeps running to expose ownership races.
    """

    name = 'lsf-pooled'


def submit_pooled(controller, tmp_path, doc, **kwargs):
    return controller.submit(prepare_invocations(doc),
        {'local': _PooledInProcess({'operation': operation})},
        _RunConfig(str(tmp_path/'records')), **kwargs)


@pytest.mark.parametrize('when', ['initial', 'gate-pending', 'draining'])
def test_force_records_intent_without_cancelling_entered_execution(tmp_path, when):
    async def check(controller):
        links = []
        async def bind(_, handle, __):
            links.append(handle)
        original_offload = controller.offload
        gate_pending, release_gate = asyncio.Event(), asyncio.Event()
        async def offload(function, *args):
            if when == 'gate-pending' and getattr(function, '__name__', '') == 'cancel_before_start':
                gate_pending.set()
                await release_gate.wait()
            return await original_offload(function, *args)
        controller.offload = offload
        run = submit_pooled(controller, tmp_path, document(
            ('sole', {'marker': str(tmp_path), 'role': 'sole'}, None)), bind=bind)
        await until(lambda: (tmp_path/'started-sole').exists())
        entry = run.active['invoke:sole']
        try:
            if when != 'initial':
                run.stop()
                if when == 'gate-pending':
                    await gate_pending.wait()
                else:
                    await until(lambda: 'invoke:sole' in run.draining)
                assert not links[0].interrupt_requested()
            run.stop(force=True)
            release_gate.set()
            await until(links[0].interrupt_requested)
            assert run.stopped and run.force_requested
            assert not run.done
            assert not entry.task.done() and not entry.future.cancelled()
        finally:
            release_gate.set()
            (tmp_path/'release-sole').touch()
        # Force intent is not a fabricated computation outcome. This test's
        # cooperative local body finishes successfully; Exec evidence survives.
        report = await run.wait()
        assert report.succeeded
        assert report.outcomes[0].record and report.outcomes[0].try_number is not None
    asyncio.run(exercise(tmp_path, check))


@pytest.mark.parametrize('binding_pending', [False, True])
@pytest.mark.parametrize('escalate', [False, True])
def test_force_withdrawal_preserves_attached_or_binding_shared_consumer(tmp_path, binding_pending, escalate):
    async def check(controller):
        links = []
        second_bound, release_binding = asyncio.Event(), asyncio.Event()
        async def first_bind(_, handle, __):
            links.append(handle)
        async def second_bind(_, handle, __):
            links.append(handle)
            second_bound.set()
            await release_binding.wait()
        doc = document(('shared', {'marker': str(tmp_path), 'role': 'shared'}, None))
        first = submit_pooled(controller, tmp_path, doc, bind=first_bind)
        await until(lambda: (tmp_path/'started-shared').exists())
        if escalate:
            first.stop()
            await until(lambda: 'invoke:shared' in first.draining)
        second = submit_pooled(controller, tmp_path, doc, bind=second_bind)
        await second_bound.wait()
        assert links[0] == links[1]
        if not binding_pending:
            release_binding.set()
            await until(lambda: bool(second.active))
        try:
            first.stop(force=True)
            report = await first.wait()
            assert report.outcomes[0].disposition == 'withdrawn'
            assert not links[0].interrupt_requested()
            assert not second.done
        finally:
            release_binding.set()
            (tmp_path/'release-shared').touch()
        assert (await second.wait()).succeeded
    asyncio.run(exercise(tmp_path, check))


@pytest.mark.parametrize('stop_waiter', [False, True])
def test_force_seals_attachment_before_gate_offload_and_waits_for_actual_settlement(tmp_path, stop_waiter):
    async def check(controller):
        original_offload = controller.offload
        gate_pending, release_gate = asyncio.Event(), asyncio.Event()
        async def offload(function, *args):
            if getattr(function, '__name__', '') == 'cancel_before_start' and not release_gate.is_set():
                gate_pending.set()
                await release_gate.wait()
            return await original_offload(function, *args)
        controller.offload = offload
        links = []
        async def bind(_, handle, __):
            links.append(handle)
        doc = document(('same', {'marker': str(tmp_path), 'role': 'same'}, None))
        first = submit_pooled(controller, tmp_path, doc, bind=bind)
        await until(lambda: (tmp_path/'started-same').exists())
        first.stop(force=True)
        await gate_pending.wait()
        entry = first.active['invoke:same']
        assert entry.closing
        prepared = asyncio.Event()
        preparation_count = 0
        async def counted_offload(function, *args):
            nonlocal preparation_count
            value = await offload(function, *args)
            if getattr(function, '__name__', '') == '_prepare_item':
                preparation_count += 1
                if preparation_count == 2:
                    prepared.set()
            return value
        controller.offload = counted_offload
        second = submit_pooled(controller, tmp_path, doc, bind=bind)
        third = submit_pooled(controller, tmp_path, doc, bind=bind)
        await prepared.wait()
        try:
            assert len(links) == 1 and not entry.bindings
            release_gate.set()
            await until(links[0].interrupt_requested)
            assert len(links) == 1 and not second.active
            assert not first.done and not second.done and not third.done
            independent = submit_pooled(controller, tmp_path,
                document(('independent', {'role': 'independent'}, None)))
            assert (await independent.wait()).succeeded
            assert not first.done and not second.done and not third.done
            if stop_waiter:
                second.stop(force=True)
                report = await asyncio.wait_for(second.wait(), 1)
                assert report.blocked and report.outcomes[0].record is None
                assert len(links) == 1 and not first.done and not third.done
        finally:
            release_gate.set()
            (tmp_path/'release-same').touch()
        first_report = await first.wait()
        second_report = await second.wait()
        third_report = await third.wait()
        assert len(links) == (2 if stop_waiter else 3)
        assert all(link != links[0] for link in links[1:])
        if not stop_waiter:
            assert second_report.outcomes[0].disposition == 'reused'
            assert second_report.outcomes[0].record == first_report.outcomes[0].record
        assert third_report.outcomes[0].record == first_report.outcomes[0].record
    asyncio.run(exercise(tmp_path, check, capacity=2))


def test_force_on_nonpooled_entered_work_only_drains(tmp_path):
    async def check(controller):
        links = []
        async def bind(_, handle, __):
            links.append(handle)
        run = submit(controller, tmp_path, document(
            ('local', {'marker': str(tmp_path), 'role': 'local'}, None)), bind=bind)
        await until(lambda: (tmp_path/'started-local').exists())
        run.stop(force=True)
        await until(lambda: 'invoke:local' in run.draining)
        assert not links[0].interrupt_requested() and not run.done
        (tmp_path/'release-local').touch()
        assert (await run.wait()).succeeded
    asyncio.run(exercise(tmp_path, check))


def test_interrupt_intent_failure_still_drains_and_preserves_actual_evidence(tmp_path, monkeypatch):
    from hedloom_run.execution import ExecutionHandle
    def fail(_):
        raise OSError('interrupt storage unavailable')
    monkeypatch.setattr(ExecutionHandle, 'request_interrupt', fail)
    async def check(controller):
        run = submit_pooled(controller, tmp_path, document(
            ('held', {'marker': str(tmp_path), 'role': 'held'}, None)))
        await until(lambda: (tmp_path/'started-held').exists())
        run.stop(force=True)
        await until(lambda: run.error is not None)
        assert not run.done and controller._executions
        (tmp_path/'release-held').touch()
        with pytest.raises(OSError, match='interrupt storage unavailable') as caught:
            await run.wait()
        outcome = caught.value.report.outcomes[0]
        assert outcome.outcome == 'succeeded'
        assert outcome.record and outcome.try_number is not None
    asyncio.run(exercise(tmp_path, check))


def test_one_slot_dependencies_reuse_and_report_order(tmp_path):
    async def check(controller):
        doc = document(('seed', {'role': 'a'}, None), ('consumer', {}, 'seed'))
        first = await submit(controller, tmp_path, doc).wait()
        assert first.succeeded
        assert [out.authored_key for out in first.outcomes] == ['seed', 'consumer']
        assert first.outcomes[1].value == {'out': 'aa'}
        assert all(out.record and out.try_number is not None for out in first.outcomes)
        second = await submit(controller, tmp_path, doc).wait()
        assert len(second.reused) == 2
        assert [out.input_digest for out in first.outcomes] == [out.input_digest for out in second.outcomes]
    asyncio.run(exercise(tmp_path, check))


def test_staggered_shared_consumers_withdraw_independently(tmp_path):
    async def check(controller):
        doc = document(('one', {'marker': str(tmp_path), 'role': 'shared'}, None))
        links = []
        async def bind(identifier, handle, evidence):
            links.append(handle.execution_id)
        first = submit(controller, tmp_path, doc, bind=bind)
        await until(lambda: (tmp_path/'started-shared').exists())
        second = submit(controller, tmp_path, doc, bind=bind, priority=10)
        await until(lambda: len(links) == 2)
        assert links[0] == links[1]
        first.stop()
        report = await first.wait()
        assert report.outcomes[0].disposition == 'withdrawn'
        assert not second.done
        (tmp_path/'release-shared').touch()
        assert (await second.wait()).succeeded
    asyncio.run(exercise(tmp_path, check))


@pytest.mark.parametrize('force', [False, True])
def test_stop_during_binding_prevents_entry_and_close_drains_entered(tmp_path, force):
    async def check(controller):
        entered = asyncio.Event()
        release = asyncio.Event()
        async def bind(*_):
            entered.set()
            await release.wait()
        run = submit(controller, tmp_path,
            document(('queued', {'marker': str(tmp_path), 'role': 'queued'}, None)), bind=bind)
        await entered.wait()
        run.stop(force=force)
        release.set()
        report = await run.wait()
        assert report.blocked
        assert not (tmp_path/'started-queued').exists()
        active = submit(controller, tmp_path,
            document(('active', {'marker': str(tmp_path), 'role': 'active'}, None)))
        await until(lambda: (tmp_path/'started-active').exists())
        active.stop()
        await asyncio.sleep(.05)
        assert not active.done
        (tmp_path/'release-active').touch()
        assert (await active.wait()).succeeded
    asyncio.run(exercise(tmp_path, check))


def test_failure_blocks_dependents_and_optional_independent_branches(tmp_path):
    async def check(controller):
        doc = document(('bad', {'fail': True}, None), ('dependent', {}, 'bad'),
                       ('independent', {'role': 'good'}, None))
        report = await submit(controller, tmp_path, doc, stop_on_failure=False).wait()
        assert [out.outcome for out in report.outcomes] == ['failed', 'blocked', 'succeeded']
        stopped = await submit(controller, tmp_path, doc).wait()
        assert stopped.outcomes[1].outcome == 'blocked'
    asyncio.run(exercise(tmp_path, check))


def test_priority_selects_ready_work_and_receipts_exceed_32(tmp_path):
    async def check(controller):
        held = submit(controller, tmp_path,
            document(('held', {'marker': str(tmp_path), 'role': 'held'}, None)))
        await until(lambda: (tmp_path/'started-held').exists())
        low = [submit(controller, tmp_path, document((f'low{i}', {
            'marker': str(tmp_path), 'role': f'low{i}'}, None))) for i in range(35)]
        high = submit(controller, tmp_path, document(('urgent', {
            'marker': str(tmp_path), 'role': 'urgent'}, None)), priority=10)
        await until(lambda: bool(high.active))
        (tmp_path/'release-held').touch()
        await held.wait()
        await until(lambda: (tmp_path/'started-urgent').exists())
        assert not any((tmp_path/f'started-low{i}').exists() for i in range(35))
        for run in low:
            run.stop()
        (tmp_path/'release-urgent').touch()
        assert (await high.wait()).succeeded
        assert all(report.blocked for report in await asyncio.gather(*(run.wait() for run in low)))
    asyncio.run(exercise(tmp_path, check))


def test_binding_failure_settles_receipt_with_partial_report(tmp_path):
    async def check(controller):
        async def fail(*_):
            raise OSError('store unavailable')
        run = submit(controller, tmp_path, document(('one', {}, None)), bind=fail)
        with pytest.raises(OSError, match='store unavailable') as caught:
            await run.wait()
        assert caught.value.report.outcomes[0].outcome == 'failed'
        assert run.done
    asyncio.run(exercise(tmp_path, check))


def test_late_shared_binding_cannot_remove_replacement_generation(tmp_path):
    async def check(controller):
        doc = document(('same', {'marker': str(tmp_path), 'role': 'same'}, None))
        links = []
        second_bound = asyncio.Event()
        release_second = asyncio.Event()
        third_bound = asyncio.Event()
        release_third = asyncio.Event()
        async def first_bind(_, handle, __):
            links.append(handle.execution_id)
        async def second_bind(_, handle, __):
            links.append(handle.execution_id)
            second_bound.set()
            await release_second.wait()
        async def third_bind(_, handle, __):
            links.append(handle.execution_id)
            third_bound.set()
            await release_third.wait()
        first = submit(controller, tmp_path, doc, bind=first_bind)
        await until(lambda: (tmp_path/'started-same').exists())
        second = submit(controller, tmp_path, doc, bind=second_bind)
        await second_bound.wait()
        (tmp_path/'release-same').touch()
        first_report = await first.wait()
        third = submit(controller, tmp_path, doc, bind=third_bind)
        await third_bound.wait()
        assert links[0] == links[1] != links[2]
        release_second.set()
        second_report = await second.wait()
        assert second_report.outcomes[0].record == first_report.outcomes[0].record
        current_ids = [(await entry.handle_task).execution_id
                       for entry in list(controller.entries.values())]
        assert links[2] in current_ids
        release_third.set()
        assert (await third.wait()).succeeded
    asyncio.run(exercise(tmp_path, check))


def test_equal_priority_runs_share_one_slot_and_interrupted_wait_is_observation(tmp_path):
    async def check(controller):
        seen = []
        async def event(outcome):
            seen.append(outcome.authored_key)
        first = submit(controller, tmp_path, document(
            ('a0', {'marker': str(tmp_path), 'role': 'a0'}, None),
            ('a1', {'role': 'a1'}, None), ('a2', {'role': 'a2'}, None)), on_event=event)
        await until(lambda: (tmp_path/'started-a0').exists())
        waiter = asyncio.create_task(first.wait())
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert not first.stopped
        second = submit(controller, tmp_path, document(
            ('b0', {'role': 'b0'}, None), ('b1', {'role': 'b1'}, None)), on_event=event)
        await until(lambda: bool(second.active))
        (tmp_path/'release-a0').touch()
        assert all(report.succeeded for report in await asyncio.gather(first.wait(), second.wait()))
        assert seen.index('b0') < seen.index('a2')
        assert seen.index('a1') < seen.index('b1')
    asyncio.run(exercise(tmp_path, check))


def test_stopping_one_slow_binding_preserves_another_pending_consumer(tmp_path):
    async def check(controller):
        first_bound, second_bound = asyncio.Event(), asyncio.Event()
        release_first, release_second = asyncio.Event(), asyncio.Event()
        links = []
        async def first_bind(_, handle, __):
            links.append(handle.execution_id)
            first_bound.set()
            await release_first.wait()
        async def second_bind(_, handle, __):
            links.append(handle.execution_id)
            second_bound.set()
            await release_second.wait()
        doc = document(('shared', {'marker': str(tmp_path), 'role': 'shared'}, None))
        first = submit(controller, tmp_path, doc, bind=first_bind)
        await first_bound.wait()
        second = submit(controller, tmp_path, doc, bind=second_bind)
        await second_bound.wait()
        assert links[0] == links[1]
        first.stop()
        release_first.set()
        assert (await first.wait()).blocked
        assert not (tmp_path/'started-shared').exists()
        (tmp_path/'release-shared').touch()
        release_second.set()
        assert (await second.wait()).succeeded
    asyncio.run(exercise(tmp_path, check))


def test_large_plan_withdrawal_uses_one_projection_task_and_yields(tmp_path):
    from hedloom_exec.planned import PlannedInvocation
    async def check():
        async def forbidden_offload(*_):
            raise AssertionError('blocked projections need no filesystem work')
        class NoExecutor:
            def submit(self, *_args, **_kwargs):
                raise AssertionError('withdrawn work cannot enter executor')
        controller = Controller(NoExecutor(), {'local': 1}, forbidden_offload)
        items = [PlannedInvocation(str(i), 'never', str(i), (), None, {}) for i in range(10000)]
        observed = []
        heartbeat = []
        async def observe(outcome):
            assert len(run.pending) == 1
            observed.append(outcome.invocation_id)
        async def tick():
            while not run.done:
                heartbeat.append(len(observed))
                await asyncio.sleep(0)
        run = controller.submit(items, {'local': object()}, _RunConfig(str(tmp_path)), on_event=observe)
        run.stop()
        ticker = asyncio.create_task(tick())
        report = await run.wait()
        await ticker
        await controller.close()
        assert len(report.blocked) == len(observed) == 10000
        assert any(0 < count < 10000 for count in heartbeat)
    asyncio.run(check())


def test_output_delivery_failure_retains_execution_and_isolates_other_runs(tmp_path, monkeypatch):
    import hedloom_run.controller as module
    original = module.produced_by
    def broken(item, result, **kwargs):
        if item.authored_key == 'broken':
            raise OSError('controlled output projection failure')
        return original(item, result, **kwargs)
    monkeypatch.setattr(module, 'produced_by', broken)
    async def check(controller):
        held = submit(controller, tmp_path,
            document(('held', {'marker': str(tmp_path), 'role': 'held'}, None)))
        await until(lambda: (tmp_path/'started-held').exists())
        bad = submit(controller, tmp_path, document(
            ('broken', {'role': 'bad'}, None), ('dependent', {}, 'broken')), stop_on_failure=False)
        try:
            with pytest.raises(OSError, match='controlled output projection failure') as caught:
                await bad.wait()
            actual, dependent = caught.value.report.outcomes
            assert actual.outcome == 'succeeded'
            assert actual.record and actual.try_number is not None
            assert actual.artifacts['out']['value'] == 'bad'
            assert actual.observation_errors
            assert dependent.outcome == 'blocked' and dependent.record is None
            assert not held.done and not held.stopped
        finally:
            (tmp_path/'release-held').touch()
        report = await held.wait()
        assert report.succeeded
        assert report.outcomes[0].record and report.outcomes[0].try_number is not None
    asyncio.run(exercise(tmp_path, check, capacity=2))


def test_fatal_controller_failure_drains_entered_work_before_terminal_report(tmp_path, monkeypatch):
    async def check(controller):
        held = submit(controller, tmp_path,
            document(('held', {'marker': str(tmp_path), 'role': 'held'}, None)))
        await until(lambda: (tmp_path/'started-held').exists())
        def engine_failure():
            raise RuntimeError('controlled engine failure')
        monkeypatch.setattr(controller, '_ordered_runs', engine_failure)
        waiting = submit(controller, tmp_path, document(('waiting', {'role': 'waiting'}, None)))
        await until(lambda: controller._closed)
        try:
            await asyncio.sleep(.03)
            assert not held.done
            assert controller._executions
        finally:
            (tmp_path/'release-held').touch()
        with pytest.raises(RuntimeError, match='controlled engine failure') as caught:
            await held.wait()
        actual = caught.value.report.outcomes[0]
        assert actual.outcome == 'succeeded'
        assert actual.record and actual.try_number is not None
        with pytest.raises(RuntimeError, match='controlled engine failure') as caught:
            await waiting.wait()
        assert caught.value.report.blocked
    asyncio.run(exercise(tmp_path, check))
