"""Synchronous test harness around the sole asynchronous execution path."""
import asyncio

from distributed import Client, Scheduler, SpecCluster, Worker
from hedloom_exec.planned import prepare_invocations
from hedloom_run.binding import available_transports
from hedloom_run.cluster import _silent
from hedloom_run.controller import Controller
from hedloom_run.graph import _RunConfig, _placement_of


def run_bound_plan(document, transport=None, *, transports=None, records_dir,
                   work_dir=None, commands=None, outputs=None, identity_env=None,
                   source_fingerprints=None, source_addresses=None,
                   stop_on_failure=True, on_event=None, on_execution=None):
    async def execute():
        items = prepare_invocations(document, commands=commands, outputs=outputs,
            identity_env=identity_env, source_fingerprints=source_fingerprints)
        available = available_transports(transport, transports)
        names = {_placement_of(item) for item in items if available.get(_placement_of(item)) is not None}
        # One total runnable task keeps deterministic tests of stop policy.
        resources = {f'placement:{name}': 1 for name in names}
        cluster = await SpecCluster(scheduler={'cls': _silent(Scheduler), 'options': {
            'protocol': 'inproc', 'dashboard': False, 'dashboard_address': None}},
            workers={'worker': {'cls': _silent(Worker), 'options': {
                'nthreads': 1, 'resources': resources}}}, asynchronous=True, silence_logs=50)
        client = await Client(cluster, asynchronous=True, set_as_default=False)
        async def offload(function, *args):
            return await asyncio.to_thread(function, *args)
        controller = Controller(client, {name: 1 for name in names}, offload)
        async def bind(*args):
            await offload(on_execution, *args)
        async def event(outcome):
            on_event(outcome)
        try:
            run = controller.submit(items, available, _RunConfig(records_dir,
                work_dir=work_dir, outputs=outputs, sources=dict(source_addresses or {})),
                bind=bind if on_execution else None, on_event=event if on_event else None,
                stop_on_failure=stop_on_failure)
            return await run.wait()
        finally:
            await controller.close()
            await client.close()
            await cluster.close()
    return asyncio.run(execute())
