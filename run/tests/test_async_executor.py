"""Owned asynchronous placement and pooled-worker lifecycle evidence."""

import asyncio
import json
import os
from pathlib import Path
import shutil
import sys

import pytest

from hedloom_exec.transport import SubmissionRefused
from hedloom_run.cluster import async_cluster_for
from hedloom_run.pooled import (
    COMMAND_RESOURCE, LSFPooledTransport, attach_pools_async,
    close_pools_async, open_pools_async,
)
from hedloom_run.site import Site
from hedloom_run.execution import ExecutionHandle

distributed = pytest.importorskip("distributed")


def test_interrupt_intent_is_durable_and_separate_from_gate_state(tmp_path):
    handle = ExecutionHandle.create(tmp_path)
    assert not handle.request_interrupt()
    assert not handle.interrupt_requested()
    assert handle.enter()
    assert handle.request_interrupt()
    assert handle.request_interrupt()
    reloaded = ExecutionHandle(handle.location, handle.execution_id)
    assert reloaded.interrupt_requested()
    assert reloaded.state() == "entered"
    reloaded.finish()
    assert not handle.request_interrupt()
    assert handle.state() == "finished"


def test_async_cluster_uses_owner_loop_and_placement_capacity(tmp_path):
    async def check():
        cluster = await async_cluster_for(Site(
            records_dir=str(tmp_path), placements={"local": 2, "farm": 3},
        ))
        client = await distributed.Client(cluster, asynchronous=True, set_as_default=False)
        try:
            await client.wait_for_workers(2, timeout=10)
            assert cluster.loop.asyncio_loop is asyncio.get_running_loop()
            assert client.loop is cluster.loop
            assert not hasattr(cluster.scheduler, "http_server")
            workers = (await client.scheduler.identity())["workers"].values()
            assert sorted(worker["nthreads"] for worker in workers) == [2, 3]
            assert await client.submit(lambda: 42, resources={"placement:local": 1}) == 42
        finally:
            await client.close()
            await cluster.close()
        assert str(cluster.status) == "Status.closed"

    asyncio.run(check())


def test_pooled_requirements_and_priority_are_explicit():
    calls = []

    class Future:
        def result(self):
            return {"returncode": 0, "stdout": "ok", "stderr": ""}

    class Client:
        def submit(self, *args, **kwargs):
            calls.append(kwargs)
            return Future()

    transport = LSFPooledTransport("pool", settings={"cores": 4, "memory_mb": 2000})
    transport._client = lambda: Client()
    bundle = {
        "command": ["echo", "ok"],
        "placement": {"requested": {"options": {"cores": 2, "memory_mb": 1000}}},
        "scheduling": {"priority": 10},
    }
    assert transport.submit("test", bundle)["returncode"] == 0
    assert calls[0]["resources"] == {COMMAND_RESOURCE: 1}
    assert calls[0]["priority"] == 10
    assert calls[0]["fifo_timeout"] == "0 ms"
    for options in ({"cores": 8}, {"memory_mb": 3000}, {"licences": {"tool": 1}},
                    {"queue": "other"}, {"cores": True}):
        bundle["placement"]["requested"]["options"] = options
        with pytest.raises(SubmissionRefused):
            transport.submit("refused", bundle)
    assert len(calls) == 1, "unsupported demands must refuse before command submission"


def test_async_pool_command_reservation_and_ordered_close(tmp_path, monkeypatch):
    pytest.importorskip("dask_jobqueue")
    fake = Path(__file__).resolve().parents[2] / "exec" / "tests" / "fakefarm"
    monkeypatch.setenv("PATH", str(fake) + os.pathsep + os.environ["PATH"])
    farm = tmp_path / "farm"
    monkeypatch.setenv("FAKE_LSF_STATE", str(farm))
    for command in ("bsub", "bjobs", "bkill"):
        assert Path(shutil.which(command)).parent == fake
    log = tmp_path / "commands.log"
    site = Site(records_dir=str(tmp_path / "records"), placements={
        "pool": {"kind": "lsf-pooled", "queue": "normal", "cores": 2,
                 "memory_mb": 1000, "workers": 1, "max_jobs": 2},
    })

    async def check():
        pools = await open_pools_async(site)
        cluster = client = pool_client = None
        try:
            cluster = await async_cluster_for(site)
            client = await distributed.Client(cluster, asynchronous=True, set_as_default=False)
            await attach_pools_async(client, pools)
            pool_client = await distributed.Client(
                pools["pool"], asynchronous=True, set_as_default=False
            )
            await pool_client.wait_for_workers(1, timeout=60)
            assert pools["pool"].loop.asyncio_loop is asyncio.get_running_loop()
            assert all(w["resources"][COMMAND_RESOURCE] == 1
                       for w in (await pool_client.scheduler.identity())["workers"].values())
            transport = site.transports["pool"]

            def invoke(identity):
                script = (
                    "import pathlib,time; p=pathlib.Path(" + repr(str(log)) + "); "
                    "f=p.open('a'); f.write('start\\n'); f.flush(); time.sleep(.25); "
                    "f.write('end\\n'); f.close(); print(42)"
                )
                return transport.submit(identity, {"command": [sys.executable, "-c", script]})

            futures = [client.submit(invoke, str(i), pure=False,
                                     resources={"placement:pool": 1}) for i in range(2)]
            results = await asyncio.wait_for(asyncio.gather(*futures), timeout=30)
            assert all(result["stdout"] == "42\n" and result["returncode"] == 0
                       for result in results)
            assert log.read_text().splitlines() == ["start", "end", "start", "end"]
        finally:
            if pool_client is not None:
                await pool_client.close()
            if client is not None:
                await client.close()
            if cluster is not None:
                await cluster.close()
            await close_pools_async(pools)

    asyncio.run(asyncio.wait_for(check(), timeout=90))
    jobs = [json.loads(path.read_text()) for path in farm.glob("*.json")]
    assert len(jobs) == 1
    assert all(job["state"] not in ("PEND", "RUN") for job in jobs)
