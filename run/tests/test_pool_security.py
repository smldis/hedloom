"""Pool authentication, credential isolation and owned lifetime evidence."""
import asyncio
import json
import os
from pathlib import Path
import shutil
import ssl
import stat
import struct
import sys
from urllib.parse import urlsplit

import pytest

from hedloom_exec.transport import TransportError
from hedloom_run._pool_security import PoolCredentials, temporary_security
from hedloom_run.pooled import open_pools, open_pools_async, close_pools_async, run_command
from hedloom_run.site import Site

distributed = pytest.importorskip("distributed")
jobqueue = pytest.importorskip("dask_jobqueue")
pytest.importorskip("cryptography")
from distributed.security import Security


def test_owned_contexts_preserve_verification_and_pickle_without_global_policy_changes():
    import cloudpickle
    import pickle
    owner = temporary_security()
    owner = pickle.loads(pickle.dumps(owner))
    restored = cloudpickle.loads(cloudpickle.dumps(owner))
    assert restored.require_encryption
    flag = getattr(ssl, "OP_IGNORE_UNEXPECTED_EOF", 0)
    for role in ("client", "scheduler", "worker"):
        for context in (restored.get_connection_args(role)["ssl_context"],
                        restored.get_listen_args(role)["ssl_context"]):
            assert context.verify_mode == ssl.CERT_REQUIRED
            assert context.minimum_version >= ssl.TLSVersion.TLSv1_2
            if flag:
                assert context.options & flag
    ordinary = Security.temporary().get_connection_args("client")["ssl_context"]
    if flag:
        assert not ordinary.options & flag


def test_private_security_helper_import_keeps_distributed_optional():
    import subprocess
    probe = '''
import builtins
original = builtins.__import__
def without_distributed(name, *args, **kwargs):
    if name == 'distributed' or name.startswith('distributed.'):
        raise ImportError('distributed intentionally unavailable')
    return original(name, *args, **kwargs)
builtins.__import__ = without_distributed
import hedloom
import hedloom_run._pool_security
'''
    subprocess.run([sys.executable, "-c", probe], check=True, capture_output=True, text=True)


def test_private_credentials_and_repr_do_not_expose_key_material(tmp_path):
    owner = PoolCredentials(tmp_path / "records")
    directory = owner.directory
    try:
        assert directory.parent == (tmp_path / "records").resolve()
        assert directory.stat().st_uid == os.getuid()
        assert stat.S_IMODE(directory.stat().st_mode) == 0o700
        assert owner.security.require_encryption
        assert "-----BEGIN" not in repr(owner)
        assert "-----BEGIN" not in repr(owner.security)
    finally:
        owner.close()
    assert not directory.exists()


@pytest.mark.parametrize("unsafe", ["records", "ancestor"])
def test_unsafe_writable_ancestry_refuses_before_listener_or_worker_files(tmp_path, unsafe):
    parent = tmp_path / "ancestor"
    root = parent / "records"
    root.mkdir(parents=True)
    (root if unsafe == "records" else parent).chmod(0o777)
    with pytest.raises(TransportError, match="protected|without group/world"):
        PoolCredentials(root)
    assert not list(root.glob(".hedloom-pool-*"))


def test_generation_failure_reclaims_private_directory_without_fallback(tmp_path, monkeypatch):
    def unavailable():
        raise ImportError("cryptography is unavailable")
    monkeypatch.setattr(Security, "temporary", unavailable)
    root = tmp_path / "records"
    with pytest.raises(TransportError, match=r"install hedloom-run\[pooled\]"):
        PoolCredentials(root)
    assert not list(root.glob(".hedloom-pool-*"))


def test_constructor_failure_reclaims_jobqueue_retained_worker_files(tmp_path):
    site = Site(records_dir=str(tmp_path / "records"), placements={
        "pool": {"kind": "lsf-pooled", "workers": 0, "max_jobs": 1,
                 "cores": 1, "memory_mb": 1000},
    })
    original = jobqueue.LSFCluster._dummy_job
    # Invalid Job construction follows Jobqueue's credential writer. No
    # scheduler or farm job should survive that constructor failure.
    from unittest.mock import patch
    with patch.object(jobqueue.LSFCluster, "_dummy_job", property(
            lambda self: (_ for _ in ()).throw(ValueError("invalid job shape")))):
        with pytest.raises(ValueError, match="invalid job shape"):
            asyncio.run(open_pools_async(site))
    assert jobqueue.LSFCluster._dummy_job is original
    assert not list((tmp_path / "records").glob(".hedloom-pool-*"))


def test_invalid_sync_allocation_fails_before_creating_credentials(tmp_path):
    site = Site(records_dir=str(tmp_path / "records"), placements={
        "pool": {"kind": "lsf-pooled", "workers": 0, "max_jobs": 1,
                 "cores": "invalid"},
    })
    with pytest.raises(ValueError) as failure:
        open_pools(site)
    # Retain the exception/traceback: credential removal must not require GC.
    assert failure.value is not None
    assert not list((tmp_path / "records").glob(".hedloom-pool-*"))


def test_scheduler_startup_failure_reclaims_worker_credentials(tmp_path, monkeypatch):
    def failed_await(self):
        async def fail():
            raise RuntimeError("deliberate scheduler startup failure")
        return fail().__await__()

    # Inject failure after Jobqueue has written worker credentials, before
    # startup publishes a scheduler. This exercises open_pools cleanup.
    monkeypatch.setattr(jobqueue.LSFCluster, "__await__", failed_await)
    site = Site(records_dir=str(tmp_path / "records"), placements={
        "pool": {"kind": "lsf-pooled", "workers": 0, "max_jobs": 1},
    })
    with pytest.raises(RuntimeError, match="deliberate scheduler startup failure"):
        asyncio.run(open_pools_async(site))
    assert not list((tmp_path / "records").glob(".hedloom-pool-*"))


async def rejected_client(address, security):
    client = distributed.Client(address, security=security, asynchronous=True,
                                set_as_default=False, timeout=.3)
    try:
        with pytest.raises((OSError, ValueError, TypeError, TimeoutError)):
            await asyncio.wait_for(client, timeout=2)
    finally:
        await client.close()


async def rejected_without_client_certificate(address, authority):
    # This actually reaches the TLS listener, trusting its CA but presenting
    # no client certificate. Some TLS1.3 stacks report rejection on first read.
    context = ssl.create_default_context(cadata=authority)
    context.check_hostname = False
    await rejected_tls_connection(address, context)


async def rejected_tls_connection(address, context):
    parsed = urlsplit(address)
    writer = None
    try:
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(
                parsed.hostname, parsed.port, ssl=context, server_hostname="dask-internal"), timeout=2)
            assert await asyncio.wait_for(reader.read(1), timeout=2) == b""
        except (ssl.SSLError, ConnectionError):
            pass
    finally:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except (ssl.SSLError, ConnectionError):
                pass


def test_scheduler_requires_its_own_client_certificate(tmp_path):
    site = Site(records_dir=str(tmp_path / "records"), placements={
        name: {"kind": "lsf-pooled", "workers": 0, "max_jobs": 1}
        for name in ("one", "two")
    })

    async def check():
        pools = await open_pools_async(site)
        client = None
        try:
            address = pools["one"].scheduler_address
            client = await distributed.Client(address, security=pools["one"].security,
                asynchronous=True, set_as_default=False)
            assert (await client.scheduler.identity())["type"].endswith("Scheduler")
            await rejected_without_client_certificate(address, pools["one"].security.tls_ca_file)
            foreign = Security(require_encryption=True,
                tls_ca_file=pools["one"].security.tls_ca_file,
                tls_client_cert=pools["two"].security.tls_client_cert,
                tls_client_key=pools["two"].security.tls_client_key)
            await rejected_tls_connection(address, foreign.get_connection_args("client")["ssl_context"])
            await rejected_client(address.replace("tls://", "tcp://"), Security(require_encryption=False))
        finally:
            if client is not None:
                await client.close()
            await close_pools_async(pools)

    asyncio.run(check())
    assert not list((tmp_path / "records").glob(".hedloom-pool-*"))


def test_truncated_tls_frame_closes_without_poisoning_another_authenticated_connection(tmp_path):
    from distributed.comm import connect
    site = Site(records_dir=str(tmp_path / "records"), placements={
        "pool": {"kind": "lsf-pooled", "workers": 0, "max_jobs": 1},
    })

    async def check():
        pools = await open_pools_async(site)
        client = comm = None
        try:
            pool = pools["pool"]
            client = await distributed.Client(pool, asynchronous=True, set_as_default=False)
            assert (await client.scheduler.identity())["workers"] == {}
            comm = await connect(pool.scheduler_address,
                **pool.security.get_connection_args("client"))
            server_comm = next(connection for connection in pool.scheduler._comms
                               if connection.peer_address == comm.local_address)
            # A valid size prefix followed by no frame body is incomplete in
            # supported Dask framing versions. Terminate TLS without notifying
            # the peer, as Dask does during Nanny registration and worker loss.
            await comm.stream.write(struct.pack("Q", 1024))
            comm.abort()
            async def rejected():
                while not server_comm.closed():
                    await asyncio.sleep(.01)
            await asyncio.wait_for(rejected(), timeout=2)
            assert (await client.scheduler.identity())["workers"] == {}
        finally:
            if comm is not None:
                comm.abort()
            if client is not None:
                await client.close()
            await close_pools_async(pools)

    asyncio.run(check())


def test_tls_pool_authenticates_clients_workers_and_nannies_and_cleans_retained_handles(tmp_path, monkeypatch):
    fake = Path(__file__).resolve().parents[2] / "exec" / "tests" / "fakefarm"
    monkeypatch.setenv("PATH", str(fake) + os.pathsep + os.environ["PATH"])
    farm = tmp_path / "farm"
    monkeypatch.setenv("FAKE_LSF_STATE", str(farm))
    for command in ("bsub", "bjobs", "bkill"):
        assert Path(shutil.which(command)).parent == fake
    records = tmp_path / "records"
    site = Site(records_dir=str(records), placements={
        "one": {"kind": "lsf-pooled", "workers": 1, "max_jobs": 1},
        "two": {"kind": "lsf-pooled", "workers": 0, "max_jobs": 1},
    })

    async def check():
        pools = await open_pools_async(site)
        client = None
        directories = list(records.glob(".hedloom-pool-*"))
        handles = [getattr(pool, "_job_" + field) for pool in pools.values()
                   for field in ("ca_file", "cert", "key")]
        try:
            assert len(directories) == 2
            assert pools["one"].security.tls_ca_file != pools["two"].security.tls_ca_file
            for directory in directories:
                assert stat.S_IMODE(directory.stat().st_mode) == 0o700
                files = list(directory.iterdir())
                assert len(files) == 3
                assert all(stat.S_IMODE(file.stat().st_mode) == 0o600
                           and file.stat().st_uid == os.getuid() for file in files)
            assert all(pool.scheduler_address.startswith("tls://") for pool in pools.values())
            assert all(not hasattr(pool.scheduler, "http_server") for pool in pools.values())
            client = await distributed.Client(pools["one"], asynchronous=True, set_as_default=False)
            await client.wait_for_workers(1, timeout=30)
            workers = (await client.scheduler.identity())["workers"]
            worker, metadata = next(iter(workers.items()))
            endpoints = [pools["one"].scheduler_address, worker, metadata["nanny"]]
            assert all(address.startswith("tls://") for address in endpoints)
            result = await client.submit(run_command, [sys.executable, "-c", "print(42)"], pure=False)
            assert result["stdout"] == "42\n"
            foreign = Security(require_encryption=True,
                tls_ca_file=pools["one"].security.tls_ca_file,
                tls_client_cert=pools["two"].security.tls_client_cert,
                tls_client_key=pools["two"].security.tls_client_key)
            foreign_context = foreign.get_connection_args("client")["ssl_context"]
            for address in endpoints:
                await rejected_without_client_certificate(address, pools["one"].security.tls_ca_file)
                # Trust this server but present another pool's credentials.
                # Rejection must come from server-side client authentication.
                await rejected_tls_connection(address, foreign_context)
                await rejected_client(address, pools["two"].security)
                await rejected_client(address.replace("tls://", "tcp://"), Security(require_encryption=False))
            # Worker scripts and repr contain paths/settings, never PEM data.
            assert "-----BEGIN" not in pools["one"].job_script()
            assert "-----BEGIN" not in repr(pools["one"])
        finally:
            if client is not None:
                await client.close()
            await close_pools_async(pools)
        # Keep clusters and temporary-file wrappers referenced: close must not
        # depend on garbage collection to revoke/remove filesystem credentials.
        assert all(stream.closed for stream in handles)
        assert all(not directory.exists() for directory in directories)

    asyncio.run(asyncio.wait_for(check(), timeout=60))
    jobs = [json.loads(path.read_text()) for path in farm.glob("*.json")]
    assert len(jobs) == 1
    assert all(job["state"] not in ("PEND", "RUN") for job in jobs)


def test_explicit_opt_out_uses_tcp_even_with_ambient_tls_configuration(tmp_path, monkeypatch):
    import dask
    fake = Path(__file__).resolve().parents[2] / "exec" / "tests" / "fakefarm"
    monkeypatch.setenv("PATH", str(fake) + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("FAKE_LSF_STATE", str(tmp_path / "farm"))
    monkeypatch.setenv("DASK_DISTRIBUTED__COMM__REQUIRE_ENCRYPTION", "True")
    monkeypatch.setenv("DASK_DISTRIBUTED__COMM__TLS__CA_FILE", "/invalid/ambient/ca")
    monkeypatch.setenv("DASK_DISTRIBUTED__COMM__TLS__WORKER__CERT", "/invalid/ambient/cert")
    for command in ("bsub", "bjobs", "bkill"):
        assert Path(shutil.which(command)).parent == fake
    site = Site(records_dir=str(tmp_path / "records"), placements={
        "pool": {"kind": "lsf-pooled", "workers": 1, "max_jobs": 1,
                 "authentication": "none"},
    })

    async def check():
        pools = await open_pools_async(site)
        client = None
        try:
            pool = pools["pool"]
            assert pool.scheduler_address.startswith("tcp://")
            assert not pool.security.require_encryption
            assert pool._job_kwargs["security"] is None
            assert "--tls-" not in pool.job_script()
            assert "export HEDLOOM_TEST_PROLOGUE=kept" in pool.job_script()
            client = await distributed.Client(pool.scheduler_address,
                security=Security(require_encryption=False), asynchronous=True, set_as_default=False)
            await client.wait_for_workers(1, timeout=30)
            workers = (await client.scheduler.identity())["workers"]
            assert all(address.startswith("tcp://") and metadata["nanny"].startswith("tcp://")
                       for address, metadata in workers.items())
            def worker_environment():
                import os
                return os.environ["HEDLOOM_TEST_PROLOGUE"]
            assert await client.submit(worker_environment, pure=False) == "kept"
        finally:
            if client is not None:
                await client.close()
            await close_pools_async(pools)
    with dask.config.set({"distributed.comm.require-encryption": True,
                         "jobqueue.lsf.job-script-prologue": ["export HEDLOOM_TEST_PROLOGUE=kept"]}):
        asyncio.run(check())
    assert not list((tmp_path / "records").glob(".hedloom-pool-*"))
    jobs = [json.loads(path.read_text()) for path in (tmp_path / "farm").glob("*.json")]
    assert len(jobs) == 1
    assert jobs[0]["state"] not in ("PEND", "RUN")
