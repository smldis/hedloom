"""Standalone multi-process helpers authenticate their execution listeners."""
import asyncio

import pytest

distributed = pytest.importorskip("distributed")

from distributed.security import Security
from hedloom_run.cluster import local_cluster
from hedloom_run.site import SiteError


def rejected_client(cluster, security, *, address=None, expected=(OSError, TimeoutError, TypeError)):
    async def check():
        client = None
        try:
            with pytest.raises(expected):
                client = distributed.Client(address or cluster.scheduler_address, security=security,
                    asynchronous=True, timeout="500 ms", set_as_default=False)
                await asyncio.wait_for(client, timeout=2)
        finally:
            if client is not None:
                await client.close()
    # A refused connection needs no additional synchronous Client loop/thread.
    cluster.sync(check)


@pytest.mark.parametrize("authentication", ["tls", "none"])
def test_multiprocess_cluster_requires_credentials_unless_explicitly_disabled(authentication):
    cluster = local_cluster(threads=1, processes=True, dashboard="loopback",
                            authentication=authentication)
    try:
        assert cluster.scheduler_address.startswith(authentication.replace("none", "tcp") + "://")
        assert cluster.security.require_encryption is (authentication == "tls")
        with distributed.Client(cluster, set_as_default=False) as authorized:
            assert authorized.submit(lambda: 6 * 7).result(timeout=15) == 42
        if authentication == "tls":
            # Trusting the server certificate does not authorize submitting code.
            # This client lacks the private key/certificate held by the owner.
            without_identity = Security(tls_ca_file=cluster.security.tls_ca_file,
                                        require_encryption=True)
            rejected_client(cluster, without_identity)
            rejected_client(cluster, Security.temporary(), expected=(OSError, TimeoutError))
            rejected_client(cluster, Security(require_encryption=False),
                address=cluster.scheduler_address.replace("tls://", "tcp://"),
                expected=(OSError, TimeoutError, RuntimeError))
        else:
            with distributed.Client(cluster.scheduler_address, security=Security(require_encryption=False),
                                    set_as_default=False) as uncredentialed:
                assert uncredentialed.submit(lambda: 7).result(timeout=15) == 7
    finally:
        cluster.close()


def test_security_setup_failure_never_starts_an_unprotected_cluster(monkeypatch):
    def unavailable():
        raise ImportError("test unavailable cryptography")
    def unexpected_cluster(**kwargs):
        pytest.fail("security setup failure constructed an execution listener")
    monkeypatch.setattr(Security, "temporary", unavailable)
    monkeypatch.setattr(distributed, "LocalCluster", unexpected_cluster)
    with pytest.raises(SiteError, match="no unauthenticated fallback"):
        local_cluster(processes=True, dashboard="network")
