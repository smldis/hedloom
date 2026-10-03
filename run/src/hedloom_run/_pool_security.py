"""Owned, ephemeral credentials for networked Jobqueue pools."""
from __future__ import annotations

import inspect
import os
from pathlib import Path
import stat
import tempfile

from hedloom_exec.transport import TransportError

_TLS_FIELDS = {
    "tls_ca_file": "CA_FILE",
    "tls_client_key": "CLIENT__KEY", "tls_client_cert": "CLIENT__CERT",
    "tls_scheduler_key": "SCHEDULER__KEY", "tls_scheduler_cert": "SCHEDULER__CERT",
    "tls_worker_key": "WORKER__KEY", "tls_worker_cert": "WORKER__CERT",
}


def temporary_security():
    """Create authenticated contexts tolerant of Dask's abrupt TLS closes.

    Dask closes its framed connections without TLS close_notify, including
    during Nanny startup. Recent OpenSSL reports an unexpected-EOF error that
    can poison another connection on the same thread. Only these owned
    contexts accept the EOF; Dask still rejects incomplete message frames.
    Certificate verification and encryption requirements remain unchanged.
    """
    # Stable module identity is required by Nanny's multiprocessing pickle.
    # Import only when TLS is requested so ordinary Hedloom stays optional.
    from ._tls_security import OwnedSecurity
    return OwnedSecurity.temporary()


class PoolCredentials:
    """Keep worker credentials alive until close, without exposing their text."""

    def __init__(self, records_dir, authentication="tls"):
        from distributed.security import Security

        self.directory = None
        self._temporary = None
        self._files = []
        if authentication not in {"tls", "none"}:
            raise TransportError("pooled authentication must be 'tls' or 'none'")
        if authentication == "none":
            # Override ambient credential settings as well as encryption policy.
            self.security = Security(require_encryption=False, **{
                field: None for field in _TLS_FIELDS
            })
            self.protocol = "tcp://"
            return
        self.protocol = "tls://"
        try:
            root = Path(records_dir).resolve()
            root.mkdir(parents=True, exist_ok=True)
            self._validate_parent(root)
            self._temporary = tempfile.TemporaryDirectory(prefix=".hedloom-pool-", dir=root)
            self.directory = Path(self._temporary.name)
            self._validate(self.directory, 0o700, directory=True)
            # temporary() already requires encryption; passing the keyword
            # again is incompatible with supported distributed versions.
            self.security = temporary_security()
            if not self.security.require_encryption:
                raise ValueError("temporary credentials do not require encryption")
        except TransportError:
            self.close()
            raise
        except Exception as error:
            self.close()
            raise TransportError(
                "could not configure authenticated pooled execution "
                f"({type(error).__name__}); install hedloom-run[pooled] and "
                "provide a writable records_dir shared with farm workers"
            ) from error

    def __repr__(self):
        return f"PoolCredentials(protocol={self.protocol!r}, directory={self.directory!r})"

    def worker_options(self):
        """Apply explicit opt-out inside the batch job, preserving its prologue."""
        if self.protocol != "tcp://":
            return {}
        import dask
        prologue = (dask.config.get("jobqueue.lsf.job-script-prologue", None)
                    or dask.config.get("jobqueue.lsf.env-extra", None) or ())
        return {"job_script_prologue": [
            *prologue,
            "export DASK_DISTRIBUTED__COMM__REQUIRE_ENCRYPTION=False",
            *(f"export DASK_DISTRIBUTED__COMM__TLS__{field}=null"
              for field in _TLS_FIELDS.values()),
        ]}

    @staticmethod
    def _validate_parent(root):
        for path in (root, *root.parents):
            metadata = path.stat()
            mode = stat.S_IMODE(metadata.st_mode)
            if (metadata.st_uid not in {0, os.getuid()}
                    or (mode & 0o022 and not mode & stat.S_ISVTX)):
                raise TransportError(
                    "authenticated pools need records_dir and its ancestors "
                    "protected from other users renaming credential directories"
                )
        if root.stat().st_uid != os.getuid() or stat.S_IMODE(root.stat().st_mode) & 0o022:
            raise TransportError(
                "authenticated pools need records_dir owned by this user "
                "without group/world write permission"
            )

    @staticmethod
    def _validate(path, mode, *, directory=False):
        metadata = Path(path).lstat()
        expected = stat.S_ISDIR if directory else stat.S_ISREG
        if (not expected(metadata.st_mode) or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) != mode):
            raise TransportError("pool credential ownership or permissions are unsafe")

    def capture_worker_files(self, cluster):
        """Jobqueue retains NamedTemporaryFile handles even after its close."""
        for field in ("ca_file", "cert", "key"):
            stream = getattr(cluster, "_job_" + field, None)
            if stream is not None and stream not in self._files:
                self._files.append(stream)
        if self.directory is not None:
            self._validate(self.directory, 0o700, directory=True)
            for stream in self._files:
                if Path(stream.name).parent != self.directory:
                    raise TransportError("pool worker credentials escaped their private directory")
                self._validate(stream.name, 0o600)

    def close(self):
        try:
            for stream in self._files:
                stream.close()
        finally:
            if self._temporary is not None:
                self._temporary.cleanup()


def owned_cluster_type(base, credentials):
    """Use Jobqueue's credential writer and bind cleanup to actual close."""
    class OwnedCluster(base):
        def _get_worker_security(self, security):
            if credentials.protocol == "tcp://":
                return None
            try:
                return super()._get_worker_security(security)
            finally:
                credentials.capture_worker_files(self)

        def close(self, *args, **kwargs):
            try:
                closing = super().close(*args, **kwargs)
            except BaseException:
                credentials.close()
                raise
            if inspect.isawaitable(closing):
                async def finish():
                    try:
                        return await closing
                    finally:
                        credentials.close()
                return finish()
            credentials.close()
            return closing

    return OwnedCluster
