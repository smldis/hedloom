"""TLS context policy for authenticated Hedloom-owned network services."""
import ssl

from distributed.security import Security


class OwnedSecurity(Security):
    """Preserve authentication while accepting Dask's abrupt framed EOF."""

    def _get_tls_context(self, tls, purpose):
        context = super()._get_tls_context(tls, purpose)
        if context is not None:
            context.options |= getattr(ssl, "OP_IGNORE_UNEXPECTED_EOF", 0)
        return context
