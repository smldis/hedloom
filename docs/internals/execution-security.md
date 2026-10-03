# Execution security

## Accepted requirement

Accepted by the user on 2026-10-03: by default, processes running as other OS
users and unauthenticated network clients must be unable to submit work or
otherwise trigger arbitrary code through any Hedloom-owned scheduler, worker,
or pool. This assumes ordinary OS isolation; the owner's account and privileged
administrators are outside the boundary. Startup must fail if the required
protection is unavailable. Enabling a pool or diagnostic dashboard must not
silently weaken execution protection. Every protection mechanism enabled or
introduced must have an explicit documented public opt-out, scoped to the
affected surface and visible in effective configuration.

## Execution connections

The Runtime's readiness scheduler, workers and client communicate through
`inproc` within the submitting process. Local and direct-LSF placements need
no externally connectable Dask execution service. A diagnostic dashboard does
not change that communication protocol.

Farm pools need network connections. Each `lsf-pooled` placement defaults to
`authentication = "tls"`; the effective default is materialized in
`Site.placements`. Dask's `Security.temporary()` creates credentials for mutual
TLS, and Jobqueue passes the corresponding security settings to pool workers.
The submit-host worker plugin carries the same Security configuration to its
pool clients. Each pool has its own credentials. Possessing a pool's credentials
authorizes Dask execution; this is connection authentication, not a sandbox for
code authored by the owner.

The standalone `hedloom_run.cluster.local_cluster(processes=True, ...)` helper
creates a separate, networked execution service. It also defaults to
`authentication="tls"`; its explicit public opt-out is the helper argument
`authentication="none"`. This helper does not change the Runtime's process-local
readiness topology. Inspect `cluster.security.require_encryption` and
`cluster.scheduler_address`: protected connections use `tls://`, while the
explicit unauthenticated mode uses `tcp://`.

The `pooled` extra includes the cryptography dependency needed by Dask's
temporary security support. Dask generates the credentials; users need no manual
certificate setup. Missing support or failed protected startup is
an error, not permission to create an unauthenticated pool.

Credentials are stored under `Site.records_dir` in a temporary directory with
mode `0700`; credential files use mode `0600`. Farm nodes must see those paths
at the same location and enforce the corresponding account isolation. Runtime
close and failed startup remove the temporary directory. Abrupt process death
can leave temporary files; the permissions still carry the isolation boundary.
Credentials are runtime resources, not reusable computation outputs.

Protecting the files alone is insufficient if another user can replace an
ancestor directory. Startup rejects unsafe writable ancestors; sticky directories
such as `/tmp` are acceptable only with owned, protected children. Unix ownership,
ACLs and farm UID mapping must preserve the same boundary across hosts. This
does not verify a remote filesystem's ACL implementation or protect against a
privileged administrator changing it.

## TLS close compatibility

Dask Nanny startup can close TLS connections without `close_notify`. On the
tested Python/OpenSSL stack, this left an OpenSSL error queue that disrupted
later authenticated RPCs. OpenSSL requires an empty thread-local error queue
for reliable I/O error interpretation; CPython's system-error branches can
return before their usual queue cleanup. See
[OpenSSL error handling](https://docs.openssl.org/3.6/man3/SSL_get_error/) and
[CPython 3.14's SSL error paths](https://github.com/python/cpython/blob/v3.14.0/Modules/_ssl.c#L638-L705).

Hedloom's owned TLS contexts set `ssl.OP_IGNORE_UNEXPECTED_EOF` when available.
This accepts a missing TLS close notification while retaining required peer
certificates and encryption. OpenSSL permits this option when the application
detects truncated messages itself; Dask reads declared frame lengths and aborts
incomplete messages. See
[the OpenSSL option](https://docs.openssl.org/3.6/man3/SSL_CTX_set_options/) and
[Dask's framed reads](https://github.com/dask/distributed/blob/2026.7.1/distributed/comm/tcp.py#L215-L258).
The change is confined to owned pool and multi-process helper contexts.
`authentication="none"` selects the separately documented unauthenticated mode
and bypasses TLS.

## Explicit opt-outs

Pool authentication can be disabled for one named placement with
`[placement.pool] authentication = "none"`, or the corresponding Runtime
override `{"placement": {"pool": {"authentication": "none"}}}`. It is visible
in `live.site.placements["pool"]["authentication"]`. This enables unauthenticated
execution listeners for that pool; clients that can reach them can submit code
under the pool workers' OS account. Other pools keep their own settings.

Diagnostic suppression is independently controlled by `[kernel] dashboard`.
`"none"` is the default; `"loopback"` enables local-host diagnostics and
`"network"` enables network-visible diagnostics. The effective value is
`live.site.dashboard`. Neither choice changes pool authentication. See
[Sites and placements](../guide/sites.md#dashboard--diagnostic-http-exposure).

## Diagnostics and evidence limits

Diagnostic HTTP is unauthenticated. Loopback is shared by users of a host;
HTTP can disclose operational data even when execution connections require
mutual TLS. TLS credentials for Dask communication do not authenticate the
diagnostic server. HTTP routes must be examined separately for paths that can
execute code; enabling diagnostics cannot silently create an execution bypass.

With `dashboard="none"`, readiness and pooled scheduler HTTP listeners are
suppressed. Farm workers can still expose unauthenticated health and metrics
endpoints without dashboard routes. Enabling `loopback` or `network` exposes
diagnostics independently of execution authentication. Review of the default
Dask/distributed 2026.7.1 HTTP routes found no arbitrary Python execution or deserialization;
the event stream accepts JSON ping messages, and Bokeh invokes registered
callbacks. This is a bounded route review, not a promise that every diagnostic
action is read-only or that arbitrary plugins and future Dask versions are safe.
Hedloom does not enable Dask's embedded Jupyter support. It is not part of the
supported execution or diagnostic surface.

The maintained source boundary is in `hedloom_run.cluster`, `hedloom_run.pooled`,
`hedloom_run._pool_security`, `hedloom_run._tls_security` and `hedloom_run.site`, documented in the
[Run API](../../run/docs/api.rst). On 2026-10-03, pool security and async-executor
tests passed with a real local TLS worker launched through fake LSF. Authorized
commands ran; scheduler, worker and nanny endpoints refused missing client
certificates, foreign-pool certificates and plaintext connections. The tests also
checked separate pool credentials, private modes and cleanup while cluster
references remained alive. An incomplete framed message on an authenticated
connection was rejected while another authorized client's RPC continued.
No actual-farm authentication probe has been performed.
These checks cannot establish remote filesystem isolation or a real farm's network policy. The
[first-farm-run guide](../guide/first-farm-run.md) preserves those deployment
checks and evidence limits.
