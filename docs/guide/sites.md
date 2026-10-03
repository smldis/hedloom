# Sites and placements

Nothing about *where* a study runs is authored into the study. A `Site` holds
all of it: which substrate provides each named placement, the independent
record, run, and try work directories, the address spaces a declared source resolves
through, and how much local concurrency the submit host should offer.

```python
site = Site(
    records_dir=str(work / "records"),
    work_dir=str(work / "work"),
    runs_dir=str(work / "runs"),
    address_spaces={"repository-relative": str(here)},
)
```

(`examples/grid_refinement.py` — constructed directly, because it needs no placement
besides the default in-process one.)

The three paths are configured independently and need no common parent.
`runs_dir` is required for a facade submission. `work_dir` remains optional in
low-level Run and Exec calls; if omitted, try files use the records location.

## Profiles

A profile can be read from TOML with `Site.from_file(path)`. **Relative paths
resolve against the profile's own directory**, not the working directory, so a
study run from elsewhere still means the same thing:

```toml
[study]
records_dir = "_runs/farm-smoke/records"
work_dir = "_runs/farm-smoke/work"
runs_dir = "_runs/farm-smoke/runs"

[placement.lsf]
kind = "lsf-interactive"
queue = "reg"
walltime = "1"
cores = 1
max_jobs = 4

[placement.pool]          # only if some operation asks for pooled()
kind = "lsf-pooled"
authentication = "tls"   # default: mutually authenticated execution connections
queue = "short"
cores = 1
memory_mb = 4000
walltime = "2:00"
workers = 20              # LSF jobs the pool holds open
max_jobs = 20             # invocations in flight against it

[kernel]
threads = 2
dashboard = "none"        # "none" | "loopback" | "network"

[retention]
floor = "7d"

[[retention.rule]]
name = "spent failures"
outcome = ["failed", "cancelled"]
older_than = "14d"
keep_latest = 1
keep_logs = true

[retention.automatic]
after_run = ["spent failures"]
```

(`examples/farm-smoke.site.toml` declares the direct placement;
`examples/farm-smoke-pooled.site.toml` declares one of each.)

A `[placement.*]` naming an unknown `kind` is refused outright rather than
silently dropped, because a study that quietly lost a placement fails much later
as an opaque `UnsupportedPlacement` that blames the Plan for what is a
configuration mistake. The same applies one level down: a misspelled option is
named by placement *and* key rather than raising a bare `TypeError`.

## Retention belongs to the installation

Retention says what one storage site can afford to keep, not what a study
means. Conditions within one rule are ANDed; named rules are ORed. The global
floor, standing reusable result, active pins, non-terminal tries, and
`unreconciled` evidence remain protected regardless of a rule. Every one of
those is a property of the evidence itself; none of them asks which study
requested the work.

`hedloom prune --site site.toml` prints the survey and changes nothing.
`--apply` is the separate destructive gesture, and every candidate is checked
again under its record claim before a durable removal event precedes deletion.
The optional `automatic.after_run` list may name only declared rules. Those
rules run after a completed run; failure warns and cannot change the run's
outcome. There is deliberately no `submit(prune=...)`: a study decides what it
produces, never what the installation keeps.

## Placement kinds

Three kinds exist, and the choice between the two farm ones is a trade rather
than an upgrade.

### `kind = "lsf-interactive"` — one job per invocation

`lsf(...)` on an operation, one `bsub -I` job per invocation, with that
invocation's own queue, cores, memory and licences. The job is visible,
cancellable and accountable as *that invocation*, and it is **owner-bound**: LSF
binds its lifetime to the `bsub` client, which is our child, which dies with
this process.

The vocabulary is `app`, `cores`, `licences`, `memory_mb`, `queue`,
`resources`, and `walltime`, plus `timeout` and `max_jobs`. Options authored on
an invocation override the site's values for that invocation; an option the
transport cannot express is refused before submission rather than dropped.

### `kind = "lsf-pooled"` — a shared set of workers

`pooled()` on an operation sends its command to a *shared* set of LSF workers
the study holds open, so the queue is paid once per worker instead of once per
invocation. That matters when an operation has many short invocations and the wait
to start is a large fraction of the time to run.

What it costs is everything that needs an invocation to *be* a job:
per-invocation farm allocations, per-invocation `bkill`, per-invocation
accounting, per-invocation licence arbitration, and the watcher's ability to
tell you that one particular invocation is queued. The farm sees the pool's
workers, never your invocations.

As a starting rule, pool an operation when its median queue wait is above
roughly a third of its median runtime and its invocations are uniform enough to
share one worker shape — otherwise `lsf(...)` is the better deal.

A pooled placement takes a **narrower vocabulary** than a direct one: no
licences or raw `resources`. Each worker runs one command at a time using its
`hedloom-command` token. Per-command `cores` and `memory_mb` must fit the
configured allocation. Queue, walltime and other pool-shape options must agree
with that allocation; unsupported or unknown requirements refuse. These checks
avoid silently dropping a resource need, but do not establish the farm's own
resource enforcement.

`run.stop(force=True)` can interrupt a solely owned pooled command by restarting
its Dask worker inside the existing allocation. A queued command can be cancelled
without disturbing another command using that worker. Shared executions remain
alive for other consumers. Ordinary `stop()` still drains entered work; see
[stopping and escalation](running.md#receipt-acceptance-and-completion).

Create the first pooled Runtime on the main thread, as ordinary scripts and
IPython already do. Runtime imports a cold Jobqueue dependency there and restores
the caller's SIGINT handler: Jobqueue 0.9 installs one during import. This
bootstrap does not start clusters or wait for farm jobs. Once the dependency is
loaded, background Runtime construction is supported; a cold background
constructor instead reports a clear startup error through `ready()` and rejected
Run receipts. No separate import step is needed in the ordinary workflow.

### Pool authentication

Pooled placements default to `authentication = "tls"`. Hedloom uses Dask's
temporary security support to authenticate both ends of the pool's execution
connections. Scheduler, worker and submit-host clients use credentials generated
for that pool; no manual certificate setup is needed. Install the `pooled` extra
in the environment that starts the Runtime.

Credentials live in a private temporary directory under `records_dir`. That
directory must be accessible at the **same path** on the submit host and farm
nodes, with permissions that exclude other OS users: directory mode `0700` and
credential files `0600`. Orderly close and failed startup remove it. The protection
also requires `records_dir` and its ancestors to prevent other users replacing
those paths. Ownership, filesystem ACLs and UID mapping must preserve that
isolation across hosts. Unsafe writable ancestors refuse startup; a sticky
directory such as `/tmp` is acceptable when its child is owned and protected.
The protection assumes normal OS isolation; processes running as your own account and privileged
administrators are outside that boundary. If authentication cannot be established,
startup fails; it does not fall back to unauthenticated connections.

The explicit opt-out applies to one named pool:

```toml
[placement.pool]
kind = "lsf-pooled"
authentication = "none"
# ... the pool's queue, allocation and capacity settings
```

It can also be supplied for one Runtime through the normal override mapping:

```python
with runtime(site, {"placement": {"pool": {"authentication": "none"}}}) as live:
    print(live.site.placements["pool"]["authentication"])
```

`none` allows unauthenticated execution connections to that pool. Anyone who can
reach its Dask execution listeners can execute code under the worker's account.
The effective choice, including the `tls` default, is visible in
`site.placements["pool"]["authentication"]`. Changing `dashboard` does not change
this choice. See [the execution security boundary](../internals/execution-security.md).

```{warning}
**Pooled batch workers do not have direct owner-death binding.** Orderly Runtime
close cancels their allocations. Normal process-exit cleanup also reclaimed
workers in local fake-farm probes. Extended SIGTERM/SIGKILL probes also observed
automatic command and worker reclamation in about 32.5 seconds: Dask detected
scheduler loss immediately, then allowed its default 30-second executor shutdown
grace. The initial five-second probe had only observed that delay. Immediate
cross-host death and a deadline under network loss are not established guarantees;
verify worker shutdown and farm walltime on the actual farm. See
[farm evidence](first-farm-run.md).
```

`workers` and `max_jobs` are different facts — how many LSF jobs the pool holds
open, and how many invocations may be in flight against it. Usually you want
them equal. With one command resource per worker, fewer gateways leave some
workers idle; more gateways allow commands to wait for pool workers.

### `kind = "in-process"` — the default, and the debugging one

An in-process placement needs Python callables that no TOML can hold. Declare it
in the profile anyway — `submit` supplies the `BoundTransport` from your
authored bodies, and `Site.with_transports(...)` is how a caller adds them by
hand.

## The two numbers, which are about two different machines

| | Means | Sized from |
| --- | --- | --- |
| `[placement.*] max_jobs` | how many of **that placement's** invocations may be in flight | the share of the farm this study may spend |
| `[kernel] threads` | local concurrency on the **submit host** | how much in-process work this host should do at once |

`max_jobs` is **required** for both LSF placement kinds, and it is
deliberately **not** your site's MAX JOB policy. That policy counts every job
running under your user from every source, so declaring all of it here means
your own submissions and hedloom's queue behind each other — and when it is
hedloom that waits, its worker threads are held by `bsub -I` clients that have
not started, so the placement spends its budget on queueing. Leave headroom.

There is no safe default to guess, which is why an uncapped LSF placement is
refused rather than filled in: an arbitrary small number silently throttles a
sweep, and an arbitrary large one authorises more concurrent jobs than the site
permits and more live `bsub` clients than the submit host will carry.

Getting it wrong is cheap, though. LSF is the real authority — declaring more
than it permits just means the excess pends. **The cap is a courtesy rail, not a
correctness requirement**, so ship it conservative and tune it from measured
queue latency. [The first-farm-run ladder](first-farm-run.md#the-ladder) is how
to measure it.

Why each placement's budget becomes a worker of its own, rather than a number
checked somewhere, is [in the internals](../internals/placement-and-scheduling.md).

## `dashboard` — diagnostic HTTP exposure

The Runtime's readiness scheduler and workers communicate through `inproc`
inside the submitting process. They have no network execution listener, including
when a dashboard is enabled. A pool has a separate network-reachable scheduler
and workers; their execution connections use that pool's authentication setting.

`dashboard` controls diagnostic HTTP exposure separately:

* `"none"` — the default; suppresses readiness and pool-scheduler HTTP listeners.
  A pool still needs its authenticated execution listeners. Farm workers can
  retain unauthenticated health and metrics HTTP endpoints without dashboard
  routes.
* `"loopback"` — enables diagnostics on the local host. Other users on that host
  can reach them: loopback is a host boundary, not an account boundary.
* `"network"` — explicitly enables network-visible diagnostics.

HTTP diagnostics are unauthenticated and can expose invocation names, paths,
metrics and profiling information. Mutual TLS on execution connections does not
authenticate HTTP. The execution-route audit and its evidence are documented
in [execution security](../internals/execution-security.md); diagnostic exposure
alone is not evidence of privacy on a shared host.

`none` also avoids optional dashboard imports when an installation's bokeh is
missing or incompatible. Diagnostic settings do not change computation identity,
reuse, or the chosen pool authentication mode.
