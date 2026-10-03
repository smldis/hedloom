# Hedloom Run

Cooperatively admit a validated Plan and execute ready invocations through Dask.

This unit owns dependency readiness, binding, value threading, compatible live
execution sharing and withdrawal. It imports Exec, never the planning package:
Plans arrive as plain documents. Exec owns computation identity, attempt claims,
transport execution, result capture and reuse. The [Hedloom facade](../../docs/index.md)
composes these contracts into the operator-facing Runtime and Run receipts.

## Controller and execution

`hedloom_run.controller.Controller` runs on the Runtime's dedicated async loop.
It receives an async Dask Client, Site capacities and a bounded offload service.
It admits dependencies only when their producer results exist. Input identity
is finalized before lookup in the live execution owner table, so separately
acquired but equal artifacts can converge on one compatible execution.

Dask receives ready invocation wrappers, with `pure=False`, `retries=0`, explicit
placement resources and priority. It executes those wrappers; it does not own
Plan dependencies, computation identity or reuse. The `driver` module retains
`InvocationOutcome` and `RunReport`; blocking `run_plan` and `run_plan_graph`
entry points have been retired. Operator callers use `hedloom.runtime`.

Reports remain in Plan order, independently of completion order. Every consumer
retains its own authored key, candidate inputs and history. `record` and
`try_number` identify the exact Exec selection. Output binding is shared with
the facade through `binding.output_value`, so a downstream input and an exported
port interpret the same recorded result consistently.

## Scheduling and resource ownership

A placement's `max_jobs` bounds executing wrappers in one Runtime. The ready
queue considers higher Run priorities first and rotates equal-priority Runs.
Priority is scheduling metadata, outside computation identity. Pending shared
work uses the highest attached consumer priority. Already-entered execution is
non-preemptive; priorities do not imply an LSF queue priority or a deadline.

Waiting Runs do not each own a thread or worker slot. They can wait with a
receipt when execution is full; there is no default 32-Run cap. Bounded offload
lanes keep source reads, identity preparation and persistence away from the
controller loop. Capacity bounds execution concurrency rather than all memory,
all subprocess resource use or an operator's global farm jobs.

`cluster.async_cluster_for(site)` constructs in-process Dask workers from Site
placement capacities. Every ready wrapper requests `placement:<name>: 1`,
including local work. The facade Runtime owns startup and teardown; a caller
cannot inject an arbitrary Client through its public submission surface.

No task secedes: a worker holding an external command remains occupied. A local
wrapper can wait in `bsub -I` while the actual payload runs on the farm. Its
placement budget bounds active wrappers, including pending direct LSF jobs.

## Sharing, withdrawal and replay

`Controller` owns the live execution table and durable dispatch handles. Compatibility includes
finalized computation identity, implementation, roots, placement and transport
binding. Run priority is outside this key. Completed dispatches are replaceable;
later requests re-enter Exec rather than use the owner table as a result cache.

Each consumer publishes its binding before a new execution can launch. The
worker publishes Exec's selected try and workspace through the handle. Selection
accounting remains Exec-owned; observation errors do not redefine computation.

An entered handle never re-enters Exec. Dask retries are disabled and a worker
replay refuses explicitly. Stopping one consumer does not cancel another's
shared execution. With no consumers, the durable entry gate prevents unentered
work from running; entered work settles and retains truthful evidence. This is
withdrawal, not forced termination of an arbitrary Python body or command.

Explicit `stop(force=True)` can escalate withdrawal and interrupt solely owned
pooled commands. The pooled waiter observes durable intent, freezes worker
admission and removes scheduler interest before a nanny-managed restart. Exec
publishes the confirmed cancelled attempt while the outer invocation stays alive
to report it. The allocation survives and its new worker restores capacity.
Queued cancellation preserves unrelated executing work; shared executions remain
alive for their other consumers. Other placements still drain. Pool commands use
Linux SIGKILL immediate-child binding; detached descendants are outside it.

Cross-Runtime active joining and recovery after process death are unsupported.
Exec claims remain the backstop across independent owners.

## Sites and placements

`Site` holds independent `records_dir`, `runs_dir` and `work_dir`, address spaces,
placement transports, concurrency and retention. Relative TOML paths anchor to
the profile. These are execution configuration, never Plan-owned machine paths.

Direct LSF forwards per-invocation queue, cores, memory and licences to the farm.
Pooled LSF uses separately allocated `dask_jobqueue.LSFCluster` workers. The
readiness worker holds a client plugin to the pool; a live Client never travels
inside serialized transport data.

Runtime readiness communication is process-local `inproc`. Networked pools
default to per-pool `authentication="tls"`, using Dask temporary Security and
Jobqueue worker credentials. The plugin receives Security separately from the
transport. Credentials use a private temporary directory (`0700`) under
`records_dir`, with files `0600`; farm nodes must see the same paths with account
isolation. Orderly close and failed startup remove that directory. Missing
authentication support fails startup rather than selecting an unprotected mode.

The explicit per-pool opt-out is `authentication="none"`, through Site or a
Runtime override, visible in effective `Site.placements`. Diagnostic exposure
is separate: `dashboard="loopback"` or `"network"` enables unauthenticated HTTP
and never makes loopback private to one user. See
[execution security](../../docs/internals/execution-security.md) for the accepted
requirement, opt-out consequences and verification limits.

Each pooled worker offers one `hedloom-command` resource: one command at a time,
regardless of its allocation's CPU count. Invocation cores and memory must fit
that allocation. Licences, incompatible pool options and unknown requirements
refuse rather than disappear. `workers` controls allocated farm jobs;
`max_jobs` caps gateway wrappers. Different numbers can allow waiting wrappers
or idle pool workers; they are not interchangeable resource requests.

Pool workers are ordinary batch jobs. Orderly close and normal process-exit
cleanup reclaimed allocations in fake-farm probes. Extended SIGTERM/SIGKILL probes
on Dask/distributed 2026.7.1 also reclaimed active commands and fake workers
automatically in about 32.5 seconds. Scheduler loss was detected immediately;
Dask's default shutdown then gave active executor threads a 30-second grace.
Batch workers have no direct OS parent-death binding to the submitter; these
observations do not establish cross-host detection or cleanup deadlines.
The facade bootstraps a cold Jobqueue dependency on the main thread and restores
SIGINT; cluster creation and scaling stay on its controller loop.
Local/fake-farm evidence does not establish real-farm resource enforcement or
remote shared-filesystem guarantees.

## Failure and boundaries

A failed producer blocks its dependents. `stop_on_failure=True` stops other
pending admissions and settles entered work; `False` lets independent branches
finish. A stopped or rejected Run is distinguished from successful completion
by the facade lifecycle, even if its partial report contains no failures.

Bodies execute work; they do not submit child Plans. Worker-held nested
submission is unsupported in the initial async replacement. Caller-level staged
Runs support dynamic fan-out today; future hierarchical use cases remain open.

Sources resolve on the submitting machine. Shared paths are assumed to denote
the same payload at the execution host. This unit performs no data staging,
remote history service, automated retries or result-dependent in-Plan control.

```{toctree}
:maxdepth: 1

api
```
