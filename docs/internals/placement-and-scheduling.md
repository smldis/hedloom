# Placement, clustering, and scheduling

The operator guide is [Sites and placements](../guide/sites.md). This page
explains how the execution layers compose.

```text
Plan: operations, dependencies, resolved placement
    -> Runtime-owned async Run controller: readiness, sharing, priority
    -> local Dask executor: ready wrappers, placement tokens
    -> BoundTransport: authored body and its command
        local          -> owner-bound subprocess or Python body
        lsf-interactive -> bsub -I -> LSF queue and allocation
        lsf-pooled      -> pool Client -> command on allocated LSF worker
```

## Placement and clustering

Placement resolves during planning: call override, operation default, Plan
default, then local. Site maps the resulting name to a transport and capacity.
Supported invocation options override direct transport defaults. A declared need
that the transport cannot express refuses before launch.

`Site.cluster_spec()` and `async_cluster_for(site)` construct one in-process
Dask worker per placement. Its threads and `placement:<name>` tokens both derive
from the Site's capacity. Every executing wrapper consumes one token, including
local work. A wrapper waiting on a direct LSF job continues occupying capacity;
no `secede()` hides that work from the budget.

The controller runs separately from these worker threads. Blocking preparation,
filesystem operations and persistence use bounded offload lanes. Waiting Runs
hold receipts and Plan state, not a controller thread per study. Executor
capacity does not bound all retained state or enforce CPU and memory quotas on
arbitrary authored subprocesses.

## Scheduling levels

1. Run resolves dependencies and artifact identity, then chooses compatible
   work for dispatch. Higher Run priorities precede lower eligible work;
   equal-priority Runs rotate. Scheduling priority is outside computation identity.
2. Dask orders ready wrappers with explicit resource requirements, forwarded
   priority and FIFO settings. It cannot preempt an entered Python body or shell
   command. A controller queue and a Dask queue must both respect priority;
   round-robin nomination alone does not guarantee round-robin execution.
3. Exec selects reuse or a try under the record claim. It neither knows Run
   priority nor imports Dask.
4. Direct LSF schedules submitted jobs under farm queue, resource, licence and
   user policies. Hedloom's priority does not become an LSF job priority.
5. A pool schedules commands inside already allocated worker jobs. Forwarded
   Dask priority affects waiting commands, not the farm's allocation order.

Live shared executions use the highest attached consumer priority while pending.
Once dispatched or entered, later priority changes provide no preemption
promise. A sustained stream of higher-priority work can delay lower-priority
Runs. Fairness means dispatch opportunities among eligible equal-priority Runs,
not equal CPU time or a bounded wall-clock delay.

## Pooled command resources

A pool worker advertises `hedloom-command: 1`, so it runs one command at a time.
This avoids treating a four-core allocation as permission to launch four
four-core commands concurrently. Per-command core and memory needs must fit the
allocation; licences, incompatible pool options and unknown requirements refuse.

`workers` sizes allocated LSF jobs. `max_jobs` bounds active local gateway
wrappers, including commands waiting for the pool. If the numbers differ, pool
availability and gateway capacity jointly determine throughput. Neither is a
host-global quota or evidence that the farm enforces requested resources.

Worker-local pool clients are constructed by `PooledClientPlugin`. Transport
objects travel as serializable configuration rather than carrying a live Client.
The Runtime bootstraps cold Jobqueue on the main thread, preserving SIGINT,
then creates and owns pools on its loop. Orderly release and normal exit cleanup
reclaim allocations. Extended SIGTERM/SIGKILL probes on Dask/distributed 2026.7.1
also reclaimed active commands and fake workers automatically in about 32.5
seconds. Scheduler loss triggered immediate worker shutdown, followed by Dask's
30-second executor grace; the earlier five-second check observed this delay.
Cross-host detection and cleanup deadlines still require farm evidence. See
[real-farm limits](../guide/first-farm-run.md).

## Stopping and future nesting

Stop withdraws a consumer and prevents new admissions. A shared execution stays
live for other consumers; entered work settles. Durable entry gates prevent
unentered orphaned dispatches from launching and reject automatic replay.

Cancelling a Dask Future does not interrupt an already executing synchronous
worker thread or terminate the subprocess it started. `Run.stop()` therefore
drains entered work. `Run.stop(force=True)` additionally interrupts solely owned
pooled commands using Dask's existing `Client.restart_workers()` mechanism.
Shared executions continue for their remaining consumers; other placements and
entered Python bodies still drain. An ordinary stop can be escalated while the
Run is pending. Late consumers cannot attach after an interruption is committed.

The controller records interrupt intent through the durable execution handle.
The existing submit-host pooled waiter observes it, freezes worker admission,
and checks actual worker execution rather than treating scheduler assignment as
entry. It withdraws scheduler interest and confirms cancellation before restarting
the nanny-managed worker. Queued commands can be cancelled without a restart;
unrelated executing work prevents a destructive restart. Restart restores capacity
within the same allocation, avoiding a new batch queue wait. Exec's normal
reconciliation publishes the confirmed cancelled outcome and exact record/try;
the outer invocation Future remains alive to deliver this evidence.

This suppresses replay during intentional force cancellation. Unexpected pool
worker loss can still make Dask reschedule the separate command task;
`retries=0` does not prevent that. The durable dispatch gate protects re-entry
into Exec, rather than the remote command's entry. General pool worker-loss
replay prevention remains an unresolved contract.

Pool commands use Linux SIGKILL parent-death binding so their immediate process
cannot survive worker loss by ignoring SIGTERM. Detached descendants are outside
that binding. Interruption requires a supported nanny-managed Linux worker;
errors or unconfirmed restart remain visible rather than fabricated cancellation.
An uncertain restart leaves the old worker paused rather than permitting overlap
with a possible replacement; pool capacity can therefore remain unavailable.
Force-withdrawing remaining Runs and closing their Runtime releases the owned
allocations instead of attempting automatic recovery of indeterminate work.
Closing the whole pool releases Runtime-owned farm allocations. Local fake-farm
evidence does not establish real-farm termination or capacity-restoration deadlines.

Blocking kernels, sequential mode and worker-held nested submissions are retired.
Known graphs compose in one Plan; dynamic graphs use caller-level stages between
Runs. Future hierarchical use cases remain deferred rather than excluded in
principle. This avoids keeping worker slots occupied by a parent waiting for
its child.
