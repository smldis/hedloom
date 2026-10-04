# Hedloom Run Ontology

This is the ongoing self-study of the component rooted here. Its account
separates current commitments, observed evidence, and possibilities.

## Purpose and scope

Hedloom Run joins a static validated Plan to durable executions. It owns
readiness, selected-output threading, binding compatibility, consumer sharing,
and the effect of failure or withdrawal on remaining work. Exec owns computation
identity, record selection, attempts, reuse, transports and artifacts. Flow owns
the Plan. Run imports neither the facade nor Flow.

The August 2026 Dask kernel supplied concurrent execution and placement
capacity. Selected artifact identity subsequently moved dependency admission
back onto Run: a consumer identity cannot be finalized until its producers have
selected their artifacts. The October 2026 async replacement preserves that
boundary. Dask receives ready work rather than unresolved dependent tasks.

## Mode of being

**Development state:** `prototype`

On 2026-10-01, user direction authorized replacement of the blocking execution
paths, reconsidering uncalibrated resource restrictions and retaining automatic
owner-bound lifetime. `controller.Controller` now owns readiness on the facade
Runtime's one background asyncio loop. `driver` contains immutable reports;
`graph` contains serializable worker helpers. The synchronous `run_plan` and
`run_plan_graph` APIs, sequential mode, synchronous owner table, and worker-held
nested scheduling were retired. This is an explicit architectural change,
without changing static Plan interpretation or Exec identity.

The new controller tests exercise one-slot dependency progress and reuse,
staggered sharing, independent withdrawal, stopping during durable binding,
entered-work drain, failure propagation, priority selection, more than 32 waiting
receipts, and binding-error settlement. Migrated source, file and placement tests
exercise the same async path through a test-only synchronous harness. These are
local and fake-farm observations. The Run suite also passes with matching
Dask/distributed 2023.9.2 and 2024.8.0 supplied by cached-package overlays under
Python 3.11.16, alongside the ASS venv's 2026.7.1. These do not establish actual farm scheduling,
NFS across hosts, or arbitrary process termination.

## Current contracts

- One loop owns active Runs and compatible execution entries. Coordination uses
  cooperative tasks, not one controller thread per Run. `ControlRun.wait()`
  observes a report without cancelling execution when its waiter is interrupted.
- Each Run retains authored report order and individual consumer provenance.
  Static dependencies are admitted only after successful predecessor outcomes;
  failed dependencies block consumers without entering Exec. No branch, retry,
  fallback or result-dependent expansion is introduced.
- Finalized computation identity plus execution binding, placement, roots and
  output contract determines sharing compatibility. Scheduling priority is
  excluded. Fresh `each_submission` handles contribute their acquisition identity
  before compatibility is computed. Equal selected output identities may allow
  downstream consumers to converge even when their producer acquisitions differ.
- Each consumer's durable execution link is awaited before its attachment may
  authorize dispatch. A slow or failed link does not silently submit its body.
  Preparation, hashing, serialization, gate operations and record projection use
  the Runtime's owned offload facility, avoiding filesystem waits on the loop.
- There are at most two concurrent preparation operations. Each Run prepares
  at most a placement-capacity window of work; a large static Plan does not
  eagerly materialize all its execution handles. Waiting Run receipts have no
  fixed admission count. Their retained Plan and result memory is still real
  resource use, rather than a promise of unlimited memory.
- Per-placement outstanding Dask executions are bounded by that placement's
  configured capacity. Ready nominations consider higher numeric Run priority
  first and rotate equal-priority Runs. Dask receives the same numeric priority,
  zero-millisecond FIFO grouping, declared placement resources, `pure=False`,
  and `retries=0`. This describes dispatch opportunities, not preemption, equal
  CPU time, starvation prevention, or LSF queue priority.
- Shared pending executions use the highest attached consumer priority. An
  already submitted Dask Future is not dynamically reprioritized. Computation
  identity never changes because an operator chooses urgency.
- Entry and withdrawal use the same durable `ExecutionHandle` gate. An entered
  handle never re-enters Exec, including on Dask replay. With other consumers,
  withdrawal reports this consumer cancelled while shared work continues. With
  no other consumer, the gate prevents unentered work; entered work is drained
  and its actual outcome reported. Worker stack snapshots do not establish that
  execution never happened.
  Explicit force withdrawal persists interrupt intent for solely owned pooled
  executions, and can escalate an ordinary drain. A committed interrupt seals
  later sharing until that execution settles. The existing pooled waiter pauses
  worker admission, distinguishes resource-waiting commands from executing work,
  removes scheduler interest before restarting a nanny, and returns confirmed
  cancelled evidence for Exec to publish. The outer invocation Future stays
  alive for accounting. Queued cancellation preserves another executing command;
  worker restart retains its allocation. Pool commands select Linux SIGKILL
  immediate-child owner binding, including commands that ignore SIGTERM.
  Other placements still drain; detached descendants and real-farm deadlines
  remain outside the verified contract. Indeterminate restart is not cancellation.
- `stop_on_failure=True` requests withdrawal of remaining owned work after a
  failed outcome; already entered work drains. `False` permits independent
  branches to continue, while failed dependencies remain blocked. A controller
  or binding exception settles its receipt with an inspectable partial report
  and error, rather than leaving a waiter indefinitely pending.
- Output delivery is distinct from computation outcome. The actual selected
  result and exact record/try are retained before projecting outputs. Delivery
  failure is a Run coordination error with a visible observation diagnostic;
  it blocks further admission without inventing computation failure. A child
  is ready only after its selected inputs were successfully delivered.
  Unrelated Runs continue. A catastrophic controller failure withdraws and
  drains entered work before making receipts terminal.
- Withdrawal of a large Plan uses one cooperative outcome-projection coroutine
  per Run, yielding periodically; it does not create one task per blocked node.
- `Controller.close()` requests withdrawal and drains its Runs. The facade owns
  automatic Runtime lifetime and orderly close, and closes readiness clients
  before pooled resources. Run neither creates a cluster in its controller nor
  takes ownership of an arbitrary supplied client.
- Bodies cannot submit nested Runs. The worker occupancy marker lets the facade
  refuse this explicitly instead of deadlocking. Hierarchical use cases are
  deferred for a future design; they are not permanently excluded.
- Consumer outcomes retain Exec's exact `record` and `try_number`. Public
  `disposition="reused"` projects Exec's completed selection. Publication and
  handle-accounting failures are visible as `observation_errors`; they do not
  redefine successful execution. Blocked work has a `block_reason` separate from
  computation errors. Per-consumer names do not enter Exec computation identity.
- Binding honors each invocation's resolved placement. Missing transports
  refuse rather than fall back. Invocation resource options travel to transport
  without entering computation identity. File outputs contribute their recorded
  addresses; other ports contribute named values. `binding.output_value` is
  shared with the facade so downstream input and caller export interpretation
  agree.
- Site owns independent `records_dir`, `runs_dir`, `work_dir`, address spaces,
  placement capacities and retention policy. Profile-relative paths are anchored
  at load. Source fingerprints identify content and source addresses deliver it;
  omitting fingerprints can reuse stale evidence after an in-place source edit.
  The submitter assumes resolved addresses mean the same thing on executing
  hosts; this unit provides no staging mechanism.
- Cluster construction derives worker resources and capacities from the same
  Site. Runtime readiness communication is always process-local `inproc`, without
  a network execution listener. Diagnostic exposure is explicit: `none` suppresses
  readiness and pool-scheduler HTTP listeners; `loopback` binds locally; `network`
  opts into exposed diagnostics. Farm workers can retain health/metrics endpoints.
  HTTP is unauthenticated and loopback is shared by users of a host.
  Runtime dashboard links derive their host from diagnostic exposure, not the
  inproc readiness address: loopback uses `127.0.0.1`, retaining Dask's URL
  template and assigned HTTP port. A local regression opens the advertised URL
  after Study completion while the Runtime remains open.
  A transport copied to a worker must be serializable; diagnostics name the
  failing placement.
- The composed execution-isolation requirement adopted on 2026-10-03 excludes
  other OS users and unauthenticated network clients from submitting or triggering
  arbitrary code through owned execution resources by default, assuming ordinary
  OS isolation. Networked pools default to per-pool mutual TLS using Dask temporary
  Security and Jobqueue worker credentials. Private runtime credential directories
  under `Site.records_dir` are `0700`, files `0600`, and must be shared at identical
  paths with account isolation, including safe ancestors and consistent UID/ACL
  enforcement. Orderly close and failed startup clean them up.
  Missing support must fail closed. `authentication="none"` is the explicit
  per-pool opt-out, visible in canonical `Site.placements` and supported Runtime
  overrides. The worker plugin carries Security separately from transport settings;
  authentication is outside computation identity. Dashboard exposure does not
  weaken execution authentication. Same-account processes, administrators and
  unauthorized modification of OS isolation are outside the boundary. The
  [maintained security account](../docs/internals/execution-security.md) separates
  this requirement from HTTP audit evidence and actual-farm assumptions.
  Owned TLS contexts tolerate Dask's missing close notification through OpenSSL's
  unexpected-EOF option, retaining peer-certificate verification and encryption.
  Dask's length-delimited reads remain responsible for rejecting truncated frames;
  this is a scoped compatibility choice around the transport dependency.
- Pool workers are LSF allocations. One command reserves a whole pool worker's
  command token, independently of its Dask thread count. Per-invocation CPU and
  memory requests must fit that allocation; unsupported or incompatible farm
  requests refuse before command submission. Site `max_jobs` bounds gateway
  executions and may exceed pool worker count; pool `workers` determines LSF jobs.
  Commands receive Run priority. Force interruption removes scheduler interest
  before intentional worker loss, preventing replay of the cancelled command.
  Its temporary worker pause owns admission until acknowledgement: the memory
  monitor's automatic resume is suspended during that fence and its original
  pause threshold restored on matching-owner cleanup. Spilling and Nanny memory
  termination continue. TLS regression checks exposed this competing pause
  ownership; the bulk-stop test exercises the monitor's resume attempt directly.
  A checkpoint connection loss retries before withdrawal only after the
  scheduler confirms that exact worker address has disappeared, repeating the
  ownership and location checks. A failed RPC to a still-present worker or an
  absent task does not establish cancellation.
  Assignment loss is recorded on the existing Dask task before the owned pool
  scheduler removes its worker. Unknown losses remain uncertain; only an
  acknowledged owned restart certifies its exact worker loss. Pending evidence
  waits within the interruption deadline. This preserves never-assigned queues
  and bulk interruption while refusing false cancellation after disconnection.
  Task-local admission holds prevent a replacement worker admitting queued work
  before its restart evidence or pending force withdrawal settles. Exact request
  cleanup releases its own hold and reconsiders ready work through Dask.
  Unconfirmed restart evidence remains quarantined; affected Runs can keep
  waiting until force-withdrawn, even when a replacement is live. Automatic
  recovery from uncertain physical termination is outside this prototype.
  Scheduler pause release requires the same key owner, and stale running-status
  messages cannot clear an owned fence.
  The dispatch gate prevents re-entry into Exec; it does not guard the separate
  pooled command task against rescheduling after unexpected worker loss.
  `retries=0` is not a worker-loss replay policy. That wider pool guarantee
  remains unresolved rather than being implied by the wrapper's entry gate.

## Experience and possibilities

The replacement makes an important distinction visible: an execution capacity
limits work entering Dask, while accepting another waiting study need not consume
another thread or farm allocation. Retained Plans still consume memory. The old
proposal's fixed 32-Run cap conflated these responsibilities and was contested by
the operator; the implementation uses derived execution and preparation windows
without introducing a configurable budget framework.

Sharing also means urgency belongs to consumers rather than computation. A
pending shared execution can inherit their highest urgency without creating a
second computation; priority after Dask submission and starvation under sustained
high-priority arrivals remain explicit limitations. Real workloads should decide
whether those limitations warrant additional scheduling machinery.

Pool connectivity is execution authority: a reachable scheduler is not merely
a monitoring endpoint. Per-pool authentication preserves the existing split
between process-local readiness and networked command execution while making
the latter's trust boundary explicit. Private shared paths carry temporary
credentials without a new staging or certificate-management responsibility.
Whether a farm supplies that filesystem isolation remains deployment evidence,
not something a local fake proves.

The [dated formal investigation](../design/formal/async-interruption/README.md)
exposed that scheduler disappearance can precede physical command termination.
Local public-API probes confirmed the false cancellation at the earlier revision.
Termination evidence now belongs to the owned pool scheduler's task lifetime,
without turning it into a durable history or a separate recovery service.
Bounded model checks inform this boundary; they do not prove the full runtime.

## Contribution to the parent

Run supplies the static readiness and recorded execution join used by the
facade's managed nonblocking Runtime and Run receipts. Its durable dispatch
format remains distinct from facade consumer history and Exec records.

## Exclusions

Run owns no authoring, computation journal, reuse policy, artifact capture,
result-dependent control, automatic worker replay, detached execution,
cross-Runtime joining, or restart recovery. It does not promise global user or
host quotas, real-farm fair share, command preemption, or cross-host filesystem
synchronization. Automatic process lifetime is a composed facade/transport
contract, not a property established merely by an asyncio controller.

## Child composition

There are currently no child units.
