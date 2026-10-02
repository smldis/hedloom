# Interactive execution: replacement proposal and implementation plan

Date: 2026-09-29. Status: recommendations for a prototype replacement; **not
implemented and not a record of user acceptance of the detailed design**.
Evidence: [internals census](interactive-execution-internals-census-2026-09-29.md).
Discussion input: [concept brief](interactive-execution-redesign-2026-09-29.md).

## Recommendation and decision boundary

Build one Runtime that owns a fixed dedicated background thread and its asyncio
loop. Run dependency progression as cooperative tasks on that loop. Retain Dask
as the executor of ready invocations, with an asynchronous client and local
cluster sharing that loop. Keep synchronous attempt execution on explicitly sized
Dask worker pools; do not convert Exec's durable protocol to async merely to make
submission interactive. Retire the blocking facade and Run entry points, including
the no-Dask sequential engine. A one-slot Runtime is the small-study path.

The user has accepted staggered submissions with explicit ownership, clean
replacement without legacy blocking support, and the dedicated-thread hosting
direction. Caller-loop hosting was discarded. A separately hosted controller
was excluded from this discussion. Neither is reopened here. Transport subprocesses
and existing farm workers are execution resources, not alternative controller hosts.

Everything below about names, Dask, API details, budgets, staged studies, and close
policy is my recommendation. The census establishes code facts separately. The
proposal changes some existing contracts explicitly; it does not treat their
current shape as an immutable constraint. No contradiction makes the requested
hosting direction impossible. The handoff's old history-schema number is an
evidence correction (current code is schema 3), not a contradictory requirement.

### Why this boundary

The need is independent run lifetimes and bounded shared resources. A simpler
executor candidate is one explicit thread pool per placement plus the same
controller, retaining Dask only for farm pooling. That is credible: Run already
owns dependency readiness. I recommend **not** selecting it in the first
replacement because it would additionally replace the exercised placement,
serialization, diagnostics, and pool-client integration while the run lifecycle
is changing. Making Dask the sole executor removes the dual-path burden without
rewriting those mechanisms. This is a prototype tradeoff, not a claim that Dask is
intrinsically required by the problem.

Conversely, making the entire attempt/transport protocol async now would remove
one waiter thread per direct job, but expands the change into its strongest
ordering and failure contracts. Existing bounded worker threads are acceptable
execution resources. Their number is determined by placement capacity, never by
the number of Runs. No implicit default executor is acceptable for controller I/O.

## Decision table

| Question | Recommended answer | Rationale / evidence | Consequence | Verification needed |
| --- | --- | --- | --- | --- |
| What owns work? | Explicit `Runtime`, identifiable `Run`, immutable planned `Study` | Current Session owns resources but not a live public run registry | No ambient singleton or implicit runtime per submission | Overlapping and late submissions through one Runtime |
| What does submit return? | Immediate in-memory Run receipt; durable acceptance is a separately observable milestone | Filesystem preparation/fsync can block indefinitely; hiding it inside submit defeats prompt responsiveness | `.submission_id` exists immediately; `.run_id` is absent until durable acceptance | Slow/failing storage and interruption before acceptance |
| Where is the controller? | Exactly one dedicated thread/asyncio loop per Runtime | User direction; local async-Dask probe succeeded | Runs add tasks, not controller threads | Staggered runs, idle foreground, thread accounting |
| Dask integration? | Async Client and SpecCluster on that same loop | Official async API and local probe | No synchronous Client `.result()` or `sync` calls on controller | Real attempt worker, startup/close, loop identity |
| No-Dask path? | Retire it; one-slot Dask Runtime for small studies | Same `_run_ready` today; optional engine duplicates lifecycle cases | Distributed becomes a Run dependency; Flow/Exec stay independent | Install/import boundaries; one-slot identity and reuse |
| Placement capacity? | Dask resource tokens remain sole execution-capacity authority | Existing every-placement annotation tests | Controller window limits queued work/memory, not a second farm semaphore | Independent jobs across multiple Runs never exceed caps |
| Ready work fairness? | Round-robin Runs, stable Plan order within each; bounded outstanding window per placement | Current per-run loops eagerly submit and have no cross-run fairness | Late arrivals get admission opportunities; no preemption | Saturate A, add B, observe next free window turns |
| Blocking I/O? | Explicit bounded preparation, storage, and observation lanes | Source hashing, fsync, Git and watcher calls are synchronous | Storage latency can delay evidence, not monopolize the loop | Slow lanes, queue limits, stop/status responsiveness |
| Sharing? | One runtime-local table, finalized identity plus execution compatibility | Current owner handles and generation-safe release work | No cross-runtime joining or claim-wait service | A/B sharing, different binding refusal, old-generation release |
| Cancellation? | Public `stop()` withdraws consumer demand; do not promise hard kill | Entry gate is trustworthy; record claim prevents easy concurrent hard cancellation | Last entered execution drains; remaining consumers retain it | Gate races, shared stops, observed success after stop |
| Waiting? | Explicit `wait`, `result`, and `accepted` on Run | Script ergonomics are a new API, not legacy engine support | Timeout/interrupt affects caller wait only at the API level | No implicit stop; controller-thread waits refuse |
| Runtime close? | Drain by default; explicit stop mode; timeout leaves `CLOSING` and ownership intact | Arbitrary Python/transport calls cannot be safely killed as threads | Explicit lifetime; no false successful close | Close with entered work, timeout, retry close, startup failure |
| Nested submission? | Refuse inside operation execution; replace maintained cases with caller-level staged runs | Worker-held waits and sequential escape already cause friction; staged fan-out is the actual capability | Remove global headroom counters and imported Session singleton | Two-stage one-slot study preserves per-corner reuse |
| Reload and globals? | Pin exact body versions and preparation evidence; constrain mutable dependencies | Current old-Plan binding exists, globals are not frozen | New definitions can serve later Runs; no hermetic execution claim | Edit same-origin operation while an older Run is queued |
| Saved data? | Keep record layout 1 and execution-handle schema 1; new history schema 4 plus read-only schema-3 reader | Current history is schema 3; lifecycle meaning changes | No rewriting old data, no runtime compatibility engine | Mixed old/new history and exact selected references |
| Retention? | Runtime-owned, coalesced maintenance after terminal history publication, when execution is quiescent | Current per-run scans run synchronously and can overlap | Preserve named rules; defer maintenance while Runs remain active | Shared active work plus maintenance/new-admission race |
| Pools? | Retain current separate pool substrates initially; fix async lifecycle integration | Real code uses local gateway plus remote pool; only fake-farm evidence | Account for pool schedulers/clients and gateway waiters explicitly | Async startup/teardown with installed dask-jobqueue and fake farm |
| Host loss? | Preserve evidence, expose incomplete/unknown state; no recovery service | Owner-bound direct work and pooled work have different guarantees | No invented terminal state from vanished future/host | Local process tests; real-farm guarantees remain external evidence |

## Proposed public contract

All spellings here are proposed. `Study` remains authored Plan plus implementations.
`Runtime.open(site, limits=..., watch=False)` is an explicit potentially blocking
resource-initialization call. It returns only after the controller, local cluster,
configured pools, and basic storage access checks are ready, or raises a startup
error after attempting cleanup of **all** resources already opened. The caller
can inspect the failure's cleanup diagnostics. It does not silently remove a
placement or switch executor.

```python
from hedloom import Runtime, RuntimeLimits, Site

rt = Runtime.open(Site.from_file("site.toml"),
                  limits=RuntimeLimits(active_runs=32))
a = rt.submit(first_study(), name="first")   # bounded local registration only
# Prompt returns. Runtime progresses independently of prompt input.
a.submission_id                             # UUID receipt, available immediately
a.snapshot()                                # immutable state/progress snapshot
b = rt.submit(second_study(), name="later")  # same placement budgets
b.accepted(timeout=5)                        # optional: durable RunReference
report = a.wait()                            # waits; returns terminal RunResult
if report.succeeded:
    print(report.outputs["answer"].value)
rt.close()                                  # explicit drain and resource release
```

A script can use the same engine:

```python
with Runtime.open(site) as rt:
    runs = [rt.submit(make_study(x), name=f"case-{x}") for x in cases]
    results = [run.result() for run in runs]
    # result() raises RunFailed for non-success; exception carries RunResult.
```

This context manager drains on normal exit; on exceptional exit it requests stop
for its Runs, then waits for their safe settlement. Close failures are attached
as diagnostics without replacing an exception from the body of the block.
No bulk API is needed initially: submission itself is nonblocking. An oversize
batch receives `RuntimeBusy` at the admission limit rather than an unbounded
queue of accepted responsibilities.

### Registration, acceptance, preparation, execution

1. **Local registration:** validate cheap arguments; reserve active-run/Plan-size
   allowance under a short bridge lock; copy the implementation mapping, retain
   the immutable Plan, capture caller working directory and selected context;
   allocate `submission_id`; enqueue the command and return a Run. No source
   reads, Git, cloudpickle, fsync, or cluster RPC runs in this call. The Runtime
   now owns that receipt in memory. Capacity or closed-runtime refusal raises
   synchronously and creates no Run. Planning `make_study()` remains caller work.
2. **Durable acceptance:** after bounded off-loop Plan encoding/size validation,
   the controller asks the storage lane to allocate
   `<name>.<occurrence>` using the existing no-overwrite allocator and persist a
   minimal immutable header, exact Plan, address table, and accepted event. The
   header includes the submission UUID and runtime identifier. Only successful
   publication resolves `.accepted()` and sets `.run_id`/`.reference`.
   The UUID is a receipt identity, not a new computation digest or directory tree.
3. **Preparation:** bounded preparation jobs capture source fingerprints and
   addresses, reproducibility and callable envelopes; validate used placements,
   body availability/serialization, and bindings. Persist evidence before any
   execution dispatch. Initial reproducibility remains mandatory by default.
   Preparation failure is a durable failed Run with zero entered executions.
4. **Execution:** the run joins runtime admission after successful preparation.
   Invocation identity still finalizes after selected upstream artifacts exist.
   Acceptance never means a farm job was submitted or that all identities are
   already known.

A storage failure before acceptance produces a terminal `REJECTED` receipt with
`AcceptanceError`; partial allocation remains discoverable as incomplete
preparation and is not reused as a new run. `.accepted()` raises; `.wait()` returns
the rejected RunResult and diagnostics; `.result()` raises `RunFailed`. The Run
is still inspectable in memory. After acceptance, preparation/engine failures
produce terminal `FAILED` with a failure phase and partial report. Ordinary
operation failures also appear as failed invocation outcomes. No background
exception is silently printed and forgotten.

`wait(timeout)` returns a RunResult for every terminal state, including failed or
stopped. `result(timeout)` uses the same wait but raises for non-success. The
result retains named output projections, exact record/try references, and
separate persistence diagnostics. Evaluation values such as `passes=False` do
not themselves fail execution. `snapshot()` is a bounded in-memory read and never
rescans the filesystem. `RunHistory` remains the durable, independent-process view.
No general user callbacks execute on the controller.

## Thread, loop, bridge, and task ownership

The facade owns Runtime lifetime, receipts, body capture, user results, history,
and reproducibility. A Run-unit `Controller` owns dependency state, admission,
execution sharing and executor integration. It knows neither decorated Study
builders nor consumer-history format: facade supplies an async persistence port
for acceptance/binding/outcome events. Exec continues to own computation identity,
claims, tries, reuse, transport and artifact publication and imports no Dask.
Flow's Plan schema and static-authoring role do not change.

Suggested modules: facade `runtime.py` and `run.py`; Run-unit `controller.py`,
`executor.py`, and `report.py`. These are concrete ownership cuts, not a new plugin
framework. Move/reuse binding and protocol code rather than copying it.

```text
caller threads                      fixed Runtime controller thread
  submit / stop ---- bounded ---->  asyncio loop
  snapshot      <--- immutable ---    per-run task registry
  wait Future   <--- completion --    ready queues / shared execution table
                                      | await explicit I/O futures
                                      | await Dask futures
                                  async Client + local SpecCluster (same loop)
                                      |
                                  bounded placement execution threads
                                      | Exec protocol / body / transport wait
                                  local subprocess / direct job / existing pool
```

The thread starts `asyncio.Runner` (Run package should adopt Python >=3.11, matching
the facade) and an explicit root coroutine. Construct `SpecCluster(...,
asynchronous=True)` and `Client(..., asynchronous=True, set_as_default=False)`
inside that running loop. Close them with awaited methods before closing the loop.
Tornado integration uses the current asyncio loop, as the local probe verified;
there is no new Dask synchronous LoopRunner required for the controller client.

**Cross-thread delivery:** a bounded `queue.Queue` stores submission commands;
`loop.call_soon_threadsafe` wakes a drain callback only on the empty-to-nonempty
transition. The callback processes a limited batch and yields. Close and per-Run
stop use coalesced flags in a control mailbox, so a full submission queue cannot
prevent shutdown. At most one pending stop flag per admitted Run and one close
flag exist. These flags are requests; only the controller mutates run state.

Completion delivery has a separate reserved channel: one slot per distinct
outstanding execution, reserved with its dispatch window slot. Ingress saturation
cannot discard a completion or prevent the event that releases capacity. Publish
the canonical result once, wake affected Run tasks with coalesced Events, and
process consumer projections in bounded batches. There is no unbounded queue of
one callback per waiting node.

A private `concurrent.futures.Future` carries durable-acceptance completion and
another carries terminal completion to each Run handle. Callers cannot obtain
and cancel these futures. Snapshot publication uses a short lock and detached
immutable data. Set results only after releasing locks. Do not expose
`Future.add_done_callback` as a user extension point: synchronous callbacks can
run in the thread that completes that future and stall the controller.

Use strong-reference registries for per-run tasks and per-shared-execution
supervisors. A task completion handler always retrieves errors and translates
them into runtime/run diagnostics. One Run failure does not trigger a TaskGroup
cascade cancelling unrelated Runs. Explicit task names include runtime, run and
execution identifiers. Retire completed internal task/Plan/value state after
terminal snapshot publication; a retained user handle may retain its result at
the caller's cost. Runtime does not retain every completed Study forever.

Controller mutable state needs no `threading.RLock`; it has one mutator. Across
awaits use explicit states (for example `BINDING` and `DISPATCH_PENDING`) and
per-entry supervisors, not assumptions that a multi-step transaction remained
atomic. Keep cross-process durable file gates. Controller-side gate calls run in
the storage lane. Never hold a Python bridge lock while waiting on I/O or a Future.

`wait/result/accepted/close` refuse blocking use from the controller thread and
from an operation execution context. Public submission from an operation also
refuses (staging policy below). Low-level coroutines remain internal. Supporting
an awaitable external Run is unnecessary for the initial public API and is not
caller-loop controller hosting; it can be evaluated separately if a consumer
needs it. This proposal does not add it by default.

Capture `contextvars.copy_context()` at registration for preparation jobs, copying
it separately per job. Pass explicit small trace/run metadata into worker bundles;
do not assume Dask propagates arbitrary ContextVars. Never use context to smuggle
in resource ownership, operation identity, or a mutable Session singleton.

Progress is pull-based snapshots plus a bounded ring of notifications with a
sequence number and dropped-notification count. Durable outcome events do not
use that lossy ring. A caller may drain notifications and invoke callbacks on its
own thread. Optional terminal printing/logging consumes copied events on the
observation lane; blocking handlers cannot own readiness. Body stdout behavior
is distinct: preserve diagnostic records and avoid claiming that arbitrary user
printing is ordered across workers.

## Controller algorithm and admission

Prepare dependency counts and successor adjacency once. Keep one run coroutine
for lifecycle and an event-driven controller dispatcher; do not create an async
task per blocked invocation. The Plan remains the authoritative graph.

1. A prepared Run publishes its root-ready invocation IDs into its ready deque.
   Completion of a predecessor decrements successors' counts; unresolved inputs
   never occupy Dask worker slots.
2. On a turn for a ready Run, select its next ready specification in stable Plan
   order. Failed predecessors yield `blocked: dependency failure` without
   dispatch. With stop-on-failure enabled, stop that Run's future admission;
   other Runs and their independent stop policies remain unaffected.
3. Finalize computation identity through Exec using selected identities, retaining
   candidate input provenance. Create a fresh durable observation handle for
   `each_submission` before finalization. Binding/hash/serialization work above a
   small fixed batch runs in the preparation lane, not unbounded loop callbacks.
4. Look up compatibility in the controller-owned execution table. A match joins
   without spending an additional execution window slot. Each consumer still
   durably binds its own invocation to the exact execution and candidate inputs
   before being counted as admitted. The table entry tracks pending bindings as
   well as attached consumers, so last-consumer stop cannot race an invisible join.
5. A new entry reserves one outstanding slot for its placement, creates/persists
   the dispatch handle, and waits for at least one successful consumer binding.
   If every binding fails or withdraws, gate the handle and release the entry;
   no task enters Exec. Do not hold the registry unavailable during disk waits.
6. The entry supervisor calls `client.submit` for **ready work only** with the
   placement resource, unique execution key, `pure=False`, and `retries=0`, then
   awaits the distributed Future. No `sleep(0.01)` completion polling and no
   caller-created controller thread is involved. The durable gate remains the
   authority even if a worker is replayed despite the retry setting.
7. On completion, retain the canonical Exec result/selected reference, project
   it separately for every attached invocation, release the Dask Future after
   projection, publish history, and unlock successors. Execution errors and
   persistence errors remain separate. A vanished worker after entry is unknown/
   indeterminate evidence, not proof that its body never ran.
8. Release an old generation only if the table still contains that same entry.
   Completed reuse is always Exec's decision on a subsequent dispatch, not a
   forever-cached Run table result.

The dispatcher alternates Runs with ready work and rotates placement opportunities;
a blocked placement must not prevent another placement progressing. Bound each
turn's CPU work (initially at most 64 node transitions, then yield). For each
placement cap `C`, allow at most `W=2*C` distinct outstanding executions, including
ones preparing dispatch; joining consumers do not increment it. This window
bounds serialization/futures and limits how far A can queue ahead of a late B.
It is **not** a second active-job count: only Dask grants placement capacity.
Already dispatched long jobs are not preempted, and round-robin admission is not
an equal-walltime service guarantee. State that in progress diagnostics.

### Dask and pooled execution details

Official [Dask async operation](https://distributed.dask.org/en/stable/asynchronous.html)
documents `Client(asynchronous=True)`, ordinary non-awaited `submit`, awaitable
Futures/gather, and awaited close. The local 2026.8.0 probe verifies that basic
shape on the dedicated thread. Use a supervisor awaiting each distinct execution
Future to emit completion events; do not poll or use callback-thread assumptions.

The controller performs semantic dependency/identity admission. Dask schedules
resource-constrained ready execution tasks. There is one owner of each decision,
not two competing dependency schedulers. Do not send the whole Plan to a Dask
Delayed graph as well. Dask worker `nthreads` and resource capacity continue to
come from the same Site placement declaration.

**Pools are an existing exception to the physical one-scheduler count.** The
recommended first replacement retains one local execution scheduler plus one
existing LSF pool scheduler per worker shape. It introduces no additional
scheduler for async control, and no duplicate farm-capacity semaphore. The local
placement cap bounds outstanding gateway work, while the pool's worker count and
cores bound farm worker capacity: these are different resources. Report both.
Calling this topology "one scheduler" would be false.

Retain submit-host Exec claims/accounting and command-only farm work initially.
Sending the whole attempt wrapper directly to a pool would remove a gateway
waiter, but move journals, body imports and lifetime assumptions onto farm nodes;
current shared-filesystem evidence does not justify that as an incidental change.
Likewise a mixed-worker single Dask scheduler would change Jobqueue integration.
Those alternatives do not block the present lifecycle replacement and are not
additional controller-hosting proposals.

`open_pools` must become an async lifecycle operation on the Runtime loop. Worker
pool-client setup/teardown should use async plugin hooks and await client creation/
close on the worker loop, instead of constructing blocking clients inside setup.
The transport still calls its worker-local client's synchronous result bridge
from an execution thread, never from that loop. A second local probe verified
this exact async-plugin/synchronous-task bridge with two in-process clusters on
distributed 2026.8.0: the payload returned 42 and the Client recognized the task
thread as synchronous. Adopt that mechanism; validate it with Jobqueue's remote
workers during implementation. No thread per pool query is needed.

[Distributed API](https://distributed.dask.org/en/stable/api.html) is the reference
for Client/Future and worker integration; Jobqueue cluster startup/scale/close
remains an implementation probe because dask-jobqueue was absent here.
The initial supported dependency target should be the measured distributed
2026.8.0 family, with matching Dask resolution. Do not inherit the old >=2023.9.2
compatibility claim without testing this new path. Keep Jobqueue optional;
Flow's separate pinned Delayed experiment must have its own compatible test
resolution rather than forcing the entire workspace onto a contradictory pair.

## Explicit resource budgets

These are proposed conservative starting defaults for a prototype, not measured
optimal values. `RuntimeLimits` is operator data, separate from Study identity.
Site/TOML should have a `[runtime]` section for these limits; use placement
`max_jobs` consistently and retire the old `[kernel] threads` alias rather than
accepting two local-capacity owners. No unbounded setting is needed initially.

| Resource | Initial bound / owner | Behavior at capacity |
| --- | --- | --- |
| Controller | Exactly 1 thread and 1 loop per Runtime | No scale-out with Runs |
| Nonterminal Runs | 32, including registered/accepting/preparing/stopping | `submit` raises `RuntimeBusy` before accepting responsibility |
| Submission mailbox | At most active-run limit; one command per receipt | Reservation and enqueue atomic under bridge lock; rollback on failure |
| Plan state | 10,000 invocations per Run, 50,000 total admitted; 16 MiB encoded Plan per Run | Node limits checked at registration, byte limit before durable acceptance; no task per blocked node |
| Outstanding execution tasks | `2 * max_jobs` per placement; consumers bounded by admitted Plan nodes | Ready IDs stay in bounded admitted Plan state; round-robin dispatch resumes on completion |
| Hedloom async tasks | At most `3*R + sum(W) + 8`, where R is active-run limit and W each placement's outstanding window | Lifecycle/preparation/binding tasks are bounded per Run, execution supervisor per distinct entry; joins and blocked nodes remain data, not tasks |
| Local/direct execution threads | Sum of placement `max_jobs`; each worker pool explicitly sized | Dask enforces resource admission; no `secede`, unrestricted task, or automatic nesting pool |
| Preparation lane | 1 ThreadPoolExecutor worker, at most 32 queued jobs | Async backpressure; run remains PREPARING/ACCEPTING, loop remains available |
| Storage lane | 2 workers, at most 64 outstanding jobs; serialize writes per run/handle | Await before enqueue, never feed an unbounded executor queue; critical binding/terminal writes precede best-effort reads |
| Observation/maintenance lane | 1 worker, one query and one coalesced retention request pending | Skip/coalesce refreshes; no query backlog; no user callback on controller |
| Direct farm | Declared cap per direct placement includes PEND and RUN | One owned `bsub -I` process per admitted execution; LSF separately arbitrates licences/site limits |
| Pooled farm | Explicit workers per pool, cores/memory/walltime; gateway cap remains distinct | Fixed scale, no adaptive growth; reject unsupported per-command resource promises |
| Notification ring | 1,024 events per Runtime | Drop oldest notification and increment count; never drop durable outcome evidence |

ThreadPoolExecutor's internal queue is unbounded by default; a semaphore/reservation
**before** submit bounds each lane's outstanding jobs. Merely supplying
`max_workers` is insufficient. Do not use `asyncio.to_thread` or
`run_in_executor(None, ...)`. Per-run persistence chains preserve event order;
critical dispatch links must be acknowledged before dispatch. A hung filesystem
can exhaust a lane and prevent durable progress or close. This design keeps the
loop responsive but does not promise to interrupt kernel filesystem operations.

The async-task bound covers Hedloom-created tasks, not Dask's internal protocol
tasks. Reserve task slots before creating them; a large shared-consumer set must
be processed in bounded batches by its Run task, never one task per consumer.
Resource diagnostics should expose both Hedloom registries and observed Dask
overhead, without pretending to enforce arbitrary third-party task creation.

Preparation covers source reads/hashing, reproducibility/Git queries, callable
serialization and expensive immutable transformations. Storage covers allocations,
history, durable dispatch gates and live evidence reads. Attempt-owned journals,
output capture/content hashing, bodies and transport waits stay on bounded Dask
execution threads. Observation covers bounded-time queue queries and retention;
a retention scan can delay refresh without affecting correctness. It must check
a stop flag between records; it cannot promise interruption within a filesystem
call. Large command output remains a concrete memory cost: current transports
buffer complete streams before recording bounded diagnostic tails. Retaining
those transports does not provide an RSS bound, even with finite concurrency.
Returned Python values and declared stream outputs also remain JSON-compatible
in-memory values; recommend file artifacts for bulk data. Stream spooling is a
separate targeted improvement if measurements show this cost binding, not a
prerequisite hidden inside the controller replacement.

Dask itself adds resources. The local probe observed one process-global offload
thread (`max_workers=1` in installed source), a profiler thread for the loop, and
one execution thread at one-slot capacity. Workers also configure an actor
executor; Hedloom must not use actors or expose arbitrary client submission as a
way around accounting. Library serialization/offload queues and worker memory
are not governed by the I/O lanes. The bounded execution window limits Hedloom's
feed into them. Startup diagnostics should report Dask version, local workers,
thread caps, pool clients, configured pool jobs and observed auxiliary threads.
Do not promise an exact total thread count across third-party code or arbitrary
bodies. Require repeated-open/close tests to rule out per-Runtime accumulation;
do not shut down Dask's process-global executor behind other consumers.

Threads share the GIL. Pure Python CPU work competes with the controller; a C
extension holding the GIL can stall it completely. Cooperative async hosting
cannot guarantee prompt response during such a foreground command. Recommend
CPU-heavy work as a Shell executable or an appropriate farm placement. A body
requiring Python's main thread is unsupported in the new runtime; provide a
specific refusal/documented constraint, not an inline fallback. Arbitrary user
bodies may create their own threads/processes; Hedloom's declared budgets govern
its resources, not containment of malicious or undeclared body behavior.

## State, stop, failure, and lifetime

```text
Runtime: NEW -> STARTING -> OPEN -> CLOSING -> CLOSED
                         \-> FAILED       \-> FAILED (diagnostics retained)

Run: REGISTERED -> ACCEPTING -> ACCEPTED -> PREPARING -> RUNNING -> SUCCEEDED
          |             |           |          |          |
          |             +-> REJECTED |          +----------+-> FAILED
          +-------------------------+---------------------> STOPPING -> STOPPED
```

`ACCEPTED` is a durable milestone (possibly brief), not an assertion of complete
preparation. A stop before storage allocation starts can settle a receipt without
a durable run. Once allocation starts, finish or diagnose acceptance and record
the stopped run; never recycle the occurrence. Terminal Run state, invocation
outcome, and persistence completeness are separate fields. `STOPPING` may retain
entered work for a long time. Failure has precedence over stop in the final Run
summary if a genuine invocation/preparation/engine failure was observed; an
explicit-stop flag is retained either way.

`Run.stop(reason=...)` is idempotent and immediately returns a request identifier;
the snapshot exposes controller acknowledgement, and history records the request
when storage is writable. It means **stop admitting and withdraw this
consumer**, not kill a Python thread or prove an external job is cancelled.
Do not add misleading `cancel/terminate` aliases. If a later transport-stop API
is added, it must record intent and observed completion separately and solve
claim ownership; that is not silently included here.

Queued preparation jobs can be cancelled before they start. Already-running
preparation/storage calls must settle under Runtime ownership; stop marks their
Run so their eventual result cannot trigger dispatch. Never cancel an asyncio
wrapper and forget its still-running thread. A stopped receipt cannot free its
resource reservation while such work still holds it.

For shared entries:

- If completion has already been received, preserve its observed result.
- Otherwise remove this consumer after resolving its pending binding. If other
  consumers remain, record `withdrawn` for this Run; keep the shared execution.
  History still links the exact execution, whose eventual outcome can be inspected
  independently. Consumer withdrawal is not a claim that computation was cancelled.
- With no consumers, race the durable entry gate. If cancellation wins, publish
  blocked-before-entry and release execution resources when its wrapper settles.
  If entry wins, retain a runtime-owned draining interest and await the actual
  result. Never lose that obligation just because no user holds the Run object.
- A stopped Run can finish promptly after detaching shared work, but it waits for
  its exclusive entered work. The final result preserves succeeded/failed outcomes
  from those entered invocations; unstarted nodes remain blocked with a stop reason.

Stop-on-failure uses the same protocol, scoped to that Run. Unhandled controller
failure closes admission, faults affected live handles, attempts conservative
withdrawal, and retains cleanup diagnostics. It does not claim Dask cancellation
proves a remote job ended. Reuse remains successful-standing-evidence selection
under the existing explicit acceptance rules; no automatic retries are introduced.

### Interrupted waits and process signals

Timeout and a Python exception interrupting `wait` do not submit a stop command.
The private completion Future must not be cancelled by that interrupted wait.
The caller can inspect the same Run and wait again. This contract alone does not
make terminal Ctrl-C harmless: current Shell/direct children share the terminal
process group and may receive SIGINT independently of the Python wait.

For the replacement, make Shell/direct launch ownership explicit in Exec: use a
small Linux subprocess bootstrap (transport helper, **not a controller**) that
starts a separate process group/session, installs parent-death signaling after
exec into the helper, verifies the expected parent is still alive, then execs
the requested command. This removes Python's threaded `preexec_fn` hook and
isolates ordinary terminal SIGINT from owned jobs. It can be a private Python
module using the existing Linux libc binding; no new package or native build is
required to test the hypothesis. Failure to establish the promised binding must
refuse launch. Verify fork/parent-loss windows and child cleanup with real local
processes before making the isolation contract current. Do not assume it controls
arbitrary grandchildren that deliberately detach. Farm propagation is still LSF's
unverified part of the chain.

The library installs no process-wide signal handlers from its background thread.
Scripts may catch KeyboardInterrupt on their main thread and call `stop/close`;
interactive users can resume after interrupt and decide explicitly. Host loss
or SIGKILL cannot publish a final history record. Readers must show incomplete
or unknown execution, not synthesize success/failure. Pooled command-tree cleanup
on worker/host loss needs a dedicated probe; existing pool walltime is a bound on
allocation, not proof every invocation was terminally accounted for.

### Close and interpreter exit

`close(mode="drain", timeout=None)` first refuses new submissions, lets accepted
Runs finish, publishes terminal history, settles/coalesces maintenance, drains
owned I/O, then closes worker pool clients/local client/local cluster before
remote pools, closes I/O executors, stops the loop, and joins the controller.
Pool ordering must follow actual client references after the async plugin change.
Every cleanup stage runs even if an earlier one fails; errors are aggregated.
`mode="stop"` requests the withdrawal protocol first, then follows the same drain.
`close` is idempotent; no resource is transferred to an ownerless task.

A close deadline bounds the **caller's wait**. On expiry raise `CloseTimeout`
carrying remaining runs/executions/I/O and leave Runtime `CLOSING`; a later close
can wait again. It must not report CLOSED while an entered execution still uses
a transport. There is no safe force-kill of arbitrary Python threads.

Recommend a **non-daemon controller thread** and explicit close as the prototype
normal-exit policy. Ending a script or leaving IPython with an unclosed Runtime
may keep the process alive, including when it is idle. This deliberate cost is
preferable to silently abandoning evidence; examples must use a context manager
or `finally: rt.close(...)`. Do not claim an `atexit` hook repairs it: interpreter
thread shutdown ordering and executor threads can prevent that hook from solving
live-work shutdown. No destructor performs blocking cleanup. This is a documented
usability tradeoff open to user preference, not a guarantee of automatic draining
on an uncoordinated interpreter exit. The first lifecycle slice must demonstrate
both clean exit after close and the diagnosed unclosed-runtime case in bounded
child-process tests. No daemon or recovery service is proposed to hide this cost.

## Nested studies: preserve staging, retire worker-held submission

The concrete need in `studies/ota_pvt_clean_nested.py` is to materialize a corner
set, then author an inspectable second Plan with per-corner reuse. It is not
intrinsically a need to block an execution worker on another scheduler. Current
`_OCCUPANCY/_BLOCKED_UNITS` counts those waiters; the study also creates a separate
sequential Session to avoid contention. Both mechanisms obstruct one explicit
resource owner.

Recommend rejecting `Runtime.open`, `submit`, blocking wait, or close from an
operation context with `NestedSubmissionUnsupported`. The attempt wrapper sets
and clears that context even on exceptions. Runtime objects are nonserializable;
there is no worker-client escape hatch. This is a deliberate removal of nested
submission capability, not a claim that nesting was never supported.

Move maintained staged examples to caller-level Python, with one Runtime:

```python
with Runtime.open(site) as rt:
    first = rt.submit(read_corner_declarations(), name="declarations").result()
    jobs = first.outputs["jobs"].value
    inner = corner_study(jobs)  # ordinary static authoring over materialized data
    second = rt.submit(inner, name="corners",
                       derived_from=[first.reference]).result()
    # Subsequent reporting can be another explicit Study if it spends work.
```

`derived_from` is an optional facade history reference to prior Runs, not a
scheduler dependency, recursive runtime call, or assertion of scientific
causality. This makes the staged relationship inspectable without inventing a
campaign engine. Save the second Plan in its own history; remove the wrapper's
redundant nested Plan output. Preserve stable per-corner operation definitions
and inputs so unchanged corners still reuse. Change the nested wrapper's version
or retire it explicitly; do not pretend the outer aggregate attempt identity
survives a structural rewrite.

For entirely known graphs, composing static `@flow`s remains simpler. For the
word-analysis example, submitting `word_analysis` directly retains its result and
reuse without `nested_studies_state.SESSION`. For the ASS example, preserve its
CLI result/report behavior with explicit staging and verify that changing only
limits reuses simulation and reruns evaluation. This recommendation challenges
both the body-scheduling escape and the global headroom contract at the facade/
Run boundary. If transparent recursive body submission is later a user requirement,
this proposal needs a distinct suspended-orchestration-node design; do not quietly
reintroduce threads, spare slots, or a sequential backend. It is not required by
the current staggered-submission direction.

## Reloads, evidence, live history, and storage

At registration, detach the Study's implementation mapping and immutable Plan
reference from future registry edits. Preparation must produce a per-submission
binding envelope that pins the exact operation body versions, not resolve module
names anew when a queued task eventually runs. Use the existing cloudpickle
boundary with explicit by-value treatment of authored function code/defaults/
closures; test imported-module and `__main__` cases. Do not use global
`register_pickle_by_value` toggles across concurrent Runs as a hidden process
policy. If a binding cannot be frozen/serialized, fail preparation visibly.

Use a private capsule serializer for ordinary authored Python functions: clone
the function with `types.FunctionType`, retaining its code/defaults/closure and
metadata but giving the clone a non-importable private module identity before
cloudpickle serialization. This makes the envelope carry the selected body
rather than a later module lookup, without changing global pickle registration.
Build and serialize it off-loop; the attempt worker deserializes it. Original
Plan identity and source-origin metadata remain separate from this transport
detail. This mechanism is a recommendation to probe in WP6, not a tested claim
that every callable or transitive module can be frozen. Refuse unsupported
callable shapes rather than silently falling back to name-only dispatch.

Facade registration should retain source text/fingerprint evidence for each
registered definition when available, so preparation of an older Plan after a
file edit cannot falsely label newly read source as that old body. Capture
per-run current source/Git evidence separately and mark any mismatch/gap. Imported
modules, external files and opaque mutable objects are not hermetically frozen
by serializing a function. Referenced plain configuration should be declared in
the Plan; authors must not mutate undeclared dependency state during active work.
A reload may create a new body for future Studies while older Runs keep their
captured binding; full live-environment mutation is not guaranteed reproducible.
Do not silently add every global to computation identity.

Retain the explicit environment cache epoch: one capture per Runtime/project root,
async single-flight, shared by concurrent preparation jobs. A refresh replaces
future submissions' epoch only on success; old Runs retain their snapshot. Copy
submission cwd and explicit metadata at registration so a later `chdir` does not
retarget relative attachments. File content is observed during preparation and
labelled with that time, not retroactively claimed to be submission-time content.
Borrowed output lifetime and shared-path stability remain the author's existing
responsibility. Runtime replacement does not solve source mutation between
fingerprinting and payload read.

Storage consequences are explicit:

- Keep independent `records_dir`, `runs_dir`, `work_dir` and direct named run
  directories. Keep allocation and execution bookkeeping under `_meta`.
- Keep record layout 1, attempt identity, try numbering and execution-handle
  schema 1 unless implementation evidence demands a separately reviewed change.
  Renaming Session to Runtime does not justify invalidating reusable records.
- Write **history schema 4** for new lifecycle semantics: submission UUID,
  runtime identifier, acceptance/preparation/stop/final states, failure phase,
  optional `derived_from`, and existing exact selection/input references.
  Publication order is header/Plan acceptance, preparation evidence, binding,
  dispatch, outcomes, terminal run event. Plan schema remains 4 independently.
- Add a small read-only schema-3 history decoder for existing saved evidence;
  no copying, auto-migration, rewrite, or interpretation as an active new Run.
  Keeping evidence readable is not keeping the old blocking runtime. Older
  layouts already refused today need no new migration machinery.
- Keep the meaning of CLI paths, pins and explicit prune. New readers expose
  accepted-but-unprepared, stopping, rejected/incomplete allocation and degraded
  persistence without claiming an unreachable controller is alive.
- Coalesce automatic retention per Runtime and run it after terminal history
  handling, only when its admitted execution/preparation work is quiescent.
  Keep current floor/rules/claims and refusal protections. Hold new preparation/
  dispatch while that pass is applying; incoming registrations may still receive
  receipts and inspect status. Let maintenance yield between records when new
  work arrives. This deliberately defers `after_run` maintenance while other Runs
  remain active, avoiding an additional active-workspace pin/exclusion protocol.
  Report deferred maintenance. Other-runtime/manual destructive actions still
  require operator coordination; no cross-host lease is invented.

## Ordered implementation work packages

This is the proposed next implementation, **not work performed in this report**.
Keep a temporary replacement development path only until the retirement gate;
never publish permanent dual-engine selection or legacy shims. The source and
ontology changes listed below would require their own implementation task scope.

### WP1 — lifecycle and Dask integration experiment

Impact: new Run-unit async host/executor experiment, facade Runtime skeleton,
focused tests; cluster constructor changes. Depends on no other package here.
Use a scratch or explicitly temporary prototype before switching public exports.

Acceptance: one background thread hosts controller plus asynchronous local Dask
client/cluster; A stays active when B is submitted later; foreground has no loop;
capacity one and two are enforced by independent tasks. Compare idle prompt,
foreground sleeping/I/O, and Python CPU load without claiming GIL immunity.
Measure controller lag and thread names at 1, 8 and 32 Runs. Repeated open/close
must not accumulate controller/worker threads. Cover startup failure and close
from the wrong thread. Add a real terminal-IPython smoke only when actually run;
the present report did not run one.

### WP2 — receipts, bounded lanes, acceptance, and history

Depends on WP1. Impact: facade `runtime.py`, proposed `run.py`, `history.py`,
`discovery.py`, `reproducibility.py`; Run-unit controller ports; Site parsing.

Acceptance: immediate receipt without source/filesystem work on submit; slow
acceptance leaves status/control responsive; capacity refusal is synchronous and
creates no hidden run. Minimal durable acceptance precedes preparation failure;
required evidence failure prevents body entry. Run wait/result/accepted failures
are observable without callbacks. All lane queues have tested finite bounds;
no default executor. History schema 3 remains readable beside schema 4, with
unchanged storage roots and no rewrites. Per-run persistence order and partial
allocation discovery have injection tests.

### WP3 — runnable vertical slice through real Exec

Depends on WP2. Impact: `controller.py`, `executor.py`, `execution.py`, existing
binding functions, report types moved out of `driver.py`, facade body binding.

Acceptance: real local file-producing operation followed by a consumer, through
recorded Exec and new history. Submit A, hold it on a barrier, then B sharing it;
observe both workspace paths from a separate history reader before release.
Exactly one body runs, consumers retain their names and candidate provenance.
Run C later reuses the same try through a new dispatch. Exercise
`each_submission` acquisition converging to equal downstream identity, ordered
collection inputs, named ports, unavailable outputs, missing placement and body
serialization refusal. This is the minimum useful replacement slice, not a mock
async scheduler demonstration.

### WP4 — fairness, withdrawal, and failure accounting

Depends on WP3. Impact: controller and execution supervisors, gate offload,
Run state/results, facade notifications.

Acceptance: new Run enters after earlier large ready queue within the documented
window; occupied placement cannot starve another. Independent stop policies share
one execution safely. Cover pending-bind/stop, gate enter/cancel race, last versus
non-last consumer, replay after failure, old-generation release, worker loss,
body exception, and persistence degradation. Interrupted/timeout waits do not
cancel completion. Slow observer/printing does not stall controller. Verify
bounded task count at the active-run/node/window limits rather than timing alone.

### WP5 — transports, pools, signals, and complete close

Depends on WP3; finish after WP4. Impact: `cluster.py`, `pooled.py`, Exec subprocess
runner/bootstrap, watcher query timeout, Runtime release sequence.

Acceptance: local real subprocess owner-death and isolated Ctrl-C tests; no Python
`preexec_fn`; a killed parent cannot launch a surviving new payload. Preserve
stdout outputs and bounded recorded diagnostics. Fake direct farm confirms
cap across staggered Runs and correct resource options. Fake pooled and mixed
cases verify async plugin/client integration, lifecycle rollback, loss reporting,
correct close order, and absence of residual fake-farm jobs. Check command subtree
behavior when a pool worker dies; if the old guarantee is inadequate, record and
fix the concrete transport boundary before claiming owner-bound pooling.

Close timeout must retain ownership and diagnosable CLOSING state; a later close
succeeds after barrier release. Test non-daemon unclosed-runtime behavior in an
externally bounded subprocess and document it. No tests here authorize real farm
jobs: a separately authorized real-farm check can later extend the evidence ladder.
An unverified farm boundary must remain labelled, not block all useful local work.

### WP6 — reloads, retention, and staged consumer conversion

Depends on WP3–WP5. Impact: facade registries/binding/reproducibility/history,
runtime maintenance scheduling and collector cooperative stop checks, examples
and ASS consumers.

Acceptance: submit an old Study, edit/reload its same-origin definition, submit a
new Study; each uses its correct captured body and source evidence. Imported
mutable-state limitation is demonstrable and documented. Concurrent preparations
share one environment epoch; refresh failure preserves it. Automatic maintenance
and active Run execution cannot overlap; a new arrival receives a receipt while
the collector yields at a safe boundary. Maintenance never changes computation
outcome.

Convert `examples/nested_studies.py` and remove `nested_studies_state.py`; convert
`studies/ota_pvt_clean_nested.py` to explicit staged submissions. Demonstrate the
staged result with one local slot, inspectable second Plan, linked run histories,
unchanged per-corner reuse and targeted evaluation recomputation. Refuse body
submission before it can open resources. Update preparation integration tests;
do not require a full simulator sweep just to check the API conversion.

### WP7 — public switch, documentation, and retirement gate

Depends on every preceding acceptance criterion. Impact: facade exports, Run
exports, packaging/dependency resolution, tests/examples/docs/skills and affected
ontologies. This is the gate for calling the replacement delivered.

Remove or replace exactly these execution mechanisms:

- `src/hedloom/session.py`: `Session`, `session`, `_client_class`,
  thread-per-study `submit_all`, synchronous environment-lock path.
- `src/hedloom/study.py`: blocking `Study.submit`, `_run`, module `submit`,
  per-submission watcher startup, synchronous post-run maintenance;
  retain/adapt Study and named-result projection code under their new owners.
- `run/src/hedloom_run/driver.py`: `run_plan`; move useful report data first.
- `run/src/hedloom_run/graph.py`: `run_plan_graph`, `_run_plan_graph`, `_run_ready`,
  polling and `client=None` branch, nested occupancy/headroom counters;
  move tested binding/attempt-worker helpers once, without old-entry aliases.
- Replace `ExecutionOwner`'s lock-based table with loop-local state; preserve its
  handle gate, exact-reference publication and generation semantics.
- Retire public `sequential`, `locally`, `as_default`, arbitrary `client=`
  injection and kernel-choice history options. An explicit debug Site can serve
  all placements locally through the one engine, with the remap recorded.
- Retire `history.verify_workers`' filesystem-writing `client.run` handshake.
  Local execution workers are structurally in the same process; check root
  publication capability in the storage lane and assert that topology at startup.
  Pooled commands still require the explicitly documented shared work filesystem;
  this does not claim that local checks verify farm-node visibility.
- Remove tests requiring two runtime engines, sequential no-Dask execution,
  pooled sequential refusal, or worker-held nesting headroom. Port their
  meaningful identity, value, placement, failure and reuse checks first.
  Keep Flow's bounded Delayed instrument and Exec's standalone synchronous
  attempt APIs: neither is the legacy blocking study controller.

Update all executable example callers (`grid_refinement`, `live_source`,
`retention`, `farm_smoke`, `farm_smoke_pooled`, `farm_multi_client`, staged example),
their facade test files, and four ASS studies plus the preparation integration
test identified in the census. Update any dashboard access to Runtime diagnostics;
do not expose a raw client whose arbitrary tasks bypass budgets.

Maintain these descriptions in the same replacement change: facade README;
`docs/guide/running.md`, `sites.md`, `results.md`, `discovery.md`,
`runtime-artifacts.md`, `first-farm-run.md`, `refusals.md`,
`docs/internals/mechanism.md`, `placement-and-scheduling.md`,
`stop-admitting-protocol.md`, relevant Dask scheduling advice; Run README/docs;
Flow/Exec schema prose where stale; `skills/hedloom-study/SKILL.md`; root
`studies/README.md`, `docs/vision/open-concepts.md`, composition docs/dependencies,
and affected root/Hedloom/Run/Exec ontologies and agent invariants. Do not revise
dated design records to impersonate current documentation. Promote adopted
contracts explicitly. All units remain prototypes.

Retirement acceptance:

1. Maintained caller searches find no old execution calls/options outside
   clearly historical examples/tests intentionally retained as text.
2. There is one supported study execution path, used even at capacity one.
   No compatibility adapter or silent fallback imports the old controller.
3. New lifecycle suite plus affected facade/Run/Exec/Flow semantic suites pass;
   ASS preparation contract passes. Use proportionate targeted tests during work,
   then all four Hedloom test directories at this gate. Preserve identity/reuse
   fixtures and old schema-3 saved-history fixtures.
4. Build maintained docs and inspect warnings, check public exports and resolved
   package dependencies, and exercise local script + staggered REPL examples.
5. No records, histories, existing study data, or Git history are deleted to
   make the new tests pass. Farm verification status remains precisely qualified.

## Small remaining uncertainties and preference points

The design resolves its ordinary choices above; none requires an answer before
this report can be completed. Before implementation claims, these probes matter:

- Async Jobqueue cluster lifecycle: untested here because Jobqueue is absent.
  Local async Client/SpecCluster hosting and the async worker-plugin to synchronous
  task bridge are tested separately with in-process clusters.
- Loop latency, by-value body capture, and repeated lifecycle resource release
  under realistic source capture and long bodies. The basic probe does not measure
  full-runtime responsiveness or environment immutability.
- Linux launcher and pooled subprocess-tree termination: concrete process probes
  precede any strong owner-lifetime claim. Real LSF propagation remains a separate
  farm check; cross-host NFS guarantees are not established locally.

User preference could revise the proposed non-daemon/mandatory-close policy,
initial numeric limits, API spelling, or retirement of body-nested submission.
The recommended staged replacement and failure semantics are concrete enough to
review without pretending these preferences were already accepted. No new
controller host, recovery service, migration framework, or production deployment
work is needed to answer them.

## External comparison and report verification

AiiDA's [process usage](https://aiida.readthedocs.io/projects/aiida-core/en/stable/topics/processes/usage.html)
separates blocking execution from submission returning a persistent process node.
Its [2.9.2 Runner source](https://raw.githubusercontent.com/aiidateam/aiida-core/v2.9.2/src/aiida/engine/runners.py)
separates loop/resources and process tracking, and its
[performance guide](https://aiida.readthedocs.io/projects/aiida-core/en/stable/howto/tune_performance.html)
uses explicit limits on active processes. The transferable lesson is separation
of run identity, resource ownership and progression; none of its daemon machinery
is imported into this proposal.

[IPython autoawait](https://ipython.readthedocs.io/en/stable/interactive/autoawait.html)
and [event-loop integration](https://ipython.readthedocs.io/en/stable/config/eventloops.html)
distinguish terminal IPython's command-driven loop behavior from IPykernel's
persistent loop. These sources were rechecked, but this installation was not
interactively tested. The dedicated thread avoids requiring either caller loop
for controller progress. It does not avoid the GIL or process signals.
[Python asyncio development guidance](https://docs.python.org/3.11/library/asyncio-dev.html)
supports thread-safe scheduling and explicit executor offload; it does not make
blocking Python or filesystem calls cooperative.

This assignment performed investigation and edited only the two new reports,
the design index, and the existing Hedloom Open execution inquiry. It did not
implement any of these runtime changes. No package installation, farm operation,
publication, branch, commit, reset, or data migration occurred.

Checks: four focused current-behavior tests passed; both the dedicated-thread
async Dask API probe and local pool-client bridge probe passed; local
source/consumer/schema checks were performed. Exact
commands, versions, probe observations and the corrected search error are in the
census. Final whitespace checks in ASS and Hedloom and relative-link/scope checks
were performed after writing the reports. The input concept brief's SHA-256
remained `ff2cf6fbcf73cba4e436e6a06f488e8ff04b53d29e5683be8dcf7d62a920ea29`.
No whole-suite pass or new real-farm evidence is claimed. Confidence is high in
the core source census and moderate in the proposed integrated runtime until the
vertical slice and lifecycle probes run.
