# Interactive execution: usability and plan review

Date: 2026-10-01. This qualifies the
[2026-09-29 proposal](interactive-execution-proposal-and-plan-2026-09-29.md)
after the operator's discussion and its six `.ovnote` annotations. The original
proposal and annotations remain intact. Accepted user direction, recommendations,
current behavior, and local probe evidence are distinguished below. No runtime
replacement is implemented by this review.

## Automatic lifetime

**User direction:** preserve automatic owner-bound lifetime; mandatory manual
closing is not an inherited requirement. Current `Session.__exit__` releases its
resources, and `Study.submit` owns a Session context. Direct subprocess execution
also binds child lifetime to its parent on Linux. The previous usability answer
mistakenly presented the proposed mandatory close as an existing obligation.

**Recommendation:** make explicit close an optional way to release resources
early or request orderly settlement. Define what lifetime owns a long-lived
Runtime, and make normal interpreter exit and interruption reclaim its resources
automatically. Parent-death binding already addresses an important part of this;
it does not make a new non-daemon controller or standard executor cease keeping
the parent interpreter alive. Merely adding an ordinary `atexit` callback is not
a demonstrated solution to that shutdown ordering. Test exit without explicit
close, interruption, and abrupt owner death separately. Do not promise graceful
history completion on abrupt death.

Pooled batch jobs have a different lifetime from direct `bsub -I` children.
Current pool close cancels its jobs; loss of the submit host also involves worker
connectivity/death-timeout behavior. The probes below establish orderly local
teardown, not automatic cleanup of a new Runtime or all real-farm failure modes.

## Admission versus execution capacity

**Exact proposal behavior:** the 32 allowance counts every nonterminal Run,
including REGISTERED, ACCEPTING, PREPARING, RUNNING, and STOPPING. At capacity,
`submit` synchronously raises `RuntimeBusy` before allocating a receipt or doing
source, storage, or Dask work. There is no waiting Run, no automatic retry, and
no blocking wait for room. The next submission may succeed after a Run becomes
terminal.

This is painful for ordinary sweeps of many small studies. In a list comprehension
the first 32 submissions have already happened when the 33rd raises. Under the
proposed context manager, if that exception escapes the block, exceptional exit
requests stop for those earlier Runs and drains entered work. That consequence
follows from two proposed contracts; it is not current runtime behavior.

**Recommendation, not yet adopted:** remove 32 as the default public admission
ceiling. Accept waiting Run receipts and bound the work admitted to preparation,
storage, and execution. A waiting Run still owns retained Plan/receipt state, so
this does not establish unlimited memory or a durable queue service. If measured
state pressure requires admission refusal, expose a meaningful optional state or
queue budget with clear synchronous rejection semantics. Distinguish admission
refusal from ordinary waiting for placement capacity.

The constraints have different purposes and implementation costs:

| Constraint | Purpose | Consequence |
| --- | --- | --- |
| Placement capacity | Bound actual concurrent wrappers/jobs/commands | Existing Dask resources largely supply it; report local gateway and pool capacity separately. |
| Bounded preparation/storage work | Keep synchronous work off the loop and bound queued jobs | Requires owned lanes, backpressure, cancellation, and persistence ordering; a worker count alone does not bound the executor queue. |
| Run/Plan limits | Bound retained state | Simple counters are possible, but refusal becomes a public API contract. These particular numbers have no workload calibration. |
| Controller batching and notification coalescing | Keep control and observation responsive | Mostly internal scheduling choices; derive bounds rather than asking users to tune many independent knobs. |

The dedicated controller is the main architectural change. Most numerical caps
are policies, not prerequisites for that architecture. Fairness, sharing,
durable dispatch, and ordered shutdown are substantive coordination work.
A responsive controller can still have slow preparation or blocked persistence;
show those phases rather than equating a free prompt with useful progress.

## Priorities across the scheduling path

**User direction:** consider Dask priorities. **Recommendation:** start with a
submission priority, default zero, as Runtime scheduling metadata. Higher numeric
values should follow Dask's existing convention. Priority must not change the
study's computation identity or prevent reuse/sharing.

Apply it where work is selected for preparation/dispatch and forward it to local
Dask submission and to pooled command submission. A priority only passed to Dask
cannot help while all dispatch-window slots hold older work that the controller
has already selected. Keep the executor feed small and compare a capacity-sized
window with the proposed double-capacity window. Round-robin can break ties among
equally prioritized, dispatchable Runs; it does not by itself control Dask's
execution order. The local probe below supports `fifo_timeout='0 ms'` for this
particular ready-wrapper tie-breaking problem, not universal fairness.

For work shared by several Runs, recommend the highest attached consumer priority
while the execution is still controller-owned and pending. Withdrawal recomputes
that priority. Promotion of an already submitted Dask Future is a separate
compatibility question; do not assume resubmitting its key reprioritizes it or
duplicate the computation to obtain priority. Starting with priorities fixed at
submission avoids promising live reprioritization before that mechanism is proved.

Priority orders eligible work; it does not preempt entered bodies, subprocesses,
or farm jobs. Dask priority does not automatically become LSF queue priority.
Long-running work and unmet dependencies still delay foreground work. Strict
priority can starve lower-priority Runs; first document that tradeoff rather than
adding an untested aging scheduler.

## Nested submissions

**Accepted scope direction:** omit worker-held nested submissions from the first
replacement. This is deferral, not a permanent exclusion of hierarchical study
composition or a promise that caller-level staging covers every future use case.

Static composition and caller staging are sufficient for some current consumers.
Future demand may include reusable hierarchical studies or a controller-owned
child Run attached to an explicit parent lifetime. Result-dependent expansion
would additionally challenge Flow's static-authoring contract and needs its own
review. A future child coordinator should suspend without holding an execution
slot while its children need that slot; the current deadlock/thread-headroom
problem must not return disguised as compatibility. Preserve that ownership
possibility without building a nesting API now.

## Remaining ovnote answers

- **Completion without waiting:** the proposal's bounded `snapshot()` can expose
  a terminal state without blocking. Recommend a simple `run.done()` convenience
  and nonblocking terminal-result inspection. Completion and success differ:
  FAILED, REJECTED, and STOPPED are also done. These APIs remain proposed, not
  available behavior. Polling must not read the filesystem or require `.wait()`.
- **Tornado:** the async networking/event-loop framework used by distributed.
  Its modern asyncio bridge automatically shares the current asyncio loop.
  It is an implementation dependency; operators should not configure a second
  loop or learn a separate execution model.
- **Farm compatibility:** required. Existing `hedloom-run` packaging deliberately
  uses `distributed>=2023.9.2`; its README and commit `2b81542` record Run/façade
  suite passes on 2023.9.2, 2024.8.0, and 2026.7.1. Carry that compatibility
  obligation into the async slice, rather than imposing 2026.8.0 because one
  probe used the system interpreter. The new path still needs testing on the
  supported versions; this review does not identify a recorded exact remote-farm
  Python/Jobqueue installation or claim a new multi-version pass.
- **Schema 3:** the operator permits dropping all schema-3 history support.
  Remove the proposed compatibility decoder from replacement scope. This does
  not delete or migrate saved files. Current runtime history still uses schema 3
  until the replacement changes it.
- **Venv and fake farm:** present and usable. Root `AGENTS.md` now tells agents
  to use the ASS `.venv/bin/python`, inspect its imports/versions, and use isolated
  fake-farm state. The earlier absence claim concerned the system interpreter,
  not the ASS development environment.

## Verification and limits

ASS `.venv`: Python 3.11.16, Dask/distributed 2026.7.1, dask-jobqueue 0.9.0.

- Three tests passed: both cases in `exec/tests/test_owner_bound.py` and
  `run/tests/test_pooled_farm.py::test_closing_the_pool_leaves_no_farm_job_behind`.
- A bounded scratch probe hosted async LSFCluster, local SpecCluster, and Client
  on one dedicated thread/asyncio loop. An async worker plugin built the pooled
  client; the ordinary execution thread used its synchronous interface to run
  `run_command` on a fake-farm worker and obtained stdout `42`. Awaited teardown
  left its one fake job EXIT and the controller thread joined. Evidence:
  `/tmp/hedloom-ovnote-jobqueue-20261001-n6at1eoo/result.json`.
- On distributed 2026.7.1, a one-slot ready-wrapper probe queued a1,b1,a2,b2
  behind a held task. Equal priorities/default 100 ms gave b2,a2,b1,a1;
  equal priorities/0 ms gave a1,b1,a2,b2. Raising a2 to priority 10 gave
  a2,a1,b1,b2. Evidence:
  `/tmp/hedloom-ovnote-priority-20261001-u3b0x8ys/result.json`.

The first attempts failed before exercising their intended boundary: pytest's
scratch parent was absent, and Jobqueue requires host in `scheduler_options`.
Corrected reruns produced the evidence above. These checks establish local
integration feasibility and existing cleanup behavior. They do not implement
Runtime admission, priority-aware sharing, automatic Runtime shutdown, or new
real-farm/NFS guarantees. No packages were installed and no real farm was used.

Primary references: [Dask priorities](https://distributed.dask.org/en/stable/priority.html)
and [Tornado/asyncio integration](https://www.tornadoweb.org/en/stable/asyncio.html),
checked 2026-10-01; current Session, Study, direct-lifetime and pooled sources;
`run/pyproject.toml`, `run/README.md`, root `pyproject.toml` and `uv.lock`.
