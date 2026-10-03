# Interactive execution: internals census

Date: 2026-09-29. Status: source-checked investigation, not an implemented redesign.
Companion: [proposal and implementation plan](interactive-execution-proposal-and-plan-2026-09-29.md).
Input: [replacement concept](interactive-execution-redesign-2026-09-29.md).

## Finding

Hedloom already has one dependency-admission algorithm and a useful separation
between consumer history, dispatch ownership, and computation evidence. Its
blocking interface hides that structure: a submission owns a caller stack until
preparation, execution, reporting, and automatic retention finish. Concurrent
submissions require additional caller/controller threads. Replacing that lifetime
with one explicitly owned async runtime is plausible without replacing identity,
Plan authoring, or the attempt protocol.

The hard work is not adding `async def` to `submit`. It is making acceptance,
blocking I/O, shared-execution withdrawal, and shutdown explicit while preserving
the evidence that tells an operator what actually ran. Current source already
solves several of those evidence problems; its APIs and thread placement need not
survive in order for those solutions to survive.

## Baseline and scope

- ASS revision: `7f1616b01316b9c226680ecff35f0968e8a98f00`.
- Hedloom revision: `e9dbe70e3371cc789665a2ffa05e05bb38e69a64`.
- Initial ASS status: ` m hedloom`. Initial Hedloom status: modified
  `ONTOLOME.md`, modified `design/README.md`, untracked concept brief above.
  These were intentional inputs and were preserved. No runtime file was changed.
- Scope: facade, all Run execution modules, Exec identity/derivation/attempt/
  journal/transports/retention, Flow's authoring and Plan handoff, relevant tests,
  maintained guides/ontologies, examples, ASS studies and integration consumers.
  This is an architectural census, not a claim to audit every branch of every file.
- Context included the root manifesto/ontology, applicable agent guidance,
  unit manifests and READMEs, child ontologies, Exec's decision ledger, Flow's
  historical trackers/architecture, and the root open-concepts register.
  Dated designs are context, not evidence of current runtime behavior.

Reproducible size inventory, from the ASS root; counts include all `.py` files
beneath each named directory. Test definitions are AST function names beginning
`test_`, including methods; they are **not** collected/parameterized test cases.

| Directory | Python files | Source lines | Test definitions |
| --- | ---: | ---: | ---: |
| `hedloom/src` | 10 | 3,319 | 0 |
| `hedloom/run/src` | 8 | 2,909 | 0 |
| `hedloom/exec/src` | 14 | 4,694 | 0 |
| `hedloom/flow/src` | 5 | 3,332 | 0 |
| `hedloom/tests` | 22 | 4,696 | 188 |
| `hedloom/run/tests` | 8 | 2,569 | 90 |
| `hedloom/exec/tests` | 30 | 5,193 | 308 |
| `hedloom/flow/tests` | 6 | 2,869 | 76 |

```python
from pathlib import Path
import ast
for root in ['hedloom/src', 'hedloom/run/src', 'hedloom/exec/src',
             'hedloom/flow/src', 'hedloom/tests', 'hedloom/run/tests',
             'hedloom/exec/tests', 'hedloom/flow/tests']:
    files = sorted(Path(root).rglob('*.py'))
    tests = sum(sum(isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and n.name.startswith('test_')
                    for n in ast.walk(ast.parse(p.read_text()))) for p in files)
    print(root, len(files), sum(len(p.read_text().splitlines()) for p in files), tests)
```

## The actual execution path

```text
caller: @study / StudyBuilder -> immutable Flow Plan + selected Python bodies
  |
  Study.submit -> opens Session, or uses supplied client
  Session.submit -> environment cache -> Study._run                  [blocks]
  |                  source fingerprints / bindings / reproducibility
  |                  HistoryWriter -> named run + consumer evidence
  v
  driver.run_plan -------+             graph._run_plan_graph
                         |                    |
                         +---- _run_ready -----+                    [polls]
                               dependencies -> finalize_invocation
                               execution compatibility -> owner entry
                               consumer binding -> durable dispatch handle
                                      |
               inline call OR Dask ready task with placement resource
                                      |
                         handle.enter -> _run_one_here
                                      |
             Exec.execute: record claim -> select try/reuse -> publish reference
                                      |
                        BoundTransport -> authored body
                           | value      | Shell
                           |            +-- local subprocess / direct bsub -I
                           |            +-- pooled client -> farm worker command
                                      |
                    capture -> manifest -> terminal -> standing evidence
                                      |
                owner future -> per-consumer projection -> history -> successors
                                      |
                     final report -> retention -> StudyRun -> release Session
```

### Authoring, binding, and preparation

[`src/hedloom/__init__.py`](../src/hedloom/__init__.py), `operation`,
`_implementations_for`, and `StudyBuilder.__call__`, join Flow declarations to
bodies. Same-origin reloads can register new definitions while older Plans retain
bodies selected by complete definitions. The four module registries are mutable
process globals, with no lock. A Study copies the implementations dictionary;
it does not freeze functions' globals or imports.

[`flow/src/hedloom_flow/model.py`](../flow/src/hedloom_flow/model.py), `Plan`,
accepts schema **4** only. Planning is static: flow-building Python executes;
operation bodies do not. The separate
[`experimental/local_dask.py`](../flow/src/hedloom_flow/experimental/local_dask.py)
is a non-exported Delayed-lowering instrument with no submission, persistence,
or placement authority. It is not an alternative production runtime to migrate.

[`src/hedloom/study.py`](../src/hedloom/study.py), `Study.submit`, normally opens a
Session, calls its blocking `submit`, then closes it. A supplied client bypasses
that ownership. `Study._run` constructs `BoundTransport`s, fingerprints external
sources, obtains their addresses, captures reproducibility, constructs history,
then invokes Run. An `on_started` callback arrives only after this preparation.
Failures in source reading or initial history creation can happen before the
caller receives a run reference. There is no live public Run object.

[`src/hedloom/session.py`](../src/hedloom/session.py), `_environment`, serializes
lazy environment capture per project root under a `Lock`. Capture runs in the
submission thread and can enumerate installed distributions, inspect files, and
invoke Git. `refresh_environment` replaces the cache only on success. Per-run
source and attachment evidence remains fresh. This split is worth retaining;
its execution location and synchronization should change.

### Three identities, not one

| Identity | Source owner and meaning | Consequence |
| --- | --- | --- |
| Named consumer run | `HistoryWriter`: `runs_dir/<name>.<occurrence>/`; reservations under `_meta/allocations` | Distinct requests retain distinct names, Plans, input candidates, and outcomes even when work is shared. |
| Dispatch execution | `ExecutionHandle`: UUID, durable gate and selected reference under `_meta/executions/<owner>/<execution>/` | One dispatch may enter Exec only once, regardless of Dask replay. It is not a try reservation. |
| Computation record and try | Exec `input_digest`, `attempt_identity`, `AttemptJournal.begin_try` | Declared computation selects a record; tries identify actual execution/recovery evidence. Placement and requester names do not choose the record. |

[`exec/src/hedloom_exec/planned.py`](../exec/src/hedloom_exec/planned.py),
`prepare_invocations`, emits symbolic specifications. `finalize_invocation`
substitutes selected artifact identities **when dependencies have completed**.
Fresh `execution="each_submission"` work incorporates a durable dispatch
identifier; equal acquired content can still make downstream computations equal.
This is why sending an unresolved whole Plan to Dask is no longer the current
implementation.

[`run/src/hedloom_run/binding.py`](../run/src/hedloom_run/binding.py),
`build_bundle`, `produced_by`, and `output_value`, distinguish values, addresses,
artifact identity, and exact producer references. Candidate consumer inputs live
in the run's binding; Exec's `inputs_bound` records inputs actually used by the
selected try. Reuse does not rewrite old evidence to match a new requester.

### Admission and sharing

[`run/src/hedloom_run/driver.py`](../run/src/hedloom_run/driver.py), `run_plan`, and
[`run/src/hedloom_run/graph.py`](../run/src/hedloom_run/graph.py),
`_run_plan_graph`, both call `_run_ready`. There are two dispatch choices, not two
independent readiness algorithms. The controller scans remaining specifications,
collects completed futures, propagates failures, finalizes ready identities,
builds bindings, and dispatches. It sleeps for 0.01 seconds when work remains
active. No async loop, bounded ingress queue, run limit, fair cross-run admission
queue, or bounded ready-task submission window exists here.

The execution-sharing key includes computation digest, serialized implementation/
delegate binding, policy, record/work roots, and output binding. Sharing is
stronger than reuse identity. Compatible work is joined only inside the same
`ExecutionOwner`; independent Sessions can contend on the same Exec record and
receive `ConcurrentClaim`. Session budgets are also independent, not a user-wide
farm quota.

[`run/src/hedloom_run/execution.py`](../run/src/hedloom_run/execution.py),
`ExecutionOwner`, protects an active table with `RLock`; it permits one Dask client.
Completed entries can be replaced, and `release` checks entry object identity so
an old consumer cannot remove a replacement. The owner lock currently spans
history binding and submission, including filesystem writes. Moving that whole
critical section onto an event loop would stall every run.

A task receives ready concrete inputs. Dask's `client.submit` uses `pure=False`,
`retries=0`, unique execution-suffixed keys and `placement:<name>` resources.
`_require_admission` refuses missing worker resources before dispatch;
`_require_shippable` cloudpickles transports before execution. Neither check is
an excuse to use Dask identity as computation identity.

Sequential dispatch creates a `concurrent.futures.Future` but calls `_run_handle`
inline before setting it. Sharing can still work between external concurrent
submitters. `submit_all(sequential=True)` serializes studies, but independently
concurrent `Session.submit` calls on a sequential Session have no shared placement
semaphore. The sequential flag is not a general resource owner across such callers.

### Attempts and substrates

[`exec/src/hedloom_exec/durability.py`](../exec/src/hedloom_exec/durability.py),
`execute`, holds a nonblocking exclusive record claim across selection,
submission, and reconciliation. The facade always uses `Durability.RECORDED`,
including local Python bodies. Exec's separate `EPHEMERAL` facility is real but is
not exposed by this facade path; Flow's ephemeral reference classification does
not mean a facade execution skips recording.

[`exec/src/hedloom_exec/attempt.py`](../exec/src/hedloom_exec/attempt.py) and
[`journal.py`](../exec/src/hedloom_exec/journal.py) enforce the valuable ordering:
try allocation and submit intent flush before transport; manifest publication
precedes terminal event. Selection publication is diagnostic, not permission to
execute. Claims refuse contention; they do not become a waiting scheduler.
Current `request_cancel` also needs the record claim, so it cannot provide prompt
cancellation from another thread while `execute` holds that claim around a
blocking transport. A general hard-stop API cannot simply wrap this function.

| Substrate | Actual work and residency | Limits and unresolved costs |
| --- | --- | --- |
| Local Python | `BoundTransport._call` in caller thread for sequential, or submit-host Dask worker thread | Dask placement cap in graph mode; GIL and body behavior remain Python constraints. Main-thread-only code has no graph-mode accommodation. |
| Local Shell | `binding._run_locally` uses Exec `SubprocessRunner`, synchronous `subprocess.run` | One waiting execution thread and subprocess per invocation; captured stdout/stderr may grow before truncation/recording. |
| Direct LSF | `LSFInteractiveTransport.submit` waits on one `bsub -I -J <record>-<try> -W ...` | Dask slot includes queue wait. `max_jobs` is a runtime share, not site MAX JOB or licence inventory. Invocation options are resolved per job; LSF arbitrates declared licences. |
| Pooled LSF | `hedloom_run.pooled.LSFPooledTransport.submit` calls a worker-local pool client and blocks on `future.result`; only `run_command` executes on the farm | One local gateway execution thread per outstanding invocation, remote worker threads/processes, one separate `LSFCluster` per shape. Farm jobs count workers, not invocations. No authoritative recovery discovery or per-command hard cancellation. |

[`run/src/hedloom_run/cluster.py`](../run/src/hedloom_run/cluster.py),
`cluster_for/spec_cluster`, creates a local in-process SpecCluster: one Worker per
placement, each with `nthreads == max_jobs` and the matching placement resource.
No nanny or `secede`. Default no-HTTP classes suppress scheduler and worker HTTP;
communications are in-process. Exposure and process mode are distinct contracts.

[`run/src/hedloom_run/pooled.py`](../run/src/hedloom_run/pooled.py), `open_pools`,
creates actual additional schedulers and LSF workers. `PooledClientPlugin` creates
one synchronous pool Client per local worker per pool. Session closes local client
and cluster before pools because those workers hold the pool clients. Pooled
`run_command` uses plain `subprocess.run`; do not transfer the local Linux
parent-death guarantee to it. Worker death/lost communication can leave an
indeterminate result. A single-scheduler mixed topology is not implemented.

### Projection, history, retention, and withdrawal

[`src/hedloom/history.py`](../src/hedloom/history.py), `HistoryWriter`, currently
uses history schema **3**, not schema 2 mentioned in the handoff and older prose.
It allocates names with exclusive directory creation, checks independent storage
roots and no-overwrite publication, writes Plan/addresses/reproducibility, and
checks worker visibility before execution. `verify_workers` uses `client.run`;
its filesystem work executes on worker event-loop threads, another blocking-I/O
seam to remove in an async design.

Consumer binding must succeed before admission. Later selection-publication or
history-outcome failures return diagnostics/degraded persistence without inventing
a different computation result. `RunHistory` joins exact execution references,
reads complete journal prefixes, distinguishes missing/reclaimed/inaccessible
workspaces, and exposes requested versus executed inputs. It is not a recovery
controller. Terminal `StudyRun` exposes authored-key outcomes and named output
projections; there is no aggregate last-result `.value` to preserve.

On stop-on-failure or an escaping exception, `_run_ready.withdraw` removes that
run's consumer interests. Remaining consumers protect shared work. With no
remaining consumer, `ExecutionHandle.cancel_before_start` races `enter` under
one short file gate. Unentered work is prevented; entered work is awaited and
reported truthfully. A Dask stack snapshot is not used. The public API has no
ordinary Run stop method; callbacks can raise and cause withdrawal. Callback
exceptions and `KeyboardInterrupt` are caught by the controller's `BaseException`
cleanup path, which can continue waiting for entered work before raising.

Automatic retention follows `writer.finish`, before submission returns. It uses
only named Site rules, rechecks claims, protects standing/pinned/live/unreconciled
tries, and warns on failure. It can perform extensive scans/hashing/deletion in
the submitting thread. Runtime-level scheduling should coalesce this maintenance;
changing orchestration does not authorize discarding records or old workspaces.

## Resource, synchronization, and shutdown census

| Resource/state | Current ownership | Replacement pressure |
| --- | --- | --- |
| Submission stacks | One caller stack per `submit`; `submit_all` creates and joins one thread per study in graph mode | Runs need tasks and registries, not caller threads. Exceptions propagate only after joins. |
| Readiness polling | One `_run_ready` loop per submission, 10 ms sleep; unbounded ready submissions relative to Plan size | Event-driven completion and finite admission window. |
| Dask infrastructure | Synchronous cluster/client loop machinery plus worker execution pools; library offload/profiling threads | Count separately from controller thread; async constructors can share its loop. |
| Watcher | One daemon thread per Session, `Event.wait`; extra standalone-client watcher possible | One runtime observer task and bounded external-query lane. Current join timeout is 1 s, but the query itself is not bounded. |
| Environment cache | Session dict and `Lock`, capture performed while locked | Async single-flight cache; no blocking I/O under controller locks. |
| Execution table | `ExecutionOwner.groups`, `RLock`, consumers, futures | Loop-local mutation; durable gate still needed across workers. |
| Nested accounting | `threading.local` occupancy, process-global placement counter and lock | Counts are not scoped by Runtime/client; name collisions across runtimes can contaminate the headroom model. |
| Registries and result caches | Facade module dictionaries; per-transport `_results`; per-run produced/outcome maps | Freeze admission bindings; keep mutable operation code limitations explicit. |
| Filesystem locks | Blocking short dispatch `flock`; nonblocking attempt claim; allocator mkdir and fsync | All can incur storage latency. Short logical duration does not mean safe on an event loop. |
| Subprocesses | Local command, direct `bsub`, `bjobs`/`bkill`, Git; pooled commands and worker batch jobs | Budget query/preparation processes separately from payload processes. |
| Signals | No runtime signal owner; Python interrupt usually reaches main thread; children stay in same process group | An interrupted Python wait and terminal Ctrl-C are not equivalent: the latter can also signal children. |
| Close | Session `_release` closes resources, clears owner, signals watcher | No public registry of live Runs to drain or stop first; releasing an owner does not itself withdraw every consumer. |

Exec's [`lsf.py`](../exec/src/hedloom_exec/lsf.py), `_bind_child_lifetime`, already
records the Linux fork-to-prctl race and general threaded `preexec_fn` hazard.
Moving the controller does not repair either. A subprocess cannot be assumed to
die merely because a Run handle was dropped or an asyncio task was cancelled.
No interpreter-exit hook or guarantee of prompt shutdown was found in these
runtime sources. There is no project-owned queue/backpressure mechanism hidden
elsewhere in these paths.

## Consumers and retirement map

| Surface | Keep meaning | Change needed for replacement |
| --- | --- | --- |
| Facade exports in `src/hedloom/__init__.py` | `Study`, declarations, named outputs, reproducibility and history | Replace `Session/session`, module `submit`, `Study.submit` and terminal-only submission result with explicit Runtime/Run. |
| Run exports and `driver.py`, `graph.py` | Report/outcome data, binding, readiness, explicit placement | Retire both blocking entry points and `_run_ready`; move report types to an execution-neutral module. |
| `Site` and profiles | Three roots, address spaces, placement caps/options, retention, exposure | Replace `[kernel] threads`/mode vocabulary with explicit runtime limits; retire sequential, locally, as_default and arbitrary client injection. Explicit local Site transformation remains useful. |
| CLI/discovery | CLI `runs list/show/path`, `attempts list/show`, pins and prune; Python `RunHistory.outputs/reproducibility` (also projected by CLI show) | Read new lifecycle states and pre-preparation acceptance; preserve current saved records. CLI remains read-only for live history, not a second controller connection. |
| Examples | `grid_refinement.py`, `live_source.py`, `retention.py`, farm smoke/direct/pool/multi-client evidence | Submit handles and wait deliberately; show staggered submissions; update matching example tests. |
| Nested example | `nested_studies.py`, `nested_studies_state.py` | Replace imported live-Session singleton and worker-held waits with explicit staged caller orchestration, or a separately justified nested design. |
| ASS study callers | Four files in `studies/` | `rc_corners.py`, `ota_pvt.py`, `ota_pvt_clean.py`: Runtime/Run calls; clean variant currently inspects `farm.client.dashboard_link`. Nested variant needs stage restructuring, not a keyword rename. |
| ASS tests | `integration-tests/test_ota_preparation_contract.py` directly submits sequentially | Move to one-slot Runtime; measurement helper and static Plan tests do not need a new execution API. |
| Agent-facing consumer | `skills/hedloom-study/SKILL.md` and `docs/guide/agent-skill.md` | Replace sequential-first instructions and Session examples. The skill was inspected as a consumer, not invoked for this engine investigation. |
| Dependencies | Facade/Run optional distributed extras, Run cloudpickle, optional dask-jobqueue | Recommendation makes distributed a Run dependency; Flow/Exec stay independent. Reconcile root/Hedloom dependency declarations and lock resolution when implemented. |

The outer caller scan found five Python files matching the runtime/import pattern
outside Hedloom: the four studies and the preparation integration test. This is
a scoped source-search result, not proof about arbitrary external installations:

```console
rg -l 'from hedloom( |\.|_run)|import hedloom|run_plan_graph\(|submit_all\(' \
  --glob '*.py' --glob '!hedloom/**' --glob '!**/_runs/**' \
  --glob '!**/prototypes/**' .
rg -n 'Thread\(|ThreadPoolExecutor|ProcessPoolExecutor|asyncio|RLock\(|Lock\(|Queue\(|sleep\(|\.join\(|\.result\(|signal\.' \
  hedloom/src hedloom/run/src hedloom/exec/src
```

Tests worth carrying as semantic criteria include `test_shared_execution.py`
(staggered consumers, late arrival, replay refusal, gate race, independent stop
policy), `test_runtime_identity.py` (fresh observations converging on shared
analysis and generation-safe release), `test_discovery.py` (live paths, initial
refusal, degraded history), `test_interactive_reload.py`, `test_reproducibility.py`,
`test_study_outputs.py`, and `test_prune_after_run.py`. Retire cross-kernel parity,
no-Dask sequential, and nested-headroom assertions after their meaningful
identity/value/capacity properties are expressed against the new runtime.
`exec/tests` protocol cases remain valuable without becoming runtime tests.

## Contradictions and evidence limits

These are documentation/evidence corrections, not contradictions in the user's
interactive-runtime requirement. They do not require stopping this investigation.

1. Run's ontology introduction and current source assign readiness to Run.
   `run/README.md`, `run/docs/index.md` ("Gives readiness to Dask"), parts of
   `docs/internals/mechanism.md`, `docs/internals/placement-and-scheduling.md`,
   facade README, and root open-concepts current tables still describe older
   dependency ownership. The open-concepts `as_completed` claim is also stale:
   graph imports it but completion collection now polls futures.
2. `docs/internals/stop-admitting-protocol.md` and the started-work advice in
   `docs/internals/dask-scheduling-rules.md` describe `call_stack`/Dask cancellation.
   The maintained protocol must instead explain the durable entry gate. Historical
   model evidence can remain dated; it is not the current algorithm.
3. Handoff/ontology prose mentions history schema 2. Code uses history schema 3,
   Plan schema 4, execution-handle schema 1, and record layout 1. Flow and Exec
   READMEs still mention Plan schemas 2/3. Preserve these distinct version axes.
4. Run ontology says its graph kernel has not run a real study; current ASS
   callers run local graph studies. The accurate unverified boundary is real-farm
   graph/concurrency/pooling, not all non-fake study execution. Exec ontology's
   early "never contacted a real cluster" wording also conflicts with the
   maintained sequential direct-LSF smoke evidence.
5. Exec README's initial detached-lifetime motivation conflicts with its
   owner-bound contracts. The Exec ledger also contains differing pooled lifetime
   claims. Current pooled command code does not establish process-tree cleanup
   after host loss; do not infer it from a worker timeout description.
6. Old fixed test totals in agent guidance are stale; the inventory above gives
   scoped definitions, not a substitute claimed total pass count. Historical
   ~16 KiB/thread measurements are not a current general memory guarantee.

[First-farm evidence](../docs/guide/first-farm-run.md) records a real **sequential
direct-LSF** smoke: argv/job naming, artifact chaining, failure, and reuse. Graph
capacity, watcher parsing and pooled/mixed operation have local/fake-farm evidence.
Cross-host NFS exclusion and cache visibility, real licence arbitration, and LSF
owner-death propagation remain unverified. No new farm evidence is claimed here.

## Checks performed for this report

Environment: `/usr/bin/python`, Python 3.14.7; Dask/distributed 2026.8.0;
pytest 9.1.1; IPython 9.17.1; **dask-jobqueue not installed**. No installation or
farm command was attempted. Initial search commands included a nonexistent root
`tests/` directory and emitted `rg` errors; the consumer search was corrected to
`integration-tests/` and repository-wide patterns above. No test failure occurred.

Targeted current-behavior check from ASS root:

```console
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH=hedloom/src:hedloom/flow/src:hedloom/exec/src:hedloom/run/src \
python -m pytest -q -p no:cacheprovider \
  --basetemp=/tmp/hedloom-async-census-20260929/pytest \
  hedloom/tests/test_shared_execution.py::test_staggered_consumers_share_live_paths_and_keep_their_names \
  hedloom/tests/test_shared_execution.py::test_entry_and_cancellation_have_one_durable_winner \
  hedloom/tests/test_shared_execution.py::test_last_consumer_cancels_before_entry_even_when_dask_started_the_wrapper
```

Result: **4 passed in 2.81 s** (staggered test parameterized for success/failure).

A small API probe at
`/tmp/hedloom-async-census-20260929/async_dask_probe.py` constructed an asynchronous
SpecCluster and Client inside one named background thread using `asyncio.run`,
with in-process communication and the existing no-HTTP classes. Two 0.2 s tasks,
submitted 0.05 s apart, requested the same one-slot resource. Both clients' loop
references matched the controller's loop; task B started after A finished. The
foreground waited on an Event and ran no event loop. This is **not an IPython
installation test**, nor an implemented Hedloom runtime.

Observed live thread names: main, probe controller, one Dask execution thread,
`Dask-Offload_0`, and `Profile`. Immediately after cluster/loop close the last two
remained visible. Installed `distributed.utils` declares the offload executor
with `max_workers=1`; Worker teardown deliberately does not shut it down. Dask
profiling watches loop lifetime; its immediate presence after close is not proof
of a permanent leak. This observation rules out promising that Runtime.close
returns the process to its exact pre-import thread count. Output is saved beside
the probe in `async_dask_probe.out`. Both the probe and targeted tests completed
successfully. No full unchanged-runtime suite was run.

A second bounded local probe,
`/tmp/hedloom-async-census-20260929/pool_client_bridge_probe.py`, constructed two
in-process no-HTTP clusters on one asyncio loop. An async WorkerPlugin created
and closed an asynchronous pool Client. A synchronous gateway worker task used
that client to submit a payload and call `future.result(timeout=5)`. Result:
`{"value": 42, "client_asynchronous_in_task_thread": false}`, exit 0. This verifies
the proposed async-plugin/synchronous-task bridge on distributed 2026.8.0,
without Jobqueue or a farm. It does not verify LSFCluster startup/scale/close.
Output is saved as `pool_client_bridge_probe.out` in the same scratch directory.

Probe commands (from ASS root; both exit 0):

```console
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=hedloom/run/src:hedloom/exec/src \
  python /tmp/hedloom-async-census-20260929/async_dask_probe.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=hedloom/run/src:hedloom/exec/src \
  python /tmp/hedloom-async-census-20260929/pool_client_bridge_probe.py
```

Final report checks: both Git diff whitespace checks, relative-link existence,
source-symbol spot checks, scope review, and preservation of the concept brief
are recorded in the companion plan's verification section.
