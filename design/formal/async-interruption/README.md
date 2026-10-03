# Pooled interruption: bounded TLA+ investigation, 2026-10-03

The original `683a88f` investigation is retained below. The
[correction follow-up](#correction-follow-up-2026-10-03) records the separate
small models and current-source checks; neither is a whole-program proof.

**Dated review of `683a88f`, not a proof of Python, Dask, TLS, or a real farm.** The two
reported historical races have TLC counterexamples. The main model of the inspected protocol passes
its checked safety properties under the explicit physical-worker-loss assumption
below. Relaxing that assumption exposes a candidate false-cancellation gap for
parent investigation. A separate cleanup model identifies a scheduler-pause
ownership race, reproduced against the actual helpers. The herdr agent changed
only these investigation artifacts and submitted no jobs.

## Run it

From the ASS root:

```sh
JAVA_BIN=/usr/lib/jvm/java-8-jdk/bin/java \
  bash hedloom/design/formal/async-interruption/check.sh
```

`JAVA_BIN=java` works where Java is on PATH. The script downloads only the pinned
[official TLA+ v1.7.4 tools](https://github.com/tlaplus/tlaplus/releases/tag/v1.7.4)
to `/tmp`, checks SHA-256, runs one TLC worker, and validates both successful
checks and expected invariant violations (exit 12, exact invariant name).
The default script now runs only the small correction follow-up and original
cleanup checks. It does **not** repeat the large historical `faults` exploration.
To reproduce that historical run explicitly, pass `faults` as a positional argument.
`TLA_JAR` and `CHECK_DIR` can select existing external locations. Positional
arguments select individual configurations, for example
`check.sh current legacy-rpc`. Nothing is installed in the project environment. Full logs/state databases remain in the
printed temporary directory; `results.tsv` records actual counts.

Equivalent single check, from this directory:

```sh
/usr/lib/jvm/java-8-jdk/bin/java -Xmx2g \
  -cp /tmp/hedloom-tla2tools-1.7.4.jar tlc2.TLC \
  -workers 1 -fp 0 -seed 1 -metadir /tmp/pool-interrupt-current \
  -config current.cfg PoolInterrupt.tla
```

Tools used: Oracle Java `1.8.0_202-b08`; TLC `2.19 of 08 August 2024`,
revision `5a47802`, released as tools v1.7.4. JAR SHA-256:
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
The release's published SHA-1 also matched:
`bee4a54f3ee3d4afc347c3240ec2d9e93b075104`.

## Scope, state and source correspondence

`PoolInterrupt.tla` starts with two workers (addresses 1, 2), three submitted
commands (A, B, C), and two owners (`one`, `two`). A executes on worker 1, B is
resource-constrained on worker 1, and C executes on worker 2. In
`registration.cfg`, B's scheduler registration is delayed. A and B have
independent interruption coroutines. The model has one possible replacement
address per worker (3, 4), no address recycling, and one attempt per command.
It is exhaustive within these bounds, not a parameterized theorem.

Scheduler `registered`, `loc`, `interest`, and `spause` are distinct from worker
`local`, `body`, `pause`, `marker`, and `mem`. A scheduler-processing command
can be queued locally rather than executing. `pc`, `address`, `entered`, and
`snapshot` retain each coroutine's observations across awaits. `intent` is
durable Runtime interruption intent, not proof of termination. `withdrawn`,
`free`, `restarted`, and `claimed` distinguish scheduler withdrawal, pending
free-key delivery, physical restart, and returned cancellation evidence.
`finished` means natural body completion; it need not have reached the waiter.
Ghost sets record collateral kills, repeated starts, and post-withdrawal starts.

Paths below are relative to `hedloom/`. Source is the frozen working tree over
`8268a4406dada45cb56dc9bd539a05c2a29f60a3` on `feat/authenticated-pools`, **including
uncommitted fixes**, not just that commit. During checking the parent committed those same source
bytes as `683a88f` (`Authenticate owned execution pools and preserve force-stop
fences`); the modeled `pooled.py` SHA-256 remained
`13749aabf6739be56755d34bb8d2bfaaec022d08700da7c42120f3ee42af5f97`.
**Original handoff caveat:** after these checks began, the parent added new uncommitted
assignment-loss tracking and scheduler-fence ownership in `pooled.py` and
`_pool_scheduler.py`. Those changes are outside the original models and were not certified
by the historical pass. The follow-up below checks specific corrected boundaries. Findings below refer to the recorded snapshot, not a claim that they
remain unfixed in the moving working tree.

Installed dependency source examined:
Python 3.11.16, Dask/distributed 2026.7.1 in ASS `.venv`.

| Model actions/state | Implementation correspondence |
| --- | --- |
| `Persist`, `intent` | `run/src/hedloom_run/controller.py:Controller._withdraw`; `execution.py:ExecutionHandle.request_interrupt` writes `interrupt.json` under the entry gate. |
| `Locate`, `registered`, `interest` | `pooled.py:_locate_interrupt`; exclusive `who_wants` and no dependents. Missing task retries; absence does not claim cancellation. Dependents are abstracted as an additional interest. |
| `Pause`, `BusyOrMissing`, snapshots | `pooled.py:_pause_command`; Worker executing/long-running vs constrained tasks, key-owned pause, other owner's marker returns busy. |
| `MemoryResume`, `mem` | `distributed.worker_memory.WorkerMemoryManager._maybe_pause_or_unpause`; current pause temporarily disables its pause threshold. Legacy switch leaves it enabled. |
| `RPCClosed`, `LostReply`, `Recheck` | `_interrupt_command` catches only pre-withdrawal `CommClosedError`, refreshes scheduler observation, refuses a live address or shared task, retries only after old-address removal. A lost reply can follow an installed fence. |
| `Prepare` | `_prepare_interrupt`: recheck location/consumers, use actual-worker snapshot, fence scheduler admission and release interest in one scheduler callback. There is deliberately **no current-body collateral guard** in `Restart`; the snapshot/fence must suffice. |
| `Ack`, `FreeKey`, `ClaimQueued` | `client.cancel`, `_interrupt_ack`, `_worker_released`; removing a scheduler key does not remove an executing body. Worker free-key delivery is separate and may lag. |
| `Restart`, `Replacement`, `Confirm` | `client.restart_workers`, Nanny termination and replacement registration; Linux immediate-child SIGKILL owner binding in `exec/src/hedloom_exec/lsf.py`. Confirmation requires replacement registration. |
| `Cleanup` | `_resume_command_worker` restores policy only for the matching key; `finally` resumes before a restart, quarantines after one. Original threshold is abstracted to enabled; the existing unit test separately includes original `False` and `.8`. |
| `Assign`, `Start`, `Finish` | Dask scheduler assignment vs Worker admission and full-worker `hedloom-command: 1` reservation; task completion may race any await. |
| `UnexpectedLoss` | `distributed.scheduler.Scheduler.remove_worker`, release/rescheduling independent of `retries=0`. `Partition=FALSE` couples removal to physical body death; `TRUE` deliberately separates them. |
| `Deadline`, `TimedSpec` | Outer `_INTERRUPT_TIMEOUT`/`asyncio.wait_for`: eventual error, not manufactured cancellation. Wall-clock time is abstracted. |

`Evidence.tla` is a **separate abstraction**, not mechanically composed with the
pool model. One computation has two possible consumers, pending durable binding,
entry gate, ordinary drain, force escalation, sealing, durable interruption
intent, observed outcome, manifest publication, terminal event, manual acceptance,
and reuse. `Reserve`/`Bind` model pending ownership before filesystem work;
`Withdraw`/`Gate`/`Recheck` model `Controller._withdraw` and closing attachment.
`ConfirmedInterrupt` assumes a trustworthy transport observation—the pool model
checks that assumption separately. `PublishManifest` precedes `PublishTerminal`
as in `AttemptJournal.publish_terminal`; `Reuse` follows `attempt.is_reusable`.
Runtime `interrupt.json` is not Exec's distinct `request_cancel` journal event;
the latter's success-disagreement reconciliation path is outside this model.

`CleanupOverlap683a88f.tla` preserves the original cleanup model unchanged
except for its module name. It expands the two cleanup RPCs the main model
collapses. The switchable `CleanupOverlap.tla` is covered in the follow-up. It starts at a reachable cut: A timed out after scheduler withdrawal,
its body still occupies the resource, and its `finally` block is about to resume
the worker. B remains queued. Two pause owners and one worker suffice here.
Worker cleanup checks A's marker; scheduler cleanup checks only paused status.
`WorkerPausePreserved` holds, but B can lose its scheduler acknowledgement.
The short helper replay loads the pinned `683a88f` helper source from Git and
checks its SHA-256, so later runtime edits cannot silently change this dated
reproduction. It runs no command bodies, cluster, or jobs:

```sh
.venv/bin/python hedloom/design/formal/async-interruption/check_cleanup.py --mode historical
```

## Historical outcomes at 683a88f

See the checked-in results table below (full raw traces intentionally excluded).
A counterexample run stops at its first violation: its counts are a search
prefix, **not** an exhausted state space. Passing runs exhaust their state space.
No symmetry reduction, state constraint, probabilistic simulation, or hidden
invariant filtering was used. TLC fingerprinting remains a hash-based check.

| Configuration | Result | Generated | Distinct | Depth reached |
| --- | --- | ---: | ---: | ---: |
| `current` | Pass; `Settles` also checked | 29,395 | 8,308 | 24 |
| `faults` | Pass; `Settles` also checked | 26,991,140 | 6,362,073 | 37 |
| `shared` | Pass; `Settles` also checked | 6,091 | 1,760 | 17 |
| `registration` | Pass; `Settles` also checked | 49,515 | 13,742 | 25 |
| `evidence` | Safety pass | 611 | 171 | 12 |
| `cleanup-worker` | Safety pass | 25 | 12 | 6 |
| `legacy-memory` | Expected counterexample: `NoCollateral` | 4,192 | 1,530 | 10 |
| `legacy-rpc` | Expected counterexample: `NoStaleRPCFailure` | 2,841 | 1,051 | 10 |
| `partition` | Expected counterexample: `NoFalseTermination` | 7,006 | 2,536 | 7 |
| `unexpected-replay` | Expected counterexample: `NoReplay` | 1,040 | 483 | 5 |
| `error-cleanup-start` | Expected counterexample: `NoPostWithdrawalStart` | 2,601 | 972 | 9 |
| `cleanup-overlap` | Expected counterexample: `NoSpuriousAckError` | 17 | 10 | 6 |
| `manual-reuse` | Expected counterexample: `NoCancelledReuse` | 790 | 217 | 12 |

The broad `faults` run took 23 min 04 s with a 1 GiB Java heap, `-fp 58`,
seed `-4192160642343066940`, and one TLC worker; its final temporal check passed.
Other recorded runs used the script's fixed `-fp 0 -seed 1` and 2 GiB heap.
The script defaults to 2 GiB for reproducibility without the tighter heap's
extra collection overhead. TLC reported an actual-fingerprint collision estimate
of `5.7E-6` for the broad run; this is model checking, not a deductive proof.


Current safety checks: no collateral executing-command kill; no returned
cancellation while the modeled body survives; withdrawal and durable intent
before intentional restart; no start of a deliberately restarted/cancelled
command; at most one command body per worker; owned pause remains paused;
and no legacy stale-address RPC error. `shared.cfg` starts B with both consumers.
`Evidence` checks pending/shared protection, durable intent, manifest-before-
terminal ordering, and exclusion of incomplete/cancelled automatic reuse.

**Deadlock and liveness interpretation:** default TLC deadlock checking stays
on. Completed pool calls explicitly permit quiescent stuttering; `Evidence`
permits stuttering at all points and makes no liveness claim. Nonterminal pool
calls always admit `Deadline`. `Settles` proves only eventual return of both
interruption calls under weak fairness of each `Deadline`, meaning a responsive
controller/event loop eventually services its finite timeout. This is not a
40-second real-time proof, successful cancellation, body termination, or restored
capacity. RPC/free-key/replacement delivery can be indefinitely delayed in the
safety model. Successful cancellation would additionally need delivery,
responsive workers/Nanny, and scheduling assumptions; no such stronger liveness
claim was checked. The one-replacement bound can leave a later force call waiting
for a restart the model cannot represent; it then errors by timeout.

## Small counterexamples and parent follow-up

1. **Known memory-monitor race, reproduced.** A persists intent, locates worker 1,
   pauses and snapshots A executing, withdraws interest, receives scheduler ack.
   A finishes. `MemoryResume(1)` admits B; B starts. A's `Restart` kills B.
   `legacy-memory.cfg` violates `NoCollateral`. Existing source triggers:
   `run/tests/test_pooled_interrupt.py:test_owned_pause_prevents_actual_memory_monitor_resume_and_restores_policy`
   and `tests/test_force_stop.py:test_runtime_force_stop_cancels_active_and_queued_runs_together`.
   The latter invokes the real memory monitor at the pause checkpoint.

2. **Known stale pause RPC race, reproduced.** A pauses/withdraws and reaches
   restart; B locates old address 1. A restarts it. B's delayed RPC gets
   `CommClosedError`. Legacy aborts immediately; current `Recheck` must consult
   scheduler membership and ownership before retry. `legacy-rpc.cfg` violates
   `NoStaleRPCFailure`, a recovery property, not a termination-safety invariant.
   Current live-address failures still propagate. Source trigger:
   `test_checkpoint_connection_loss_retries_only_after_authoritative_worker_removal`
   (`removed`, `live`, `shared`, `missing` cases). `registration.cfg` retains
   the missing-task wait, with timeout rather than a cancellation claim.

3. **Candidate implementation gap: removed address with surviving body.**
   A persists intent; scheduler removes worker 1 but A's body survives;
   `Locate(A)` sees its task unassigned; `Prepare` releases interest without a
   worker snapshot; `Ack` accepts the missing key; `ClaimQueued` returns
   cancellation while A still runs. `partition.cfg` violates
   `NoFalseTermination` in six transitions. This need not traverse the new
   retry handler; it challenges the older unassigned-task branch too.
   Parent later reproduced this through public Runtime/Run, as recorded in the
   follow-up below. Installed Dask `Scheduler.remove_worker` sends a close message and removes
   the worker/processing assignments without waiting for physical termination;
   `close=False` is also supported. A missing worker therefore does not by
   itself establish death. **Not integration-reproduced here.** Proposed parent
   trigger: hold an entered command, remove its worker from the scheduler while
   keeping the process alive and replacement admission blocked, then force-stop
   before the worker's shutdown grace ends. Check whether cancelled evidence
   appears while the command still runs. Parent should review this before
   claiming force-stop safety under disconnect/unexpected removal. This model
   does not establish how often Dask realizes that interleaving on the real farm.

4. **Newly explicit failure-path limit, not confirmed-cancellation failure.**
   B is paused while queued, then its interest is withdrawn. Timeout fires
   before worker free-key delivery; matching cleanup resumes worker 1. A
   finishes and B starts before its delayed free-key arrives.
   `error-cleanup-start.cfg` violates the deliberately stronger
   `NoPostWithdrawalStart`. B's call is `error`, not `cancelled`.
   Source: `_interrupt_command`'s pre-restart `finally` cleanup and Dask's
   separate scheduler-to-worker release message. Proposed regression trigger:
   delay free-key delivery beyond `_INTERRUPT_TIMEOUT`, then release the other
   body. Parent review should distinguish an interruption request/error from
   verified prevention of a queued command's execution.

5. **Known unexpected-loss replay boundary, reproduced.** C finishes on worker
   2; lose worker 1 while retaining A's scheduler interest; assign A to worker 2
   and start it again. `unexpected-replay.cfg` violates `NoReplay`. This is the
   already documented separate pooled-command task limitation; the outer
   `ExecutionHandle.enter` gate does not guard that task. It is not evidence
   that intentional cancellation replays work.

6. **New cleanup acknowledgement race, helper-reproduced.** A times out after
   withdrawal, before restarting; its worker cleanup succeeds and returns true.
   B acquires a new worker pause and `_prepare_interrupt` pauses the scheduler
   and withdraws B. A's delayed `_resume_interrupted_worker` sees scheduler
   status paused and sets it running, without checking the new owner. B's
   `_interrupt_ack` raises `RuntimeError("pooled cancellation lost its exclusive paused worker")`.
   `cleanup-overlap.cfg` violates `NoSpuriousAckError` in five transitions;
   B's actual worker pause remains owned and paused. `check_cleanup.py` replayed
   these exact snapshot helper calls and verified both the exception and preserved
   worker fence. This is a source-level interleaving reproduction using minimal
   fixtures, not network integration evidence. Proposed parent regression:
   suspend A's scheduler cleanup after its worker cleanup returns, allow B's
   `_prepare_interrupt`, then deliver A's cleanup before B's ack. A scheduler
   ownership token or an equivalent ordering mechanism merits review; no fix
   was made here. This demonstrates why matching ownership at the worker alone
   does not establish matching ownership for the separate scheduler callback.

7. **Reuse qualification, source-verified.** Force intent → confirmed cancelled
   observation → cancelled manifest → terminal event → explicit acceptance →
   reuse violates `NoCancelledReuse` in `manual-reuse.cfg`. Automatic reuse
   excludes it. `attempt.accept_for_reuse` accepts any terminal published try;
   `is_reusable` returns true for `reuse_accepted`. A read-only ASS-venv probe
   with actual `TryState(number=1, phase='terminal', outcome='cancelled', ...)`
   returned false without acceptance and true with it. This is an existing
   explicit operator override, not a newly discovered automatic-reuse defect.
   An absolute prohibition of cancelled-result reuse would contradict current
   code and requires a separate user/parent decision.

## Assumptions, omissions and architectural interpretation

- Successful restart atomically ends the old immediate command body; detached
  descendants, process groups, Nanny bugs, TLS framing/authentication, OS failure,
  PID/address reuse, cross-host deadlines and filesystem durability are not proved.
  A restart request that fails before killing its worker is not separately
  represented: the main model collapses restart initiation and physical loss.
  It therefore does not verify every uncertain-restart quarantine branch.
- Default unexpected loss means actual process death, not merely failure
  detection. The partition experiment exposes why that assumption matters.
  A single `body` location cannot represent simultaneous duplicate incarnations;
  the partition trace stops at the earlier false claim. Full split-brain replay
  needs a multiset of incarnations and a separate abstraction.
- Resource reservation and Python worker/scheduler callback atomicity are
  dependency/source assumptions, not derived OS guarantees. Snapshot receipt can
  be delayed relative to subsequent environment steps; assignment-to-worker
  task delivery, body-to-scheduler completion notification, and the two cleanup RPCs are collapsed in `PoolInterrupt`;
  `CleanupOverlap` explicitly expands the latter. Only low-memory automatic
  resume is modeled, not the full memory/spill state machine. No arbitrary external
  task submissions or new consumers after sealed withdrawal are modeled.
- Shared Dask interest is abstracted separately from Runtime shared bindings.
  Pool interests are initially sole or shared and subsequently withdrawn;
  dynamic scheduler-client attachment is not explored. `Evidence` includes a pending binding, normal withdrawal, late attachment
  before sealing, and force escalation, but not multiple Runtime controllers,
  graph topology, artifact identity, try rollover, crash recovery, or manifests
  with corrupted data. It is not a refinement proof of the two implementations.
- Ownership release has the actual matching-key guard and frame conditions;
  it cannot clear another worker marker in these models; scheduler cleanup
  has the demonstrated weaker ownership check. There is no independent proof
  of every possible late cleanup RPC, same-key concurrent caller, or policy
  mutation by an external actor. The original false memory threshold case is
  covered by existing Python test source, not by these enabled-threshold states.
- Requirement: stop solely owned commands without harming other consumers.
  Chosen mechanism: worker pause plus Nanny restart. These are not synonymous.
  The model suggests keeping physical termination evidence separate from
  scheduler absence. A direct command-control handle could avoid worker-level
  pause/restart coordination but would move responsibility into the worker
  process boundary. That is a proposal for review, not an adopted architecture
  or a runtime change made by this task.

Confidence is high for the two bounded historical reproductions and for the
source-level reuse qualification and cleanup helper replay; moderate for the
abstraction's correspondence;
the partition and delayed-message interleavings need targeted integration review.
No unconditional real-farm or whole-program guarantee follows. Live memory
retrieval succeeded in project `analog-sim-studies-29428c6f90`; prior notes were
used as evidence and checked against the frozen source.

The final concise memory-note write was rejected by the MCP client:
`MCP tool call requires approval, but approval policy is never`.
No verification note was saved to memory; this report retains the results.
This is a tooling limitation, not an empty or unavailable retrieved memory store.


## Correction follow-up, 2026-10-03

### Reviewed source and evidence status

User/parent reported **ready for review** after the implementation added
admission holds. Runtime files remained read-only to this investigation. These
source hashes were checked while the parent was integrating the correction.
At final review they exactly match Hedloom commit
`03be6a9e5d305103c8b8cc280b45f236ada50028`, following historical revision
`683a88f83e3bc3bccef171eea6fa59b6b86e5f85`:

| File, relative to `hedloom/` | SHA-256 checked with the fixed helper probe |
| --- | --- |
| `run/src/hedloom_run/pooled.py` | `fb2e2c2945eee7167f8db0222d5cb6f4477306f19f3cb1d7a11d7c10c8101ed8` |
| `run/src/hedloom_run/_pool_scheduler.py` | `3934e7f27bee250d0faecbdff785eaeeb6a9f8bb9cea2436899883024295a9c7` |

Live text-search/read of **Hedloom interruption race corrections 2026-10-03**
succeeded in the same memory project. That reviewed note and the parent's
follow-up report independently establish the historical surviving-body defect:
public Runtime/Run at `683a88f` returned STOPPED and a cancelled manifest after
worker removal, including default `close=True`, while the command PID survived
another 1.25 seconds. Sources: `/tmp/hedloom-removal-force-probe.py`,
`/tmp/hedloom-removal-force-probe-unexpected.log`, and
`/tmp/hedloom-removal-force-probe-default-close.log`. This agent did not rerun
those farm probes. The earlier candidate finding is therefore now
**parent integration-reproduced at the old revision**, not merely a model concern.
The parent also independently reproduced the original cleanup helper trace and
8,308-state baseline pass.

### LossEvidence: a small separate abstraction

`LossEvidence.tla` has one scheduler key, two distinct TaskState identities,
two worker identities/addresses, at most two assignment losses, and one
certification callback. Initial body execution is nondeterministic (queued or
entered). A set of `(task identity, worker identity)` copies keeps actual body
survival distinct from the current assignment. Physical restart and its
acknowledgement are separate actions; either may be delayed beyond timeout.

| Model action/check | Actual source correspondence and boundary |
| --- | --- |
| `Begin`, `captured` | `_begin_owned_restart` requires an owned paused worker, records a restart token, and snapshots task/worker identities. Consumer/fence preconditions are assumed here, not re-proved. |
| `Lose` | `OwnedPoolScheduler.remove_worker` calls `assignment_loss` before `super().remove_worker` clears processing bookkeeping. It does not condition this on `expected`/`safe` or `suspicious`. Both administrative and unexpected removal use this action. |
| `unknown`, `pending` | `loss_record` stores these on that TaskState. No token, or another loss while pending, sets sticky unknown; otherwise the exact worker/token/object-identity tuple becomes pending. Metadata is not durable history. |
| `PhysicalRestart`, `Acknowledge` | Successful physical restart is assumed to kill that worker's immediate copies before an OK acknowledgement. Timeout alone never grants this evidence. OS/Nanny correctness is an assumption, not a theorem here. |
| `Certify` | `_certify_owned_restart` checks current TaskState identity plus exact pending token, address and worker identity. It clears only matching pending evidence and never clears unknown. Mismatched proof variants exercise the helper's defensive contract. The normal caller emits its captured proof only after OK. |
| `ReplaceTask` | A replacement TaskState for the same scheduler key starts fresh metadata. An earlier task's proof cannot clear the new task's pending evidence. The old body's identity does not silently become the new task's identity. |
| `Decide` | `_assignment_evidence` at both `_locate_interrupt` and `_prepare_interrupt`: unknown errors before withdrawal; pending waits; clean unassigned tasks may withdraw. `Legacy=TRUE` ignores the evidence to reproduce the old false claim. Missing tasks and assigned-worker interruption belong to other abstractions. |
| `Timeout` | Bounded pending wait becomes error and retains interest; it never fabricates cancelled evidence. Weak fairness of timeout servicing establishes eventual **return including error**, not successful termination. |

`Assign` deliberately allows more behavior than the ready-for-review runtime:
the newer `OwnedPoolScheduler.valid_workers` prevents normal reassignment while
pending certification or while a force admission hold exists. The model allows
it so that extra-loss handling is checked even under that stronger environment.
The direct loss helper's two-loss defense is not claimed to be an ordinary
scheduler trace while the newer admission filter is holding. `ADMISSION`,
`_release_interrupt_admission`, and `reconsider` scheduling/ownership behavior
are **not modeled or certified** by this pass. No desired admission guard was
invented to make the loss-evidence invariants pass.

Safety properties check surviving copies of the **current TaskState**, sticky
unknown evidence, exact certification, no cancellation with pending/unknown
evidence, and retained interest on error. An old task's remaining physical copy
is not proof about a newly created task. Cross-TaskState attempt/key lifetime,
Python `id()` recycling/ABA, general scheduler transitions, natural result loss,
consumer attachment and arbitrarily many losses are outside these bounds.
Worker/task identities and restart tokens are distinct and non-recycled in the
model. The models do not prove that a real OK reply always means physical death.

Five `loss-witness-*` configurations intentionally negate desired reachable
states. Their TLC invariant violations are **positive reachability witnesses**,
not new defects: never-assigned cancellation, acknowledged-loss cancellation,
extra loss remaining unknown after certification, stale TaskState proof being
ignored, and timeout retaining unconfirmed pending evidence. These prevent
mistaking a model that refuses all cancellation for a useful correction.

### Cleanup correction and actual-helper replay

The original `cleanup-overlap.cfg` and `cleanup-worker.cfg` still run the frozen
`CleanupOverlap683a88f` module, preserving their 17/10 and 25/12 results.
The extended `CleanupOverlap` independently switches matching-owner scheduler
cleanup, running-message protection, scheduler-first cleanup ordering, and
presence of delayed running messages.

- `cleanup-legacy` reproduces the original acknowledgement failure.
- `cleanup-fixed` models the current scheduler-first cleanup, matching key
  check, and running-message suppression while FENCE exists; it passes.
- `cleanup-matching` deliberately retains the adverse worker-first ordering
  and also passes with the two ownership protections. This tests their defense
  under a wider ordering, not a claim that current `finally` uses that order.
- `cleanup-owner-only` keeps matching-key cleanup and scheduler-first ordering
  but removes stale running-message protection. Its counterexample shows that
  an old running notification can still break B's acknowledgement. The fixed
  hook supplies that missing protection. A delayed duplicate cleanup is also
  allowed as a defensive helper-level input, not asserted to be a normal RPC
  retransmission trace.

`check_cleanup.py` now has `--mode historical`, `--mode fixed`, and `--mode both`
(default). Historical mode uses hash-verified Git source at `683a88f`. Fixed mode
imports the actual working-tree API and `OwnedPoolScheduler` override, with only
Dask's broad base status-transition method replaced by a minimal fixture. It
checks scheduler-first cleanup, B waiting until A's worker marker releases,
wrong-key/wrong-address cleanup refusal, both string and enum running messages
with address and WorkerState arguments, B's successful acknowledgement, matching
cleanup, policy restoration, and normal message forwarding after fence release.
It prints the exact source hashes. Both modes passed under ASS Python 3.11.16 /
distributed 2026.7.1. No cluster, command body or farm job is started.

```sh
.venv/bin/python hedloom/design/formal/async-interruption/check_cleanup.py --mode both
JAVA_BIN=/usr/lib/jvm/java-8-jdk/bin/java \
  bash hedloom/design/formal/async-interruption/check.sh
```

### Follow-up TLC results

Same pinned tools v1.7.4 / TLC 2.19 / Java 8u202, one worker, `-fp 0 -seed 1`.
The unchanged massive Faults exploration was **not repeated**. Fixed safety
runs exhaust their graphs; diagnostic/witness runs stop at the first expected
invariant violation. Default deadlock checking is enabled, with terminal
stuttering and the explicitly stated timeout/stuttering conventions.

| Configuration | Outcome | Generated | Distinct |
| --- | --- | ---: | ---: |
| `loss-legacy` | Expected `NoFalseClaim` violation | 33 | 29 |
| `loss-fixed` | Safety + `Settles` pass | 4,047 | 1,312 |
| `loss-never-assigned` | Safety + `Settles` pass | 7,569 | 2,443 |
| `loss-witness-never-assigned` | Desired cancellation reached | 3 | 2 |
| `loss-witness-certified` | Desired certified cancellation reached | 540 | 336 |
| `loss-witness-extra-loss` | Sticky extra loss survives certification | 1,017 | 567 |
| `loss-witness-task-identity` | Stale task proof rejected | 483 | 308 |
| `loss-witness-timeout` | Unconfirmed pending timeout reached | 51 | 41 |
| `cleanup-overlap` | Historical acknowledgement failure | 17 | 10 |
| `cleanup-worker` | Historical worker-marker safety pass | 25 | 12 |
| `cleanup-legacy` | Historical acknowledgement failure | 17 | 10 |
| `cleanup-fixed` | Safety pass | 33 | 12 |
| `cleanup-matching` | Safety pass with adverse ordering | 54 | 18 |
| `cleanup-owner-only` | Expected stale-message acknowledgement failure | 27 | 13 |

All 14 checker invocations returned their expected outcome; the checking script
exited zero. The five exhaustive passing graphs had no deadlock or checked
safety violation. The two loss graphs also passed `Settles` under weak fairness
of timeout servicing, at depths 13 and 14 respectively. Cleanup graphs check
safety only (depths 6, 7, 7); no cleanup liveness claim is made. The nine diagnostic
or witness prefixes are not exhaustive safety/deadlock/liveness passes.
Logs and the generated count table are in
`/tmp/hedloom-formal-followup-final/`; `check.sh` reproduces them without relying
on those temporary files.

The new legacy loss trace has three states: a task body is alive on worker 1;
`Lose` removes its assignment while the body remains alive and marks unknown;
legacy `Decide` ignores that evidence, withdraws interest and reports cancelled.
This maps the parent-reproduced removal defect to the missing historical
assignment-loss evidence. Fixed `Decide` reports error instead. The owner-only
mutation reaches B's owned pause, then accepts a delayed running notification;
B's acknowledgement fails. This isolates the new status hook's purpose from the
matching-key cleanup correction.

No new failure of the checked fixed evidence/cleanup invariants was found.
The owner-only mutation identifies the need for the implemented running-message
filter. This is bounded evidence for separate helper contracts, not a composed
proof of the runtime or the newer admission holds. Parent owns fresh focused
Python checks, full-suite verification, and integration decisions after source
freeze. No runtime, existing test, maintained documentation, installation,
commit, push, or farm job was performed by this follow-up.

Memory retrieval remains verified. The earlier note write failed because MCP
required approval under policy `never`; per parent direction it was **not retried**.
Parent will save reviewed evidence through its ordinary memory connection.
Confidence is high for these finite checks and actual-helper assertions,
moderate for model-to-runtime correspondence, with the boundaries above explicit.
