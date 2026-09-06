# Implementation handoff: readable run history and live execution discovery

2026-09-06. Source inspected at Hedloom `bfaac7d` (PR #19 merged).
Status: implementation plan requested by the user; no implementation performed.
The implementer should execute the phases below, verifying the current checkout
first. Do not reopen settled choices merely because another design is possible.

## 1. Outcome and authority

An operator can discover an earlier or ongoing submission by a name they chose,
list its human-readable invocations, and reach the exact journal and workspace
for a selected try while the operation is still running. The same interfaces
work from a fresh process after the submitting process exits. Reused executions
remain shared; each submission retains its own account of what it requested.

Read the current applicable `AGENTS.md` files, the composition root's
`MANIFESTO.md` and `ONTOLOME.md`, and affected component ontolomes, READMEs,
`unit.toml`, and references required by those instructions. The current manifesto
has been adopted; the hedloom-dev saved snapshot predates that adoption.
The [original proposal](discoverability-proposals-2026-09-05.md) and
[identity handoff](../DISCOVERABILITY-ATTEMPT-IDENTITY-HANDOFF.md) are background
suggestions. This plan incorporates the subsequent user decisions, including
automatic readable invocation names and live selection recording.

Preserve unrelated changes. At inspection the only untracked Hedloom file was
the original proposal, including two user `@` comments; leave it untouched.
The containing repository also had four modified submodule pointers. Do not
reset, stash, commit, publish, or spawn agents merely because this file exists.
An assigned implementing agent may make the implementation and verification
changes described here without asking routine design questions.

## 2. Settled behavior

| Concern | Decision |
| --- | --- |
| Submission name | Required user-supplied string on every Hedloom submission; no study-name or UUID fallback. |
| Complete run address | `<name>.<N>`, always including `.1` on the first occurrence. |
| Namespace | One history directory configured by the site; names span study definitions within that directory. |
| Occurrences | Positive, unpadded integers; allocated atomically and never reused by supported operations. Gaps are valid. |
| Invocation names | Explicit key first, existing sweep-generated key second, otherwise operation name plus `.N`. |
| Internal Plan IDs | Continue deriving from resolved authored keys and boundaries; expose readable addresses separately. |
| Live discovery | Persist actual selection before a blocking launch; show workspace and journal paths without waiting for terminal results. |
| Initial write failure | Refuse before any invocation executes. |
| Later history failure | Continue computation, report degraded persistence prominently, preserve available in-memory outcomes. |
| Query behavior | Read only. No launch, reconcile, scheduler poll, pin, pruning, or mutable latest alias. |
| First release | Run list/show/path, invocation states and exact references, basic computation-store browsing, Python queries and JSON CLI output. |

Deferred: comparisons between runs, GUI, integrated log following, artifact
previews, history deletion/renaming, database/index service, migration of old
records into invented consumer history, retention redesign, requester-aware
pins, retry/coalescing, and inquiry/conclusion ownership. A different run name
does not request independent computation; existing declaration-based reuse stays.

## 3. Identity and address contracts

### Run names

Use `name=` on `Study.submit`, the module-level `submit`, and `Session.submit`.
This names the submission and does not change `Study.name`, which identifies the
authored study definition. `Session.submit_all({name: study, ...})` already has
user-supplied keys: use those keys as submission names and retain its mapping
return shape. Keep `label` as presentation only; do not silently treat it as a
fallback for missing `name`. Nested submissions must also name themselves.

Engineering default: case-sensitive names matching
`[A-Za-z0-9][A-Za-z0-9._-]*`; no trimming, normalization, truncation, or path
separators. Refuse names that cannot fit the target filesystem's component
length with the occurrence suffix; do not invent a universal four-digit cap.
Store `name` and integer `occurrence` separately. Parse a complete run address
at its last dot, requiring a positive canonical decimal suffix. Thus user name
`experiment.2` produces `experiment.2.1`. Exact addressing never means latest.
`runs list --name experiment.2` selects that literal name's occurrences.
`runs show` requires a complete address; avoid ambiguous shorthand parsing.

An address is unique within its history store, not globally. Persist the record
root and workspace-root configuration used by the submission in its header.
A later profile edit must not silently redirect an old run to another store.
Store relocation tooling is deferred; report missing recorded locations.

### Invocation and boundary names

Change Flow authoring, not the saved Plan after submission. Current operation
authoring uses `<sweep-key>:<function-name>` in sweeps, otherwise `invoke:0001`
when no key is supplied. `:04d` is minimum width, not a maximum. Explicit keys
already derive opaque Plan IDs through `_keyed_plan_id`.

Add deterministic automatic keys `<function-name>.<N>` for otherwise unkeyed
operation calls, counting automatic calls of that name within the containing
boundary. Start at 1, no padding. Use the same callable name source as sweep
naming (`authored.__name__`); if it does not satisfy key syntax, ask for an
explicit key rather than silently sanitizing it. Explicit/sweep calls do not
consume the automatic counter. Distinct operations with the same short name
must require explicit naming when their generated namespace would collide;
track the definition identity associated with each automatic name base.

Nested flows currently also use sequential unnamed boundaries. Apply the same
function-name occurrence rule to unkeyed flow boundaries, so complete addresses
contain no opaque ancestor IDs. Operation and flow keys retain the existing
shared namespace per scope. An explicit/generated collision is an error in
either authoring order; never silently skip numbers to resolve it. Preserve
existing explicit and sweep spellings. All new counters and name-base ownership
state must participate in `PlanDraft._checkpoint` and `_restore`.

Generated keys are deterministic for an unchanged construction. Inserting an
earlier call of the same operation can renumber later automatic keys; explicit
keys are recommended for stable meaning across edits. Adding a different
operation must not renumber them. This changes some Plan IDs, not computation
identity. Verify that the latter remains unchanged for equivalent declarations.

Resolve an invocation address from all enclosing boundary keys plus its key.
Store components as an array in history. For CLI presentation, join components
with `/` and encode a literal slash inside a key as `%2F` (percent is not valid
in existing authored keys). Decode once, accepting only this documented escape;
do not URL-decode arbitrarily. This distinguishes key `a/b` from boundary `a`
containing key `b`. Never use the display address as a filesystem path.
Provide a leaf-key shortcut only when unique in that run, otherwise list the
candidate complete addresses. Complete addresses always resolve exactly.

Hedloom preflight requires readable keys on every invocation and its ancestors.
Hand-constructed old Plans with missing keys receive an actionable error before
execution; do not rewrite their IDs/references. Flow's low-level model may still
represent unkeyed Plans for other consumers. Discovery snapshots can include
opaque `invocation_id` for exact machine lookup. Fix `StudyRun.__getitem__`'s
current first-match behavior for duplicate leaf keys using the same resolver.

## 4. Ownership and source map

| Unit/files | Required work |
| --- | --- |
| [Flow authoring](../flow/src/hedloom_flow/authoring.py), [model](../flow/src/hedloom_flow/model.py) | Automatic keys, boundary keys, rollback correctness; retain existing key/ID validation. |
| [Exec attempt](../exec/src/hedloom_exec/attempt.py), [durability](../exec/src/hedloom_exec/durability.py) | Narrow observation of the actual selected try before blocking execution. No run names or history format in Exec. |
| [Exec journal](../exec/src/hedloom_exec/journal.py), [reuse](../exec/src/hedloom_exec/reuse.py), identity and artifact helpers | Consume readers and exact-try path rules. Extend a public read-only helper if needed instead of copying private rules. |
| [Run driver](../run/src/hedloom_run/driver.py), [graph](../run/src/hedloom_run/graph.py) | Forward selection observations with invocation context in both kernels; retain references on post-selection errors. |
| [Site](../run/src/hedloom_run/site.py) | Configure and propagate `history_root`; no history persistence implementation here. |
| [Study](../src/hedloom/study.py), [Session](../src/hedloom/session.py) | Submission preflight, lifetime, required name, recording, observer composition, final accounting and history status. |
| New `src/hedloom/history.py` | Versioned history models, allocation, atomic persistence, folding, serializable live selection sink. Split only if size warrants it. |
| New `src/hedloom/discovery.py`, [CLI](../src/hedloom/cli.py) | Structured read-only queries, address resolution, thin command formatting. |

Exec still imports neither Hedloom nor Run nor Dask. Flow remains executor-neutral.
Run accepts a narrow observer contract; it does not understand the history layout.
The facade owns the join of a run/invocation to shared execution evidence.

## 5. Persistence implementation

### Site and files

Add optional `Site.history_root` and `[study] history_root`, anchored like the
existing roots. Keep it optional for direct lower-level Run users, but require it
for Hedloom submission. Carry it through every `Site(...)` reconstruction
(`served_in_process`, overrides and related helpers), not only `from_file`.
Querying must not create it. Configure history separately from computation and
workspace storage; preflight rejects overlap with those roots for this version.

Use schema version 1 with this layout:

```text
<history-root>/
    allocations/<user-name>/<N>/       # permanent, tiny reservation directories
    runs/<user-name>.<N>/
        run.json                       # immutable header, schema, roots, submission options
        plan.json                      # exact submitted Plan document
        addresses.json                 # invocation ID -> readable component array
        events.jsonl                   # one writer: submitting controller
        selections/<safe-slot>/
            selected.json              # immutable actual record/try selection
            workspace.json             # immutable binding, once known
```

`safe-slot` is the Plan invocation's ordinal in the saved address table. It
is an internal filename, not an operator identity, and must never be parsed from
an arbitrary user path. Header fields include run ID, name, occurrence, UTC
submission time, study name, resolved storage roots, and relevant execution
options (kernel, stop-on-failure). Do not dump transport objects, environment
variables, credentials, or executable Python into history.

### Occurrence allocation

Use persistent reservation directories and atomic exclusive `mkdir`, not
`count(existing runs) + 1`. Scan that name's reservations for its maximum,
attempt `mkdir(N+1)`, and on `FileExistsError` refresh/retry. Do not treat other
I/O errors as contention. Flush the parent directory before treating allocation
as durable. Concurrent threads/processes therefore compete on creation, not on
an assumed process-local lock. Parent directory creation also needs appropriate
durability ordering. Reservations survive all supported cleanup of run content;
this release provides no history deletion command. Manual deletion of allocator
metadata is outside the guarantee. A crash may leave a reserved gap, which is
acceptable and must not be recycled. Assume a shared filesystem with coherent
atomic mkdir/rename; do not claim distributed availability or fault tolerance.

Publish documents through a same-directory temporary file, flush/fsync, atomic
replace, then directory fsync. Fail if required durability cannot be established.
Publish `run.json` last as the ready header after Plan and address table exist.
No operation executes until the complete initial record is durable. Reserved
directories without a ready header are incomplete preparations, never completed
runs. Listing should report preparations separately when their state is useful.

### Events and writer topology

Controller events have `schema_version`, monotonic `seq`, UTC `at`, `event`, and
plain-data `data`. Initial events, invocation outcomes, final accounting and
submission exceptions belong here. Append one encoded line, flush and fsync
before acknowledging persistence. Do not reuse Exec's event enum or its state
machine: run observations have different ownership.

The graph kernel executes inside tasks; its existing controller callback fires
only after tasks finish. Therefore a single controller journal alone cannot
provide the required live link. Use the small selection file above: a
serializable facade sink containing only immutable addresses/configuration is
passed through Run and called from the executing task. It publishes that task's
selection atomically before transport submission. Tasks never append to the
controller's journal. This is a deliberate refinement of the earlier three-file
sketch, avoiding a new messaging service or concurrent JSONL appenders.

Selection contains invocation ID, record, try number, observation time and
selection disposition. Publish as soon as the try is established; publish the
separate workspace binding after it is known, before the blocking transport
call. A binding can explicitly state that no workspace applies. Until a binding
exists, workspace knowledge is incomplete, not an invented directory.

Publish these immutable files with a flushed temporary file and atomic
no-overwrite hard-link publication, then directory fsync and temporary-file
cleanup. On an existing destination, read and compare semantic fields:
identical selection is idempotent (timestamps need not match); a different
record/try or workspace is an explicit conflict. Do not use check-then-replace
for these files. This closes the race even if an unexpected duplicate task
arrives. Require same-filesystem hard-link support for this small metadata
store; refuse initial capability failure. Ordinary mutable controller documents
still use atomic replacement. Do not enable task retries/speculation for this
feature. Normal completion cross-checks selection against the outcome reference.

Outcome events store invocation ID, operation, input digest, placement,
disposition, outcome, selected reference when known, and error. Do not serialize
arbitrary `InvocationOutcome.value` objects into a second result store. Named
outputs resolve using the saved Plan and the selected try's manifest; values,
artifacts and diagnostics remain Exec evidence. Record blockers using Plan
dependencies and the kernel's reported reason/state, distinguishing dependency
failure, stop-on-failure and unreported work wherever evidence supports it.

On final return, reconcile reported outcomes with the accumulated account and
write any missing final outcomes before the final event. Conflicting duplicate
outcomes or references are an integrity error. `run_finished` means every Plan
invocation has a recorded final accounting, including blocked/skipped entries;
it does not mean every computation succeeded. Record execution success and
history completeness separately. Finalize history before automatic retention.

### Failure and reading contracts

- Initial history failure: refuse with a useful error; no body executes.
- Later failure: emit a visible diagnostic and retain structured history errors
  on the returned run. Do not change computation outcomes or stop further work
  solely for this reason. Best-effort persistence of the warning may also fail.
- Selection sink failures must be caught by the observation adapter, not escape
  into the transport failure handler. Carry errors back to the controller with
  the task outcome, including failure paths. A worker-local warning alone is
  insufficient. If the worker dies first, history remains incomplete.
- After a controller append failure, stop appending to that journal in this run;
  do not append beyond a possibly torn tail. Preserve remaining outcomes in
  memory. No successful-history marker may be returned or published.
- If a selection write failed but final evidence later arrives, persist its
  final reference with explicit late provenance; retain the live-history error.
  Do not claim it was available during execution.
- Reader folds complete newline-terminated records. A truncated final fragment
  yields the valid prefix plus an explicit incomplete-tail diagnostic; malformed
  complete lines, sequence gaps, unknown versions, conflicting references or
  missing required documents are corruption/incompatibility, not silently empty
  history. Read-only commands never repair files.
- No final event means completion was not recorded. It cannot distinguish a
  running submitter from a dead one. Expose last observation time, without an
  unearned `running` or `crashed` assertion.
- A selected try can later be terminal in Exec while this run never reported
  completion. Display these as separate facts: `run outcome: unreported` and
  `selected execution: succeeded`, for example. Never infer consumer completion.

## 6. Early selection observation: the critical integration

Invariant: every persisted link names a try actually selected under Exec's
existing protocol, and healthy history exposes it before its blocking launch.

Add a small plain-data selection type and optional observer through
`execute -> launch_or_attach -> _launch_or_attach_locked`. It reports only Exec
facts: record, try, selection disposition, and workspace binding if known.
Do not compute a prospective reference in the facade and call it selected.

Place notifications at these actual branches:

1. New try: after durable `begin_try`, then after workspace binding, before
   `transport.submit`. Keep existing submit-intent-before-transport ordering.
2. Standing reuse: name the manifest's selected try, even if another current
   try exists; notify before returning its result.
3. Repaired terminal reuse: notify the exact repaired/reused try.
4. Attach: notify the established attached try before reconciliation/polling.
   An unresolved submission intent is not yet an established attachment; do
   not publish a guessed selection before discovery resolves it.
5. Preselection refusal (unsupported placement, lost claim, etc.): no link.

Capture the last actual selection in a shared Run adapter used by both kernels.
If launch subsequently raises `SubmissionRefused`, `TransportError`, or another
handled attempt error, the outcome must retain an established selection. Current
catch branches drop all references; update them deliberately. A refused launch
may have a real allocated try worth inspecting, whereas a refused claim has
none. Adjust the current overly broad outcome docstring accordingly.

Define the plumbing explicitly: Exec accepts `on_selection(Selection)`; Run
accepts `on_selection(InvocationSelection)`, enriching the notice with invocation
ID. Add `observation_errors: tuple[str, ...] = ()` to Run's `InvocationOutcome`
for diagnostic delivery. A per-invocation Run adapter captures actual selection,
calls the sink, catches its ordinary exceptions and accumulates these errors.
It is used by both kernels and by every handled-error outcome construction.
Exec's observer delivery must likewise prevent ordinary observer exceptions
from changing its lifecycle; emit a diagnostic if a direct lower-level caller
supplies a failing observer. Do not swallow process-control BaseExceptions.
Hedloom retains precise history failures through Run's adapter, so its normal
path does not depend on warning capture. These diagnostics never enter a
computation bundle or its digest.

The sink must be serializable, contain no open file/lock/client objects, and
use the shared history filesystem. Do not close over a controller journal in
graph tasks or rely on module globals to pass selection information. Supported
cluster configurations must have the history location visible to executing
tasks. Before scheduling computation, verify that visibility with a small
preflight handshake on participating worker processes when not structurally
guaranteed in-process. Use an out-of-band `client.run` call, not a graph task
that needs placement capacity and could deadlock nested submission. The
controller writes a fresh internal challenge token; each worker reads it and
atomically publishes its acknowledgement back into the run's preflight area;
the controller verifies those files. This proves visibility in both directions,
not just that an identical path exists. Remove completed handshake files after
validation. Tokens are internal protocol data, never operator/run identities.
A worker that cannot reach the history directory is an initial persistence
failure. This is not a remote history service.

Observer errors are diagnostics with the continue-computation policy above;
observers do not acquire scheduling authority. Preserve existing user
`on_event(InvocationOutcome)` behavior and exception semantics. Use a distinct
selection observer; do not change that callback into a heterogeneous event API.

## 7. Facade and query surfaces

Add `run_id` and `history: HistoryPersistence` to `StudyRun`, with the history
location, status (`complete` or `degraded` for a returned run), and immutable
errors. A selection write failure keeps status degraded even if its final
reference was recorded later. On escaping exceptions, attach the available
history descriptor without replacing the original error or its existing partial
`report`. A durable best-effort interruption event does not assert all work
stopped. Add an optional `on_started` callback carrying an immutable run
reference, invoked after durable initialization and before execution. Print the
run address immediately to stderr under existing watch output. Other processes
can always discover it through `runs list`; library calls need not print by
default. Pass required `name` and the startup hook through all facade wrappers.
Make omission fail before compute starts. Update first-party callers, examples,
fixtures and affected root study scripts; do not conceal the new requirement
with a default equal to `Study.name`.

Provide immutable query results through public facade APIs, for example
`RunHistory.list_runs`, `read_run`, `list_invocations`, and `resolve_path`.
Keep presentation out of these functions. Include `history_status`,
`run_reported_outcome`, and `selected_execution_state` as separate fields rather
than deriving one overloaded status. An inspection snapshot is data, not
an executable reconstructed `Study`. Loading requires neither the original
authoring module nor a Dask client. Resolve named output ports through the
existing binding semantics; preserve unavailable versus successful `None`.

CLI contract (put `--site` on leaf commands consistently):

```sh
hedloom runs list --site site.toml
hedloom runs list --site site.toml --name investigate-start --study STUDY --since 2026-09-06
hedloom runs show --site site.toml investigate-start.2
hedloom runs show --site site.toml investigate-start.2 --invocation prepare.1
hedloom runs path --site site.toml investigate-start.2 --invocation prepare.1 --workspace
hedloom runs path --site site.toml investigate-start.2 --invocation prepare.1 --journal-dir
hedloom attempts list --site site.toml --since 2026-09-06 --outcome failed
hedloom attempts show --site site.toml --record RECORD --try 3
```

`runs show` includes the invocation table; no separate invocation-list command
is required. List/show support `--json` from the same structured query results.
List newest submissions first, using timestamps for order, not lexical `.N`.
Define date-only `--since` as UTC midnight; accept timezone-explicit timestamps.
Expose complete run addresses, authored study labels, readable invocation
addresses, actual execution state, run-reported outcome and persistence status.

Path commands require exactly one target switch and one unambiguous invocation.
Success prints exactly one absolute existing directory plus newline to stdout;
failure prints no path, a diagnostic to stderr and a nonzero status. Support:

```sh
cd "$(hedloom runs path --site site.toml investigate-start.2 \
    --invocation prepare.1 --workspace)"
```

No selection, not-yet-created workspace, no workspace for a value-only call,
inaccessible/missing workspace and positively recorded reclamation must be
distinguished. Missing is not automatically reclaimed. Use Exec's read-only
workspace naming/location rules, including fallback roots; never create a
workspace during resolution. `--journal-dir` points at the selected record's
journal directory; show its selected try separately since one journal has many
tries. Existing pin/prune selector syntax is unaffected by the new run-address
separator choice.

The computation browser must expand every folded try, not just `scan_attempts`'
standing projection. Include recorded operation, times, state, standing status,
pins and payload availability where known. Old readable records without consumer
history remain accessible here; do not assign a study owner to them.

Reads must not claim the journal lock held by a long-running launch. Concurrent
reads may see a partial final append: expose an incomplete observation rather
than blocking or silently repairing it. If existing strict Exec readers need a
read-only snapshot helper for this, keep strict mutation/recovery readers strict
and test the new snapshot behavior separately.

## 8. Implementation sequence and acceptance gates

### Phase A — readable authoring

Implement automatic operation/flow keys and canonical facade address resolution.
Update authoring tests whose numeric-ID expectations are intentionally replaced.
Retain explicit-key tests. Cover automatic counters beyond 9999, unrelated-call
insertion, same-name insertion, distinct scopes, collision in both authoring
orders, ambiguous leaves, slash-containing keys, and rollback after a nested
flow fails. Verify equivalent declarations still select equal computation IDs.

### Phase B — isolated history storage

Implement models, allocator, file publication, writer and read-only fold. Add
`history_root` parsing/propagation. Tests must include concurrent allocation by
separate processes, reserved gaps, missing run directories with retained
reservations, names ending in `.N`, initial-write failure, partial preparation,
torn tail, corruption, unknown schema and missing payload references. Use
temporary files and injected failure points; no farm needed.

### Phase C — selected-try observation

Implement Exec hook and common Run adapter, including graph task propagation.
Test fresh selection, reuse of standing versus current, attachment, preselection
refusal, transport refusal after allocation, observer-write failure, and
agreement between early reference and final reference. Verify the observer
sees the selected try before entering a deliberately blocking transport.
Keep existing identity, intent/publication ordering and claim tests passing.

### Phase D — submission recording

Integrate required names, preflight, selection sink, controller outcomes,
startup callback and finalization through Study/module/Session/submit_all and
nested callers. Test parallel same-name submissions, reused runs, callback
composition, mid-run history failure without altered computation behavior,
escaping exceptions/partial reports, and finalization before retention. Verify
every Site transformation keeps the history root. Update first-party consumers
and meaningful signature assertions rather than mass-replacing arbitrary text.

### Phase E — operator queries and end-to-end evidence

Implement Python queries then thin CLI. The decisive scenario is a child
process running a body that writes a partial file and waits on a bounded test
barrier. From a separate process, discover the run, select its readable
invocation and read that file via the reported workspace path BEFORE releasing
the barrier. Exercise sequential and graph kernels. Use synchronization signals
and timeouts, not timing-only sleeps; always clean up test processes.

Also demonstrate:

- Two submissions (including different Study definitions) point to one reused
  try while keeping separate names, Plans and histories.
- Later standing changes never redirect an older run's selected reference.
- A killed submitting process leaves inspectable selection and visibly
  incomplete consumer accounting, even if Exec later shows terminal evidence.
- Inspect failed and blocked work without initially knowing any authored key.
- Reclaimed versus absent payload, successful value-only output including
  `None`, named exported ports, and no-workspace path failure.
- Path stdout is shell-usable, errors leave stdout empty, ambiguous addresses
  refuse, and every query leaves files/mtimes unchanged and launches no work.
- Graph worker visibility failure refuses before invocation execution.

### Phase F — contracts, documentation and final verification

Update Hedloom's ontolome to make run history an implemented join with bounded
evidence. Update Flow's automatic-key and cross-edit-stability account, Run's
observation/Site contracts, and Exec's narrow observation contract and affected
DECISIONS entries. Keep components at prototype maturity. Update maintained
README/docs, CLI help and examples for required names/history roots and live
inspection. Do not rewrite old design records or the original user comments.

Run focused tests after each phase. Once integrated, run all four suites from
Hedloom using the repository's suitable interpreter:

```sh
PYTHONPATH=src:flow/src:exec/src:run/src python -m pytest -q tests exec/tests run/tests flow/tests
```

Build composed docs from the containing repository with `python composition.py
docs` and inspect warnings. The workspace previously needed its `.toolchain`
virtual environment for Sphinx; verify availability instead of assuming system
Python has it. Run applicable root integration checks if root consumers change.
No real farm smoke is required for this implementation; accurately distinguish
local/fake evidence from real scheduler evidence.

## 9. Completion report expected from the implementing agent

Report implemented contracts, changed areas, exact validation results/skips and
known limits. Include one actual run address and the commands used to inspect
it while its invocation was still executing. Call out any material departure
from this plan with its evidence. Do not declare success on terminal-only
discovery, a UUID-backed operator address, a latest shortcut, or graph callbacks
that reveal selection only after task completion. Those miss the user's need.

Planning verification: source and user comments inspected; no runtime probes,
tests, implementation edits or farm actions were performed to author this file.
