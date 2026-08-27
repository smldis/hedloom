# Phase 1 census: record / workspace split

**Measured 2026-08-27 against `0fec414`, the head of hedloom#7.** This is a
code census, not a description of implemented behaviour. No Phase 1 source or
test change was made during the census.

## Verdict

The source design is implementable, and the four failure modes called out in
the brief are present at the stated locations. Two delivery requirements are
currently mutually exclusive, however, and one proposed Phase 1 type refers to
a Phase 4 type that does not exist yet. Under the requested stop rule, Phase 1
must not be implemented until the authority resolves these points.

1. **The required register edit is outside this repository.** The Phase 1
   contract list requires `../docs/vision/open-concepts.md` to move. Its real
   path is
   `/home/smldis/working/AI/analog-sim-studies/docs/vision/open-concepts.md`,
   owned by the outer `analog-sim-studies` Git repository. `git ls-files` from
   Hedloom refuses the path as outside the repository, so a Hedloom PR cannot
   carry it. The same brief says not to commit anything in the outer repo (and
   the original repository boundary says not to touch it). Leaving the register
   stale violates the contract-change rule; editing or committing it violates
   the repository boundary.
2. **The prerequisite is not on `main`.** GitHub reports hedloom#7 as `OPEN` at
   `0fec414`; `origin/main` remains `904ec5c`. A Phase 1 branch made from current
   `main` lacks Phase 0/0b, including `workspace_path`, component digests,
   authored keys, supersession, aliases, and lineage. A branch containing those
   prerequisites must instead be stacked on #7 and its PR would include another
   phase when compared with `main`. That contradicts both “branch off main” and
   “Phase 1 must not share a PR with anything else.” This census branch is
   temporarily stacked on #7 solely so it can inspect the actual prerequisite
   code; no Phase 1 implementation was started.
3. **`AttemptState.pins` is specified four phases before its type exists.** The
   Phase 1 signature at plan line 836 requires `pins: tuple[Pin, ...]`, while
   `Pin` is first defined by Phase 4 at plan line 1112 and there is no pins
   module or pin event today. Defining a placeholder in Phase 1 would implement
   part of Phase 4; omitting it changes the written signature. The intended
   resolution should be recorded (the narrow resolution is to add `pins` with
   Phase 4).

The rest of this census records the implementation surface so work can resume
without repeating discovery once those blockers are answered.

## Identity and allocation

| Exact location | What it does today | What Phase 1 must do |
| --- | --- | --- |
| `exec/src/hedloom_exec/identity.py:18` | Exports only `AttemptIdentity`, `IdentityError`, and `attempt_identity`. | Export `try_name` and `parse_try_name`. |
| `exec/src/hedloom_exec/identity.py:29-37` | `AttemptIdentity` carries `sequence`; the rendered value names both record and try. | Remove `sequence`; the object names the content-addressed record only. |
| `exec/src/hedloom_exec/identity.py:62-101` | `attempt_identity(..., sequence=0, ...)` validates and hashes the sequence. | Remove the public argument while preserving **exactly** the old sequence-zero rendered value. The internal hash material must retain the literal zero slot; merely deleting it would invalidate every Phase 0 identity. Add strict try-name formatting/parsing. |
| `exec/src/hedloom_exec/durability.py:57-89` | `_select_sequence` probes up to `max_attempts` record identities and chooses the first unfinished or reusable one. | Delete it. Derive one record identity, then allocate or resume a try under that record's held claim. |
| `exec/src/hedloom_exec/durability.py:111-124` | `execute` accepts `max_attempts=20`. | Remove `max_attempts`; retained try count is unbounded. |
| `exec/src/hedloom_exec/durability.py:160-203` | Chooses a sequence before the claim, records it in the bundle, and only treats sequence zero as a new lineage record. | Derive the record once from `(plan, invocation, digest)`. Compute supersession only when that record is genuinely new. Try selection cannot occur here unlocked. |
| `tools/attempt-census.py:22-25,88-107` | Reconstructs old identities by probing 64 sequence hashes and groups one directory per try. | Read the declared layout and the per-record folded tries directly; remove `MAX_PROBE` and sequence-based identity reconstruction. This reader is additional coupling not named in the Phase 1 item list. |

`identity.py:89-93` confirms an important compatibility detail: “identity is
what sequence zero already returns” does **not** mean hashing only three text
components. The old canonical material has four slots and the third contains
`"0"`. Keeping that byte sequence is required by the Phase 0 hard-coded
identity regression at `exec/tests/test_created_event.py:101-110`.

## Record layout, fold, and manifests

| Exact location | What it does today | What Phase 1 must do |
| --- | --- | --- |
| `exec/src/hedloom_exec/journal.py:31-56` | Recognises a flat event vocabulary with no try-boundary event. | Add `try_started`; all try-scoped events must record a try number. Later pin events remain Phase 4. |
| `exec/src/hedloom_exec/journal.py:79-110` | One `AttemptState` mixes record lifetime with the latest phase; cancellation, reuse acceptance, and observations are flat sticky fields. | Add `TryState`, `tries`, `current_try`, and compatibility projections from `AttemptState.current`. Preserve cancellation and reuse reasons even though the Part 3 sketch omits them: `attempt.py:168-172` and `:365-373` consume `cancel_reason`, and existing attribution tests consume `reuse_reason`. |
| `exec/src/hedloom_exec/journal.py:142-148` | One directory contains `events.jsonl`, `claim.lock`, and fixed `manifest.json`; there is no declared layout. | Add a checked `layout` file, `manifest/<try>.json`, and `standing.json`. Expose a per-try manifest resolver rather than one mutable `manifest_path`. |
| `exec/src/hedloom_exec/journal.py:153-245` | `claim()` creates the record directory and holds `flock`, but the object does not know whether this caller holds the claim. | Create/validate layout 1 at the record boundary and track claim ownership so `begin_try(journal)` can raise `ClaimNotHeld`. A pre-existing directory with no recognised layout must be refused; there is no migration read path. |
| `exec/src/hedloom_exec/journal.py:247-276` | `append` flushes and fsyncs every event but permits append without the claim and does not attribute events to a try. | `begin_try` must append and fsync `try_started` before returning. Try-scoped writers supply the current try explicitly; the record-level `created` remains once-only and carries the first try as Phase 0 specified. |
| `exec/src/hedloom_exec/journal.py:313-382` | One last-write-wins fold plus sticky `cancel_requested`, `reuse_accepted`, and `observations`. | Fold each try independently. A new try resets every try-scoped field, while `AttemptState.phase`, `outcome`, handle, transport, substrate, placement, cancellation, acceptance, reasons, and observations project only the current try. |
| `exec/src/hedloom_exec/journal.py:384-459` | Every terminal publication writes `manifest.json.partial`, replaces `manifest.json`, then appends terminal; a later try overwrites earlier evidence. | Publish to `manifest/<try>.json` and update `standing.json` only for a reusable result. Recovery must repair journal/standing state from the correct per-try manifest without overwriting another try. |

The plan's sticky-field warning is exact. At `journal.py:347-356`, cancellation
and acceptance only ever become true and observations only accumulate; nothing
resets them when `submit_intent` resets phase at `:331-346`.

The Phase 1 sketch also lists `started_at` and `ended_at`. `started_at` is the
`try_started` event timestamp and `ended_at` is the terminal event timestamp;
neither fact exists as a folded field today.

## Claim, launch, recovery, and publication

| Exact location | What it does today | What Phase 1 must do |
| --- | --- | --- |
| `exec/src/hedloom_exec/attempt.py:107-114` | `is_reusable` reads record-wide sticky acceptance and one fixed manifest. | Evaluate the standing/current try only. Acceptance of an older try cannot leak forward. |
| `exec/src/hedloom_exec/attempt.py:117-131` | `accept_for_reuse` reads and appends without a claim (the known gap). | Hold the record claim, target the current terminal try, append acceptance with `try`, and atomically make that try the standing result. |
| `exec/src/hedloom_exec/attempt.py:143-166` | `launch_or_attach` owns the claim; callers cannot prepare a try-specific workspace between allocation and submission. | Allocate/resume the try and prepare its workspace while this same claim is held. No unlocked caller may choose a try number. |
| `exec/src/hedloom_exec/attempt.py:168-198` | Cancellation, terminal repair, spent-result handling, and error paths all refer to one flat state and fixed manifest. | Apply them to `state.current`; use the current per-try manifest and the standing pointer. A terminal non-reusable try leads to allocation of the next try, not `AttemptSpent` at the high-level path. |
| `exec/src/hedloom_exec/attempt.py:200-219` | Crash-window discovery calls `transport.discover(journal.identity)`. | Call discovery with `try_name(journal.identity, state.current_try)`. This is the duplicate-work hazard identified in the plan. |
| `exec/src/hedloom_exec/attempt.py:221-257` | Writes `created`, placement, and `submit_intent`, then submits using the record identity. | Ensure `try_started` is already durable; write every try-scoped event with that try; submit the try name. `created` remains once per record. |
| `exec/src/hedloom_exec/attempt.py:306-320` | Cancellation appends without a claim and delegates using the current handle. | Hold the claim and append cancellation against exactly the current try. The handle and LSF cancel name must be the try name. |
| `exec/src/hedloom_exec/attempt.py:323-419` | Reconciliation reads one manifest/current flat state; `write_diagnostics` at `:381` is outside the `MissingOutput` guard. | Reconcile/publish the current try. Guard diagnostics failure so completed work still receives terminal evidence; retain output capture semantics and never pre-create declared outputs. |

There is an implementation seam the plan leaves to the code: the workspace
cannot be prepared in `durability.execute` before the try is allocated, yet the
try must be allocated under the claim owned by `launch_or_attach`. The existing
private locked path at `attempt.py:159` is the natural place to allocate and
prepare before `submit_intent`; doing an unlocked preview in `execute` would
violate the stated ordering. This is not a design contradiction, but it rules
out a superficial edit confined to `_select_sequence`.

## Workspaces and aliases

| Exact location | What it does today | What Phase 1 must do |
| --- | --- | --- |
| `exec/src/hedloom_exec/durability.py:220-240` | Creates the workspace and points aliases using the record identity before entering the claim. | After allocation, create `workspace_path(workspace_root, try_name)` and point aliases at that try's declared paths before the transport call. A resumed unsubmitted try reuses its number and workspace. |
| `exec/src/hedloom_exec/artifacts.py:76-91` | Resolves/creates a workspace for the supplied name; the helper itself is already compatible. | Pass a try name. Keep `workspace_path` pure and do not pre-touch outputs. |
| `exec/src/hedloom_exec/artifacts.py:94-165` | Treats an existing declared file as production evidence. | No semantic change. This is why alias preparation must create only the symlink, never the target. |
| `exec/src/hedloom_exec/alias.py:47-71` | Atomically repoints to any target, including a dangling one. | No API change; call it once the try workspace address is known so every try repoints `latest/`. |
| `exec/src/hedloom_exec/lineage.py:67-86` | Detects current records by finding a record identity as a complete path component in alias targets. | Parse the alias target's try-name component and map its base record explicitly; a try suffix means the record identity is no longer itself a path component. |
| `exec/src/hedloom_exec/lineage.py:89-123` | Reports the Phase 0 `created.try` (always the record's original try) and flat record outcome. | Report the current/standing try and outcome from the folded record while keeping one iteration per content identity. |
| `exec/src/hedloom_exec/lineage.py:126-147` | Recognises stale paths only when the record identity is a complete workspace path component. | Parse a try workspace name before looking up the record, or every `hedloom check` path becomes unknown after the split. This is additional silent coupling not called out in Phase 1 item 3. |

## Transport, farm discovery, cancellation, and watch keys

| Exact location | What it does today | What Phase 1 must do |
| --- | --- | --- |
| `exec/src/hedloom_exec/transport.py:109-145` | Names the transport argument `identity`; no distinction exists between record and job. | Document the value as a try name. The protocol shape need not import record logic. |
| `exec/src/hedloom_exec/transport.py:173-203` | In-process results are keyed by the submitted identity. | Key by try name, just like external jobs, so recovery and cancellation exercise the same name split. |
| `exec/src/hedloom_exec/lsf.py:448-510` | `bsub -J` receives whatever identity the caller supplies. | Require a valid try name and pass it as `-J`; refuse a bare record identity. |
| `exec/src/hedloom_exec/lsf.py:512-541` | `bjobs -J` queries whatever identity the caller supplies. | Require/query a try name. An authoritative negative remains “never accepted” only for that exact try. |
| `exec/src/hedloom_exec/lsf.py:543-581` | Poll rediscovers and `bkill -J` cancels the handle's identity. | Handles must carry the try name; validate it so record-name cancellation is refused rather than silently ineffective. |
| `exec/src/hedloom_exec/watch.py:89-141` | One record-level `observations.jsonl` stores transitions with no try attribution and suppresses duplicates across the file's whole lifetime. | Store a try number/name on each observation and deduplicate within that try, or try 1 can inherit try 0's state. |
| `exec/src/hedloom_exec/watch.py:144-206` | `AttemptStatus.identity` is both record key and expected farm job name; submission time is the first intent in the whole record. | Carry the record identity and current try/job name separately; derive submission/running timestamps for that try only. |
| `exec/src/hedloom_exec/watch.py:209-227` | Scans record directories guarded by `events.jsonl`; live-ness comes from flat state. | Keep the load-bearing guard and emit at most the current live try per record. |
| `exec/src/hedloom_exec/watch.py:282-314` | Matches `bjobs` state with `states.get(item.identity)`. | Match by the try/job name and write the observation against that try. Removing the suffix must make the new tripwire test fail. |

The three silent job-name breaks asserted by the plan are all real:
`attempt.py:204` discovers by record identity, `lsf.py:581` cancels the identity
stored on the handle, and `watch.py:303` matches queue job names against the
record identity.

## Readers and public compatibility

| Exact location | What it does today | What Phase 1 must do |
| --- | --- | --- |
| `exec/src/hedloom_exec/reuse.py:116-134` | `AttemptRecord` represents one attempt directory/try and carries a created-event try number. | Represent one content record, projecting the folded standing/current try for compatibility. Later pruner phases will read the full `state.tries`. |
| `exec/src/hedloom_exec/reuse.py:137-173` | Scans one directory per old try and reports one flat outcome. | Require recognised layout, scan one record, and report its current/standing outcome without mistaking `manifest/` for a record. |
| `src/hedloom/cli.py:91-161` | `where`, `check`, and `log` consume `AttemptRecord`/`Iteration` and assume identity-named workspaces indirectly. | No operator signature change, but their readers must return try-specific workspaces and outcomes correctly. |
| `run/src/hedloom_run/graph.py:467` | Comment says `_select_sequence` chooses identity inside `execute`. | Update the explanation: `execute` derives a record and allocates a try under its claim. Runtime imports stay unchanged. |
| `exec/src/hedloom_exec/__init__.py:15-45` | Re-exports identity and journal public names. | New `TryState`, allocation error/API, and try-name helpers become public through existing wildcard composition. |

No additional attempt-root reader lacking the `events.jsonl` guard was found.
`scan_attempts` (`reuse.py:149-152`) and `live_attempts`
(`watch.py:221-224`) remain the only general scans. The census tool has its own
equivalent guard at `tools/attempt-census.py:58-60`.

## Tests to replace, adapt, and add

The complete Phase 1 blocks in Part 3 name 46 tests across identity, record,
allocation, fold partitioning, recovery names, watcher keys, LSF, and claims.
All are applicable, subject to the unresolved `pins` signature above.

Deliberate replacements:

- `exec/tests/test_failure_reuse.py:54-72` currently asserts failures occupy
  separate attempt directories. Replace the file with `test_record.py`; the new
  contract is one record, several try workspaces and manifests.
- `exec/tests/test_failure_reuse.py:112-118` specifies `max_attempts`
  exhaustion. Remove it and add the unbounded-retention test.
- `exec/tests/test_failure_reuse.py:121-131` says “fresh sequence”; rewrite it
  to assert changed input creates a different record at try zero.
- `exec/tests/test_identity.py:6-24,34-47` accepts and distinguishes sequence
  in record identity. Replace those assertions with the nine identity/try-name
  tests listed in Part 3.

Existing protocol suites needing mechanical contract adaptation rather than
deletion:

- `exec/tests/test_attempt.py:38-180` and
  `exec/tests/test_failure_injection.py:31-170` construct low-level journals;
  they must create/allocate a try and address fake substrate jobs by try name.
- `exec/tests/test_journal.py:12-97` writes events without try boundaries and
  asserts `manifest.json`; rewrite around layout 1, `try_started`, per-try
  manifests, and the standing pointer while retaining append/fsync/recovery
  assertions.
- `exec/tests/test_watch.py:46-233` manually writes flat live records and one
  record-level observation stream; add try attribution and preserve the
  observer-does-not-decide-outcomes invariant.
- `exec/tests/test_lsf.py:70-146`, `exec/tests/test_fake_farm.py`, and
  `exec/tests/test_review_fixes.py` use bare strings such as `hedloom-abc` as
  job names. Job-facing cases must use valid rendered try names; explicit
  refusal tests retain bare record names.
- `exec/tests/test_created_event.py:45-138` remains a Phase 0 compatibility
  tripwire. `created` is still once per record, and its old identity constant
  must not move.
- Alias and lineage tests at `exec/tests/test_alias.py:67-189` and
  `exec/tests/test_lineage.py:89-181` must assert try-suffixed targets while
  preserving the no-pre-touch and A-to-B-to-A currentness properties.

Part 3 does not list direct tests for item 7's `layout` file. Phase 1 also needs
tests that a new record writes layout 1, a recognised record reads, a missing or
unknown layout is refused, and no legacy fallback occurs. Those are required to
make the stated no-migration contract executable.

## Contract and documentation moves

Within the Hedloom repository, the exact maintained surfaces are:

- `exec/ONTOLOME.md:54-81,93-104,129-143,166-201,207-230` — separate record
  identity from try/job name, specify per-try state and manifests, layout 1,
  unbounded tries, standing reuse, per-try cancellation/acceptance/watch state,
  and try workspaces.
- `ONTOLOME.md:56-60,76-85,109-123,143-152` — the façade exposes a workspace
  for one try while content identity remains stable; remove any implication
  that one attempt identity is also the job/workspace name.
- `exec/DECISIONS.md:16-27,91-159,184` — recovery evidence is by try name;
  replace “sequence exists in identity” and `max_attempts` reasoning.
- `README.md:67-80` and `exec/README.md:3-33,37-59,121-146` — document the new
  tree and say plainly that roots from older layouts are not readable.
- `docs/internals/mechanism.md:90-166,207-218` — identity chain ends in a
  content record; try name is separate formatting and the batch key.
- `docs/internals/attempt-claim-protocol.md:18-46,62-83,116-186,262-285` — one
  claim covers every try, `try_started` precedes intent, discovery names the
  try, and publication evidence is per try/standing.
- `docs/internals/stop-admitting-protocol.md:112-126` — describe record identity
  versus current try when matching outstanding work.
- `docs/guide/results.md:31-92` — explain record, per-try workspaces/manifests,
  standing result, layout refusal, and aliases to a try workspace.

The required outer register is
`../docs/vision/open-concepts.md:20-26,44,50-51,80,308-313`. It currently says
job name equals attempt identity, records are per-attempt workspaces, watcher
observations are per attempt, `max_attempts` remains, and workspace collection
is open. It must move with Phase 1 according to the brief, but cannot be part of
the Hedloom commit or PR under the current repository boundary.

## Constraint checks

- The identity can remain byte-for-byte equal to the pre-Phase-1 sequence-zero
  identity; no content-addressed input is weakened.
- No output needs pre-creation. Existing alias and capture APIs already support
  the required honest dangling window.
- All identified implementation work stays in `hedloom_exec` plus neutral
  caller/docs changes; no `hedloom_flow`, Dask, simulator, circuit, or domain
  dependency is needed.
- Directory-output support is not needed and remains out of scope.
- `scan_attempts` and `live_attempts` retain their load-bearing journal guards.

## Required authority before implementation

1. Land hedloom#7 on `main`, or explicitly authorise a stacked Phase 1 PR and
   say what its base branch should be.
2. Decide how the mandatory outer `open-concepts.md` update is to be delivered
   without violating the no-outer-repo-commit rule (for example, authorise a
   separate outer-repository commit/PR, or remove it from this Hedloom commit's
   required surfaces).
3. Confirm that `AttemptState.pins` is deferred to Phase 4; otherwise Phase 1
   necessarily contains part of the pin model.
