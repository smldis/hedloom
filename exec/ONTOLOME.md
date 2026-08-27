# Hedloom Exec Ontology

## Purpose and scope

Hedloom Exec owns the durable lifecycle of one record at one planned invocation
and the tries made beneath it: a content-addressed record identity, an
append-only account of what each try intended and observed, immutable terminal
evidence, and reconciliation that decides whether a try may be claimed,
attached to, retried, or completed from existing evidence.

Launched work is **owner-bound**: it is not meant to outlive the caller that
started it, and a caller crash should take it down. The durable record
therefore exists for evidence and for skipping work already validly done, not
for reattaching to work still running from a previous life. The unit was first
built on the opposite premise; see `DECISIONS.md` for that correction and what
survived it.

## Mode of being

**Development state:** `prototype`

The unit studies whether a durable record can carry attempt evidence and result
reuse without absorbing any graph scheduling authority. Its evidence is the two
failure injections named by the architecture — acceptance without receipt, and
terminal state without a recorded manifest — reproduced locally against a fake
substrate, plus a boundary test showing that reconciliation reads no topology.

Those injections were designed under the superseded detached-lifetime premise.
They remain valid evidence about the protocol's behaviour in the indeterminate
window, which owner-bound lifetime narrows but does not remove: a submission
whose outcome is unknown may still have created work that needs killing.

Two substrates exist: in-process execution, the honest degenerate case where
work cannot outlive its caller, and direct `bsub -I` submission, which has
never contacted a real cluster. Pooled execution refuses.

The unit also now studies whether a sweep can be watched without being
disturbed. `hedloom_exec.watch` is an observer over records it does not own; its
evidence is that the owner's log is untouched after observation and that a
deliberately false observation changes neither an outcome nor what reuse
returns. Like everything else here that names `bjobs`, its parsing has never
met a real farm.

The unit now also studies whether content-addressed identity derived from a
Plan document makes rerunning honest. `examples/planned_refinement.py`
is the current evidence: three points and a reduction run, rerun with nothing
recomputed, then one point refined so that it and the reduction rerun while
its siblings are reused and the superseded results stay nameable.

## Current contracts

- Distribution: `hedloom-exec`, independently installable on Python 3.10 or newer,
  with no dependencies. It does not import `hedloom_flow`.
- `attempt_identity(plan_id, invocation_id, input_digest)` is a pure stable
  record identity. Phase 1 deliberately removed the old sequence hash slot, so
  every identity rendering changed. There is no migration: roots written by an
  earlier phase are unreadable and must not be mixed with layout-1 roots.
- A record is named by that identity. A try workspace and every external job
  are named `<record>-<n>`; discovery, cancellation, and watcher matching use
  that try name, never the bare record identity.
- Every record declares layout version 1 and contains an append-only
  `events.jsonl`, immutable `manifest/<n>.json` terminal evidence, and an
  atomically replaced `standing.json` selecting reusable evidence. State is
  derived by folding events separately into `TryState` values; cancellation,
  reuse decisions and observations never leak between tries.
- `journal.claim()` holds the record exclusively (advisory lock) across fold,
  try allocation, submission and publication. `begin_try()` reserves and
  records the next unbounded try while that claim is held, and flushes it before
  any transport call. An allocated try with no submission intent is resumed,
  not skipped. Calling it without the claim is refused.
- `submit_intent` is durably flushed before any transport call. A per-try
  `terminal` event is recorded only after its manifest is atomically visible.
  The log's first write and manifest rename fsync their containing directories.
  A second caller gets `ConcurrentClaim` rather than a wait, because a duplicate
  submission is the defect being prevented.
- A cancellation blocks only its try (`AttemptCancelled`), and a
  record created from different inputs is refused at the journal boundary
  (`StaleIdentity`) rather than only in `execute`.
- `launch_or_attach(...)` resolves to exactly one of `claimed`, `attached`, or
  `completed`, or raises. `UnrecoverableAttempt` is a supported outcome, not a
  defect: it reports a substrate that cannot answer questions about its own
  accepted work.
- A transport declares `discovery_is_authoritative`. This governs the negative
  answer only; a positive discovery is always usable, because the identity
  predates the submission that created the match.
- `SubmissionRefused` is the only submission failure that permits resubmission
  without discovery. Every other failure is indeterminate and holds the attempt
  in the crash window.
- `request_cancel(...)` records intent before contacting the substrate and is
  idempotent. Cancellation is never established by the call returning.
- `reconcile(...)` publishes disagreement between the record and the substrate
  as the terminal outcome `unreconciled` rather than normalizing it into
  success or failure.
- LSF handles carry a `kind`: `completed` for a finished `bsub -I` submission,
  `live` for a job found by discovery. `poll` asks LSF about a live handle
  rather than assuming absence, and a rejected submission raises
  `SubmissionRefused` instead of being published as the work failing. A
  `bjobs` that cannot answer raises rather than reporting "never accepted".
- `CommandUnavailable` is indeterminate. Only a missing `bsub` becomes a
  refusal, because only then is it certain nothing was accepted.
- `hedloom_exec.lsf.LSFInteractiveTransport` submits one `bsub -I` job per try
  with `-J <record>-<n>` and a mandatory `-W` walltime, and waits for it. LSF
  binds the job to the submitting client; the client stays in this process's
  group and requests `PR_SET_PDEATHSIG` on Linux, so the job does not survive
  its owner. External work is a command line, not an in-process callable.
- What a job asks for is resolved per invocation. `placement_options(bundle)`
  reads what the Plan requested; `settings_for(...)` resolves it over the
  transport's site defaults, which the invocation's own options win against.
  `app`, `queue`, `cores`, `memory_mb`, `licences`, `resources`, and `walltime`
  become `-app`, `-q`, `-n`, `-R`, and `-W` on that job alone, and the resolved
  `JobSettings` is carried on the handle into the published manifest, so a run
  can be asked what it requested without reparsing a command line.
- A declared licence is a request LSF arbitrates, not a count this unit keeps:
  `licences={"<name>": n}` becomes a `rusage` term, because the scheduler is
  the only party that knows how many exist and who holds them. Resource names
  are the site's, never invented here, and a name that could not be one is
  refused rather than interpolated into a requirement string.
- The placement vocabulary is closed. An option this transport cannot express
  as a `bsub` argument is refused before submission, because dropping a stated
  resource need would run the work under conditions nobody asked for. Two
  `rusage` sections are refused for the same reason rather than merged by
  guesswork; other sections, such as a site's `span[...]`, compose into the one
  requirement string LSF's grammar allows.
- `hedloom_exec.lsf.LSFPooledTransport` is a refusing boundary. Pooled execution
  should adopt `dask_jobqueue.LSFCluster` rather than reimplement worker
  lifetime.
- Neither is reexported from the package initializer: reaching an external
  scheduler is an explicit import.
- `input_digest(...)` digests only the bundle keys that determine a result:
  operation, command, arguments, cwd, declared inputs, and explicitly nominated
  `identity_env`. Queue, walltime, cores, host, and general `env` are excluded,
  so changing where work runs never invalidates what it produced.
- `input_digests(...)` records one explanatory digest for each of the nine
  identity-bearing keys. These are additional evidence only: the aggregate
  `input_digest(...)` remains the same BLAKE2b over the same canonical mapping.
- Attempt identity may be content-addressed by folding that digest in. Reuse is
  then sound by construction: a manifest at an identity was produced by exactly
  those inputs, and changed inputs land elsewhere rather than colliding.
- `stale_attempts(...)` names prior results for an invocation whose inputs have
  since changed. Superseded work is retained and explainable, never silently
  overwritten.
- A new record's `created` event also carries its authored key, component
  digests, and the prior different-digest identity it supersedes. All are
  attribution: none participates in identity. `supersedes` orders first creation;
  it does not claim to record a later return to an already-existing identity.
- `lineage(...)` reads creation order from `supersedes`, reports which identity
  keys changed, and reads currentness separately from the output aliases. That
  separation is required after an edit is reverted: the older record becomes
  current again without receiving a second `created` event.
- `hedloom_exec.planned.plan_bundles(...)` derives content-addressed bundles from a
  Hedloom Flow Plan **document**, at schema 2 or 3; a document at any other
  schema is refused by version rather than misread. The coupling is to the portable
  plain-data artifact, not to the package: nothing imports `hedloom_flow`, and the
  base distribution stays dependency-free. An invocation's digest changes
  exactly when its own declaration or any ancestor's does, so reuse is
  transitive and staleness propagates downstream.
- Sources are identified by their declared address and artifact kind
  rather than by authored-order source ID, so inserting an unrelated source
  invalidates nothing. `plan_bundles(..., source_fingerprints=...)` folds in an
  identity for what is *at* each address, supplied by the caller because
  resolving an address is not this unit's work. Without it, editing an input in
  place changes no declared fact and the work that read it is reused — stale,
  and the reason the parameter exists.
- `source_references(document, fingerprints)` returns the string each source's
  input bindings carry, paired with that source's own declaration. It exists so
  a run can deliver a declared external file to the body that named it, and it
  hands the address back **unresolved**: this unit still locates nothing, and
  returning a declaration is not acquiring the authority to read it.
- `resolved_inputs` carries upstream values for execution and never
  participates in identity: which values they are is already implied by the
  declared input digests.
- `execute(...)` refuses a bare `identity` for recorded work, because such an
  identity says nothing about what produced the result stored under it.
- Only `succeeded` is reused automatically. A failure may be the work's own
  verdict or something incidental to it — an OOM kill, a preempted node — and
  the record cannot tell those apart. A non-reusable terminal try is retained
  and the next launch allocates the next try without a fixed attempt limit.
  `accept_for_reuse(...)` durably selects the current try as standing evidence;
  it does not imply a pin.
- Outputs are declared per invocation as `{"path": ...}` for a file the command
  wrote itself, `{"path": ..., "filesystem_kind": "directory"}` for a
  directory tree, `{"stream": "stdout"}` for a tool whose result is what it
  printed, or `{"value": True}` for an in-process return. Standard output is
  always captured to `stdout.log` as diagnostics and is never the result unless
  declared as one: a command that prints progress while writing its answer to
  disk is the ordinary case.
- Materializing a filesystem output records whether it is a file or directory,
  its address, size, and modification time; it never moves bytes. A directory's
  size is the sum of its recursively contained non-directory entries, and its
  modification time is the latest observed in the tree. On a shared filesystem
  the next invocation opens the same path, so the durable fact is the address
  rather than a copy.
- Each try runs in its own workspace, so a rerun after a failure cannot
  overwrite the evidence of what the previous attempt produced. Only declared
  outputs are recorded; anything else the command left behind stays as unnamed
  evidence.
- Declared file outputs also have a derived live view at
  `<attempt-root>/latest/<plan>/<authored-key>/<output>`. Each entry is a
  symlink atomically created or repointed after workspace preparation and
  before launch, so a fresh open follows the try currently selected even while
  that file grows. The target is deliberately not pre-created: until the work
  writes it, the honest state is a dangling alias. This view never participates
  in content-addressed identity.
- Because `latest/` is inside the attempt root, every attempt-root reader admits
  a directory only when it is a valid record with layout 1. The alias tree and any
  other ordinary directory are not attempt records.
- A declared filesystem output that does not exist with its declared file or
  directory shape after the work reports success fails the invocation.
  Publishing a manifest without it would let downstream work resolve an
  address to nothing. Empty files and empty directories are valid outputs.
- A bundle may carry `placement`, recorded as its own journal event before the
  substrate is touched and published in the manifest alongside what was
  observed. Requested, resolved, and observed are kept apart deliberately: a
  run that came out slow or misplaced is only explainable if they do not
  collapse into one fact. Placement never reaches the input digest, including
  its options: retuning a point's memory or moving it to another queue reuses
  the result it already produced rather than recomputing it.
- `hedloom_exec.watch` observes records it does not own. It reads record
  directories, asks LSF once per refresh about every live job rather than once
  per job, and appends transitions to an `observations.jsonl` beside each
  record. Every observation is keyed by try. The invariant is that an observation is evidence *about* a try,
  never a transition *of* it: the observer writes its own file, so the log that
  carries the ordering rules keeps exactly one writer, and nothing an observer
  records can change what an attempt concludes or what reuse returns. A watcher
  may be killed, restarted, or run twice with no effect on any result.
- Only state changes are recorded, which is what makes per-job **queue latency**
  derivable afterwards — the interval between `submit_intent` and the first
  `running` observation is the dispatch cost this project has been reasoning
  about without measuring.
- Job status is read with `bjobs -o "job_name stat"`. Default `bjobs` output is
  refused rather than parsed: it truncates job names and its columns shift when
  a pending job has no execution host, so a `PEND` row can be read as `RUN` —
  wrong in precisely the field being watched. A site whose LSF predates `-o`
  gets a refusal that says so.
- An attempt on a substrate that has no external job is left alone rather than
  given an invented state, and a job absent from LSF while the record says live
  is left to reconciliation, which owns the attempt.
- `Durability` is declared per invocation, never inferred from placement.
  `EPHEMERAL` touches no filesystem, requires no identity or root, and reruns
  on every call. `RECORDED` runs the full protocol and completes from an
  selected standing evidence without rerunning the payload.
- `RetentionPolicy` is operator-owned storage policy. Conditions within one
  named rule are ANDed and rules are ORed; the global age floor overrides all
  of them, `keep_latest` defaults to one, and `unreconciled` is never
  selectable. Unknown keys, empty rules, and malformed sizes or durations are
  refused rather than interpreted generously.
- `prune.survey(...)` is a read-only classification of every try. It creates
  no directory, measures actual workspace trees rather than manifest size
  claims, and explains every exclusion. Standing reusable evidence, current
  aliases, non-terminal and unreconciled tries, and workspaces escaping the
  declared root are never candidates.
- `Survey.apply()` is the only destructive retention operation. Each proposed
  try is reclassified while its non-blocking record claim is held; contention
  skips rather than waits. A durable `workspace_removed` event precedes byte
  removal, record directories and manifests remain, optional diagnostics
  remain, and a byte limit bounds each pass.

## Contribution to the parent

The unit contributes durable attempt identity, terminal evidence, and sound
reuse to the repository's author-plan-execute-evaluate vision. It is the first
unit to own any part of *execute*, and with Hedloom Flow it now composes one
runnable vertical slice: author a flow, plan it, execute it, edit one input,
and rerun with unchanged work skipped and superseded results retained.

## Exclusions

Hedloom Exec owns no graph. It does not decide readiness, order invocations,
release successors, retry, or replan; those remain outside it, and the boundary
is tested by reconciling an attempt from a record that carries no topology. It
does not own Dask transports, worker pools, placement enforcement, policy
resolution, evidence promotion, or the study lifecycle. It records where
outputs are but owns no artifact store, performs no transfer between
filesystems, and verifies no content digest. Retention removes only selected
try-workspace bytes; it never removes record evidence or external artifact
addresses. It reads a Plan document but neither
produces nor validates one, and it resolves no declared address: derivation
consumes only what the Plan already states.

Owner-bound lifetime is enforced by `bsub -I` plus local process-group and
`PR_SET_PDEATHSIG` discipline, not by any lease or heartbeat this unit owns.
The local half of that guarantee is demonstrated with real processes and
signals; LSF's half — that an interactive job dies with its client — is assumed
and unverified, and `examples/lsf_preflight.py` exists to check it on a real
submit host. The subprocess layer is exercised end to end against a fake
`bsub`/`bjobs`/`bkill` on PATH. Queue admission, scheduling, and resource
enforcement are untested.

Resource requests are *stated*, never enforced or reasoned about here. Whether
a site admits the requirement string this unit composes, whether its licence
resource names match what a plan authored, and whether contention resolves
sensibly are all facts about the farm; `examples/lsf_preflight.py --licence
<name>` is where they can be checked. Nothing counts licences locally, queues
work, or waits for a token: handing the need to the scheduler that owns the
count is the design, and a placement whose substrate cannot arbitrate its
declared resources will not say so.

Reuse soundness depends on inputs being *declared*. An operation whose result
depends on an undeclared file, wall-clock time, or a mutable network resource
is not honestly reusable, and no digest detects that; the unit records what an
author claims rather than verifying it. Attempt discovery is a directory scan,
which is fine at prototype scale and wrong at any other.

## Child composition

There are currently no child units.
