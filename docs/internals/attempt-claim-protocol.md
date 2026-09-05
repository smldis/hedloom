# The record claim and try protocol

This page documents the synchronisation in `hedloom_exec`: one durable record,
the numbered tries beneath it, and the writes that make a try recoverable and
its result reusable.

The TLA+ model in
[`AttemptClaim.tla`](attempt-claim/AttemptClaim.tla) predates the record/try
split. It modelled one old sequence identity, so its single attempt corresponds
most closely to one try now. Its counterexamples established the ordering and
exclusion rules that the current implementation retains, but it does not model
try allocation or standing evidence. Its old filenames and state vocabulary
are historical, not the layout contract below.

Record creation and `SomeoneCompletes` were added later, in 2026-08, and are
current rather than historical: they model how a record becomes visible today
and are the reason that question has a checkable answer instead of a note.

## Names and layout

One declared computation has one record identity, whoever asks for it:

```text
attempt_identity(computation_digest)
    -> hedloom-<blake2b-80bit>
```

The requester is not a component. Two studies, or two authored keys, declaring
the same computation select the same record. That makes the exclusion below
load-bearing in a way it was not when every study had its own namespace: a
concurrent equivalent request now reaches *this* record rather than a private
one, and is refused by name rather than made to wait. Reuse of a published
result still succeeds; what is refused is a second live claim. Coalescing or
waiting for the holder is a scheduler question and is not answered here.

The Phase 1 rendering deliberately differs from every earlier rendering: the
old sequence hash slot was removed rather than filled with a vestigial zero.
There is no migration in this prototype. A record must declare layout version
1; missing or unknown layouts are refused, and roots written before Phase 1 are
unreadable.

Each execution within a record has an unbounded non-negative try number. The
workspace, LSF job name, discovery key, cancellation key and watcher key are
all the strict try name:

```text
<record>-<try>
```

The record directory is shared by its tries and contains:

```text
layout                 # integer 1
events.jsonl           # append-only events, every event names its try
claim.lock              # advisory record claim
manifest/<try>.json     # immutable terminal evidence for one try
standing.json           # atomic selection of reusable evidence, when present
```

Try workspaces are siblings of the record directory. This separation lets a
failed try remain intact while a later try runs, without duplicating the
record's identity and attribution.

A new record is **published atomically**: it is built aside with its `layout`
already in it and renamed into place, so the directory never exists while
declaring nothing. That matters because a record that exists and declares no
layout is exactly what a directory Hedloom never made looks like, and that
refusal has to stay unambiguous. Losing the rename to a concurrent caller is
not a failure — their record is the same record. A caller that meets a record
already claimed is refused by name with `ConcurrentClaim`, never by a missing
layout.

## The claimed transition

`execute(..., Durability.RECORDED)` performs the stateful work under one record
claim:

```text
with journal.claim():
    validate layout and identity
    fold every event into its own TryState
    return standing evidence if it is valid
    resume an allocated try that has no submission intent,
        otherwise begin_try() reserves the next number
    flush try_started
    prepare that try's workspace
    flush submit_intent
    submit or discover using <record>-<try>
    append submit_receipt when available
    reconcile
    atomically publish manifest/<try>.json
    append terminal
    atomically replace standing.json when the outcome is reusable
```

`begin_try()` both reserves and records. It is refused unless the caller holds
the record claim, and its `try_started` event is flushed before workspace work
or any transport call. A crash after allocation but before `submit_intent`
therefore resumes the same try; it cannot silently consume a number or cause
two jobs to share one.

The claim is `flock(LOCK_EX | LOCK_NB)` on `claim.lock`. It is non-blocking on
purpose: a second caller receives `ConcurrentClaim` instead of waiting while a
first caller may be inside a blocking `bsub -I`. A filesystem that silently
implements `flock` only per host does not satisfy this protocol.

## Recovery and publication order

Three orderings carry the recovery argument:

1. `try_started` is durable before any transport call, so all later evidence
   has one already-reserved number.
2. `submit_intent` is durable before the transport is asked to accept work, so
   a lost receipt is recovered by discovering the exact try name. Discovering
   the bare record would report a false negative and risk duplicate farm work.
3. `manifest/<try>.json` is atomically visible before `terminal` is appended,
   so every terminal journal claim has readable evidence behind it.

Publication remains under the record claim. The older protocol released its
claim before reconciliation; the model found that two publishers could then
leave the journal verdict and fixed manifest disagreeing. Per-try immutable
manifests remove cross-try replacement, while claimed publication retains one
writer for the same try.

`standing.json` is an atomically replaced materialized pointer: a copy of the
selected manifest, including its record identity and try number, so the reuse
fast path remains one read. Automatic success writes it;
`accept_for_reuse(...)` may select the current failed try after inspection.
Acceptance does not imply a pin.

## Folding is per try

The record fold partitions every mutable field before projecting a current
state. In particular, these are never sticky across tries:

- phase, handle, result and manifest;
- `cancel_requested` and `cancel_reason`;
- `reuse_accepted` and `reuse_reason`;
- placement and diagnostics;
- watcher observations and their timestamps.

Compatibility accessors on `AttemptState` project only the current try. They do
not merge an earlier cancellation, acceptance or observation into a later run.

## Cancellation and watching

Cancellation is per try. `request_cancel(...)` records intent under the record
claim and calls the substrate with `<record>-<try>`; `bkill -J` never receives a
bare record name. A successful return still establishes only requested
cancellation, not a terminal outcome.

The watcher scans record directories but joins scheduler rows by strict try
name. Its `observations.jsonl` entries carry the try number, and deduplication
is per try, so a later try may independently pass through the same queue states.
Observations remain evidence about work, never state transitions of it.

## What the model still establishes

The historical model remains useful for the local safety rules it actually
checked:

- with an honoured claim and publication under it, at most one matching job is
  live and record evidence agrees with the published outcome;
- recording terminal state before evidence is visible violates
  `TerminalHasEvidence` even without a crash;
- a silently ineffective lock can leave a live job without a truthful durable
  trace, then permit a duplicate;
- a detached substrate with inaccurate negative discovery cannot be recovered
  safely; refusing is better than guessing;
- creating the record in two visible steps lets every caller refuse and the
  work go undone, while violating no invariant at all. `MCSplitCreate` is that
  mutation and `MCAtomicCreate` is the repair; the difference between them is
  the whole reason a temporal property exists here.

It does not establish layout-1 compatibility, allocation correctness, or
per-try folding. Those are executable tests in `test_claim.py`,
`test_try_allocation.py`, `test_fold_partitioning.py`,
`test_recovery_names.py`, and `test_watch_keys.py`.

## Not modelled

- Filesystem guarantees beyond the assumed advisory exclusion, atomic rename,
  append and fsync behaviour. The mount configuration remains operational fact.
- Dask scheduling. The record owns no readiness or topology, and
  `hedloom_exec` imports neither `hedloom_flow` nor Dask.
- Retry, retention or pin policy. Phase 1 supplies unbounded mechanical tries;
  later phases decide what to retain and protect.
- Liveness beyond record creation. `SomeoneCompletes` is checked, and it is
  the only temporal property here; nothing says whether a farm finishes.
