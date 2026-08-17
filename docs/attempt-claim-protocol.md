# The attempt claim, model-checked

This page documents the synchronisation in `hedloom_exec` — the claim, the
journal, and the two durable writes that publish a result — and reports what a
TLA+ model of it found. The model is in [`attempt-claim/`](attempt-claim), and
it is checkable in about a second.

It exists because the argument for this protocol is entirely a prose argument.
The docstrings in `journal.py` and `attempt.py` state four ordering rules and
one exclusion rule, the test suite exercises them on a local filesystem with one
process, and nothing else stands behind them. That is a good argument. It is not
a checked one, and the place it is weakest — a second controller against the
same study root — is exactly the place no test goes.

## The protocol

One invocation, one set of declared inputs, one attempt identity, one directory
under the study root. Everything that follows is per identity; two identities
share nothing.

`execute(..., Durability.RECORDED)` runs three phases, and only the middle one
is locked:

```
_select_sequence()          read each sequence's manifest, pick the first free   [unlocked]
launch_or_attach()
    with journal.claim():   read manifest, fold the journal, decide,             [LOCKED]
                            append submit_intent, submit, append submit_receipt
reconcile()                 poll, write manifest.json.partial, rename,           [unlocked]
                            append terminal
```

The claim is `flock(LOCK_EX | LOCK_NB)` on `claim.lock` in the attempt
directory. Non-blocking on purpose: a second caller is told `ConcurrentClaim`
rather than made to wait, because a second submission of one attempt is the
defect and the honest response is to name who is already doing it.

Two ordering rules carry the recovery argument, and both are about what a
*crash* leaves behind:

* `submit_intent` is flushed **before** the transport is asked to accept work,
  so an accepted submission whose receipt is lost still has a durable trace
  naming the identity to look for.
* `terminal` is appended **after** the manifest is atomically visible, so a
  journal that claims a terminal outcome always has readable evidence.

## What the model is

[`AttemptClaim.tla`](attempt-claim/AttemptClaim.tla) is those steps as discrete
atomic actions, with two callers running them concurrently against one attempt
directory and a crash action that can kill either caller at any point. Durable
state survives a crash; the advisory lock does not, because the kernel drops it
when the file descriptor closes; and the farm job does not either, because its
`bsub -I` client *was* that process.

Kept faithful where it matters:

* `Fold` is `journal.fold()` transcribed — a left fold over the event log, later
  events overriding earlier ones. `submit_lost` really does return the phase to
  `unsubmitted`, which is what makes one of the counterexamples below possible.
* The claim is released where `launch_or_attach` returns, so `reconcile` and
  `publish_terminal` run **unlocked**, as they do today.
* `bsub -I` blocks until the job is over, so submission is two actions with the
  job live in between. That live window is where a crash costs money.

Abstracted away: bytes, filesystems, weak memory. Every action is atomic and TLC
explores all interleavings, which is enough here because every claim rests on
*which* durable fact is consulted and *in what order* the two durable writes
happen — not on the width of any single write.

Five constants make the load-bearing assumptions switchable, so denying one and
re-running shows what it was holding up: `LockHonoured`, `DiscoveryIsAccurate`,
`OwnerBoundLifetime`, `PublishUnderClaim`, `PublishOrder`.

The four invariants:

| Invariant | The rule it encodes |
|---|---|
| `AtMostOneLive` | One identity, at most one farm job at a time. What the claim is for, and the only property that costs money when lost. |
| `LiveJobHasDurableTrace` | A job that exists is a job the record can name. The `submit_intent`-before-submit rule. |
| `TerminalHasEvidence` | A journal claiming a terminal outcome has a readable manifest behind it. The publish-before-record rule. |
| `RecordMatchesEvidence` | The outcome the record names is the outcome the visible manifest carries. `execute` returns the phase from the journal and the artifacts from the manifest; the two disagreeing is a result reported under the wrong verdict. |

## Reproduce

With a JRE 21 and [`tla2tools.jar`](https://github.com/tlaplus/tlaplus/releases):

```console
cd docs/attempt-claim
java -cp tla2tools.jar tlc2.TLC -config MCShipped.cfg AttemptClaim.tla
```

Each configuration runs in about a second and explores a few hundred states.

## What TLC found

| Configuration | What it denies | Result |
|---|---|---|
| `MCShipped` | nothing — the protocol as shipped | **`RecordMatchesEvidence` violated**, 18-state trace, no crash needed |
| `MCClaimedPublication` | nothing; adds the repair below | no violation, no deadlock, 250 distinct states |
| `MCNoLock` | `LockHonoured` | **`LiveJobHasDurableTrace` violated** in 9 states; `AtMostOneLive` in 11 |
| `MCRecordFirst` | `PublishOrder` | **`TerminalHasEvidence` violated** in 10 states, no crash needed |
| `MCStaleDiscovery` | `DiscoveryIsAccurate` | no violation — see below |
| `MCDetached` | `DiscoveryIsAccurate` and `OwnerBoundLifetime` | **`LiveJobHasDurableTrace` violated** in 10 states; `AtMostOneLive` in 12 |

Trace lengths for `AtMostOneLive` are from a run with the other invariants
removed, since TLC stops at whichever is violated first.

Three of these are mutations that had to fail, and did. The first one is a
finding.

### The finding: publication has no writer exclusion

`AtMostOneLive` holds under `MCShipped`. The claim does the job it was written
for: a second caller arriving while the first holds the lock is refused, and a
second caller arriving *after* the first has submitted folds `submitted` and
attaches instead of submitting again. No duplicate farm job.

But `launch_or_attach` returns before `reconcile` runs, and the claim goes with
it. The attached caller then reconciles too — `execute` calls `reconcile` for
both the `claimed` and the `attached` disposition — so two callers can be inside
`publish_terminal` for one identity at once. `reconcile` does guard against
this: its first act is to re-read the manifest and return if one is visible. The
guard is just read outside the lock, so both callers can pass it before either
publishes.

TLC's trace, with no crash in it:

```
c1  claim → created → submit_intent → submit → submit_receipt → release
c2  claim → fold sees "submitted" → attached → release
c1  poll → succeeded          c2  poll → unreconciled
c1  rename  (manifest = succeeded)
c1  append terminal:succeeded (journal = succeeded)
c2  rename  (manifest = unreconciled)
```

The run ends with `events.jsonl` saying `succeeded` and `manifest.json` saying
`unreconciled`. `execute` reports the outcome from the journal and the artifacts
from the manifest, so this is a result returned under a verdict that is not its
own. One more step of that trace has `c2` appending its own `terminal` event,
leaving two terminal records for one attempt.

The same unlocked window has a second consequence the model does not cover,
found by reading the code while writing it: `publish_terminal` stages through
`manifest.json.partial`, a fixed name in the shared attempt directory. Two
concurrent publishers open the same temp file with `"w"`. The rename is atomic
and the published file is never torn — but *which* writer's bytes get renamed is
whoever wrote last, not whoever renames.

**The repair, and what it costs.** `MCClaimedPublication` holds the claim until
the terminal record is written, and every invariant holds with no deadlock. It
is not an expensive change: the claim is *already* held across the entire
blocking `bsub -I`, so extending it through a poll and two file writes adds
nothing to how long an attempt holds the lock. The re-read guard inside
`reconcile` then means what it looks like it means, because it is read under the
lock that makes it true.

**How exposed is this today?** Not at all, and for exactly the reason the NFS
note in `journal.claim()` gives: it needs two live callers for one identity.
`run_plan_graph` submits one task per invocation, so a single controller never
produces two. It opens with two controllers against one study root — two people,
or the same study started twice on two login hosts — and with pooled placement,
where journals would be written from farm nodes. That is the same exposure
`docs/pooled-placement-plan.md` §2 already defers, and this is one more thing to
fix before it lands, alongside the flock question.

### The mutations

**`MCNoLock`** models a study root on an NFS mount that answers `flock` locally
(`local_lock=flock`, `local_lock=all`, or `-o nolock`): both hosts are granted
the lock and neither is told. The interesting part is what breaks *first*. Not
the duplicate job — the record:

```
c1  claim → created → submit_intent      (phase: intended)
c2  claim → fold sees "intended" → discover finds nothing (c1 has not
    started yet) → append submit_lost    (phase: unsubmitted)
c1  bsub -I starts                       (a job is now running)
```

Nine states in, a job is live on the farm and the durable record says nothing
was ever submitted. Two states later both callers have a job running. This is
the concrete form of the warning already in `journal.claim()`: a silently
degraded lock does not merely produce two `bsub` jobs, it makes the journal lie
about the one it already had.

**`MCRecordFirst`** reverses the two writes in `publish_terminal`. TLC violates
`TerminalHasEvidence` in ten states *without a crash*, which is sharper than the
docstring's own reasoning: the window between the two writes is itself a state
where the journal claims a terminal outcome no manifest backs. Any reader
arriving there — including `_launch_or_attach_locked`, which checks for exactly
this — raises `ReconciliationError`. The shipped order has a window too, but its
window is manifest-visible-with-no-terminal-record, and that one the code
repairs on sight (`repaired=True`). One order's crash window is recoverable and
the other's is a hard error; that is the whole content of the rule.

**`MCStaleDiscovery`** was meant to show that `discovery_is_authoritative = True`
is load-bearing for `LSFInteractiveTransport`. It does not: with discovery
returning a false negative for accepted work, every invariant still holds. The
reason is worth writing down, because it was not obvious before the model said
it. The window in which a stale answer could do damage is the window in which a
job is live and someone else is deciding — and there is no such window. The
claim is held across the entire blocking submission, so no second caller can
decide anything while a job runs; and if the first caller dies, the lock is
released *and the job dies with it*, so "not found" is the truth.

So the crash-window argument is carried by owner-bound lifetime, not by
discovery. `MCDetached` denies both and gets the duplicate immediately: a caller
crashes mid-`bsub`, its job keeps running, the next caller finds `intended`,
discovers nothing, records `submit_lost`, and submits a second job for an
identity that already has one. That pairing — work that outlives its submitter,
and discovery that cannot see it — is what the `attached` disposition and
`UnrecoverableAttempt` were written for, and `attempt.py` says as much: they are
unreachable today because nothing detaches. The model agrees, and adds that
`discovery_is_authoritative` is a claim that starts mattering on the same day
detached work does.

## What is not modelled

* **One identity.** Different inputs derive different identities and share no
  state, so N identities are N independent copies. Nothing here says anything
  about `_select_sequence` choosing *between* sequences, which is an unlocked
  read of several manifests and deserves its own look.
* **The filesystem.** `flock` is modelled as exclusion when `LockHonoured`
  holds. Whether a given mount delivers that is the open question in
  `journal.claim()`, and a model cannot answer it — only `/proc/mounts` can.
  `O_APPEND` atomicity is assumed, which NFS does not provide.
* **Dask, and the cluster.** No `SpecCluster` surface is modelled: not placement
  annotation, not the resource budget, not the lockout that annotating every
  task prevents. Deliberately — a model of Dask's scheduler would only be as
  good as this author's reading of Dask, and would "prove" things about a
  scheduler that does not exist. Those claims are cited to `distributed` source
  lines in `dask-scheduling-rules.md` and measured in probes, which is the
  right kind of evidence for someone else's implementation.

  Worth stating explicitly, though, because it is a dependency in the other
  direction: **what keeps the shipped protocol safe today is supplied by
  `graph.py`.** One task per invocation, `pure=False` so Dask cannot decide two
  invocations are one call, and a key made unique before submission — together
  those are why one controller never produces two live callers for one
  identity, which is the precondition every counterexample above needs. Retries
  do not break it (a retry is sequential, not concurrent). Two controllers, or
  pooled placement writing journals from farm nodes, do.

  What *is* worth modelling on that side is `_stop_admitting` — hedloom's own
  protocol over Dask rather than Dask itself. That is done, in
  [`stop-admitting-protocol.md`](stop-admitting-protocol.md): the loss its
  comment calls bounded turns out to be a false report line, and repairing it
  needs the attempt record for both the classification and the outcome.
* **Liveness.** Only safety invariants and deadlock. The model says a bad state
  is unreachable, not that a run finishes.
