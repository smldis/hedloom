# Proposal: bind the attempt identity

A change proposed on 2026-08-17, after the TLA+ models in
[`attempt-claim-protocol.md`](attempt-claim-protocol.md) and
[`stop-admitting-protocol.md`](stop-admitting-protocol.md). Not implemented.

## The defect, restated

`_stop_admitting` reports `blocked` for work that ran. The model shows it in
seven states and the code's own comment predicts it. But the race is a symptom,
and stated as a race it invites a fix that treats the symptom.

Stated properly: **the kernel asks an in-memory handle a question only the
durable record can answer.** It asks Dask "did this invocation run?" — and Dask
cannot know, because a cancelled future reports `cancelled` whether or not its
thread ever started. The record knows: `submit_intent` is written before the
substrate is touched, precisely so this question has an answer.

That is the one place `hedloom_run` breaks the rule the rest of the system
keeps. `attempt.py` opens by stating it: *"No live object is treated as the
authority for any of them."*

## Why the kernel cannot ask

Because it cannot name the attempt. `binding.py` exists to hold what both
kernels must agree on, and says so:

> Changing which kernel decides readiness changes how long a plan takes and
> nothing else — the same results, **under the same identities**.

It binds the command, the transport, the declared outputs, the addresses
upstream landed at. It does not bind the identities. Those are chosen inside
`execute`, on the worker, by `_select_sequence` reading the attempt root — after
`_run_one` is already running. The controller submits work it cannot refer to.

Three symptoms, one gap:

* `_stop_admitting` classifies from a Dask snapshot because it has no identity
  to fold a journal with.
* `watch.live_attempts(root)` scans the directory with `iterdir()`. A watcher
  attached to a run shows whatever is under that root, not this run's corners.
* A Plan cannot say where a corner's record will be until the corner has run,
  so nothing can be inspected, pre-flighted or linked in advance.

`identity.py` already argues this should not be so. Its first paragraph: *"An
attempt identity must exist **before** a transport is asked to accept work"*, and
it is *"derived only from authored planning facts and an attempt sequence"*.
Everything in that derivation is known at plan time except the sequence, and the
sequence is a pure function of the durable state plus those same facts.

## Two things not to do

**Do not add a disposition for "we are not sure".** Reporting the raced tasks as
`indeterminate`, naming the attempt directory to look in, is cheap and it is
honest, and I proposed it before thinking about it properly. It is the wrong
change. It leaves the source of truth wrong and renames the consequence; it
exports a question the system can answer to the person least able to answer it;
and it sets a precedent, because every later question of the form "did this
actually happen" then wants its own escape hatch. The report's vocabulary grows
and its meaning weakens.

**Do not withhold submission.** Submitting the whole graph up front is what
creates the problem — there is no admission to stop, only work to cancel. But
withholding it means deciding in the controller what is ready, which is exactly
the authority this kernel exists *not* to hold. Cancellation is the right
mechanism. The defect is in what the run then says, not in how it stops.

## The change

Bind the identity where the other bindings live.

**1. `hedloom_run.binding` gains identity resolution.**

```python
def attempt_for(item: PlannedInvocation, *, plan_id: str, root: str) -> AttemptIdentity:
    """Which attempt this invocation will use, decided before it is submitted."""
```

The body is `_select_sequence`, moved rather than rewritten: walk sequences from
zero, take the first that is unfinished or finished-and-reusable. It needs
`plan_id`, `item.invocation_id`, `item.input_digest` and `root` — all of which
the controller already holds, and the first three of which are already on the
`PlannedInvocation`.

**2. Both kernels resolve every identity before submitting anything, and pass
it in.** `execute` stops choosing and starts being told. The guard that today
refuses a bare identity becomes a type distinction rather than a boolean:
`AttemptIdentity` carries `plan_id`, `invocation_id` and `input_digest`, so it
is self-evidently derived; a bare `str` still is not, and is still refused.
`unchecked_identity=` can go.

**3. `_stop_admitting` stops classifying and starts reading.** Cancel as it does
now, then for each cancelled invocation fold its journal:

* no `submit_intent` — it never reached a substrate, so `blocked` is true;
* `submit_intent` present — it ran, so wait for that record to settle and report
  what it actually did.

The in-flight and finished sets stay, but as what the model showed them to be:
an optimisation. A future that still works returns a `_Step` that `execute`
already derived from the record, so using it costs nothing and delivers outcomes
as they complete. It is not what makes the report true.

## The rule this buys

> A future says *when* an invocation finished. The record says *what* it did.
> Where they can disagree, the record wins; where the future is gone, only the
> record remains.

One sentence, applicable to the next question of the same shape, and it makes
`hedloom_run` obey the rule `hedloom_exec` already states.

## What else it fixes

This is where the change pays for itself, and why it is worth more than the
narrow fix:

* The watcher can be handed this run's identities instead of scanning a root —
  so `watch=True` reports on *this study's* corners, and two studies sharing a
  root stop showing each other's work.
* A Plan can name where every corner's record will land, before anything runs.
  Inspection, links in a report, and `Study.summary()` all become possible
  without a run.
* Preflight can refuse an exhausted sequence up front — the `AttemptSpent`
  failure that today happens on a worker, mid-sweep, per corner.
* The two kernels get "the same identities" by construction rather than by two
  call sites passing the same arguments to a function that decides privately.

## What it costs, and what could go wrong

* **N filesystem reads before submission.** One `read_manifest` per sequence per
  invocation — the same reads reuse already performs, moved earlier. Negligible
  for a sweep; worth measuring before a plan with tens of thousands of corners.
* **A longer window between choosing a sequence and claiming it.** Not a new
  race: `_select_sequence` → `claim()` is already a check-then-act. The window
  grows from "microseconds on a worker" to "the length of the run". The failure
  mode is unchanged and clean — `_launch_or_attach_locked` refuses a spent
  sequence with `AttemptSpent` rather than reusing it wrongly — and it needs two
  controllers against one root, the same exposure gate as the flock DEVNOTE and
  the publication one. Worth stating in the same place as those.
* **Waiting on a record instead of a future**, for raced invocations only. That
  is a poll where there was an event. `hedloom_exec.watch` already has the
  reader; the cost is bounded by how many tasks slip the cancel window, which is
  usually none.
* **Blast radius**: `execute`'s signature, two kernels, one example. The
  identity derivation itself does not change, so no existing attempt directory
  moves and nothing already recorded is invalidated.

## How it would be checked

* The models are already parameterised for it: `BlockedFromRecord` and
  `OutcomeFromRecord` in `StopAdmitting.tla` *are* this change, and
  `MCRecordTruth` is clean across the state space.
* The race becomes testable. Once the controller resolves identities, a test can
  inject a stale snapshot — a fake `_executing_keys` that omits a task which is
  in fact running — and assert the report line matches the journal rather than
  the snapshot. The defect stops being a story about interleaving and becomes an
  assertion.
* The existing suite covers the rest: identities are unchanged, so reuse,
  sequence selection and recovery tests should pass untouched. If they do not,
  the move was a rewrite.

## Not in this change

* The report does not become a fold over records. Invocations refused before
  they reach a substrate — an unservable placement, a transport error — have no
  record to fold, and they are deterministic rather than racy. They stay
  in-memory steps.
* Reuse is untouched. This moves *when* an identity is decided, not *how*.
* The publication window found by the other model is a separate fix in
  `hedloom_exec`, on the same exposure gate. Doing both at once would make one
  review of two arguments.
