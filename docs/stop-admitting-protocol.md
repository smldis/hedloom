# Stopping a sweep, model-checked

The companion to [`attempt-claim-protocol.md`](attempt-claim-protocol.md), one
layer up. That page models what `hedloom_exec` does with one attempt; this one
models what `hedloom_run.graph` does with the *rest* of a sweep when the first
invocation comes back failed. The model is in
[`stop-admitting/`](stop-admitting), and runs in about a second.

`_stop_admitting` is a good target for this because it is hedloom's own protocol
rather than Dask's, and because its own comment already admits a race:

> A task can acquire a thread after the stack snapshot but before this call.
> Dask cannot interrupt that Python thread: its `bsub -I` runs to completion and
> its journal is published normally. The bounded loss is only this run report's
> line for it, which is marked blocked below.

The model agrees that a task can slip through. It disagrees about "bounded
loss".

## What the protocol does

Every task is submitted up front, so there is no admission queue to close. The
stop is therefore a cancellation, and it splits everything outstanding three
ways from one `Client.call_stack()` snapshot:

| Set | Test | Treatment |
|---|---|---|
| `in_flight` | key has a live Python stack | cannot be stopped — waited for, real outcome reported |
| `finished` | `future.done()`, not yet consumed | waited for too |
| `cancelled` | everything else | `client.cancel(force=False)`, reported `blocked` |

## What the model is

[`StopAdmitting.tla`](stop-admitting/StopAdmitting.tla) is three independent
invocations, a controller running the `as_completed` loop, and an environment
that keeps moving while the controller decides. Dask appears as three facts and
nothing else: a task is queued, or running a Python thread, or finished; a
cancel removes a queued task, cannot touch a running one, and destroys the
future either way.

Placement, resources, stealing and worker capacity are absent on purpose.
Unbounded parallelism is the worst case for the race being checked, so assuming
it is conservative — and a model of Dask's scheduler would only be as good as
one reading of Dask, which is the wrong kind of evidence for someone else's
code.

Four constants: `ExecutingTest`, `PreserveInFlight`, `BlockedFromRecord`,
`OutcomeFromRecord`.

## Reproduce

```console
cd docs/stop-admitting
java -cp tla2tools.jar tlc2.TLC -config MCShipped.cfg StopAdmitting.tla
```

| Configuration | What it changes | Result |
|---|---|---|
| `MCShipped` | nothing — as shipped | **`BlockedNeverRan` violated**, 7 states |
| `MCRecordClassification` | blocked decided from the record | **`TruthfulOutcomes` violated**, 9 states |
| `MCRecordTruth` | record decides classification *and* outcome | no violation, 933 distinct states |
| `MCProcessingAsExecuting` | `processing()` instead of `call_stack()` | **`NoQueuedWorkSurvivesTheStop` violated**, 6 states |
| `MCCancelInFlight` | in-flight work cancelled, not waited for | no violation |

## What TLC found

### The report can say `blocked` about work that ran

Seven states, and the trace is the one the comment predicts:

```
t1  start → finish(failed)
t1  consumed → stop decision
    snapshot: t2, t3 both queued → doomed = {t2, t3}
t2  start                       ← the window
    cancel: t3 really is cancelled; t2 holds a thread and cannot be
    report: t2 = blocked, t3 = blocked
```

`t3` is reported correctly. `t2` ran, wrote `submit_intent`, spent a farm job,
and published a journal — and the report says it never started.

The disagreement with the comment is over what that costs. It is not a missing
line, it is a *false* one. `blocked` is a claim with operational meaning: it is
what tells an operator this corner was never attempted, and it is what makes a
rerun look free. The attempt record for `t2` says otherwise, so the two
artifacts of one run contradict each other, and the durable one — the one this
whole architecture says is authoritative — is the one not being read.

The window cannot be closed from the client side. There is no
cancel-if-not-started in Dask, and after `client.cancel` a future reports
`cancelled` whether or not its thread ever ran, so the classification cannot be
recovered afterwards either.

### Deciding `blocked` from the record is not enough on its own

The obvious repair is to stop trusting the snapshot: after the cancel, ask the
durable record which of the doomed actually reached the substrate — they wrote
`submit_intent` before touching it — and treat those as work to wait for.

TLC violates `TruthfulOutcomes` in nine states. `t2` is correctly moved out of
the blocked set, but `_collect_preserved` reads it back with `future.result()`,
and that future was destroyed by the same `client.cancel`. It raises
`CancelledError`, `_abnormal` turns that into a **failed** line, and `t2`
actually succeeded.

That is a worse lie than the one it replaces. `blocked` is a claim about the
scheduler; `failed` is a claim about the circuit. An engineer reading "this
corner failed" concludes something about the design.

The distinction the model forces is between *classification* and *evidence*.
Both have to come from the record, because the cancel destroyed the only other
source of either. `MCRecordTruth` — record decides both — is clean across the
whole state space.

**Why it cannot do that today.** `graph.py` does not know an invocation's
attempt identity. It is chosen inside `execute` by `_select_sequence`, from the
input digest, after `_run_one` is already on a worker — so the controller
submits work it cannot name, and has nothing to fold a journal with.

That is the actual defect, and it is not confined to this protocol:
`watch.live_attempts` scans the attempt root for the same reason, and a Plan
cannot say where a corner's record will land until the corner has run. The
proposal is [`binding-the-attempt-identity.md`](binding-the-attempt-identity.md)
— resolve identities in `binding.py`, where the invariant "the same results,
under the same identities" is already stated and everything *except* the
identities is already bound.

### The `processing()` mutation, as a state space

`MCProcessingAsExecuting` replaces "has a live Python stack" with "is assigned
to a worker", which is what `Client.processing()` answers and what this code
said before R10. Six states: `in_flight` swallows every outstanding task,
`doomed` comes out empty, `client.cancel` is never called, and both remaining
corners run to completion. The stop does not stop.

This is already covered by a test — reverting `call_stack()` to `processing()`
fails with "no unstarted future reached Dask's cancelled state" — so the model
adds no new fact. It is here because the two agree, which is the cheapest
evidence that the model is describing this code and not a nearby imaginary one.

### Preserving in-flight futures is an optimisation, not a correctness property

`MCCancelInFlight` cancels the in-flight work instead of waiting for it. With
the record authoritative, every invariant still holds: the jobs run anyway,
their futures are gone, and the record answers for all of them.

So waiting on in-flight futures is worth keeping for what it actually buys —
outcomes arrive as they complete, `on_event` keeps firing, no record has to be
re-read — but it is not what makes the report true. Only the record is.

## What is not modelled

* **Dependencies.** Three independent invocations, which is the shape of a
  sweep. A dependent of a cancelled task is blocked for a reason `_run_one`
  settles by itself, before this protocol is involved.
* **The `finally` path.** `run_plan_graph` calls `_stop_admitting` a second time
  when an exception escapes, with `notify=False`, then fills the rest with
  `blocked` via `setdefault` and attaches `report` / `in_flight` /
  `cleanup_error` to the error. Same race, one more caller.
* **Worker capacity.** Every queued task may start at any time. Real thread
  budgets only shrink the window.
* **Liveness.** Safety invariants and deadlock only.

## A note on method

The first version of this model let a cancelled future still return its result,
and `MCCancelInFlight` came back clean because of it. That flattered the repair:
it made "consult the record for the classification" look sufficient when it is
not. The fix was to model what `client.cancel` actually destroys.

Worth recording, because it is the same mistake as assuming `processing()` means
executing: taking an API's name for an answer to the question you had. A model
inherits every such assumption, silently, and returns it looking like a proof.
