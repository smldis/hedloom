# Cancellation: a draft plan, not a build order

**Status: not to be built now.** Written so that when a sweep needs stopping
there is a plan rather than an improvisation, and so the shape is decided while
nothing is under pressure. Today, killing the process is the right answer and
this document says why that is more than a shrug.

---

## 1. What already holds, before anything is built

**Killing the process works, and it is not a hack.** Three mechanisms overlap:

* **`PR_SET_PDEATHSIG` is per *thread*, not per process.** You are right to
  correct me on this — Linux considers "the parent" to be the *thread that
  created the child*, so a `bsub` client dies when its owning worker thread
  exits, not merely when the process does. That is a **stronger** guarantee than
  I stated, and it means a worker thread pool that is shrunk or recycled takes
  its farm jobs with it. Worth knowing in both directions: it is the mechanism a
  cancellation feature would lean on, and it is a hazard if anything ever
  resizes a worker's pool mid-run.
* **The process group.** Children are placed in their own group, so one signal
  reaches the whole tree.
* **`walltime` is required at construction**, documented as *"the only orphan
  bound that survives this process being killed without warning"*. Whatever
  escapes the first two mechanisms is bounded by LSF itself.

So the failure mode cancellation must actually protect against is narrow: **an
exception escaping into a caller that catches it and carries on**, leaving the
process alive and the jobs running with nobody waiting for them. That is the
same gap as A2 in `dask-usage-review-2026-08-16.md`, and it would be fixed by
the `try/finally` there — not by a cancellation feature. **As of 2026-08-16,
A2 is still open:** no such cleanup or `client.cancel` call surrounds the
`as_completed` loop.

## 2. The invariant any cancellation must preserve

> **Cancellation is a signal to the substrate, never a write to the record.**

The owner of an attempt is blocked in `bsub -I`. Kill the job and that call
returns non-zero, and the owner records a terminal outcome *itself*, through the
path it already uses. Nothing else ever writes `events.jsonl`.

This is the same shape as the watcher's rule — *an observation is evidence about
an attempt, never a transition of it* — and it is what keeps the whole feature
free of the machinery the project has deliberately avoided: no lease, no
heartbeat, no reaper, no second writer, no reconciliation pass. A cancel that
wrote a `cancelled` event from outside would need all four.

## 3. Level 1: stop a sweep (the basic feature)

Three parts, in the order they matter.

**(a) Stop admitting new work.** Cheapest and covers most of the value. A sweep
of 200 corners with 8 in flight is 192 jobs that simply never start.
* `client.cancel(outstanding_futures)` — stops tasks not yet started. Dask
  cannot interrupt a task already executing in a thread, so this is exactly and
  only what it does.
* A flag checked before `launch_or_attach`, for the sequential driver and for
  tasks already dispatched but not yet submitted.

**(b) Kill what is in flight on the farm.** The important discovery here is that
**nothing new needs recording**: `bsub -I -J <identity>` already uses the attempt
identity as the LSF job name, and the identity is the attempt directory name. So

```
bkill -J <identity>
```

is addressable from the durable record exactly as it exists today, for every
live attempt `live_attempts(root)` can already enumerate. The watcher's
directory scan is the enumeration half of cancel, already written.

**(c) Kill what is in flight locally.** The one gap. A `shell(...)` body at a
`local` placement runs a child directly, and that child has no LSF job name. Its
process-group id has to be recorded to be addressable — one field on the
existing `submit_intent` event, as execution detail that never reaches an input
digest.

## 4. Level 2: cancel from outside the process

Falls out of level 1 almost free, and is strictly more useful:

```
hedloom cancel <root> [--placement lsf] [--dry-run]
```

Reads `live_attempts(root)`, issues `bkill -J` per identity (and a group signal
for local children), writes nothing. Works when the owning process is *already
gone* — which is the situation an operator is actually in when they want this,
and which an in-process cancel cannot help with at all.

If only one thing is ever built, build this one.

## 5. What must not be built

* **A cancelled state written by a third party.** See §2.
* **A reaper, lease, or heartbeat.** Owner-bound lifetime plus a required
  `walltime` already bounds every orphan.
* **Interrupting a running Python task.** Dask cannot, and neither can we; the
  thread is inside a blocking subprocess call. The honest options are: let it
  finish, or exit the process.
* **A cancel path through `hedloom_exec`'s protocol.** Cancel is an operational
  action against a substrate, not a step in the attempt lifecycle.

## 6. Interactions to check when it is built

| With | Question |
| --- | --- |
| reuse | A killed job must not be reusable. `REUSABLE_OUTCOMES = {"succeeded"}` covers it — **provided** a `bkill`ed `bsub -I` returns something the transport records as failed rather than as indeterminate. Verify against a real farm; a `TransportError` here would leave the attempt claimed and unresolved. |
| the graph kernel | Same `try/finally` as A2. Cancel outstanding futures, return the partial report, do not raise away 190 completed corners. |
| the watcher | An observer may be mid-sweep when jobs vanish. It already handles "absent from LSF while the record says live" by recording nothing. No change. |
| `stop_on_failure` | Cancellation and stop-on-failure want the same "stop admitting" primitive. Build it once. |
| pooled placement | A pooled worker is not addressable by `bkill -J <identity>`; cancelling there means cancelling a *future*, not a job. See `pooled-placement-plan.md`. |

## 7. Test shape

A fakefarm whose `bkill` flips the fake job's state to `EXIT`, then assert:

1. the owner records a terminal, non-reusable outcome — written by the owner,
   with no second writer;
2. the run returns a **partial** report naming which invocations were stopped,
   rather than raising;
3. a second run of the same plan re-runs the cancelled invocation and reuses the
   ones that completed.

Test (3) is the one that matters: it proves cancellation did not corrupt reuse.

---

**Your call:** ☐ this shape ☐ build level 2 only, skip in-process cancel
☐ not now, revisit at: ☐ other:
