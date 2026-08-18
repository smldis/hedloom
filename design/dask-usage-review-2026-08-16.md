# Are we using Dask correctly, and what are we leaving on the table?

Asked before starting development. Checked against the pinned
`distributed==2026.7.1` source in this environment *and* against the published
documentation, which turned out to matter — one finding below comes only from
the docs and contradicts an option I offered you two days ago.

Same convention: `**Your call:**` slots.

**Short answer.** The way the graph kernel *uses* Dask is sound and in several
places better than typical — see §D, which is most of the code. Three things are
wrong or missing, one of which I had ranked as "tolerable" and now would not.
Two supported features are being left on the table, and both are things the
register currently describes as open problems.

---

## A. Wrong or missing

### A1. Work stealing actively pushes local work onto the farm worker

New, and it upgrades the severity of §2 in `concurrency-two-workers-2026-08-15.md`.

I described the risk there as initial placement: an unrestricted task *may* be
placed on the farm worker because the scheduler's preference is a heuristic. The
documentation adds a second, independent mechanism that keeps doing it
afterwards:

> "If a task has been specifically restricted to run on a subset of workers …
> the scheduler will still actively attempt to steal the task to balance the
> load. However, the scheduler will strictly enforce those restrictions during
> the steal."
> — *Work Stealing*, distributed docs

Read the two halves separately:

* A **restricted** task is safe. It can only be stolen onto a worker that
  satisfies its restriction, and with one farm worker there is nowhere to move.
* An **unrestricted** task has no restriction to enforce, so it can be stolen
  anywhere — including onto the farm worker, at any point during the run.

And the heuristic is aimed squarely at our shape. Stealing targets *saturated*
workers with a backlog on behalf of *idle* ones; a local worker with one thread
and a queue of post-processing is the saturated one, and a farm worker with
`nthreads` sized to a 200-job cap always has free slots. Work stealing is on by
default (`distributed.scheduler.work-stealing`).

So the option I offered as "real but tolerable, rely on Dask's preference" is
not tolerable — there is no preference to rely on, and a second subsystem is
working against it. The important consequence is the reverse, though, and it is
good news:

> Annotating **every** task does not merely fix initial placement. It makes work
> stealing *safe*, because restrictions are strictly enforced during a steal.
> Pin every task and stealing becomes a load-balancer you can leave on; pin only
> farm tasks and it becomes a mechanism that undoes your placement.

The alternative — disabling work stealing globally — is a worse trade: it is a
cluster-wide setting, it does nothing about initial placement, and it gives up
rebalancing for local work that would genuinely benefit.

**Built 2026-08-16.** A1 shipped as the single change argued here and in
`concurrency-two-workers`: one worker per placement, one placement resource on
every task the run can serve, and a preflight that refuses a served placement
the supplied cluster cannot admit. Work stealing remains enabled and safe.

**Your call:** ☐ pin every task, leave stealing on ☐ pin every task *and*
disable stealing ☐ other:

### A2. Nothing cleans up when the `as_completed` loop is left early

`graph.py:317-318`:

```python
for future in as_completed(list(futures.values())):
    step = future.result()
```

No `try`, no `finally`, no cancellation. `.result()` re-raises on the client, so
anything that escapes `_run_one` leaves the loop. `_run_one` catches
`(AttemptError, TransportError)` — so `ConcurrentClaim` (a `JournalError`, not
an `AttemptError`) escapes, as does any bug in the kernel, a worker death, or a
`KeyboardInterrupt` in the loop itself.

What is left behind, in order of unpleasantness:

1. **The report is lost entirely.** Not degraded — the function raises, so a
   sweep that was 190/200 complete returns nothing, even though every one of
   those 190 attempts is durably recorded and reusable. The record survives; the
   run's own account of itself does not.
2. **Every outstanding future keeps running**, each holding a live `bsub -I`
   client and a real farm job.

Dask's primitive is `client.cancel(futures)`, and it is worth being exact about
what it can and cannot do, because owner-bound lifetime interacts with it:

* it stops tasks that have **not yet started**;
* it does **not** interrupt a task already executing in a worker thread — Dask
  has no mechanism for that, and ours are sitting in a blocking `subprocess`
  call.

So cancellation stops the backlog and nothing else; the in-flight jobs die only
when the owning process does, which is exactly what `PR_SET_PDEATHSIG` already
guarantees. That is a coherent story — but only if the process actually exits.
An exception that escapes into a caller who catches it and carries on is the bad
case, and it is the one that costs a farm.

Shape of the fix, all in the kernel:
`try/finally` around the loop; cancel outstanding futures on abnormal exit;
return the partial report rather than raising, with the unfinished invocations
carried as an outcome; and catch the journal errors inside `_run_one` so a claim
conflict becomes a recorded outcome instead of an escape. This is review point 5
with the Dask half named.

**Status 2026-08-16: still open.** The `as_completed` loop still has no
`try/finally` and does not call `client.cancel` on outstanding futures.

**Your call:** ☐ partial report + cancel ☐ re-raise but cancel first
☐ other:

### A3. The task keys defeat Dask's own statistics

`_task_key` (`graph.py:229-238`) returns `f"{authored_key}-{digest[:8]}"`. Dask
derives a task's *prefix* by splitting at the first `-`, which I checked:

```
key_split('measure[tt]-a1b2c3d4')  ->  'measure[tt]'
```

So every corner is its own prefix. Two things follow, and neither is obvious:

* **Dask learns nothing about how long anything takes.** `TaskPrefix` carries
  `duration_average` as an exponentially weighted moving average
  (`scheduler.py:996`), and with one task per prefix it never accumulates.
  `_get_prefix_duration` then falls back to
  `distributed.scheduler.unknown-task-duration`, default **500 ms**
  (`distributed.yaml:33`). Every task is estimated at half a second — a
  three-hour farm job and a ten-millisecond local call alike.
* Occupancy is built from exactly those numbers, and occupancy is what
  `decide_worker` and work stealing both run on. So A1 is not just operating on
  a heuristic; it is operating on a **fabricated constant**.
* The dashboard's aggregate view is one group per corner, which is useless
  precisely when the sweep is large enough to want it.

One-line fix — put the operation first:

```python
return f"{item.operation}-{name}-{item.input_digest[:8]}"
```

The prefix becomes the operation, so corners of the same operation share
statistics and share a colour in the task stream, and the authored key stays in
the key where the docstring wanted it. After the first few corners finish, the
estimates become real, which improves placement for the rest of the sweep. An
operation name containing `-` truncates the prefix early but still shares it
across corners, so it degrades to harmless.

**Built 2026-08-16.** `_task_key` now returns
`f"{item.operation}-{name}-{item.input_digest[:8]}"`, so the prefix is the
operation and its rolling duration average can be learned.

**Your call:** ☐ operation-first keys ☐ leave keys alone, they are for humans
☐ other:

---

## B. Supported features we are not using, that we should at least decide about

### B1. `WorkerPlugin` is the named answer to a question the code leaves open

`graph.py`'s cluster note ends:

> "A transport that must be a singleton — a pooled one holding a client to a
> second cluster — cannot be passed this way and will need a factory constructed
> on the worker."

That factory is a documented Dask feature, not something to invent.
`WorkerPlugin` provides `setup()` / `teardown()` hooks around a worker's
lifecycle, is registered with `Client.register_plugin()`, and the docs say the
user code "always runs within the Worker's main thread" — which is exactly the
place a non-serializable client belongs.

Nothing to build now. Worth recording, because the pooled-versus-direct
question in the register is currently priced as if it needed new machinery on
the Dask side. It does not; the cost is all in `hedloom_exec`.

**Your call:** ☐ record in the register ☐ other:

### B2. `distributed.Semaphore` exists, and it is the steel-man of what we rejected

The register rejects putting a `threading.Semaphore` in `LSFTransport` partly
because it would be "silently wrong across processes". `distributed.Semaphore`
is scheduler-side and correct across processes, so that objection does not
survive contact with it.

The decision should still stand, on the stronger reason:

> A semaphore gates **inside** the task, so a task waiting for farm capacity
> holds a worker thread for its whole wait. A resource gates **at the
> scheduler**, so a waiting task occupies nothing at all.

Worth amending the register so the rejection rests on the reason that does not
have a counterexample.

**Your call:** ☐ amend the register ☐ other:

### B3. `retries=` is unreachable today, and the reason is interesting

`client.submit` takes `retries=`, and it can never fire here: `_run_one` catches
both `AttemptError` and `TransportError` and returns an outcome instead of
raising. So a transient `bsub` hiccup permanently fails a corner and blocks
every dependent, with no distinction in the report between "this simulation
diverged" and "the submit host hiccuped".

The interesting part is that Dask retries would actually be *safe* here, which
they usually are not for side-effecting work: an attempt's identity is chosen
before submission and is content-addressed, so a re-executed `_run_one` re-enters
`launch_or_attach` and resolves to `attached` or `completed` rather than
duplicating a job. The durable-record design bought that property without
anyone claiming it.

I would still not use them. `TransportError` is documented as *indeterminate*
and `SubmissionRefused` as "definitely nothing was accepted", and that
distinction — which is the whole basis for a safe retry — lives in
`hedloom_exec`, not in a scheduler that cannot see it. But it should be decided
rather than left to an omission.

**Your call:** ☐ retry policy in exec, on `SubmissionRefused` only ☐ use Dask
`retries=` ☐ leave it: a transient failure is a failure ☐ other:

### B4. A warning to carry forward about `dask.annotate`

The resources documentation warns that annotations "may be lost" through graph
optimization and fusion, and gives a config flag to disable fusion. This does
**not** affect us — `client.submit(..., resources=...)` is explicit and is not
subject to it — but if anyone later reaches for `dask.annotate` on a collection
to express placement, the annotation can silently vanish. Given that a lost
annotation here is R7 (a permanent hang) or a lost cap, it is worth writing down
that `resources=` on `submit` is the form this project uses.

**Your call:** ☐ note it in the kernel docstring ☐ other:

### B5. Nothing in the documentation blesses what we are doing with resources

Worth knowing rather than acting on. The resources page is written entirely
about GPUs and memory; it never mentions rate-limiting submissions to an
external system, which is what we are using it for. It does say

> "Dask does not model these resources in any particular way"

and that it relies on the user to declare availability accurately — which is
precisely the permit-slip reading, and it is what makes declaring `lsf` = the
site's MAX JOB policy a legitimate use rather than an abuse. But the
justification has to live in our register, because upstream has not written it
down and could change the feature without thinking about us.

---

## C. Correctly declined — recorded so they do not get re-litigated

* **`pure=True` / Dask-level deduplication.** Reuse is `hedloom_exec`'s decision
  against declared inputs. `pure=False` is right and the comment already says why.
* **Raising on failure.** The docs confirm what the alternative costs: "All
  futures that depend on an erred future also err with the same exception." The
  kernel returns a blocked outcome instead, so one corner failing does not
  abandon the other forty-nine. This is the single best decision in the file.
* **`fire_and_forget`.** Offered by the docs for side-effecting tasks with no
  future reference; wrong here, because the report and its ordering are the
  product.
* **Dask's event log as the record.** The journal must outlive the cluster and be
  the only authority. Correctly not used.
* **`secede()`.** Rejected for observability (R9), and the resource mechanism
  beats it on that axis rather than trading it away.
* **Nannies / multi-process workers.** A restart would take live `bsub` clients
  with it.
* **Root-task queuing.** Will be bypassed once every task carries a resource
  (R6). No loss: queuing exists to stop many root tasks saturating memory, and
  ours hold addresses and strings.
* **`priority=`.** Would let complete corners land before half-finished ones,
  but content-addressed reuse already makes partial progress durable, so the
  ordering buys little.
* **Adaptive scaling, spill-to-disk.** Shapes for a compute cluster; this is a
  controller.

---

## D. What is already right, and unusually so

Worth saying, because it is most of the code and none of it should change:

* **`_require_shippable`.** A cloudpickle pre-check that refuses an unshippable
  transport *by placement name*, before anything runs. Dask gives you no such
  thing — left alone it fails deep in the protocol naming neither the placement
  nor the cause. This is the pattern the resource pre-flight check in
  `concurrency-two-workers-2026-08-15.md` §7 should copy.
* **Dependencies as real task arguments.** `_Step` travels the graph edge, so a
  task depends on exactly the outputs it declared inputs from, with no shared
  state between tasks. This is what lets the same function serve a thread, a
  process, or a future pooled worker.
* **The client is required, never created.** Cluster shape is an operational
  decision; a library that started one silently would be making it.
* **Report in plan order, events in completion order.** Two different questions,
  answered separately and deliberately.
* **`pure=False` with explicit, collision-checked keys.** The correct
  combination, and the collision loop is not an accident — two tasks sharing a
  key would be one task to Dask, and one invocation would never run.

---

## E. Ordering

Against the plan in `concurrency-two-workers-2026-08-15.md` §10, these slot in
without disturbing it:

* **A3 (keys)** is a one-line change and should go in *first*, before any
  placement work — it is what makes Dask's own placement decisions run on
  measured numbers instead of a 500 ms constant.
* **A1** is not a new task; it is a reason the "pin every task" step is
  mandatory rather than a nicety, and a reason not to take the
  disable-stealing shortcut.
* **A2 (cleanup)** joins review point 5 as one change.
* **B1, B2, B4, B5** are register amendments, not code.

**Status 2026-08-16.** The first three implementation steps from the companion
ordering are done: operation-first keys, the façade's placement transports,
and the indivisible cap/cluster/annotation/preflight change. A2 remains open;
there is still no cleanup or cancellation around `as_completed`.

**Your call:** ☐ this ordering ☐ other:

---

Where you disagree, the argument is the useful part. A1 is the one I would most
like you to push on, because it is the difference between "annotate everything"
being a design preference and being the only thing that works.

---

# Answers (2026-08-16)

## A1 — can we auto-annotate unannotated tasks as `local` at the `@operation` boundary? Should we?

**It is already done there, and that is the answer.** `hedloom_flow.model`:

```python
def local(**options: Any) -> Policy:      # model.py:379
    return Policy("local", options)

policy: Policy = field(default_factory=local)   # model.py:585
```

So every invocation leaves authoring carrying an explicit `Policy("local", {})`.
The records confirm it — a local invocation's placement event reads
`{"requested": {"name": "local", "options": {}}}`, not `{}`. The
`or "local"` in `select_transport` is belt-and-braces, not the mechanism.

**There is therefore no unannotated invocation anywhere.** The unannotated
*task* is created entirely by the kernel, which never translates a resolved
placement into `resources=`. Nothing needs adding at the `@operation` boundary.

**And no, do not put the annotation in the Plan.** A Dask resource name is
kernel vocabulary. Encoding it in the document would give one fact two
encodings — the single-writer violation from `concurrency-two-workers` §4 — and
hand the sequential driver a field it cannot use. The placement name is already
the one encoding; the kernel's job is to translate it, not to have it restated.

**Correction after implementation.** The earlier sketch that raised whenever a
placement was absent from the caps was too broad. The shipped kernel must
distinguish two cases:

```python
def _admission(item, transports):
    name = (item.policy or {}).get("name") or "local"
    if transports.get(name) is None:
        return {}
    return {f"placement:{name}": 1}
```

A placement the run **can serve** must be annotated, and
`_require_admission` refuses before submission if the supplied cluster does not
declare its capacity. A placement the run **cannot serve at all** is
deliberately left unannotated: `select_transport` then refuses that invocation
exactly as the sequential kernel does, while unrelated branches continue.
Annotating it would leave it unrunnable forever and make the two kernels
disagree about what the plan does.

**Built 2026-08-16 as one change, not three.** `Site` supplies the caps and
cluster spec, `Study.submit` supplies transports for every declared in-process
placement (including the default `local`), the graph annotates every servable
task, and the admission preflight rejects cluster/profile mismatches.

**Your call:** ☐ annotate every servable task and refuse a missing cluster cap
☐ other:

## A3 — should the key be more general still: `measure`, not `measure[tt]`?

**Yes — the operation is the right prefix, and that is what I meant.** The rule
worth writing down:

> The prefix should be the **coarsest grouping whose members have similar
> durations.**

By that test: the corner is too fine (one task per group, so nothing is ever
learned); the placement is too coarse (a ten-second extract and a three-hour
simulation share one); the operation is right, because corners of one operation
are the same work on different inputs.

```python
return f"{item.operation}-{name}-{item.input_digest[:8]}"
```

Prefix becomes `measure`. The authored key stays in the key, so the task stream
still tells you which corner is running, and now every corner of one operation
shares a colour and a duration average.

Two notes:

* Where an authored key already begins with the operation you get
  `measure-measure[tt]-a1b2`. Cosmetically redundant; not worth a branch to
  strip, because a conditional here is a second rule someone has to remember.
* **A refinement worth considering.** If one operation is authored at more than
  one placement, its average blends two very different populations. Since the
  placement name is available at submit time, `f"{operation}@{placement}-…"`
  keeps them apart (`@` is not `-`, so the prefix survives) and is strictly more
  accurate for free.

**Built 2026-08-16.** The operation-prefix option shipped; the
operation-at-placement refinement did not.

**Your call:** ☐ operation prefix ☐ operation@placement prefix ☐ other:

## Retries — recorded as delayed

Added to the register's *Deferred, still wanted* table rather than restated
here, since that table is the feature list. The row carries the trigger and the
reason the retry belongs in `hedloom_exec` rather than in `resources=`-style
scheduler configuration.
