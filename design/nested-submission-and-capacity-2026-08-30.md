# Nested submission, and what a waiting invocation costs (2026-08-30)

Written on a date, about a tree that will move. For what the code does now,
read `docs/guide/refusals.md`, `run/ONTOLOME.md`, and `examples/live_source.py`.

The question that started it was ordinary: *a study reads something served from
outside it — a live service, a database, a file another team owns. How does the
study re-read it on every run, without recomputing work the change did not
touch?* Answering it walked into staged plans, and staged plans walked into a
deadlock. This records the walk, the measurements, and why the thing shipped is
a refusal rather than a scheduler feature.

## 1. Why the fetch cannot be an ordinary operation

Three facts, in the order they bind.

**A declared source is the only input identified by its content.** Every source
is read exactly once per submission, before anything runs, and fingerprinted
(`Site.fingerprints` → `fingerprint_sources` → `fingerprint_file`,
`run/src/hedloom_run/site.py`). Identical bytes give an identical fingerprint,
so the plan below is reused; different bytes invalidate it transitively.

**An operation cannot do that job, structurally.** `plan_bundles`
(`exec/src/hedloom_exec/planned.py`) walks the plan document in one pass and
computes every `input_digest` before returning, and a consumer's `inputs` entry
for a produced artifact is `output:<producer digest>:<name>` — the producer's
*identity*, never the bytes it wrote. `IDENTITY_KEYS` (`exec/.../reuse.py`) is
the complete list of what participates, and output content is not on it and
could not be: every digest is fixed before the first body runs, so nothing an
operation produces exists yet to be digested.

So an operation that fetches unconditionally must carry something that changes
every submission — a nonce in `config` — and that change propagates to everything
downstream whether the fetched document moved or not. **Freshness and reuse
cannot come from the same node.** `reuse.py`'s module docstring already said
so: *"An operation whose result depends on … a mutable network resource is not
honestly reusable, and no digest can fix that."*

**A `Site` is a declaration; a `Session` is the live compute.** Sharing the
`Site` shares roots and address spaces, not workers. `Session.__enter__` is
what opens the cluster, the client and — via `open_pools` — the farm pools. A
`submit` not given a session opens its own, and would start a second set of LSF
jobs beside the ones already held.

## 2. The shape that works, and the trap in it

Two stages. A nonce-bearing invocation fetches unconditionally, then authors and
submits an inner Plan whose declared source decides what that fetch
invalidated. Each plan is still fully determined before it spends anything,
which is the invariant staging keeps and result-dependent control would break.
The inner run goes through the session the caller already holds, so one budget
stays open rather than two.

`examples/live_source.py` is that shape, end to end, with a dict standing in for
the service. Measured output:

```
first  (new document)      inner: tally:ran     summarise:ran
second (unchanged)         inner: tally:reused  summarise:reused
third  (document changed)  inner: tally:ran     summarise:ran
```

The trap: **the staging invocation holds one unit of its own placement for the
whole inner run**, because it is blocked waiting on it. Every task is annotated
with one unit of the placement it resolved to (`graph.py`, `_admission`), and
that annotation is not accounting — it is what stops Dask placing or *stealing*
a local invocation onto a farm-sized worker (`graph.py` header). With the
example's site declaring `local: 1`, the run hung: exit 124 at 45 seconds, no
exception, no log line, workers all reading busy.

## 3. Alternatives, measured

### `secede()` — already rejected, and would not have been enough

The register rejected it for observability (`docs/vision/open-concepts.md`,
*"secede(): understood, and deliberately not used"*; user direction 2026-08-04,
*a submit worker holding at least one live `bsub -I` should read as running*).

It would also not have fixed this. Rule R9 says secede opens the thread gate
only. Confirmed against `distributed 2026.8.0`, one minor ahead of the version
the rules page measured: `_transition_executing_long_running`
(`worker_state_machine.py`) moves the task to `long_running` and calls
`_ensure_computing()`, never touching `available_resources`;
`_release_resources` is called from exactly one place, `_execute_done_common`,
when the task is genuinely done.

### `worker_client()` — Dask's own answer, and its boundary

`get_client()` + `secede()` + `rejoin` on exit. Two details are worth keeping:
`secede()` calls `_adjust_thread_count()`, so the vacated slot is a real thread
rather than a notional one; and `rejoin()` blocks on a rendezvous through the
executor's `_rejoin_list` — *"The next thread to finish a task will leave the
pool to allow this one to join"* — so reacquisition is arbitrated inside the
`ThreadPoolExecutor`, with no scheduler round trip.

Measured, one worker, two threads, `resources={"slot": 1}`, outer task using
`worker_client()` to submit and gather an inner task:

| inner task asks for | result |
| --- | --- |
| no resource | `42`, immediately |
| `resources={"slot": 1}` | `TimeoutError` after 20 s — never scheduled |

The split is principled, not an oversight. **Threads model parallelism;
resources model possession.** A task waiting is not computing, so vacating a
thread is correct; a task that reserved a GPU still holds the GPU while it
waits, so releasing a resource is not.

That distinction is exactly where hedloom sits astride: for `lsf()` and
`pooled()` the unit is genuine possession — a blocked `bsub -I` really is
holding a farm job, and Dask's refusal is right — while for `local` it is
really parallelism. The staging invocation is the only case that possesses
nothing while it waits.

### Donation — the idea, the demonstration, and the edge

Direct user proposal: since the parent is blocked anyway, let the child use the
parent's unit. It is a donation, not Dask's work-stealing (which moves
*unannotated* tasks between workers and is what the annotation exists to
prevent).

The core trick needs no patching. Submit the child **unannotated**, so the
resource gate does not stop it, but **pinned to the parent's own worker**, so
being unannotated cannot let it be placed or stolen elsewhere:

```python
def outer(x):
    with worker_client() as c:
        here = get_worker().address
        return c.submit(inner, x, workers=[here],
                        allow_other_workers=False).result()
```

At `resources={"slot": 1}` — the configuration that timed out above — this
returns `42`.

The edge, measured: unannotating does not lend one unit, it removes the gate.
Declared capacity 1, parent submits 4 children:

```
declared slot capacity = 1, children = 4, peak concurrent = 4
```

Bounded only by the worker's threads. On `local` that over-subscribes the
submit host; on `lsf` it is four `bsub -I` against a budget of one — the exact
thing the annotation scheme exists to make unrepresentable, reintroduced
through the back door.

So donation must be **counted**, and Dask cannot count it: resources are
declared per worker at construction, with no supported API for minting a
one-off grant. The counting would live in hedloom — a semaphore in the parent
sized to the units it holds. Two further constraints:

* **Per-placement.** Pinning children to the donor's worker is only right for
  children on the donor's own placement, since each placement is its own
  worker. An inner `lsf()` invocation must still be annotated normally.
* **A parent can only lend what it holds.** One unit means the inner plan runs
  at concurrency 1 on that placement, however many points it has. Donation buys
  deadlock-freedom, not parallelism.

### Configuration — what the register already predicted

*"If waiters and compute live on different workers, or if the pool only ever
waits, configuration replaces it entirely"* — the same move that resolved the
`local`/`farm` split. Give the staging operation its own placement:

```python
SITE = Site(..., placements={"local": 1, "staging": 1})

@operation(policy=named_policy("staging")(), config={"nonce": parameter(str)}, ...)
def refresh(*, nonce): ...
```

Verified: the example runs to completion with `local` at **1** — the
configuration that deadlocks otherwise — with inner reuse unchanged. The waiter
no longer competes with what it is waiting for, because it was never in the
same pool. Strictly better for throughput than donation, since the inner plan
gets the whole declared budget in parallel.

## 4. What was built: a refusal, not a feature

Donation was judged too complex for now. What shipped is the safety net:
`NestedCapacityExhausted`, raised before the inner plan spends anything.

The condition is decidable, which is what allows a refusal rather than a
warning:

> Every running task holds one unit of its placement. Units held by *blocked*
> waiters cannot be released until the work they are waiting for runs. If the
> waiters account for a placement's whole capacity, every unit is held by
> something that cannot proceed, and no task of that placement will ever be
> admitted again — by this run or any other.

Sound rather than cautious, and the counter-case is pinned by a test: one
waiter against a declared capacity of two proceeds, because one free unit is
enough for the inner plan to make progress serially. Refusing there would be
the kernel inventing a deadlock.

Mechanically: `_run_one` became a thin wrapper recording which placement's unit
the calling thread holds — it already received the `PlannedInvocation`, so the
placement was in hand — and `run_plan_graph` a wrapper counting its caller's
unit as blocked for the duration. `_require_nesting_headroom` sits beside the
existing `_require_admission` and `_require_shippable` pre-flight checks. A run
submitted from the driver holds no unit, reads an empty counter, and returns:
the ordinary path pays nothing. Both structures are process-global on purpose —
this kernel's workers are threads in the submitting process, so a sibling
waiter is as much a claim on capacity as an ancestor is.

The 45-second silent hang is now a sub-second refusal naming the placement, its
capacity, how many units are held by waiters, and the two ways out.

## 5. One gotcha worth not rediscovering

A body that reaches a live `Session` cannot name it as a plain global of the
module that *defines the operation*. Dask serializes every task even on an
in-process cluster, and a function defined in `__main__` is pickled **by
value** — every global it names travels with it, and a `Session` holds a
`_thread.lock`. The first run of the example died on exactly that:

```
TypeError: the transport for placement 'local' cannot be sent to a worker
(TypeError: cannot pickle '_thread.lock' object)
```

Reaching it through an imported module ships a reference instead, which is what
`examples/live_source_state.py` exists for. A module-level `Site` is
unaffected: it is plain data.

## 6. Left open

* **Donation, if it is ever wanted.** The design is above and the core is
  demonstrated; what is missing is the counting, and a decision about whether
  "runs, serially" is worth the machinery when one placement declaration gets
  the same safety with better throughput.
* **The observability rule would need amending, not breaking.** A donating
  parent reads as `long-running`, so the Workers table shows `executing = 0`
  for it. The 2026-08-04 direction was written about farm-job waiters; a
  plan-authoring coordinator holds no `bsub`. Distinguishing *waiting on a farm
  job* from *waiting on a nested plan* looks like a legitimate amendment, and
  should be recorded as its own entry rather than assumed.
* **The refusal is graph-kernel only.** The sequential driver has no cluster
  and no capacity gate, so a nested submit there simply recurses. That is the
  same asymmetry the existing `UnsupportedPlacement` pre-flight check already
  has, and for the same reason: it is a fact about a cluster, not about a plan.
