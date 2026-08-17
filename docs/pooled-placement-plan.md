# Pooled LSF placement via `dask_jobqueue.LSFCluster`: the plan

You asked whether an implementation already exists. It does, and the register
already picked it — this document is the plan on top of that, not a
rediscovery.

---

## 0. Where the register already stands

Two entries and a code stub, all pointing the same way:

* *Deferred, still wanted* — **"Pooled LSF via `dask_jobqueue.LSFCluster`"**,
  with the trigger recorded as *"many short invocations, where per-job queue
  dispatch costs more than the work"*, and explicitly **not** "many
  invocations".
* *Deferred, still wanted* — **"One-scheduler mixed topology"**, which requires
  pooled mode first and notes `LSFCluster` normally owns its own scheduler.
* `hedloom_exec.lsf.LSFPooledTransport` — a refusing boundary that already says
  it *"should adopt `dask_jobqueue.LSFCluster` rather than reimplement worker
  lifetime"*, because those workers *"already die with their scheduler via
  `death_timeout` and are `bkill`ed on cluster close"*.

And four constraints, from *What "both slots" must not be allowed to mean*: not
wholly pooled; not pooled workers for the readiness cluster; not inside
`hedloom_exec`; not a replacement for the `bjobs` watcher.

So the question is not *whether* dask-jobqueue, it is *what shape*, and there is
one design fork that decides everything else (§2).

## 1. What pooled actually buys — restated against the newest evidence

The register prices this as saving *queue dispatch latency*, which is right and
is measurable once the watcher works. There is a second gain it does not
record, and it may be the larger one:

> `concurrency-two-workers-2026-08-15.md` §9 found that the real ceiling on the
> submit host is not threads (~16 KiB each) but **one live `bsub` client process
> per in-flight job**. Pooled placement removes that process entirely — a task
> waiting on a pooled worker is waiting on a future, not on a subprocess.

Note what it does **not** remove: the *thread*. A task waiting for a pooled
result still occupies a worker thread on the readiness cluster, exactly as a
`bsub -I` waiter does. So pooled is a **process-count** fix, not a thread-count
fix — and that makes it, not the deferred async transport, the answer to §9.
The async path removes the thread and keeps the process; pooled removes the
process and keeps the thread. Only one of those addresses the constraint that
was found to bind.

## 2. The fork that decides everything: what runs on the pooled worker?

**(i) The command only — recommended.** The pooled transport ships the argv to
the LSF-backed cluster, waits for the result, and returns an observation.
Identity, the journal, the workspace and `execute()` all stay on the submit
host, exactly where they are today. A transport is a way to reach a substrate,
and this keeps it one.

**(ii) The whole of `_run_one` — reject unless measured.** Running the task
itself on the pooled worker moves journal writes onto farm nodes. The claim
protocol uses `fcntl.flock` on `events.jsonl`; over NFS, `flock` is the one
piece of the durability argument that does not obviously survive being moved to
many hosts. Every other invariant is host-agnostic; this one is not.

If (ii) is ever wanted, it needs a measurement first: concurrent `flock`
contention on the actual shared filesystem, from several farm nodes. Not an
argument — a test.

## 3. The mechanism nobody has named yet

A pooled transport holds a live `Client` to a second cluster. Therefore:

* `_require_shippable` **will refuse it**, correctly — it cannot be cloudpickled
  and sent to a worker with each task.
* `hedloom_run.graph`'s own docstring already anticipates this: *"a transport
  that must be a singleton … will need a factory constructed on the worker."*

That factory is `distributed.WorkerPlugin` — `setup()` / `teardown()` around the
worker lifecycle, registered with `Client.register_plugin()`, with user code
running in the worker's main thread. It is the documented mechanism for exactly
this, and it is finding B1 of `dask-usage-review-2026-08-16.md`. The pooled
placement is not blocked on new machinery; it is blocked on writing this plugin.

## 4. Two clusters in one process — the risk the register calls "assumed"

The register flags *"the in-process `SpecCluster` for readiness plus a separate
`LSFCluster` used as a transport is two clusters in one process; that it
composes cleanly is assumed, not demonstrated."* Concretely, three things to
demonstrate:

| Risk | What to check |
| --- | --- |
| global default client | `Client(...)` installs itself as the process default. The second one wins, silently, and any code relying on the default targets the wrong cluster. Mitigation: never rely on the default; the plugin hands the pooled client to the transport explicitly. |
| `get_client()` inside a task | Returns the *worker's* client, i.e. the readiness cluster — not the pool. Anything reaching for it gets the wrong answer with no error. |
| blocked threads | A task on cluster A waiting on a future from cluster B holds an A thread. Bounded by A's farm-worker resource cap, which is the same rail as today. |
| dashboard exposure | `hedloom_run.cluster`'s exposure work assumed one host. Pooled workers talk to a scheduler *over the farm network*, so `dashboard = "none"` and loopback binding become a different question, not a solved one. |

## 5. Profile shape

A pooled placement is a *pool*, so it names a worker shape rather than a job:

```toml
[placement.pool_short]
kind      = "lsf-pooled"
queue     = "short"
cores     = 1
memory_mb = 4000
walltime  = "2:00"
workers   = 20          # LSF jobs held open
max_jobs  = 20          # the Dask resource cap on the readiness cluster
```

Heterogeneous work needs more than one pool, because one `LSFCluster` has one
worker shape. That is precisely the register's *mixed topology* row, and it is
why "not wholly pooled" is a design constraint rather than a preference:
per-corner `-R rusage[...]`, per-corner `bkill`, per-corner accounting and
licence arbitration all live in the one-job-per-corner shape and are lost inside
a pool. Keep direct placement the default; route only short, uniform operations
to a pool.

## 6. The measurement that decides, and the rule to apply to it

Needs the watcher, which was **fixed on 2026-08-16**: it now filters on an
attempt's substrate, so façade-submitted jobs are visible and
`AttemptStatus.queue_seconds` — the whole input to this decision — becomes
obtainable on the next run with farm access.

> Pool an operation when its **median queue wait is a significant fraction of
> its median runtime** — as a starting rule, above roughly a third — *and* its
> corners are uniform enough to share one worker shape.

Below that, one job per corner is the better deal and buys everything in §5.
This is a per-operation decision, which is exactly why placement is authored per
operation and not per study.

## 7. Spike sequence

Each step is falsifiable on its own; stop at the first one that fails.

1. **Two clusters, one process.** An in-process `SpecCluster` with the shipped
   readiness shape and an `LSFCluster` alongside it. Submit trivial work to
   both. Demonstrates §4 or kills the design. No hedloom code involved.
2. **The plugin.** A `WorkerPlugin` that builds a pooled client in `setup()`, and
   a transport that finds it. Assert `_require_shippable` still refuses the
   naked pooled transport — that refusal is the guard that keeps the seam
   honest.
3. **One operation through a pool**, with the journal on the submit host
   (design (i)). Assert identity, reuse and the report are byte-identical to the
   direct path — the kernel invariant applies here too: *changing where work
   runs changes how long a plan takes and nothing else*.
4. **Mixed plan.** Some corners direct, some pooled, one run. This is the
   register's mixed-topology row and the first point at which the design pays.

## 8. Dependency — settled 2026-08-17

`dask-jobqueue` is now an optional extra, `hedloom-run[pooled]`, resolved at
**0.9.0**. It asks only for `distributed>=2022.02.0`, so it composes with the
`distributed==2026.7.1` pin rather than arguing with it — checked, not assumed.

It is separate from `[dask]` on purpose: those extras answer different
questions, and a farm sweep placing one job per corner needs the scheduler
without ever needing a pool. It sits in `hedloom-run` and no lower unit,
because a pooled transport holds a live Dask client and `hedloom-exec` imports
neither Dask nor `hedloom_flow` — the exclusion that keeps the attempt record
independent of how anything is scheduled. `LSFPooledTransport` in that unit
therefore stays a refusing boundary whatever else is built.

Its LSF support no longer has to wait for a site's `bsub` to be checked, for the
reason below.

## 9. Status, 2026-08-17: steps 1 and 2 pass, and step 3 has a substrate

**Step 1 — two clusters in one process: demonstrated.** Both schedulers come up
and both run work. The §4 table is now measured rather than feared:

* *global default client* — **confirmed**. The second `Client(...)` silently
  takes the process default, no warning. `set_as_default=False` is the fix and
  must be passed when the pooled client is *constructed*: once the default has
  moved, a later non-default client does not move it back.
* *`get_client()` inside a task* — **confirmed**. It returns the readiness
  cluster, never the pool. Wrong answer, no error. The plugin has to hand the
  pooled client over explicitly and nothing may reach for the ambient one.
* *blocked threads* — behaves as predicted, bounded by the readiness cluster's
  placement resource cap, which is the rail that already exists.
* *dashboard exposure* — still open, and still the one item on that table that
  is genuinely LSF-specific rather than a property of two clusters.

**Step 2 — the plugin: passes.** A `distributed.WorkerPlugin` builds the pooled
client in `setup()` and a task finds it through `get_worker()`. The cost was
never new machinery, exactly as B1 of `dask-usage-review-2026-08-16.md` said.
`_require_shippable` still refuses the naked pooled transport, naming the
placement — that refusal is the guard that keeps the seam honest, and it is
intact.

**Step 3's blocker was the substrate, and it is gone.** `exec/tests/fakefarm`
now implements batch submission — `bsub` with a job script, returning
`Job <id> is submitted to queue <q>.` and leaving the work to a detached
supervisor — so a real `LSFCluster` scales, runs work and cancels on close
against it, with no LSF on the host. `run/tests/test_pooled_farm.py` is that
test.

Modelling batch honestly matters more than convenience: a batch job is **not**
owner-bound. `bsub -I` dies with its client and the whole crash-window argument
in `hedloom_exec.attempt` rests on that; a pooled worker has no such binding,
and its lifetime is held instead by `death_timeout` from the worker's side and
`bkill` on cluster close from the pool's. A fake that bound pooled workers to
their client would have made pooled placement look safer than it is and left
both of those mechanisms untested.

Two findings worth carrying into the implementation:

* **Teardown order is load-bearing.** Close the client before the cluster, and
  the pool after the readiness workers that hold clients into it. The wrong
  order produces a reconnect storm that reads as a failure and is not one.
* **A pooled payload must not be pickled by reference.** Workers are separate
  processes with nothing of the caller's on their path, so a function sent by
  reference simply never completes — no exception at the client, a `gather`
  that waits forever. Anything crossing to a pooled worker has to be sent by
  value or be importable there.

**Still farm-only:** `AttemptStatus.queue_seconds`, which is §6's whole
decision input, and the dashboard-exposure question in §4.

## 10. Built, 2026-08-17: design (i), all four spike steps

The decision box below was taken as **design (i), command only**, and the spike
sequence was run to the end. `hedloom_run.pooled` is the implementation.

* **`Site` accepts `kind = "lsf-pooled"`**, with the §5 profile shape and a
  vocabulary narrower than a direct placement's: no `licences`, no raw
  `resources`, because a pool's workers are claimed before any invocation is
  routed to them and a per-corner request would be accepted and then ignored.
  `max_jobs` is required, as it is for `lsf-interactive`, and it is a different
  number from `workers` — invocations in flight versus LSF jobs held open.
* **`hedloom.pooled()`** is the authoring policy, beside `local()` and `lsf()`.
  Placement stays per operation, which is what §6 said it had to be.
* **`hedloom.session(...)` opens the pool**, registers the plugin on every
  readiness worker, and closes readiness *before* the pool. A site declaring no
  pool opens none and never imports `dask_jobqueue`.
* **The transport holds no client.** It carries the pool name and what the pool
  was asked for; the live client is built on the worker by the plugin and found
  through `get_worker()`. `_require_shippable` still refuses a transport that
  holds one, which is the guard that keeps this honest rather than a habit.
* **Step 4, the mixed plan, runs**: some corners on their own `bsub -I`, some
  through the pool, one plan, one run, one report — `tests/test_pooled_placement.py`.

Three things the implementation had to settle that this plan did not anticipate:

* **The readiness cluster is `inproc`; a pool must not be.** Those workers are
  on farm nodes and reach the scheduler over the network, so `open_pools` passes
  no protocol and no loopback binding. The exposure work in `hedloom_run.cluster`
  assumed one host and does not transfer — §4's dashboard row, still open.
* **`register_plugin` refuses a duck-typed plugin.** Real inheritance from
  `distributed.WorkerPlugin` is required, so the plugin class is built by a
  factory at call time — that is what lets this module import without Dask, so
  a profile naming a pool can be *read* on a machine that will never run one.
* **Pooled placement genuinely diverges from the sequential kernel**, and is the
  first thing that does. A pooled invocation reaches its pool through a client a
  Dask worker holds, and `sequential=True` has no worker. It is refused by name
  rather than quietly run here, because succeeding would publish an attempt
  record and teach the author that `sequential=True` means something it does not.

What is still unmeasured is what it *buys*: that needs `queue_seconds` from a
real farm, and §6's rule to apply to it.

---

**Your call:** ☑ design (i), command only ☑ spike sequence as written
☐ not until the watcher gives us queue_seconds ☐ other:
