# How Dask decides where work runs

The companion to [`dask-scheduling-rules.md`](dask-scheduling-rules.md). That
file is the reference: ten rules, each cited to a line of
`distributed==2026.7.1`. This one is the explanation — what the pieces are, and
why hedloom's cluster is shaped the way it is.

There is an interactive version of this in servedgui under **Scheduling &
resources**, which steps a small simulated scheduler through ten worked
setups. This page is the same material as prose, so it survives without a
browser and travels with the code it describes.

---

## The whole idea in six things

| Thing | What it is |
| --- | --- |
| **cluster** | What you build: a scheduler plus its workers. Lives entirely on the submit host. **It is not the farm.** |
| **scheduler** | Holds the whole plan; decides which worker gets each task, and when. |
| **worker** | Threads (how many tasks it may run at once) and optional resources. |
| **task** | One invocation. May ask for resources, or ask for nothing. |
| **resource** | Just a name and a number. Nothing measures it. It is a permit slip. |
| **transport** | Hedloom's, not Dask's: the object a running task calls to actually start the work — `bsub -I`, or a python function. |

**A resource is not a thing the machine has.** Declaring
`resources={"placement:lsf": 200}` on a worker means exactly one thing: at most
200 tasks carrying the label `placement:lsf` may execute here at once. No
memory is checked, no licence is counted, nothing is measured. That is the
entire mechanism, and everything below is a consequence of it.

## Where all of this lives

```
submit host — one python process
+------------------------------------------------------+
|  THE CLUSTER            LocalCluster or SpecCluster   |
|                                                       |
|  scheduler    decides which worker gets which task    |
|     |                                                 |
|     +-- worker "local"   threads [ ][ ]               |
|     |          |                                      |
|     |          +-- task --> TRANSPORT --> a python fn |
|     |                                                 |
|     +-- worker "farm"    threads [ ][ ][ ][ ]         |
|                |                                      |
|                +-- task --> TRANSPORT --> bsub -I ----+---> LSF
+------------------------------------------------------+       real machines,
                                                                real queue
```

Everything inside the box is one Python process on the submit host. The workers
are threads, not machines. The only thing that ever leaves the box is a `bsub`
command.

## The cluster is not the farm

"Cluster" is the word that causes the most confusion here, because there are two
of them and only one has machines in it.

The **Dask cluster** is a controller: a scheduler and one worker per placement,
all inside the single Python process the study was launched from. It owns no
compute. The **LSF farm** is the thing with machines, and it does the real
scheduling — queues, priorities, MAX JOB limits, licences.

| Dask decides | LSF decides |
| --- | --- |
| which of your tasks may be in flight, and when | where each job lands, and when it actually starts |

Two things follow. A farm placement's thread count means *how many jobs may be
in flight* and is derived from that placement's `max_jobs`; the local
placement's cap is local concurrency on this host. And if the Dask cap exceeds
what LSF permits, LSF wins and the extra jobs simply pend — **the Dask cap is a
courtesy rail, not a correctness requirement.** It is there to keep a study
inside the share of the farm you decided to spend on it, which is a smaller
number than the site's ceiling because that ceiling counts everything you have
running from every source.

## Two shapes of cluster

| What you write | Can it express the per-placement shape? |
| --- | --- |
| `LocalCluster` — one recipe applied to every worker: `n_workers`, `threads_per_worker`, `resources` | **No.** Every worker gets the same threads and the same permits. |
| `SpecCluster` — a dict per worker: `{"local": {…}, "farm": {…}}` | **Yes.** Each worker has its own threads and its own permits. |

They are not rival families: `LocalCluster` *is* a `SpecCluster` that writes the
repetitive spec for you (`deploy/local.py:23`). Using `SpecCluster` directly
just means writing it yourself, which is what `Site.cluster_spec()` and
`cluster_for(site)` do.

Whichever you use, **the workers must be in-process** (`cls=Worker`, no nanny).
A nanny restarts a worker under memory pressure, which would take that worker's
live `bsub` clients with it — and because a farm job is bound to the very
thread that spawned it rather than merely to the process, that means that many
running jobs.

## Two decisions, not one

This is the distinction worth keeping separate, because almost every confusion
about the diagrams is these two collapsing into each other.

| Who decides | When | What it determines |
| --- | --- | --- |
| **placement** — your `@operation`, and the Plan it produces | planning time | which transport runs the work: a `bsub` job, or a python call in this process |
| **worker** — Dask's scheduler | run time | which worker's thread executes the task |

**Placement is never in doubt.** An operation declaring `lsf()` becomes an LSF
job; one that declares nothing gets `local`, explicitly, at authoring time. No
local task is ever sent to the farm, in any scenario, ever.

So a local task running on the worker *named* `farm` has not been misrouted.
Both workers are threads in one process on the submit host — the worker named
`farm` is not on the farm. It is the worker whose threads are the farm's
in-flight budget, one thread held per waiting `bsub -I`. A local task there ran
in-process, exactly where its placement said, and ate one of the slots meant to
hold a farm job.

### The lockout, which is the defect this shape exists to prevent

The cost is capacity, and that is not a nuance. The farm worker's threads *are*
the farm's in-flight budget and nothing else should be spending them. Every
thread taken by local work is one fewer `bsub -I` that can be in flight, so a
farm job waits behind a python function **while the permit it needs sits
unused.** The symptom always looks the same: a permit meter with room left, and
no thread to spend it with.

Annotating only the farm tasks does not prevent this. A task that requests no
resource is not weakly preferred anywhere — it is legal on *every* worker, so
the scheduler may place it on the farm worker, and work stealing will keep
moving it there for the whole run, because restrictions are strictly enforced
during a steal and a task with none has nothing to enforce. Annotating **every**
task, `local` included, is what makes the lockout unrepresentable rather than
unlikely — and it is also what makes stealing safe to leave switched on.

### One deliberate exception

If the run has no transport for a placement at all, that task is left
*unannotated* on purpose, so transport selection refuses just that invocation
exactly as the sequential kernel does. Annotating it would strand it forever
against a capacity nobody declares, and would stop unrelated branches from
completing — which would make the two kernels disagree about a plan, the one
thing the readiness kernel may not do.

## Why a thread is expensive

A **placement** is what the plan says: run this one on `lsf`. A **transport** is
what the site provides: an object that knows how to turn a command into a real
`bsub`. Binding one to the other is the run's job. A transport decides nothing
else — not what is ready, not what an attempt is called, not whether work can be
reused. Inside a running task, all that happens is: look up this invocation's
placement, find the transport bound to it, call it.

`bsub -I` submits *and waits*: the call returns when the job is over. So one
in-flight farm job holds one worker thread from submission until completion,
queue wait included. That single fact is why everything above matters:

- **"threads"** really means "jobs allowed in flight". Nothing to do with CPUs.
- **a busy worker** may be running nothing at all — the jobs could all still be
  queued on the farm.
- **the record** has a gap between submit and finish, because the thread that
  could ask LSF is the thread that is waiting. Only something outside, polling
  `bjobs`, can tell pending from running. That is what `hedloom_exec.watch` is,
  and what `submit(watch=True)` now runs.

## A transport is copied, never shared

Dask serializes every task — even on an in-process cluster where the worker is a
thread in the same program. So each task gets its own copy of the transport.
Ours hold nothing between submissions, so a copy is correct.

But it constrains what a transport may be. One holding a live client, a socket
or a lock cannot travel this way, which is why the run refuses it up front and
names the placement rather than letting it fail deep inside Dask's protocol
(`_require_shippable`). And a pooled transport — one that must be a single
shared object holding a connection — could not be passed at all. It would have
to be built on the worker instead, which is the unbuilt half of
[`pooled-placement-plan.md`](pooled-placement-plan.md).

One consequence has already bitten. The façade wraps every site transport in a
wrapper that calls your authored function first, so what travels is named
`bound:lsf-interactive`, not `lsf-interactive`. A watcher exact-matching the
transport name therefore saw nothing through the façade — an empty sweep being
indistinguishable from a finished one. The journal now records `transport` (who
submitted) and `substrate` (where the job lives) as two facts, and the watcher
matches the second. **Placement admission avoids this whole bug class** by using
the Plan's placement name, never the transport's.

## Where the scheduler's numbers come from

Rule R4 says the scheduler picks the least busy worker. It is worth knowing what
that number is made of, because it is more fragile than it sounds.

Dask groups tasks by **prefix** — everything in the task's name before the first
`-` — and keeps a rolling average of how long tasks with that prefix take
(`scheduler.py:996`). A prefix it has never seen falls back to
`unknown-task-duration`, which is **500 ms** (`distributed.yaml:33`).

That is why task keys are `operation-authoredkey-digest` and not
`authoredkey-digest`. Keyed by corner, every task was its own prefix, no average
was ever learned, and every estimate in every placement decision was a flat
500 ms — for work that takes minutes. Keyed by operation, the average becomes
real after the first few corners finish.

## The scenarios, and what each is for

The interactive board steps through ten setups. What they are for:

| # | Setup | Why it is there |
| --- | --- | --- |
| 1 | One worker, no resources | The starting point: threads are the only gate. |
| 2 | A cap, but only one worker | A permit with nowhere else to spend it changes nothing. |
| 3 | Two workers, only farm tasks annotated | **The lockout.** The recorded design before it was corrected. |
| 4 | Every servable task annotated | **Shipped behaviour.** |
| 5 | Threads below the cap | Two independent gates; the smaller binds, silently. |
| 6 | A permit nobody issues | Permanent `no-worker`. No exception, no log line, an idle-looking cluster. |
| 7 | A per-queue cap and a global cap together | Two resource names on one task — an unbuilt extension. |
| 8 | What a farm task is actually doing | The thread is held through the queue wait. |
| 9 | The same lockout arriving by work stealing | Placement is not decided once. |
| 10 | Annotated and steal-proof | **Shipped behaviour**, under stealing. |

Scenarios 1, 2, 3 and 9 are failure modes the current design excludes. They are
kept because they are the argument for why every servable task carries a
placement resource.
