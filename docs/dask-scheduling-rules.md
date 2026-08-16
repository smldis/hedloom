# Dask scheduling and resources: the rules, schematically

Reference, not a decision document. Everything below is read from the pinned
`distributed==2026.7.1` in this environment; file:line citations are that
source, so an upgrade can be re-checked against them.

Written because the two-worker concurrency decision turns entirely on these
rules, and they were being reasoned about from memory.

---

## 1. The four things that exist

| Thing | Has | Set by |
| --- | --- | --- |
| **Scheduler** | the whole task graph; decides *which worker* and *when to hand it over* | one per cluster |
| **Worker** | `nthreads` (task slots), `resources` (name -> capacity, floats) | cluster construction |
| **Task** | dependencies, optional `resources` (name -> amount), optional `workers=`/`host=` restrictions, priority | `client.submit(...)` / `dask.annotate(...)` |
| **Resource** | just a **name and a number**. Nothing measures or enforces the underlying thing | declared on workers, requested by tasks |

A resource is pure bookkeeping. Declaring `{"lsf": 200}` does not mean the
worker has anything; it means "at most 200 units of tasks labelled `lsf` may be
executing here at once".

---

## 2. The pipeline a task goes through

```
  submitted
     |
     v
  [ waiting ] ------ all dependencies in memory? ---- no --> stay
     |  yes
     v
  Scheduler: valid_workers(ts)          <-- RULE 1, 2, 3
     |
     +-- restrictions? --no--> [ queued ]  (root-ish tasks held back)  <-- RULE 6
     |                            |
     +-- yes ------------------+  |
                               v  v
                     decide_worker(): pick one of the valid  <-- RULE 4
                               |
              no valid worker  |  a worker
                     v         v
              [ no-worker ]  [ processing ] --> handed to that worker
              (waits forever)          |
                                       v
                     Worker: ready heap | constrained heap   <-- RULE 5
                                       |
                          nthreads free AND resources free
                                       |
                                       v
                                  [ executing ] --> memory / erred
```

---

## 3. The rules

**R1 — Dependencies gate eligibility.**
A task is not considered at all until every dependency is in memory somewhere.
This is the only thing graph edges do.

**R2 — Restrictions decide *which* workers are valid.**
`Scheduler.valid_workers` (`scheduler.py:3199`) intersects worker restrictions,
host restrictions, and resource restrictions. For resources, a worker is valid
when its **declared total** for every requested name is `>= required`
(`scheduler.py:3243` — declared, not currently-available; runtime availability
is the worker's business, R5).

**R3 — No restriction means every worker is valid.**
`valid_workers` returns `None` when the task has no restrictions, documented as
*"If all workers are valid then this returns None, in which case any running
worker can be used"* (`scheduler.py:3202`). **This is the whole of the §2 gap in
the previous document.** An unannotated task is not "preferentially local"; it
is unrestricted, and the farm worker is a legal home for it.

**R4 — Among valid workers, the choice is a heuristic.**
`decide_worker` prefers a worker that already holds the task's dependencies,
otherwise the least *occupied*. Occupancy is an estimate built from measured
average durations per task-key prefix. It is a preference, never a guarantee —
and for us the estimates are poisoned, because a `bsub -I` task's measured
duration includes its queue wait.

**R5 — The worker has two independent gates, and both must open.**

| Gate | Capacity | Held for |
| --- | --- | --- |
| threads | `nthreads` | the whole task execution |
| resources | `available_resources[name]` | acquired at `constrained -> executing`, released at completion |

Worker-side, tasks split into two heaps: `ready` (no resources) and
`constrained` (has resources), and `_next_ready_task` pops from whichever has
the better priority (`worker_state_machine.py:1735-1754`). They compete for the
**same** `nthreads` slots.

> Effective concurrency for one resource = `min(nthreads, floor(capacity / per-task amount))`.
> Whichever is smaller binds, and nothing says which one did.

**R6 — Resource-annotated tasks bypass root-task queuing.**
`is_rootish` returns False as soon as a task has any restriction
(`scheduler.py:3111`), and `validate_queued` asserts a queued task has none
(`scheduler.py:3636`). So annotated tasks are assigned to a worker as soon as
they are eligible, rather than held in the scheduler's `queued` state. For us
this is fine — the worker-side resource gate is what limits them.

**R7 — A requested resource that no worker declares is a permanent hang.**
The task goes to `no-worker` and stays there. Not slow: never scheduled, no
error, no log line at the client. The cluster looks idle.

**R8 — Resources are per-worker and additive across workers.**
Two workers each declaring `{"lsf": 200}` permit 400 concurrent. A cap is only
a global cap if exactly one worker declares it.

**R9 — `secede()` opens the thread gate only.**
It removes the calling thread from the pool so a new task can start
(`worker.py:2656`). It does not release resources, and the register already
rejected it here for a different reason (a worker holding live `bsub -I`
clients should read as busy, not idle).

---

## 4. Where each number is written

| Number | Declared | Requested |
| --- | --- | --- |
| threads | `Worker(nthreads=N)` / `LocalCluster(threads_per_worker=N)` | — |
| resource capacity | `Worker(resources={"lsf": 200})`, `dask worker --resources "lsf=200"`, or per-worker in a `SpecCluster` spec | — |
| resource amount | — | `client.submit(f, ..., resources={"lsf": 1})` or `with dask.annotate(resources=...)` |

`LocalCluster(resources=...)` applies the **same** dict to every worker. That is
the reason the two-worker shape needs `SpecCluster` (§5).

---

## 5. What `SpecCluster` is

A cluster defined by an explicit **specification** instead of a count: a dict for
the scheduler and a dict of worker-name -> `{"cls": ..., "options": {...}}`.
Each worker entry is independent, so workers can differ in `nthreads`,
`resources`, `memory_limit`, or class.

It is not an alternative family — `LocalCluster` **is** a `SpecCluster`
subclass (`deploy/local.py:23`) that generates a homogeneous spec for you.
Using `SpecCluster` directly means writing that spec yourself.

```python
from distributed import Client, SpecCluster, Scheduler, Worker

cluster = SpecCluster(
    scheduler={"cls": Scheduler, "options": {"dashboard_address": ":8787"}},
    workers={
        "local": {"cls": Worker, "options": {
            "nthreads": 4,
            "resources": {"placement:local": 4},
        }},
        "farm": {"cls": Worker, "options": {
            "nthreads": 200,
            "resources": {"placement:lsf": 200},
        }},
    },
)
client = Client(cluster)
```

Three things worth knowing for our case:

* **`cls=Worker` is in-process, no nanny** — which is exactly what the register
  requires (*"the farm worker must be in-process (`cls=Worker`, no nanny)"*).
  `SpecCluster` states it directly; `LocalCluster(processes=False)` arranges it
  indirectly.
* **Workers start inside `__init__`** in synchronous mode:
  `self.sync(self._start)` then `self.sync(self._correct_state)`
  (`deploy/spec.py:290-292`). **This resolves the caveat I raised yesterday** —
  the `_without_http_servers` seam in `hedloom_run.cluster`, which is scoped to
  cluster construction, does cover `SpecCluster`'s workers. `dashboard = "none"`
  survives the move. **Verified 2026-08-16:** a behavioural test builds a real
  silent two-worker cluster and runs a resource-annotated task through it.
* **Worker names become part of the address**, so `"local"` / `"farm"` show up
  in the dashboard and in `client.scheduler_info()`, which is free legibility.

---

## 6. How hedloom's vocabulary maps onto the rules

Purely a mapping of the implementation shipped 2026-08-16.

| hedloom | Dask |
| --- | --- |
| placement (`lsf(queue=...)`, `named_policy(...)`) | `resources={"placement:<name>": 1}` on each task the run can serve |
| `[kernel] threads` | local concurrency; the default cap for an in-process placement, and for implicit `local` |
| `max_jobs` | `Site.placements[name]`, used for both the worker's `nthreads` and `resources={"placement:<name>": cap}` |
| one in-flight `bsub -I` | one task holding one thread for its whole life |
| the sequential driver | no scheduler at all; concurrency 1 |

**On "a default for unannotated tasks":** at the *Plan* level there is already
one, and it is not a gap — `select_transport` resolves
`(item.policy or {}).get("name") or "local"` (`run/binding.py:123`), so **every
invocation has a resolved placement before anything runs**. There is no such
thing as an unannotated invocation.

The graph kernel now translates that resolved placement into `resources=` for
every task whose placement has a transport. R3 therefore does not apply to any
task the run can actually execute.

There is one deliberate exception. If the run has **no transport** for a
placement, `_admission` returns no resource annotation and `select_transport`
refuses that invocation on the worker. This is not a fallback: it preserves the
sequential kernel's per-invocation refusal and lets unrelated branches run.
Annotating the task would instead leave it permanently unrunnable under R7 and
make the two kernels disagree about the plan.

The shipped path, stated in terms of these rules:

```
served placement:    submit(..., resources={p: 1}) -> the worker declaring p (R2)
unserved placement:  submit(..., resources={})     -> run and refuse that invocation (R3)
```

---

## 7. Consequences worth carrying into the design discussion

Stated as facts, not recommendations:

1. **Addressed.** Isolation between local and farm work requires both kinds of
   servable task to be annotated (R3); the graph kernel now does so.
2. **Addressed.** `nthreads` and resource capacity remain independent gates,
   and `Site.cluster_spec()` derives both from the same placement cap so threads
   cannot bind below it (R5).
3. **Addressed for servable placements.** A resource-name mismatch is still a
   permanent Dask hang (R7), so `_require_admission` refuses it before
   submission. Unserved placements follow the deliberate exception above.
4. **Still true, not built as profile vocabulary.** Per-queue and global user
   caps can compose by requesting both names on one task (R2 + R5), but the
   shipped profile declares one cap per placement, not a second global cap.
5. **Addressed.** `cluster_for(site)` now uses `SpecCluster`; `LocalCluster`
   still cannot express heterogeneous workers (§4).

---

**Your call:** ☐ keep this in `docs/` as the reference these decisions cite
☐ fold the rules into the register instead ☐ something here is wrong or unclear:
