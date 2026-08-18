# Placement, clustering, and scheduling

A description of the current implementation, for someone changing it. The
operator-facing version of the same material is
[Sites and placements](../guide/sites.md).

## The three concepts

```text
placement   where an operation runs and what resources it requests
clustering  the controller workers available to launch ready operations
scheduling  which ready operation starts next, and when
```

They form this chain:

```text
                              Plan
                operations + dependencies + policy
                                |
                                v
                       readiness kernel
                   +------------+------------+
                   |                         |
             sequential loop            graph kernel
             one at a time            Dask over a cluster
                   |                         |
                   +------------+------------+
                                |
                                v
                      placement selection
                 placement name -> Site transport
              +--------------+--------------+
              |              |              |
           in-process   lsf-interactive  lsf-pooled
              |              |              |
              v              v              v
        local execution   bsub -I     dask-jobqueue worker
                               |              |
                               +------+-------+
                                      v
                                LSF scheduler
                                      |
                                      v
                                 compute node
```

Both kernels reach placement selection through the same
`hedloom_run.binding.select_transport`, which is the mechanism behind the
invariant `hedloom-run` exists to hold: *changing which kernel decides readiness
changes how long a plan takes and nothing else.*

## Placement

Placement is authored into the Plan. It is data, not an act of submission:

```python
@operation(policy=lsf(queue="reg", cores=1, walltime="30"))
def integrate(...):
    ...
```

The Plan records the policy name (`lsf`) and its options. Policy precedence is
resolved at *planning* time and stored on the invocation:

```text
call override -> operation default -> plan default -> local
```

So there is no such thing as an unplaced invocation, which is what lets
`_placement_of` read one field and never guess.

The `Site` maps the resolved name to a concrete transport:

```toml
[placement.lsf]
kind = "lsf-interactive"
queue = "reg"
cores = 1
walltime = "30"
max_jobs = 4
```

Site values are transport defaults; supported options authored on an invocation
override them for that invocation. An option the transport cannot express is
refused **before submission** rather than dropped, because dropping a stated
resource need would run the work under conditions nobody asked for.

### Named placement routes

Different operations can name different transports:

```python
regular = named_policy("regular")
large_memory = named_policy("large_memory")

@operation(policy=regular(cores=1))
def prepare(...):
    ...

@operation(policy=large_memory(cores=8))
def solve(...):
    ...
```

```toml
[placement.regular]
kind = "lsf-interactive"
queue = "reg"
walltime = "30"
max_jobs = 8

[placement.large_memory]
kind = "lsf-interactive"
queue = "bigmem"
walltime = "120"
max_jobs = 2
```

`lsf(...)` is convenience syntax for the policy name `lsf`; `named_policy(...)`
provides additional route names. A placement no transport provides raises
`UnsupportedPlacement` rather than falling back silently: running work somewhere
other than where it was asked to run is how a study quietly stops meaning what
it says.

## Clustering

The Dask cluster here is a **controller** cluster. It is not the LSF compute
farm. A task on it prepares one invocation, selects its transport, and may then
block in `bsub -I` while LSF chooses and manages the compute node the payload
actually runs on.

```text
worker thread for placement `lsf`
    -> prepare invocation
    -> select placement transport
    -> build command
    -> call bsub -I
    -> wait for and record the result
```

### One worker per placement

`Site.cluster_spec()` produces one worker per declared placement, and
`hedloom_run.cluster.cluster_for(site)` builds the cluster from it:

```python
from hedloom_run.cluster import cluster_for

cluster = cluster_for(site)
```

Each worker gets `nthreads` **and** `resources={"placement:<name>": cap}` from
that placement's own `max_jobs`. The thread count is *derived* rather than
configured beside the cap, because the two are independent gates on the same
worker and the smaller one binds silently: a worker declaring capacity for two
hundred farm jobs while holding eight threads runs eight, reports nothing, and
looks correct.

A hand-built `LocalCluster` is not a lighter alternative. It applies one recipe
to every worker, so it can express neither two workers that differ nor the
per-placement capacities every task requests — and a task asking for a capacity
no worker declares is not slow, it is *never scheduled*. `run_plan_graph` refuses
such a cluster up front rather than hanging against it.

`SpecCluster` with `cls=Worker` means in-process and **no nanny**, deliberately:
a nanny restarting a worker under memory pressure would take that worker's live
`bsub` clients with it, and under owner-bound lifetime that many running farm
jobs.

Most callers never see any of this. `submit` and `session` open the compute the
site declares and give it back; `cluster_for` is for a caller who wants to hold
one across several sessions, and `client=` is the escape hatch for one who
already has a `distributed.Client`.

## Scheduling

There are two scheduling decisions, and they belong to different systems.

1. The kernel — or Dask on its behalf — determines dependency **readiness**.
2. After an LSF invocation is submitted, **LSF** determines when and where the
   job runs, and arbitrates queues, cores, memory, licences and user limits.

Nothing in hedloom counts licence tokens or waits for one. A declared
`licences={"name": n}` becomes a `rusage` term on that job, because the
scheduler owns the count.

### Sequential kernel

```python
subject.submit(site=site, sequential=True)
```

Walks the Plan one invocation at a time, waits for each `bsub -I`, records its
result, and only then considers the next invocation. It needs no scheduler,
which is what keeps `distributed` an optional extra, and it remains the
reference implementation. With the default failure behaviour, everything after
the first failure is reported as `blocked`.

This is the kernel that has [reached a real farm](../guide/first-farm-run.md#what-has-never-met-a-real-farm).

### Graph kernel

```python
subject.submit(site=site)          # a site declaring capacity opens a cluster
```

One Dask task per invocation, with edges where one invocation's output feeds
another's input. Dask decides what is ready and how much runs at once; it does
not decide where anything runs, what an attempt's identity is, or whether work
may be reused.

Three details are load-bearing:

* **Every task is annotated** with `resources={"placement:<name>": 1}`,
  including `local`. Leaving local work unannotated is what lets it be scheduled
  onto — and later *stolen* onto — the worker whose threads are the farm's
  budget.
* **One deliberate exception.** An invocation whose placement this run cannot
  serve is left unannotated, so it is refused per invocation exactly as the
  sequential kernel refuses it. Annotating it would hold it unrunnable forever
  instead, and the two kernels would then disagree about a plan. This is the one
  place they are kept in step by an omission rather than by a symmetry.
* **`_require_admission`** refuses a cluster that declares no capacity for a
  placement the plan uses, before submitting anything.

Task keys are `operation-authoredkey-digest`. The operation-first ordering is
not cosmetic: Dask groups by everything before the first `-`, so keying by
point alone gave every task its own group, taught the scheduler nothing, and
left every placement decision running on a flat 500 ms default. Tasks are
submitted `pure=False`, because reuse is `hedloom_exec`'s decision against
declared inputs, never Dask's against call signatures.

A failed invocation blocks its dependents by returning a blocked outcome, not by
raising, so independent branches continue — one point failing does not abandon
the other forty-nine. That is a deliberate difference in the *scope* of a
failure between the two kernels, not in what a result means.

### Pooled placement

A pooled placement adds a second scheduler: `dask_jobqueue.LSFCluster` holds
`workers` batch jobs open, and invocations routed to that placement are executed
against them rather than each becoming a job. The transport that reaches a
readiness worker is data only — it holds no client, because Dask serializes
every task even on an in-process cluster, so **a transport always travels as a
copy**. The live client is built on the worker by
`hedloom_run.pooled.PooledClientPlugin`. That is a rule rather than a
convenience: a transport that must be a singleton cannot be passed by value and
needs a factory constructed on the worker instead.

Pooled workers are **not** owner-bound. See
[the warning in the guide](../guide/sites.md#kind--lsf-pooled--a-shared-set-of-workers).

## Why a thread, and why not `secede()`

An invocation waiting on `bsub -I` costs about 16 KiB of thread and one client
process. A placement's thread count is therefore not a statement about this
host's CPUs but about how many of its jobs may be in flight.

Nothing secedes. A worker holding live `bsub -I` clients should read as
*running*, and `secede()` would report it idle by excluding the task from the
parallelism count.

The measurements behind those choices are recorded under *Two concurrency
limits, not one* in `docs/vision/open-concepts.md` at the repository root. The
rules Dask itself imposes are in [the rules page](dask-scheduling-rules.md), and
the reasoning as prose is in [the concepts page](dask-scheduling-concepts.md).
