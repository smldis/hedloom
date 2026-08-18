# Hedloom Run

Walk a validated Plan and run it.

```python
from hedloom_exec.transport import InProcessTransport
from hedloom_run import run_plan

report = run_plan(
    plan_document,
    transport,
    plan_id="refinement",
    root="attempts",
    workspace_root="/nfs/studies/refinement",
    commands={"solve": ["awk", "-f", "rule.awk", "point.in"]},
    outputs={"simulate": {"raw": {"path": "point.raw"}}},
)

print(report.summary())
```

The Plan declares meaning; the run binds mechanism. `commands` and `outputs`
say how an operation actually runs and which files count as its results;
operations named in neither run in-process.

A second run reuses everything. Edit one point and only that point and its
dependents rerun. A failure stops the run, and its successors are reported as
`blocked` rather than executed against inputs that do not exist — a failed step
is not cached, so fixing the cause and rerunning retries exactly it.

Run the evidence:

```console
PYTHONPATH=src:../exec/src python -m pytest -q
```

## Running a sweep on Dask

`run_plan` is one invocation at a time. For a real sweep, readiness belongs to
Dask (adopted 2026-08-04):

```python
from distributed import Client
from hedloom_run.cluster import cluster_for
from hedloom_run.graph import run_plan_graph

# Concurrency is each placement's own `max_jobs`, read from the profile along
# with the exposure. There is no limit parameter here to disagree with it: a
# waiting invocation costs ~16 KiB of thread and one client process, so size
# `max_jobs` from the share of the farm this study may spend — not from your
# site's MAX JOB policy, which counts every job you have running from any
# source. Build the cluster this way rather than by hand; the capacity a
# worker declares and the placement a task asks for have to be one reading of
# one profile, or Dask holds the task unrunnable and says nothing.
cluster = cluster_for(site)

with Client(cluster) as client:
    report = run_plan_graph(
        plan_document,
        client=client,
        transports={"local": local, "lsf-direct": lsf},
        plan_id="refinement",
        root="attempts",
        on_event=lambda outcome: print(outcome.authored_key, outcome.outcome),
    )
```

Same Plan, same identities, same report order — the kernel decides how long a
run takes, never what it means, and `hedloom_run.binding` holds the rules both use
so they cannot drift. Two differences are deliberate: a failed point blocks
its dependents while independent branches finish, and tasks are keyed by
authored name so a dashboard shows points rather than digests.

The cluster is local and threaded on purpose. No nanny to restart a worker
holding live `bsub -I` clients — under owner-bound lifetime that would kill
their farm jobs — and nothing secedes, so a worker with jobs in flight reads as
running. Note that Dask serializes every task even in-process, so a transport
is *copied* to its worker; one that cannot be serialized is refused by
placement name before anything runs.

`distributed` is an optional dependency (`pip install hedloom-run[dask]`), reached
by explicit import: a plan small enough to walk in one thread should not need a
scheduler. It takes a **floor**, `>=2023.9.2` — where `Client.register_plugin`
arrives — rather than a pin: a site does not always get to choose its
`distributed`, and a hard pin turns "a version behind" into "cannot install".
Verified against 2023.9.2, 2024.8.0 and 2026.7.1.

If your `distributed` has no matching **bokeh**, its dashboard cannot be built,
and Dask says so as `AttributeError: module 'distributed.dashboard' has no
attribute 'scheduler'` — naming neither bokeh nor the dashboard, from a cluster
you never asked to have one, since `"network"` is the default. Worse, the import
is lazy, so under concurrency one cluster can fail while its neighbour succeeds.
That is translated into a message that names bokeh and offers
`dashboard = "none"`. What Dask still cannot tell you is whether a point is `PEND` or
`RUN` — that needs a watcher over the attempt records, which is
`hedloom_exec.watch` and which `hedloom.Study.submit(watch=True)` now runs for
the duration of a run.

`dask-jobqueue` is a second, separate extra (`pip install hedloom-run[pooled]`),
for pooled LSF placement — where invocations reach a cluster whose *workers* are
themselves LSF jobs, rather than one job per invocation. It is deliberately not
folded into `[dask]`: a farm sweep placing one job per point needs the
scheduler and never needs a pool. It also belongs to this unit and no lower one,
because a pooled transport holds a live Dask client and `hedloom-exec` imports
neither Dask nor `hedloom_flow`. `LSFPooledTransport` there stays a refusing
boundary; the implementation is `hedloom_run.pooled`.

A pool is a *second* cluster, opened beside the readiness one by
`hedloom.session(...)`, and the two are not interchangeable. The readiness
cluster's scheduler and workers are objects in this process and talk over
`inproc`; a pool's workers are LSF jobs on farm nodes and must reach their
scheduler over the network, so that one is TCP and the exposure choices in
`hedloom_run.cluster` do not transfer to it. Teardown order follows from the
same fact: readiness workers hold clients into the pool, so they close first.

## What the cluster exposes

A Dask scheduler starts an HTTP server whether or not anyone opens a browser —
`dashboard=False` only drops the bokeh routes — and every worker starts one
too, both on all interfaces. On a shared submit host that publishes your point
names, workspace paths and profiler to everyone who can reach it. So the site
says how much of that it wants:

```toml
[kernel]
threads = 32
dashboard = "network"     # "network" | "loopback" | "none"
```

* `"network"` — the default, and exactly Dask's own behaviour: `cluster_for`
  passes no address, so declaring nothing changes nothing.
* `"loopback"` — scheduler *and* worker on `127.0.0.1:0`. Off the network;
  still reachable by other users of the same host, because loopback is per host
  and not per user.
* `"none"` — no listening socket at all. Only possible for the in-process
  cluster this kernel documents, since workers in their own processes must dial
  a listener; asking for it with `processes=True` is refused rather than
  quietly downgraded.

`"none"` costs the dashboard, `/health` and `/metrics`. It does not cost the
post-mortem: `distributed.performance_report(...)` is computed on the scheduler
and travels over the comm channel, which is `inproc://` here, so it still
writes its HTML with nothing bound. Live progress comes from `on_event`.

Exposure changes how a run can be watched and nothing about what it computes —
no identity, no reuse, no Plan content.

See [`ONTOLOME.md`](ONTOLOME.md) for the owned boundary.
