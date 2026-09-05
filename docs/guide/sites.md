# Sites and placements

Nothing about *where* a study runs is authored into the study. A `Site` holds
all of it: which substrate provides each named placement, the roots records and
workspaces are written under, the address spaces a declared source resolves
through, and how much local concurrency the submit host should offer.

```python
site = Site(
    root=str(work / "attempts"),
    workspace_root=str(work / "work"),
    address_spaces={"repository-relative": str(here)},
)
```

(`examples/grid_refinement.py` — constructed directly, because it needs no placement
besides the default in-process one.)

## Profiles

A profile can be read from TOML with `Site.from_file(path)`. **Relative paths
resolve against the profile's own directory**, not the working directory, so a
study run from elsewhere still means the same thing:

```toml
[study]
root = "_runs/farm-smoke/attempts"
workspace_root = "_runs/farm-smoke/work"

[placement.lsf]
kind = "lsf-interactive"
queue = "reg"
walltime = "1"
cores = 1
max_jobs = 4

[placement.pool]          # only if some operation asks for pooled()
kind = "lsf-pooled"
queue = "short"
cores = 1
memory_mb = 4000
walltime = "2:00"
workers = 20              # LSF jobs the pool holds open
max_jobs = 20             # invocations in flight against it

[kernel]
threads = 2
dashboard = "none"        # "none" | "loopback" | "network"

[retention]
floor = "7d"

[[retention.rule]]
name = "spent failures"
outcome = ["failed", "cancelled"]
older_than = "14d"
keep_latest = 1
keep_logs = true

[retention.automatic]
after_run = ["spent failures"]
```

(`examples/farm-smoke.site.toml` declares the direct placement;
`examples/farm-smoke-pooled.site.toml` declares one of each.)

A `[placement.*]` naming an unknown `kind` is refused outright rather than
silently dropped, because a study that quietly lost a placement fails much later
as an opaque `UnsupportedPlacement` that blames the Plan for what is a
configuration mistake. The same applies one level down: a misspelled option is
named by placement *and* key rather than raising a bare `TypeError`.

## Retention belongs to the installation

Retention says what one storage site can afford to keep, not what a study
means. Conditions within one rule are ANDed; named rules are ORed. The global
floor, standing reusable result, active pins, non-terminal tries, and
`unreconciled` evidence remain protected regardless of a rule. Every one of
those is a property of the evidence itself; none of them asks which study
requested the work.

`hedloom prune --site site.toml` prints the survey and changes nothing.
`--apply` is the separate destructive gesture, and every candidate is checked
again under its record claim before a durable removal event precedes deletion.
The optional `automatic.after_run` list may name only declared rules. Those
rules run after a completed run; failure warns and cannot change the run's
outcome. There is deliberately no `submit(prune=...)`: a study decides what it
produces, never what the installation keeps.

## Placement kinds

Three kinds exist, and the choice between the two farm ones is a trade rather
than an upgrade.

### `kind = "lsf-interactive"` — one job per invocation

`lsf(...)` on an operation, one `bsub -I` job per invocation, with that
invocation's own queue, cores, memory and licences. The job is visible,
cancellable and accountable as *that invocation*, and it is **owner-bound**: LSF
binds its lifetime to the `bsub` client, which is our child, which dies with
this process.

The vocabulary is `app`, `cores`, `licences`, `memory_mb`, `queue`,
`resources`, and `walltime`, plus `timeout` and `max_jobs`. Options authored on
an invocation override the site's values for that invocation; an option the
transport cannot express is refused before submission rather than dropped.

### `kind = "lsf-pooled"` — a shared set of workers

`pooled()` on an operation sends its command to a *shared* set of LSF workers
the study holds open, so the queue is paid once per worker instead of once per
invocation. That matters when an operation has many short invocations and the wait
to start is a large fraction of the time to run.

What it costs is everything that needs an invocation to *be* a job:
per-invocation resource requests, per-invocation `bkill`, per-invocation
accounting, per-invocation licence arbitration, and the watcher's ability to
tell you that one particular invocation is queued. The farm sees the pool's
workers, never your invocations.

As a starting rule, pool an operation when its median queue wait is above
roughly a third of its median runtime and its invocations are uniform enough to
share one worker shape — otherwise `lsf(...)` is the better deal.

A pooled placement takes a **narrower vocabulary** than a direct one — no
`licences`, no raw `resources` — and says so rather than accepting them and
quietly ignoring them: those describe what *one invocation* needs, and a pool's
workers are claimed before any invocation is routed to them.

```{warning}
**A pool's workers are not owner-bound.** They are ordinary batch jobs, and
unlike `bsub -I` they do not die with the client that submitted them. Two things
stop them: `LSFCluster.close()` when the session ends, and the pool's
`walltime` if the submit host dies without warning. That walltime is the only
bound that survives a hard kill, so declare it, and keep it no longer than the
work needs.
```

`workers` and `max_jobs` are different facts — how many LSF jobs the pool holds
open, and how many invocations may be in flight against it. Usually you want
them equal; when they differ, the smaller one binds and the other quietly means
nothing.

### `kind = "in-process"` — the default, and the debugging one

An in-process placement needs Python callables that no TOML can hold. Declare it
in the profile anyway — `submit` supplies the `BoundTransport` from your
authored bodies, and `Site.with_transports(...)` is how a caller adds them by
hand.

## The two numbers, which are about two different machines

| | Means | Sized from |
| --- | --- | --- |
| `[placement.*] max_jobs` | how many of **that placement's** invocations may be in flight | the share of the farm this study may spend |
| `[kernel] threads` | local concurrency on the **submit host** | how much in-process work this host should do at once |

`max_jobs` is **required** for both LSF placement kinds, and it is
deliberately **not** your site's MAX JOB policy. That policy counts every job
running under your user from every source, so declaring all of it here means
your own submissions and hedloom's queue behind each other — and when it is
hedloom that waits, its worker threads are held by `bsub -I` clients that have
not started, so the placement spends its budget on queueing. Leave headroom.

There is no safe default to guess, which is why an uncapped LSF placement is
refused rather than filled in: an arbitrary small number silently throttles a
sweep, and an arbitrary large one authorises more concurrent jobs than the site
permits and more live `bsub` clients than the submit host will carry.

Getting it wrong is cheap, though. LSF is the real authority — declaring more
than it permits just means the excess pends. **The cap is a courtesy rail, not a
correctness requirement**, so ship it conservative and tune it from measured
queue latency. [The first-farm-run ladder](first-farm-run.md#the-ladder) is how
to measure it.

Why each placement's budget becomes a worker of its own, rather than a number
checked somewhere, is [in the internals](../internals/placement-and-scheduling.md).

## `dashboard` — what the cluster exposes

A Dask scheduler starts an HTTP server whether or not anyone opens a browser,
and every worker starts one too, both on all interfaces. On a shared submit host
that publishes your invocation names, workspace paths and profiler to everyone who
can reach it.

* `"none"` — the default; no listening socket at all. Refused for a
  multi-process cluster, whose workers must dial a listener.
* `"loopback"` — off the network; still reachable by other users of the same
  host, because loopback is per host and not per user.
* `"network"` — explicit opt-in to Dask's own network-visible behaviour.

Exposure changes how a run can be watched and **nothing** about what it
computes.

Two schedulers can exist once a pool is declared: the in-process readiness
cluster, and one per pool whose workers dial in from the farm. `"none"` skips
installing the dashboard routes on both — which is also the setting to reach for
when an installation's bokeh is missing or mismatched, since that failure
surfaces inside `distributed.dashboard.scheduler` as an `AttributeError` naming
neither bokeh nor the dashboard. It does not make either scheduler silent: a
pool's workers are on farm nodes and must be able to reach their scheduler, so
its comm address stays network-reachable regardless.
