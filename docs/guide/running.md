# Running a study

A study is authored once and can be run many times. Nothing on this page
changes what a run *means* — that is settled by
[what you authored](authoring.md) and by
[what gets reused](results.md#reuse-and-what-invalidates-it). Everything here is
about how a run executes.

## `Study.submit(...)` — the one-run form

```python
run = subject.submit(site=site, watch=True)
```

| Argument | Default | What it does |
| --- | --- | --- |
| `site` | *required* | Where work runs, where records go, what addresses mean |
| `watch` | `False` | Print each invocation as it settles, **and** poll the farm queue |
| `stop_on_failure` | `True` | On the first failure, stop admitting new work |
| `override` | `None` | Change how this run executes, never what it means |
| `sequential` | `False` | One invocation at a time, no cluster, no `distributed` |
| `locally` | `False` | Serve every placement in this process — the debugging pair |
| `on_event` | `None` | Your own per-invocation callback instead of the printed line |
| `client` | `None` | Escape hatch for a caller who already holds a `distributed.Client` |

**Concurrency is the site's.** `submit` opens the compute the site declares, for
as long as the run needs it, spends up to each placement's `max_jobs`, and gives
it back. There is no kernel to choose and never was a second mode — a site that
declares nothing has capacity one, which *is* one invocation at a time.

## Several runs: `session(...)`

`Study.submit` is the one-run form of a session. When you have more than one
run, open the session yourself so they share one cluster, one budget and one
watcher:

```python
with session(site, watch=True) as farm:
    first  = farm.submit(subject)
    second = farm.submit(subject)      # reuse, same cluster, same watcher
```

(`examples/farm_smoke.py`)

What is deliberately **not** hidden is the lifetime. Leaving the block ends the
runs inside it, and under owner-bound lifetime that takes their farm jobs with
them. That is a real fact about running work here, so it keeps a real shape.

`submit_all` runs several studies against that one cluster — which is what makes
the shared budget structural rather than a convention, since two studies cannot
between them put more on the farm than the site declared:

```python
with session(site) as farm:
    runs = farm.submit_all({"north": north_study, "south": south_study})
```

`examples/farm_multi_client.py` measures exactly this, and also the arrangement
where the cap does **not** hold: two *separate* sessions each have their own
cluster and therefore their own budget, so two controllers can put twice
`max_jobs` on the farm.

## Running less, or running elsewhere: `override`

An override speaks the profile's own vocabulary and applies to this session
only, so a site needs one declaration rather than one per way of running it:

```python
with session(site, {"placement": {"lsf": {"max_jobs": 1, "queue": "express"}}}) as farm:
    ...
```

**An override changes how a run executes and never what it means.** Nothing it
can reach is identity-bearing, so an overridden run lands on the same attempt
identities as a plain one and the two reuse each other's work. It may carry
`placement` and `kernel`; roots are refused, because moving the record changes
what is reused — that is a different installation, not a different way of
running this one.

## Debugging: `sequential` and `locally`

```python
subject.submit(site=site, sequential=True)   # one at a time, no scheduler
subject.submit(site=site, locally=True)      # ...and every placement served here
```

Say `sequential=True` rather than leaving it to be inferred from a missing
argument: a site declaring `max_jobs = 8` and quietly running one at a time is
indistinguishable from a busy farm. It is also what keeps `distributed`
optional — a plan small enough to walk in one thread should not need a
scheduler, and if the extra is missing you get a `SiteError` naming both ways
out rather than a silent downgrade.

`locally=True` is `sequential=True` plus every placement served by its authored
body in this process — for debugging a farm study on the submit host. The
placement names, budgets and Plan are untouched, so identity is untouched, which
is the point **and** the catch: a local run publishes attempts a later farm run
will reuse. Sound as far as your declared inputs go; a result that genuinely
depends on the machine needs that fact in `identity_env`.

## Watching a run

`watch=True` does two different things, because there are two questions:

```
[      ran] coarse:integrate                  succeeded
[watch] invoke:coarse pending → running (48s queued)
```

The first line is an invocation settling. The second is a **queue transition**,
polled from the attempt records by `hedloom_exec.watch` — the only thing here
that can tell `PEND` from `RUN`. It matters because `bsub -I` blocks from
submission to completion, so without it a farm sweep prints nothing at all for
the whole queue wait and then a burst.

`on_event=callback` replaces the first of those, for a caller that wants its own
progress reporting. It does **not** replace the second: a queue transition is
not an invocation settling. The watcher can never fail a run — an LSF too old
for `bjobs -o` prints once, disables the poller, and leaves the run alone.

## When something fails

`stop_on_failure=True` (the default) means **stop admitting new work**: cancel
what has not started, let what is already executing finish, and return a
partial report naming what was skipped. The reasoning is that the usual answer
to a failed invocation is to debug it rather than to spend the farm on the other
forty-nine — and resubmitting afterwards is cheap, because content-addressed
reuse means the invocations that completed are reused and only the failure re-runs.

`stop_on_failure=False` lets independent branches finish, which is what a sweep
wants when the failure is known and local. Dependents of a failure are blocked
either way; they are never run against inputs that do not exist.

The *scope* of a failure differs between the two kernels and its *meaning* does
not: the sequential kernel blocks everything after a failure, while the graph
kernel lets independent branches continue. Both record the same thing about the
invocation that failed. What `_stop_admitting` does with the rest of a sweep,
and what a model checker found in it, is in
[stopping a sweep, model-checked](../internals/stop-admitting-protocol.md).

Whatever the run raises at you, [the refusals table](refusals.md) says what it
means.
