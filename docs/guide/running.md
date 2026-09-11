# Running a study

A study is authored once and can be run many times. Nothing on this page
changes what a run *means* — that is settled by
[what you authored](authoring.md) and by
[what gets reused](results.md#reuse-and-what-invalidates-it). Everything here is
about how a run executes.

## Running manually from a script

Use the Python environment where Hedloom and your study's dependencies are
installed. Keep operation and study definitions at module scope, and put
submission in a guarded `main()` so the same file can be loaded interactively.
For example, save this as `manual_study.py`:

```python
import argparse

from hedloom import Site, local, operation, parameter, returned, study


@operation(config={"n": parameter(int)}, outputs={"value": returned(kind="number")})
def square(*, n):
    return {"value": n * n}


@study(name="manual-square", default_policy=local())
def square_study(n):
    result = square.named("square")(n=n)
    return {"value": result.value}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=3)
    parser.add_argument("--site", default="site.toml")
    parser.add_argument("--name", default="manual-check")
    parser.add_argument("--plan-only", action="store_true")
    args = parser.parse_args()

    subject = square_study(args.n)
    print(subject.summary())
    if args.plan_only:
        return 0

    run = subject.submit(
        site=Site.from_file(args.site),
        name=args.name,
        sequential=True,
        watch=True,
    )
    print("Run:", run.run_id)
    print("Succeeded:", run.succeeded)
    output = run.outputs["value"]
    print("Value:", output.value if output.available else "unavailable")
    return 0 if run.succeeded else 1


if __name__ == "__main__":
    raise SystemExit(main())
```

Alongside it, create `site.toml`:

```toml
[study]
root = "records"
workspace_root = "workspaces"
history_root = "history"
```

These storage paths are relative to the Site file. History must be separate
from records and workspaces. From the directory containing both files:

```console
python manual_study.py --n 3 --plan-only
python manual_study.py --n 3
python manual_study.py --n 4 --name next-check
```

Planning executes no operation bodies. The example submits sequentially for
simple local debugging; omit `sequential=True` when the Site should manage
parallel execution. Keep the same storage roots across experiments to reuse
unchanged work. A new submission name creates a new history occurrence; it does
not force recomputation. Check exported verdicts separately when a study has
them: execution success alone does not mean its conclusion passes.

Use [run discovery](discovery.md) to inspect saved results without executing or
importing the study:

```console
hedloom runs list --site site.toml --name manual-check --json
```

Use the actual run ID returned by the script or listing with `hedloom runs show`
and `hedloom runs path`; see the discovery guide for their arguments.

## Working in IPython

The same file can serve as the definitions for an interactive working session.
Start IPython in the environment used for the script, from the directory
containing `manual_study.py` and `site.toml`:

```ipython
%run -n manual_study.py

site = Site.from_file("site.toml")
subject = square_study(3)
print(subject.summary())

run = subject.submit(
    site=site, name="exploration", sequential=True, watch=True,
)
run.succeeded
run.run_id
output = run.outputs["value"]
output.value if output.available else "unavailable"
```

`%run -n` loads the file under its module name, so the guarded `main()` does not
run. Top-level statements still execute: keep submission inside the guard.
Plain `%run manual_study.py --n 3` instead runs the script's command-line entry
point, including submission.

After editing the file, repeat `%run -n manual_study.py` and build a **new**
`subject` before submitting. Previously built studies retain their matching
operation bodies. The [source-reloading rules](authoring.md#editing-and-rerunning-a-script)
explain what edits affect reuse.

File-backed definitions give interactive authoring a stable source location.
You can define operations directly at the prompt, but Hedloom fingerprints
source through `inspect.getsource()`: if source cannot be recovered, reuse
requires explicit operation version discipline, such as changing
`@operation(version="2", ...)` when the implementation changes. Do not assume
all interactive definitions can be redefined like functions in a stable file;
operation registration also checks their source origin.

Neither source reloading nor retaining an IPython namespace snapshots mutable
globals, imported helpers, or external state. Pass changing values as declared
configuration or inputs. Keep planning handles inside the authored graph;
inspect ordinary values through `run.outputs` after submission.

For several submissions that should share live compute, use an explicit
[`session(...)` block](#several-runs-session). Retaining a `Site` variable shares
storage configuration, not a live cluster. Saved history remains available
after IPython exits through the same discovery commands used for scripts.

## `Study.submit(...)` — the one-run form

```python
run = subject.submit(name="investigate-start", site=site, watch=True)
```

| Argument | Default | What it does |
| --- | --- | --- |
| `site` | *required* | Where work runs, where records go, what addresses mean; includes a separate `history_root` |
| `name` | *required* | Chosen submission name; each request gets a durable occurrence such as `investigate-start.1` |
| `on_started` | `None` | Receive the durable run reference before execution |
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
    first  = farm.submit(subject, name="investigate-start")
    second = farm.submit(subject, name="investigate-start")      # reuse, same cluster, same watcher
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

## Nested studies in one Session

An operation can author and submit an inner study through the Session already
open. The runnable [nested-studies example](../../examples/nested_studies.py)
does this on local in-process workers. Its wrapper executes every submission;
the two inner operations reuse their records for unchanged text.

Both levels call the same `Session.submit(...)`. Reusing a `Site` alone shares
storage declarations, not live workers: a separate `Study.submit(site=...)`
opens another Session and its own compute budget.

Two details matter in the example:

- The wrapper retains a local placement slot while waiting. The Site declares
  two slots so the inner work has one available. A single slot causes the graph
  kernel to refuse with `NestedCapacityExhausted`; a separate wrapper placement
  can also provide headroom. Account for all concurrent wrappers when sizing it.
- An [imported state module](../../examples/nested_studies_state.py) holds the
  Session reference. A body defined in `__main__` is serialized by value, so a
  direct Session global would attempt to serialize its locks. The module travels
  by reference and reaches the existing Session on in-process workers. This
  wiring does not give a separate process or remote worker access to the Session.
  The caller clears the reference in `finally` before leaving the Session.

The outer Plan contains the wrapper; the inner Plan is authored and saved when
the wrapper submits it. Each Plan remains static, but inspecting the outer Plan
alone does not show the inner work. Prefer flow composition when the whole graph
can be authored together. [Fresh acquisition](runtime-artifacts.md) now works in
one Plan and does not require nesting.

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
subject.submit(name="investigate-start", site=site, sequential=True)   # one at a time, no scheduler
subject.submit(name="investigate-start", site=site, locally=True)      # ...and every placement served here
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
