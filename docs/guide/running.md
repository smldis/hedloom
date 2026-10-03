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

from hedloom import Site, local, operation, parameter, returned, run_study, runtime, study


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

    run = run_study(subject, site=Site.from_file(args.site),
                    name=args.name, watch=True)
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
records_dir = "records"
work_dir = "work"
runs_dir = "runs"
```

These storage paths are relative to the Site file. History must be separate
from records and workspaces. From the directory containing both files:

```console
python manual_study.py --n 3 --plan-only
python manual_study.py --n 3
python manual_study.py --n 4 --name next-check
```

Planning executes no operation bodies. `run_study` waits for one terminal
`RunResult` and closes its owned Runtime before returning. Failed runs are
returned for inspection too; use `require_success=True` to raise `RunFailed`,
whose `.result` retains the failed result. Startup and argument errors raise
directly. An interrupted call requests ordinary withdrawal and drains entered
work before cleanup; it does not force-kill a command.

The helper accepts the same Site overrides, `locally`, `watch`, `priority`,
`reproducibility`, `environment`, and `stop_on_failure` choices as Runtime and
submission. Each call opens fresh execution resources, including configured
pools. Retain one Runtime for repeated or overlapping submissions to share
capacity and keep pools warm, as in the interactive example below.
`Runtime.submit` returns a Run receipt immediately; `wait()` explicitly observes
its terminal result. A Site with
local capacity one executes one invocation at a time through the same async
controller and Dask executor as larger Sites. Keep the same storage roots across experiments to reuse
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

```python
%run -n manual_study.py

site = Site.from_file("site.toml")
subject = square_study(3)
print(subject.summary())

live = runtime(site, watch=True)
receipt = live.submit(subject, name="exploration")
receipt.done()                    # nonblocking; work progresses at an idle prompt
receipt.snapshot()                # lifecycle and currently available evidence
run = receipt.wait()              # only when you want to wait
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
inspect ordinary values through `run.outputs` after completion.

## One owner, several Runs

A Runtime owns one controller thread and async loop, Dask executor, placement
capacity, offload lanes and optional queue watcher. It accepts submissions from
an ordinary script or REPL without depending on the caller's event loop.
Additional Runs can wait for capacity with their own receipt; there is no fixed
32-active-Run refusal limit. Retained Plans and receipts still use memory, so
release references you no longer need and avoid submitting an unbounded stream.

```python
from hedloom import runtime

with runtime(site, watch=True) as live:
    north = live.submit(north_study, name="north")
    south = live.submit(south_study, name="south", priority=10)
    # Both were submitted before either caller waits.
    north_result = north.wait()
    south_result = south.wait()
```

The Site holds storage and capacity configuration. Reusing a Site variable does
not share live execution: two Runtimes each have their own placement budget.
Compatible active work within one Runtime shares an execution handle; every
Run keeps its own name and durable history. Later submissions re-enter Exec,
which decides reuse from the recorded computation.

## Receipt, acceptance and completion

`submit(subject, name=...)` returns an in-memory `Run` receipt. During
`ACCEPTING`, the Runtime publishes the saved Plan and initial history header;
`accepted(timeout=None)` then returns that durable reference. Source reads,
body serialization and reproducibility capture follow in `PREPARING`.
Acceptance therefore does not promise that preparation will succeed.

Failure before initial publication produces `REJECTED`; an accepted preparation
failure produces `FAILED`. Neither launches computation. No execution is
admitted until preparation evidence and its consumer execution binding are
durable. Observable phases are `REGISTERED`, `ACCEPTING`, `PREPARING`, `RUNNING`
and `STOPPING`; terminal states are `SUCCEEDED`, `FAILED`, `STOPPED` and `REJECTED`.

- `done()` and `snapshot()` inspect without waiting. Completion does not mean
  success; inspect the terminal result's `state`, errors and `succeeded`.
- `wait(timeout=None)` returns the terminal `RunResult`, including unsuccessful
  work, for inspection. A timeout leaves the Run running.
- `result(timeout=None)` waits and raises when the Run is unsuccessful; otherwise
  it returns the same result projection.
- `stop()` withdraws this consumer. It prevents new work and lets already
  entered work settle. It does not promise to kill a running command or cancel
  shared work another consumer still needs.
- `stop(force=True)` also requests interruption of solely owned pooled commands.
  A running command's Dask worker is restarted within the same farm allocation;
  queued commands can be cancelled without restarting a worker. Shared work
  continues for its remaining consumers. Other placements and entered Python
  bodies still drain. A pending ordinary stop can be escalated with this call.

```python
run.stop(force=True)
result = run.wait()  # Inspect the actual outcomes after interruption settles.
```

Force is a request, not a successful-termination receipt. The live snapshot's
`force_requested` records that request; terminal outcomes and saved attempt
records establish what happened. If interruption cannot be confirmed, the
result retains the failure rather than claiming cancellation. Failed interruption
can leave admitted work running or allow queued work to enter while cancellation
messages are delayed. A worker disappearing from scheduler state is not proof
that its command stopped. A command that finishes before interruption retains
its actual result. Restarting restores
pool capacity and does not automatically rerun the cancelled computation.
`live.stop(force=True)` applies the same request to all Runs it currently owns.

`RunResult` exposes `outputs`, `report`, `run_id`, `history`, `summary()` and
invocation lookup, as explained in [results](results.md). Names identify consumer
history and never force recomputation. An engineering verdict is a returned
value, separate from execution success.

| Where | Argument | Purpose |
| --- | --- | --- |
| `runtime(site, override=None, ...)` | `locally=False` | Serve every placement by its authored body on this host |
| Runtime construction | `watch=False` | Print settling invocations and poll farm queue transitions |
| `live.submit(subject, ...)` | `name` | Required durable consumer submission name |
| submission | `priority=0` | Scheduling preference; higher numbers precede lower eligible work |
| submission | `stop_on_failure=True` | Stop admitting new work after the first failure |
| submission | `reproducibility=None`, `environment=None` | [Capture or supply reproducibility evidence](discovery.md#reproducibility-records) |

`Study.submit`, top-level `submit`, `Session`, `session`, `submit_all`,
`sequential`, external `client=`, and public `on_started`/`on_event` callbacks
are retired. They are not compatibility wrappers.

## Priority and capacity

Placement capacity comes from the Site. The controller rotates equal-priority
Runs and forwards priorities to Dask and the pool. Pending shared work uses the highest attached consumer priority.
Priority orders eligible pending work; it does not preempt an entered
invocation, reserve a farm allocation, or
become an LSF queue priority. Low-priority work can wait behind a steady stream
of higher-priority work. Dependencies still need to finish before their
consumers are eligible. See [placement and scheduling](../internals/placement-and-scheduling.md)
for the executor boundary and pooled command limits.

## Lifetime and shutdown

Automatic cleanup preserves process ownership; an interactive caller does
not have to remember a mandatory manual close. A context block provides a clear
optional scope: normal exit drains registered Runs, exceptional exit requests stop
and then settles entered work. `close()` provides early orderly release. None of
these imply that work survives interpreter exit or that a hard kill can perform
graceful settlement. Automatic exit cleanup is bounded and can leave incomplete
history; orderly `close()` is the drain boundary. Real pooled farm workers
additionally depend on cluster cancellation, connectivity and farm walltime; see [farm evidence](first-farm-run.md).

For a dashboard before submission, explicitly wait for startup:

```python
live.ready()
print(live.dashboard_link)
```

Runtime construction and context entry do not wait for cluster or farm startup;
construction begins opening the Site's configured resources, including pooled
worker allocations, before its first submission. Build and inspect your Study
before creating the Runtime when you want to review the Plan first. Submission
remains nonblocking. A first pooled Runtime imports Jobqueue on the
main thread while preserving SIGINT; [pooled bootstrap](sites.md#kind--lsf-pooled--a-shared-set-of-workers)
explains the cold-background-constructor limit.

## Caller-level stages

Keep a known graph in one composed Plan. If a later Plan needs result values,
author it from the caller after the earlier Run completes:

```python
jobs = live.submit(discovery, name="discover").result().outputs["jobs"].value
corners = corner_study(jobs)
print(corners.summary())
result = live.submit(corners, name="corners").result()
```

Worker-held nested submission is unsupported in this replacement; future
hierarchical use cases remain deferred. The [historically nested example](../../examples/nested_studies.py)
now demonstrates caller staging with one placement slot. Fresh acquisition
usually needs no staging: [content identity](runtime-artifacts.md) preserves
reuse inside one static Plan.

## Overrides and local debugging

```python
with runtime(site, {"placement": {"lsf": {"max_jobs": 1, "queue": "express"}}}) as live:
    run = live.submit(subject, name="small-farm-check").wait()

with runtime(site, locally=True) as live:
    run = live.submit(subject, name="local-debug").wait()
```

Overrides may change `placement` and `kernel` settings, never storage roots or
computation meaning. `locally=True` preserves the authored Plan and placement
capacity while serving its bodies locally; Dask remains the executor. A local
result can be reused by later farm work. Declare environment differences that
change results in computation identity rather than assume placement changes it.

## Watching and failures

`watch=True` prints settling invocation outcomes and, for direct farm work,
queue transitions from the attempt watcher. Watcher failure disables observation
and does not determine computation success. For custom interaction, poll Run
snapshots; there is no arbitrary callback on the controller loop.

With `stop_on_failure=True`, a failure stops new admissions and entered work
settles. `False` lets independent branches finish; failed dependencies block
their consumers either way. A stopped Run or failed preparation must not be
mistaken for an empty successful report. [Refusals](refusals.md) explain common
failures; saved history remains available after the Runtime ends.
