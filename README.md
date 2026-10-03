# Hedloom

Author a study, see what it will do, and run it — from one file.

Here `Study` means a named execution envelope: a Plan and the operations that
implement it. It supplies the executable part of a wider inquiry, which also
includes intent, context, interpretation, and decisions. A completed run
provides evidence; accepting a conclusion requires judgment about that evidence.

This abbreviated example assumes `render`, `RULE`, `POINTS`, and a configured
`site.toml`; the complete local example is `examples/grid_refinement.py`.

```python
from hedloom import Site, artifact, file, flow, local, lsf, operation, parameter, runtime, shell, study, sweep

GRID = artifact("grid-declaration")

@operation(config={"steps": parameter(int)}, outputs={"grid": file("grid.txt", kind="grid-declaration")})
def write_grid(out, *, steps):
    out.grid.write_text(render(steps))           # the body really runs

@operation(inputs={"grid": GRID},
           outputs={"result": file("result.txt")},
           policy=lsf(queue="normal", cores=4, licences={"solver": 1}))
def integrate(grid, out):
    return shell("awk", "-f", RULE, "-v", f"out={out.result}", grid)  # at its placement

@flow
def refine(points):
    results = {}
    for point in sweep(points, key="key"):        # keyed scope per point
        results[point["key"]] = integrate(write_grid(steps=point["steps"])).result
    return results

@study(name="grid-refinement")
def refinement(points):
    return refine.named("refinement")(points)     # records; nothing runs

subject = refinement(POINTS)                      # planning, not spending
print(subject.summary())                          # nothing spent yet
with runtime(Site.from_file("site.toml"), watch=True) as live:
    run = live.submit(subject, name="investigate-start").wait()
print(run["coarse:integrate"].artifacts["result"]["address"])
```

`examples/grid_refinement.py` is the whole of that, runnable, against real awk:
three grids whose integral is analytic, so the answer can be checked rather than
believed — and whose error falls by sixteen for each refinement by four, which
is the trapezoid rule's own second-order convergence and not a tolerance anyone
chose.

Hedloom names no tool and no domain. It was built for analog simulation studies
and that is where it is exercised hardest, but nothing in the package knows
what the work is: the studies that do live in
[`../studies/`](../studies/README.md), one level up, where naming a simulator
is honest.

## Documentation

For a script running one study, `run_study(subject, site=site, name="check")`
returns its completed `RunResult` and cleans up automatically. Set
`require_success=True` to raise `RunFailed` on an unsuccessful result. Retain
one `runtime(site)` for repeated or overlapping submissions and shared pools.

[`docs/`](docs/index.md) is the guide. Start at
[authoring a study](docs/guide/authoring.md), then
[running one](docs/guide/running.md) and
[pointing it at a farm](docs/guide/sites.md). The
[storage path migration guide](docs/guide/storage-migration.md) covers the
breaking Python, TOML, CLI, and saved-run changes.
[Internals](docs/internals/index.md) is for working *on* the package.

Build the standalone site from this checkout with Python 3.12:

```console
python -m venv .venv
.venv/bin/python -m pip install -r docs/requirements.txt ./flow ./exec ./run .
.venv/bin/python tools/stage_docs.py
.venv/bin/python -m sphinx -b html -W --keep-going build/docs-source build/docs/html
```

Open `build/docs/html/index.html`. The site includes all four units' guides
and API references, using their `unit.toml` documentation declarations.
The parent workspace can still build its aggregate site with
`python composition.py docs`.

For hosting, import `https://github.com/smldis/hedloom` into Read the Docs,
choose `main` as the default branch, and use `.readthedocs.yaml` as the
configuration file. Once this configuration is on `main`, trigger the first
build and verify the GitHub webhook is connected so pushes rebuild `latest`.
The configuration stages these same sources and treats Sphinx warnings as
build failures. See the [Read the Docs import guide](https://docs.readthedocs.com/platform/stable/intro/add-project.html).

Two neighbouring surfaces are deliberately not part of that site. `ONTOLOME.md`
in each unit states the contracts that unit currently guarantees, and is where a
change to a contract must be recorded. [`design/`](design/README.md) holds
reviews, plans and proposals written on a date — not maintained against the
code, and never to be cited as evidence of how it now behaves.

## What this unit adds

Nothing that the three units below it could not already do — it removes the
seam between them. Before it, an operation body was dead code, and a study
needed a second file supplying implementations, command lines, output paths,
transports and roots; for this project's reference study that file is six
hundred lines whose only job is to agree with the first one.

Each try-named workspace is immutable evidence, and each invocation's outcome
says which record and try it landed on — `run["point:solve"].record` and
`.try_number` — so a caller keeps an exact reference to the execution it got,
reused or fresh. Editing an input moves the record; retrying moves only the
try. [Run discovery](docs/guide/discovery.md) preserves named submissions and
reveals their selections before blocking launch. Configure a separate
`Site.runs_dir` or `[study] runs_dir` before submitting.

- **The body is the implementation.** `@operation` here is `hedloom_flow`'s,
  wrapped so the function it already kept is remembered as callable. The Plan
  records `module:qualname` and a fingerprint of the source, so an edited body
  reruns the work it produced instead of relying on someone bumping `version`.
- **`out` is the attempt's own workspace.** A body writing `out.grid` writes
  where the executor will look, because both read one declaration.
- **`shell(...)` is a launcher.** Returning a command instead of running one is
  what lets it reach a placement: locally it is a subprocess, on `lsf` it is one
  `bsub -I` job with that invocation's queue, cores and licences.
- **`@study` is the named execution envelope.** Calling the decorated function
  records its Plan and hands back something inspectable; `submit` is the only
  thing that spends. Its definition name and the chosen submission name are
  recorded in consumer history. Neither names a computation record: a record is
  selected by the computation an invocation declares, so two studies declaring
  the same work share one record and neither owns it. A `@flow` is the same
  planning shape one level down, without an operator name or submission
  authority.
- **`sweep(points, key=...)`** gives calls semantic point names.
  `.named("...")` supplies a key for a single call; otherwise `function_name.N`
  is generated within its boundary. Computation reuse is independent of these
  readable Plan identities.
- **`Site`** holds what is not the study: placements, independent
  `records_dir`, `runs_dir`, and `work_dir` locations, address spaces,
  threads, and retention. From TOML, relative paths anchor to the profile.

`hedloom prune --site site.toml` is always a survey unless `--apply` is
present. It reports candidates and exclusions from the Site's named retention
rules; `--json` makes that plan usable in CI. Pins are separate operator
promises: `hedloom pin`, `hedloom unpin`, and `hedloom pins` protect terminal
try paths with durable reasons and attribution.

## What it does not change

`hedloom-exec` still owns one durable record and its tries, and imports neither this
package nor Dask. `hedloom-run` owns binding and cooperative readiness. The facade Runtime owns
its controller loop and resource lifetime, and Dask executes ready invocations.
Exec keeps attempt identity and reuse; this unit composes those contracts.

```console
PYTHONPATH=src:flow/src:exec/src:run/src python -m pytest -q
python examples/grid_refinement.py
```

## Farm sweep test

After `hedloom/exec/examples/lsf_preflight.py --queue reg` passes, exercise the
complete plan-to-record path with no real tool at all:

```console
python examples/farm_smoke.py examples/farm-smoke.site.toml
```

The Plan sweeps four points, each with explicit `start` and `count` parameters.
For each point one `/bin/sh` command generates a numeric file and a second
POSIX-shell command consumes it, producing eight visible `bsub -I` jobs and four
deterministic summaries. It then submits the same Plan again and
requires all eight invocations to be reused without new jobs. Results live under
`examples/_runs/farm-smoke/`. The profile explicitly requests queue `reg`, one
core per job, and a one-minute walltime; copy the TOML and change those site
facts when needed.

Both submissions run inside one runtime, which is the whole of what a study
author has to hold:

```python
with runtime(site, watch=True) as farm:
    first = farm.submit(subject, name="investigate-start").wait()
    second = farm.submit(subject, name="investigate-start").wait()      # reuse, same cluster, same watcher
```

The Runtime owns the controller, executor and optional queue watcher. Context
exit drains work and releases resources; automatic cleanup also preserves
owner-bound lifetime outside a block. Local capacity one uses the same async
engine as a concurrent Site. `locally=True` serves authored bodies on this host
for debugging. No sequential execution mode remains. An override can narrow
capacity or change a queue without another profile:

```python
with runtime(site, {"placement": {"lsf": {"max_jobs": 1, "queue": "express"}}}) as farm:
    ...
```

An override changes how a run executes and never what it means, so an overridden
run lands on the same attempt identities and the two reuse each other's work.

## Two studies at once

What the first test does not ask is what happens when something else is already
running. `examples/farm_multi_client.py` does, using the same operations:

```console
python examples/farm_multi_client.py --queue reg --max-jobs 2
```

Its site is built in Python rather than read from a profile — the other half of
the smoke test. A profile is right when a queue, a walltime and a farm share
belong to an installation and get copied per site; here the site *is* the
experiment, three arrangements differing in one declared number, so arguments
are both shorter and more honest than three TOML files.

Each arrangement is measured from the attempt journals rather than from the
process that started the work — one interval per job, between the
`submit_intent` written before the transport is touched and the receipt written
when `bsub -I` returns:

* **One runtime, two studies.** A runtime is one cluster, and a placement's
  budget belongs to that cluster's workers. Concurrent submissions stay within
  the declared farm share however many studies use it. Eight jobs
  are wanted, `max_jobs` is two, and no more than two are ever in flight.
* **One runtime, the same study twice.** Compatible ready invocations
  share Runtime-owned execution handles. Both submissions succeed and retain
  exact live history; completed evidence is still reused through Exec on later runs.
* **Two runtimes, the same study.** Different key namespaces, so both callers
  really do reach the attempt protocol and the journal claim is what prevents
  the duplicate. The loser is refused by name rather than made to wait. This is
  also the arrangement where the cap does *not* hold: each runtime has its own
  cluster and therefore its own budget, so two controllers can put twice
  `max_jobs` on the farm.

A fourth pass resubmits all of it from one runtime and must spend nothing.
`tests/test_farm_multi_client_example.py` runs the whole thing against the fake
`bsub`, checking the same numbers from the submission records rather than from
the journals, so the two instruments have to agree.

Storage is the one resource a study spends that nothing returns on its own.
`examples/retention.py` spends some deliberately and then takes it back:

```console
python examples/retention.py
```

Four points, two of which write their whole trace and then diverge. Nothing is
reclaimable yet — the shipped seven-day floor protects work that recent, which
is a fact about the evidence rather than about whose it is. A second pass runs
the corrected points as their own computations, the floor is then lowered on
purpose and said out loud, and the spent tries become candidates. One is pinned
first, so the refusal to reclaim it is shown rather than asserted. The survey
states how many bytes it would free; the filesystem is measured before and
after; the two have to agree.
