# Hedloom

Author a study, see what it will do, and run it — from one file.

```python
from hedloom import Site, artifact, file, flow, local, operation, shell, study, sweep

GRID = artifact("grid-declaration")

@operation(config={"steps": parameter(int)}, outputs={"grid": file("grid.txt")})
def write_grid(out, *, steps):
    out.grid.write_text(render(steps))           # the body really runs

@operation(inputs={"grid": GRID},
           outputs={"result": file("result.txt")},
           policy=lsf(queue="normal", cores=4, licences={"solver": 1}))
def integrate(grid, out):
    return shell("awk", "-f", RULE, "-v", f"out={out.result}", grid)  # at its placement

@flow
def refine(points):
    for point in sweep(points, key="key"):        # keyed scope per point
        yield integrate(write_grid(steps=point["steps"]))

@study(name="grid-refinement")
def refinement(points):
    return refine.named("refinement")(points)     # records; nothing runs

subject = refinement(POINTS)                      # planning, not spending
print(subject.summary())                          # nothing spent yet
run = subject.submit(site=Site.from_file("site.toml"), watch=True)
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

[`docs/`](docs/index.md) is the guide, built into the project's Sphinx site by
`python composition.py docs` from the repository root. Start at
[authoring a study](docs/guide/authoring.md), then
[running one](docs/guide/running.md) and
[pointing it at a farm](docs/guide/sites.md);
[internals](docs/internals/index.md) is for working *on* the package.

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

Declared file outputs also get a stable current-result name under
`<Site.root>/latest/<study>/<authored-key>/<output>`. Each try-named workspace
remains immutable evidence;
editing an input still moves the record identity, while the alias is repointed
to the selected try before the next launch. Use `hedloom where`, `hedloom check`, and `hedloom log` to resolve
the current output, detect a cached stale path, and inspect why records reran.

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
  thing that spends. Its name is the stable namespace used by records and CLI
  selectors. A `@flow` is the same planning shape one level down, without an
  execution namespace or submission authority.
- **`sweep(points, key=...)`** names every call inside the loop, so reuse cannot
  be lost to renumbering — the trap that made unnamed invocations dangerous.
  `.named("...")` does it by hand for a single call.
- **`Site`** holds what is not the study: placements, roots, address spaces,
  threads, and retention. From TOML, with relative paths anchored to the profile.

`hedloom prune --site site.toml` is always a survey unless `--apply` is
present. It reports candidates and exclusions from the Site's named retention
rules; `--json` makes that plan usable in CI. Pins are separate operator
promises: `hedloom pin`, `hedloom unpin`, and `hedloom pins` protect terminal
try paths with durable reasons and attribution.

## What it does not change

`hedloom-exec` still owns one durable record and its tries, and imports neither this
package nor Dask. `hedloom-run` still owns binding and readiness and both of its
kernels, so a session is the same run with a scheduler deciding readiness rather
than a loop. Reuse, identity, placement, licences, the watcher — untouched. This
unit composes; it does not reimplement.

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

Both submissions run inside one session, which is the whole of what a study
author has to hold:

```python
with session(site, watch=True) as farm:
    first = farm.submit(subject)
    second = farm.submit(subject)      # reuse, same cluster, same watcher
```

The session owns the cluster, the client and one queue watcher, and gives them
back when the block ends — which matters, because under owner-bound lifetime
leaving it ends any farm job still in flight. There is no kernel to choose:
capacity is the site's, and a site that declares none has capacity one. Ask for
`sequential=True` to run one at a time with no scheduler at all (this is what
keeps `distributed` an optional extra), or `locally=True` to debug the whole
study in this process without touching the farm. Either can be narrowed for a
single run without a second profile:

```python
with session(site, {"placement": {"lsf": {"max_jobs": 1, "queue": "express"}}}) as farm:
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

* **One session, two studies.** A session is one cluster, and a placement's
  budget belongs to that cluster's workers, so `submit_all` cannot put more on
  the farm than the site declared however many studies it is given. Eight jobs
  are wanted, `max_jobs` is two, and no more than two are ever in flight.
* **One session, the same study twice.** Dask keys belong to the scheduler, so
  identical work submitted twice is one task: four jobs, not eight. The attempt
  claim is never consulted here — there is only ever one caller — so this is
  Dask's idempotence, not hedloom's. Both reports say `claimed`.
* **Two sessions, the same study.** Different key namespaces, so both callers
  really do reach the attempt protocol and the journal claim is what prevents
  the duplicate. The loser is refused by name rather than made to wait. This is
  also the arrangement where the cap does *not* hold: each session has its own
  cluster and therefore its own budget, so two controllers can put twice
  `max_jobs` on the farm.

A fourth pass resubmits all of it from one session and must spend nothing.
`tests/test_farm_multi_client_example.py` runs the whole thing against the fake
`bsub`, checking the same numbers from the submission records rather than from
the journals, so the two instruments have to agree.

Once a study has run, the questions stop being about authoring and start being
about a path. `examples/cli.py` is that loop, through the command line:

```console
python examples/cli.py
```

`hedloom where` resolves the current output to hand a tool; one point's inputs
are then edited, and `hedloom check` answers `behind: … was superseded by …
(arguments changed)` and **exits 1**, so a script can branch on it. The point
nobody touched still answers `current`. `hedloom log` shows both iterations,
and `hedloom pin` refuses an authored key that now names two of them rather
than guessing which one you meant.

Storage is the one resource a study spends that nothing returns on its own.
`examples/retention.py` spends some deliberately and then takes it back:

```console
python examples/retention.py
```

Four points, two of which write their whole trace and then diverge. Nothing is
reclaimable yet — `latest/` still resolves to those failures, because an alias
is bound before a body runs so a tool can watch an output while it is written.
A second pass corrects the diverging points, the alias moves, and the spent
tries become candidates. One is pinned first, so the refusal to reclaim it is
shown rather than asserted. The survey states how many bytes it would free; the
filesystem is measured before and after; the two have to agree.
