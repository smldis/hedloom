# Hedloom

Author a study, see what it will do, and run it — from one file.

Hedloom composes three units into one gesture: `hedloom-flow` authors a Plan,
`hedloom-exec` owns one attempt's durable record, and `hedloom-run` walks the
Plan and executes it. What this package adds is the join — a `submit` that runs
exactly what was authored, because it holds both halves rather than asking two
files to agree.

```python
from hedloom import Site, artifact, file, flow, local, lsf, operation, parameter, shell, study, sweep

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

Three ideas carry the rest of this documentation:

- **Calling an operation does not run it.** It records an invocation and returns
  a handle, so a Plan predicts everything that will run before anything is spent.
- **A body decides what runs; it never decides *whether* it runs.** Reuse,
  identity, ordering and placement are settled before the executor calls a body.
- **Where a study runs is not part of what it means.** Placement, queue and
  concurrency come from a `Site`, and changing them reuses work rather than
  repeating it.

## Start here

| If you want to | Read |
| --- | --- |
| write your first study | [Authoring a study](guide/authoring.md) |
| run one, or several, or debug one | [Running a study](guide/running.md) |
| point it at a farm, or tune concurrency | [Sites and placements](guide/sites.md) |
| read results, or understand what gets reused | [Results, reuse, and looking before you run](guide/results.md) |
| spend real queue time for the first time | [Your first run on a real farm](guide/first-farm-run.md) |
| find out what an error is telling you | [Refusals you will actually meet](guide/refusals.md) |
| work *on* this package rather than with it | [Internals](internals/index.md) |

## Runnable evidence

Every snippet in this guide is taken from a file in `examples/`, and every
printed output is real.

* [`examples/grid_refinement.py`](../examples/grid_refinement.py) — the whole
  path in one file, against real awk: three grid resolutions whose integral is
  analytic, so the answer can be checked rather than believed, and whose error
  falls by sixteen for each refinement by four.
* [`examples/farm_smoke.py`](../examples/farm_smoke.py) with
  [`farm-smoke.site.toml`](../examples/farm-smoke.site.toml) — a tool-free
  farm check: four points, eight `bsub -I` jobs, all reused on a second
  submission. `farm_smoke_pooled.py` is the same shape through a pool.
* `examples/farm_multi_client.py` — what happens when two studies, or two
  controllers, want the farm at once, measured from the attempt journals.
* [`examples/cli.py`](../examples/cli.py) — the operator's loop rather than the
  author's, driven entirely through the `hedloom` command: `where` resolves a
  path to hand a tool, one point's inputs are edited, and `check` answers
  `behind` with a non-zero status so a script can branch on it. `log`, `prune`
  and `pin` finish the tour. Every command is really executed.
* [`examples/live_source.py`](../examples/live_source.py) — a study that
  reads something served from outside it, re-read on every run: a
  nonce-bearing stage fetches unconditionally and then submits an inner plan
  whose declared source is fingerprinted by content, so an unchanged document
  reuses everything below it and a changed one reuses nothing. Both stages
  share one session, which is what keeps one budget open rather than two.
* [`examples/retention.py`](../examples/retention.py) — what storage a study
  spends and when it can be taken back: two points diverge, a second pass
  supersedes them, and the survey's promised byte count is checked against
  what the filesystem actually loses. One spent try is pinned first, so the
  refusal to reclaim it is shown rather than asserted.

The same study appears twice more, one layer down each time, which is the
shortest way to see what each unit is responsible for:
[`flow/examples/refinement.py`](../flow/examples/refinement.py) authors it and
prints the Plan while running nothing, and
[`exec/examples/planned_refinement.py`](../exec/examples/planned_refinement.py)
executes that document through the attempt API, refines one point, and reuses
the rest. All three arrive at the same three estimates — one of them by way of
real `awk` — because the trapezoid rule does not care who evaluates it.

Nothing in `examples/` names a domain. The studies that do — real simulators,
real circuits, real sign-off limits — live in `../studies/`,
one level up from this package, and are where hedloom is exercised hardest.

```{toctree}
:maxdepth: 2
:caption: Using Hedloom

guide/authoring
guide/running
guide/sites
guide/results
guide/first-farm-run
guide/refusals
```

```{toctree}
:maxdepth: 2
:caption: Reference

api
```

```{toctree}
:maxdepth: 2
:caption: Working on Hedloom

internals/index
```
