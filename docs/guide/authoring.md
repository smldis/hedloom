# Authoring a study

Three decorators, and only the third one can spend anything.

| You write | What it is | When its body runs |
| --- | --- | --- |
| `@operation` | one unit of work, with declared inputs, config and outputs | at `submit`, on whatever substrate its placement names |
| `@flow` | a reusable strategy that wires operations together | once, at *authoring* time, to build a static graph |
| `@study` | the named top-level execution envelope | once, at authoring time; calling it plans and spends nothing |

The consequence worth internalising early: **calling an operation does not run
it.** It records an invocation and hands back a handle. So a flow body cannot
branch on a result — it never has one — and that is what lets a Plan predict
everything that will run before anything is spent.

Every snippet below is taken from a runnable example in
[`examples/`](../../examples/grid_refinement.py), and every printed output is
real. Nothing here is invented. The example integrates `exp(-x)` by the
trapezoid rule at three grid resolutions — small enough to read in one sitting,
and analytic, so its answer can be checked rather than believed.

## Authoring an operation

`@operation` here is `hedloom_flow`'s decorator, wrapped so the body it already
kept is remembered as callable under the operation identity the Plan records.
The body is real code that runs later:

```python
@operation(config={"key": parameter(str), "steps": parameter(int)},
           outputs={"grid": file("grid.txt", kind="grid-declaration")})
def write_grid(out, *, key: str, steps: int) -> None:
    out.grid.write_text(GRID_TEMPLATE.format(key=key, steps=steps, ...))
```

(`examples/grid_refinement.py`)

### `config` vs `inputs` — the distinction that decides reruns

* **`config`** is data folded straight into that invocation's identity — a
  step count, a tolerance. Two invocations of one operation with different
  config are different attempts, full stop. There is **no dependency edge**,
  because nothing has to run first to produce a step count.
* **`inputs`** are artifacts: references to another invocation's declared
  output, or to a source declared outside the plan. Every input becomes a
  dependency edge, and the invocation cannot start until its producer has
  finished.

```python
@operation(inputs={"grid": GRID},
           outputs={"result": file("quadrature.txt", kind="quadrature-result")})
def integrate(grid, out):
    ...
```

### `out.<name>` — writing where the executor will look

A body that declares a file or directory output receives an `out` argument **if
and only if its signature names `out`**. `out.grid` is a path inside that
attempt's own workspace, at exactly the location the executor checks
afterwards. The two cannot drift apart because they are the same declaration,
read once.

Use `file("result.txt", kind="report")` for one file and
`directory("results", kind="report-bundle")` for a directory tree. In both,
`kind=` is the artifact-contract label that connects operations; `file` versus
`directory` is the filesystem shape. A successful run must leave that declared
shape behind. Directory metadata records the recursive payload size, and an
empty directory is valid just as an empty file is.

Asking `out.<name>` for a name the operation never declared raises
`AttributeError` rather than resolving to something unexpected. A body that
declares only `returned(...)` or `stdout(...)` outputs is not handed `out` at
all.

### Returning a value, or returning a command

A body that computes a value returns it:

```python
@operation(inputs={"result": QUADRATURE},
           outputs={"estimate": returned(kind="integral-estimate")})
def estimate(result) -> float:
    ...
    return value
```

A body that wants a command run **at its placement** returns `shell(...)`
instead of calling a subprocess itself:

```python
@operation(inputs={"grid": GRID},
           outputs={"result": file("quadrature.txt", kind="quadrature-result")})
def integrate(grid, out):
    return shell("awk", "-f", RULE, "-v", f"out={out.result}", grid)
```

Returning a `Shell` rather than running a subprocess is what lets the command
reach a placement. Locally it is a subprocess bound to this process's lifetime.
Give the operation an `lsf(...)` policy and the **identical body** puts that
same command on its own `bsub -I` job with that invocation's queue, cores and
licences — a one-line change on the operation, not a different body:

```python
@operation(..., policy=lsf(queue="normal", cores=1, memory_mb=2048))
def integrate(...):
    return shell("awk", "-f", RULE, "-v", f"out={out.result}", grid)
```

`pooled()` is the third choice. Which of the three to reach for, and what each
costs, is [where placement is configured](sites.md#placement-kinds) — the
authoring side is one keyword either way.

Placement is not identity-bearing, so moving an operation between the three
reuses everything it already produced rather than recomputing it.

The rule underneath all of this:

> **A body decides what runs; it never decides *whether* it runs.**

Reuse, identity, ordering and placement are settled before the executor calls
the body, so writing Python inside an operation cannot acquire scheduling
authority.

## Authoring a flow, and a study

A `@flow` body is ordinary Python that runs once, at authoring time:

```python
@flow
def refinement_sweep(points):
    measured = []
    for point in sweep(points, key="key"):
        grid = write_grid(key=point["key"], steps=point["steps"])
        measured.append(estimate(integrate(grid)))
    return {"verdict": compare.named("compare")(measured).verdict}
```

A study is the same planning shape one level up, plus the stable name under
which its attempts and current outputs are recorded. The decorated function is
a **family** of study instances: its arguments change inputs, while every
instance retains the family's name:

```python
@study(name="grid-refinement", default_policy=local())
def grid_refinement():
    return refinement_sweep.named("refinement")(POINTS)

subject = grid_refinement()     # -> a Study. Nothing spent.
assert subject.name == "grid-refinement"
```

Omit `name=` and Hedloom infers `module.qualname`, using the same default as
operation and flow definitions. An explicit name is useful for a short, stable
CLI namespace. Output names do not name a study: returning `{"psf": psf}`
still leaves this study named `grid-refinement`.

**What the study returns is what it exports**, and those names are how the run
is read afterwards — `run.outputs["verdict"].value`, never an aggregate over
whatever happened to run last. Export everything a reader of this study needs,
including a measurement that a later step also consumes; see
[reading results](results.md#what-the-study-produced-runoutputs).

`default_policy` is where work runs unless a call says otherwise.

Calling `.submit` on the decorated *name* rather than on a study is the mistake
this shape invites, so it is answered with the call to make instead:

```python
grid_refinement.submit(site)
# AttributeError: 'grid_refinement' is a family of studies, not one:
#                 call it first, as grid_refinement(...).submit
```

### Handles are references, never values

```python
grid = write_grid(key="coarse", steps=8)    # a handle, not a path
if grid:                                    # HandleUsedAsValue (also TypeError)
    ...
```

`__bool__` and `__eq__` both raise. Answering `True`, or comparing handles by
name, would be an answer about the *reference* and silently wrong about the
*result* — no handle has a value yet to be true, false, or equal to anything.

### `sweep` — keys, and why they matter for reuse

`sweep(points, key=...)` opens a keyed scope: every call inside takes
`<point-key>:<operation>` unless it names its own key.

```python
for point in sweep(points, key="key"):
    grid = write_grid(key=point["key"], steps=point["steps"])
```

`key=` may name a field on each item, or be a callable
(`key=lambda item: item.key`, used in `../../studies/ota_pvt.py`).

This is not only ergonomics. **An unkeyed call's identity depends on authored
order**, so inserting an unrelated call earlier renumbers it — and a renumbered
invocation silently loses its reuse. `sweep` is what keeps that from depending
on an author remembering to key every call by hand.

The eight lines that do it, and why the same trap is live in other DAG builders,
are in [how `@study`, `@flow` and `sweep` work, and how that
compares](../internals/mechanism.md).

## Before spending: `summary()`

`Study.summary()` renders every invocation, its operation and the placement it
resolved to, and spends nothing:

```python
subject = grid_refinement()
print(subject.summary())
```

```
study grid-refinement
plan schema 3: 10 invocations, 0 sources
  coarse:estimate    grid_refinement.estimate    local
  coarse:integrate   grid_refinement.integrate   local
  coarse:write_grid  grid_refinement.write_grid  local
  compare            grid_refinement.compare     local
  fine:estimate      grid_refinement.estimate    local
  fine:integrate     grid_refinement.integrate   local
  fine:write_grid    grid_refinement.write_grid  local
  medium:estimate    grid_refinement.estimate    local
  medium:integrate   grid_refinement.integrate   local
  medium:write_grid  grid_refinement.write_grid  local
```

`study.name` is its durable operator-facing name, for authoring and run
context. It does not reach storage: a record is selected by the computation an
invocation declares, so a study that declares the same work as another one
reuses that work rather than repeating it under its own name, and neither
study owns the record; see
[how a study becomes work](../internals/mechanism.md). `study.plan` is the
exact document `submit` will run — not a second, hand-written description of
it.

`hedloom.visualize` draws the same thing two other ways; see
[looking at a study before running it](results.md#looking-at-a-study-before-running-it).

## What a study cannot do, and will not learn quietly

There is **no result-dependent control**. A flow body runs at planning time and
produces a fixed graph, so a Plan still predicts what will run. A `retry=`,
`max_iterations=` or `until=` argument on `submit` is **the tripwire, not a
feature** — if a study needs to branch on a result, raise it rather than working
around it. The open architectural question is recorded in
`docs/vision/open-concepts.md` at the repository root.

*Staged* plans are a different thing and are already demonstrated: an invocation
may author and submit an inner Plan (`../../studies/ota_pvt_clean_nested.py`).
Each
plan is still fully determined when authored; a later stage is authored only
after an earlier one produced ordinary Python values. See
[internals](../internals/index.md#staged-plans).
