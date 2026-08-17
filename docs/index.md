# Hedloom

Author a study, see what it will do, and run it — from one file.

```python
from hedloom import Site, artifact, file, flow, local, operation, parameter, shell, study, sweep
```

This page is the user guide. How the package is put together, and why it exists
at all, is in [Hedloom internals](internals.md) — a contributor's page that
nothing here depends on.

Every snippet below is taken from a runnable example in
[`examples/`](../examples/rc_corners.py), and every printed output is real.
Nothing here is invented.

---

## The shape of a study

Three decorators, and only the third one can spend anything.

| You write | What it is | When its body runs |
| --- | --- | --- |
| `@operation` | one unit of work, with declared inputs, config and outputs | at `submit`, on whatever substrate its placement names |
| `@flow` | a reusable strategy that wires operations together | once, at *authoring* time, to build a static graph |
| `@study` | the whole plan | once, at authoring time; calling it plans and spends nothing |

The consequence worth internalising early: **calling an operation does not run
it.** It records an invocation and hands back a handle. So a flow body cannot
branch on a result — it never has one — and that is what lets a Plan predict
everything that will run before anything is spent.

## Authoring an operation

`@operation` here is `hedloom_flow`'s decorator, wrapped so the body it already
kept is remembered as callable under the operation identity the Plan records.
The body is real code that runs later:

```python
@operation(config={"key": parameter(str), "temp_c": parameter(int)},
           outputs={"deck": file("corner.cir", kind="spice-deck")})
def write_deck(out, *, key: str, temp_c: int) -> None:
    out.deck.write_text(DECK_TEMPLATE.format(key=key, temp_c=temp_c, ...))
```

(`examples/rc_corners.py`)

### `config` vs `inputs` — the distinction that decides reruns

* **`config`** is data folded straight into that invocation's identity — a
  temperature, a spec limit. Two invocations of one operation with different
  config are different attempts, full stop. There is **no dependency edge**,
  because nothing has to run first to produce a temperature.
* **`inputs`** are artifacts: references to another invocation's declared
  output, or to a source declared outside the plan. Every input becomes a
  dependency edge, and the invocation cannot start until its producer has
  finished.

```python
@operation(inputs={"deck": DECK},
           outputs={"raw": file("corner.raw", kind="simulator-raw-results")})
def simulate(deck, out):
    ...
```

### `out.<name>` — writing where the executor will look

A body that declares a file output receives an `out` argument **if and only if
its signature names `out`**. `out.deck` is a path inside that attempt's own
workspace, at exactly the location the executor checks afterwards. The two
cannot drift apart because they are the same declaration, read once.

Asking `out.<name>` for a name the operation never declared raises
`AttributeError` rather than resolving to something unexpected. A body that
declares only `returned(...)` or `stdout(...)` outputs is not handed `out` at
all.

### Returning a value, or returning a command

A body that computes a value returns it:

```python
@operation(inputs={"raw": RAW}, outputs={"hz": returned(kind="corner-frequency")})
def corner_frequency(raw) -> float:
    ...
    return frequency
```

A body that wants a command run **at its placement** returns `shell(...)`
instead of calling a subprocess itself:

```python
@operation(inputs={"deck": DECK},
           outputs={"raw": file("corner.raw", kind="simulator-raw-results")})
def simulate(deck, out):
    return shell("env", "SPICE_ASCIIRAWFILE=1", "ngspice", "-b", "-r", out.raw, deck)
```

Returning a `Shell` rather than running a subprocess is what lets the command
reach a placement. Locally it is a subprocess bound to this process's lifetime.
Give the operation an `lsf(...)` policy and the **identical body** puts that
same command on its own `bsub -I` job with that invocation's queue, cores and
licences — a one-line change on the operation, not a different body:

```python
@operation(..., policy=lsf(queue="normal", cores=1, memory_mb=2048))
def simulate_ac(...):
    return shell("ngspice", "-b", "-r", out.raw, deck)
```

The rule underneath all of this:

> **A body decides what runs; it never decides *whether* it runs.**

Reuse, identity, ordering and placement are settled before the executor calls
the body, so writing Python inside an operation cannot acquire scheduling
authority.

## Authoring a flow, and a study

A `@flow` body is ordinary Python that runs once, at authoring time:

```python
@flow
def rc_sweep(corners):
    measured = []
    for corner in sweep(corners, key="key"):
        deck = write_deck(key=corner["key"], temp_c=corner["temp_c"])
        measured.append(corner_frequency(simulate(deck)))
    return {"verdict": compare.named("compare")(measured).verdict}
```

A study is the same shape one level up. The decorated function *is* the study,
and its arguments make it a **family** of studies — a different corner list is a
different study, not a different file:

```python
@study(default_policy=local())
def rc_corners():
    return rc_sweep.named("rc")(CORNERS)

subject = rc_corners()          # -> a Study. Nothing spent.
```

`default_policy` is where work runs unless a call says otherwise.

Calling `.submit` on the decorated *name* rather than on a study is the mistake
this shape invites, so it is answered with the call to make instead:

```python
rc_corners.submit(site)
# AttributeError: 'rc_corners' is a family of studies, not one:
#                 call it first, as rc_corners(...).submit
```

### Handles are references, never values

```python
deck = write_deck(key="cold", temp_c=-40)   # a handle, not a path
if deck:                                    # HandleUsedAsValue (also TypeError)
    ...
```

`__bool__` and `__eq__` both raise. Answering `True`, or comparing handles by
name, would be an answer about the *reference* and silently wrong about the
*result* — no handle has a value yet to be true, false, or equal to anything.

### `sweep` — keys, and why they matter for reuse

`sweep(points, key=...)` opens a keyed scope: every call inside takes
`<point-key>:<operation>` unless it names its own key.

```python
for corner in sweep(corners, key="key"):
    deck = write_deck(key=corner["key"], temp_c=corner["temp_c"])
```

`key=` may name a field on each item, or be a callable
(`key=lambda item: item.key`, used in `examples/ota_pvt.py`).

This is not only ergonomics. **An unkeyed call's identity depends on authored
order**, so inserting an unrelated call earlier renumbers it — and a renumbered
invocation silently loses its reuse. `sweep` is what keeps that from depending
on an author remembering to key every call by hand.

## Before spending: `summary()`

`Study.summary()` renders every invocation, its operation and the placement it
resolved to, and spends nothing:

```python
subject = rc_corners()
print(subject.summary())
```

```
plan schema 3: 10 invocations, 0 sources
  cold:corner_frequency     rc_corners.corner_frequency  local
  cold:simulate             rc_corners.simulate          local
  cold:write_deck           rc_corners.write_deck        local
  compare                   rc_corners.compare           local
  hot:corner_frequency      rc_corners.corner_frequency  local
  hot:simulate              rc_corners.simulate          local
  hot:write_deck            rc_corners.write_deck        local
  nominal:corner_frequency  rc_corners.corner_frequency  local
  nominal:simulate          rc_corners.simulate          local
  nominal:write_deck        rc_corners.write_deck        local
```

`study.plan` is the exact document `submit` will run — not a second,
hand-written description of it.

---

## Running a study

### `Study.submit(...)` — the one-run form

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

### Several runs: `session(...)`

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

### Running less, or running elsewhere: `override`

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

### Debugging: `sequential` and `locally`

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

### Watching a run

`watch=True` does two different things, because there are two questions:

```
[      ran] cold:simulate                     succeeded
[watch] invoke:corner-tt pending → running (48s queued)
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

### When something fails

`stop_on_failure=True` (the default) means **stop admitting new work**: cancel
what has not started, let what is already executing finish, and return a
partial report naming what was skipped. The reasoning is that the usual answer
to a failed corner is to debug it rather than to spend the farm on the other
forty-nine — and resubmitting afterwards is cheap, because content-addressed
reuse means the corners that completed are reused and only the failure re-runs.

`stop_on_failure=False` lets independent branches finish, which is what a sweep
wants when the failure is known and local. Dependents of a failure are blocked
either way; they are never run against inputs that do not exist.

---

## `Site` and site profiles

Nothing about *where* a study runs is authored into the study.

```python
site = Site(
    root=str(work / "attempts"),
    workspace_root=str(work / "work"),
    address_spaces={"repository-relative": str(here)},
)
```

(`examples/rc_corners.py` — constructed directly, because it needs no placement
besides the default in-process one.)

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

[kernel]
threads = 2
dashboard = "network"     # "network" | "loopback" | "none"
```

(`examples/farm-smoke.site.toml`)

### The two numbers, which are about two different machines

| | Means | Sized from |
| --- | --- | --- |
| `[placement.*] max_jobs` | how many of **that placement's** invocations may be in flight | the share of the farm this study may spend |
| `[kernel] threads` | local concurrency on the **submit host** | how much in-process work this host should do at once |

`max_jobs` is **required** for an `lsf-interactive` placement, and it is
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
queue latency.

### `dashboard` — what the cluster exposes

A Dask scheduler starts an HTTP server whether or not anyone opens a browser,
and every worker starts one too, both on all interfaces. On a shared submit host
that publishes your corner names, workspace paths and profiler to everyone who
can reach it.

* `"network"` — the default, and exactly Dask's own behaviour.
* `"loopback"` — off the network; still reachable by other users of the same
  host, because loopback is per host and not per user.
* `"none"` — no listening socket at all. Refused for a multi-process cluster,
  whose workers must dial a listener.

Exposure changes how a run can be watched and **nothing** about what it
computes.

### `kind = "in-process"`

An in-process placement needs Python callables that no TOML can hold. Declare it
in the profile anyway — `submit` supplies the `BoundTransport` from your
authored bodies. A `[placement.*]` naming an unknown `kind` is refused outright
rather than silently dropped, because a study that quietly lost a placement
fails much later as an opaque `UnsupportedPlacement` that blames the Plan for
what is a configuration mistake.

---

## Reading results: `StudyRun`

```python
run["cold:simulate"].artifacts["raw"]["address"]
run.value            # the plan's conclusion: its final invocation's value
run.succeeded        # True iff every invocation succeeded
run.summary()        # one line per invocation: disposition, key, outcome
run.report.outcomes  # every InvocationOutcome, in plan order
run.document         # the Plan this run executed
```

`run["cold:simulate"]` looks up an `InvocationOutcome` by the authored key,
raising `KeyError` for a key nothing was authored with rather than returning
`None` and deferring the mistake.

Each outcome carries `authored_key`, `operation`, `input_digest`,
`disposition`, `outcome`, `placement`, `value`, `artifacts` and `error`. Those
first three are public join keys on purpose: **result tooling — a corner table,
a run diff — is a consumer of this data and needs no change to hedloom.**

The report is in **plan order** regardless of completion order, so two runs of
one plan stay comparable; `on_event` fires in completion order, because those
are two different questions.

## Reuse, and what invalidates it

Work whose declared inputs are unchanged is reused, not repeated. This is
`hedloom_exec`'s decision against content-addressed identity, and `hedloom`
neither re-implements nor overrides it.

**Folded into identity:**

* the operation's name, version, and a fingerprint of its body's *source*;
* every declared `config` value;
* every declared input's own identity, **transitively** — an upstream change
  propagates downstream automatically;
* a declared external source's **content** fingerprint, so editing an input
  netlist in place correctly invalidates everything that read it.

**Deliberately excluded**, so changing it never invalidates a result: which
queue an invocation ran on, its walltime, cores, memory, host, and general
environment. Retuning a corner's memory request or moving it to another queue
reuses the result it already produced.

The body fingerprint ignores blank lines and trailing whitespace but includes
everything else, docstrings included. So editing an operation body reruns every
invocation of that operation, and editing only a comment does not. That is
coarser than "the behaviour changed", deliberately: **a needless rerun costs
time, a missed one costs correctness.**

Only a `succeeded` attempt is reused automatically. A failure may be the work's
own verdict, or something incidental to it — an OOM kill, a preempted node —
that the record cannot tell apart. Failed attempts are retained rather than
silently retried, and accepting one is a separate, durable, human action
(`hedloom_exec.reuse.accept_for_reuse`).

**Reuse trusts your declaration.** An operation whose result depends on an
undeclared file, wall-clock time, or a mutable network resource is not honestly
reusable, and no digest detects that.

## Starting from a file the study did not write

An operation may declare an `input_artifact` source as an input and be handed
its located path. Every declared source is read **exactly once per submission,
before anything else runs**, and that one reading does both jobs: it
fingerprints the source (deciding whether downstream work is stale) and locates
it (what the body receives). They are computed together because those two
questions must agree about which file was meant.

A source that cannot be resolved, or does not exist, is fatal **before anything
runs** — the alternative is a run that reuses results computed from a file
nobody can show you. Addresses resolve on the submitting machine, which assumes
a shared filesystem for any placement that is not local.

## `hedloom.visualize` — looking before running

Two independent views, because they answer different questions:

* `structure(study)` reads the Plan into plain nodes and edges, in the
  vocabulary the study was authored in. No Dask, no graphviz; works even for a
  plan bound for a farm.
* `render(study, "graph.svg")` draws the *lowered Dask graph*. Needs `graphviz`
  and a system `dot`.

```python
import hedloom.visualize as visualize

print(json.dumps(visualize.structure(subject), indent=2))
visualize.render(subject, "graph.svg")
```

Every operation is bound to a stand-in that refuses to run — computing it raises
`RefusedComputation` rather than producing a number nobody simulated.
`submit()` remains the only way a study runs.

`visualize` is a submodule, deliberately not in `hedloom`'s top-level `__all__`:
drawing a graph is a diagnostic, not part of the authoring surface. `graphviz`
and `bokeh` are diagnostics the units themselves do not depend on — install them
into the project-local `.toolchain/venv` (`.toolchain/README.md`).

---

## Refusals you will actually meet

This project treats "silently doing something reasonable" as a defect, so a
surprising amount of the surface is refusals. Each of these is telling you
something specific.

| What you see | What it means |
| --- | --- |
| `HandleUsedAsValue` | You read a planning handle as a value. There is nothing there yet — a plan is built before anything runs. |
| `'x' is a family of studies, not one` | Call the decorated function first: `x(...).submit`, not `x.submit`. |
| `AttributeError` on `out.<name>` | This operation never declared a file output by that name. |
| `UnsupportedPlacement` (per invocation) | No transport provides the placement this invocation asked for. Deliberately fatal rather than run elsewhere. |
| `UnsupportedPlacement` (before anything runs) | The cluster declares no capacity for a placement the plan uses. Build it with `cluster_for(site)`, or just let `submit` do it. |
| `SiteError: placement 'x' declares no max_jobs` | An LSF placement needs its budget stated. There is no safe default. |
| `SiteError: placement 'x' declares unknown option 'queeu'` | A typo'd or unrepresentable placement option, named by placement *and* key. Never a bare `TypeError`. |
| `SiteError: a run may override ... nowhere` | An override tried to reach something that changes what a run *means*. |
| `SiteError: this session needs a scheduler` | The site declares real capacity and `distributed` is missing. Install the extra, or ask for `sequential=True`. |
| `ConcurrentClaim` | Another caller holds this attempt — usually the same study still running. Reported as one refused invocation; the rest of the sweep continues. |
| `AttemptSpent` | A terminal result exists at this identity that may not be reused. |
| `UnrecoverableAttempt` | A substrate that cannot say whether it accepted work. **A supported outcome, not a bug** — guessing here is what produces duplicate farm jobs. |
| a transport refusing an option before submission | Dropping a stated resource need would run the work under conditions nobody asked for. |

## What is not here, and will not arrive quietly

There is **no result-dependent control**. A flow body runs at planning time and
produces a fixed graph, so a Plan still predicts what will run. A `retry=`,
`max_iterations=` or `until=` argument on `submit` is **the tripwire, not a
feature** — if a study needs to branch on a result, raise it rather than working
around it. The open architectural question is recorded in
`docs/vision/open-concepts.md` at the repository root.

*Staged* plans are a different thing and are already demonstrated: an invocation
may author and submit an inner Plan (`examples/ota_pvt_clean_nested.py`). Each
plan is still fully determined when authored; a later stage is authored only
after an earlier one produced ordinary Python values. See
[internals](internals.md).

---

## Further reading

* [`examples/rc_corners.py`](../examples/rc_corners.py) — the whole path in one
  file, against real ngspice, with an analytic answer you can check by hand.
* `examples/ota_pvt.py` — the full OTA/PVT reference: structural analysis, real
  ngspice, real spec checking. `examples/ota_pvt_clean.py` is the same study
  reduced to the sign-off path, writing a `report.md`.
* [`examples/farm_smoke.py`](../examples/farm_smoke.py) with
  [`farm-smoke.site.toml`](../examples/farm-smoke.site.toml) — a simulator-free
  farm check: four points, eight `bsub -I` jobs, all reused on a second
  submission. How much of it runs at once is the profile's `max_jobs`, not a
  flag on the command.
* [`review-before-the-farm.md`](review-before-the-farm.md) — **read this before
  a first real farm run.** It has the reading order by what reaches the farm,
  and the smallest sequence of runs that learns the most.
* [`dask-scheduling-concepts.md`](dask-scheduling-concepts.md) for the
  scheduling model as prose, and
  [`dask-scheduling-rules.md`](dask-scheduling-rules.md) for the same as ten
  source-cited rules.
* [`hedloom/ONTOLOGY.md`](../ONTOLOGY.md) — the unit's current contracts, and
  what its examples have and have not demonstrated. In particular: the
  sequential kernel has reached a real farm; the graph kernel has not.
* [Hedloom internals](internals.md) — for working *on* this package.

```{toctree}
:hidden:

../ONTOLOGY
```

```{toctree}
:maxdepth: 1
:caption: Working on this package

internals
```

```{toctree}
:maxdepth: 1
:caption: Running it on a farm

review-before-the-farm
placement-clustering-scheduling
dask-scheduling-concepts
dask-scheduling-rules
```

```{toctree}
:maxdepth: 1
:caption: Protocols, and what checking them found

attempt-claim-protocol
stop-admitting-protocol
binding-the-attempt-identity
```

```{toctree}
:maxdepth: 1
:caption: Reviews and plans, by date

architecture-review-2026-08-14
concurrency-two-workers-2026-08-15
dask-usage-review-2026-08-16
implementation-plan-2026-08-16
cancellation-plan
pooled-placement-plan
```
