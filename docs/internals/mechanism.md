# `@study`, `@flow`, `sweep` — how they work, and how that compares

Two halves. The first explains the mechanism as it is actually implemented in
this repository, with file references. The second sets it against other systems
that build a DAG and then run it: pure Dask, TensorFlow graph mode, LibreLane,
and CACE.

Where this page sits: [the guide](../guide/authoring.md) tells you how to
*write* a study, and [internals](index.md) tells you which unit owns what. This
one is between them — what the machinery does underneath the three decorators, and why
it is shaped that way rather than the way the neighbouring systems are. Read it
when you want the reasoning, or when someone asks why this is not just Dask.

The one-line thesis, so the rest reads as evidence for it:

> Hedloom traces Python **once** to produce a plain-data document, derives every
> identity as a **pure function of that document**, and only then lets a
> scheduler decide readiness. The three layers cannot see each other, which is
> why changing the scheduler cannot change results, and why an unchanged
> invocation is reused across processes rather than recomputed.

---

## Part 1 — The mechanism

### The three decorators, and the only one that spends

| You write | What it returns | When its body runs | What it costs |
| --- | --- | --- | --- |
| `@operation` | an immutable `Operation` (definition + body + signature) | at `submit`, on whatever substrate its placement names | the actual work |
| `@flow` | a `Flow` (a reusable planning strategy) | **once, at authoring time**, as ordinary Python | nothing |
| `@study` | a `StudyBuilder` — a *family* of studies | once, at authoring time; calling it plans | nothing |

`Study.submit()` is the only verb that spends anything
(`src/hedloom/study.py:114`).

### Planning is tracing, not evaluation

`@study` wraps the decorated function in `planned(...)`, so calling it opens a
`PlanDraft` context (`flow/src/hedloom_flow/authoring.py:603`), runs the body,
and freezes the result:

```
grid_refinement()       # StudyBuilder.__call__   src/hedloom/__init__.py:175
  └─ planned(fn)        # opens PlanDraft, runs body, calls _finish
       └─ Plan(...).validate()      # immutable, normalized IR
            └─ Study(plan, implementations)
```

Inside that context, calling an operation does **not** call the body. It goes to
`PlanDraft._call_operation` (`authoring.py:723`), which:

1. binds the arguments to the declared signature;
2. splits them — **`inputs`** are artifact handles and become dependency edges;
   **`config`** are literal values folded straight into identity with *no* edge
   (nothing has to run to produce a temperature);
3. resolves placement by precedence `call > operation default > plan default`
   (`resolve_policy`);
4. assigns the invocation ID (below);
5. appends an `Invocation` and its `DependencyEdge`s to the draft;
6. returns `InvocationResult` — a bag of `ArtifactValue` **handles**.

A handle is a reference with no value. `__bool__` and `__eq__` both raise
`HandleUsedAsValue` (`authoring.py:133`, `:138`), because answering either would
be a true statement about the reference and a silently false one about the
result. That refusal is what makes the next property enforceable rather than
merely conventional: **a flow body cannot branch on a result, because it never
has one.** So the traced graph is total, and the Plan predicts everything that
will run before anything is spent.

The whole draft is transactional — `_checkpoint()` / `_restore()` around each
call — so a rejected call leaves no partial invocation behind.

### `@flow` is a naming boundary, not a subgraph runtime

`_call_flow` (`authoring.py:871`) pushes a `FlowBoundary` onto a stack, runs the
body as plain Python, and pops it. The boundary does three things and nothing
else:

- **scopes keys** — uniqueness is enforced per `(boundary_id, key)`
  (`_reserve_key`, `authoring.py:1050`), so two points may each own an
  `integrate` without collision;
- **groups for display** — the visualizer nests by boundary;
- **carries outputs** — a flow's return value is named and lifted.

There is no runtime object for a flow. After planning, boundaries are metadata
on a flat list of invocations. This is why nesting flows costs nothing at
execution time.

### The identity chain — the part that actually matters

Three IDs, derived at three different layers, and each one is a pure function of
what precedes it.

**1. Invocation ID** — authored, at planning time
(`_keyed_plan_id`, `flow/src/hedloom_flow/model.py:82`):

| Situation | Invocation ID | Survives editing? |
| --- | --- | --- |
| `.named("compare")` | `invoke:key:<sha256(kind, scope, key)>` | yes |
| inside `sweep(..., key="key")` | same, key = `"<point>:<operation>"` | yes |
| plain call | `invoke:0001` — a positional counter | **no** |

The positional form is the trap. Insert one operation earlier in the file and
every later invocation renumbers, so every downstream result silently fails to
match and is recomputed — or worse, matches the wrong one. `sweep` exists to
make that unrepresentable without the author writing a key at every call.

**2. `sweep` itself** is eight lines (`authoring.py:454`):

```python
def sweep(items, key):
    resolve = key if callable(key) else (lambda item: str(item[key]))
    for item in items:
        token = _SWEEP_KEY.set(str(resolve(item)))
        try:
            yield item
        finally:
            _SWEEP_KEY.reset(token)
```

A `ContextVar` set per point. That choice is load-bearing: the key reaches calls
made *inside functions called from the loop body*, several frames down, without
being threaded through as a parameter — and the `finally` guarantees the scope
ends exactly at the point boundary. Any call inside that has not named itself
takes `<point>:<operation_name>`. Three operations across three points is nine
correct, stable keys from one `key=` argument.

Dependency edges get the same treatment: when both endpoints are keyed, the edge
ID is derived from the endpoints (`_stable_edge_id`), not from a counter.

**3. Input digest** — content, at binding time
(`plan_bundles`, `exec/src/hedloom_exec/planned.py:192`). For each invocation, in
deterministic topological order, a bundle is hashed:

| In the digest | Not in the digest |
| --- | --- |
| operation name + version | **placement** (`local` / `lsf` / `pooled`) |
| config arguments | `max_jobs`, queue, cores, memory |
| resolved input identities (`output:<producer-digest>:<name>`, `source:<digest>`) | which kernel ran it |
| declared output bindings (paths) | wall-clock, process, transport handle |
| **implementation fingerprint** — blake2b of the body's source | attempt sequence (that's separate) |
| the `shell` command, `identity_env` | |

Two consequences fall out of that split. Editing an operation's body reruns
every invocation of that operation and nothing else — no `version=` bump
required (`_implementation_of`, `authoring.py:420`). And moving an operation from
`local()` to `lsf()` reuses everything it already produced, because placement is
about *how* work runs, never *what it means*.

Digests chain: a producer's digest is part of its consumer's inputs, so one
changed point invalidates exactly its own downstream cone and nothing sideways.

**4. Attempt identity** — the durable name
(`exec/src/hedloom_exec/identity.py:60`):

```
attempt_identity(plan_id, invocation_id, sequence, input_digest)
    -> "hedloom-<blake2b-80bit>"
```

Chosen **before** a transport is asked to accept work, and used as both the
record directory and the LSF `-J` job name. That ordering is the whole point: a
submission whose receipt is lost can still be found afterwards by asking the
scheduler about a name that was computed, not returned. Finding a valid manifest
at this identity means the work was done with *exactly these inputs* — reuse
cannot be stale by construction.

### Execution — readiness is the only thing the kernel owns

`Study._run` (`study.py:175`) binds transports per placement, computes source
fingerprints, and hands the plain document to one of two kernels:

- `run_plan` — a sequential loop, no cluster, which is what keeps `distributed`
  an optional extra;
- `run_plan_graph` (`run/src/hedloom_run/graph.py`) — one Dask task per
  invocation, edges where outputs feed inputs.

The invariant the graph kernel is written against, stated in its own module
docstring:

> Changing which kernel decides readiness changes how long a plan takes and
> nothing else — the same results, under the same identities.

The cluster shape is unusual and deliberate: one in-process worker per
placement, each worker's thread count *derived* from that placement's declared
`max_jobs`, and **every** task annotated with its placement resource. Annotating
only the farm tasks would not work — an unannotated task is legal on every
worker and Dask will steal it onto whichever falls idle, so a local invocation
ends up holding a thread that was sized for a `bsub -I`. Annotating all of them
makes that unrepresentable.

A failed invocation returns a blocked outcome rather than raising, so one point
failing does not abandon the other forty-nine.

---

## Part 2 — Against other DAG builders

### The comparison

| | **Hedloom** | **Pure Dask** (`delayed`/futures) | **TensorFlow** (`tf.function`) | **LibreLane** | **CACE** |
| --- | --- | --- | --- | --- | --- |
| **Graph is built by** | tracing Python once, inside a `PlanDraft` | tracing Python once, into a task dict | tracing Python once per input signature, into a `GraphDef` | an ordered list of `Step` classes on a `Flow` | expanding a declarative YAML/text datasheet spec |
| **Unit of work** | `@operation` — declared inputs/config/outputs, a real body | any Python callable | a primitive op (not your function) | a `Step` — declared input/output design views | a *parameter*, with a testbench template |
| **Graph exists as** | plain-data document (`plan.to_data()`), inspectable, serializable | in-memory dict of tasks, keyed by token | serializable `GraphDef` / SavedModel | an ordered step list resolved at flow start | the condition cross-product, expanded at run |
| **Node identity** | authored key, hashed & scoped (`invoke:key:<sha256>`) | `funcname-<tokenize(args)>` when `pure=True` | op name + namescope, uniquified by **counter** | ordinal + step ID (`01-verilator-lint`) | parameter name + condition tuple |
| **Reuse key** | content digest over config, inputs, output bindings, **body source**, command | in-graph dedup by token; nothing durable | none (graph caching, not result caching) | resume by step ordinal (`--from` / `--last-run`) | none by identity; results are re-simulated |
| **Reuse across processes** | **yes** — content-addressed record on disk, with manifests | no — results live in worker memory | no | partial, by directory and explicit resume flag | no |
| **Edited body invalidates** | **yes** — source fingerprint is in the digest | n/a (nothing to invalidate) | retraces, but nothing to reuse anyway | no — a changed script does not invalidate a run dir | no |
| **Data-dependent branching** | **refused by construction** (handles raise on `__bool__`) | not in-graph; recompute and rebuild | yes — autograph lowers to `tf.cond`/`tf.while_loop` | steps are sequential; a step may act on prior metrics | no; the grid is declared |
| **Who decides readiness** | Dask, or a sequential loop — **swappable, no result change** | Dask scheduler | TF runtime + Grappler | the flow, in order | a local job queue |
| **Who decides placement** | the **Plan** (per-invocation policy), never the scheduler | scheduler, hinted by `resources=` / `workers=` | placer, with `tf.device` as a *soft* hint | the host it runs on | the host it runs on |
| **HPC / batch** | first-class: `lsf()` per invocation, `pooled()` for shared workers, job name = attempt identity | via `dask-jobqueue` (workers are batch jobs, tasks are not) | no | no | no |
| **Sweeps** | `sweep(points, key=...)` — a keyed scope, keys derived per point | a list comprehension; keys come from token hashing | vectorization / `vmap`-style batching | not a concept | **declared conditions grid** — its native idiom |
| **Failure** | blocked outcome, siblings continue, recorded durably | exception propagates; `gather` raises | graph execution aborts | flow aborts at the failing step | parameter marked failed, others continue |
| **Domain vocabulary** | **none** — generic operations; analog meaning is just another operation | none | tensors | digital implementation views (RTL→GDS) | **rich** — corners, limits, units, plots, datasheet |
| **What it will not own** | attempt records, identity, reuse, transports (each lives in a lower unit) | durability, provenance | anything outside the tensor program | scheduling, cache identity, provenance | execution substrate, provenance graph |

### Where each one is genuinely closest

**Pure Dask** is the nearest relative, and Hedloom uses it — for readiness only.
Dask's `tokenize` is real deterministic hashing, so the "content-addressed" idea
is not foreign to it. The difference is *scope and durability*: a Dask token
dedupes tasks **within one computation, in memory**. Restart the process and
everything recomputes. Hedloom's digest is over the plain-data document, written
to a durable record, so a second `submit` weeks later on a different host reuses
the same attempts. The second difference is authority: in Dask the scheduler
decides where a task runs and may steal it; in Hedloom the Plan has already
decided, and the kernel is forbidden from changing it. The graph kernel's
placement annotations exist precisely to take that authority back.

**TensorFlow graph mode** is the closest *conceptual* match — trace Python once,
get a static graph, execute it elsewhere. Two instructive divergences. TF names
ops by counter within a namescope, which is exactly the renumbering failure
`sweep` was built to prevent; TF gets away with it because its graphs are
rebuilt from scratch every time and nothing is reused across processes. And
autograph deliberately *does* lower `if`/`while` into graph ops — TF wanted
data-dependent control flow inside the graph. Hedloom refuses it, and says so in
`ONTOLOME.md`, naming `submit(retry=…, until=…)` as the tripwire where it would
arrive disguised as convenience. Both positions are coherent; they optimize for
different things. TF wants one graph to serve all inputs. Hedloom wants a graph
that can be *read* before it is paid for.

**LibreLane** is the strongest influence on discipline and the weakest on shape.
Its immutable validated configuration, explicit state transitions, declared step
inputs/outputs and isolated numbered step directories are all things Hedloom
does too. But a LibreLane flow is a *sequence*, not a DAG, and its resume story
is ordinal-based (`--from`, `--last-run`) rather than content-addressed —
changing a config value does not, by itself, tell you which later steps became
stale. That is fine for RTL→GDS, where the pipeline is fixed and long. It is the
wrong shape for a fifty-point sweep, where the interesting structure is *wide*
and the interesting question is "which points are still valid".

**CACE** is the complementary system rather than the competing one. Its native
idiom — declare parameters, declare a conditions grid, declare spec limits, get
a datasheet — is the thing Hedloom's `sweep` most directly resembles, and CACE's
version is *better at being analog*: it knows about units, corners, min/typ/max
limits, plots, and how to render a datasheet. Hedloom knows none of that, on
purpose. In Hedloom, "compare against spec limits" is just another
`@operation`, and the spec file is a declared source whose content fingerprint
is in the digest — which is why editing `spec_limits.json` in `../studies/ota_pvt.py`
reruns `evaluate-pvt` alone and reuses the other fifteen invocations. What CACE
does not have is the execution substrate: no per-invocation batch placement, no
content-addressed reuse, no durable attempt record. The repository's own
prior-art notes reach the same conclusion — CACE contributes vocabulary and an
interchange boundary, not a runtime
(`../../docs/vision/deferred-study-runtime-research.md`).

### The axis that actually separates them

Most of the table collapses into one question: **what names a node, and is that
name a function of the node's content?**

- Counter or ordinal (TensorFlow ops, LibreLane steps, an unkeyed Hedloom call):
  cheap, and fragile under editing. Fine when nothing is reused.
- Token over in-memory arguments (Dask `pure=True`): content-derived, but scoped
  to one process, so it buys deduplication and not resumption.
- Declared coordinates (CACE conditions): stable and readable, but says nothing
  about whether the *work* changed — only about which point it was.
- Scoped authored key **plus** a content digest that includes the body's source
  (Hedloom): stable under editing, and it changes exactly when the meaning of
  the work changes.

The cost of the last one is the constraint that makes it possible: the graph has
to be total before anything runs, so no operation may branch on a result. Every
other system in the table either pays a different price or declines the
guarantee.

---

## Reading order

- `docs/index.md` — the user guide, with runnable snippets
- `docs/internals/index.md` — how the package is assembled
- `ONTOLOME.md` — the contracts, and what this unit refuses to own
- `flow/src/hedloom_flow/authoring.py` — planning, keys, sweep
- `exec/src/hedloom_exec/planned.py` — the digest bundle
- `run/src/hedloom_run/graph.py` — readiness, and the cluster shape argument
