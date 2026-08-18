# Hedloom Flow

Hedloom Flow is the independently installable prototype for Python-native static
operation and flow planning described in [`PLANNING.md`](PLANNING.md). It
captures immutable definitions, explicit dependencies, nested flow boundaries,
ordered collection fan-in, scoped authored keys, and deterministic,
JSON-inspectable Plan IR without executing operation bodies. A Plan can also
declare an opaque address for an external source it reads but does not
produce. Source references are classified as `artifact`; operation outputs
remain `ephemeral`.

Plans are emitted at **schema 3**, and a `Plan` refuses any other
`schema_version` rather than validating a document it does not describe.
`hedloom_exec.plan_bundles` reads schema 2 and 3, so a record written from an
earlier document is still derivable.

Use `address(...)` with `input_artifact(..., artifact=...)` to record that
data-only source handoff. A source is its address and its artifact contract,
and nothing else: anything further would be a claim about bytes this unit
cannot open, so it is the run's to establish, not the Plan's to assert.

Use `artifacts(kind)` for a required, non-empty ordered collection input. The
normalized binding preserves member order and emits one positioned dependency
edge per member. Use `.named("...")` on operation and flow calls when a
call needs explicit identity within its containing flow boundary. Names may be
reused in distinct scopes, but operation and flow calls share one namespace
inside any one scope.

A plan is authored by a function: decorate it with `@planned` and calling it
returns one finished `Plan`, with the return value naming the plan's outputs
exactly as a `@flow`'s does. `plan()` remains available as an explicit draft for
a plan assembled across several strategies; it must be closed before `finish`
freezes it, which is the ordering `@planned` exists to remove.

Names identify Plan nodes only. They are not cache or scheduler keys, attempt or
runtime identities, or sequential slots. Cross-edit stability requires every
relevant enclosing boundary and endpoint to be keyed: unkeyed boundaries and
calls, external sources, and fallback edges retain deterministic authored-order
IDs that can change when earlier work is inserted.

Install the base planning package and run its planning-only evidence from this
directory with:

```console
python -m pip install -e .
PYTHONPATH=src python examples/refinement.py | python -m json.tool
```

The base distribution keeps `dependencies = []`: `import hedloom_flow` and the
planning-only refinement command do not require Dask. To inspect the
non-reexported local lowering experiment, select the exact optional dependency
and run its tool-free example explicitly:

```console
python -m pip install -e '.[dask]'
python -m pytest -q
PYTHONPATH=src:. python examples/local_dask_refinement.py \
  | python -m json.tool
```

That example reuses the public refinement Plan, binds its two operation
identities to explicit callables, injects the one already-decoded source value,
and asks Dask to compute synchronously with optimization disabled. Its canonical
stdout contains semantic results and stable Plan metadata, not the fresh Dask
task namespace. The verdict visibly retains the authored `coarse`, `medium`,
`fine` collection order, and its three estimates are the ones
`../examples/grid_refinement.py` gets from real `awk`.

Flow bodies are ordinary authored Python used to construct a static plan;
their freedom from side effects is an authoring discipline. `submit(...)` still
refuses execution. The explicit `hedloom_flow.experimental.local_dask` instrument
only constructs Dask Delayed values; it does not compute, submit, choose or
enforce a scheduler/placement, or provide a general execution API. Public Dask
execution, Distributed/Futures, LSF, retries, persistence, recovery, plugins,
dynamic replanning, production hardening, and result-dependent replanning are
outside this unit. Address resolution, actual access checks, publication,
materialized operation outputs, and runtime artifact values are also outside
it. The archived sequential-flow convenience is inactive
historical material, not an active API or backlog.

See [`ONTOLOME.md`](ONTOLOME.md) for the owned boundary,
[`docs/architecture.md`](docs/architecture.md) for the graduated architecture
adapted to current development status, [`docs/index.md`](docs/index.md) for the
component documentation entry point, and
[`IMPLEMENTATION.md`](IMPLEMENTATION.md) for current evidence and limitations.
