# Hedloom Flow

Hedloom Flow provides a Python-native, executor-neutral planning boundary. Authored
operation and flow calls inside `plan(...)` become one immutable normalized
graph whose bindings, artifact dependencies, nested flow boundaries, policies,
outputs, and canonical JSON can be inspected before runtime work exists.

Collection inputs declared with `artifacts(kind)` are required, non-empty, and
ordered. The Plan records their artifact references in authored order and one
positioned dependency edge per member. Optional operation and flow call keys
share one namespace within each containing boundary. A fully keyed subgraph has
stable scoped invocation, boundary, and connecting-edge IDs across unrelated
earlier insertions.

That stability is deliberately precise rather than global. A keyed call inside
an unkeyed boundary depends on the boundary's authored-order ID. External
sources, unkeyed calls and boundaries, and fallback edges involving an external
source or unkeyed endpoint can likewise be renumbered by earlier authored work.
Keys are Plan identity only, never cache keys, scheduler keys, attempts, runtime
identity, or sequential slots.

The planning package deliberately has no executor authority. Operation bodies
are not called during planning; `submit(...)` refuses execution. Flow bodies do
run as ordinary Python to author the static graph, so avoiding external side
effects inside them remains an authoring responsibility rather than an enforced
property.

Calling an operation or a flow returns a handle — `ArtifactValue` or
`InvocationResult` — never the value it will eventually stand for. Both
refuse `bool()` and `==` by raising `HandleUsedAsValue` (also a `TypeError`)
rather than answering: every available answer would be about the reference,
not the result, since no invocation has run yet when a flow body executes.
This is also what keeps a flow body honestly unable to branch on a result —
there is nothing yet to branch on.

The composition-root unit `hedloom` builds on this package to add the piece it
deliberately excludes: a `submit` that actually runs the operations a Plan
names, by pairing this package's Plan with `hedloom_run`'s kernels and
`hedloom_exec`'s durable record. See `hedloom`'s own documentation for that.

## Experimental local lowering evidence

The non-reexported `hedloom_flow.experimental.local_dask` module is a bounded
instrument for testing whether Plan IR lowers to Dask Delayed. Install the
optional dependency with `python -m pip install -e '.[dask]'`, then run
`PYTHONPATH=src python examples/local_dask_characterization.py`. The
[experimental characterization example](../examples/local_dask_characterization.py)
reuses the public [planning example](../examples/characterization.py), injects
decoded source data, binds exact operation identities to explicit callables,
and prints deterministic semantic results without Dask keys. It is runnable
evidence, not a convenience layer or a working `submit(...)` path.

The instrument does not compute, enforce local placement, schedule, persist,
cancel, publish, resolve source addresses, or execute codecs. Public/general
execution, Distributed/Futures, LSF, retries, persistence, recovery, plugins,
dynamic replanning, production hardening, and sequential convenience remain
excluded.

The focused tests, planning-only characterization, and explicit local Dask
characterization command are the current evidence for this prototype boundary.

The unit's development history — phase trackers `PLANNING.md` and
`IMPLEMENTATION.md`, and the inactive sequential-convenience design in
`design/` — is kept in the repository and deliberately not published. It records
what each phase delivered, including contracts that have since changed, so it is
not a description of this unit and not a backlog. `ONTOLOME.md` states the
current contracts.

```{toctree}
:maxdepth: 1
:caption: Architecture

architecture
```
