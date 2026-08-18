# Hedloom Run agent guidance

Inherit the project guidance from `../AGENTS.md`. Read `../MANIFESTO.md`, the
root `ONTOLOME.md`, this unit's `ONTOLOME.md`, and
`../docs/vision/open-concepts.md` before working here.

This unit owns plan traversal, readiness, value threading, and failure
handling. Keep attempt identity, journals, transports, reuse, and artifact
recording in `hedloom-exec`; keep authoring and Plan IR in `hedloom-flow`.

The standing constraint: do not add result-dependent control here. Branching on
a result, retrying with different inputs, or falling back to another operation
are open architectural questions, and the inquiry explicitly rejected hidden
imperative controllers. If a workload needs one, raise it rather than
implementing it.

## Where to read, and what to trust

`ONTOLOME.md` states this unit's contracts and is maintained; `docs/index.md`
is its published documentation and is maintained. The parent's `design/`
directory holds reviews and plans that are not — in particular
`design/concurrency-two-workers-2026-08-15.md` and
`design/pooled-placement-plan.md`, both of which this unit's source still cites
by path for their *reasoning*, never for their description of the code.

Two invariants above everything else here: both kernels must agree about a
plan, and `cluster_for(site)` is the only supported way to build a cluster this
kernel accepts — the capacity a worker declares and the placement a task asks
for come from one reading of the profile, so they cannot drift apart.
