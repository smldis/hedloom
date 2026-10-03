# Hedloom Run agent guidance

Inherit the project guidance from `../AGENTS.md`. Read the containing ASS
`../../MANIFESTO.md` and `../../ONTOLOME.md`,
`../ONTOLOME.md`, this unit's `ONTOLOME.md`, and
`../../docs/vision/open-concepts.md` before working here.

This unit owns plan traversal, readiness, value threading, and failure
handling. Keep attempt identity, journals, transports, reuse, and artifact
recording in `hedloom-exec`; keep authoring and Plan IR in `hedloom-flow`.

The standing constraint: do not add result-dependent control here. Branching on
a result, retrying with different inputs, or falling back to another operation
are open architectural questions, and the inquiry explicitly rejected hidden
imperative controllers. If a workload needs one, raise it rather than
implementing it.

## Where to read, and what to trust

`ONTOLOME.md` is this unit's ongoing self-study, including commitments, evidence,
assumptions, and open questions. Refine it when work yields useful insight;
update commitments explicitly when they change. `docs/index.md` is its published
documentation and is maintained. The parent's `design/`
directory holds reviews and plans that are not — in particular
`design/concurrency-two-workers-2026-08-15.md` and
`design/pooled-placement-plan.md`, both of which this unit's source still cites
by path for their *reasoning*, never for their description of the code.

The async controller is the only readiness path. Keep selected-artifact
identity, per-consumer evidence, durable binding before dispatch, and the
no-replay entry gate intact. Dask receives only ready invocations. Controller
ownership is loop-local; offload filesystem and serialization work rather than
blocking that loop. Preparation windows derive from placement capacity; do not
reinstate an arbitrary active-Run admission cap or worker-held nested waits.

Cluster construction and task admission read one Site capacity declaration.
Scheduling priority is execution metadata, never computation identity or farm
queue policy. Actual-farm and local/fake-farm guarantees must remain distinct.
