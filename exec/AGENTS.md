# Hedloom Exec agent guidance

Inherit the project guidance from `../AGENTS.md`. Before work here, read
`../MANIFESTO.md`, `../ONTOLOME.md`, local `ONTOLOME.md`, local `README.md`,
local `unit.toml`, and `DECISIONS.md`, then inspect the implementation and
tests.

This unit owns one attempt at one planned invocation: identity chosen before
submission, an append-only durable record, atomic terminal publication, and
reconciliation. Keep graph readiness, successor release, retries, replanning,
policy resolution, artifact storage, and the study lifecycle outside this
boundary.

Two ordering rules carry the recovery argument and must not be weakened:
submission intent is durably flushed before any transport call, and the
terminal record is written only after the manifest is atomically visible. If a
change makes either ordering inconvenient, that is evidence about the design,
not a reason to reorder them.

Prefer failing loudly over guessing. `UnrecoverableAttempt` is a supported
result. Update the local ontology when the unit's being changes.

## Incompleteness may refuse; it may not be silently wrong

A partial implementation is expected here. A partial implementation that
returns a plausible but possibly false answer is not. `LSFPooledTransport`
raising `NotImplementedError` is the correct shape for something unfinished.
Shipping reuse that ignored input identity was the wrong shape: it answered,
and the answer could be stale. Recording that in the ontology did not make it
safe, because a caller reads the return value, not the ontology.

When a surface cannot yet be correct, make it decline. Document the limitation
*and* close the path.

## State the invariant before writing the surface

One sentence, before the code: what must be true for this to be right? "Reuse
is correct iff identity implies inputs." "An attempt may be claimed only if
nothing was accepted before." If the sentence cannot be written, the design is
not understood yet, and writing it is cheaper than discovering that later.

This replaces the heavier work-order ceremony, whose retirement is recorded in
the unpublished `design/development-process-2026-08-03.md`. It is meant to cost
seconds. Reasoning depth per step is not
what has caught real errors in this project — contact with the requirement and
with running code is — so keep the loop short rather than making each step
more elaborate.

## Where to read, and what to trust

`ONTOLOME.md` is this unit's ongoing self-study, including commitments, evidence,
assumptions, and open questions. Refine it when work yields useful insight;
update commitments explicitly when they change. It is repo-native;
`DECISIONS.md` is a published, living ledger of what is settled, what is open,
and what would change our minds. Both are maintained against code and use,
keeping observations distinguishable from commitments.
`design/` is not maintained and not published — it records how the unit was
built on a date. Do not cite it as evidence of current behaviour.

One name collision worth holding: this unit's `LSFPooledTransport` is a refusing
boundary and stays one. The pooled implementation is
`hedloom_run.pooled.LSFPooledTransport`, in a different unit, for a reason.
