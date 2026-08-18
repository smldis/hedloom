# Hedloom Exec: how this unit was developed, and what two errors taught

> **Archived working material, not published and not maintained.** These
> sections were moved out of `DECISIONS.md` because they record how the unit
> was built on a particular date rather than what it now guarantees. Their
> surviving conclusions are in the ledger's *Settled by user direction* and
> *Open* sections, and in `../AGENTS.md`.

## Recorded process revision (2026-08-03)

The graduated main adopted allocation policy 3, "a reviewed evidence work
order," as the default while the architecture was provisional, and explicitly
made that policy falsifiable: *"Reassess it when repeated reviews add ceremony
without changing scope."*

**Observation.** Across Hedloom Flow Phases 1–5 the policy produced roughly 1,700
lines of governance around 3,171 lines of source, a paired plan commit and
feature commit per phase, and an independent reviewer pass per phase, while the
component still could not execute a single operation. The ceremony grew; the
scope per slice did not.

**Revision.** Direct human-reviewed development against this ledger, with
review at natural boundaries rather than per phase. What is retained from the
prior policy, because it was the part that worked: falsifiable framing, named
discriminating observations, honest ontologies, and the refusal to let passing
tests silently graduate architecture. What is dropped: work-order identities,
authorization records, stop-condition recitals, and delegated review panes.

This revision is recorded rather than drifted into, as the main requires.

### Amendment (2026-08-03): what the two real errors taught

Two substantive errors have occurred in this line of work, with opposite
shapes, and neither is explained by "not enough reasoning per step".

*The detached-lifetime premise* — that an accepted job outlives its submitter —
was produced by the heavy dialectic workflow, argued at length in the graduated
main, and survived reviewer passes. It was falsified by one sentence of user
direction. Depth did not catch it; contact with the requirement did.

*The unsound reuse default* — `RECORDED` returning results computed from
different inputs — was produced by the light workflow, flagged in the same
message that shipped it, recorded in the ontology, and fixed two turns later.
The reasoning happened; the shipping decision was wrong.

The corrective is therefore not more ceremony, and not deeper per-step review.
It is two cheap rules, now in `AGENTS.md`: incompleteness must refuse rather
than answer wrongly, and the invariant gets stated in one sentence before the
surface is written. Adversarial review is reserved for finished slices, where
it finds things, rather than applied per increment, where it mostly restates
what the increment already claims about itself.

## Premise correction (2026-08-03, user direction)

The unit was built on the architecture's lifetime asymmetry: an accepted batch
job outlives the process that submitted it, so a transient handle cannot own
its identity. **The user has stated the opposite as the design intent — a job
is not supposed to outlive its owner, and a caller crash should take the work
down with it.**

This removes the premise of the graduated main's provisional decision that "an
attempt protocol owns LSF." With owner-bound lifetime, the unsafe transition
that argument was built to survive becomes a defect to prevent rather than a
state to reconcile, and Dask owning the lifecycle is no longer unsound on
lifetime grounds.

**What the failure mode becomes.** Duplicate prevention is replaced by orphan
prevention. The indeterminate-submission window still exists and still matters,
but the correct response inverts: from "refuse to guess, wait to attach" to
"discover it and kill it." Lookup by a pre-chosen unique identity is therefore
still a required site capability, used for reaping rather than attaching, and
identity uniqueness matters more than before because the action it enables is
destructive.

**What enforces it.** An expectation is not a mechanism; unenforced owner-bound
lifetime is how orphans happen. `dask-jobqueue` already implements this for
pooled workers via `--death-timeout` plus `bkill` on cluster close, so pooled
mode should adopt it rather than rebuild it. Direct mode should borrow the same
discipline. Enforcement must not depend on `bsub -I`: the manifesto forbids
authority living in an interactive session, and a lease works identically for a
terminal, a script, CI, or an agent.

**What "resume" means here.** Not reattaching to running work, which no longer
exists. The manifesto's actual requirement is to rerun from a clean environment
without repeating results whose inputs remain valid. That is result reuse and
staleness, and it is the durable record's real purpose in this design —
evidence and reuse, not recovery.

**Consequence for this unit.** Attempt identity, the append-only journal, atomic
terminal publication, the refused/indeterminate distinction, and reconciliation
all survive with changed justifications. The attach disposition and
`UnrecoverableAttempt` are demoted: they are correct only for a transport that
declares detached lifetime, and no such transport exists or is currently wanted.
They are retained, unreachable by default, rather than deleted, because the
distinction they encode is what makes the orphan-reaping path expressible.

## Review findings and their resolution (2026-08-03)

An adversarial review at `xhigh` over the eight unpushed commits returned 15
findings; all are addressed. The distribution is the useful part: five in
`lsf.py`, none at all in `planned.py`, `reuse.py`, `identity.py`, or
`transport.py`.

That is not chance. `lsf.py` was written against a fake this unit also wrote,
from the same assumptions, so the fake agreed with the code's
misunderstandings — `discover()` returned a handle shaped unlike `submit()`'s,
and no test ever polled a discovered handle. The same pattern produced the
worst finding: `FakeBatchStore.accept()` reset its own run counter, so the
no-duplication assertions in the decisive failure injections could not fail,
and this ledger cited them as evidence. Where feedback came from outside — a
Plan document Hedloom Flow really produced, real process signals — the code was
clean.

**The standing lesson:** a fake authored alongside the code inherits its blind
spots. Prefer evidence from something the unit did not write.
