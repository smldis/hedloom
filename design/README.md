# Design record — working material, not documentation

Nothing in this directory is published. `composition.py docs` stages only
`hedloom/docs/`, so these files never reach the Sphinx site, and that is the
point: they are correspondence and working notes, written on a date, about a
tree that has since moved. **They describe what was decided, not what the code
now does.** For what the code now does, read `docs/`.

Kept in the repository rather than deleted because the argument behind a
boundary outlives the boundary, and because two of these are still live
proposals someone may pick up. The durable conclusions are also in the project
knowledge graph, which is where to ask "why is it like this?" without reading
seven dated files.

## What is here

| File | What it is | Status |
| --- | --- | --- |
| `interactive-execution-redesign-2026-09-29.md` | A replacement concept for staggered interactive submissions with explicit resource ownership. | **Adopted and implemented, 2026-10-01.** Dated concept retained; Runtime owns a background loop, Dask executes ready work, and automatic ownership replaces the proposed mandatory resource ceremony. |
| `interactive-execution-internals-census-2026-09-29.md` | Source-checked execution, resource, evidence, and consumer census at `e9dbe70`, with targeted local probes and documentation contradictions. | **Investigation complete.** Describes the pre-refactor baseline and evidence limits at its recorded revisions. |
| `interactive-execution-proposal-and-plan-2026-09-29.md` | Concrete Runtime/Run proposal, decision table, bounded async/Dask integration, lifecycle and storage contracts, and ordered replacement/retirement plan. | **Implemented with reviewed revisions, 2026-10-01.** Dated proposal retained; fixed admission caps and mandatory manual close were not adopted. Current contracts live in docs and ontologies. |
| `interactive-execution-usability-review-2026-10-01.md` | Responses to the proposal's ovnotes and review of automatic lifetime, admission, priorities, deferred nesting, compatibility, and local Jobqueue evidence. | **Review complete; implementation follows.** Retains ovnote responses and the evidence available at review time; see the implementation work plan for later scope and checks. |
| `async-refactor-work-plan-2026-10-01.md` | Autonomous implementation scope, work ownership, checkpoints and final verification evidence. | **Complete.** New async runtime and maintained consumers implemented; local/fake-farm/compatibility/doc checks pass with stated limits. |
| `architecture-review-2026-08-14.md` | A review of the tree before the first farm run, written to be answered inline. | Answered; the answers became `implementation-plan-2026-08-16.md`. Some `**Your call**` slots are still blank. |
| `concurrency-two-workers-2026-08-15.md` | Companion to points 6 and 9 above: what two in-process workers buy and what they still lack. | Superseded by the shipped per-placement cluster (`cluster_for`). |
| `dask-usage-review-2026-08-16.md` | "Are we using Dask correctly, and what are we leaving on the table?" | Findings implemented; see `docs/internals/dask-scheduling-rules.md` for the rules that survived. |
| `implementation-plan-2026-08-16.md` | Six work packages turned from the review's answers into instructions for agents. | Delivered. Read it as a record of intent, never as a backlog. |
| `pooled-placement-plan.md` | The plan for pooled LSF placement via `dask_jobqueue.LSFCluster`. | Implemented — `hedloom_run.pooled`, `kind = "lsf-pooled"`. |
| `reading-before-the-farm-2026-08-16.md` | A reading order over `58d0764..HEAD`, by what reaches the farm. | Spent. The commit range it names is long behind `HEAD`. The part worth keeping became `docs/guide/first-farm-run.md`. |
| `cancellation-plan.md` | How a sweep would be stopped, if it needed stopping. | **Superseded by async withdrawal and targeted pooled interruption.** `Run.stop()` stops new admission and drains entered work; `stop(force=True)` can interrupt solely owned pooled commands by restarting their nanny-managed worker. The dated proposal is retained. |
| `binding-the-attempt-identity.md` | Resolve an attempt's identity in `binding.py` before submitting, so a kernel can ask the record rather than a future. | **Live proposal, not implemented.** Comes out of the two model-checked protocols. |
| `reclaiming-produced-files-2026-08-26.md` | Reclaiming storage from produced files: a `reclaimed` journal event, safety tiers, a plan generation record, and site-owned retention. | **Live proposal, not implemented.** Its first work package is a reuse defect fix that stands on its own. |
| `attempt-record-census-2026-08-26.md` | Census of what a per-invocation attempt record (sequence out of the identity, onto the workspace) would touch: coupling sites, silent breaks, test baseline, contract surfaces. | **Live — a measurement, not a proposal.** Numbers are from its date; rerun before acting. |
| `attempt-record-and-collector-plan-2026-08-27.md` | Plan for the per-invocation attempt record plus an operator-run storage collector: phases, the `hedloom collect`/`pin` API, retention rules, and a test list written before the code. | **Live proposal, not implemented.** Four decisions in Part 5 block Phase 1. |
| `iterating-on-a-study-2026-08-27.md` | Editing an input mid-development moves the attempt identity, so the path moves and orphans accumulate. Four options; the recommended one records *which* identity key changed. | **Live proposal, awaiting review.** |
| `rearchitecting-nested-studies-2026-09-04.md` | Asks whether nested submission is the right primitive or a workaround made safe: the costs it has accumulated, four directions a re-architecture could take, and what to check before deciding. | **Deferred hierarchical composition.** Worker-held nesting was retired in the async replacement; current callers stage Runs. The future need remains open. |
| `nested-submission-and-capacity-2026-08-30.md` | An invocation that submits a further Plan blocks while holding one unit of its own placement, which can deadlock the inner run. Records the measurements behind `secede`, `worker_client`, resource donation and the configuration answer. | **Historical implementation retired.** The old capacity/headroom refusal was replaced by an explicit nesting refusal; donation was never adopted. |

## The rule

A file here is never edited to stay true. If something in it is now wrong,
that is expected — it was written before the change. If something in it is
still *right and load-bearing*, it belongs in `docs/` or in an `ONTOLOME.md`,
not here.
