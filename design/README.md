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
| `architecture-review-2026-08-14.md` | A review of the tree before the first farm run, written to be answered inline. | Answered; the answers became `implementation-plan-2026-08-16.md`. Some `**Your call**` slots are still blank. |
| `concurrency-two-workers-2026-08-15.md` | Companion to points 6 and 9 above: what two in-process workers buy and what they still lack. | Superseded by the shipped per-placement cluster (`cluster_for`). |
| `dask-usage-review-2026-08-16.md` | "Are we using Dask correctly, and what are we leaving on the table?" | Findings implemented; see `docs/internals/dask-scheduling-rules.md` for the rules that survived. |
| `implementation-plan-2026-08-16.md` | Six work packages turned from the review's answers into instructions for agents. | Delivered. Read it as a record of intent, never as a backlog. |
| `pooled-placement-plan.md` | The plan for pooled LSF placement via `dask_jobqueue.LSFCluster`. | Implemented — `hedloom_run.pooled`, `kind = "lsf-pooled"`. |
| `reading-before-the-farm-2026-08-16.md` | A reading order over `58d0764..HEAD`, by what reaches the farm. | Spent. The commit range it names is long behind `HEAD`. The part worth keeping became `docs/guide/first-farm-run.md`. |
| `cancellation-plan.md` | How a sweep would be stopped, if it needed stopping. | **Live proposal, deliberately not built.** Killing the process is still the answer. |
| `binding-the-attempt-identity.md` | Resolve an attempt's identity in `binding.py` before submitting, so a kernel can ask the record rather than a future. | **Live proposal, not implemented.** Comes out of the two model-checked protocols. |

## The rule

A file here is never edited to stay true. If something in it is now wrong,
that is expected — it was written before the change. If something in it is
still *right and load-bearing*, it belongs in `docs/` or in an `ONTOLOME.md`,
not here.
