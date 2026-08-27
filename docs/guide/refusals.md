# Refusals you will actually meet

This project treats "silently doing something reasonable" as a defect, so a
surprising amount of the surface is refusals. Each of these is telling you
something specific, and none of them is a bug report.

| What you see | What it means |
| --- | --- |
| `HandleUsedAsValue` | You read a planning handle as a value. There is nothing there yet — a plan is built before anything runs. See [handles](authoring.md#handles-are-references-never-values). |
| `'x' is a family of studies, not one` | Call the decorated function first: `x(...).submit`, not `x.submit`. |
| `AttributeError` on `out.<name>` | This operation never declared a file output by that name. |
| `UnsupportedPlacement` (per invocation) | No transport provides the placement this invocation asked for. Deliberately fatal rather than run elsewhere. |
| `UnsupportedPlacement` (before anything runs) | The cluster declares no capacity for a placement the plan uses. Build it with `cluster_for(site)`, or just let `submit` do it. |
| `SiteError: placement 'x' declares no max_jobs` | An LSF placement needs its budget stated. There is [no safe default](sites.md#the-two-numbers-which-are-about-two-different-machines). |
| `SiteError: placement 'x' declares unknown option 'queeu'` | A typo'd or unrepresentable placement option, named by placement *and* key. Never a bare `TypeError`. A pooled placement has a [narrower vocabulary](sites.md#kind--lsf-pooled--a-shared-set-of-workers) than a direct one. |
| `SiteError: placement 'x' names an unknown kind` | A site builds `lsf-interactive` and `lsf-pooled` from configuration; `in-process` must be given its implementations. |
| `SiteError: a run may override ... nowhere` | An [override](running.md#running-less-or-running-elsewhere-override) tried to reach something that changes what a run *means*. |
| `SiteError: this session needs a scheduler` | The site declares real capacity and `distributed` is missing. Install the extra, or ask for `sequential=True`. |
| `ConcurrentClaim` | Another caller holds this attempt — usually the same study still running. Reported as one refused invocation; the rest of the sweep continues. |
| `UnrecoverableAttempt` | A substrate that cannot say whether it accepted work. **A supported outcome, not a bug** — guessing here is what produces duplicate farm jobs. |
| `RefusedComputation` | Something tried to compute a visualization stand-in. `submit()` is the only way a study runs. |
| a transport refusing an option before submission | Dropping a stated resource need would run the work under conditions nobody asked for. |
