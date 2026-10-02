# Refusals you will actually meet

This project treats "silently doing something reasonable" as a defect, so a
surprising amount of the surface is refusals. Each of these is telling you
something specific, and none of them is a bug report.

| What you see | What it means |
| --- | --- |
| `HandleUsedAsValue` | You read a planning handle as a value. There is nothing there yet — a plan is built before anything runs. See [handles](authoring.md#handles-are-references-never-values). |
| `'x' is a family of studies, not one` | Call the decorated function first: build `x(...)`, then pass that subject to `live.submit`. |
| `AttributeError` on `out.<name>` | This operation never declared a file output by that name. |
| `UnsupportedPlacement` (per invocation) | No transport provides the placement this invocation asked for. Deliberately fatal rather than run elsewhere. |
| `UnsupportedPlacement` (before anything runs) | The cluster declares no capacity for a placement the plan uses. Use a Runtime constructed from the intended Site. |
| nested-submission refusal | An operation attempted Runtime submission or waiting for a child Plan. Move staging to the caller; worker-held nesting is deferred. |
| `SiteError: placement 'x' declares no max_jobs` | An LSF placement needs its budget stated. There is [no safe default](sites.md#the-two-numbers-which-are-about-two-different-machines). |
| `SiteError: placement 'x' declares unknown option 'queeu'` | A typo'd or unrepresentable placement option, named by placement *and* key. Never a bare `TypeError`. A pooled placement has a [narrower vocabulary](sites.md#kind--lsf-pooled--a-shared-set-of-workers) than a direct one. |
| `SiteError: placement 'x' names an unknown kind` | A site builds `lsf-interactive` and `lsf-pooled` from configuration; `in-process` must be given its implementations. |
| `SiteError: a run may override ... nowhere` | An [override](running.md#overrides-and-local-debugging) tried to reach something that changes what a run *means*. |
| missing Dask/distributed dependency | Async execution requires the Dask extra; install it in the project environment. There is no sequential fallback. |
| `RunFailed` | `receipt.result()` reached unsuccessful lifecycle completion. Use `wait()` to inspect terminal state and errors without success-only raising. |
| `ConcurrentClaim` | An independent owner holds this attempt. It refuses rather than silently joining; independent branches continue only with `stop_on_failure=False`. |
| `UnrecoverableAttempt` | A substrate that cannot say whether it accepted work. **A supported outcome, not a bug** — guessing here is what produces duplicate farm jobs. |
| `RefusedComputation` | Something tried to compute a visualization stand-in. Runtime submission is the execution surface. |
| a transport refusing an option before submission | Dropping a stated resource need would run the work under conditions nobody asked for. |
