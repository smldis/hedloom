# Nested studies: the working implementation before async replacement

Written 2026-10-02 at the user's request, to preserve a recoverable reference
when hierarchical study execution is needed again. **Historical implementation,
currently retired.** This records working code and bounded evidence; it does not
authorize restoring the mechanism or make the old API supported today.

## Recoverable revisions

The last pre-refactor baseline used here is Hedloom
`e9dbe70e3371cc789665a2ffa05e05bb38e69a64`, with ASS consumers at
`7f1616b01316b9c226680ecff35f0968e8a98f00`.

| Historical source | What to recover |
| --- | --- |
| [Hedloom nested example](https://github.com/smldis/hedloom/blob/e9dbe70e3371cc789665a2ffa05e05bb38e69a64/examples/nested_studies.py) and [state module](https://github.com/smldis/hedloom/blob/e9dbe70e3371cc789665a2ffa05e05bb38e69a64/examples/nested_studies_state.py) | A wrapper operation submits an independently authored child study through the already open Session. |
| [Example tests](https://github.com/smldis/hedloom/blob/e9dbe70e3371cc789665a2ffa05e05bb38e69a64/tests/test_nested_studies_example.py) | Shared Session, child reuse and invalidation, both kernels, and fresh-interpreter script execution. |
| [Session](https://github.com/smldis/hedloom/blob/e9dbe70e3371cc789665a2ffa05e05bb38e69a64/src/hedloom/session.py) | Blocking submission with the existing client, execution owner and Site. |
| [Graph kernel](https://github.com/smldis/hedloom/blob/e9dbe70e3371cc789665a2ffa05e05bb38e69a64/run/src/hedloom_run/graph.py) and [kernel tests](https://github.com/smldis/hedloom/blob/e9dbe70e3371cc789665a2ffa05e05bb38e69a64/run/tests/test_graph.py) | Occupancy tracking, blocked-unit counting and `NestedCapacityExhausted`. |
| [ASS nested consumer](https://github.com/smldis/analog-sim-studies/blob/7f1616b01316b9c226680ecff35f0968e8a98f00/studies/ota_pvt_clean_nested.py) | Runtime-derived corner fan-out, a saved child Plan, separate child records and the sequential-child workaround. |

## Working approach A: submit through the shared Session

The operator opened one `session(site)` and put its live reference in an
imported state module. An outer operation authored a child Study from values
available during execution, then called that Session's blocking `submit` and
returned selected child results. The outer Plan showed the wrapper; the child
Plan was separately authored and recorded when the wrapper ran. Each Plan was
static before its own execution, although the outer Plan did not expose the
complete eventual hierarchy.

The word-analysis example declared the wrapper `execution="each_submission"`.
It therefore entered on every outer submission, while child operations retained
ordinary computation identity and reuse. Identical text reused both child
operations; changed text recomputed them. Reusing a normal wrapper could instead
skip the child submission entirely, so wrapper freshness was an explicit choice.

The Session mattered: using `child.submit(site=...)` would open another Session,
with another cluster and independently scoped capacity. A Site shared storage
and configuration; it did not share live resources. The example test explicitly
refused new cluster construction after opening its Session.

The imported `nested_studies_state.SESSION` was essential example wiring.
Cloudpickle serializes `__main__` functions by value, including referenced
globals; a directly referenced Session contained locks that could not be
serialized. Importing the state module let in-process workers reach the existing
object by reference. This was a process-local technique, not a way to send the
Session to another process or a farm host. The caller cleared the reference in
`finally` before leaving the Session context.

## Capacity and the guard that made this usable

The parent wrapper held a placement resource while blocking on its child. In
the demonstrated graph case, `local: 2` left one slot for child work. With
`local: 1`, a parent and child needing that same placement could not progress.

The graph kernel recorded each body thread's placement in `_OCCUPANCY`.
`_waiting_on_nested_run` counted blocked holders in `_BLOCKED_UNITS`, protected
by a lock. `_require_nesting_headroom` compared those counts with declared
placement resources before dispatching the child. It raised
`NestedCapacityExhausted` when blocked holders consumed all capacity required by
the child; one blocked holder against capacity two was admitted. The counters
were process-global because the readiness workers were threads in one process,
and sibling waiters mattered as well as ancestors. This was a preflight guard,
not a general deadlock detector for arbitrary distributed nesting.

A separately configured coordinator placement could also leave child capacity
free. Merely freeing a Dask thread with `secede()` or `worker_client()` did not
release the placement resource. The August probes showed that unannotated,
worker-pinned children could bypass this resource gate, but also exceed the
declared budget. Counted resource donation was investigated and **never shipped**.
See [the capacity investigation](nested-submission-and-capacity-2026-08-30.md).

## Working approach B: execute the child sequentially

The ASS consumer's `run_corner_study` operation, version 2, authored its corner
Plan from discovered jobs, wrote `corner-plan.json`, then called
`inner.submit(site=site, sequential=True, ...)`. This avoided starting a child
graph scheduler while the outer wrapper held the sole local slot. The sequential
driver recursed without the graph kernel's placement-resource gate.

Child preparation, simulation and measurement still had individual records and
reuse. Child records/workspaces lived outside the parent's attempt workspace,
so changing the wrapper did not erase prior child evidence. However, their paths
were supplied as operation configuration, making installation paths part of the
wrapper identity. It was a local serial workaround, not evidence of parallel or
farm nesting. The two kernels also differed in their nesting admission behavior.

## Why it was retired, and what to retain

The async replacement removed blocking Session submission and the sequential
kernel, and the user accepted deferring worker-held nesting. Copying the old
wrapper into today's Runtime would retain the capacity dependency and would hit
the explicit operation-body nesting refusal. Current examples and ASS consumers
stage child Runs from the caller, including with capacity one.

The useful requirement remains: integrate an independently authored study whose
inputs or child Plan become available during execution, while retaining child
identity, reuse, evidence and understandable ownership. Preserve that requirement
when revisiting hierarchy; the old mechanism is a reference, not a settled design.

The August investigation's freshness motivation also predates later artifact
identity support. Already at the baseline above, `nested_studies.py` directed
freshness/output-identity use cases to the single-Plan `live_source.py` example.
Recheck the actual use case before assuming that fetching fresh data needs nesting.

A future design should first compare explicit caller staging with controller-owned
hierarchical coordination that does not occupy the child's execution capacity.
If hierarchy is adopted, define parent/child history links, priorities, shared
child ownership, withdrawal/force behavior and shutdown before relying on it.
Exercise capacity-one progress, concurrent parents, child failure, exact evidence
and repeated-child reuse. These are design questions and verification targets;
no replacement hierarchy is implemented or adopted by this note.

## Verification and recovery

On 2026-10-02, an isolated `git archive` of Hedloom `e9dbe70` passed **six tests**
using the ASS Python 3.11.16 environment and Dask/distributed 2026.7.1: the nested
example's graph and sequential cases, its fresh-interpreter script case, and the
three kernel headroom tests. This freshly confirms the process-local example
and its guard. The ASS sequential-child variant was source-checked at `7f1616b`,
not rerun as part of this historical check. No real-farm nesting is established.

To inspect and rerun without changing the current checkout, start at the ASS root
and choose an unused worktree path:

```console
ass_python="$PWD/.venv/bin/python"
git -C hedloom worktree add --detach /tmp/hedloom-nested-reference e9dbe70
cd /tmp/hedloom-nested-reference
PYTHONPATH=.:src:flow/src:exec/src:run/src "$ass_python" -m pytest -q \
    tests/test_nested_studies_example.py \
    run/tests/test_graph.py::test_a_nested_run_whose_waiters_hold_every_unit_is_refused \
    run/tests/test_graph.py::test_a_nested_run_with_one_unit_to_spare_is_admitted \
    run/tests/test_graph.py::test_a_run_submitted_from_the_driver_is_never_refused_for_nesting
```

Read the preserved [September architectural questions](rearchitecting-nested-studies-2026-09-04.md)
alongside this reference. Both are dated evidence; current API contracts belong
to the maintained guides and ontologies.
