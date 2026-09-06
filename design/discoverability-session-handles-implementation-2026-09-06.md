# Session-owned execution handles — implementation evidence, 2026-09-06

This follow-up implements the accepted architecture reassessment. It is added
after `2bf070a`; the original implementation commit and dated reports are not
rewritten. Current contracts are maintained in the ontologies and discovery guide.

## Delivered

- The facade Session owns one explicit `hedloom_run.execution.ExecutionOwner`.
  Compatible overlapping bound graphs share execution handles. Consumer names
  and invocation IDs remain local to their respective reports and histories.
- Compatibility is deliberately conservative: the complete normalized bound
  graph, dependency topology, transport serialization, roots, outputs and source
  bindings must match. This does not merge arbitrary overlapping subgraphs.
  Complete predecessors stay with their active graph, allowing late consumers
  to share a still-running successor. A fully terminal graph is not a cache;
  the next submission uses new handles and Exec decides result reuse/new tries.
- Every consumer binding is durable before that graph admits any new task.
  Workers receive shared handles rather than per-consumer callbacks. A handle
  records the actual selected try and workspace once; Python and CLI discovery
  follow its durable binding directly, without scanning, a live scheduler, an
  observer task or background polling. Legacy direct selection files remain
  readable. Sequential execution uses the same handle contract without Dask.
- The entry/cancel gate is durable and held only briefly. Cancellation before
  entry prevents an Exec call, including when Dask has already started the
  wrapper. Entered work is awaited and reported from its actual result.
  A consumer withdrawing while another still needs the execution reports
  `cancelled`/`withdrawn` and leaves the shared future intact. Binding a request
  does not assert successful final consumption.
- A handle cannot enter Exec twice. Worker replay explicitly refuses before
  another try can be selected. The gate is execution control, separate from
  selection observation; selection or later accounting failures still produce
  persistence diagnostics without replacing computation outcomes.
- Unowned low-level graph calls have isolated task keys. Passing a Client alone
  no longer accidentally grants shared lifetime. Their legacy cancellation
  implementation remains; the durable admission protocol is the owned path.
- The fake-farm same-study-twice example again requires both consumers to
  succeed with complete history and only four jobs, rather than accepting
  `ConcurrentClaim` as a successful demonstration.

## Evidence

```sh
PYTHONPATH=src:flow/src:exec/src:run/src python -m pytest -q -rs tests exec/tests run/tests flow/tests
```

**662 passed, 5 skipped, 3 expected warnings**, in 38.16 seconds. Warnings are
the existing fake incomplete-kernel reports and escaping-kernel fixture.
Three skips require `dask_jobqueue`; two require Graphviz/Bokeh. The existing
containing-repository toolchain passed **27 visualization/cluster tests**,
covering the latter optional dependencies.

Containing-repository integration: **22 passed**. Composed Sphinx docs built
successfully with **no build warnings**. `git diff --check` passed.

`tests/test_shared_execution.py` adds eleven integrated/protocol cases covering
staggered consumers with different authored names, success and failure, exact
fresh-process CLI workspace lookup before completion, one actual body per shared
invocation, finished predecessors with active successors, later submission
reuse/new tries, failed initial binding, replay refusal, consumer withdrawal,
different stop policies, both sides of cancellation versus entry, and different
record/workspace bindings that must not share. The existing killed-controller
subprocess cases now exercise durable handle-based discovery in both kernels.

The earlier exploratory probe and reassessment are retained as dated design
evidence, distinct from the integrated implementation and tests above.

## Limits

Sharing is Session-local and whole-graph-compatible. Independent Sessions retain
Exec's existing claim/refusal behavior and completed-evidence reuse. The change
adds neither cross-controller joining nor automatic worker recovery. History
and gate storage assume coherent shared filesystems with the documented atomic
publication and advisory-lock semantics. Verification is local and fake-farm;
no real farm or multi-host storage validation was performed. Metadata latency
has not been benchmarked. No Flow schema or computation identity changed.
