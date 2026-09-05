# Study discovery and history: handoff from attempt identity

Updated 2026-09-05 against PR #19, merged as `604f53f`. This replaces the
pre-implementation handoff's source observations. The original is preserved on
`recovery/local-before-main-alignment-20260905`. This note separates delivered
execution contracts from proposed discovery work; it does not authorize that
work or claim new runtime verification.

## User direction retained

- Use one shared computation store; manage study relationships separately.
- Support multiple pin requesters eventually. Releasing one request must leave
  other requesters' protection intact; operator and study requests may coexist.
- Reclaim execution payloads using record/try properties and pins. General
  study-history reclamation is deferred.
- Remove `latest/` rather than preserving its behavior as a discovery contract.
- Design study history with discoverability, separately from execution identity.

## Delivered by PR #19

A declared computation digest selects a record independently of study name,
Plan ID, and authored invocation key. Equality depends on faithful declarations;
independent repetitions require a computational distinction such as a seed.
See [identity.py](exec/src/hedloom_exec/identity.py) and
[the mechanism guide](docs/internals/mechanism.md).

The store contains record evidence and numbered execution workspaces:

```text
<Site.root>/<record-id>/             layout, journal, manifests, standing
<workspace_root>/<record-id>-<try>/  execution files
```

With no separate workspace root, workspaces use the record root. `latest/`
alias creation and protection, creator-based lookup and lineage, and the old
`where`, `check`, and `log` commands have been removed. Existing evidence roots
were not migrated or deleted by that source change. Standing reusable evidence
is separate from aliases and remains protected by reclamation.

`ExecutionResult` and `InvocationOutcome` now carry `record` and `try_number`
for the execution selected by the call, including reuse. Both kernels forward
these fields; work that never selects a try must not invent one. See
[durability.py](exec/src/hedloom_exec/durability.py),
[driver.py](run/src/hedloom_run/driver.py), and
[graph.py](run/src/hedloom_run/graph.py).

`StudyRun` retains the Plan, study name, invocation outcomes, and named outputs
in memory. `run.outputs` resolves the Plan's exports; aggregate `run.value` was
removed by PR #18. The facade does not persist a separate study-run history.
See [study.py](src/hedloom/study.py) and [results](docs/guide/results.md).

## Proposed seam for study history

Preserve the relationship:

> This invocation in this particular study run used this record and this try,
> with this outcome and whether it executed or reused evidence.

Capture the selected references from the invocation outcome at execution/reuse
time. Looking up standing evidence later may select a different try; artifact
paths alone are insufficient for value-only operations. The execution-reference
API gap described by the original handoff is now closed. Persisting consuming
contexts and making them discoverable remain open work.

Represent blocked or refused work without fabricating a try. No history schema,
study/run ID format, persistence protocol, replacement UI, or directory naming
is chosen here. History could live under `Site.root`, outside reclaimable
execution workspaces. A study name is an operator-facing name, not ownership of
a shared computation record.

## Pins and reclamation still to design

Current [pins.py](exec/src/hedloom_exec/pins.py) records a pin ID, actor, reason,
and per-try inventory, but rejects an already-pinned try. Unpinning can restore
write permissions immediately. Multiple requesters therefore require deliberate
handling of both remaining protection and final permission restoration.

Proposed invariant: a try remains protected while any requester has an active
pin request. Distinguish request ownership from the actor performing the action.
Define repeated requests and use an identity stable across display-name changes.
Which runs a study pin covers, and whether it covers future runs, remain open.
Pinning cannot recover reclaimed bytes; retained history must report absent
payloads honestly.

[Pruning](exec/src/hedloom_exec/prune.py) retains record evidence while removing
eligible workspace payloads, rechecks under the record claim, and protects
standing evidence. Making standing payloads reclaimable would be a separate
contract change requiring safe reuse handling; alias removal did not authorize it.

## Concurrent requests remain a scheduling question

Shared identity does not guarantee that simultaneous callers both receive one
successful result. The record claim remains nonblocking; contention can produce
`ConcurrentClaim`, reported as a refused, failed invocation by the graph kernel.
Exclusion protects against duplicate execution; waiting, coalescing, shared
failure/cancellation policy, and preservation of each consumer's history remain
separate design questions. See the
[nested scheduling follow-up](todo/nested-studies-as-a-tree-2026-09-05.md#follow-up--shared-computation-requests-and-nested-scheduling-2026-09-05).

The [initial implementation plan](todo/attempt-identity-implementation-2026-09-05.md)
and [cleanup brief](todo/shared-store-cleanup-2026-09-05.md) are historical scope
records, not instructions to repeat completed work. Current contracts belong in
the maintained documentation and ontolomes.
