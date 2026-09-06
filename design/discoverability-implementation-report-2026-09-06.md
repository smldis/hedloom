# Discoverability implementation report — 2026-09-06

Implemented the named-submission and live-discovery plan in the current Hedloom
checkout. No commits or publishing were performed. The original plan, proposal
and unrelated containing-repository submodule changes were preserved.

## Delivered contracts

- Flow resolves explicit, sweep, or automatic `function_name.N` keys, including
  flow boundaries. Scoped automatic counters and definition ownership roll back
  on failure. Collisions refuse in both authoring orders. Readable facade
  addresses distinguish nested boundaries from slash-containing keys and refuse
  ambiguous leaves. Computation identity remains declaration-based.
- `Site.history_root` is parsed, anchored and retained through transformations.
  Facade submissions require `name`; `submit_all` uses mapping keys. History
  roots cannot overlap computation/workspace roots. Durable reservations allocate
  permanent positive occurrences without an artificial digit cap.
- The facade saves the exact Plan, address table, immutable ready header,
  controller events and immutable per-invocation selections/workspace bindings.
  Initial failures prevent invocation execution. Later failures return degraded
  persistence and preserve computation outcomes. A failed append stops subsequent
  appends. Best-effort `persistence.json` makes diagnostics independently readable
  even when the controller event tail is unusable.
- Exec observes actual selection before blocking launch/attachment. Run enriches
  notices with invocation identity and captures observation errors in both
  kernels, retaining selected references after handled launch failures. Reuse
  workspace bindings come from the original receipt, not a newly configured root.
- Graph history visibility is checked through out-of-band worker challenge and
  acknowledgement. Sinks carry immutable configuration, without controller file
  handles. Final accounting precedes automatic retention. Startup callbacks and
  exception history descriptors are propagated through facade wrappers.
- Read-only Python snapshots and CLI list/show/path commands separate consumer
  outcome, selected execution state and persistence status. They preserve exact
  references and recorded roots, expose interrupted/torn observations, resolve
  named output ports, distinguish successful `None` from unavailable output, and
  browse every computation try without assigning a study owner.
- Maintained ontology, decision ledger, documentation, examples, fixtures and root
  study callers were updated. All components remain prototypes.

## Validation

From Hedloom:

```sh
PYTHONPATH=src:flow/src:exec/src:run/src python -m pytest -q -rs tests exec/tests run/tests flow/tests
```

**651 passed, 5 skipped.** Three warnings deliberately exercise incomplete reports
from fake kernels and an escaping exception. Three skips are optional
`dask_jobqueue` suites; two require Graphviz/Bokeh.

Using the existing containing repository's `.toolchain/venv/bin/python`, the
visualization and cluster suites passed **27 tests**, covering the latter two
skips. `dask_jobqueue` is absent from both available interpreters.

Containing-repository integration: **22 passed** with all child source directories
on `PYTHONPATH`. Composed Sphinx documentation built successfully using the
existing toolchain, **without warnings**. `git diff --check` passed.

Focused evidence includes separate-process allocation, automatic counters above
9999, rollback, collisions, reuse, standing-versus-current selection, attachment,
post-selection refusal, history write failure, torn/corrupt records, callbacks,
worker visibility refusal, retained roots, ambiguous addresses, reclaimed/absent
payload, and query file/mtime preservation.

## Live evidence

`test_live_discovery_from_separate_process_before_body_returns` ran a producer
child with a FIFO barrier and used separate CLI processes before releasing it.
Both sequential and graph cases discovered **`live-inspection.1`**, selected
**`waiting.1`**, and read `partial.txt` containing `visible before return` while
the producer was still blocked. Killed-controller variants retained the selected
reference and incomplete consumer accounting; a later terminal Exec publication
did not invent a consumer completion.

The graph case used this profile:

```text
/tmp/pytest-of-smldis/pytest-446/test_live_discovery_from_separ1/site.toml
```

These commands were executed before releasing its barrier:

```sh
python -m hedloom.cli runs list --site /tmp/pytest-of-smldis/pytest-446/test_live_discovery_from_separ1/site.toml --json
python -m hedloom.cli runs path --site /tmp/pytest-of-smldis/pytest-446/test_live_discovery_from_separ1/site.toml live-inspection.1 --invocation waiting.1 --workspace
```

Its selected reference was `hedloom-ff32071f85b73d48fdb2#0`. The test has since
completed successfully. These temporary evidence paths are not permanent assets;
the test reproduces the experiment with a fresh directory.

## Implementation refinements and limits

Graph submissions with observers now have distinct task namespaces. Previously,
identical explicit Dask task keys coalesced concurrent submissions and bypassed
one consumer's selection sink. Each consumer now reaches Exec independently:
completed computations reuse, while overlapping claims can refuse and block that
consumer's dependents. The fake-farm example was updated accordingly. This is
required to keep each consumer's live history honest; it does not introduce retry
or coalescing in Exec.

Blocked work carries `block_reason` separately from `error`, preserving the
existing distinction between a failed computation and work that never ran.
Run occurrence numbering begins at 1; existing Exec try numbering begins at 0
and is retained exactly.

Evidence is local and fake-farm only. Shared filesystem atomicity, fsync and hard
links remain requirements; there is no distributed history service. No real farm
run was performed. Comparisons, GUI, log following, previews, deletion/renaming,
migration, retention redesign, coalescing/retries and wider inquiry ownership
remain deferred as specified in the plan.
