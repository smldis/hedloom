# Finding runs and live workspaces

Every submission requires a chosen `name` and a separate Site history directory:

```toml
[study]
root = "records"
workspace_root = "workspaces"
history_root = "history"
```

```python
run = subject.submit(site=site, name="investigate-start", watch=True)
print(run.run_id)       # investigate-start.1, then investigate-start.2
print(run.history)      # persistence status and immutable errors
```

`Study.name` identifies the authored definition. Submission names identify each
request, including reused requests. `Session.submit_all({name: subject, ...})`
uses the mapping keys; `label` is presentation only. `on_started(reference)`
receives the complete address and history location after durable initialization,
before execution. Watch mode prints the address to stderr immediately.

Names are case-sensitive and match `[A-Za-z0-9][A-Za-z0-9._-]*`. Addresses always
include a positive unpadded occurrence: name `experiment.2` starts at
`experiment.2.1`. There is no latest alias. Permanent atomic reservation
directories prevent number recycling; gaps are valid. History cannot overlap
the computation or workspace roots.

## Inspect from another process

```sh
hedloom runs list --site site.toml
hedloom runs list --site site.toml --name investigate-start --since 2026-09-06 --json
hedloom runs show --site site.toml investigate-start.1
hedloom runs show --site site.toml investigate-start.1 --invocation prepare.1 --json
hedloom runs path --site site.toml investigate-start.1 --invocation prepare.1 --journal-dir
cd "$(hedloom runs path --site site.toml investigate-start.1 --invocation prepare.1 --workspace)"
```

List results are newest first. `--study` filters the authored definition.
Date-only `--since` means UTC midnight; timestamps require an explicit timezone.
A path command prints exactly one existing absolute directory and newline on
success. Failure prints only a diagnostic to stderr and exits nonzero.
The journal directory contains all tries; show identifies the selected try.

Operations without explicit or sweep keys receive `function_name.N`. Unnamed
flow boundaries use the same rule. Counters belong to each function name in its
containing boundary; explicit and sweep calls do not consume them. Inserting a
call of the same function can renumber later automatic calls. Explicit keys
provide stable meaning across those edits. Keys do not change computation identity.

Invocation addresses join all boundary keys with `/`. A slash inside a key is
spelled `%2F`; other percent escapes refuse. A leaf key works only when unique;
ambiguity reports complete candidate addresses. Old manually constructed Plans
without readable keys refuse submission; existing computation records remain
browseable.

```python
from hedloom import RunHistory
history = RunHistory(site.history_root)
runs = history.list_runs(name="investigate-start")
view = history.read_run("investigate-start.1")
invocations = history.list_invocations(view.run_id)
path = history.resolve_path(view.run_id, "prepare.1", workspace=True)
outputs = history.outputs(view.run_id)  # available is separate from value=None
```

Queries need neither the original study module nor a Dask client. They never
launch, poll a scheduler, reconcile, pin, prune, repair, or create directories.
Recorded roots continue to locate old evidence after profile changes. Missing
locations are reported; relocation is not implemented.

## Interpreting observations

Run-reported outcome, selected execution state, and history persistence status
are separate fields. A selected execution can succeed after its consumer stops
reporting. Without a final run event, completion is **unreported**: history cannot
tell whether the controller is alive. Last observation time is not a heartbeat.
A torn final append exposes the valid prefix with a diagnostic; malformed complete
events, incompatible schemas and conflicting references refuse.

Selection names the actual try chosen by Exec before blocking launch or
attachment. Workspace binding arrives separately. No selection, unreported
binding, no workspace, missing directory and recorded reclamation remain
distinct. Missing alone does not prove reclamation.

Initial persistence failure prevents execution. Later failure continues work,
emits a warning and leaves `run.history.status` degraded. Selection errors
travel back with task outcomes. Final evidence can recover a reference with
late provenance, without claiming it was available during execution. After an
append failure the controller stops writing its log and preserves outcomes in
memory. Escaping exceptions carry `.history` alongside any partial `.report`.

Within one Session, compatible overlapping bound graphs share execution handles.
Each invocation has a durable binding to its handle before new work is scheduled;
the worker publishes the actual selected try and workspace once beneath that
handle. A later consumer can immediately read an existing selection. Queries
follow these references directly, without scanning for a candidate or waiting for
completion. `execution_id` in an invocation snapshot identifies that dispatch;
it is separate from the computation record and try.

Sharing requires the same bound graph, including dependencies, source bindings,
roots, placement and transport configuration. Renaming invocations does not
change this contract. Finished predecessors remain available while the graph is
active, so staggered submissions can share downstream work too. After the graph
finishes, a subsequent submission makes a new dispatch and Exec decides reuse or
a new try. Independent Sessions share completed evidence through Exec; joining
their running work is unsupported and contention may still refuse.

If a consumer stops while another needs the execution, its unfinished outcomes
are `cancelled` with disposition `withdrawn`; shared execution can continue.
Its recorded request binding does not claim successful consumption. With no
remaining consumer, a durable gate prevents unstarted work from entering Exec;
already-entered work is awaited and reported truthfully. These decisions do not
cancel the shared Dask future. A dispatch cannot enter Exec twice: automatic
worker replay refuses explicitly rather than redirecting history to a new try.

The same durable handle/selection relationship is used sequentially, without
Dask or an observer thread. Exec owns selection accounting and returns it on
results and handled failures. The handle only publishes that evidence; Run adds
each consumer's identity when constructing its report.

The metadata store requires a coherent shared filesystem with atomic mkdir,
rename, hard links, fsync and working advisory locks for the short dispatch gate.
Graph preflight uses an out-of-band worker challenge
and acknowledgement before scheduling, verifying visibility in both directions.
It is not a remote history service.

## Computations without consumer history

```sh
hedloom attempts list --site site.toml --since 2026-09-06 --outcome failed --json
hedloom attempts show --site site.toml --record RECORD --try 0
```

Every folded try is listed, including old records without run history. Rows carry
operation, times, state, standing status, pins and observed payload availability.
They acquire no invented study owner. Run occurrences start at 1; Exec's existing
try numbers start at 0 and are preserved exactly.

This remains a prototype. Local subprocess tests prove live discovery for both
kernels with a bounded barrier before completion. Fake-farm evidence tests
placement and concurrency; this feature has not run on a real farm. Comparisons,
GUI, log following, previews, deletion, renaming, migration, retention redesign
and wider inquiry ownership remain outside this release.
