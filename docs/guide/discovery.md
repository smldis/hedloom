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

If the submitting script constructed `Site(...)` in Python, supply its
`history_root` directly; no TOML profile or original script is needed:

```sh
hedloom runs list --history-root /shared/studies/history
hedloom runs show --history-root /shared/studies/history investigate-start.1 --json
hedloom runs path --history-root /shared/studies/history investigate-start.1 --invocation prepare.1 --workspace
```

`runs list`, `runs show`, and `runs path` require exactly one of `--site` or
`--history-root`. Relative history paths resolve against the current directory.
The saved run metadata supplies the record and workspace roots.

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

Within one Session, compatible ready invocations share execution handles.
Each invocation has a durable binding to its handle before new work is scheduled;
the worker publishes the actual selected try and workspace once beneath that
handle. A later consumer can immediately read an existing selection. Queries
follow these references directly, without scanning for a candidate or waiting for
completion. `execution_id` in an invocation snapshot identifies that dispatch;
it is separate from the computation record and try.

Sharing is decided when an invocation is ready, using its finalized computation
identity and compatible implementation, roots, placement and transport bindings.
Equivalent runtime artifact identities may refer to different suitable paths.
Each consumer records its own candidate inputs; the shared try records the inputs
actually used. Completed producer artifacts remain in each run's result map.
A later lookup of a completed execution creates a dispatch and lets Exec decide
reuse or a new try. Independent Sessions share completed evidence through Exec; joining
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

## Reproducibility records

Every submission saves a compact `reproducibility.json` before execution, including
submissions that reuse earlier results. Capture is enabled by default.
Configure capture with `reproducibility=Reproducibility(...)` and read it with
`RunHistory.reproducibility(run_id)`. The module is `hedloom.reproducibility`,
and CLI JSON uses the `reproducibility` field. The initial `Provenance` class and
`provenance=` keyword are removed; the reader still accepts records saved under
the old filename without rewriting them.

The record has two observations, with separate `captured_at` timestamps:

- **Environment:** Python version, installed package names and exact versions,
  direct installation origins, the nearest project `pyproject.toml`, editable
  package manifests, and Git commits, dirty flags and tracked patches. Automatic
  capture does not read lockfiles, collect every ancestor manifest, or retain
  interpreter paths, full platform descriptions or verbose Git status.
- **Submission:** fresh Git references for available entry-script and operation
  source files in clean repositories; saved bytes otherwise. Supplied text/files,
  working directory and arguments are captured for every run.

```python
from hedloom import Reproducibility, RunHistory, session

with session(site) as live:
    first = live.submit(subject, name="baseline", reproducibility=Reproducibility(
        text="Toolchain release 4",
        files=("setup.sh", "site.toml"),
    ))
    second = live.submit(subject, name="repeat")
    # After changing installed packages or editable dependency sources:
    live.refresh_environment()
    third = live.submit(subject, name="updated")

evidence = RunHistory(site.history_root).reproducibility(first.run_id)
print(evidence["environment"]["captured_at"])
print(evidence["captured_at"])
```

The first enabled submission discovers the environment. Later submissions in the
same Session reuse it, including concurrent `submit_all` calls. A lock ensures
that simultaneous first submissions share one discovery. There is no dependency
metadata rescan, dependency manifest reread, or dependency Git command on a cache
hit. Study Git state is observed separately for every submission. The environment
is **assumed unchanged** until explicit refresh: installing a package, editing an
editable dependency, or changing its manifest requires `refresh_environment()`
or a new session. Refresh failures leave the previous snapshot intact; runs
already holding it retain their original observation.

`Reproducibility(project_root=...)` selects a different project. The default is
the submission working directory. Sessions cache separately for each absolute
project-root spelling, so a session serving several projects cannot confuse
manifests. Refresh a nondefault entry using
`live.refresh_environment(project_root=...)`. No persistent or global automatic
cache is used, and no filesystem change detection is implied.

Standalone submissions can share an explicit immutable snapshot too:

```python
from hedloom import capture_environment

environment = capture_environment(project_root="/path/to/project")
first = subject.submit(site=site, name="first", environment=environment)
second = other.submit(site=site, name="second", environment=environment)
```

`environment=` works on `Study.submit`, `submit`, `Session.submit`, and
`Session.submit_all`. It takes precedence over automatic discovery and the
configuration's `project_root`. `EnvironmentSnapshot.to_data()` returns a copy;
editing that copy cannot mutate future records. To refresh an explicitly supplied
snapshot, capture another and pass it to subsequent submissions. Each standalone
submission otherwise opens a new session and pays discovery once.

Supplied file paths are relative to the submitting working directory. Files are
copied, never executed; a missing supplied file refuses before operations run.
`Reproducibility(enabled=False)` records an explicit opt-out and performs no
environment discovery. You can explicitly attach a lockfile if a particular run
needs one. Old runs without the feature return `None` from the history reader.

`hedloom runs show --site site.toml baseline.1 --json` includes the saved record.
File bytes are base64-encoded with SHA-256 digests. Dependency manifests appear
under `environment.files`; copied source and supplied files appear under `files`.
Clean study source references appear under `sources`, with repository location,
commit SHA, repository-relative path and blob ID. `study_repositories` records
fresh commit and dirty observations, independently of the cached environment.

For a clean tracked source file, Git stores the bytes already, so the run saves
only its reference. The file's bytes are checked against the committed blob;
staged/unstaged changes, untracked files, an unavailable Git command, or a blob
mismatch fall back to copied bytes. Repository commit/status queries are shared
among sources in the same repository within one capture. Explicit `files=`
attachments always keep their bytes, even if Git could represent them.

Retain the repository and commit. To read a referenced source, run
`git -C <repository> show <commit>:<relative-path>` or check out the commit in a
separate directory. These references do not archive the repository, fetch it,
or automatically restore an environment. Only discovered source files and
explicit attachments are covered; this is not a recursive Python-import or
study-data inventory.
For example, to recover a supplied file after the original has disappeared:

```python
import base64
from pathlib import Path
saved = evidence["files"]["/original/path/setup.sh"]
Path("recovered-setup.sh").write_bytes(base64.b64decode(saved["content"]))
```

To reconstruct an environment, use its recorded Python version and exact package
versions, recover the project manifests and setup files, and restore editable
repositories at their recorded commits with any tracked patches. Retain those
repositories: a commit reference is not a Git archive. Installed versions are a
compact reconstruction aid, not a lockfile with exact distribution artifacts and
hashes. System libraries and remote/external tool environments require supplied
evidence. Arbitrary environment variables and installed package payloads are
not copied.

Dirty dependency patches are retained, but untracked and ignored file contents,
submodule working trees, and external data are not automatically archived.
Available study source files and explicit attachments are exceptions. Gaps are
reported with status `partial`; `captured` means collection completed, not that
an exact rerun is guaranteed. Source bytes are read at submission time, which
cannot reconstruct an already-loaded module whose source was subsequently
edited. Environment Git observations retain their original capture time;
`study_repositories` and `sources` describe the current submission's files.
A dirty study repository marks the record partial: captured source bytes do not
preserve every modified helper, input, or untracked file in that repository.

Caching avoids repeated **discovery**, not repeated storage: each run currently
contains its compact environment snapshot. The record does not change
computation identity or reuse. A reused result may have been produced under a
different environment; its record and try remain the execution reference.
