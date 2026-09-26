# Storage paths after the naming change

Hedloom now names three independent locations. They may live on different
filesystems; no common parent is required.

| Purpose | Python `Site(...)` and `[study]` TOML | CLI path flag |
| --- | --- | --- |
| Shared computation records | `records_dir` | `--records-dir` |
| Saved named submissions | `runs_dir` | `--runs-dir` for `runs` discovery |
| Per-try files | `work_dir` | `--work-dir` for pin, unpin, pins, and prune |

Edit scripts and profiles manually: `Site.root` / `[study] root` becomes
`records_dir`; `record_root` in lower-level calls and saved producer references
becomes `records_dir`; `Site.history_root` / `[study] history_root` becomes
`runs_dir`; `Site.workspace_root` / `[study] workspace_root` becomes `work_dir`.
The lower-level `hedloom_exec.execute`, `AttemptJournal`, `scan_attempts`,
`prune.survey`, `pins.resolve_selector`, `watch.status_of`,
`watch.live_attempts`, `watch.observe`, `hedloom.discovery.list_attempts`,
and `hedloom_run.run_plan` / `run_plan_graph` record-location arguments now
use `records_dir`. `Survey.records_dir` and its `as_data()["records_dir"]`
replace the old `root` field and key. APIs that accept a try-location argument
use `work_dir`, including `workspace_path` and `workspace_for` when called by
keyword. The `RunHistory` constructor takes
`runs_dir`. CLI `--root`, `--history-root`, and `--workspace-root` become
`--records-dir`, `--runs-dir`, and `--work-dir`, respectively. There are no
old-name aliases. Stale TOML fields and CLI flags refuse with a replacement
message; old Python keywords fail at the call site.

The new layout is:

```text
records_dir/<record identity>/       journal, manifests, standing result, pins
work_dir/<record identity>-<try>/    files from one try
runs_dir/<submission>.<occurrence>/  plan.json, run.json, events, selections
runs_dir/_meta/allocations/           occurrence reservations
runs_dir/_meta/executions/            shared dispatch handles
```

`run.json` and other consumer-history documents now have schema version 3.
`run.json` records `records_dir` and `work_dir` in place of `record_root` and
`workspace_root`. Produced artifact references likewise use `records_dir`.
Run readers require the direct run directory and schema 3; they refuse a
previous `runs_dir/runs/` or `runs_dir/allocations/` tree. They do not promise
to read schema-2 saved runs, reinterpret old metadata, or relocate saved data.
Keep old evidence intact if it matters. To use a fresh location, choose a new
`runs_dir` and make new submissions; manually inspect old saved runs with a
matching older checkout when needed. No automatic migration is performed.

The Exec record layout remains version 1, and record identity is unchanged by
this path rename. Pointing `records_dir` to the same record store can still
reuse its records. A saved try's original work directory comes from its
receipt even when a later Site names another `work_dir`. The low-level Exec and
Run calls retain their optional `work_dir` fallback: when omitted, a try that
needs files uses the records location. Facade submissions still require
`runs_dir`; they do not silently stop saving consumer history.
