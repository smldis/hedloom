---
name: hedloom-study
description: Author or modify Hedloom studies, submit runs, read results, and discover saved runs in a local codebase. Use for study operation, not engine development.
---

Use the `hedloom` public facade and the project's Python environment. Locate the checkout, existing study and Site. For the common path below, implement and run it directly; consult additional documentation only for a missing contract or observed error.

**Author.** `@study(name="definition", default_policy=local())` builds a static Plan when called; operation bodies execute only at submission. Wire handles into inputs, never read their values or branch on them during planning. A `@flow` composes calls and returns handles/mappings; it does not yield a generator.

```python
from hedloom import operation, parameter, returned, artifact, artifacts, file

@operation(config={"n": parameter(int)}, outputs={"value": returned(kind="number")})
def compute(*, n):
    return {"value": n * n}
```

Scalar parameters go in `config`; dependencies go in `inputs`: `artifact("number")` for one, `artifacts("number")` for a list. Match producer/consumer kinds. A returned output requires a mapping keyed by its declared name. For `outputs={"report": file("report.json")}`, accept `out` and write `out.report`; return `shell(...)` for a command that runs at its placement. Declare mutable external inputs; hidden dependencies make reuse unsound.

Use `compute.named("key")(n=...)` or `sweep(points, key="key")` for readable identities. Export explicit ports, e.g. `return {"report": h.report, "verdict": h.verdict}`. Preserve unchanged operation bodies and keys when modifying parameters. Declared inputs/config/body changes affect reuse; study/submission names do not force recomputation.

**Submit.** `Site.from_file("site.toml")` anchors relative paths to the profile:
```toml
[study]
root = "records"
workspace_root = "workspaces"
history_root = "history"
```
History cannot overlap either other root. Call `subject = my_study(...)`, inspect `subject.summary()`, then submit authorized work: `run = subject.submit(site=site, name="request", sequential=True)` for local sequential execution. Preserve requested farm placement/concurrency; several runs can share `with session(site) as s: s.submit(subject, name="request")`.

**Use results.** Check `run.succeeded` and `run.history.status`; retain actual `run.run_id`. `run.outputs["name"]` has `.available`, `.value`, `.artifact`, `.outcome`. File `.value` is an address; returned `.value` can legitimately be `None`. No `run.value` exists. Execution success does not mean the exported verdict passes. `run.report.outcomes` exposes `.authored_key`, `.reused`, `.record`, `.try_number`, `.error`.

**Discover without executing or importing the study.**
```python
from hedloom import Site, RunHistory
h = RunHistory(Site.from_file("site.toml").history_root)
rows = h.list_runs(name="request")  # newest first; optional study=definition
view = h.read_run(rows[0].run_id)
outputs = h.outputs(view.run_id)
path = h.resolve_path(view.run_id, "producer-key", workspace=True)
```
Saved outputs are **dicts**, with `available`, `value`, `artifact`, `accessible` keys. Check availability separately from `value is None`. Snapshots expose `run_id`, `run_reported_outcome`, `history_status`, `invocations`; invocation snapshots expose `address`, `record`, `try_number`, `workspace`, `run_reported_outcome`, `selected_execution_state`. Compare exact record/try pairs by address to identify shared evidence across runs. Historical snapshots have no `.reused`; fresh run outcomes do. Missing payload and unreported completion are distinct from failure.

Use discovered IDs (`request.1`, etc.), never invent a latest alias. Nested invocation addresses use `/`; ambiguous leaves need their full address. CLI equivalents: `hedloom runs list --site site.toml --name request --json`, `hedloom runs show --site site.toml RUN_ID --json`, `hedloom runs path --site site.toml RUN_ID --invocation ADDRESS --workspace`.

For missing details, read only the relevant checkout `docs/guide/` page: `authoring.md` (composition), `running.md`/`sites.md` (sessions/farm), `runtime-artifacts.md` (fresh acquisition/identity), `discovery.md` (history/CLI). Discovery field questions resolve in `src/hedloom/discovery.py`; dated `design/` proposals are not current contracts.
