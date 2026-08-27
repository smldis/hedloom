# Brief for an agent working on the pruner plan

Durable context for `design/attempt-record-and-collector-plan-2026-08-27.md`.
Read this once; it is the part a fresh session cannot re-derive cheaply.

Not a contract. `ONTOLOME.md` files are the contracts, `docs/` is the
documentation, and `design/` — including this file — is the design record.

## 0. Before anything: check your model

Your footer must show the model and effort you were launched with. If it has
changed — a quota downgrade swaps it silently — **stop at the next commit
boundary and say so.** This has happened: a pass downgraded mid-flight and
produced 490 lines across the four most sensitive files before anyone noticed.
All of it was thrown away.

## 1. The architecture, in one page

Four units, composed by containment, each independently installable.

| Unit | Owns | Package |
| --- | --- | --- |
| `hedloom-flow` | authoring, keys, Plan IR, validation | `flow/src/hedloom_flow` |
| `hedloom-exec` | attempt identity, journal, transports, reuse, artifacts | `exec/src/hedloom_exec` |
| `hedloom-run` | traversal, readiness, binding, placement, `Site`, cluster | `run/src/hedloom_run` |
| `hedloom` | binds authored bodies to planned invocations; `submit`, `session` | `src/hedloom` |

**`hedloom_exec` imports neither `hedloom_flow` nor Dask.** That independence
is what keeps the kernel and the façade separately replaceable. It has been
reversed twice already; a stray import there makes the choice irreversible.

The Plan arrives at `hedloom_exec` as a **plain-data document**, never as an
object. Coupling is to the portable artifact, not to the package.

### The identity chain

```
authored key   "coarse:integrate"      what you write, or what sweep names
   │ sha256(kind ␀ scope ␀ normalized key)
invocation_id  invoke:key:8a436d20…    WHICH NODE in the graph
   │ blake2b-16 over one canonical JSON mapping of nine identity keys
input_digest   e798de4c…               WHAT IT WAS COMPUTED FROM
   │ blake2b-10( plan ␟ invocation ␟ digest )     ← three slots, since Phase 1
record         hedloom-016ea5b1…       the attempt at these inputs
   │ f"{record}-{try}"                 string formatting, not a hash input
try            hedloom-016ea5b1…-2     one attempt; the workspace and job name
```

`docs/internals/mechanism.md` has the full version. Two things it is easy to
get wrong:

- `input_digest` is **one** hash over **one** canonical mapping. The nine
  per-key digests recorded in `created` are computed *additionally*; they are
  not parts of the aggregate and must never be recombined into it. A plan draft
  claimed otherwise and would have invalidated every identity in existence.
- Placement is deliberately **not** in the digest. Moving work to another queue
  reuses what it already produced. Never add it.

## 2. The class of bug this codebase produces

Almost every real defect found here has the same shape: **something assumes a
name or a directory means what it used to mean, and fails without failing.**
The list is worth reading because the next one will look like these.

| What | Where it bit |
| --- | --- |
| Watcher matched `"lsf-interactive"` while journals recorded `"bound:lsf-interactive"` | an empty sweep was indistinguishable from a finished one |
| Recovery called `discover(journal.identity)` when jobs are named by try | would have resubmitted accepted work — duplicate farm jobs under one identity |
| One fixed `manifest.json` with several tries per record | try 1 silently overwrote try 0's published evidence |
| Sticky fold fields (`cancel_requested`, `reuse_accepted`, `observations`) | a cancellation of try 0 blocked try 1; an accepted failure leaked forward |
| `workspace_for` calls `mkdir` | a dry run *creates* the directories it is only inspecting |
| `hedloom/examples/` shadowed `flow/examples/` | read for weeks as "two pre-existing failures in flow"; it was the invocation |
| `testpaths = ["tests"]` in every unit | the documented check ran 58 of 458 tests and exited zero |

**The habit that catches these:** when you change what a name means, grep for
every reader of that name and check each one, rather than the ones the plan
lists. A list in a plan is a hypothesis about scope. Test it.

## 3. Surfaces, and which are maintained

| Surface | Where | Maintained? |
| --- | --- | --- |
| Contracts | `ONTOLOME.md` per unit, `exec/DECISIONS.md` | **Yes** — move it in the same commit that changes a contract |
| Documentation | `docs/` per unit, built by `composition.py docs` | **Yes** |
| Design record | `design/`, `flow/PLANNING.md`, this file | **No** — written on a date, never edited to stay true |

Never cite a `design/` file as evidence of current behaviour.

The register `../docs/vision/open-concepts.md` lives in the **outer** repo
(`analog-sim-studies`, remote `git@github.com:smldis/analog-sim-studies.git`).
A contract change must update it. You may commit there **on a branch** and open
a PR; never to its `main`. Cross-reference both PR bodies.

## 4. Invariants — refuse to break these

- **A body decides what runs; it never decides whether it runs.** Reuse,
  identity, ordering and placement are settled before a body is called.
- **A study decides what is produced; it never decides what is kept.**
  Retention is the operator's. `submit(prune=…)` is a tripwire, exactly as
  `retry=` and `until=` are.
- **Incompleteness may refuse; it may not be silently wrong.** A surface that
  cannot yet be correct declines. Documenting a limitation does not make it
  safe — a caller reads the return value, not the ontology.
- **Reuse must never return work computed from different inputs.**
- **Nothing in the `hedloom` package names a simulator, a circuit, or a
  domain** — not in code, not in a docstring, not in an example. Studies that
  do live in `../studies/`.
- **Never pre-touch a declared output.** `capture_outputs` treats an existing
  declared output as evidence the work produced it, so a placeholder lets a
  command that wrote nothing report success.

## 5. Settled decisions — do not reopen

`prune`, not `collect` — `Collection` collides with `hedloom_flow`'s fan-in
cardinality, and `Policy`/`plan` are already public in `hedloom`. Hence
`RetentionPolicy`, `RetentionRule`, `survey()`, `Survey`, `PruneReport`.

No migration support; roots from earlier versions are unreadable, and the
record declares a `layout` version so that is refused by version rather than
misread. `accept_for_reuse` does **not** imply a pin. `keep_latest` defaults
to 1. Cancellation is per-try only. In-place publication is dropped. The alias
tree is `<Site.root>/latest/`, derived and built by default.

## 6. What is already built

- **Phase 0/0b** (PR #7): `created` carries `try`, `authored_key`,
  `supersedes`, `input_digests`. `workspace_path` is the pure resolver;
  `workspace_for` still creates. `latest/` aliases, `hedloom where|check|log`,
  and the rerun reason printed per invocation.
- **Directory artifacts** (PR #6): declared outputs carry an explicit file or
  directory shape. Empty files and empty directories are valid outputs.
- **Phase 1** (PR #8): records split from try workspaces. Identity is three
  components. Per-try manifests plus `standing.json`. `TryState`, `tries`,
  `current_try` — the fold is partitioned. `begin_try` under the held claim,
  raising `ClaimNotHeld`. `layout` file. `_select_sequence` and `max_attempts`
  are gone. Discovery, cancellation, LSF jobs and watching use try names.
- **PR #9**: the `flow/tests` conftest, and the check now names all four units.

## 7. Hazards specific to Phases 2–5

Things I can foresee that the plan states only in passing.

**The pruner must not treat `latest/` as a record.** It lives inside the
attempts root. `scan_attempts` and `watch.live_attempts` skip it only because
both guard on `events.jsonl` existing. That guard is load-bearing now. And
`aliased` is a skip reason: a symlink does not keep its target alive, so an
aliased workspace is never a candidate.

**`larger_than` must walk a directory output, never trust a recorded size.**
Before PR #6 a directory was recorded as `kind="file"` with `st_size` 4096 —
so the rule written to catch the biggest artifacts was the one guaranteed to
miss them. PR #6 fixed the recording; the pruner must still measure by walking,
and refuse a size rule against an artifact whose kind it cannot determine.

**A pin cannot live in the workspace.** Pruning would delete the pin — the
protection destroyed by the operation it exists to prevent. It goes in the
record, written by the same `journal.append` path as `accept_for_reuse`.

**`chmod a-w` is not enforcement.** It does not revoke open descriptors, the
owner can undo it, root ignores it, and renaming a directory needs write
permission on its *parent*. The pin contract is **refuse, detect, record** —
never *prevent*. Do not let documentation claim otherwise.

**Two pre-existing defects that pruning and pinning make reachable.**
`accept_for_reuse` does not take `journal.claim()`. And `write_diagnostics`
is called outside any `try`, so an unwritable workspace loses terminal
publication for work that finished. Fix both in the phase that reaches them.

**Never selectable:** `unreconciled` — that outcome *is* the evidence that the
record and the substrate disagreed. And the standing reusable result: skip it
as `reusable`, which is a more accurate reason than `pinned`.

## 8. Checks

```console
PYTHONPATH=src:flow/src:exec/src:run/src python -m pytest -q \
    tests exec/tests run/tests flow/tests
python ../composition.py docs     # from the outer repository root
```

Name all four unit directories — a bare `pytest -q` collects only the façade's
tests and exits zero. Read the Sphinx warnings, not the exit status.

**Baseline at `main` `0bc8040`, measured 2026-08-27:**

```
tests 58 passed 2 skipped | exec 274 | run 73 passed 2 skipped | flow 93
                                              498 passed, 4 skipped
```

## 9. The stop-clause

**If the plan turns out wrong or impossible, STOP and report. Do not improvise
a different design.** This has been used twice and was right both times — once
it caught a change that would have invalidated every identity in existence. A
census that stops is worth more than an implementation that guesses.

Tests that specify the design being replaced are rewritten **deliberately**, in
the commit that states the new contract, and named in the PR body. Never "fix
until green".
