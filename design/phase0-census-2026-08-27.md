# Census: Phase 0 and Phase 0b against the current code

**Written 2026-08-27. Read-only census before implementation.** This checks
Phase 0 and Phase 0b of
`attempt-record-and-collector-plan-2026-08-27.md` against commit `904ec5c`.
No implementation was changed while producing it.

## Verdict

**Stop before implementation.** The plan is not implementable as written while
preserving its own Phase 0 requirement of no behaviour change. The blocking
contradiction is the proposed `input_digests` representation and its required
recombination test:

- `exec/src/hedloom_exec/reuse.py:65-84` currently canonicalizes the complete
  identity-bearing mapping once and computes one BLAKE2b digest. It does **not**
  digest the nine keys individually and throw the parts away, as the plan says.
- Digests of the individual values cannot be recombined to recover the existing
  BLAKE2b digest of the canonical whole. Changing `input_digest()` to hash the
  component hashes would make every existing input digest and attempt identity
  move. That is behaviour change in Phase 0, and it conflicts with the explicit
  requirement that content-addressed identity remain unchanged.
- Storing the canonical values rather than their hashes would make recombination
  possible, but those values are not the planned `Mapping[str, str]` of short
  digests and would durably store the bundle material the plan says is not
  stored.

The plan therefore needs a decision before code: either preserve the existing
whole digest and drop the recombination requirement, or deliberately change the
digest algorithm and move that contract change out of Phase 0. This census does
not choose between them.

Two Phase 0b claims also need design correction. They are not needed to prove
the stop condition, but leaving them unreported would make the next pass repeat
the same discovery:

1. A `created.supersedes` link cannot record a return to an already existing
   identity. `created` is appended only when a record has no events
   (`exec/src/hedloom_exec/attempt.py:221-230`). For edits A -> B -> A, record A
   is reused and receives no new `created` event, so a chain walked only through
   `created.supersedes` cannot show the return the plan promises. A durable
   selection/visit event or a weaker definition of lineage is required.
2. Per-key hashes can identify that `inputs` or `implementation` changed, but
   cannot produce `changed_detail="edits.py"` or `"simulate_ac body"` as the
   proposed `Iteration` promises. `plan_bundles()` reduces a source declaration
   to an opaque digest at `exec/src/hedloom_exec/planned.py:254-267`, then stores
   that reference in the bundle at `:273-295`. Neither the source address nor a
   per-input explanation survives in `input_digests`. More explanatory material
   must be recorded, or `changed_detail` must be narrowed.

## Phase 0 change census

### Pure workspace resolution

| Exact site | What it does today | What Phase 0 requires |
| --- | --- | --- |
| `exec/src/hedloom_exec/artifacts.py:28-34` | Exports `workspace_for` only. | Export the new pure `workspace_path`. |
| `exec/src/hedloom_exec/artifacts.py:75-84` | Joins `root / identity`, creates the directory and returns it. | Split the join into `workspace_path(root, name)`, which performs no `stat` and creates nothing; retain `workspace_for` as the creating execution wrapper. |
| `exec/tests/test_artifacts.py:1-214` | Covers capture and execution workspaces, but not a pure resolver. | Add the four Part 3 resolver tests. A no-`stat` test must monkeypatch the returned `Path` operation or `Path.stat`, because non-existence alone proves only no creation. |

This item agrees with the code and is independently possible.

### Try number and the `created` event

| Exact site | What it does today | What Phase 0 requires |
| --- | --- | --- |
| `exec/src/hedloom_exec/identity.py:29-40` | `AttemptIdentity` retains `sequence`. | No identity change in this phase; pass this object far enough to record its sequence. |
| `exec/src/hedloom_exec/durability.py:55-87` | `_select_sequence()` constructs an `AttemptIdentity`, discards it, and returns `.rendered`. | Return the `AttemptIdentity` object (or both rendered identity and sequence) so derived calls know the try number. |
| `exec/src/hedloom_exec/durability.py:108-193` | `execute()` accepts a caller identity string, derives a string for normal recorded execution, creates the journal/workspace, then calls `launch_or_attach`. | Carry `try_number` only when identity was derived. A caller-supplied identity has no trustworthy sequence, matching the plan's omission test. |
| `exec/src/hedloom_exec/attempt.py:221-230` | Appends `created` once with `plan`, `invocation`, `operation`, and `input_digest`. | Add `try` when known without changing any identity input or protocol ordering. |
| `exec/tests/test_journal.py:1-104` and new `exec/tests/test_created_event.py` | Journal tests cover append/fold mechanics, not execution attribution. | Put the Part 3 execution-level created-event tests in the new file; keep journal mechanics where they are. |

The plan calls `try` attribution-only, which agrees with the code. The needed
signature change is not shown in Part 3: `_select_sequence()` cannot continue
to return `str` if the object is to be threaded. That private return type should
change; the public `execute()` signature need not change for this field.

### Authored key

| Exact site | What it does today | What Phase 0 requires |
| --- | --- | --- |
| `exec/src/hedloom_exec/planned.py:46-57,297-307` | `PlannedInvocation` already carries `authored_key` beside its bundle. | Preserve it through binding into execution attribution. |
| `run/src/hedloom_run/binding.py:134-162` | `build_bundle()` copies `item.bundle`, then adds placement, resolved inputs and optional outputs; it drops `authored_key`. | Add `authored_key` to the execution bundle as non-identity-bearing attribution. This shared site keeps sequential and graph kernels identical. |
| `exec/src/hedloom_exec/reuse.py:44-54` | `IDENTITY_KEYS` excludes `authored_key`, `plan`, `invocation`, placement and resolved inputs. | Keep that exclusion; recording attribution must not move identity. |
| `exec/src/hedloom_exec/attempt.py:221-230` | `created` does not record `authored_key`. | Append it when present, and omit it rather than writing a misleading value when absent. |
| `run/src/hedloom_run/driver.py:200-217` and `run/src/hedloom_run/graph.py:174-191` | Both use `build_bundle()` before `execute()`. | No duplicated kernel-specific binding if the shared bundle builder carries the field. |

This item agrees with the code. The plan says the authored key “already flows
through the run into the report”; that is true (`driver.py:242-253` and
`graph.py:203-217`) but it does not flow into the bundle or executor today.

### Superseded identity

| Exact site | What it does today | What Phase 0 requires |
| --- | --- | --- |
| `exec/src/hedloom_exec/reuse.py:87-156` | `AttemptRecord` exposes no creation time or authored key; `scan_attempts()` reads the first `created` event and `attempts_for()` filters by plan/invocation. Directory-name sort is not iteration order. | Expose enough created-event metadata to select a prior record deterministically, including `authored_key`, `created_at`, `supersedes`, `input_digests`, and try when present. Old four-field records must remain readable. |
| `exec/src/hedloom_exec/reuse.py:159-183` | `stale_attempts()` recomputes prior-digest records by comparing `input_digest`. | It may remain as the compatibility/read-side query; Phase 0 adds durable attribution without changing this behaviour. |
| `exec/src/hedloom_exec/durability.py:156-193` | Derives the current digest and identity, but never scans records for the same plan/invocation. | Before a genuinely new record is created, select the most recent other-digest record and pass its identity as `supersedes`; do not mark same-digest retries as superseding themselves. |
| `exec/src/hedloom_exec/attempt.py:221-230` | Writes no supersession link. | Write the selected prior identity in the one-time `created` event. |

The current code makes the initial A -> B link possible, but the plan's stronger
iteration-chain claim is contradicted by the once-only event, as described in
the verdict. Selecting “most recent” also requires a rule absent from the
proposed signatures; event timestamp is the available durable ordering fact.

### Component input digests

| Exact site | What it does today | What Phase 0 requires |
| --- | --- | --- |
| `exec/src/hedloom_exec/reuse.py:44-84` | Filters present non-`None` identity keys into one mapping, canonicalizes the whole mapping, then hashes it once. | A new component-digest API and durable mapping, while keeping the existing aggregate digest byte-for-byte unchanged. The required recombination property cannot be met with component hashes under that constraint. |
| `exec/src/hedloom_exec/planned.py:273-295` | Builds the bundle and stores only the aggregate `input_digest` on the planned invocation. | Either recompute component hashes in execution or add them to `PlannedInvocation`; the former avoids a new run-unit contract but does not solve the recombination contradiction. |
| `exec/src/hedloom_exec/attempt.py:221-230` | Recomputes only the aggregate digest for `created`. | Record component evidence without allowing it to enter `IDENTITY_KEYS`. |
| new `exec/tests/test_created_event.py` | No component evidence tests exist. | Add the coverage listed in Part 3, except the recombination test cannot truthfully pass under the current algorithm. |

This is the blocking contradiction.

### Reproducible attempt census tool

| Exact site | What it does today | What Phase 0 requires |
| --- | --- | --- |
| new `tools/attempt-census.py` | There is no repository `tools/` census script. | Add a dependency-free reader that reports the measured sequence/group counts without importing `hedloom_flow` or Dask and without creating paths. |

The design record does not specify the tool's CLI or output schema. That is a
missing signature rather than a contradiction, but it requires a choice before
implementation if reproducibility means machine-stable output.

## Phase 0b change census

### Alias primitives and execution integration

| Exact site | What it does today | What Phase 0b requires |
| --- | --- | --- |
| new `exec/src/hedloom_exec/alias.py` | No alias model exists. | Implement pure `alias_path`, atomic `point_alias`, and read-only `aliases_into`; relative symlink targets make moved trees usable and `os.replace` supplies atomic repointing. |
| `exec/src/hedloom_exec/__init__.py:15-39` | Re-exports artifacts, attempt, durability, identity, journal, reuse and transport. | Re-export alias and lineage public APIs if they are meant to be `hedloom_exec` surfaces. |
| `exec/src/hedloom_exec/durability.py:108-120,184-193` | `execute()` knows record root, optional workspace root, and output declarations, but neither authored key nor latest root. It prepares the workspace immediately before launch. | Point aliases for `{"path": ...}` outputs after workspace creation and before `launch_or_attach`; never create the output target. Add explicit alias-root/authored-key inputs or define a derivation contract. |
| `run/src/hedloom_run/site.py:99-107` | `Site` has independent `root` and optional `workspace_root`; they need not be siblings. | The plan must say where `latest/` lives. It cannot always be inferred from record root, and deriving it as a sibling of an arbitrary workspace root is a new site-layout contract. |
| `run/src/hedloom_run/driver.py:97-112,209-217` and `run/src/hedloom_run/graph.py:87-100,183-191` | Run signatures/config carry record and workspace roots only. | Carry an explicit latest root through both kernels if the site owns it. |
| `exec/src/hedloom_exec/artifacts.py:87-106,109-158` | A path output is checked only after success. Existence counts as production. | Leave this unchanged. Alias creation must not touch or pre-create the target; the intentional dangling window remains. |

The three alias primitive signatures are viable, but their integration signature
is incomplete: `execute()` cannot call `point_alias()` without a latest root and
authored key. The user-facing name `latest/` is settled; its parent is not.

### Lineage and rerun explanation

| Exact site | What it does today | What Phase 0b requires |
| --- | --- | --- |
| new `exec/src/hedloom_exec/lineage.py` | No lineage reader exists. | Read Phase 0 fields into `Iteration`, compare component evidence, and detect stale paths. |
| `exec/src/hedloom_exec/reuse.py:87-134` | Scan results omit authored key, creation timestamp, supersedes, component evidence, sequence, and workspace. | Extend the additive reader model or have lineage reopen every journal. Missing Phase 0 fields must remain accepted. |
| `exec/src/hedloom_exec/journal.py:247-276` | Every event has a durable timestamp, but only when appended. | A one-time `created.at` can order first creation; it cannot timestamp later returns to a reused identity. |
| `exec/src/hedloom_exec/planned.py:254-295` | Source and upstream identities are opaque digest strings by the time the final bundle is formed. | Additional explanatory data is required for filename/body detail; component hashes alone support only changed key names. |
| `run/src/hedloom_run/driver.py:43-56,89-94` | Outcomes and summaries carry disposition/outcome but no rerun reason. | Add an optional reason/detail to the execution result and invocation outcome, then render it only for reruns. |
| `src/hedloom/session.py:43-52` and `src/hedloom/study.py:262-275` | Live reporters print disposition, authored name, outcome and error. | Print rerun reasons for claimed changed-input work and `reused` for completed work without inventing reasons. |

`why_reran(prior, current) -> tuple[str, ...]` is implementable for key names.
`Iteration.changed_detail` is not implementable from the planned durable data,
and `lineage()` cannot faithfully represent A -> B -> A using only once-written
supersedes fields.

### `where`, `check`, and `log`

| Exact site | What it does today | What Phase 0b requires |
| --- | --- | --- |
| `pyproject.toml:5-33` | Defines the `hedloom` distribution but no console script. | Add a `hedloom` entry point and a command dispatcher, or explicitly scope tests to a callable CLI main. |
| new `src/hedloom/cli.py` | No operator CLI exists. `src/hedloom/visualize.py:313-338` has only a private standalone main. | Parse `where`, `check`, and `log`; use exec readers rather than importing Dask directly. |
| `exec/src/hedloom_exec/reuse.py:87-156` | Human selectors cannot be resolved because scan records omit authored key. | Resolve `(plan, authored_key)` from Phase 0 records and refuse zero/ambiguous matches. |
| `exec/src/hedloom_exec/journal.py:384-459` | A terminal manifest records artifact addresses and publication time. Running output addresses exist only as predicted workspace paths/aliases. | `where` should resolve the stable alias path so it also works during execution, which again requires an unambiguous latest root. |
| new `tests/test_where_check_log.py` | No operator-surface tests exist. | Add the seven Part 3 tests after the root/layout and lineage semantics are settled. |

The command spelling is clear, but the proposed CLI takes only `<root>` while
the code permits record root and workspace root to be unrelated. Consequently
`where` cannot locate `latest/` from its documented arguments in every valid
current `Site` configuration.

## Contracts that would move if the blockers were resolved

Phase 0's additive event fields and pure resolver do not change existing
behaviour, but Phase 0b adds a supported stable current-result view and an
operator CLI. In the implementation commit, the following maintained surfaces
would need to move with code:

- `exec/ONTOLOME.md:42-154` — durable attribution, component explanation,
  lineage readers, and aliases while preserving dependency freedom.
- `ONTOLOME.md:45-122` — the operator-facing current-result view and commands.
- `exec/README.md` and root `README.md` — discoverable usage, including the
  dangling window, partial-file reload, held-descriptor limitation, and the
  fact that aliases do not weaken content-addressed identity.
- `docs/guide/results.md` (and the relevant docs index) — stable view versus
  identity-named evidence.

No directory-output fix belongs in those changes. No Phase 1 record/workspace
split, layout version, per-try manifest, pruning, or pins belongs in this PR.

## Required decisions before another implementation pass

1. Keep the current aggregate digest and weaken the component contract, or
   change the digest algorithm in a contract-changing phase.
2. Decide whether lineage records every selection (including A -> B -> A) or
   only first creation of each distinct identity.
3. Decide what durable material supports `changed_detail`, or narrow it to
   changed identity-key names.
4. Define the parent of the settled `latest/` directory and expose it in the
   run/CLI signatures; one `<root>` is insufficient when record and workspace
   roots are independent.

Until these are settled, implementing a nearby design would be improvisation,
which the implementation brief explicitly forbids.
