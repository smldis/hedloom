# Census: what a per-invocation attempt record would touch

**Written 2026-08-26. A census, not a proposal.** It measures the current tree
against a candidate change. Nothing here is built, and the numbers are from this
date; rerun them before acting.

The candidate under examination, from discussion on this date:

> One attempt **record** per `(plan, invocation, input_digest)`, named by an
> identity with no sequence in it. One **workspace** per try, named
> `<identity>-<seq>`. The sequence and the per-try history live inside the
> record's own append-only log.

Two questions: is it the right shape to commit to, and what in the tree today
would break if it landed.

## Part 1 — the shape

Four candidates were on the table. Scored against what the last week of
questions actually exposed.

| | **A** slot index | **B** supersede & archive | **C** record + numbered workspace | **D** database |
| --- | --- | --- | --- | --- |
| Sequence lives in | symlink filename | archive path timestamp | the record's log **and** workspace name | a column |
| Identity means | try N at these inputs | the current try | **the work at these inputs** | either |
| Probe cost | one `readdir` | one `open` | **one `open`** | index lookup |
| Anything moves on rerun? | no | **yes — atomic rename** | **no** | no |
| Failure budget | still capped | retention | **retention** | retention |
| Failure history readable by | walking a tree | walking a tree | **folding one file** | one query |
| Storage collector | filter by outcome | one directory | **`work/<id>-<n>` below current** | a query |
| New durable state to keep consistent | an index tree | none | none | the database |
| Contract surface changed | none | identity | identity | everything |
| NFS posture | improves (atomic `symlink`) | unchanged | unchanged | **much worse** |

**C is the recommendation**, and the reason is not any single row. It is that
the sequence is an *ordinal smuggled into a content address*. Every other
component of the identity hash is a fact about what the work **is**; the
sequence is a fact about how many times it has been tried, which is history.
C puts each fact where it belongs — the content address stays content-addressed,
the history goes in the log that already exists for history, and the try
distinction goes on the workspace, which is the only thing that actually needed
distinguishing (*"a rerun after a failure must not write over the evidence of
what the previous attempt produced"* is a statement about a directory of bytes,
not about a name).

Most of this week's awkwardness is downstream of that one conflation: the
unrecordable sequence, the `max_attempts` cap that errors, and the third state
(record valid, bytes absent) with nowhere to live.

D is recorded here only to close it: `open-concepts.md` lists the file-first
sidecar under **Realized** as *"deliberately not a workflow database"*, and the
`claim()` DEVNOTE already doubts POSIX locks on NFS — which SQLite depends on
more heavily and fails harder. As a *derived, disposable index* it stays
available later; as the record it is a reversal without a trigger.

## Part 2 — the coupling census

### The identity surface is remarkably small

`attempt_identity()` has **exactly one consumer outside its own module**:

```
exec/src/hedloom_exec/durability.py:72     inside _select_sequence
```

That is the entire blast radius of the identity derivation itself. Everything
else consumes an identity *string* that someone else chose.

### Fixed names, and who owns them

| Site | Assumes | Under C |
| --- | --- | --- |
| `journal.py:145` `log_path` | `<dir>/events.jsonl` | unchanged |
| `journal.py:146` `manifest_path` | `<dir>/manifest.json`, **one file** | **breaks** |
| `journal.py:147` `lock_path` | `<dir>/claim.lock` | unchanged, scope widens |
| `reuse.py:116` | a dir with `events.jsonl` is an attempt | unchanged |
| `watch.py:222` | same test | unchanged |
| `watch.py:100` | `<dir>/observations.jsonl` | unchanged |
| `artifacts.py:167,169` | `stdout.log`, `stderr.log` in the workdir | unchanged |
| `artifacts.py:75` `workspace_for(root, identity)` | workdir == identity | **changes** (and it `mkdir`s — see below) |

### The breaks, ranked by how quietly they fail

**1. `watch.py:303` — silent, and the most dangerous.**

```python
seen = states.get(item.identity)      # LSF job_name, matched exactly
if seen is None:
    continue                          # records nothing, reports success
```

The watcher matches an LSF job name against the **record directory name**. Under
C the job must be submitted as `-J <identity>-<seq>` (one job per try), so every
lookup misses, every attempt hits `continue`, and the watcher reports a healthy
empty sweep while observing nothing.

This exact failure has already happened once. From `open-concepts.md`: the
watcher *"matched `lsf-interactive` while façade journals recorded only
`bound:lsf-interactive`, and an empty sweep is indistinguishable from a finished
one."* It was fixed 2026-08-16. C reintroduces the same shape unless the match
is made explicit — and the honest fix is that the watcher should key on the
**try name**, which is what LSF knows about, not on the record name.

**2. `journal.py:146` — a single manifest path.**

`publish_terminal` does `os.replace(temporary, self.manifest_path)`. With N tries
in one record, try 1 overwrites try 0's published evidence — precisely the thing
the per-attempt workspace exists to prevent, reintroduced on the record side.
Manifests must become per-try (`manifest/<seq>.json`), with a `standing.json`
published by the same atomic rename so the reuse fast path stays one `open()`
and the existing recovery rule — *"manifest is the evidence; the journal is
repaired to match it"* — still holds.

**3. `lsf.py:523` and `lsf.py:581` — `bjobs -J` and `bkill -J`.**

Both take an identity and mean *a job*, which under C is a try. Passing the
record identity makes `bjobs -J` find nothing (indistinguishable from "never
accepted") and `bkill -J` silently cancel nothing. Both must take the try name.

**4. `artifacts.py:75` `workspace_for` calls `mkdir(parents=True, exist_ok=True)`.**

It is a *creating* resolver. Any caller that wants to locate a workspace without
creating it — a collector, a census, a `--dry-run` — currently creates the
directories it is asking about. Needs a pure sibling.

### What does *not* break, checked rather than assumed

**`fold()` already handles a multi-try journal correctly.** It is a last-write-
wins sequential scan: `submit_intent` sets `phase="intended"`, a receipt sets
`"submitted"`, `terminal` sets `"terminal"` and the outcome. A log holding
`created, intent, receipt, terminal(failed)` then `intent, receipt,
terminal(succeeded)` folds to `phase=terminal, outcome=succeeded` — the right
answer, and a new try's intent naturally resets the phase.

What it cannot do is attribute events to tries, because nothing marks a
boundary. That is where the sequence gets recorded: a `try` field on each event,
or an explicit `attempt_started` event carrying it. Correctness of the *current*
state needs neither; *history* needs one of them.

**`claim.lock` improves.** Today it is per-try, so two controllers can both probe,
both find slot 1 free, and only then contend — a TOCTOU window. Under C the lock
is per-input-set and the race is settled before anyone picks a try.

**`StaleIdentity` improves.** It refuses a record created from different inputs.
Today that guards a per-try directory; under C it guards a per-input-set record,
which is what it was checking for.

**`execute()` stays source-compatible.** `max_attempts` is passed by nobody —
`driver.py:209` and `graph.py:183` both omit it — so removing or repurposing it
breaks no call site.

### Test baseline to protect

Measured 2026-08-26, before any change:

```
tests        49 passed,  2 skipped
exec/tests  183 passed
run/tests    73 passed,  2 skipped
flow/tests    2 errors            ← PRE-EXISTING, unrelated
            ─────────────────────
            305 passed,  4 skipped
```

`flow/tests` fails at **collection**, not assertion:
`ImportError: cannot import name 'refinement' from 'examples'` in
`test_acceptance.py` and `test_local_dask.py`. A namespace-package resolution
problem that predates this work. **Recorded here so it is not later misread as a
regression caused by the rewrite.**

### Tests that are specifications, not chores

Nine files encode the contract. These must be rewritten by decision, never
"fixed until green":

```
exec/tests/test_identity.py          sequence renders distinct identities
exec/tests/test_failure_reuse.py     ← the specification of what changes
exec/tests/test_durability.py        slot selection
exec/tests/test_journal.py           one lifecycle per journal
exec/tests/test_reuse.py             scan/attempts_for/stale_attempts
exec/tests/test_review_fixes.py      sequence numbers without rereading the log
run/tests/test_placement.py
tests/test_farm_smoke_example.py
tests/test_farm_multi_client_example.py
```

`test_failure_reuse.py` is the load-bearing one. `test_repeated_failures_each_
get_their_own_attempt` asserts `len(scan_attempts(tmp_path)) == 3` — three
failures, three attempt directories. Under C that is one record with three
tries. **The assertion is a statement of the design being replaced**, so
changing it is the decision itself, and it should be changed in the commit that
states the new contract, not in a follow-up that makes the suite pass.

### Contract and documentation surfaces

`AGENTS.md` requires a contract change to move in the same commit. These state
the current contract and would become false:

```
exec/ONTOLOME.md                        identity, journal, reuse, watch
ONTOLOME.md                             attempt identity in the composition
exec/DECISIONS.md                       owner-bound lifetime argument
docs/internals/mechanism.md             §"The identity chain" — states the
                                        four-component derivation explicitly
docs/internals/attempt-claim-protocol.md
docs/internals/stop-admitting-protocol.md
docs/guide/results.md
exec/docs/index.md, run/docs/index.md
../docs/vision/open-concepts.md         register entry for the decision
```

### Existing roots

171 attempt directories in this tree. **120 of them (70%) already carry
identities that the current `attempt_identity()` cannot reproduce** — they are
rendered `ass-…` rather than `hedloom-…`, from an earlier scheme, with complete
`created` events whose sequence is permanently unrecoverable.

That is the precedent worth naming: the last time this rendering changed, old
records became partially unreadable and nothing migrated them. Decide
deliberately this time — dual-read, a migration, or an explicit statement that
roots written before the change are read-only.

## What to do before building

1. **Run the census script on a root with real work** (`attempt-census.py`).
   Two numbers decide whether any of this is urgent: *deepest sequence in use*
   and *groups with ≥5 attempts*. If the answer is 0 and 0, C is a
   simplification worth doing on its merits and nothing is on fire.
2. **Decide the watcher's key** — record name or try name — before writing any
   code, because it is the one break that fails silently.
3. **Rewrite `test_failure_reuse.py` first**, as the statement of intent. If the
   new assertions do not read better than the old ones, the design is wrong.
