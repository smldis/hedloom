# Plan: per-invocation attempt records, and an operator-run pruner

**Written 2026-08-27. A plan, not a description of the code.** Nothing here is
built. It follows `attempt-record-census-2026-08-26.md`, which measured what the
first half would touch; read that first for the coupling inventory and the test
baseline this plan must not regress.

Two things are being built, and they are separable. The first is a change to
what an attempt record *is*. The second is a storage pruner that only becomes
simple once the first has landed. They ship in that order and the first is
useful alone.

## Part 0 — the settled shape

Today one identity string names both the record and the workspace, and repeated
tries are distinguished by folding a `sequence` into the identity hash. That
conflates a fact about *what the work is* with a fact about *how many times it
has been tried*.

**The record loses the sequence. The workspace keeps it.**

```
attempts/hedloom-016ea5b1…/          one record per (plan, invocation, digest)
    claim.lock                       one writer, across every try
    events.jsonl                     append-only, all tries, boundaries marked
    manifest/0.json                  try 0's published result
    manifest/1.json                  try 1's
    standing.json                    what reuse returns — or absent

work/hedloom-016ea5b1…-0/            try 0's workspace, and its job name
work/hedloom-016ea5b1…-1/            try 1's
```

`attempt_identity()` takes three components — plan, invocation, digest — and
`<identity>-<seq>` is string formatting, not a fourth hash input.

**The rendered values change, and that is fine.** An earlier draft said the
record identity "is what it already returns for `sequence=0`", which is true of
the *meaning* and false of the *bytes*: the hash material joins four slots
(`identity.py:90-92`), so removing the sequence removes a slot and every
rendering moves.

Do not preserve a vestigial `"0"` in the material to keep the old values. There
is nothing to keep compatible with — this prototype supports no migration
(Part 4), and every run root on the machine was deleted on 2026-08-27. Freezing
a literal zero into the hash forever, to protect records that do not exist,
would keep the sequence inside identity while claiming to have removed it,
which is the whole change reversed for no benefit.

One test asserts a rendering: `test_an_identity_computed_before_phase_zero_is_unchanged_after_it`
(`exec/tests/test_created_event.py:108`). It was written to prove **Phase 0**
added nothing to identity, and it did its job. Phase 1 updates it to the new
rendering — deliberately, in the commit that states the new contract, like every
other test that specifies the design being replaced.

Consequences that are the point rather than side effects: identity becomes
purely content-addressed; slot selection stops being a probe; `max_attempts`
and its exhaustion error disappear, because *how many tries to retain* becomes
retention policy rather than an exception; and the pruner gets a stable
record to attach a pin to.

## Part 0b — the docs already say Option C is how it works

An independent census (codex, 2026-08-27, read-only) found three **maintained**
surfaces asserting something the current code contradicts, all the same thing:

| Surface | Says | Code |
| --- | --- | --- |
| `exec/ONTOLOME.md:54` | identity derives "from planning facts **alone**" | one of its four components is chosen by probing the filesystem (`durability.py:71`) |
| `docs/internals/mechanism.md:16` | "derives **every identity** as a pure function of that document" | the attempt identity folds in a state-selected sequence (`mechanism.md:154` says so, four sections later) |
| `exec/DECISIONS.md:184` | "`sequence` exists in the identity and is **otherwise unused**" | it selects the retry, and names the record directory, the workspace, the transport identity, the LSF job name and the manifest path |

This is worth more than a documentation bug. **Option C is the design these
three sentences describe.** Under it the record identity really is a pure
function of the plan document — plan, invocation, digest, all from the document
— and the try number really is otherwise unused, because it moves out of the
identity and into the record.

So this change does not require rewriting a contract around a compromise. It
makes three already-written contracts true. The alternative is to keep the code
and correct the documents *downward*, and nobody argued for that.

## Part 1 — implementation phases

Each phase is independently landable and leaves the suite green.

### Phase 0 — instrumentation, no behaviour change

Lands first because it makes every later phase checkable, because two of its
items are defects in their own right, and because **four of them are fields in
one event.** `created` is written once per record, at `attempt.py:221`, guarded
by `if not state.events:` — it is the record's answer to *what am I, and what
was I computed from?*, asked before anything is submitted. Everything added here
is the same shape as the four facts already in it: attribution-only, one read
away, and available at the moment it is written but currently thrown away.

- **`hedloom_exec.artifacts.workspace_path(root, identity)`** — a pure resolver
  beside `workspace_for`, which calls `mkdir(parents=True, exist_ok=True)` and
  therefore *creates* what a pruner or a dry run merely wants to locate.
  `workspace_for` keeps creating; callers that only read switch.
- **Record the try number in the `created` event.** `_select_sequence` builds an
  `AttemptIdentity` carrying `sequence` and immediately discards everything but
  `.rendered`; thread the object through `execute()` to `launch_or_attach` and
  record it. Correct under the current scheme too, and it makes the migration in
  Phase 1 verifiable rather than assumed.
- **Record `authored_key` in the `created` event.** Beside `plan`,
  `invocation` and `input_digest`, and attribution-only exactly as those are.
  It is already carried on every planned invocation (`planned.py:52`,
  populated at `:301`) and already flows through the run into the report
  (`driver.py:168`) — it simply never reaches the durable record. One field
  replaces the whole "generation record" idea: with it,
  `ota:tt_1v80_27c:simulate_ac` resolves to an identity from records alone,
  and no Plan document has to be persisted anywhere.
- **Record `supersedes` in the `created` event.** When a run derives a digest
  for an invocation that already has records at other digests, the new record
  names the one it displaces. `attempts_for(root, plan_id, invocation_id)`
  supplies it at plan time, before anything is spent. This is what makes a
  debug iteration a *chain* rather than a pile: `hedloom log` walks it, and the
  pruner gains a `superseded` condition, which is the rule that today has no
  way to see the orphans an edit leaves behind.
- **Record `input_digests` in the `created` event.** Nine digests, one per
  identity key, recorded **beside** the existing `input_digest` — not derived
  from it.

  *Corrected 2026-08-27 by the implementation census.* An earlier draft said the
  nine keys were "already digested individually inside `input_digest`" and that
  the function threw the parts away. **That is false.** `input_digest`
  (`reuse.py:63`) builds one canonical JSON mapping of the present identity keys
  and takes a single `blake2b` over the whole thing. There are no parts.

  The correction matters because of where the wrong version led: making the
  aggregate a hash-of-hashes would change **every input digest and therefore
  every attempt identity**, which is the opposite of "Phase 0 changes no
  behaviour" and would silently invalidate every existing record. The right
  shape is additive — compute nine extra digests for explanation, leave the
  aggregate byte-for-byte untouched.

  What that buys is narrower than the draft claimed, and Phase 0b's contract is
  narrowed to match: comparing two records names **which of the nine keys
  moved**, not what changed inside one. The bundle is never stored durably, so
  `inputs` moving is recoverable; *which* input, by its declared name, is not —
  see Phase 0b.
- **`tools/attempt-census.py`** — the census script into the repository, so the
  numbers in the design record can be reproduced rather than cited.

Together the four fields make `created` self-describing enough that Phases 2–5
need no new durable state of their own — no generation record, no plan
document, no index.

### Phase 0b — following the current result

Depends only on Phase 0, so it can land while Phase 1 is still being written —
and it is the part that makes daily development bearable, which argues for
early rather than tidy.

The problem it answers is stated in
`design/iterating-on-a-study-2026-08-27.md`: editing an input mid-development
moves the digest, which moves the identity, which moves the directory you were
watching. Content addressing requires that; nothing here weakens it. **The
identity moves; the view stays.**

- **The alias tree lives at `<Site.root>/latest/` and is built by default.**
  (Decided 2026-08-27, user.) The census found the tree had no declared home;
  the answer is not a third configurable root but a fixed location under the one
  root that is always present. `Site.root` points at the attempts directory
  itself (`examples/farm-smoke.site.toml:4`), so the aliases sit beside the
  records they describe. No `latest_root` setting, no opt-in, nothing to
  configure — a run that writes records writes aliases.

  **The consequence to hold on to:** `latest/` then sits *inside* the directory
  that `scan_attempts` (`reuse.py:115`) and `watch.live_attempts`
  (`watch.py:221`) iterate. Both skip it today, because both guard on
  `(directory / "events.jsonl").exists()` before treating an entry as an
  attempt. That guard stops being incidental and becomes load-bearing: every
  present and future reader of the attempts root must skip a non-record entry,
  and a test says so rather than leaving it to be rediscovered.
- **`latest/<plan>/<authored-key>/<output>`** — a symlink per declared output,
  created when the workspace is prepared, *before the command launches*, and
  repointed at the start of each try.

  ```
  latest/ota/tt_1v80_27c:simulate_ac/raw  ->  ../../../work/hedloom-016ea5b1-0/ota_ac.raw
  ```

  `open()` follows symlinks, so a viewer opens the stable name, lands on the
  workspace file, and reads it **as the simulator extends it** — the streaming
  case, which nothing that publishes at end-of-run can serve. Reload re-opens
  the name and follows it again. Zero bytes are duplicated: the output exists
  once, in the workspace, and the alias is a name for it.

  Not `.latest`. It is a surface users are meant to find, `ls`, and hand to a
  tool; hiding it would say the opposite of what it is for.

  The address is known before the work runs — `out.raw` resolves at bind time
  from the declared output binding — which is exactly what makes the alias
  available *during* execution rather than after it.

- **`hedloom where <selector> --output <name>`** — resolve, for scripts. A
  program that caches a path goes stale; one that asks each time cannot. This
  is the project's own idiom: the address is recorded, and resolving a recorded
  address is an operation rather than a constant.
- **`hedloom check <path>`** — the inverse, so a consumer that *did* cache a
  path can ask whether it is behind, and exit non-zero rather than silently
  plotting last hour's data. Needs `supersedes` from Phase 0.
- **`hedloom log <selector>`** — the iteration chain, newest first, each line
  naming which identity key moved. Needs `supersedes` and `input_digests`.
- **The rerun reason, printed as it happens.** Costs nothing durable: the run
  already holds both bundles when it decides to rerun.

  ```
  invoke  tt_1v80_27c:simulate_ac   rerun: inputs changed (edits.py)
  invoke  tt_1v80_27c:evaluate      rerun: upstream
  invoke  ff_1v98_m40c:simulate_ac  reused
  ```

Three costs, all stated rather than discovered. **A dangling window** between
repointing the alias and the tool's first write, where a reload fails to open —
short, and it fails visibly. Hedloom must *not* pre-touch the declared output to
close it, because `capture_outputs` treats an existing declared output as
evidence the work produced it, and a placeholder would let a command that wrote
nothing report success. **A coupling to the pruner:** a symlink does not keep
its target alive, so `aliased` joins the skip reasons and an aliased workspace
is never a candidate. **A reload mid-iteration lands on a partial file**, which
is what "current" means and is the behaviour asked for — but the file is
genuinely incomplete rather than merely new, so it is documented.

Two things deliberately not built. **In-place publication is dropped**
(2026-08-27, user): once the alias points at the workspace file there is no
second copy and no `atomic`/`in-place` choice to make, and a viewer that
auto-reloads by polling `fstat` on a *held* descriptor is not served — stated
rather than worked around, because after a repoint the inode it holds genuinely
did not change. And **directory outputs are out of scope here**, being worked
separately; the finding that matters to this plan is recorded in Phase 2.

### Phase 1 — the record / workspace split

The substantive change. One commit, because the pieces are not independently
correct.

1. **Identity.** `execute()` derives the record identity from
   `(plan_id, invocation_id, input_digest)`. `_select_sequence` is replaced by
   `next_try(journal)`, which folds the record and returns the next try number —
   a read of one file, not a probe over twenty paths. `max_attempts` is removed
   from `execute()`'s signature; no caller passes it.
2. **Workspaces and job names.** The workspace becomes
   `workspace_path(workspace_root, f"{identity}-{seq}")`, and the same string is
   what `LSFInteractiveTransport` submits as `-J`. `discover()` and `cancel()`
   take the try name, not the record identity.
3. **Every consumer of the job name.** Three code paths pass an identity to a
   transport expecting it to name *a job*. Under C a job is a try, so all three
   must be given the try name — and they fail differently:

   - **Recovery discovery — the most dangerous thing in this plan.**
     `_launch_or_attach_locked` calls `transport.discover(journal.identity)`
     (`attempt.py:204`) to settle the crash window after a lost receipt. Given
     the record identity it asks LSF about a name no job carries, finds
     nothing, and — where `discovery_is_authoritative` — concludes *never
     accepted* and submits again. **That is duplicate farm work under one
     identity: the exact defect the whole claim protocol exists to prevent.**
   - **Cancellation.** `bkill -J <record>` matches nothing and silently
     cancels nothing (`lsf.py:581`).
   - **The watcher.** `watch.py:303` matches job names against attempt
     identities by exact equality; keyed on the record it misses every lookup,
     skips every attempt, and reports a healthy empty sweep. The same shape of
     bug was fixed here on 2026-08-16 — do not reintroduce it.

   Under C the journal is per-record and so no longer knows the try name by
   itself. It has to come from the folded state, which is why item 5's
   partition and item 7's ordering rule are prerequisites for this one rather
   than independent work.
4. **Manifests.** `journal.manifest_path` is one fixed file published by
   `os.replace`; under a multi-try record, try 1 overwrites try 0's evidence.
   Manifests become `manifest/<seq>.json`, plus `standing.json` published by the
   same atomic rename, so the reuse fast path stays one `open()` and the
   existing recovery rule — *manifest is the evidence, the journal is repaired
   to match it* — still holds.
5. **Try boundaries, and the sticky fields.** Every event carries `try`, and
   `fold()` gains `state.tries`, a per-try view.

   `phase`, `outcome`, `manifest_path`, `handle`, `transport`, `substrate` and
   `placement` are assigned by a last-write-wins scan, so they already mean
   "the latest try" for a multi-try log and need no change.

   **Three fields do not, and this was missed in the first draft of this plan.**
   `cancel_requested`, `reuse_accepted` and `observations` are *sticky* — set
   once and never reset (`journal.py:346`, `:352`, `:354`). A multi-try record
   therefore folds to a mixed state: some fields describe the latest try, some
   describe the record's whole lifetime, and nothing marks which is which. Two
   concrete contaminations follow:

   - `accept_for_reuse` on try 0 sets `reuse_accepted` for the record's whole
     lifetime, so **`reuse_accepted` needs a `try` field for exactly the reason
     `pinned` does** — the two fall out of this partition rather than being
     separate jobs.
   - A cancellation recorded against try 0 leaves `cancel_requested` true
     forever. Try 1's `submit_intent` resets `phase` out of terminal, and with
     per-try manifests `read_manifest()` for the new try is `None` — so
     `_launch_or_attach_locked` (`attempt.py:168`) raises `AttemptCancelled`
     for a try nobody cancelled.
   - `accept_for_reuse` on try 0's failure leaves `reuse_accepted` true, so
     `is_reusable` returns true for whatever manifest is present at a later
     try — a failure standing in as a result it was never inspected as.

   So the fold must partition: sticky fields become per-try, with an explicit
   record-level view only where one is meaningful. This is the largest single
   piece of work in Phase 1 and it is not optional.
7. **The record declares its layout.** `attempts/<identity>/layout` holds a
   single integer, written when the record is created. `hedloom_exec` refuses a
   layout it does not recognise, by version, rather than misreading it — the
   discipline `hedloom_flow` already applies to the Plan's `schema_version`.
   This is what makes the no-migration policy checkable and the pin promise
   expirable rather than quietly false. Cheap now; impossible to add
   retroactively, as the 120 unrecognisable records demonstrate.

8. **A new ordering rule, and the window it closes.** Today the identity is a
   pure function chosen before submission and *is* the lookup name, so
   "durably recorded before the transport is called" comes free. C chooses a
   try number, and a number that is not durable before `transport.submit` is a
   name that cannot be recovered: a crash after acceptance leaves an accepted
   job nothing in the record can ask about.

   **The try marker is flushed and fsynced before `submit_intent`**, which
   itself precedes any transport call. That makes the try name recoverable in
   the same window the identity already is.

   The other side of the window must also be stated, because it is currently
   undefined: a crash *before* intent leaves an allocated try with no
   submission. **Such a try is resumable — the same number is reused**, because
   nothing was ever submitted under it. This matches how `phase ==
   "unsubmitted"` is already treated and stops an interrupted run from burning
   try numbers.

6. **Contracts.** `exec/ONTOLOME.md`, `ONTOLOME.md`, `exec/DECISIONS.md`,
   `docs/internals/mechanism.md` §"The identity chain",
   `docs/internals/attempt-claim-protocol.md`,
   `docs/internals/stop-admitting-protocol.md`, `docs/guide/results.md`, and the
   `open-concepts.md` register entry move in this commit, per `AGENTS.md`.

### Phase 2 — the pruner, read-only

Classification and a dry run that spends nothing. Ships alone and is useful
alone: it answers "where is my storage going" without deleting anything.

### Phase 3 — the pruner, destructive

`--apply`, the `workspace_removed` event, and the claim discipline.

### Phase 4 — pins

Operator protection, and the selector grammar that makes it usable.

### Phase 5 — site policy and the automatic trigger

`[retention]` in the site profile, and a bounded post-run pass.

## Part 2 — the user-facing API

### Command line

```console
hedloom prune --site site.toml [options]          # DRY RUN unless --apply
hedloom pin   --site site.toml <selector> --reason TEXT
hedloom unpin --site site.toml <selector> --reason TEXT
hedloom pins  --site site.toml

hedloom where --site site.toml <selector> --output NAME   # resolve, for scripts
hedloom check --site site.toml <path>                     # is this path behind?
hedloom log   --site site.toml <selector>                 # the iteration chain
```

**A site, not a positional root.** An earlier draft took one `<root>`. The
census caught that `Site` already permits `root` and `workspace_root` to be
independent paths on different filesystems (`site.py:103`, `:105`), so one
argument cannot name what `prune` needs — it reads records under one and
reasons about bytes under the other.

The alias tree is *not* a third thing to pass: it is `<root>/latest/`, derived.
So `where`, `check` and `log` need only `--root`, and `--workspace-root` is
required just by `prune` and `pin`. A command given fewer roots than it needs is
refused rather than inferring one from another, which would be the same guess in
a friendlier costume.

`prune` prints a survey and spends nothing. `--apply` is a second, deliberate
gesture. This is the same shape the unit already has — `summary()` shows what a
run will do, `submit()` spends — and it holds for the same reason: a destructive
plan should be inspectable, and diffable in a review, before it costs anything.

```
Selection (ANDed within one invocation)
  --outcome failed,cancelled     terminal outcomes to consider
  --failed                       shorthand for the above
  --older-than 30d               published_at older than
  --larger-than 1GiB             workspace bytes at least
  --keep-latest N                spare the N newest tries per record
  --plan NAME                    restrict to one study
  --invocation KEY               restrict to one authored key

Behaviour
  --site site.toml               read [retention] rules; CLI flags override
  --rule NAME                    run only this named rule from the site
  --apply                        actually remove. Without it, nothing changes
  --json                         machine-readable plan, for CI
  --limit-bytes 500GiB           stop once this much has been freed
```

**Selection composes by AND, never OR.** Adding a criterion must *narrow* what
gets deleted; a flag that widens a destructive operation is a trap. Disjunction
exists, but only as named rules (below), where each branch is written out and
reviewable rather than inferred from a flag combination.

Two things are not selectable at all: `unreconciled` attempts, because that
outcome *is* the evidence that the record and the substrate disagreed; and
anything inside the global floor.

### Policy file

Lives in the **site**, beside placements and roots, because retention is a
property of where work lands rather than of what the work means. A study run at
two sites should keep for a week at one and a year at the other without its
text changing.

```toml
[retention]
floor = "7d"                  # nothing younger than this, in any rule, ever
                              # "unreconciled" is never selectable; not a knob

[[retention.rule]]
name        = "spent failures"
outcome     = ["failed", "cancelled"]
older_than  = "14d"
keep_latest = 1               # the default: the newest try of a record always stands
keep_logs   = true            # stdout.log / stderr.log survive regardless

[[retention.rule]]
name        = "bulky old results"
outcome     = ["succeeded"]
older_than  = "90d"
larger_than = "1GiB"
keep_latest = 1

[retention.automatic]
after_run = ["spent failures"]   # rules a finished run may apply, bounded
```

**Rules OR with each other; conditions AND within a rule.** That is the shape
operators already know from firewall and lifecycle policies, and it makes the
mixed case explicit. An unrecognised key is refused rather than ignored. A rule
that would select `unreconciled` is refused. A rule with no condition at all is
refused, because "everything" should require typing everything.

`[retention.automatic]` names which rules a finished run may apply by itself.
The study never decides — `submit(prune=…)` is the tripwire, exactly as
`retry=` and `until=` already are, for the same reason: *a study decides what is
produced; it never decides what is kept.*

### Python — names chosen against the existing namespace

`Policy` and `plan` are **already public in `hedloom`** (`src/hedloom/__init__.py:53`
and `:75`, re-exporting `hedloom_flow.Policy` and `hedloom_flow.plan`). A
pruner called `prune.Policy` / `prune.plan` would shadow the authoring
vocabulary in the operator-facing package. So: `RetentionPolicy`,
`RetentionRule`, and `survey()` — which also says the right thing, since a
survey looks and does not touch.

```python
from hedloom import Site
from hedloom_exec import prune

policy = prune.RetentionPolicy.from_toml(site.retention)
found  = prune.survey(root, policy, workspace_root=site.workspace_root)

found.summary()          # str, spends nothing
found.candidates         # tuple[Candidate, ...]
found.skipped            # tuple[Skip, ...], each with a reason
found.freed_bytes        # int
report = found.apply(limit_bytes=None, actor=None)   # the only thing that removes
```

### What a pin promises

A pin is not "skip the pruner". That is a consequence, not the meaning.

> A pinned workspace's **content is immutable** and its **path is stable**,
> because someone holds a filesystem path to it and expects to read it later.

Three obligations follow: never deleted, never modified, never moved. The third
is the expensive one — a pinned workspace's path is a published external
reference, so it outranks any later convenience.

### How long the promise holds, and how it says so

**Scoped 2026-08-27 (user).** The promise holds *while the prototype does not
change underneath it.* That is what `Development state: prototype` already
declares in all four ontologies, and it is why that declaration is load-bearing
rather than decorative. So obligation 3 is not "forever". It is:

> A pinned workspace's path is stable **for as long as the record layout that
> wrote it is the one in use.** Change the layout and every pin under it is
> void — not silently broken, *void and reported as void.*

The distance between those two is the whole design. A promise that quietly
stops being true is worse than no promise; a promise that can state its own
expiry is honest at prototype stage and needs no walking back at 1.0.

**Which means the record must declare its layout version, and today it does
not.** `hedloom_flow` already does this properly: the Plan document carries
`schema_version`, currently 3, and `plan_bundles` accepts 2 and 3 while
refusing anything else *by version* rather than misreading it. The attempt
record declares nothing at all.

That gap has already cost something measurable. The 120 `ass-…` records in this
tree were not merely unreadable — they were **unrecognisable**. Nothing in them
said which format they were, so the only way to find out was to try the current
derivation and watch it fail. A declared layout would have identified them in
one read.

Option C changes the record layout, which makes this the moment to add the
version rather than a later tidy-up:

```
attempts/<identity>/layout          "1"    ← one line, written at creation
```

A pin then records the layout it was made under; `verify()` reports
`layout-changed` as an outcome distinct from `drifted`; and the pruner
refuses a root whose layout it does not recognise instead of surveying it
wrongly. This is also what makes "we do not support migration" (Part 4) a
*checkable* statement rather than a declared one.

### Selecting a thing to pin or prune

```console
hedloom pin ./_runs ota:tt_1v80_27c:simulate_ac --reason "reference for the report"
hedloom pin ./_runs hedloom-016ea5b1#2          --reason "the try that reproduced it"
hedloom pin ./_runs hedloom-016ea5b1            --reason "..."   # sugar: every terminal try
```

| Form | Means |
| --- | --- |
| `hedloom-016ea5b1` | a record, by identity or unique prefix |
| `hedloom-016ea5b1#2` | one try of that record |
| `<plan>:<authored-key>` | the human name — `ota:tt_1v80_27c:simulate_ac` |

An ambiguous prefix is refused with the candidates listed, git-style.

**The third form needs no plan document.** An earlier draft proposed persisting
the whole Plan as a "generation record" so a human name could be resolved to a
hash. That is a great deal of machinery for one lookup, and it is unnecessary:
the authored key is already carried on every planned invocation
(`planned.py:52`, populated at `:301`) and already flows through the run into
the report (`driver.py:168`). It simply never reaches the durable record.
Recording it in `created` — Phase 0, one field, attribution-only exactly like
`plan` and `invocation` already are — makes every selector resolvable by
reading records alone.

**The generation record is dropped from this plan entirely.** Nothing else
needed it: liveness was its other job, and pruning does not need liveness
because it only ever touches spent tries.

### What a pin can and cannot enforce

The census answered this and the answer is not what the first draft assumed.
**`chmod a-w` cannot make the promise true**, for four reasons, all verified:

- It does not revoke already-open file descriptors.
- The owner can chmod it back, and this system runs authored Python bodies and
  arbitrary commands as the owner (`src/hedloom/binding.py:131`, `:158`).
- A privileged process ignores mode bits entirely.
- **Renaming or deleting a directory needs write permission on its _parent_**,
  not on the directory. A frozen workspace under a writable `work/` can still
  be moved or removed — which defeats obligation 3 outright.

So the honest contract is three verbs, and *prevent* is not among them:

| | How |
| --- | --- |
| **Refuse** | every hedloom operation checks the pin first and declines — the pruner, a rerun, try reallocation, any future compaction |
| **Detect** | the frozen inventory of digests, re-walked by `verify()` |
| **Record** | a durable `pinned` event, attributable, in the journal |
| ~~Prevent~~ | *not offered.* `chmod a-w` is a guardrail that catches accidents, and the documentation must not call it a guarantee |

That is a weaker promise than "immutable" in the operating-system sense and a
stronger one than a convention, because drift is *provable* rather than
suspected. State it that way in `ONTOLOME.md` rather than implying enforcement
the design cannot deliver.

### Two consequences that shrink the API

**Pins are per-try only.** A record owns several try workspaces; a record-scope
pin would have to carry several inventories and would leave "does this cover
tries created later?" undecidable. So a pin names exactly one workspace. The
CLI may expand a bare identity into "every terminal try of this record" as
sugar, but what is *stored* is one pin per try, each with one inventory. Record
scope disappears as a stored concept and `unpin` becomes unambiguous.

**Pinning requires a terminal try.** Freezing and hashing a workspace while a
body or command is still writing to it races the work. The record already
distinguishes live phases (`journal.py:81`); pin refuses anything that is not
terminal.

### A pre-existing defect this makes reachable

`write_diagnostics(workdir, ...)` at `attempt.py:375` is **not** inside a
`try`, while `capture_outputs` three lines below it is. Any workspace that
cannot be written — frozen, quota-exceeded, a full disk, an NFS hiccup — raises
`PermissionError` there and the attempt never reaches terminal publication,
even though the work itself finished. Freezing makes a rare failure routine.
Guard it in the same phase, exactly as the `accept_for_reuse` claim gap is
guarded.

## Part 3 — the API, and the tests that cover it

Signatures first, tests under them, so the surface can be reviewed before any
of it exists. Names state behaviour; if a name reads badly the API is wrong.

Every signature below has been through a convention census against the existing
code (Part 4c). Deviations that survived are deliberate and noted.

### Phase 0 — instrumentation

```python
# hedloom_exec/artifacts.py
def workspace_path(root: str | os.PathLike[str], name: str) -> Path: ...
    """Where a workspace is. Pure: never creates, never stats."""

def workspace_for(root: str | os.PathLike[str], name: str) -> Path: ...
    """Unchanged. Still creates — this is the execution path."""
```

```
exec/tests/test_artifacts.py
  test_workspace_path_does_not_create_the_directory
  test_workspace_path_does_not_stat_the_directory
  test_workspace_for_still_creates_for_the_execution_path
  test_workspace_path_and_workspace_for_agree_on_the_location

exec/tests/test_journal.py
```

```python
# hedloom_exec/journal.py — what `created` carries. Four facts today, eight
# after Phase 0. Every one attribution-only: none reaches input_digest, so
# recording where an attempt came from cannot change what it reuses.
CREATED_FIELDS = (
    "plan", "invocation", "operation", "input_digest",     # today
    "try", "authored_key", "supersedes", "input_digests",  # Phase 0
)
```

```
exec/tests/test_created_event.py
  test_created_is_written_once_and_only_for_a_new_record
  test_created_records_the_try_number
  test_created_omits_the_try_number_for_a_caller_supplied_identity
  test_created_records_the_authored_key
  test_created_omits_the_authored_key_when_the_plan_did_not_name_one
  test_created_records_the_identity_it_supersedes
  test_created_records_no_supersedes_for_a_first_record
  test_created_records_a_digest_for_every_identity_key
  test_recording_the_key_digests_leaves_the_input_digest_unchanged
  test_an_identity_computed_before_phase_zero_is_unchanged_after_it
  test_no_created_field_participates_in_the_input_digest
  test_a_record_missing_the_phase_zero_fields_still_scans
```

`test_recording_the_key_digests_leaves_the_input_digest_unchanged` is the
tripwire for the mistake the census caught. The nine digests are *additional*;
the aggregate is one `blake2b` over one canonical mapping and must come out
byte-identical. An implementation that recombines the parts into the whole would
move every attempt identity in existence, and this test is what says so.

The last one matters because these fields land before Phase 1 — a record written
this morning carries four of them, and `scan_attempts` must not care.

### Phase 0b — following the current result

```python
# hedloom_exec/alias.py
ALIAS_DIR = "latest"

def alias_root(root: str | os.PathLike[str]) -> Path: ...
    """<root>/latest. Derived from the attempts root, never configured."""

def alias_path(root: str | os.PathLike[str], *,
               plan_id: str, authored_key: str, output: str) -> Path: ...
    """<root>/latest/<plan>/<authored-key>/<output>. Pure; never creates."""

def point_alias(root: str | os.PathLike[str], *,
                plan_id: str, authored_key: str, output: str,
                target: str | os.PathLike[str]) -> Path: ...
    """Create or repoint one alias, atomically.

    symlink() to a temporary name, then rename() over — so a reader either
    follows the old target or the new one, never a missing link. Called when
    the workspace is prepared, before the command launches, so the alias is
    usable while the output is still being written. The target may not exist
    yet; a dangling alias is the honest state during that window.
    """

def aliases_into(root, workspace: Path) -> tuple[Path, ...]: ...
    """Every alias pointing into this workspace. The pruner's `aliased` check."""
```

```python
# hedloom_exec/lineage.py — readers over what Phase 0 records
@dataclass(frozen=True, slots=True)
class Iteration:
    identity: str
    try_number: int
    outcome: str | None
    at: str
    supersedes: str | None
    changed_keys: tuple[str, ...]      # which of the nine identity keys moved
    is_current: bool                   # what the last run actually resolved to

# `changed_keys` is the whole contract. It names `inputs` or `implementation`;
# it CANNOT name `edits.py` or a helper, because a per-key digest of the inputs
# mapping does not carry the declared input names, and the bundle is not stored
# durably. An earlier draft promised `changed_detail: str` with exactly those
# examples; the census showed it unpopulatable from the proposed fields, so it
# is removed rather than left as a field that would have to lie. Naming the
# input needs the declared names recorded too — a further field, not this one.

def lineage(root: str | os.PathLike[str], *, plan_id: str,
            authored_key: str) -> tuple[Iteration, ...]: ...
    """The iteration chain, ordered by `supersedes`.

    Two facts, kept apart. `supersedes` gives the **order records were
    created**. It cannot give *which record a run last resolved to*, because
    returning to an earlier record writes no new `created` event — the guard at
    `attempt.py:221` is `if not state.events:`, and a reused record already has
    some. So after edit -> revert -> rerun, the chain reads B supersedes A while
    the live result is A.

    `is_current` therefore comes from the alias, not from the chain:
    `<root>/latest/` is repointed by every run at bind time, so it already
    records what the last run resolved to. It is derived from the same `root`
    the records are read from, so there is nothing extra to pass and no
    configuration in which it is unavailable.
    """

def why_reran(prior: Mapping[str, str],
              current: Mapping[str, str]) -> tuple[str, ...]: ...
    """Which of the nine identity keys differ between two `input_digests`."""

def is_behind(root, path: str | os.PathLike[str]) -> Iteration | None: ...
    """The iteration that superseded this workspace, or None if it is current."""
```

```
exec/tests/test_alias.py
  test_an_alias_is_created_before_the_command_launches
  test_an_alias_resolves_to_the_current_trys_workspace
  test_repointing_an_alias_is_atomic
  test_a_reader_following_the_alias_sees_a_growing_file
  test_a_new_try_repoints_the_alias
  test_a_new_record_repoints_the_alias
  test_an_alias_to_an_unwritten_output_dangles_rather_than_lying
  test_hedloom_never_pre_touches_a_declared_output_to_close_the_window
  test_an_alias_is_never_created_inside_a_workspace
  test_aliases_into_finds_every_alias_for_a_workspace
  test_the_alias_root_is_not_hidden
  test_the_alias_root_is_derived_from_the_attempts_root
  test_aliases_are_built_by_default_with_nothing_configured
  test_scan_attempts_skips_the_alias_directory
  test_live_attempts_skips_the_alias_directory
  test_a_directory_without_a_journal_is_never_read_as_an_attempt
  test_the_pruner_never_treats_the_alias_directory_as_a_record

exec/tests/test_lineage.py
  test_lineage_walks_supersedes_newest_first
  test_lineage_of_a_first_record_is_one_iteration
  test_why_reran_names_the_single_changed_key
  test_why_reran_names_implementation_when_a_body_was_edited
  test_why_reran_names_inputs_when_a_source_was_edited
  test_why_reran_does_not_claim_to_name_which_input_changed
  test_why_reran_is_empty_for_identical_bundles
  test_lineage_marks_the_alias_target_as_current
  test_lineage_marks_nothing_current_when_no_alias_exists_yet
  test_a_reverted_edit_is_current_even_though_it_is_not_newest
  test_is_behind_returns_none_for_the_current_workspace
  test_is_behind_names_the_iteration_that_superseded_a_stale_path
  test_a_reverted_edit_returns_to_the_original_identity

tests/test_where_check_log.py                  # the operator surface
  test_where_resolves_a_selector_to_an_output_path
  test_where_refuses_a_selector_that_matches_nothing
  test_check_exits_zero_for_a_current_path
  test_check_exits_non_zero_and_explains_for_a_superseded_path
  test_log_lists_iterations_with_the_reason_each_reran
  test_a_run_prints_the_rerun_reason_per_invocation
  test_a_reused_invocation_prints_reused_not_a_reason
```

`test_hedloom_never_pre_touches_a_declared_output_to_close_the_window` is a
tripwire: closing the dangling window that way is the obvious fix, and it would
let a command that wrote nothing report success, because `capture_outputs`
treats an existing declared output as evidence it was produced.

`test_a_reverted_edit_returns_to_the_original_identity` proves content
addressing survived all of this — edit, revert, rerun, and you land back on the
record you started from rather than a fourth iteration. Its companion,
`test_a_reverted_edit_is_current_even_though_it_is_not_newest`, is the one the
census forced: the returned-to record is *older* than the one it displaced, so
anything that equates "newest" with "current" reports the wrong result.

### Phase 1 — identity and the record

```python
# hedloom_exec/identity.py
@dataclass(frozen=True, slots=True)
class AttemptIdentity:
    plan_id: str
    invocation_id: str
    input_digest: str | None
    rendered: str                      # "hedloom-" + 20 hex; `sequence` is gone

def attempt_identity(*, plan_id: str,
                     invocation_id: str,
                     input_digest: str | None = None) -> AttemptIdentity: ...

def try_name(identity: str, try_number: int) -> str: ...
    """<identity>-<n>. The workspace directory and the batch job name."""

def parse_try_name(name: str) -> tuple[str, int]: ...
    """Inverse. Validates the base is a *rendered* identity — `hedloom-` plus
    exactly 20 hex — not merely that it matches _SAFE, which permits embedded
    hyphens. Raises IdentityError otherwise."""
```

```python
# hedloom_exec/journal.py
@dataclass(frozen=True, slots=True)
class TryState:
    number: int
    phase: str                         # unsubmitted | intended | submitted | terminal
    outcome: str | None
    handle: Mapping[str, Any] | None
    transport: str | None
    substrate: str | None
    manifest_path: str | None
    placement: Mapping[str, Any]
    cancel_requested: bool             # per-try, no longer sticky
    reuse_accepted: bool               # per-try, no longer sticky
    observations: tuple[Mapping[str, Any], ...]
    started_at: str | None
    ended_at: str | None

@dataclass(frozen=True, slots=True)
class AttemptState:
    identity: str
    tries: tuple[TryState, ...]
    current_try: int | None            # None for a record with no try yet
    pins: tuple[Pin, ...]
    events: tuple[JournalEvent, ...]
    @property
    def current(self) -> TryState | None: ...
    @property
    def phase(self) -> str: ...        # the current try's, for compatibility
    @property
    def outcome(self) -> str | None: ...
```

```
exec/tests/test_identity.py
  test_a_record_identity_ignores_the_try_number
  test_attempt_identity_no_longer_accepts_a_sequence
  test_the_same_inputs_always_render_the_same_record
  test_changed_inputs_render_a_different_record
  test_a_try_name_is_the_record_identity_and_its_number
  test_a_try_name_is_usable_as_a_batch_job_name
  test_parse_try_name_round_trips_every_rendered_identity
  test_parse_try_name_refuses_a_base_that_is_not_a_rendered_identity
  test_parse_try_name_refuses_a_negative_or_padded_number

exec/tests/test_record.py                      # replaces test_failure_reuse.py
  test_repeated_failures_share_one_record
  test_each_try_gets_its_own_workspace
  test_a_rerun_cannot_overwrite_an_earlier_trys_workspace
  test_a_rerun_cannot_overwrite_an_earlier_trys_manifest
  test_a_changed_input_starts_a_new_record_at_try_zero
  test_a_succeeded_try_ends_the_sequence_of_tries
  test_an_accepted_failure_stands_as_the_result
  test_there_is_no_cap_on_retained_tries
```

### Phase 1 — try allocation, which must be indivisible

```python
# hedloom_exec/journal.py — NOT a free function by accident.
def begin_try(journal: AttemptJournal, *, actor: str | None = None) -> int: ...
    """Reserve the next try number and durably record it, in one step.

    Must be called with the claim held; raises ClaimNotHeld otherwise.
    Returning a number without recording it would let two callers agree on
    the same one — the indivisibility `claim()` exists to provide
    (`journal.py:154`). The `try_started` event is flushed and fsynced before
    this returns, and therefore before any transport call, so an accepted job
    is always discoverable by the name it was submitted under.
    """
```

```
exec/tests/test_try_allocation.py
  test_begin_try_requires_the_claim_to_be_held
  test_begin_try_records_the_number_before_it_returns
  test_the_try_number_is_durable_before_any_transport_call
  test_a_crash_after_acceptance_leaves_the_job_discoverable
  test_a_try_allocated_but_never_submitted_is_resumed_not_abandoned
  test_an_interrupted_run_does_not_burn_try_numbers
  test_two_claimants_cannot_receive_the_same_try_number
  test_a_pinned_try_number_is_never_reallocated
```

### Phase 1 — the fold partition

```
exec/tests/test_journal.py
  test_fold_reports_the_latest_trys_phase_and_outcome
  test_fold_attributes_every_event_to_its_try
  test_fold_of_a_single_try_record_is_unchanged
  test_a_new_trys_intent_resets_the_phase
  test_events_remain_append_only_across_tries

exec/tests/test_fold_partitioning.py       # the sticky-field contamination
  test_a_cancellation_of_one_try_does_not_block_the_next
  test_accepting_one_trys_failure_does_not_make_a_later_try_reusable
  test_observations_are_attributed_to_the_try_they_describe
  test_sticky_state_from_try_zero_is_not_reported_as_try_threes
```

### Phase 1 — the silent breaks

Their own files, because these are the things that fail without failing.

```
exec/tests/test_recovery_names.py          # ranked most dangerous
  test_discovery_after_a_lost_receipt_asks_for_the_try_name
  test_a_lost_receipt_does_not_cause_a_second_submission
  test_discovery_given_the_record_name_is_refused_not_answered_negatively
  test_an_authoritative_negative_discovery_still_means_never_accepted

exec/tests/test_watch_keys.py
  test_the_watcher_matches_the_job_name_not_the_record_identity
  test_a_sweep_with_live_jobs_is_distinguishable_from_a_finished_one
  test_an_observation_for_one_try_does_not_attach_to_another
  test_removing_the_try_suffix_from_the_key_fails_this_test

exec/tests/test_lsf.py
  test_bsub_is_given_the_try_name
  test_bjobs_discovery_asks_for_the_try_name
  test_bkill_cancels_the_try_not_the_record
  test_a_record_identity_passed_where_a_try_is_expected_is_refused
```

The last watcher test is deliberate: a test that only passes because the keying
is correct is worth less than one that demonstrably fails when it is not.

### Phase 1 — concurrency, and one pre-existing defect

```
exec/tests/test_claim.py
  test_the_claim_covers_every_try_at_one_input_set
  test_a_second_controller_cannot_start_a_try_while_one_is_claimed
  test_a_crash_between_tries_leaves_the_record_readable
  test_stale_identity_still_refuses_a_record_created_from_other_inputs
  test_accept_for_reuse_holds_the_claim          # gap at attempt.py:117
  test_an_unwritable_workspace_does_not_lose_terminal_publication
```

That last one is the `write_diagnostics` defect (`attempt.py:375`): today an
unwritable workspace raises `PermissionError` outside any `try`, and the
attempt never publishes even though the work finished.

### Phase 2 — the pruner, read-only

```python
# hedloom_exec/prune.py
@dataclass(frozen=True, slots=True)
class RetentionRule:
    name: str
    outcome: tuple[str, ...] = ()
    older_than: str | None = None          # "14d"; parsed, not eval'd
    larger_than: str | None = None         # "1GiB"
    keep_latest: int = 1
    keep_logs: bool = True

@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    rules: tuple[RetentionRule, ...]
    floor: str = "7d"
    @classmethod
    def from_toml(cls, data: Mapping[str, Any]) -> "RetentionPolicy": ...
        """Refuses unknown keys, empty rules, and any rule naming
        `unreconciled`. Raises RetentionError."""

@dataclass(frozen=True, slots=True)
class Candidate:
    identity: str
    try_number: int
    workspace: Path
    bytes: int
    outcome: str
    published_at: str
    rule: str                              # which rule selected it

SkipReason = Literal["pinned", "floor", "unreconciled", "contended",
                     "reusable", "aliased", "non-terminal", "outside-roots",
                     "no-rule"]

@dataclass(frozen=True, slots=True)
class Skip:
    identity: str
    try_number: int | None
    reason: SkipReason
    detail: str = ""

@dataclass(frozen=True, slots=True)
class Survey:
    root: Path
    workspace_root: Path
    policy: RetentionPolicy
    candidates: tuple[Candidate, ...]
    skipped: tuple[Skip, ...]
    surveyed_at: str
    @property
    def freed_bytes(self) -> int: ...
    def summary(self) -> str: ...
    def as_data(self) -> dict[str, Any]: ...
    def apply(self, *, limit_bytes: int | None = None,
              actor: str | None = None) -> "PruneReport": ...

def survey(root: str | os.PathLike[str],
           policy: RetentionPolicy, *,
           workspace_root: str | os.PathLike[str] | None = None,
           records: Iterable[AttemptRecord] | None = None) -> Survey: ...
    """Look, never touch. `records` accepts one prior scan_attempts()."""
```

Three names were chosen against the existing namespace rather than for their
own sake. `RetentionPolicy` and `survey` rather than `Policy` and `plan`,
because both of those are already public in `hedloom` (`__init__.py:53`,
`:75`). `reusable` rather than `standing` as a skip reason, because the durable
vocabulary for a result allowed to stand is already `reuse_accepted` /
`is_reusable`. And `Survey` rather than `Collection`, because
**`"collection"` is already a cardinality in `hedloom_flow`** — the ordered
fan-in of several artifacts into one input (`model.py:229`,
`authoring.py:509`). That third collision is also why the verb is `prune` and
not `collect`: the collection vocabulary is spoken for by authoring, and
borrowing it for storage would make one word mean two unrelated things in a
system whose whole discipline is that names carry meaning.

```
exec/tests/test_prune_survey.py
  test_a_survey_removes_nothing
  test_a_survey_creates_no_directory_it_inspects
  test_a_survey_reports_the_bytes_it_would_free
  test_conditions_within_a_rule_are_anded
  test_rules_are_ored_with_each_other
  test_the_floor_overrides_every_rule
  test_unreconciled_is_never_selected
  test_a_rule_that_would_select_unreconciled_is_refused
  test_a_rule_with_no_condition_is_refused
  test_an_unknown_policy_key_is_refused
  test_a_malformed_duration_is_refused_not_coerced
  test_keep_latest_spares_the_newest_tries_of_each_record
  test_keep_logs_spares_stdout_and_stderr
  test_the_reusable_result_is_never_a_candidate
  test_a_non_terminal_try_is_never_a_candidate
  test_a_workspace_outside_the_roots_is_refused
  test_the_survey_explains_every_skip_with_a_reason
  test_larger_than_measures_the_workspace_not_the_record
  test_larger_than_refuses_an_artifact_whose_kind_is_unknown
  test_older_than_measures_publication_not_file_mtime
  test_an_aliased_workspace_is_never_a_candidate
  test_a_survey_of_an_empty_root_is_not_an_error
```

### Phase 3 — the pruner, destructive

```python
# hedloom_exec/prune.py
@dataclass(frozen=True, slots=True)
class PruneReport:
    removed: tuple[Candidate, ...]
    skipped: tuple[Skip, ...]
    freed_bytes: int
    stopped_at_limit: bool
    applied_at: str
```

`apply()` re-checks every precondition under `journal.claim()` before removing
anything: a `Collection` is a proposal, and the tree may have moved since it
was surveyed. Each removal appends a durable `workspace_removed` event to the
record **before** the bytes go, so the journal never describes a workspace that
silently is not there — state in this unit is derived from durable events
(`journal.py:313`), and an in-process `PruneReport` is not state.

```
exec/tests/test_prune_apply.py
  test_apply_removes_exactly_what_the_survey_named
  test_apply_records_workspace_removed_in_the_record
  test_apply_leaves_the_record_directory_intact
  test_apply_rechecks_preconditions_under_the_claim
  test_apply_refuses_a_candidate_pinned_since_the_survey
  test_apply_skips_a_contended_record_rather_than_waiting
  test_the_removal_event_is_written_before_the_bytes_go
  test_a_crash_after_the_event_and_before_the_unlink_self_heals
  test_apply_stops_at_the_byte_limit
  test_pruning_a_try_does_not_change_the_next_try_number
  test_pruning_a_try_does_not_change_what_reuse_returns
  test_a_run_after_pruning_behaves_as_if_nothing_was_pruned
```

The last is the acceptance test for the whole feature: **the pruner must be
undetectable by the runner.**

### Phase 4 — pins

```python
# hedloom_exec/pins.py
@dataclass(frozen=True, slots=True)
class FrozenFile:
    relpath: str
    size: int
    modified_ns: int
    digest: str                            # blake2b of the content, taken once

@dataclass(frozen=True, slots=True)
class Pin:
    pin_id: str                            # stable, targetable by unpin
    identity: str
    try_number: int                        # pins are per-try. Always.
    workspace: str                         # the absolute path promised stable
    contents: tuple[FrozenFile, ...]
    reason: str
    actor: str
    at: str
    layout: int                            # the record layout this holds under
    froze: bool                            # whether chmod a-w was applied
    released_at: str | None = None
    released_by: str | None = None
    released_reason: str | None = None
    @property
    def is_active(self) -> bool: ...

def pin(journal: AttemptJournal, *,
        try_number: int,
        workspace_root: str | os.PathLike[str],
        reason: str,
        actor: str | None = None,
        freeze: bool = True) -> Pin: ...
    """Promise that this workspace stays put, unchanged.

    Refuses a try that is not terminal: freezing and hashing a workspace a
    body is still writing races the work (`journal.py:81` distinguishes the
    live phases). Refuses a try already actively pinned.

    Order is inventory -> chmod -> journal event, and the event records
    whether the chmod succeeded. Recording first could leave an active pin
    over writable content; chmodding first could leave an unrecorded
    read-only workspace, which `verify` can still explain.
    """

def unpin(journal: AttemptJournal, *, pin_id: str,
          reason: str, actor: str | None = None,
          thaw: bool = True) -> Pin: ...
    """Release one pin by id. Appends `unpinned`; never erases the `pinned`
    event, so the record shows the promise and its release, with both
    reasons and both actors. `thaw` restores the modes recorded at pin time."""

def pins_of(state: AttemptState, *, try_number: int | None = None,
            active_only: bool = True) -> tuple[Pin, ...]: ...

def is_pinned(state: AttemptState, try_number: int) -> bool: ...

VerifyOutcome = Literal["intact", "drifted", "layout-changed", "missing"]

@dataclass(frozen=True, slots=True)
class Verification:
    outcome: VerifyOutcome
    drifted: tuple[str, ...] = ()          # relpaths: changed, added or removed
    detail: str = ""

def verify(pin: Pin, *, layout: int) -> Verification: ...
    """Whether the promise still holds.

    `layout-changed` is reported *instead of* drift, not alongside it: a pin
    written under another record layout is void, and comparing its inventory
    would be answering a question that no longer means anything. Voiding it
    loudly is the point — a promise that expires silently is worse than none.
    """
```

```
exec/tests/test_pins.py
  test_a_pinned_try_is_never_a_candidate
  test_a_pinned_try_is_skipped_while_its_siblings_are_pruned
  test_a_pin_survives_a_rerun_of_the_same_record
  test_a_pin_is_attributable_to_a_reason_and_an_actor
  test_unpin_targets_one_pin_by_id
  test_unpin_appends_rather_than_erasing
  test_the_record_shows_a_pin_that_was_later_released
  test_two_pins_on_one_try_release_independently
  test_pinning_a_non_terminal_try_is_refused
  test_pinning_an_already_pinned_try_is_refused
  test_an_ambiguous_identity_prefix_is_refused_with_candidates
  test_an_authored_key_selector_resolves_from_records_alone
  test_an_authored_key_that_matches_no_record_is_refused
  test_an_authored_key_matching_several_records_lists_them
  test_accept_for_reuse_does_not_create_a_pin
  test_an_accepted_failure_is_skipped_as_reusable_not_as_pinned
  test_an_accepted_failure_becomes_prunable_once_a_later_try_stands
  test_a_pin_is_never_written_into_the_workspace
  test_pruning_a_workspace_cannot_delete_its_own_pin

exec/tests/test_pin_immutability.py        # the promise, not the exemption
  test_pinning_freezes_the_workspace_read_only
  test_pinning_records_a_digest_for_every_file_it_froze
  test_the_inventory_is_taken_before_the_chmod
  test_a_rerun_refuses_to_write_into_a_pinned_workspace
  test_a_pinned_workspace_is_never_moved_by_any_operation
  test_verify_reports_no_drift_for_an_untouched_pin
  test_verify_names_exactly_the_files_that_drifted
  test_verify_detects_a_file_added_to_a_pinned_workspace
  test_verify_detects_a_file_removed_from_a_pinned_workspace
  test_verify_detects_content_changed_at_the_same_size_and_mtime
  test_unpin_thaws_the_workspace_and_says_so_in_the_record
  test_an_empty_workspace_can_still_be_pinned
  test_a_crash_between_chmod_and_the_event_leaves_an_explainable_state
  test_the_docs_do_not_claim_chmod_prevents_modification

exec/tests/test_record_layout.py           # what makes the promise expirable
  test_a_new_record_declares_its_layout
  test_an_unrecognised_layout_is_refused_by_version_not_misread
  test_a_pin_records_the_layout_it_was_made_under
  test_verify_reports_layout_changed_rather_than_drift
  test_layout_changed_is_reported_even_when_the_content_is_intact
  test_the_pruner_refuses_a_root_whose_layout_it_does_not_know
  test_a_record_with_no_layout_file_is_treated_as_foreign
```

`test_verify_detects_content_changed_at_the_same_size_and_mtime` is the one
that justifies the digest. Size and mtime alone would pass it.

The last test is a documentation assertion, and it belongs in the suite because
the promise is the feature: overclaiming enforcement the design cannot deliver
is the failure mode with the worst consequences and the fewest symptoms.

### Phase 5 — site and the automatic trigger

```
run/tests/test_retention_policy.py
  test_a_command_given_one_root_where_it_needs_two_is_refused
  test_retention_rules_parse_from_a_site_profile
  test_cli_flags_override_site_rules
  test_a_watched_path_outside_the_roots_is_refused

tests/test_prune_after_run.py
  test_a_run_applies_only_the_rules_the_site_names
  test_the_newest_try_of_each_record_is_never_pruned
  test_a_prune_failure_warns_and_does_not_fail_the_run
  test_submit_has_no_prune_argument
```

That last one is a tripwire test, not a feature test. It exists so that the day
someone adds `submit(prune=True)` for convenience, a test says why not.

## Part 4 — migration: there is none

**Decided 2026-08-27 (user).** The prototype does not support migration and its
only user knows it. Every run root on this machine existed for testing and
development, and keeping them was itself the thing to avoid.

So Phase 1 does **not** carry a dual-read path, a converter, or a
read-only-legacy mode. A record whose directory name does not match the new
derivation is simply not something the code has to think about, because no such
record is kept.

The three roots that existed were deleted the day this was decided —
`studies/_runs`, `hedloom/examples/_runs`,
`docs/reference/ota-pvt-plan/_runs`, 6.7 MiB in total. All three were untracked
and already covered by `.gitignore`, so nothing in version control moved. Every
one was regenerable by rerunning the example or study that wrote it, which is
the property that made them disposable in the first place.

This removes the whole compatibility surface the census warned about, including
the 120 records that were already unreadable under an earlier identity scheme.
The lesson those 120 taught still stands and is worth stating rather than
discarding with them: **a record that states its own fields survives a change to
the identity algorithm; a record that only implies them does not.** That is why
Phase 0 records the try number even before Phase 1 needs it.

The obligation this creates is small and should be met before the change lands:
`README.md` and `docs/guide/` must say plainly that roots written by an earlier
version are not readable, so the next person to find one deletes it instead of
filing a bug.

And it should be **enforced rather than announced**. Phase 1 item 7 gives the
record a declared layout version, so a foreign root is refused by version in
one read instead of failing somewhere downstream in a way that looks like a
bug. "We do not support migration" then becomes a checkable property of the
code rather than a sentence in a README that a future reader may not find.

## Part 4b — the census that produced Parts 0b, 1.3, 1.5 and 1.7

An independent read-only pass (codex medium, 2026-08-27, 7m23s) audited this
plan against the code. What it changed here is recorded so the plan is not read
as though it arrived correct:

- **`fold()` was overstated.** The first draft said the existing scan already
  handles a multi-try log. True for the phase machine, false for the three
  sticky fields. Now Phase 1 item 5, with its own test file.
- **The discovery path was missed entirely.** The draft flagged the watcher and
  stopped there. `transport.discover(journal.identity)` is the same class of
  bug with a far worse outcome — duplicate accepted work rather than a quiet
  under-report. Now ranked first in Phase 1 item 3.
- **The new crash window was unstated.** Choosing a try number creates an
  ordering obligation the current design gets for free. Now Phase 1 item 7.
- **The three document contradictions** became the argument in Part 0b, which
  is stronger than anything in the draft.

Two things from that pass are **not** adopted:

- Its test totals are void. All three pytest invocations exited 1 before
  collection — `No usable temporary directory found` — because the pass ran
  read-only. The baseline that stands is the one measured on 2026-08-26 with
  write access: **305 passed, 4 skipped**, plus two pre-existing `flow/tests`
  collection errors.
- Its framing that "Option C as written is incomplete: it removes the only
  sequence allocator while still requiring a sequence" is accurate about the
  draft and answered by Phase 1 item 1 (`next_try(journal)` reading the record)
  together with item 7's ordering rule. Replacing the allocator was always the
  work; the draft simply had not said where the number comes from.

Its self-assessed confidence was 0.88, and every claim relayed above was
re-verified against the code before being written here.

## Part 4c — the API census (codex, 2026-08-27, read-only)

A second independent pass audited the proposed signatures before they were
written here. Every claim below was re-verified against the code. What it
changed:

- **`Policy` and `plan` were exact collisions** with public names in `hedloom`
  (`__init__.py:53`, `:75`). Renamed to `RetentionPolicy` and `survey`.
- **`chmod a-w` does not make the immutability promise true** — open
  descriptors survive it, the owner can undo it, root ignores it, and renaming
  a directory needs write permission on its *parent*, which stays writable.
  The promise was rewritten from *prevent* to *refuse, detect, record*.
- **A record-scope pin could not be represented.** One record owns several
  workspaces; one `Pin` carried one path and one inventory. Pins are now
  per-try only, and record scope is CLI sugar rather than stored state.
- **`unpin` was ambiguous** with two pins on one try. `pin_id` added.
- **`next_try()` as a free function violated the claim's indivisibility.**
  Replaced by `begin_try(journal)`, which reserves *and* records under the
  held claim, and refuses without it.
- **`PruneReport` was in-process only**, while this unit derives state solely
  from durable events (`journal.py:313`). A `workspace_removed` event is now
  explicit in the signature contract.
- **`write_diagnostics` (`attempt.py:375`) is unguarded** while `capture_outputs`
  three lines below it is wrapped. An unwritable workspace loses terminal
  publication for work that finished. Pre-existing; freezing makes it routine.
  Now a test in `test_claim.py`.
- **Skip reason `standing` contradicted `reuse_accepted`/`is_reusable`.**
  Renamed `reusable`.
- **`parse_try_name` survives** but must validate the base as a *rendered*
  identity — `hedloom-` plus exactly 20 hex — because `_SAFE`
  (`identity.py:20`) permits embedded hyphens and would make the suffix
  ambiguous.
- **Pinning a live try races the work.** A terminal precondition was added.

Not adopted: nothing. Every finding either changed a signature or became a
test. Its self-assessed confidence was 0.91.

## Part 4d — the implementation census (codex, 2026-08-27, high effort)

The agent asked to implement Phase 0 ran the census first, found the plan
inconsistent, and **stopped rather than improvising** — which is the outcome the
brief asked for. It filed
[hedloom#7](https://github.com/smldis/hedloom/pull/7) carrying the design record
and `design/phase0-census-2026-08-27.md`, and wrote no source. All four findings
were re-verified against the code before being applied here.

| Finding | Verdict | Fix |
| --- | --- | --- |
| The nine identity keys are **not** individually digested; `input_digest` is one `blake2b` over one canonical mapping | **My error, twice.** Recombining parts into the whole would move every attempt identity | Nine *additional* digests; aggregate untouched. The impossible test is replaced by a tripwire asserting the aggregate is unchanged |
| A once-only `created.supersedes` cannot express A → B → A | Correct. Returning to a record writes no new `created` (`attempt.py:221`) | `is_current` comes from the alias, which every run repoints; `supersedes` gives order only |
| Per-key digests can say `inputs` moved but cannot name `edits.py` | Correct. The mapping's digest carries no declared names | `changed_detail` removed; `changed_keys` is the whole contract |
| One positional `<root>` cannot name what these commands need | Correct. `root` and `workspace_root` are independent (`site.py:103`, `:105`) | Commands take `--site` or explicit roots. **The alias tree is not a third root** — it is `<root>/latest/`, derived and built by default (user, 2026-08-27) |

The first is the one that mattered. Left in, an implementer following the plan
literally would have made `input_digest` a hash-of-hashes and **silently
invalidated every record in existence** — under a heading promising no
behaviour change. It survived my own review, a design pass, and a prior census;
it did not survive someone opening `reuse.py` with the intention of typing the
code.

Worth keeping as evidence for how this plan is written: the three earlier
censuses audited *prose against prose*. This one audited prose against an
implementation it was about to attempt, and that is the pass that found the
identity-breaking one.

## Part 5 — decisions, all closed 2026-08-27

1. ~~**Migration**~~ — **none.** See Part 4.
2. ~~**Does `accept_for_reuse` imply a pin?**~~ — **No.** Pin exists for
   pruning; acceptance is not an exception to it.

   This is the better answer and it makes the model cleaner rather than
   looser. An accepted failure *is* protected — but as the **reusable result**,
   not as a pinned one, and that is a more accurate reason. It is skipped
   because downstream work resolves its addresses, which is a fact about the
   graph rather than an operator's promise. And the moment a later try
   succeeds and takes over as the standing result, the accepted failure becomes
   prunable like any other spent try — which is correct, and which a pin would
   have wrongly prevented forever.

   Two roles stay separate: `accept_for_reuse` says *this result stands*,
   `pin` says *these bytes stay put*. Nothing implies the other.
3. ~~**`keep_latest` default**~~ — **1.** The newest try of each record always
   stands, so a failure keeps its most recent evidence for diagnosis without
   the operator asking.
4. ~~**Does the pruner need the plan document?**~~ — **No, and the generation
   record is dropped.** Record `authored_key` in the `created` event instead:
   one field, already in hand at every layer, and every selector resolves from
   records alone. See "Selecting a thing to pin or prune".
5. ~~**Record-level cancellation?**~~ — **Per-try only**, and it does make
   sense. It preserves exactly what the code does today: `_launch_or_attach_locked`
   raises `AttemptCancelled` for an attempt whose own cancel was recorded, which
   under Option C reads as *this try was cancelled, do not launch this try*.
   Nothing is lost.

   It is also the consistent choice. `design/cancellation-plan.md` is a live,
   deliberately-unbuilt proposal whose current answer is *killing the process is
   still the answer* — and work is owner-bound, so that genuinely does stop it.
   A record-level "stop trying at these inputs" would be that unbuilt feature
   arriving as a side effect of a storage refactor, which is exactly the kind of
   quiet scope growth `submit(retry=…)` is a tripwire against.

**Nothing blocks Phase 0.** It is three additive changes — `workspace_path`,
the try number in `created`, `authored_key` in `created` — plus landing the
census script. None of them changes behaviour, and all four are defensible
alone.
