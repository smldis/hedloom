# Editing an input mid-development: four ways out

**Written 2026-08-27. A proposal for review.** Nothing here is built.

## The problem, stated precisely

While developing a study you edit an input artifact — a sidecar `edits.py`, a
fixture, a spec file — to fix a failure. That edit changes the source
fingerprint, which changes `input_digest`, which changes the attempt identity.
Under Option C the identity names the record, so **a new record appears** rather
than a new try of the existing one.

Four annoyances follow, and only the first is the one that gets noticed:

1. **The path moves every iteration.** The directory you were watching is stale.
   You cannot keep a terminal on it, bookmark it, or script against it.
2. **Orphaned records accumulate**, one per edit. `keep_latest` cannot see them:
   it spares the newest *try within a record*, and these are separate records.
   There is no rule today that names them.
3. **The thread is lost.** "What did my last three iterations produce?" means
   correlating records that share `(plan, invocation)` but differ in digest,
   by hand.
4. **Nothing says what changed.** The digest moved; which of the nine identity
   keys moved is not recorded anywhere. You know something changed, not what.

That fourth one is the quiet one, and it turns out to be the key.

## The constraint every option must respect

Content-addressed identity is what makes reuse sound: *a manifest at this
identity was produced by exactly these inputs.* Any option that stabilises the
path by weakening that is not a usability improvement, it is the staleness bug
returning with a friendly face.

**So a fifth option is named here only to reject it:** a `--dev` mode that drops
source fingerprints from the digest so the identity holds still. It would reuse
results computed from the old file content, which is precisely the defect
`source_fingerprints` was added to close. Editing an input *must* move the
identity. What must not move is the **view**.

That distinction — identity moves, the view stays — is what the options below
are all variations of.

---

## Option 1 — Simple: a `latest` pointer per invocation

```
latest/ota/tt_1v80_27c:simulate_ac  ->  ../../hedloom-016ea5b1-0
```

A symlink per `(plan, authored_key)`, replaced atomically at the end of each run
(`symlink` to a temp name, then `rename` over). `cd` into it and you are always
in the newest attempt for that node, whatever the digest did.

**Concept:** the identity churns; one stable name follows it.

- Derived, disposable, rebuildable from the records — a stale one is repaired,
  never trusted.
- Touches no identity, no digest, no protocol. Perhaps forty lines.
- Needs `authored_key` in the record, which Phase 0 already adds.

Solves **1**. Does nothing for 2, 3 or 4.

---

## Option 2 — Elegant: record what a new record supersedes

When a run derives a digest for an invocation that already has records at other
digests, the new record's `created` event names the one it displaces:

```json
{"event":"created","data":{"plan":"ota","invocation":"invoke:key:8a43…",
 "authored_key":"tt_1v80_27c:simulate_ac","input_digest":"e798de…",
 "supersedes":"hedloom-9c1f4a2b8e0d5f6a7b3c"}}
```

**Concept:** a chain of iterations, written down instead of reconstructed.

One recorded field, and three things fall out:

- **Lineage is walkable.** `hedloom log ota:tt_1v80_27c:simulate_ac` lists the
  iterations in order, newest first. That is annoyance 3, gone.
- **Pruning gets the rule it is missing.** `superseded` becomes a real
  `RetentionRule` condition: *prune records displaced more than N iterations
  ago.* `hedloom prune --superseded --keep-latest 2` cleans a debug session and
  keeps the last two attempts at each node. That is annoyance 2.
- **`stale_attempts()` gets durable backing.** It recomputes prior-digest work
  today by scanning and comparing; with `supersedes` the answer is recorded at
  the moment it was true, rather than inferred later from whatever survives.

The run already has what it needs: `attempts_for(root, plan_id, invocation_id)`
names the prior records at plan time, before anything is spent.

Solves **2** and **3**. Combines cleanly with 1 for **1**.

---

## Option 3 — Creative: record *which* identity key moved

Today the record stores one digest: `input_digest: "e798de…"`. Store the nine
component digests as well:

```json
"input_digests":{"operation":"3f2a…","implementation":"9c04…",
                 "inputs":"7bd1…","outputs":"e5a8…","command":"11c9…", …}
```

**Concept:** the record explains its own invalidation.

Diffing two attempts then names the cause in one comparison, with no bundle
stored and no reconstruction:

```console
$ hedloom log ota:tt_1v80_27c:simulate_ac
hedloom-016ea5b1  12:04  failed     inputs changed   <- edits.py  7bd1… → 4c92…
hedloom-9c1f4a2b  11:47  failed     implementation changed  <- simulate_ac body
hedloom-3ef88d10  11:20  succeeded  (first)
```

That is annoyance 4, and it converts the whole experience: hash churn stops
being noise and becomes *a log of what you changed*. It is nearly free — the
nine keys are already digested individually inside `input_digest`
(`reuse.py:44`); the function throws the parts away and keeps the whole.

It also removes a real gap. The bundle is **never stored durably** — only the
combined digest is — so today nothing on disk can answer "why did this rerun?"
even in principle. `docs/internals/mechanism.md` explains the rule to a reader;
the record cannot explain itself to a tool.

Solves **4**, and makes 2 and 3 far more useful than they are alone.

---

## Option 4 — Recommended: 2 + 3, with 1 as the cheap ergonomic layer

Take all three, in that order of value, and take nothing from the rejected
fifth.

| | Adds | Solves |
| --- | --- | --- |
| `supersedes` in `created` | one field | orphan pruning, lineage |
| `input_digests` in `created` | one dict, ~9 short hashes | *why* it reran |
| `latest/<plan>/<key>` symlink | one derived tree | the moving path |
| Rerun reason printed at run time | one line, no storage | the daily annoyance |

That last row is the one to build first and costs nothing durable. When a run
reruns an invocation whose digest moved, it already holds both bundles — so it
can say so as it happens:

```
invoke  tt_1v80_27c:simulate_ac   rerun: inputs changed (edits.py)
invoke  tt_1v80_27c:evaluate      rerun: upstream
invoke  ff_1v98_m40c:simulate_ac  reused
```

**Why this combination rather than a development mode.** Every piece is a fact
recorded about work that really happened, readable by any tool, and consistent
with the file-first record. None of it is a second way to run a study, none is a
mode with different rules, and none weakens the identity that makes reuse
honest. The churn is not suppressed — it is explained, followed, and made
prunable.

**Ordering.** `input_digests` and `supersedes` are two fields in the same event
and land together in Phase 0, where they cost nothing and change no behaviour.
The printed reason follows immediately, since it needs only what is already in
memory. The `latest/` symlink and `hedloom log` are Phase 2 work, alongside the
pruner, because they are read-side conveniences over records that by then say
enough to support them.

## Amendment 2026-08-27 — the reload button, and two kinds of consumer

Two corrections came out of review, one to a claim and one to the framing.

### The correction to the framing

The reload workflow — open an output in a viewer, rerun the study, press
reload — makes it clear that hedloom serves two different consumers and has a
name for only one of them:

| Consumer | Wants | Served by |
| --- | --- | --- |
| **Evidence** | *this exact attempt*, forever, unchanged | `pin` — an identity-named path |
| **Current result** | *whatever is newest*, at a name that never moves | nothing today |

People with a reload button are the second kind, and today the only path
hedloom offers them is a workspace — which is attempt-scoped **by design**,
because a rerun must not overwrite the evidence of the previous attempt. They
are using an evidence path as a current-result path and getting hurt when it
moves, exactly as the contract says it will.

So publication is not new scope. It names a need that already exists and is
presently served by the wrong mechanism.

### The correction to the claim

An earlier note said a `latest/` symlink does not help because a consumer
resolves it once. That is true for consumers that resolve — anything calling
`realpath()`, or a shell whose `pwd -P` recorded the target. It is **not**
true for consumers that keep the path *string* they were given and re-`open()`
it, which is what most GUI viewers do: reload re-opens the symlink, follows it,
and lands on the new target.

So the symlink is genuinely useful for the reload case and genuinely useless
for the `cd` case. Stating which is which is more useful than a blanket
verdict.

### What publication has to get right

**Atomic replacement, not in-place rewrite.** `os.replace` onto the published
name — the mechanism the codebase already uses for manifests
(`journal.py:442`). A viewer holding the old file keeps a consistent inode and
finishes reading it; a reload re-opens the name and gets the new one. Writing
in place would let a reload catch a half-written file.

**Hardlink rather than copy** where the filesystem allows it. The manifesto's
rule is that materialization records an address and never moves bytes; a
hardlink honours it, costs nothing for a multi-gigabyte raw output, and keeps
the published result alive even after its workspace is pruned, because the link
count is not zero. Copy is the cross-filesystem fallback and should say so.

**And one thing it cannot fix.** A tool that holds the file open and polls
`fstat` for changes — *automatic* reload rather than a button — will never
fire, because after a rename the inode it holds genuinely did not change. No
publishing scheme can fix that: the alternative is writing in place, which
trades a missed refresh for a torn read.

That trade belongs to the operator rather than to us, so it is declared:

**Superseded by Amendment 2.** The `atomic`/`in-place` choice existed only
because publication copied something. Once the alias points at the workspace
file itself there is no second copy and no mode to choose, and **in-place
support is dropped from this plan** (decided 2026-08-27, user). A viewer that
auto-reloads by polling `fstat` on a held descriptor is not served, and that is
stated rather than worked around.

## Amendment 2 — streaming outputs, and why publication was the wrong shape

Two objections in review, and the second one dissolves the first.

**On duplication.** The proposal said hardlink rather than copy, which
duplicates no bytes at all — a second directory entry for one inode. But the
objection lands anyway, because *publish-at-end-of-run* is the wrong shape for a
reason duplication only hints at.

**On streaming.** Simulators write their output progressively: a transient run
fills a raw file over minutes or hours, and the engineer opens it and watches it
grow. Publication at the end of a run cannot serve that at all — by the time
there is something to publish, the thing the user wanted has already finished
happening.

That requirement rules out the whole publish-then-replace design. What is needed
is not a copy of a finished result under a stable name; it is **a stable name
that points at the output being written, from the moment it starts being
written.**

### The shape that follows

A symlink, per declared output, created when the workspace is prepared — before
the command launches — and repointed at the start of each try:

```
latest/tt_1v80_27c/ac.raw  ->  ../../work/hedloom-016ea5b1-0/ota_ac.raw
```

`open()` follows symlinks, so:

- The viewer opens `latest/…/ac.raw`, lands on the workspace file, and reads it
  as the simulator extends it. **Streaming works.**
- Reload re-opens the name, follows it again, and reads whatever is current.
  **The reload button works.**
- The next iteration repoints the link; the viewer's held descriptor keeps the
  old file until the next reload, then moves. **The path is stable across
  identity churn.**
- **Zero bytes are duplicated.** The output exists exactly once, in the
  workspace, and the alias is a name for it.

The address is known before the work runs — `out.raw` resolves at bind time from
the declared output binding — so the alias needs no manifest and no completed
run. It is a *predicted* address rather than a recorded one, which is precisely
what makes it available during execution.

### What this dissolves

The `atomic` versus `in-place` publish mode disappears, along with the tradeoff
between a torn read and a missed refresh. There is no second file to write
either way. That whole branch of the design existed only because publication was
copying something, and it stops existing when it does not.

It also collapses two proposals into one. The earlier `latest/` directory
symlink and the publication rule were the same idea at two granularities; this
is that idea at the granularity that actually matters — the output file — and
built at the moment that actually matters, before the work starts.

### What it costs, honestly

**A dangling window.** Between repointing the link and the tool's first write,
the alias resolves to nothing and a reload fails to open. Short, and it fails
visibly rather than showing a wrong answer.

The obvious fix is not available: hedloom must not `touch` the declared output
to close the window, because `capture_outputs` treats an existing declared
output as evidence the work produced it. An empty placeholder would let a
command that wrote nothing report success — trading a brief honest error for a
silent wrong one.

**A coupling to the pruner.** A symlink does not keep its target alive the way a
hardlink would, so pruning an aliased workspace breaks the alias. The current
alias always points at the newest try, which `keep_latest = 1` spares by
construction, so the ordinary case is safe. The rule should still be explicit
rather than incidental: **`aliased` joins the skip reasons**, and a workspace
some alias points at is never a candidate.

**A reload mid-iteration jumps to a partial file.** If a new run starts while a
viewer is open on the previous one, reload lands on a file that is still being
written. That is what "current" means, and it is the behaviour asked for — but
it should be documented rather than discovered, because the file is genuinely
incomplete rather than merely new.

## Amendment 3 — directory outputs, and a defect they already have

> **Scoped out 2026-08-27 (user).** Directory-output support is being worked in
> a separate session. What stays here is the finding — the recording defect and
> its silent consequence for `larger_than` — because the pruner in the main plan
> would inherit it. The fix itself belongs to that other work.


Raised in review: a declared output may be a directory, not a file.

**The alias mechanism survives.** A symlink to a directory resolves for
`open()`, for `readdir`, and for any path *through* it —
`latest/tt/run/wave.raw` follows the link at `run` and lands in the current
target. Repointing is still one atomic `rename` over the symlink. Streaming into
files inside the directory works, and so does reload.

What does not survive is the recording underneath it, and that is a defect the
alias merely makes visible.

### What the code does today

`artifacts._file_reference` (`artifacts.py:85`) is the only path for a declared
`{"path": ...}` output. It checks `candidate.exists()` — which a directory
satisfies — then:

```python
stat = candidate.stat()
return ArtifactRef(name=name, kind="file", address=str(candidate),
                   size=stat.st_size, modified_ns=stat.st_mtime_ns)
```

- **`kind="file"` is hardcoded.** A directory output is recorded as a file.
  Nothing downstream can tell the difference.
- **`size` is `st_size` of the directory entry** — 4096 on ext4, whatever the
  tree beneath it holds. A forty-gigabyte output is recorded as four kilobytes.

The authoring surface offers no way to say otherwise either:
`file(path, *, kind="file")` (`authoring.py:351`) takes a `kind`, but that is
the *artifact contract* label — a semantic name like `"grid-declaration"` — not
a statement about what is on disk. There is no `directory(...)` declaration.

### Why this matters more now than before

It has been harmless because nothing consumed the recorded size. The pruner
would be the first thing to, and it would fail **silently in the worst
direction**: `larger_than = "1GiB"` compares against 4096 and never selects a
directory output, so the rule written specifically to catch the biggest
artifacts is the one guaranteed to miss them. No error, no warning, just a rule
that quietly does nothing.

`keep_logs`, `verify`, and the pin inventory are unaffected — they walk the tree
rather than trusting a recorded number — which is why the defect has stayed
invisible.

### The fix, and one thing it buys

1. **`directory(path)` beside `file(path)`** in the authoring surface, so the
   declaration says which it is rather than the recorder guessing.
2. **`ArtifactRef.kind` records `"file"` or `"directory"`**, and for a directory
   `size` is the walked total. Costs one walk at capture time, on a tree the
   process just finished writing.
3. **The pruner measures a directory by walking**, never by the recorded entry
   size — and refuses a `larger_than` rule against an artifact whose kind it
   cannot determine, rather than comparing against a number it knows is wrong.

And one asymmetry falls out that is worth having. A directory output **can** be
created by hedloom before the command launches — most tools expect their output
directory to exist anyway — so its alias resolves immediately and **the dangling
window disappears for directory outputs.** A file output cannot be pre-touched,
for the reason given above.

That requires one strengthening: a declared *directory* output must be
**non-empty** to count as produced. Otherwise pre-creating it defeats the same
check that pre-touching a file would, and an empty directory is a more
plausible-looking lie than an empty file.

### What no mechanism fixes

For a file, *partial* means shorter than it will be, and growth is visible. For a
directory, partial means **files that are not there yet** — and a consumer
globbing mid-run cannot distinguish "not written yet" from "never going to
exist". That is a property of directory outputs rather than of the alias, and
the honest response is to document it rather than to pretend an incomplete
directory announces itself.

## What this does not solve

A pinned workspace still holds an old path deliberately — that is the point of
pinning, and `latest/` moving past it is correct rather than a conflict.

And nothing here makes a *reused* result appear under a new path. If you edit an
input, revert it, and rerun, you land back on the original identity and the
original directory — which is right, and which the `supersedes` chain will show
as a return rather than a fourth iteration.
