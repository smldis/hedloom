# Reclaiming produced files — a proposal

**Written 2026-08-26. A proposal, not a description of the code.** Nothing here
is built. Read `ONTOLOME.md` and `docs/` for what the units guarantee today.

The request was "garbage collection of submitted studies and other produced
files, automatic and manual, with a simple policy." What follows accepts the
need and rejects two thirds of the framing, for reasons the record itself
supplies.

## Why "garbage" is the wrong word, and what to call it instead

`hedloom_exec.reuse.stale_attempts` already answers this in its own docstring:

> These are not garbage. They are what the work used to conclude, and being
> able to name them is how a changed input gets explained rather than silently
> overwritten.

A superseded attempt is not waste, and a failed one is retained on purpose. What
is expendable is never the *record* — it is the **bytes**, and only because the
manifesto already licenses their absence:

> Portability does not require committing every raw waveform to Git. It requires
> that another permitted operator can discover what exists, determine how it was
> produced, and retrieve **or reproduce** it without reconstructing hidden
> session state.

So the operation is not collection of garbage. It is **reclaiming storage from
results that remain explained and re-derivable**. The target state of a reclaimed
attempt is not *gone*; it is *recorded, reproducible, not present*. Every design
decision below falls out of holding that distinction.

The operator-facing verb is `reclaim`. `forget` is a second, louder operation
(§10) that destroys provenance and must not share a name with the safe one.

## The hole this closes, which is the real reason to build it

`hedloom_exec.attempt.is_reusable` decides reuse from the manifest's outcome and
nothing else:

```python
if manifest is None:
    return False
if state.reuse_accepted:
    return True
return manifest.get("outcome") in REUSABLE_OUTCOMES
```

It never asks whether the artifact addresses in that manifest still resolve.
Materialization records `address`, `size` and `modified_ns` *precisely* so a
later invocation can reopen and validate the file — and no code path validates
anything. Today, if a scratch cleaner, a quota sweep, or an operator's `rm -rf`
removes `work/<identity>/`, the next run reuses the attempt, publishes its
address downstream, and the consumer opens nothing. The failure surfaces
somewhere else entirely, as a tool error about a missing file, with a reuse
decision three steps upstream as the actual cause.

That hazard exists **now**, without any reclaim feature. Building reclaim without
closing it would make a rare corruption routine. Closing it makes the system
more correct than it is today, which is the honest justification for the slice:
this is not a storage chore that happens to touch reuse, it is a reuse defect
whose fix happens to enable storage policy.

Per the unit's own rule — *incompleteness may refuse; it may not be silently
wrong* — the three cases must be told apart:

| Record | Bytes | Disposition |
| --- | --- | --- |
| `succeeded`, no `reclaimed` event | present, size and mtime agree | reusable, as today |
| `succeeded`, `reclaimed` event recorded | absent, by decision | **not** reusable; rerun re-derives it under the same identity |
| `succeeded`, no `reclaimed` event | absent, or size/mtime disagree | **refuse loudly** — `ArtifactVanished`. Something outside the record removed evidence it did not own. |

The third row is the one that pays for the whole feature.

## What is produced, and who owns it

| Produced | Where | Size | Owner | Reclaimable |
| --- | --- | --- | --- | --- |
| Authored study, site profile | the repository | tiny | the author | **never** |
| External sources reached by `input_artifact` | an address space | any | **not us** — we located them | **never** |
| Attempt record: `events.jsonl`, `manifest.json`, `observations.jsonl` | `<root>/attempts/<identity>/` | KiB | `hedloom-exec` | only by `forget` |
| Declared outputs | `<workspace_root>/work/<identity>/<name>` | **the bulk** | the attempt | by policy |
| Undeclared workspace residue and `stdout.log` | same directory | often larger than the outputs | nobody resolves to it | earliest and cheapest |
| Run reports, `perf.html`, `plan.svg` | `<root>/` | KiB | the run | by age, last |

Two of those rows decide most of the design. Files reached through an address
space are read by us and written by someone else; a reclaim pass that can delete
outside the roots it was given is a bug with no upside, so **reclaim never leaves
`root` and `workspace_root`**, and refuses a policy that names a path outside
them. And the record/bytes split is clean on disk already — `attempts/` and
`work/` are separate trees, and `Site.workspace_root` can put them on separate
filesystems — so "keep the KiB, drop the GiB" needs no migration.

## The missing state: `reclaimed`

Add one event to the journal's closed vocabulary, beside `reuse_accepted`:

```
reclaimed  {"policy": "...", "actor": "...", "freed_bytes": N,
            "artifacts": [{"name": ..., "address": ..., "size": ..., "digest": ...}]}
```

Folded into `AttemptState.reclaimed`, consulted by `is_reusable`. The manifest is
**not** rewritten — it is the published terminal result and rewriting it would
make published evidence mutable. The log carries the later fact, which is what an
append-only log is for.

This buys three properties for one event:

- **Reuse stays sound by construction.** A reclaimed attempt is not reusable, so
  nothing downstream can be handed an address that no longer resolves.
- **The deletion is explainable.** Which policy, when, by whom, how much. The
  ontology's promise that superseded work is "retained and explainable" survives
  the bytes.
- **Rerun restores it exactly.** Identity is content-addressed, so re-deriving a
  reclaimed result lands on the same identity in the same directory. Reclaiming
  is spending compute later to save storage now — a knob, not a loss.

### Hash once, at reclaim

`open-concepts.md` records artifact checksums as *deliberately* deferred: hashing
multi-GiB files on every run is a real cost, and mtime plus size is the cheap
staleness signal. That argument is about the **run** path, and it does not apply
here. A reclaim pass is already about to spend the I/O of removing the file; a
digest taken at that moment costs one sequential read of a file that is about to
stop existing, once in its life, off the critical path of every study.

So record a digest in the `reclaimed` event. The record of a reclaimed artifact
is then *stronger* than the record of a live one: size and mtime become size,
mtime and content.

**Corrected 2026-08-26, by measurement.** An earlier draft of this section
claimed the digest would let a rerun *prove bit-identical re-derivation*. It
will not, and the repository's own roots say so. Across every run root in this
tree there are 40 raw output files of the bulk kind, every one 29 389 bytes, and
**40 distinct digests** — because the tool stamps a wall-clock `Date:` into the
header it writes. Identical declared inputs, identical size, different bytes,
every single run. Anything that verifies re-derivation must compare *declared
measurements*, never bytes, and no digest can change that.

What the digest is still worth is narrower and real: it identifies a copy
restored from a backup or an archive as the same bytes that were reclaimed, and
it distinguishes "we removed this, deliberately, and here is what it was" from
"something damaged this." That is evidence about an artifact's history, not
proof about an operation's determinism, and it should not be sold as the
second.

## What may be reclaimed, ordered by confidence

Confidence that bytes are not needed is not uniform, and a policy that treats it
as uniform is how a GC eats a result someone was about to read. Six tiers,
cheapest and safest first. A pass walks them in order and stops when its goal is
met.

**Tier 0 — never.** Authored files. External address spaces. Any attempt whose
phase is not terminal: live work owns its workspace, and the tenant is still in
it. Any attempt with a `pinned` event.

**Tier 1 — undeclared residue.** Files in a workspace that are not declared
outputs. Nothing can resolve to them; no reuse decision reads them; they are
"unnamed evidence" and often the largest thing a launcher leaves behind. Reclaim
by age, keeping `stdout.log` while the attempt's tier allows it. Highest
bytes-per-risk of any tier by a wide margin.

**Tier 2 — superseded outputs.** `stale_attempts()` already names these exactly:
prior results for an invocation whose inputs have since changed. Their record is
what explains a changed input; their bytes are what the work *used to* produce,
and nothing will ever ask reuse for them again, because reuse asks by digest and
the digest moved. Bulk reclaim, record kept in full.

**Tier 3 — failed attempts.** Retained deliberately and never reused, so their
bytes are diagnostics with a half-life. Age-gate them, generously. An attempt
carrying `reuse_accepted` leaves this tier and joins tier 5.

**Tier 4 — live outputs nothing reads.** A current, reusable result whose
artifacts no downstream invocation of any retained plan generation consumes — a
terminal output already folded into a report. Requires liveness (§6) to be
honest, and is where a mistake first costs real compute. Off by default.

**Tier 5 — live outputs something reads.** Pinned results, accepted failures, the
current run's conclusions. Only by explicit `reclaim --identity`, one at a time,
never by policy.

## Roots, liveness, and the one prerequisite

A collector needs roots, and hedloom cannot currently produce them.

`scan_attempts` reads `plan`, `invocation` and `input_digest` from each `created`
event. That is enough to group attempts and to compare digests **against a
current digest** — but the current digest comes from `plan_bundles(document)`,
and the Plan document is never written to the root. `report.json` records
`plan_id`, authored keys and dispositions; it does not record the document, and
it does not even record which attempt identity served which invocation.

So an operator standing at a study root today cannot answer "which attempts does
this study still resolve to?" without re-authoring the study in Python and
replanning it. That is exactly the private-state failure the headless-authority
test rejects: a durable operation depending on something no file records.

Without the document, "superseded" degrades to "not the newest digest I happened
to see", which is a heuristic, and this project does not ship heuristics where a
fact is available.

**Work package one is therefore not the collector.** It is: a run writes its Plan
document, its resolved source fingerprints, and its invocation → attempt-identity
map into the root as a **generation record**, `<root>/generations/<plan_id>/<ts>/`.
Retaining N generations is itself the coarsest retention policy an operator has,
and stated in exactly the terms they think in: *"the last three times I ran this
study stay reproducible; older ones keep their records and lose their bytes."*

The root then explains itself. Liveness is a mark over the retained generations,
sweep is everything else in tier order, and the whole thing is inspectable
without importing the study. This package is worth building even if reclaim is
never built.

## Policy

One `[retention]` table in the **site** profile, next to placements and roots,
because retention is a property of where work lands, not of what the work means.
A study authored once and run at two sites should keep for a week at one and a
year at the other without its text changing.

```toml
[retention]
generations = 3           # plan generations that stay live (tier 0)
residue     = "3d"        # tier 1
superseded  = "reclaim"   # tier 2: reclaim | keep
failed      = "30d"       # tier 3
unread      = "keep"      # tier 4: keep | reclaim
floor       = "14d"       # nothing younger than this, in any tier

[retention.watermark]
path  = "workspace_root"
free  = "500GiB"          # run a pass when free space drops below this
```

Six keys and a watermark. Every key names what may be reclaimed; none names what
is deleted, and none can name a path — the paths are the site's roots, already
declared. A duration is a *floor on safety*, never a schedule: `failed = "30d"`
means a failed attempt younger than thirty days is off limits, not that one older
than thirty days is doomed. `floor` overrides every tier, so a policy edit cannot
make the last hour of work vanish.

An unrecognised key is refused rather than ignored. A retention table that would
reclaim tier 5 is refused. A `watermark.path` outside the site's roots is
refused.

## Triggers, and the tripwire

**Manual, and the default.** `hedloom reclaim <root>` **is a dry run.** It prints
what each tier would free and spends nothing. `--apply` is the second gesture.
This is the same shape as the surface the unit already has — `study(plan).summary()`
shows what a run will do and spends nothing; `submit()` spends — and it should be
the same shape for the same reason. A reclaim plan is inspectable before it costs
anything, and diffable in a review.

```console
$ hedloom reclaim ./_runs
plan  ota-study     generations 5 retained 3   live 47 attempts
tier1 residue       118 files   41.2 GiB   oldest 19d
tier2 superseded     22 attempts 12.8 GiB   ~ 3h 40m to re-derive
tier3 failed          6 attempts  0.9 GiB   oldest 44d
tier4 unread          — disabled by policy
                     ------------------------------
                     54.9 GiB reclaimable, 0 records removed
                     re-run with --apply
```

**Automatic after a run**, governed by the site. The façade may run a bounded
pass when a run finishes, if the site declares retention. It is bounded in the
strict sense: tiers 1–3 only, never the generation just written, and a failure to
reclaim never fails the run — it warns, because a study that fails for lack of
disk hygiene has confused two concerns.

**Automatic under pressure**, which is the trigger that actually keeps a shared
filesystem alive. Age policies are proxies for the thing operators care about,
which is free space. A watermark states the real goal: when free space at the
watched path drops below `free`, run a pass in tier order and stop as soon as it
is satisfied. This reclaims the least it can rather than the most it may, which
is the correct bias — every deleted byte is compute someone might spend again.

**Scheduled**, by cron or CI calling the same CLI on the same plain root. No
daemon, no service, no second way to express the policy. Headless authority means
the scheduler is already someone else's problem, solved.

**The tripwire.** `submit(reclaim=...)`, `@study(retention=...)`, or any argument
that lets an authored study decide what is collected. The unit's invariant reads
*a body decides what runs; it never decides whether it runs*; the same reasoning
gives *a study decides what is produced; it never decides what is kept*. Retention
is an operator's decision about a filesystem they own, and a study that could
delete its own competitors' evidence is a study that can lie about what it
compared. `retry=`/`until=` are named in `AGENTS.md` as the tripwires on `submit`;
`reclaim=` joins them.

## Reclaim as a cost decision, not a chore

Every fact needed to price a reclaim is already in the journal. `events.jsonl`
timestamps every event, so `terminal.at − submit_intent.at` is the attempt's wall
duration, and the watcher's `observed` transitions separate queue latency from
compute. The manifest carries every artifact's size. So each candidate has both
sides of the trade already recorded:

> **bytes freed per second of recompute risked.**

Rank candidates by it and the tiers acquire an interior order that is measured
rather than assumed: 40 GiB of residue from a two-second launcher is free money;
900 MiB from a nine-hour invocation is not, whatever its age. Print it, and the
question stops being "is this old enough?" and becomes an engineering decision an
operator can defend — which is what this project asks of every other number it
reports.

It also gives the watermark pass a correct greedy order for free, and it costs no
new metadata, no new file, and no index. It is a fold over records that
`scan_attempts` already reads.

## `forget`, kept separate

Removing an attempt's *record* destroys provenance, and provenance is what the
manifesto protects. It is occasionally right — a study run against a mistaken
input, a root being decommissioned, an operator with an obligation to delete —
so it should exist and should not be `rm -rf`.

`hedloom forget <root> --plan <plan_id>` removes generations, records and bytes
together, refuses while any attempt is non-terminal, refuses without `--apply`,
and writes a tombstone naming what was removed, when, and by whom. It is not a
tier, it is not reachable from `[retention]`, and no automatic trigger may call
it. The separation is the safety property: everything automatic is recoverable by
rerunning; only a deliberate, named operation is not.

## Concurrency, and the lock this must not trust blindly

A reclaim pass and a run can meet on the same attempt. The mechanism already
exists: `journal.claim()` holds the attempt exclusively across read, intent and
submission, and raises `ConcurrentClaim` rather than waiting. Reclaim takes the
same claim, and on `ConcurrentClaim` **skips the attempt** — a contended attempt
is being used, which is the answer to the question the pass was asking. Deleting
bytes while holding the claim, then appending `reclaimed` under it, makes the
window between "reusable" and "reclaimed" unobservable to any other claimant.

The order within the claim matters and inverts the run path's: record the
`reclaimed` event **before** unlinking. A crash then leaves an attempt marked
reclaimed whose bytes may still exist — wasted space, self-healing on the next
pass. The opposite order leaves bytes gone with the record still claiming they
are reusable, which is the `ArtifactVanished` state, and a crash must not
manufacture the case the feature exists to detect.

This rests entirely on `flock` being honoured, and `open-concepts.md` already
records, unreviewed, that **NFS may not honour it**. A reclaim pass makes that
concern materially worse: it is the first operation likely to be run from a
different host than the study, on a schedule, against a shared root — the exact
second-controller scenario the register names as the trigger to revisit. So the
package below is gated on it, and until it is settled reclaim refuses a root
whose mount cannot honour the lock, rather than proceeding carefully.

## Where each piece lives

Put a capability in the smallest component whose ontology can explain it.

| Piece | Unit | Why there |
| --- | --- | --- |
| `reclaimed` and `pinned` events; folding them into `is_reusable` | `hedloom-exec` | It owns the durable record and the reuse decision. Nothing else may. |
| Artifact validation on the `completed` disposition; `ArtifactVanished` | `hedloom-exec` | It published the addresses; it validates them. |
| `reclaim_attempt(journal, ...)` — one attempt, claimed, hashed, unlinked, recorded | `hedloom-exec` | One attempt's lifecycle is its whole purpose. |
| Tier classification over a root and a plan document | `hedloom-exec` | `plan_bundles` already takes a *document*, so this needs no `hedloom_flow` import and keeps the unit dependency-free. |
| `[retention]` parsing; watermark path resolution | `hedloom-run` (`Site`) | Roots and address spaces are already the site's. |
| Generation record written at run end | `hedloom` | It is the only unit holding both the authored study and the run. |
| `hedloom reclaim` / `hedloom forget` CLI | `hedloom` | The operator-facing composition, which is what this unit is. |

`hedloom-exec` still imports neither `hedloom_flow` nor Dask. `hedloom-flow` is
untouched: retention is not a planning concept.

## Work packages

Each is independently useful and leaves evidence. Ordered so that the correctness
fix lands before anything deletes.

1. **Validate on reuse.** Stat every declared artifact before returning
   `completed`; raise `ArtifactVanished` on absence or on size/mtime
   disagreement. *Evidence:* an existing example, run to completion, its work
   directory removed by hand, rerun — refuses with the identity and the missing
   address, where today it reuses and fails downstream. **Ship alone.** It is a
   defect fix and needs no policy.
2. **The generation record.** A run writes its Plan document, source
   fingerprints and invocation → identity map under `<root>/generations/`.
   *Evidence:* a fresh clone can name every live attempt for a study from files
   alone, with no Python import.
3. **`reclaimed`, `pinned`, and the single-attempt primitive.** Event vocabulary,
   fold, `is_reusable`, claim-ordered `reclaim_attempt` with the digest.
   *Evidence:* reclaim an attempt from an example, rerun, observe it re-derived
   under the same identity with a digest matching the one recorded at reclaim.
   That is the round trip the whole design rests on, measured rather than argued.
4. **Tiers and the dry-run CLI.** Classification, the cost ranking, `hedloom
   reclaim` printing and spending nothing. *Evidence:* the printed plan on the
   accumulated example roots, which today hold several superseded generations —
   real input, not a fixture.
5. **`--apply`, `[retention]`, and the post-run trigger.** *Evidence:* a run
   under a site with retention leaves the tree in the state the dry run
   predicted, exactly.
6. **The watermark.** Free-space trigger, greedy in cost order, stopping at the
   goal. *Gated on the NFS lock question* if the watched root is shared.
7. **`forget`.** Last, separately, loudly.

Packages 1 and 2 are worth building whether or not 3–7 ever are.

## What this does not solve, measured

The mechanism above reclaims whole attempt workspaces inside one root. Two
questions that framing does not answer, checked against this tree rather than
reasoned about:

**Is there redundancy worth deduplicating?** Barely. Across every `work/` tree
in the repository, 27% of *files* are byte-identical copies of another file —
but only **3% of bytes**. The repeats are small declared inputs (a 578-byte deck
copied into fifteen workspaces); the artifacts that actually occupy space are
unique by construction, per the timestamp finding above. A content-addressed
store, hardlink coalescing, or reflink sharing would recover roughly three
percent here and would cost an indirection between an artifact's recorded
address and where its bytes are — which is the fact the whole provenance
argument rests on. **On this evidence, do not build one.** The unit of reclaim
is the attempt, and it is the right unit, because there is nothing shareable to
be clever about.

**Where does the space actually go, then?** Sideways, into roots. This tree
holds nine roots for what is recognisably the same study — `ota`, `ota-clean`,
`ota-clean.prev-1785982115`, and six `ota-clean-<timestamp>` siblings — each
with a full set of distinct, unshareable outputs. A pass bounded to `root` and
`workspace_root`, as §3 requires it to be, is structurally blind to this: every
one of those roots looks entirely live when examined alone.

That is not an argument for widening the pass. It is an argument that the
duplication is *created* upstream, by there being no legible way to hold more
than one run of a study in one root — so an operator makes a new root and
renames the old one by hand, which is exactly what `ota-clean.prev-1785982115`
is a fossil of. **The generation record (package 2) is therefore not merely a
prerequisite for liveness; it is the change that stops this from accumulating in
the first place.** One root, N generations, reuse working across them, and the
per-root pass becomes sufficient because the thing it cannot see stops being
produced.

The honest summary: reclaim bounds accumulation *over time* within a root, and
does nothing about multiplication *across* roots. Only the second half of that
was in the request, and only the first half is in this mechanism.

## Exclusions

- **No index, no database.** `scan_attempts` says an index belongs here only once
  a real workload makes the scan hurt. A reclaim pass over a large root is
  plausibly that workload, and if it proves so, the index is its own decision
  with its own evidence — not something smuggled in as an optimisation inside a
  storage feature. Until then, the pass rescans, and prints how long it took.
- **No reference counting.** Liveness is a mark over retained generations, which
  are files. A refcount is derived state that can disagree with the tree, and the
  tree is the authority.
- **No compression, no deduplication, no tiering to object storage.** Each is a
  separate capability with its own contract, and reclaim removes rather than
  transforms, because a transformed artifact is a new artifact and this operation
  must not create any. Deduplication additionally has the measurement against it
  (3% of bytes), so it is excluded on evidence and not only on principle — which
  means the exclusion is falsifiable: measure a real root, and if the number is
  different there, this bullet is what should change.
- **No partial-artifact reclaim.** An attempt's declared outputs are reclaimed
  together or not at all. A half-materialised attempt is a state nothing in the
  record can express, and inventing one to save a few GiB is how the reuse
  argument stops being checkable.
- **Reclaim never touches an address space.** Those files are someone else's.

## Open questions for the operator

1. **What is the real pressure?** A shared filesystem near quota, a laptop, or an
   accumulation nobody has hit yet? The watermark is the right answer to the
   first and overbuilt for the third — and the third would justify shipping
   packages 1–4 only, leaving every deletion manual.
2. **Should `generations` be the whole policy?** "Keep the last three runs of
   this study whole, reclaim the bytes of everything older" is one number, needs
   no durations, and covers most of what durations are used to approximate. The
   six-key table may be five keys too many.
3. **Is tier 4 wanted at all?** It is the only tier that can cost real compute,
   and disabling it permanently would simplify the liveness argument to
   "generations only".
4. **Does anything already delete these roots?** `examples/_runs/` holds an
   `ota-clean.prev-1785982115` directory, which suggests ad-hoc rotation is
   already happening by hand. If a wrapper script exists, it is the requirement
   document for this feature and should be read before any of it is built.
