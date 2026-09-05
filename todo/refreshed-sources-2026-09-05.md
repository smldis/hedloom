# A source that refreshes itself (2026-09-05)

**Written 2026-09-05. A proposal for review.** Nothing here is built.

This designs the second bullet of "What a re-architecture might mean" in
`design/rearchitecting-nested-studies-2026-09-04.md`: *a source that is
re-read per submission by declaration*. Read that note first, and
`design/nested-submission-and-capacity-2026-08-30.md` behind it — the
measurements there are not repeated, only used.

## What this is for

The staged pattern's whole payload is one sentence: **fingerprint this again,
now.** Everything else in `examples/live_source.py` — the nonce, the inner
plan, the shared session, the capacity headroom, the fourteen-line
`live_source_state.py` — is scaffolding holding that sentence in place.

## The incumbent this has to beat

Not the staged shape. `design/rearchitecting-nested-studies-2026-09-04.md`
records that the study which prompted the example went the other way and **pulled
before `submit()`** — no nonce, no inner plan, a symlink at a stable locator
under an address space whose directory is not identity-bearing. That is
materially simpler and gets the same invalidation.

So the question is not whether hedloom *can* re-read a source. It can, and the
answer is three lines in `main`. The question is what those three lines fail to
say. Four things:

1. **The obligation is not declared.** The plan says "read this address."
   Nothing says the address is live. A second caller who submits the same study
   without pulling reads whatever is there, reuses against it, and is not
   refused — the run looks identical to a correct one.
2. **The record cannot answer "was this fresh?"** It holds a fingerprint, not
   whether anyone re-read the world before taking it. Two runs a week apart
   with the same fingerprint are indistinguishable from one run and one
   skipped pull.
3. **The landing discipline is on every caller.** `fingerprint_file`
   (`run/src/hedloom_run/site.py:80`) falls back to `stat:<size>:<mtime_ns>`
   above 64 MiB, so re-writing identical bytes invalidates the study by
   touching the clock; and a fingerprint taken mid-write reads half a file.
   `examples/live_source.py:fetch` gets both right and needs a docstring to
   explain why. Every author rediscovers this.
4. **Conventions drift across callers.** In the example today, `main` pulls and
   the test monkeypatches; a scheduled rerun somewhere else may do neither.

**If none of those four matter to you, pull-before-submit is the answer and
this proposal should be rejected.** It is cheaper. What it cannot do is make
freshness a property of the study rather than of the caller.

## The observation that makes it cheap

hedloom already has the phase this needs. Every declared source is resolved,
read and fingerprinted **once per submission, on the submit host, before any
identity is fixed** — `Site.fingerprints` → `fingerprint_sources`
(`run/src/hedloom_run/site.py:425`, `:623`), called from `Study._run` before
`plan_bundles` ever runs.

    today:     resolve → fingerprint → plan_bundles → spend
    proposed:  resolve → refresh → fingerprint → plan_bundles → spend

One step, inserted into a phase that already exists, on the host that already
claims the authority to resolve an address. No new invariant, no new lifetime,
nothing that touches a worker. The staged pattern builds an entire second plan
to get a side effect into this phase.

## The constraints any option must respect

**1. A refresher must not become a way to run compute outside a plan.** The
invariant is *nothing is spent until submit, and what runs is what the Plan
showed*. A refresher is not an operation: no placement, no workspace, no
reuse, no record of its own. It must be cheap by contract, and the contract
needs a refusal behind it (see below), or this becomes an unplaced,
unrecorded, unbounded escape hatch — which is the thing hedloom exists to not
have.

**2. A source is identified by the bytes it holds, never by the code that
fetched them.** `_source_identity` (`exec/src/hedloom_exec/planned.py:65`)
digests `artifact`, `address` and `fingerprint`. A refresher joins none of
them.

This is deliberate and it is the opposite of the rule for operation bodies,
whose source text *is* in the digest (`authoring.py:406`). The asymmetry is
principled: an operation's effect is not observed, so the only honest proxy is
its text; a refresher's effect **is** observed — it is the fingerprint taken
one step later. Digesting the fetch code as well would invalidate a study for
reformatting a function whose output was byte-identical.

The cost is real and should be stated where an author will read it: **editing
your fetch code invalidates nothing.** If the new code fetches the same bytes,
the study reuses. That is correct, and it will surprise someone.

**3. Failure is fatal and early**, exactly as a missing source already is —
"the alternative is a run that reuses results computed from a file nobody can
show you" (`fingerprint_sources`).

---

## Option 1 — Simple: a callable on the declaration

```python
input_artifact(address("served", "document.txt"), artifact=DOCUMENT, refresh=fetch)
```

`refresh` is called with the resolved path, before fingerprinting. The Plan
document records `"refreshed": true` on the source — the boolean, never the
callable, since the document is plain data and must stay so.

**Concept:** the existing declaration grows one keyword.

Smallest possible change; the landing discipline (constraint 3 of the
incumbent's four) stays entirely with the author, because the refresher still
writes the file itself. Solves 1, 2 and 4. Does nothing for 3.

## Option 2 — Elegant: the refresher returns bytes; hedloom lands them

```python
@source(artifact=DOCUMENT, at=address("served", "document.txt"))
def document() -> bytes:
    return SERVICE["document"].encode()
```

hedloom owns the landing: write to a temp sibling, compare with what is there,
`rename` over it **only on a real change**. The mtime trap and the torn read
stop being things an author can get wrong, for everyone, once.

Returning `None` means *I refreshed it in place, look again* — the escape for a
fetch too large to hold in memory (an `rsync`, a database dump). Two shapes,
one declaration, and the common one is the safe one.

**Concept:** a source declares where it comes from, not only where it is.

Solves all four. Costs a new authoring verb.

## Option 3 — Creative: freshness belongs to the address space, not the study

The Site already owns what an address *means*; the plan holds an opaque
address and resolves nothing. Let the Site own whether that address is live:

```toml
[address_space.served]
root    = "/data/served"
refresh = "mypkg.feeds:document"     # called at submit, before fingerprinting
```

**The plan document does not change at all.** Which buys a property none of
the others do: the *same study document* runs against a frozen snapshot at one
site and a live service at another — a regression suite pinned to fixtures, and
production reading the feed, from one authored study with no flag. That is
precisely the Site/Plan split hedloom already believes in, applied one step
further than it currently reaches.

The cost is the mirror image of Option 2's benefit: a reader of the study can
no longer see that anything is live. Freshness becomes invisible exactly where
the work is authored.

## Option 4 — Recommended: Option 2, with Option 1's boolean

Take `@source` as the authoring surface, and from Option 1 keep the plain-data
`"refreshed": true` on each source in the document, so that:

* a plan document **alone** says which of its inputs are live, without needing
  the Python that authored it;
* `Study.summary()` can print it before anything is spent —

```
study live-source
plan schema 1: 2 invocations, 1 source
  served/document.txt   refreshed at submit
  tally                 tally        local
  summarise             summarise    local
```

which replaces the current example's `0 sources` and the paragraph explaining
what that zero proves.

**Option 3 is named as the follow-on and deliberately not built.** It is the
right answer to "one study, one site live and one frozen", and nobody has asked
for that. It can be added later without moving Option 2, because both write the
same `"refreshed"` boolean into the same field.

---

## Refusals it needs

A refresher is unplaced and unrecorded, so the refusals are what keep it from
being an escape hatch. All are raised on the submit host before anything is
spent:

| When | Refusal |
| --- | --- |
| the refresher raises | fatal and early, naming the source and its address — the class `fingerprint_sources` already raises for a missing source |
| it exceeds its time budget | names the source, states the budget, and points at the staged shape for work that wants a placement |
| it returns something that is not `bytes` or `None` | names the source and both accepted shapes |
| it returns `None` and the path still does not exist | the existing missing-source refusal, with the refresher named as what was expected to create it |
| a refresher is declared on an address space the site does not define | the existing resolution refusal, unchanged |

## What this does not solve

**Adaptive composition.** A child plan whose *shape* depends on a parent's
result is untouched by this. What this removes is the degenerate case — a
fixed, single-child tree whose only purpose is to get a side effect into the
pre-run phase. If a real study needs a tree, nesting or its replacement is
still an open question and `design/rearchitecting-nested-studies-2026-09-04.md`
still stands in full.

**Heavy fetches.** A refresher runs on the submit host, unplaced, unrecorded,
with no reuse and no retry. A 40 GB pull, or one that wants a farm job, is an
operation, and the staged shape is right for it. The boundary is: *cheap enough
that nobody would want it placed*.

## The example, after

```python
@source(artifact=DOCUMENT, at=address("served", "document.txt"))
def document() -> bytes:
    return SERVICE["document"].encode()

@study(name="live-source", default_policy=local())
def live_source():
    return reading.named("reading")(document)
```

plus the two operations that already exist, unchanged. Against the current
example, the concepts a reader must hold go from nine to two — the source
refreshes; everything below it reuses on content. Gone: the nonce, the second
study, the nested `submit`, the shared `Session`, `live_source_state.py`,
`live_session()`, the `local: 4` headroom and its comment, the atomic-write
discipline, and the hand-rolled `[{"key":…, "reused":…}]` projection.

`placements` in that example goes back to meaning *how much I want running at
once*.

## Work packages

* **W1 — flow.** `source()` authoring verb; `refreshed` on `ArtifactSource` and
  in `to_data()`. Refuse a refresher on a plan built from a finished document.
* **W2 — run.** `Site.refresh(document)` called immediately before
  `Site.fingerprints`; atomic landing; the budget; the refusals above.
* **W3 — facade.** Ordering in `Study._run`; the `summary()` line.
* **W4 — docs and example.** Rewrite `examples/live_source.py` to the shape
  above; decide whether the staged version survives as the *adaptive* example
  or is deleted.

W1 and W2 are independent and can be reviewed separately; nothing is observable
until W3.

## Tests, written before the code

1. Unchanged bytes → identical fingerprint → the whole plan below reuses.
2. Changed bytes → nothing below reuses.
3. A refresher writing byte-identical content does **not** move the mtime.
4. A refresher whose bytes exceed the hashed limit is fingerprinted by
   `stat:` and still reuses when unchanged — the trap, pinned.
5. A raising refresher fails the submission before any invocation runs, and the
   record shows no attempt.
6. Editing a refresher's source without changing its bytes reuses everything
   (constraint 2, pinned as behaviour rather than left as prose).
7. Two sources, one refreshed and one static; the static one is not re-read.
8. `summary()` names the refreshed source before anything is spent.
9. The same study submitted twice in one session refreshes twice.

## Open questions

1. **Sequential and `locally=True`.** The refresher runs on the submit host in
   every case, so unlike the nesting refusal — which is graph-kernel only, and
   leaves two kernels disagreeing about whether a plan is runnable — this has
   no kernel-dependent behaviour at all. Worth confirming that is accepted as
   an advantage rather than a coincidence.
2. **Should a refresher see the plan document?** Proposed: no. It would make a
   source depend on the plan reading it, and two studies sharing an address
   would refresh it differently — the address space would stop meaning one
   thing.
3. **What is the budget, and is it wall-clock?** A default that refuses a
   refresher taking minutes seems right; the number is a guess until something
   real uses this.
4. **A refresher runs even when everything below it would have been reused.**
   Necessarily — you cannot know without re-reading. It is the price of
   freshness, and the same price the staged shape pays. Confirm it is
   acceptable rather than assumed.
5. **Two submissions sharing an address space, refreshing at once.** The atomic
   rename keeps each read whole, but they can land different bytes and each
   fingerprint whichever it sees. Last writer wins, per submission. Is that
   enough, or does an address space need a lock?

Pending work, not a design record. If this is accepted it becomes a change and
the durable half of it belongs in `docs/` and in `flow/ONTOLOME.md`; if it is
rejected, the reasoning belongs in `design/` beside the note that raised the
question. Either way this file is then deleted rather than kept true.
