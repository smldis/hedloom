# Results, reuse, and looking before you run

## Reading a run: `StudyRun`

```python
run.outputs["verdict"].value  # what the study exported under that name
run["coarse:integrate"].artifacts["result"]["address"]
run.succeeded                 # True iff every invocation succeeded
run.summary()                 # one line per invocation: disposition, key, outcome
run.report.outcomes           # every InvocationOutcome, in plan order
run.document                  # the Plan this run executed
run.study_name                # the durable operator name this run was requested under
```

`run["coarse:integrate"]` looks up an `InvocationOutcome` by the authored key,
raising `KeyError` for a key nothing was authored with rather than returning
`None` and deferring the mistake.

## What the study produced: `run.outputs`

A study produces what its Plan **exports** — the mapping the `@study` body
returns — under the names the author gave those outputs:

```python
@study(default_policy=local())
def characterise():
    measured = measure.named("measure")(write_grid.named("grid")(steps=64))
    return {"measurements": measured, "verdict": evaluate.named("evaluate")(measured)}

run = characterise().submit(site=site)

run.outputs["measurements"].value       # 6
run.outputs["verdict"].value            # {"passes": False, "measured": 6}
run.outputs["verdict"].authored_key     # "evaluate" — which invocation produced it
run.outputs["verdict"].outcome.reused   # whether this run recomputed it
```

Authored names, never report or completion order. Appending an invocation to a
study cannot change what its outputs mean, a study that exports nothing has an
empty mapping, and one that exports several keeps them several — there is no
unwrapping to a single value and no preferred entry. A name the study never
exported raises `KeyError`, naming the ones it did.

Each entry is a `StudyOutput`, which keeps the reference and the outcome rather
than resolving them away:

| | |
| --- | --- |
| `.value` | what the exported port resolved to |
| `.artifact` | the recorded artifact for that port, or `None` if it has none |
| `.available` | whether the producing invocation succeeded in this run |
| `.outcome` | the producing `InvocationOutcome`, or `None` |
| `.authored_key` | the producer, as the study was authored |
| `.invocation_id`, `.output_name`, `.reference` | what the Plan exported |

A **port**, not a producer: one invocation declaring both `file("note.txt")`
and `returned()` exports two different things, and each resolves to its own.
A file or directory output resolves to its **recorded address**, the same value
a downstream operation receives — reading the bytes stays the caller's
decision. This is `hedloom_run.binding`'s rule, shared rather than restated, so
an exported output and a downstream input cannot disagree about what an
invocation produced.

**An output nobody produced is not `None`.** Reading `.value` or `.artifact` for
a failed or blocked producer raises `OutputUnavailable`, naming the invocation
and its recorded error. `None` returned by a succeeded body is a result and
stays one; the two are never the same answer.

```python
run = characterise().submit(site=site, stop_on_failure=False)
verdict = run.outputs["verdict"]
if verdict.available:
    decide(verdict.value)
else:
    print(verdict.outcome.outcome, verdict.outcome.error)
```

Exporting a value does not make it durably serializable. What is recorded is
what the attempt record can hold; an arbitrary Python object returned by a body
is available to this process and is not a preservation format.

### Execution, verdict, and conclusion

Three different questions, and only the first is `run.succeeded`:

* **Execution** — did the work run? An evaluation that computes
  `{"passes": False}` **succeeded**: it did its job and reported a failing
  measurement.
* **Verdict** — what did it return? That is `run.outputs["verdict"].value`, and
  hedloom neither interprets it nor prefers an output named like one.
* **Conclusion** — is the result accepted? That depends on criteria,
  assumptions and interpretation. Nothing here infers it from execution
  success, from reuse, or from a pin.

There is no aggregate `run.value`. It answered with the last invocation in
report order, which is the study's conclusion only when the conclusion happens
to be authored last — and silently stopped being it the moment anything was
appended. Export what the study produces and read it by name.

Each outcome carries `authored_key`, `operation`, `input_digest`,
`disposition`, `outcome`, `placement`, `value`, `artifacts`, `record`,
`try_number` and `error`. Those first three are public join keys on purpose:
**result tooling — a summary table, a run diff — is a consumer of this data and
needs no change to hedloom.**

`record` and `try_number` are the exact execution this invocation landed on:
the content-addressed record, and the try whose evidence was published or
reused. They are what to pin, prune around, or read back later, and they are
stated by the run rather than reconstructed afterwards. An invocation that was
blocked or refused reached no execution and leaves both `None`.

The report is in **plan order** regardless of completion order, so two runs of
one plan stay comparable; `on_event` fires in completion order, because those
are two different questions.

Execution outcome, evaluation verdict, and accepted conclusion answer different
questions. An evaluation operation can run successfully and return
`{"passes": False}` because the measured value misses its declared tolerance.
`run.succeeded` can still be `True`: the evaluation completed correctly.
An exception or a failed command instead reports an execution problem; the
execution record alone cannot decide whether that problem also has meaning
for the inquiry.

Accepting a conclusion requires interpreting the evidence under its criteria
and assumptions. Hedloom does not infer that acceptance from a returned value,
execution success, reuse, or pinning. A failing verdict can be useful evidence
for revising the question or choosing the next experiment.

## Reuse, and what invalidates it

Work whose declared inputs are unchanged is reused, not repeated. This is
`hedloom_exec`'s decision against content-addressed identity, and `hedloom`
neither re-implements nor overrides it.

**Folded into identity:**

* the operation's name, version, and a fingerprint of its body's *source*;
* every declared `config` value;
* every declared input's own identity, **transitively** — an upstream change
  propagates downstream automatically;
* a declared external source's **content** fingerprint, so editing an input
  input file in place correctly invalidates everything that read it.

**Deliberately excluded**, so changing it never invalidates a result: which
queue an invocation ran on, its walltime, cores, memory, host, and general
environment. Retuning an invocation's memory request or moving it to another queue
reuses the result it already produced — which is also why moving an operation
between `local`, `lsf()` and `pooled()` costs nothing.

The body fingerprint ignores blank lines and trailing whitespace but includes
everything else, docstrings included. So editing an operation body reruns every
invocation of that operation, and editing only a comment does not. That is
coarser than "the behaviour changed", deliberately: **a needless rerun costs
time, a missed one costs correctness.**

Only a `succeeded` try is reused automatically. A failure may be the work's
own verdict, or something incidental to it — an OOM kill, a preempted node —
that the record cannot tell apart. Failed tries are retained. A later
submission allocates the next try in the same content-addressed record, and
accepting the current one is a separate, durable human action
(`hedloom_exec.reuse.accept_for_reuse`). Acceptance selects standing evidence;
it does not pin it or accept its interpretation as an engineering conclusion.

## Finding what a run actually executed

A record keeps the content-addressed name, while each execution has an
immutable `<record>-<try>` workspace. Editing an identity-bearing input moves
the record; retrying unchanged inputs increments only the try.

The run tells you which one it used:

```python
outcome = run["coarse:integrate"]
outcome.record        # 'hedloom-<20 hex>' — the computation's record
outcome.try_number    # the try whose evidence was published or reused
outcome.artifacts["result"]["address"]   # where that try's output landed
```

That pair is the address for everything else. There is no per-study view of
outputs and no name-shaped selector, because a record holds a computation and
belongs to no study: two studies declaring the same work reach the same record,
so `<study>:<key>` could only ever have named whichever of them ran first.

The operator commands take a record identity, or any unambiguous prefix of one,
optionally with `#<try>`:

```console
hedloom pin   --site site.toml hedloom-3f9c2a10#0 --reason "quoted in a report"
hedloom pins  --site site.toml
hedloom unpin --site site.toml pin-1a2b3c --reason done
hedloom prune --site site.toml --record hedloom-3f9c2a10 --failed
```

`pin` protects one terminal try's workspace; `prune` surveys and reclaims spent
ones. Both address records and tries and nothing else.

What this does **not** give you is a way to find a record you have no reference
to. Discovery — listing studies, browsing runs, asking what a study produced
last week — is not built. A caller that wants a reference later must keep the
one its run reported. This is a real gap in the operator interface, and it is
the piece being designed separately rather than approximated here.

On disk, a layout-1 record keeps `events.jsonl`, one immutable
`manifest/<try>.json` per terminal try, and an atomic `standing.json` pointer
to the evidence currently reusable. Identity renderings have changed as the
contract changed, so a record written under an older one is not selected by
today's digest — its contents remain perfectly readable, because layout 1 has
not changed. There is no migration and none is needed: old records simply are
not reused.

```{warning}
**Reuse trusts your declaration, across studies.** An operation whose result
depends on an undeclared file, wall-clock time, or a mutable network resource
is not honestly reusable, and no digest detects that. Because one shared record
serves every study that declares the same work, such an error reaches whoever
declares it, not only the study that made it. To repeat work deliberately,
declare the distinction — a seed, a repetition index. Renaming a study or an
authored key does not ask for a second execution.
```

## Starting from a file the study did not write

An operation may declare an `input_artifact` source as an input and be handed
its located path. Every declared source is read **exactly once per submission,
before anything else runs**, and that one reading does both jobs: it
fingerprints the source (deciding whether downstream work is stale) and locates
it (what the body receives). They are computed together because those two
questions must agree about which file was meant.

A source that cannot be resolved, or does not exist, is fatal **before anything
runs** — the alternative is a run that reuses results computed from a file
nobody can show you. Addresses resolve on the submitting machine, which assumes
a shared filesystem for any placement that is not local.

Sources are hashed rather than stat'ed, so an ordinary `git checkout` does not
invalidate a sweep by touching `mtime`. Anything above 64 MiB falls back to size
and modification time, and the fingerprint's own prefix (`blake2b:` versus
`stat:`) says honestly which method produced it.

## Looking at a study before running it

Three views, answering three questions, none of which spends anything.

`Study.summary()` is the first — see
[before spending](authoring.md#before-spending-summary). The other two are in
`hedloom.visualize`:

```python
import hedloom.visualize as visualize

print(json.dumps(visualize.structure(subject), indent=2))
visualize.render(subject, "graph.svg")
```

* `structure(study)` reads the Plan into plain nodes and edges, in the
  vocabulary the study was authored in. No Dask, no graphviz; works even for a
  plan bound for a farm.
* `render(study, "graph.svg")` draws the *lowered Dask graph*. Needs `graphviz`
  and a system `dot`.

Every operation is bound to a stand-in that refuses to run — computing it raises
`RefusedComputation` rather than producing a number nobody computed.
`submit()` remains the only way a study runs.

`visualize` is a submodule, deliberately not in `hedloom`'s top-level `__all__`:
drawing a graph is a diagnostic, not part of the authoring surface. `graphviz`
and `bokeh` are diagnostics the units themselves do not depend on — install them
into the project-local `.toolchain/venv` (`.toolchain/README.md`).

## Retaining and pinning try workspaces

Every try remains inspectable until the Site's operator-owned retention policy
selects its workspace. `hedloom prune --site site.toml` is a dry-run survey;
only `--apply` removes selected bytes, after a second check under the record
claim. Records and immutable manifests remain even when a spent workspace is
reclaimed.

What protects a try is a property of the evidence, never of who asked for it:
the retention floor (seven days by default) protects recent work, the standing
evidence a later run would reuse is never a candidate, an unreconciled or
non-terminal try is never a candidate, a contended record is skipped rather
than waited on, and a pin is a refusal. `examples/retention.py` runs exactly
that lifecycle and checks the byte arithmetic at every step.

Use `hedloom pin --site site.toml <record>#<try> --reason TEXT` when a report
or external tool holds a try path; `run[key].record` and `.try_number` are
where that reference comes from. A pin is terminal-only and per-try, stored in
the record with actor, reason, layout and content digests. Verification detects
drift; a changed record layout reports the promise void. Write-bit removal is
an accident-catching guardrail, not enforcement—the owner and open descriptors
can still modify content, and a writable parent can still rename it.
