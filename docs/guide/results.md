# Results, reuse, and looking before you run

## Reading a run: `StudyRun`

```python
run["coarse:integrate"].artifacts["result"]["address"]
run.value            # the plan's conclusion: its final invocation's value
run.succeeded        # True iff every invocation succeeded
run.summary()        # one line per invocation: disposition, key, outcome
run.report.outcomes  # every InvocationOutcome, in plan order
run.document         # the Plan this run executed
```

`run["coarse:integrate"]` looks up an `InvocationOutcome` by the authored key,
raising `KeyError` for a key nothing was authored with rather than returning
`None` and deferring the mistake.

Each outcome carries `authored_key`, `operation`, `input_digest`,
`disposition`, `outcome`, `placement`, `value`, `artifacts` and `error`. Those
first three are public join keys on purpose: **result tooling — a summary table,
a run diff — is a consumer of this data and needs no change to hedloom.**

The report is in **plan order** regardless of completion order, so two runs of
one plan stay comparable; `on_event` fires in completion order, because those
are two different questions.

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

Only a `succeeded` attempt is reused automatically. A failure may be the work's
own verdict, or something incidental to it — an OOM kill, a preempted node —
that the record cannot tell apart. Failed attempts are retained rather than
silently retried, and accepting one is a separate, durable, human action
(`hedloom_exec.reuse.accept_for_reuse`).

```{warning}
**Reuse trusts your declaration.** An operation whose result depends on an
undeclared file, wall-clock time, or a mutable network resource is not honestly
reusable, and no digest detects that.
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
