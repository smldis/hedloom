# Results, reuse, and looking before you run

## Reading a run: `StudyRun`

```python
run["coarse:integrate"].artifacts["result"]["address"]
run.value            # the plan's conclusion: its final invocation's value
run.succeeded        # True iff every invocation succeeded
run.summary()        # one line per invocation: disposition, key, outcome
run.report.outcomes  # every InvocationOutcome, in plan order
run.document         # the Plan this run executed
run.study_name       # the durable namespace this run was recorded under
```

`run["coarse:integrate"]` looks up an `InvocationOutcome` by the authored key,
raising `KeyError` for a key nothing was authored with rather than returning
`None` and deferring the mistake.

Each outcome carries `authored_key`, `operation`, `input_digest`,
`disposition`, `outcome`, `placement`, `value`, `artifacts`, `changed_keys` and
`error`. Those
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

Only a `succeeded` try is reused automatically. A failure may be the work's
own verdict, or something incidental to it — an OOM kill, a preempted node —
that the record cannot tell apart. Failed tries are retained. A later
submission allocates the next try in the same content-addressed record, and
accepting the current one is a separate, durable human action
(`hedloom_exec.reuse.accept_for_reuse`). Acceptance selects standing evidence;
it does not pin it.

## Following the current file output

A record keeps the content-addressed name, while each execution has an immutable
`<record>-<try>` workspace. Editing an identity-bearing input moves the record;
retrying unchanged inputs increments only the try. For each declared file
output, Hedloom also maintains a stable view:

```text
<Site.root>/latest/<study>/<authored-key>/<output>
```

The entry is a symlink to the selected try's workspace file. It is created
or atomically repointed before the work launches, so reopening it follows the
current try and can observe a file while it grows. A program that already has
the old file open keeps that file descriptor until it reopens. Before the work
first writes the file, the symlink intentionally dangles; Hedloom does not
pre-create the declared output, because existence is evidence that the work
produced it.

The operator commands accept either `--site site.toml` or `--root ATTEMPTS`:

```console
hedloom where --site site.toml amplifier:point:write --output result
hedloom check --site site.toml /path/cached/by/a/consumer
hedloom log --site site.toml amplifier:point:write
```

The selector is `<study-name>:<authored-key>`. The first colon separates the
study namespace; further colons belong to the authored key, as in the keyed
sweep point `point:write`.

`where` prints the current workspace path for a script that resolves rather
than remembers. `check` exits zero for a current recorded path, one for a stale
one, and two for a path that is not a recorded attempt. `log` lists distinct
record creations newest first, marking the alias target as current and naming
which identity keys changed. Returning to an earlier result moves the current
marker back to it without weakening or replacing its original identity.

On disk, a layout-1 record keeps `events.jsonl`, one immutable
`manifest/<try>.json` per terminal try, and an atomic `standing.json` pointer to
the evidence currently reusable. Roots written before the Phase 1 identity and
layout change are unreadable; this prototype has no migration.

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

## Retaining and pinning try workspaces

Every try remains inspectable until the Site's operator-owned retention policy
selects its workspace. `hedloom prune --site site.toml` is a dry-run survey;
only `--apply` removes selected bytes, after a second check under the record
claim. Records and immutable manifests remain even when a spent workspace is
reclaimed.

Spent storage is storage nothing resolves to, so a failure is not reclaimable
while it is still the newest try at its authored key. A `latest/` alias is
bound before a body runs, which is what lets a tool watch an output as it is
written — and it means every current try has an alias, whether it succeeded or
not. Such a try is skipped as `aliased`. A failed try becomes a candidate once
a later run at the same key supersedes it and the alias moves, which is also
the point at which nobody is still reading it.
`examples/retention.py` runs exactly that lifecycle.

Use `hedloom pin --site site.toml <selector> --reason TEXT` when a report or
external tool holds a try path. A pin is terminal-only and per-try, stored in
the record with actor, reason, layout and content digests. Verification detects
drift; a changed record layout reports the promise void. Write-bit removal is
an accident-catching guardrail, not enforcement—the owner and open descriptors
can still modify content, and a writable parent can still rename it.
