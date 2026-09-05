# Hedloom Ontology

This is the ongoing self-study of the component rooted here. Briefly inhabit
its perspective as you work: what are you learning about what it is, why it
exists, and what it might become? Help this account evolve when you have
something useful to add.

## Purpose and scope

Hedloom is the operator-facing composition of the three units beneath it. It owns
one thing none of them could own alone: the join between an authored study and
its execution. An author writes operations, a flow, and a plan; `submit` runs
exactly those, because it holds both halves rather than requiring two files to
agree about them. The facade also owns the study's durable, operator-facing
name: what an operator selects a run by. It is not a record namespace — records
are selected by the computation an invocation declares, so equal declarations
in different studies are one record — and it does not make one study's results
private to it.

`Study` is the named execution envelope for a Plan and its implementations.
It implements the executable part of the manifesto's wider study: an inquiry
bringing together intent, context, actions, evidence, and decisions. Several
executions may contribute to that inquiry as its question changes. Executing a
Plan supplies outcomes and evidence; it does not by itself establish an
accepted conclusion. Ownership of the wider inquiry remains open until use
gives a reason to place it.

It exists because that agreement kept being written by hand. A Plan declared
what work meant; a separate binding supplied implementations, command lines,
output paths, transports and roots; a third caller walked the result. Every
seam was a place where a study could run as something other than what was
authored, and the OTA reference needed six hundred lines of binding whose only
purpose was to restate the first file correctly.

## Mode of being

**Development state:** `prototype`

The unit's commitment is to join authoring and execution through the children's
public contracts while preserving inspection before submission. Its working
hypothesis is that this join removes repeated agreement between separate files
without creating a second account of what those children own. The examples
below provide bounded evidence for that benefit. They do not settle whether
every current convenience belongs in this facade: a convenience that repeatedly
restates a child's rules would challenge the present boundary, even if it works.

Operator experience exposes a further limit of the join: retained evidence can
be difficult to find after execution. An operator reported struggling to
discover earlier attempts for inspection and debugging. The name-based
resolution that used to stand in for this has been removed rather than kept as
a partial answer: it resolved a shared record to whichever study created it,
which was wrong once records became shared. A run now hands back the exact
record and try it used, and that reference provides direct access to
an execution. The facade does not persist a study-run history or provide a
study-to-execution discovery surface; retained records alone do not supply it.
Discovery from a study or attempt root through to tries, diagnostics, and
results is a concrete need for this operator-facing join. Its eventual surface
remains to be developed; this observation does not settle wider inquiry ownership.

The unit studies whether one authoring file can connect a Plan to its execution
while retaining inspection before submission. Its evidence is
`examples/grid_refinement.py`: three grid resolutions integrating `exp(-x)` over
[0, 1] with real `awk`, whose value is analytic, so the measured 0.632943
against an exact 0.6321206 is checkable — the coarse grid's 0.130% gap is the
trapezoid rule's own discretisation error, not a fabricated number. The
observed error ratios are 15.996 and 16.000 across refinements by four, which is
that rule's second order measured rather than asserted.

Its reuse behaviour is measured on the same example. A second run reuses all ten
invocations. Editing the `medium` point's declared `steps` reran exactly
`medium:write_grid`, `medium:integrate`, `medium:estimate` and the shared
`compare`, reusing `coarse` and `fine` untouched. Editing the *body* of
`estimate` reran all three of its invocations and the downstream `compare`,
reusing the six `write_grid` and `integrate` invocations — so a body edit
invalidates that operation's invocations and their downstream cone, not the
whole plan.

Nothing in this package names a simulator, a circuit or a domain; its own
example runs `awk`. The domain studies that exercise it hardest live in
`../studies/`, one level up, where naming a simulator is honest. `ota_pvt.py`
there is the reference ported from the two files it used to be: sixteen
invocations over three PVT points, four declared external sources, real AC
sweeps, and gain/GBW/phase-margin computed from the raw file rather than
transcribed. What the port demonstrates is the seam closing: `run_study.py`
re-declared all six operations, supplied their output paths, and wrote
`del base, edits  # unresolved source reference` three times because a declared
source could not reach a body. None of that survives. Editing `spec_limits.json`
reran `evaluate-pvt` alone — fifteen invocations reused — and flipped the
verdict to failing; restoring the file reused the original attempt rather than
recomputing it.

What it has met of a farm, and what it has not, is worth splitting rather than
totalling. `examples/farm_smoke.py` has run against a real LSF installation and
proved the `shell` launcher reaching a real `bsub -I` job: argv, `-J` identity,
an artifact chaining from one job into the next, failure recording and reuse.
It ran **sequentially**, so it says nothing about the graph kernel, about
`max_jobs` bounding anything against a real queue, or about the watcher's
`bjobs` parsing, none of which have met a farm. Every domain study in
`../studies/` — `rc_corners.py`, all three `ota_pvt` variants — still runs
entirely at `local` placement; `ota_pvt.py` carries the one-line policy change
that would place each point on its own job as a comment rather than a claim.

## Current contracts

- Distribution: `hedloom`, Python 3.11 or newer, depending on `hedloom-flow`,
  `hedloom-exec` and `hedloom-run`. `distributed` remains optional and is reached only
  when a run is given a client.
- `@operation` is `hedloom_flow`'s decorator, wrapped so the body it already kept is
  registered as callable under the operation identity the Plan records. The
  registry resolves a name the document names and refuses a different body
  claiming the same operation name (the executable bundle binds by name); it
  introduces no second notion of what an operation is.
- `@study(name=...)` gives every instance built by one decorated function the
  same durable study name. Without `name=`, the definition's
  `module.qualname` is inferred, following operation and flow identities. Two
  definitions in one process cannot claim one name. A finished Plan requires
  an explicit name because it has no defining function from which to infer
  one. Exported Plan output names do not participate in study identity, and
  the study name does not participate in execution-record identity: it names
  the requester, and the declared computation names the record.
- An operation body **runs**. It receives the inputs the Plan resolved, its
  declared config, and — if it names the reserved parameter `out` — a
  `Workspace` addressing that attempt's own directory. Attribute access on the
  workspace resolves declared file and directory outputs only; the workspace
  itself is the attempt directory as an `os.PathLike`. A body that computes a
  value returns it; a body that writes a filesystem artifact writes to
  `out.<name>`.
- `file(...)` and `directory(...)` state filesystem output shape independently
  of the artifact-contract `kind=` used to connect operations. Successful
  capture requires that shape; directory manifests record recursive payload
  size rather than the filesystem's directory-entry size.
- Returning a `Shell` makes the body a launcher: the command is executed at the
  placement the invocation resolved to. Locally that is a subprocess bound to
  this process's lifetime, and on `lsf` it is the delegate's `bsub -I` job with
  that invocation's queue, cores and licences.
- `A body decides what runs; it never decides whether it runs.` Reuse,
  identity, ordering and placement are all settled before a body is called, so
  writing Python cannot acquire scheduling authority.
- `sweep(points, key=...)` opens a keyed scope, so calls inside take
  `<point>:<operation>` unless they name a key. This is what keeps reuse from
  depending on an author keying every call by hand, where a mistake is silent
  staleness rather than an error.
- `study(plan, name=...).summary()` shows the study name and every invocation, its operation and its
  placement, and spends nothing. `submit(site=...)` then runs it, opening the
  compute the site declares for as long as the run needs it and giving it back
  afterwards. There is no kernel to choose: concurrency is each placement's own
  `max_jobs`, and a site that declares none has capacity one. `sequential=True`
  asks for one invocation at a time and builds no cluster, which is what keeps
  `distributed` optional; `locally=True` additionally serves every placement by
  its authored body in this process, for debugging a farm study on the submit
  host. A caller who already holds a `distributed.Client` may pass it instead.
- `session(site, override=None, ...)` is the form for more than one run: it owns
  one cluster, one client and one queue watcher for a `with` block, so several
  runs share a budget rather than each opening their own. Its lifetime is
  deliberately visible, because leaving the block ends the runs inside it and,
  under owner-bound lifetime, their farm jobs with them. `Session.submit_all`
  runs several studies against that one cluster, which is what makes the shared
  budget structural rather than a convention.
- An override — `session(site, {"placement": {"lsf": {"max_jobs": 1}}})` —
  changes how a run executes and never what it means. Nothing it may reach is
  identity-bearing, so an overridden run lands on the same attempt identities as
  a plain one and the two reuse each other's work. Roots are refused, because
  moving the record changes what is reused, which is a different installation
  rather than a different way of running this one.
- `StudyRun` retains `study_name` and is addressable the way the study was
  authored: `run["coarse:integrate"]` is that invocation's outcome.
- `run.outputs` is what the study's Plan exported, under the names its author
  gave those outputs. Authored names decide it; report order and completion
  order do not, so appending an invocation cannot change what a study produced.
  A Plan exporting nothing has an empty mapping, and one exporting several
  keeps them several: there is no unwrapping to a single value and no preferred
  entry. A name the study did not export raises `KeyError`.
- Each entry is a `StudyOutput`, which retains the Plan's reference and the
  producing `InvocationOutcome` rather than resolving them away, so provenance
  and reuse are inspectable without guessing an authored key. `.value` resolves
  that exported **port** — one invocation declaring both a file and a returned
  output exports two different things — through `hedloom_run.binding`, shared
  with both kernels so an exported output and a downstream input cannot
  disagree. A file or directory output resolves to its recorded address; the
  bytes are the caller's to read.
- An output nobody produced is refused rather than answered. `.value` and
  `.artifact` raise `OutputUnavailable` for a failed, blocked or unreported
  producer, naming it and its recorded error; `.available` asks the same
  question without raising. `None` returned by a succeeded body is a result and
  stays distinguishable from an absent one. Exporting a value does not make it
  durably serializable: what an attempt record can hold is unchanged by being
  exported.
- Execution, verdict, and accepted conclusion are three questions.
  `run.succeeded` reports the first, over invocation outcomes only: an
  evaluation returning `{"passes": False}` succeeded. The second is the value
  that evaluation exported, which this unit neither interprets nor prefers by
  name. The third depends on criteria, assumptions and interpretation, and is
  inferred here from nothing — not from execution, not from reuse, not from a
  pin.
- The aggregate `StudyRun.value` is **removed**. It answered with the last
  invocation in report order, which is a study's conclusion only when the
  conclusion happens to be authored last, and stopped being it silently as soon
  as anything was appended. The removal is breaking, and deliberately has no
  alias: a convenience that keeps its name would keep its meaning.
- Identity-bearing inputs choose a record, and each execution gets a distinct
  try workspace beneath it. A declared output's address is that try's path, and
  `InvocationOutcome.record` and `.try_number` name the execution an invocation
  landed on, whether it ran or reused. There is no per-study view of outputs:
  a record is shared by everyone who declares its computation, so a name-shaped
  view would have had to choose one requester's spelling for work that belongs
  to none of them.
- Attempt-record layout 1 is the only readable recorded layout, and it has not
  changed. Identity *renderings* have changed as the identity contract changed,
  so records written under an earlier one are not selected by today's digest and
  are not reused; their contents remain readable. There is no migration path in
  this prototype and none is needed.
- `hedloom pin`, `hedloom unpin`, and `hedloom pins` protect and inspect
  terminal try workspaces by record identity or unique prefix, optionally with
  `#<try>`.
  Pinning is an operator action with a reason and actor; it is never authored
  into a study and never implied by accepting a result for reuse. Neither
  pinning nor reuse acceptance constitutes acceptance of an engineering
  conclusion.
- A study may begin from a file it did not write. An operation declaring an
  `input_artifact` source as an input is handed its located path, and the same
  reading that locates it fingerprints it, so delivery and staleness cannot
  disagree about which file was meant. The path is resolved where the study is
  submitted, which assumes a shared filesystem for any placement that is not
  local.
- Nothing about the site is authored into the study. Placements, roots, address
  spaces, thread counts, and retention come from a `Site`, which a profile file
  can supply. Named `retention.automatic.after_run` rules run only after a
  completed run and warn rather than changing its result. No `submit(prune=...)`
  surface exists: a study decides what is produced, never what is kept.

## Contribution to the parent

This is where the repository's author-plan-execute-evaluate path becomes one
operator gesture instead of three. It also answers a question the register left
open — whether any unit should be the operator-facing "flow" — by being it,
without absorbing what the others own.

## Exclusions

Hedloom owns no attempt record, no identity, no reuse policy, no transport, no
readiness, and no Plan validation. It composes; each of those remains where it
was, and `hedloom-exec` in particular imports neither this package nor Dask, which
is what keeps the kernel and the façade independently replaceable.

It does not branch on results. A flow body runs at planning time and produces a
fixed graph, so a Plan still predicts what will run. Result-dependent control is
recorded as future work in `docs/vision/open-concepts.md`, and `submit` is named
there as the façade where it would most plausibly arrive disguised as
convenience — a `retry=`, `max_iterations=` or `until=` argument is the
tripwire, not a feature.

The implementation fingerprint is coarser than "the behaviour changed": it
ignores blank lines and trailing whitespace, so an added comment reruns the work
that operation produced. Deliberate, since a needless rerun costs time and a
missed one costs correctness, but it is a limitation rather than a property.

## Child composition

The immediate children are `hedloom-flow`, `hedloom-exec`, and `hedloom-run`,
authored as `flow`, `exec`, and `run` in `unit.toml`. Their composition is the
operator-facing join this ontology owns; containment grants Hedloom no
authority over the narrower contracts each child retains.
