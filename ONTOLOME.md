# Hedloom Ontology

## Purpose and scope

Hedloom is the operator-facing composition of the three units beneath it. It owns
one thing none of them could own alone: the join between an authored study and
its execution. An author writes operations, a flow, and a plan; `submit` runs
exactly those, because it holds both halves rather than requiring two files to
agree about them.

It exists because that agreement kept being written by hand. A Plan declared
what work meant; a separate binding supplied implementations, command lines,
output paths, transports and roots; a third caller walked the result. Every
seam was a place where a study could run as something other than what was
authored, and the OTA reference needed six hundred lines of binding whose only
purpose was to restate the first file correctly.

## Mode of being

**Development state:** `prototype`

The unit studies whether one file can be a whole study without the Plan ceasing
to be inspectable before it spends anything. Its evidence is
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
  registry resolves a name the document names; it introduces no second notion
  of what an operation is.
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
- `study(plan).summary()` shows every invocation, its operation and its
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
- `StudyRun` is addressable the way the study was authored: `run["coarse:integrate"]`
  is that invocation's outcome, and `run.value` is the plan's conclusion.
- A recorded file output has a stable live view under
  `<Site.root>/latest/<plan>/<authored-key>/<output>`. The attempt identity and
  workspace still move whenever an identity-bearing input moves; only this
  operator-facing view stays put. Every run repoints it before launch, including
  a run that returns to an older reusable identity.
- `hedloom where`, `hedloom check`, and `hedloom log` resolve the current output,
  reject a cached path that is behind, and list creation-order iterations with
  their changed identity keys. They accept either a site profile or an explicit
  attempt root. Live run output names reruns by changed key and labels completed
  reuse as `reused` rather than inventing a rerun reason.
- A study may begin from a file it did not write. An operation declaring an
  `input_artifact` source as an input is handed its located path, and the same
  reading that locates it fingerprints it, so delivery and staleness cannot
  disagree about which file was meant. The path is resolved where the study is
  submitted, which assumes a shared filesystem for any placement that is not
  local.
- Nothing about the site is authored into the study. Placements, roots, address
  spaces and thread counts come from a `Site`, which a profile file can supply.

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
