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

Public invocation outcomes use Run's `reused` disposition consistently in live
reporting, summaries, and consumer history. Exec selection evidence keeps its
`completed` protocol term; translating it at the consumer boundary preserves
selection facts without requiring authors to translate executor terminology.

The async replacement adopted on 2026-10-01 makes submission an owned occurrence,
not a blocking call. `Runtime.submit` immediately returns a `Run` receipt;
durable acceptance and terminal completion are independently observable. One
Runtime owns a background asyncio controller and Dask clients/clusters for the
site. Waiting Runs retain data, without a fixed 32-Run admission cap or a thread
per Run. Placement capacity bounds outstanding ready executions. Bounded daemon
lanes keep synchronous preparation, storage and observation off the controller.
This bounds entered work, not the total memory of arbitrarily many waiting Plans.

Run history joins named consumers to shared execution evidence. Every accepted
submission requires a chosen name and `Site.runs_dir`, independently located from
`records_dir` and `work_dir`. `runs_dir/<submission>.<occurrence>/` holds the
saved Plan and outcomes; `runs_dir/_meta/` holds allocation and dispatch
bookkeeping. Consumer history schema 4 records lifecycle and storage roots;
schema 3 and earlier are refused without deleting saved data. Acceptance publishes
the saved Plan/header before expensive preparation. Prepared evidence and each
consumer's execution link must be durable before dispatch authority is granted.
A failed initial publication rejects acceptance; failed preparation settles an
accepted Run as failed without fabricated execution references. Later observation
failure degrades persistence independently of actual computation. Readers distinguish
pending or unavailable preparation evidence from corruption of a prepared record.

Run's controller shares compatible ready invocations within one Runtime, including
execution bindings and placement, and retains exact Exec record/try references.
Withdrawing one consumer preserves other consumers; solely owned entered work
must drain. Completed dispatches are not a result cache: later requests re-enter
Exec's reuse selection. Output delivery is also distinct from execution: a failed
projection preserves the actual execution evidence, blocks dependent work, and
cannot settle unrelated entered work prematurely. This experience places lifetime
and delivery authority at the composition boundary rather than in Dask key equality.

Context exit drains owned Runs, or requests withdrawal before draining on an
exception. Manual close is optional; process ownership provides automatic exit
cleanup outside a context. Local Linux subprocess owner-death tests and fake-farm
normal-exit pool probes pass. Extended probes on 2026-10-02 also reclaimed the
active command and fake batch worker automatically after SIGTERM and SIGKILL,
in about 32.5 seconds. Dask detects scheduler loss immediately but its default
worker shutdown gives active executor threads a 30-second grace. The earlier
five-second observation measured this delay, not permanent orphaning. Direct-child
binding still does not establish an immediate cross-host guarantee, and the async
path has not been exercised on a real farm.

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
That historical run used the retired **sequential** kernel, so it says nothing
about the async controller, about `max_jobs` bounding anything against a real
queue, or about the watcher's
`bjobs` parsing, none of which have met a farm. Every domain study in
`../studies/` — `rc_corners.py`, all three `ota_pvt` variants — still runs
entirely at `local` placement; `ota_pvt.py` carries the one-line policy change
that would place each point on its own job as a comment rather than a claim.

## Current contracts

Submission history captures compact submit-host reproducibility evidence by
default before execution. Python and installed package versions, selected project
and editable manifests, and Git commits/patches are captured once per Runtime
project root (lazily through shared capture), or explicitly through
`capture_environment`. Dependencies are assumed unchanged until
`refresh_environment` or a new Runtime;
there is no cache-hit filesystem validation. An immutable snapshot retains its
capture time while study Git revisions, supplied configuration and invocation
context are captured anew for each run. Clean tracked source bytes matching a
committed blob are represented by repository/commit/path/blob references; dirty,
untracked or unverifiable sources retain bytes. Explicit attachments always
retain bytes. Source Git queries are separate from dependency caching, and known
source files do not constitute a complete import or data inventory. The public
API, module, CLI field and new record filename consistently use reproducibility;
schema-4 history uses that spelling without the previous filename fallback.
Automatic lockfile capture and verbose platform,
interpreter-path and Git-status metadata are excluded. Dirty dependency patches
remain supported, with gaps explicit. History still stores each compact snapshot
per run; discovery caching does not imply storage deduplication.

This evidence belongs to the named consumer, independently of computation
identity and reuse. It neither archives whole repositories nor claims the consumer
environment produced a reused try. Saved bytes support reconstruction after local
edits; Git references still depend on retaining the repository. Remote tool
environments require supplied evidence. See `docs/guide/discovery.md` and
`tests/test_reproducibility.py`.

Runtime-identified outputs test a narrower claim about composition: acquisition can
be an ordinary operation without a second source lifecycle. The local Git
A → A → B → A test exercises the async path: separate observations can reuse the
same analysis while preserving each consumer's candidate provenance. Plan schema
4 declares execution mode and output identity; history schema 4 binds candidate
inputs and dispatch together. Named returns replace whole-return projection.
Borrowed locations remain externally owned, including after checkout mutation.
The author guarantees identity and lifetime; present accessibility is a separate
query and does not verify revision. General successful-result eviction remains
outside the current collector. See docs/guide/runtime-artifacts.md.

- Distribution: `hedloom`, Python 3.11 or newer, depending on `hedloom-flow`,
  `hedloom-exec` and `hedloom-run`. Authoring remains independent of Dask;
  execution needs the optional `hedloom-run[dask]` dependency.
- `@operation` is `hedloom_flow`'s decorator, wrapped so the body it already kept is
  registered as callable under the operation identity the Plan records. The
  registry resolves a name the document names and refuses a different body
  claiming the same operation name (the executable bundle binds by name); it
  introduces no second notion of what an operation is.
- `@study(name=...)` gives every instance built by one decorated function the
  same authored study-definition name. Without `name=`, the definition's
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
  value returns a mapping under its declared output name; a body that writes a filesystem artifact writes to
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
  `<point>:<operation>` unless they name a key. These are stable readable Plan
  addresses; computation reuse depends on declarations and resolved inputs,
  independently of authored keys.
- `study(plan, name=...).summary()` shows the study name, invocations and
  placement without spending compute. `runtime(site).submit(subject, name=...)`
  registers work against one shared site budget. `Run.accepted()` observes durable
  acceptance; `Run.wait()` returns any terminal `RunResult`; `Run.result()` requires
  success. `await run` observes completion without making the caller own the loop.
  Timeout or cancellation of an observer does not withdraw the Run.
- `Run.stop()` requests withdrawal; it does not preempt entered Python or shell
  work. Live `snapshot()` and `result_if_done()` are in-memory observations.
  `Runtime.close()` closes admission and drains, and a context handles it
  automatically. Legacy Session, blocking submit helpers, sequential mode and
  worker-held nested submissions are removed. `locally=True` serves authored
  bodies locally through the same async scheduler, rather than a second kernel.
  `Run.stop(force=True)` can escalate an ordinary withdrawal and interrupt
  solely owned pooled commands through Dask's nanny-managed worker restart,
  retaining the allocation. Shared consumers remain protected. The existing
  submit-host waiter observes durable interrupt intent, cancels scheduler
  interest before worker loss, and lets Exec publish the confirmed cancelled
  outcome with its exact reference. Queued work needs no destructive restart;
  scheduler assignment alone is not evidence that a command entered. Pool
  commands select Linux SIGKILL immediate-child binding so they cannot ignore
  owner death. Other placements and Python bodies still drain; detached
  descendants are outside this binding. Unconfirmed interruption is reported
  honestly. Force requests do not make incomplete work eligible for automatic
  reuse; a confirmed cancelled attempt can be followed by a new try.
- Submission `priority` orders eligible preparation, controller nominations and
  Dask/pool work. Higher values go first; equal-priority Runs rotate dispatch.
  Priority is outside computation identity and offers neither preemption nor LSF
  queue priority. Continuous high-priority work can delay lower-priority work;
  aging and live reprioritization are not implemented.
- An override — `runtime(site, {"placement": {"lsf": {"max_jobs": 1}}})` —
  changes execution mechanism without changing computation identity. Roots are
  refused, because changing the store is a different installation. Pooled commands
  occupy one worker each: `workers` bounds farm allocations and `max_jobs` bounds
  outstanding gateway work. Unsupported per-command requests are refused before
  submission rather than silently reduced to fit a pool.
- `RunResult` retains `study_name` and is addressable as authored:
  `result["coarse:integrate"]` returns that invocation's outcome.
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
  with the controller so an exported output and a downstream input cannot
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
  `result.succeeded` requires successful Run settlement and invocation outcomes: an
  evaluation returning `{"passes": False}` succeeded. The second is the value
  that evaluation exported, which this unit neither interprets nor prefers by
  name. The third depends on criteria, assumptions and interpretation, and is
  inferred here from nothing — not from execution, not from reuse, not from a
  pin.
- The aggregate result `.value` remains **removed**. It answered with the last
  invocation in report order, which is a study's conclusion only when the
  conclusion happens to be authored last, and stopped being it silently as soon
  as anything was appended. The removal is breaking, and deliberately has no
  alias: a convenience that keeps its name would keep its meaning.
- Identity-bearing inputs choose a record in `records_dir/<identity>/`, and
  each execution gets a distinct `work_dir/<identity>-<try>/` when a work
  directory is configured. A declared output's address is that try's path, and
  `InvocationOutcome.record` and `.try_number` name the execution an invocation
  landed on, whether it ran or reused. There is no per-study view of outputs:
  a record is shared by everyone who declares its computation, so a name-shaped
  view would have had to choose one requester's spelling for work that belongs
  to none of them.
- Attempt-record layout 1 is the only readable recorded layout, and it has not
  changed in this storage naming refactor. This concerns computation records;
  old saved-run history has a different, now unreadable schema and tree.
  Identity *renderings* have changed as the identity contract changed,
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
- Execution isolation was explicitly adopted on 2026-10-03: ordinary OS users
  other than the owner and unauthenticated network clients must not submit work
  or trigger arbitrary code through Hedloom-owned schedulers, workers or pools
  by default. The boundary assumes OS account isolation and excludes the same
  account and privileged administrators. Missing protection must fail startup;
  enabling pools or diagnostics must not silently weaken it. Every enabled or
  introduced protection has a documented public opt-out, scoped and visible in
  effective configuration. The maintained requirement and evidence limits are
  in [execution security](docs/internals/execution-security.md).
  Runtime readiness communication remains process-local. Networked pools default
  to per-pool mutual TLS with private temporary credentials in `records_dir`;
  named `authentication="none"` opts out for that pool. Site and Runtime overrides
  retain the effective mode. Diagnostic HTTP exposure is independently selected
  by `dashboard`, is unauthenticated and is not private merely because it binds
  loopback. Authentication is execution configuration, outside computation
  identity; it does not make an authorized operation a sandboxed program.

## Execution learning and possibilities

The [replacement concept](design/interactive-execution-redesign-2026-09-29.md),
[internals census](design/interactive-execution-internals-census-2026-09-29.md),
[proposal](design/interactive-execution-proposal-and-plan-2026-09-29.md) and
[usability review](design/interactive-execution-usability-review-2026-10-01.md)
retain the dated inquiry. The user's later authorization adopted the async
replacement while reconsidering its resource proposals. Explicit resource
management now means observable ownership and bounded entered work; it does not
mean mandatory manual close or an arbitrary receipt quota. A responsive prompt
alone would be insufficient: priority and separate preparation/storage lanes also
make progress inspectable when execution capacity is occupied.

Selected function code and referenced same-module helpers are pinned at
registration, then serialization and reproducibility capture happen off-loop.
Mutable external objects, imported module state and closure contents remain
author-owned dependencies. Source files read during preparation may have changed
since registration; code hashes identify selected code but are not a hermetic
source archive. These limits matter for interactive reload rather than being
claims of complete environment capture.

Reviewing network listeners exposed a distinction between locality and authority:
loopback is shared by host users, while an unauthenticated farm scheduler can
accept work outside the authored-Plan path. Execution protection therefore
belongs to the composed Runtime's resource ownership, rather than to Plan
validation or a dashboard setting. The adopted per-pool boundary reuses Dask's
temporary Security support and keeps opt-outs inspectable without introducing
certificate administration. HTTP routes require separate evidence; local checks
cannot establish real-farm filesystem or network isolation.

TLS integration exposed a lower transport seam: abrupt Dask connection closes
could contaminate later RPC error interpretation on the tested Python/OpenSSL
stack. Owned TLS contexts accept the missing close notification while retaining
mutual certificate checks; Dask message framing still rejects incomplete data.
Local fake-farm pool security and async-executor tests now exercise authenticated
command execution, unauthorized endpoint refusal and credential cleanup. This
supports the chosen integration without establishing real-farm isolation.

Formal issues discovery distinguished scheduler absence from physical command
termination: a removed worker could leave its command alive while the earlier
force path published cancellation. The owned pool scheduler now retains loss
evidence with each task; acknowledged restarts certify only their own loss,
and uncertainty stays an indeterminate failure. Exact pause ownership also
prevents delayed cleanup from changing another interruption's fence. The
[bounded investigation](design/formal/async-interruption/README.md) records
source correspondence, counterexamples and the remaining assumptions.

The retained Dask path passes local and fake-farm checks with the ASS environment
(Dask/distributed 2026.7.1). Run tests and facade lifecycle checks also pass on
cached 2023.9.2 and 2024.8.0 versions without a dependency floor increase.
Jobqueue 0.9.0's cold import installs SIGINT handling, so the first pooled Runtime
bootstraps it on the main thread and restores the caller's handler before its
controller starts. A cold background-thread construction refuses startup; this
is a dependency boundary, not a reason to import Jobqueue for local studies.

Worker-held nesting is deferred, not excluded in principle. Maintained dynamic
consumers now stage discovery, ordinary corner work and reporting at the caller,
with recorded operations and exported artifacts. The
[historical nesting reference](design/nested-studies-historical-reference-2026-10-02.md)
preserves recoverable working revisions and freshly checked local evidence;
retiring the mechanism does not erase its demonstrated capability. Future hierarchical composition
must return coordination to an owner that does not consume the child's execution
slot. Real-farm async scheduling, immediate abrupt pooled-owner cleanup and source
capture beyond the declared dependencies remain unverified possibilities; the
unit remains a prototype.

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

The standalone documentation site composes each unit's declared docs and
resources through `tools/stage_docs.py`, preserving repository-relative links.
Read the Docs can build this checkout without the parent workspace. The
unit manifests remain the source of which children and documentation belong;
generated run evidence and design records are not staged for publication.

The immediate children are `hedloom-flow`, `hedloom-exec`, and `hedloom-run`,
authored as `flow`, `exec`, and `run` in `unit.toml`. Their composition is the
operator-facing join this ontology owns; containment grants Hedloom no
authority over the narrower contracts each child retains.

External file outputs may request content identity without transferring ownership.
Flow validates the file declaration, the facade accepts `located(path)` without
an author identity, and Exec hashes the borrowed file at capture. Consumers reuse
by captured bytes; the author still guarantees payload stability and accessibility
while dependents use it. This separates identity calculation from file ownership.

Interactive source reloads may register revised operation definitions from the
same file and qualified function name. Automatic study binding selects bodies
by the complete Plan operation definition, retaining old versions for previously
captured Plans. Different source origins still cannot claim the same operation
name. This supports editing within a long-lived process without treating a name
as sufficient evidence that a replacement body implements an older Plan. Python
globals and imported state are not snapshotted by this binding.

Run discovery accepts either a Site profile or a direct `runs_dir`. Saved run
metadata supplies execution locations, so inspecting a Python-authored Site's
results does not require reconstructing its placement configuration as TOML.
