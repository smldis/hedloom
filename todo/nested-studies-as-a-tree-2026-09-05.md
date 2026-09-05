# Nested studies are a tree: vocabulary, prior art, and the constructs (2026-09-05)

**Status: a map, not a proposal.** Nothing here is designed and nothing is
recommended for implementation. It exists because the two notes before it have
a measurement and a question but no *vocabulary*, and the next person to argue
about nesting should not have to invent one.

Read in order:

1. `nested-submission-and-capacity-2026-08-30.md` — why the fetch cannot be an
   ordinary operation, and what `secede()`, `worker_client()`, donation and
   configuration each cost. Measured. Do not revisit.
2. `rearchitecting-nested-studies-2026-09-04.md` — the open question: is nesting
   the right primitive or a workaround made safe? Also the empirical point that
   the study this was built for chose pull-before-submit instead.
3. `todo/refreshed-sources-2026-09-05.md` — designs the *other* answer: a
   source that re-reads itself by declaration, which removes the need to nest
   at all for the degenerate case. If that note is adopted, this one narrows to
   the genuinely adaptive studies described below.
4. This file — if the answer to (2) is ever "yes, we want it", this is the map.

**Everything below is conditional on (2).** The demand question is answered
there, not here. This note deliberately assumes the feature is wanted in order
to think clearly about its shape; that assumption is not evidence.

---

## Part 1 — The shape is a tree, not a sequence

The 08-30 note describes the working shape as "two stages", which is accurate
for `examples/live_source.py` and misleading in general. Staging is a sequence
only when the inner plan is fixed. The reason to nest at all — the reason a
`submit` inside a body buys anything a source could not — is that **the inner
plan is composed programmatically from the outer results**. Its extent is not
known until the parent runs, and a node may author several children, each of
which may author more.

What that makes is a derivation tree of plan documents. An edge parent → child
means: this child document was *authored by* a body running in the parent.

`examples/live_source.py` is a **degenerate tree** — one fixed edge, statically
known, whose child does not depend on the parent's result except through a file
on disk. That is why it reads as over-machined: it pays the whole cost of
nesting for something nesting is not for. This reinforces 09-04's warning
rather than contradicting it — the example is evidence of a mechanism, and it
is not even evidence of *this* mechanism's motivating case.

If a case is ever wanted that would justify the machinery, it looks like
adaptive refinement: refine only the cells above tolerance, recursively.
`examples/grid_refinement.py` already carries an analytic answer to check
against, so the adaptive variant would be checkable rather than merely
demonstrable. **Writing that example is the cheapest possible test of whether
the demand in 09-04 is real** — if it cannot be written compellingly, there is
nothing here to build.

### The invariant, restated

Today, from `src/hedloom/study.py`:

> Nothing is spent until `submit`, and what runs is what the Plan showed.

Under a tree it would become:

> **Every plan is determined before it spends; the tree is not.**

That is a genuine weakening and it must be written as one rather than smuggled
in. But note what survives: the property was always *per plan*. A tree of
fully-determined plans keeps every identity, reuse and inspectability guarantee
at the node level. What is given up is only the ability to print the whole
future before spending — which was never available for adaptive work anyway.
The current position does not offer that guarantee for adaptive studies; it
declines to express them at all.

---

## Part 2 — The rule this collides with, and why staging is on the right side of it

`AGENTS.md`:

> **Do not add result-dependent control.** `submit` is the surface where it
> would most plausibly arrive disguised as convenience — a `retry=`,
> `max_iterations=` or `until=` argument is the tripwire, not a feature.

A tree looks like exactly the prohibited thing, and getting this distinction
precise is a precondition for any further work here. The distinction is:

| | Who decides | When | Verdict |
| --- | --- | --- | --- |
| **Result-dependent control** | the *kernel*, mid-run, by reading a value | a plan is running | Prohibited. It gives a body scheduling authority. |
| **Result-dependent authoring** | an *author*, between plans, by reading a value | a plan has finished | Already sanctioned. |

The second is what staging already does, and the 08-30 note endorses it in
those words: *"Each plan is still fully determined before it spends anything,
which is the invariant staging keeps and result-dependent control would
break."*

So the tree is the *generalisation of something already permitted*, not a
breach of the rule. But the rule's protection is currently supplied by
friction — nesting is awkward enough that nobody reaches for it casually. Make
it ergonomic and the friction is gone, so the boundary would have to be held
explicitly instead:

* A body still never decides *whether* it runs, or whether anything already
  planned runs. It decides only what to author next.
* The authored child is a complete Plan, inspectable before it spends, subject
  to every ordinary refusal.
* No `until=`, `retry=` or `max_iterations=` appears on `submit`. A loop is
  expressed as a study that authors its own successor, which is a different
  thing: each iteration is a plan someone could have written by hand.

**If that boundary cannot be stated crisply enough to refuse violations of it,
that is a reason not to build any of this.**

---

## Part 3 — The theory: where hedloom sits

*Build Systems à la Carte* (Mokhov, Mitchell, Peyton Jones) gives the two axes
that classify every system in Part 4, and names hedloom's fork exactly.

### Axis 1 — applicative vs monadic dependencies

* **Applicative**: the dependency graph is knowable without running anything.
  Make, Bazel, **hedloom today**.
* **Monadic**: dependencies are discovered *during* execution. Shake (`need`
  inside a rule), Nix import-from-derivation, Excel `INDIRECT`.

Nested studies are a request to become monadic. The paper's result worth
knowing: monadic systems buy expressiveness and pay in scheduling and in early
cutoff, because you cannot plan what you cannot see.

hedloom's position is unusual and worth stating: it would be **monadic between
plans, applicative within one**. Each node is a fully applicative graph; only
the edges between nodes are monadic. That is a narrower weakening than Shake's,
and it is the precise technical content of "every plan is determined before it
spends; the tree is not."

### Axis 2 — scheduler: topological, restarting, or suspending

* **Topological** — order everything up front. Only possible when applicative.
  This is `run_plan_graph` today.
* **Restarting** — run; on discovering a new dependency, abort and retry with
  more knowledge. Excel, Snakemake checkpoints.
* **Suspending** — run; pause; recursively build the dependency; resume.
  Shake, Ray, Luigi.

**hedloom's current nested submit is a suspending scheduler implemented by
blocking a worker thread.** That single sentence explains PR-era
`NestedCapacityExhausted` completely: a suspending scheduler must park the
suspended computation somewhere, and hedloom parks it in a resource unit that
means *possession of a farm job*. Everything in the 08-30 note — secede,
donation, the configuration answer — is an attempt to make that parking spot
cheaper.

The three re-architecture directions in Part 5 are, in this vocabulary:

| Direction | Scheduler | Where the suspended computation is parked |
| --- | --- | --- |
| Today | suspending | a worker thread holding a placement unit |
| Author on the submit host (C1) | restarting | nowhere — the parent completed |
| Checkpointed coroutine (Part 4, family 6) | suspending | a durable record |

---

## Part 4 — Prior art, by construct

Grouped by mechanism rather than by library, because the mechanism is what
transfers. **Details are from memory and should be verified against current
documentation before any of them is cited as evidence** — this is a reading
list with annotations, not a measurement.

### Family 1 — Suspend the parent, and argue about what it holds

| System | Construct | What it costs |
| --- | --- | --- |
| Luigi | `yield` sub-tasks from `run()`; worker `.send()`s results back in | the task process stays occupied throughout — hedloom's wart exactly |
| Dask | `worker_client()` / `get_client()` + `secede()` | measured in 08-30: secede vacates a thread, never a resource |
| Ray | blocked `ray.get` gives up the parent's CPU; extra workers started | this *is* donation; buys donation's oversubscription |
| Airflow | `SubDagOperator` | **deprecated, then removed** |

Two of these are decisive evidence rather than colour:

**Airflow is the cautionary precedent.** `SubDagOperator` did exactly what
hedloom does — a parent operator consumed a slot in a pool while its child DAG
ran — and it deadlocked in production often enough that Airflow deprecated it
and replaced it with scheduler-materialised expansion (`TaskGroup` for the
static case, dynamic task mapping for the dynamic one). **They reached
hedloom's 45-second hang and concluded the construct was wrong, not the
capacity accounting.**

**Ray shows what donation actually buys.** Ray tolerates deep nesting because a
blocked parent releases its CPU, which is the donation designed and shelved in
08-30. Ray can afford it because a "CPU" there is advisory. hedloom cannot,
because a unit of `lsf` is possession of a real farm job. This is the strongest
available argument that donation was correctly shelved: the system that
implements it can only do so because its resources do not mean what hedloom's
mean.

### Family 2 — Return a continuation; do not wait

The parent's job is to *author*; the runtime runs what was authored. Best fit
to hedloom's constraints, and the family with the closest domain matches.

| System | Construct | Why it is relevant |
| --- | --- | --- |
| **Parsl** | `@join_app` — returns futures from other apps; completes when they do, **without occupying an executor slot** | HPC-native (Slurm/LSF, submit host, block workers). Exists precisely because blocking a worker was unacceptable there. |
| **FireWorks** | `FWAction(detours=…, additions=…)` — a task returns an action object that inserts a sub-workflow | Literally "an invocation returns a plan". Materials-science HPC — adjacent operational world. |
| Ray Workflows | `workflow.continuation()` — a step returns a DAG, substituted in place, durably checkpointed | Ships C5 below. |
| Temporal | `ContinueAsNew` — a workflow restarts itself with new input, truncating history | Turns *depth* into a *chain*; keeps every plan small. Worth stealing for iterative refinement specifically. |

### Family 3 — Re-plan from outside, in the controlling process

Restarting schedulers. The planner never leaves the submit host.

| System | Construct | Why it is relevant |
| --- | --- | --- |
| **Snakemake** | `checkpoint` — completing one forces **DAG re-evaluation**; input functions call `checkpoints.<name>.get()`, which raises while the DAG is not final | No nesting, no blocking, no held slot. The planner simply runs again with more knowledge. |
| **Flyte** | `@dynamic` — a task runs to emit a workflow graph, which is then **compiled and type-checked** before its sub-nodes execute, with its own record | Distinct decorator from `@task` on purpose. Flyte also caches on inputs + version. **Closest overall peer: typed, cached, dynamic subgraphs with a durable tree.** |
| Spark | Adaptive Query Execution — re-optimises the physical plan at shuffle boundaries from measured statistics | The principle worth keeping: replan only at **materialisation boundaries**, where data is already durable. |
| JAX | retrace in Python when the shape changes | Same move, different key. |

### Family 4 — Static shape, dynamic extent

The dominant industrial answer, and the one that gives up the least.

Airflow `.expand()` (with `max_map_length`, default ~1024, as an explosion
bound); Dagster `DynamicOut` → `.map()` → `.collect()` — the cleanest API in
the family, and note that the *fold* is explicit and named; Metaflow
`foreach`/`join`; Flyte `map_task`; Step Functions `Map`; Argo `withParam`;
CWL `scatter`.

The trade: handles fan-out beautifully, **cannot express recursion or
heterogeneous children**. Coarse-to-fine refinement to unknown depth is out of
scope for every system in this family. If hedloom's real demand is fan-out over
a runtime-known list, this family is the whole answer and Parts 5–6 are
unnecessary.

### Family 5 — Control flow as a node

TF1 `tf.while_loop` / `tf.cond` / `tf.map_fn`; JAX `lax.scan` / `lax.while_loop`.

Keep the graph fully static and make the dynamic part a *primitive*. The plan
shows a loop, not a thousand unrolled nodes, so **nothing is lost from
"materialise before spending"**. See C7.

### Family 6 — Checkpointed coroutine: waiting costs storage, not a worker

| System | Construct |
| --- | --- |
| **AiiDA** | `WorkChain` — `submit()`s children, registers them with `ToContext`, and **returns to the daemon**. State is persisted; the daemon reloads it when children finish. The whole tree lands in the provenance graph as queryable data. |
| Prefect | subflows; parent/child linkage first-class in the *record*, not only at runtime |

**AiiDA is the closest peer in the entire survey** and should be read before
anything is written into an `ONTOLOME.md`. It is computational-science HPC with
a content-addressed provenance graph — hedloom's problem domain almost exactly —
and its answer is a third option sitting between suspending and restarting:
waiting occupies a durable record, never a worker.

### Family 7 — Refuse the feature

* **Bazel** — analysis and execution are separate phases; the action graph is
  total before execution. Dynamic extent is handled by making the unknown a
  property of an *artifact*: tree artifacts / `declare_directory`, an output
  whose contents are not known until the action runs.
* **Nix** — has the feature (import-from-derivation) and the community treats
  it as a smell, because it destroys the evaluate-then-build phase separation
  and makes evaluation require builds. **That phase separation is hedloom's
  invariant, and the ecosystem that broke it regrets it.** Read their arguments
  before weakening ours.
* **CWL** — no dynamic graph, deliberately.

---

## Part 5 — The constructs, and how each maps onto hedloom

Nine separable concepts. They are not a package; several are independently
useful and at least two are alternatives to the rest.

### C1 — Authoring belongs on the submit host

**The unifying observation.** Every wart in the current shape comes from one
decision: that the child plan is authored and submitted **on a worker**. That
is where the pickled `Session` comes from, and `live_source_state.py`, and the
held unit, and the capacity refusal, and the donation debate.

hedloom already has a place where the world is read and plans are made — the
submit host. `@study` bodies author there. `Site.fingerprints` reads sources
there. `run_plan_graph`'s docstring already states path resolution on the
submit host as a claim about the site.

So the rule would be: **workers run operations; the submit host reads the world
and authors plans.** The driver loop becomes:

```
frontier = [root]
while frontier:
    run what is ready
    for each completed invocation carrying an expander:
        child = expander(its_results)      # submit host, cheap by contract
        graft(child)
```

The parent invocation *completes* when it has authored. It never blocks, never
holds a unit, never needs a live `Session` copied anywhere.
`NestedCapacityExhausted` becomes unreachable rather than survivable, and the
donation machinery left open in 08-30 is deleted rather than built.

The contract that makes it safe: **an expander is cheap Python that reads its
parent's declared outputs.** Anything expensive is an operation whose *result*
the expander reads. If it is slow enough to matter, it was an operation
somebody forgot to write.

Known objections to answer: an expander that hangs stalls the driver loop (bound
it, run it off the loop thread); an expander's failure is not an attempt and
has no record (it may need one); and the expander must be able to reach the
parent's outputs from the submit host, which is an assumption `source_addresses`
already makes but which should be re-stated rather than assumed.

### C2 — A node's identity is its plan document

The property that makes a tree worth building rather than merely tolerable.

A child plan document already contains every input identity it depends on —
that is what `plan_bundles` computes in one pass. So **the document's digest is
the subtree's identity**, with no new mechanism, which gives:

* **Reuse composes upward.** Author the same child document twice and it is the
  same subtree; everything under it reuses.
* **Siblings share.** Two branches converging on the same child are not two
  subtrees. It is a **tree by derivation, a DAG by identity** — hash-consed for
  free.
* **Adaptive studies become resumable.** Rerun a refinement, a bisection, an
  optimiser loop: every node whose document is unchanged reuses, and the run
  resumes at the frontier.

The general form of the freshness/reuse split that 08-30 established:
**freshness lives on the edges, reuse lives in the nodes.** The expanding
invocation is the thing permitted to be non-reusable; everything it authors is
honestly content-addressed.

Nothing in the Part 4 survey does this. Flyte caches per task, AiiDA records the
tree, Ray Workflows checkpoints it — none makes "the same child document is the
same subtree" the reuse key. **This is the one place where hedloom's existing
identity model would buy something no peer system has**, and it is therefore the
strongest argument for the whole direction.

Open: what an expander reading live state does to this. If two runs' expanders
see different worlds they author different documents, which is correct — but
then "resumable" means something weaker than it sounds, and the note that says
so should say it plainly.

### C3 — One frontier, one budget

Today capacity is per-run, which is exactly why nesting breaks it: two
`run_plan_graph` calls, one budget, no shared admitting authority, hence the
waiter accounting and the refusal.

A frontier over the whole tree makes the budget global **by construction**.
There is one admitting authority; the site's declared capacity cannot be
exceeded because nothing else can admit. `submit_all` stops being a special
case — it is a tree with a synthetic root. `placements={"local": 4}` in the
example goes back to meaning *how much may run at once* rather than *headroom
for a waiter*.

This is the strongest *architectural* argument, distinct from C2's strongest
*capability* argument.

### C4 — Declared shape, dynamic extent

The tree cannot be printed before spending. Its *type* can:

```python
@operation(expands_into=reading_study)     # names the family, not the extent
```

```
study live-source
  refresh              local     ↳ expands into live-source-reading (extent unknown)
```

Refuse at graft time if the returned plan is not of the declared family. That
preserves "what runs is what the Plan showed" at the family level — the
strongest form still available, and strictly more than today, where nothing
warns that a whole second study is coming.

This is Airflow `.expand()` and Dagster `DynamicOut` generalised from "N of the
same" to "a plan of this family".

### C5 — Expansion fulfils an output contract; the fold is an ordinary edge

Results coming back up need no new concept if an expansion satisfies the node's
declared outputs:

```python
@study(name="search")
def search():
    coarse = grid.named("coarse")(POINTS)
    best   = refine.named("refine")(coarse)     # expands into a subtree
    return report.named("report")(best)         # ordinary consumer; knows nothing
```

`refine` declares `outputs={"best": …}` and returns a *plan* that exports
`best` instead of a value. Dependents wire to the subtree's export and never
learn the difference.

**An invocation may be replaced by a subtree that satisfies the same
contract** — that one sentence is the whole feature, and it is substitutability
rather than a new control construct. Ray Workflows' `continuation()` and
FireWorks' `FWAction(detours=…)` both ship it.

If the `unfold` / `fold` vocabulary is wanted: unfold builds the tree from a
seed, fold collapses it, and a study is the composition. Precise rather than
decorative, and it names exactly the two halves — compose inner studies from
outer outputs, get one answer back.

### C6 — The tree is data

Once expansion is explicit, the *realised* tree is a document that can be
written to the record. Which buys:

* **Replay.** Rerun the exact tree without re-running expanders — pin the
  shape, vary nothing.
* **Diff.** Two runs of an adaptive study differ in their trees; that
  difference is the scientific result.
* **A path namespace.** `plan_id` is flat today (`src/hedloom/study.py`,
  `plan_id=self.name`). A tree needs `live-source/refresh/live-source-reading/tally`,
  which incidentally removes the hand-rolled `[{"key":…, "reused":…}]`
  projection in `examples/live_source.py`: `run["refresh"]["tally"]` navigates,
  and `summary()` prints an indented preorder tree.
* **Retention that outlives the bytes.** Prune a subtree's workspaces, keep its
  tree document: the shape survives reclamation, so "what did this search
  explore?" stays answerable after the storage is gone. Connects directly to
  `reclaiming-produced-files-2026-08-26.md` and the collector plan.

AiiDA's provenance graph is the reference implementation of this idea.

### C7 — Control flow as a node (the alternative that concedes nothing)

Family 5's move, and the one direction that does **not** weaken the invariant at
all: keep the plan total and make the dynamic part a primitive. A
`refine_while(condition, body_plan)` or `map_over(set, body_plan)` invocation
type whose body is a sub-plan; the plan shows a loop, not its unrolling.

Costs: the body must be uniform, the condition must be expressible without
arbitrary Python, and the record has to represent iterations rather than nodes.

**Worth costing properly before adopting C1–C6.** If the real demand is
"refine until converged" and "map over what the previous step found", C7 serves
both while keeping "nothing is spent until submit, and what runs is what the
Plan showed" literally true. That is a large prize for a narrower feature, and
it is the direction the 09-04 note does not list.

### C8 — Bounds

The blocking model accidentally bounded explosion: you could only nest as deep
as your capacity. A frontier removes that accident, so bounds become explicit:
`max_depth`, `max_nodes`, `max_extent` per family, refused **at graft time**,
naming the path that exceeded them — the same shape as the existing pre-flight
refusals. Airflow's `max_map_length` is the precedent that this is a config key
in practice, not a theoretical concern.

### C9 — Failure

`stop_on_failure` in a tree should stop **grafting** first (the cheapest
possible brake), then stop admitting. A subtree failure needs two modes,
because both are legitimate:

* *blocks the parent's dependents* — the default, identical to an invocation
  failing;
* *is a value* — so an optimiser can see a dead branch as data and keep
  searching. There is currently no way to express "three of my five refinements
  diverged, and that is the answer."

---

## Part 6 — What this would cost

An honest inventory, for whoever prices it:

* **`ONTOLOME.md` and `AGENTS.md` both change.** The submit invariant weakens as
  in Part 1; the result-dependent-control rule needs the Part 2 distinction
  written into it, or it will be cited against every patch.
* **`StudyRun.value` becomes meaningless.** "The last invocation's value" has no
  tree analogue; a study would need to declare its result.
* **Two kernels, again.** The graph kernel is per-plan; a frontier is
  cross-plan. The sequential driver would need the same frontier or the
  kernels disagree about what is runnable — which 09-04 already lists as a
  present defect, not a new one.
* **Report ordering.** "Plan order" becomes tree preorder. The watcher, the
  CLI selectors and retention all key off `root` + `plan_id`, which becomes a
  path.
* **The attempt record becomes a tree on disk**, which collides with
  `attempt-record-and-collector-plan-2026-08-27.md`. These two should be priced
  together or not at all.

---

## Part 7 — Where an agent could start

Ordered by what unblocks the most for the least spend. None of these is
authorised by this note; they are the shapes work would take.

1. **Write the adaptive-refinement example as if the feature existed.** No
   implementation — just the study file the author would want to write, against
   `grid_refinement.py`'s analytic answer. This is the cheapest test of 09-04's
   demand question, and it produces the target API by construction. **Do this
   before anything else.** If it is not compelling, stop.
2. **Price C7 against C1–C6.** A `map_over` / `refine_while` node type that
   keeps the invariant literally true, versus a tree that weakens it. If C7
   covers the demand found in (1), most of this note is moot — and that is a
   good outcome, not a wasted one.
3. **Read AiiDA's `WorkChain` and Flyte's `@dynamic`,** and write one page on
   what each does that hedloom cannot, in hedloom's vocabulary. These are the
   two closest peers; everything else in Part 4 is context.
4. **Settle the Part 2 boundary in writing** — the crisp statement that
   distinguishes result-dependent *authoring* from result-dependent *control*,
   and a refusal that could enforce it. If this cannot be written, that is the
   answer to 09-04.
5. **Prototype C1 alone.** Move authoring to the submit host for the existing
   degenerate case, with no tree, no frontier, no new API. It should delete
   `live_source_state.py`, the shared-session plumbing and the capacity
   headroom, while leaving `NestedCapacityExhausted` in place as a guard for
   anything that still nests on a worker. **This is the only item here that is
   plausibly a small change**, and it is worth costing on its own even if the
   rest is refused.
6. **C2's identity question, on paper.** Is a child plan's document digest a
   sound subtree identity given expanders that read live state? Answer before
   any code, because the whole capability argument rests on it.

## Part 8 — Terms, so we stop renaming them

| Term | Meaning here |
| --- | --- |
| **node** | one Plan document in the tree |
| **edge** | "this child was authored by a body in that parent" |
| **expander** | the function that authors a child from a parent's outputs |
| **graft** | admitting an authored child into the running tree |
| **frontier** | the ready invocations across the whole tree, not one plan |
| **fold** | the ordinary downstream invocation that consumes a subtree's export |
| **degenerate tree** | one fixed edge, statically known — `live_source.py` |

## Follow-up — Shared computation requests and nested scheduling (2026-09-05)

Added at the user's request during the attempt-identity discussion so these
questions can be considered together. This addition does not adopt C1–C9 or
authorize a nesting/scheduler implementation.

The identity direction is now one shared store: a declared computation digest
selects a record, independent of study name and authored invocation key. Each
actual execution has its own numbered try; a future study-history surface would
record the requesting contexts separately. See [the identity implementation plan](attempt-identity-implementation-2026-09-05.md)
and [the discoverability handoff](../DISCOVERABILITY-ATTEMPT-IDENTITY-HANDOFF.md).
This is an invocation-computation identity, not adoption of C2's proposal to
identify a whole subtree by its Plan document.

Equivalent requests will therefore meet the same record more often, including
siblings and separate studies. Scheduler coalescing could let several requests
share one execution within a scheduler. It cannot alone arbitrate independent
sessions or processes sharing the store. Exec must retain exclusive claims;
the layer that waits for a shared result and returns it to each requester is
still to be chosen. Current `AttemptJournal.claim` is nonblocking and raises
`ConcurrentClaim`; the graph kernel reports this attempt error as a refused,
failed invocation. Exclusion is not yet successful result sharing.

Consider with C3's admission/budget question:

- Where do equivalent callers converge, and how does each retain its own
  invocation outcome and the exact selected record/try reference?
- Can a waiting caller release capacity without weakening placement limits?
  A nested waiter must not hold the last resource its producer needs. Account
  for dependencies across nested plans and refuse cyclic waiting.
- What happens when the producer fails, the owning controller disappears, or
  one consumer cancels while another still needs the execution? A consumer
  withdrawing interest and cancelling the shared execution are distinct acts.
- What coordinates two schedulers/controllers? Preserve the record protocol's
  exclusivity and owner-bound job lifetime; do not assume Dask task keys alone
  solve this boundary.

The identity implementation deliberately keeps the present claim refusal and
does not add waiting, retries, resource donation, or scheduler coalescing.
Verify this limitation explicitly rather than claiming that shared identity
already means every simultaneous caller receives a result.

Written on a date, and never edited to stay true — `design/README.md`'s rule
applies here as well. If any of this becomes true it belongs in `docs/` or an
`ONTOLOME.md`; if it is refused, this file records what was refused and why,
and stops being edited either way.
