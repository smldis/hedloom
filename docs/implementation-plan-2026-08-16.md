# Implementation plan — the open work from the 2026-08-14 architecture review

**Written 2026-08-16, for delegation to independent agents.**

Every decision in this file is already made. The review at
[`architecture-review-2026-08-14.md`](architecture-review-2026-08-14.md) was
answered point by point by the repository owner, and the work packages below
are those answers turned into instructions. **An agent picking one of these up
should implement it, not re-open it.** If a package turns out to be wrong on
contact with the code, stop and say so rather than substituting a different
design — the reasoning behind each choice is recorded next to it, and a
disagreement with the reasoning is worth more than a silent departure from it.

Six packages. WP-1, WP-4 and WP-5 are independent and can run in parallel.
WP-2 and WP-3 both touch `src/hedloom/study.py` and must not run concurrently
with each other. WP-6 is last by request and wants WP-1 finished first.

---

## 0. Shared brief — read this before any package

### The tree

Four Python units in one repository, each with its own `src/` and `tests/`:

| Unit | Package | Owns |
| --- | --- | --- |
| `flow/` | `hedloom_flow` | Authoring, the Plan IR |
| `exec/` | `hedloom_exec` | Attempt identity, the journal, transports, reuse |
| `run/` | `hedloom_run` | Binding, readiness kernels, `Site` |
| `src/` | `hedloom` | The façade: `study(...)`, `Study.submit(...)` |

`hedloom_exec` imports neither `hedloom_flow` nor Dask, and that is load-bearing
— it is what has let the readiness kernel be swapped twice. **Do not add an
import in that direction.**

### Running the tests

```sh
cd /home/smldis/working/AI/analog-sim-studies/hedloom
PYTHONPATH=$PWD/src ../.venv/bin/python -m pytest tests run/tests exec/tests -q
```

Baseline on 2026-08-16: **250 passed, 1 skipped**.

Two traps:

* **The `PYTHONPATH=$PWD/src` is not optional.** The shared venv has the
  `hedloom` façade installed as a *copy* in `site-packages` while the three
  units are editable installs, so without it every change under `src/hedloom/`
  is invisible and tests pass against stale code. (Fixing the install would be
  better; it is not part of any package here.)
* **`flow/tests` is already broken** — it imports `examples.characterization`,
  which does not exist, left over from an earlier rename. Do not run it, do not
  fix it, do not report it as a regression.

### House rules

* **Comments say why, not what.** This codebase records the failure a piece of
  code prevents, in prose, next to the code. Read two or three neighbouring
  docstrings before writing one; match that register. A comment restating the
  line below it is noise here.
* **Refuse rather than guess.** Ambiguous configuration, an unrepresentable
  request, a resource nobody declared: raise, name the thing by the name the
  user wrote, and explain what to do. Silently doing something reasonable is
  the failure mode this project treats as a defect.
* **Tests assert behaviour under a name that states the claim.** Look at
  `exec/tests/test_watch.py` for the style: each test name is a sentence about
  what must be true, and the docstring says why it would matter if it were not.
* **Prove a test has teeth.** For any test written against a bug, revert the
  fix, watch it fail, restore. Say in the report what the failure looked like.

### The invariant nothing may break

From `run/src/hedloom_run/binding.py:11-13`:

> Changing which kernel decides readiness changes how long a plan takes and
> nothing else — the same results, under the same identities.

Two kernels exist: `run/src/hedloom_run/driver.py` (sequential, the reference)
and `run/src/hedloom_run/graph.py` (Dask). A change that makes them disagree
about *what a run produces* is out of bounds unless the package below says
otherwise in as many words.

### Deliverable per package

A single commit on `main` (this is a prototype; do not open branches or PRs
unless asked), message in the style of `git log` here — a descriptive subject
line, then prose explaining what was wrong and why this is the fix. Report back
with: what changed, the test output, anything found that the package did not
anticipate.

---

## WP-1 — One vocabulary for placement options (review point 6)

**Owner's call:** *"build as described … refactor this with care, we want things
simple and clean … find a fix for the asymmetry."*

### The defect

There are two lists of what an LSF placement may ask for, and they disagree.

* `exec/src/hedloom_exec/lsf.py:229` — `PLACEMENT_OPTIONS`, the per-invocation
  vocabulary: `cores, licences, memory_mb, queue, resources, walltime`.
* `exec/src/hedloom_exec/lsf.py:364` — the constructor, the *site-default*
  vocabulary: `walltime, queue, resources, cores, timeout`. No `memory_mb`, no
  `licences`.
* `run/src/hedloom_run/site.py:338` — `LSFInteractiveTransport(**settings)`, raw.

So a profile writing `[placement.lsf] memory_mb = 4096` dies with a bare
`TypeError` out of a constructor, in a loader whose own docstring
(`site.py:319-325`) promises that an unbuildable placement is refused *by name*.

### What to build

**One tuple governs both halves.** `PLACEMENT_OPTIONS` becomes the single
vocabulary: it is what an invocation may declare *and* what a profile may set as
a default. Concretely:

1. `LSFInteractiveTransport.__init__` takes its job vocabulary as one
   `defaults` mapping rather than as five hand-listed keyword arguments,
   validated against `PLACEMENT_OPTIONS` and refused by name if it contains
   anything else. `timeout` and `runner` stay separate keyword arguments —
   they are transport mechanics, not things a job asks LSF for, and conflating
   them is how this asymmetry started.
2. `settings_for` resolves each invocation's options over that same mapping,
   which it very nearly does already (`lsf.py:390-429`) — the merge logic
   should end up shorter, not longer. `walltime` remains required.
3. `_transports_from` (`site.py:316`) splits *kernel* keys from *transport*
   keys and refuses anything in neither, by placement name, as a `SiteError`.
   Kernel keys today: `kind`, `max_jobs`. It must also wrap a construction
   failure in a `SiteError` naming the placement, so no profile error can
   surface as a bare `TypeError` or a naked `SubmissionRefused` again.

**Add `-app`.** The owner uses `bsub -app NAME` at their site. Add `app` to
`PLACEMENT_OPTIONS`, to `JobSettings`, to `as_data`, and to `build_argv`. LSF
spells it **`-app NAME`, single dash** — not `--app`.

**Keep `licences`.** The owner does not use it ("licenses are managed in some
other way"), and an earlier reply proposed dropping it from the profile
vocabulary. Do not: `_licences`/`_rusage` are written and tested, and a
special-cased key excluded from the site half is precisely the asymmetry this
package deletes. One tuple, no exceptions. It stays available and unused.

**Nothing to build for `-R rh80`.** A profile can already write
`resources = "select[rh80] span[hosts=1]"`, and `_resource_arguments`
(`lsf.py:329`) merges it with a composed `rusage[...]` into one `-R`, refusing
only when the raw string already carries its own `rusage` section. Verify this
with a test rather than changing it.

### Acceptance

* `[placement.lsf] memory_mb = 4096` loads, and an invocation declaring no
  memory of its own submits with `-R rusage[mem=4096]`.
* `[placement.lsf] app = "spectre"` puts `-app spectre` in the argv.
* An invocation's own `memory_mb` overrides the profile default; the profile
  default applies when the invocation declares nothing.
* `resources = "select[rh80] span[hosts=1]"` plus a memory default renders as
  the single argument `select[rh80] span[hosts=1] rusage[mem=…]`.
* `[placement.lsf] queeu = "reg"` raises `SiteError` naming both the placement
  and the key. **Never a `TypeError`.**
* `[placement.lsf] max_jobs = 4` still reaches the cluster shape and never the
  transport.
* Existing `run/tests/test_site.py` and `exec/tests` LSF tests still pass.

Update `docs/placement-clustering-scheduling.md:72-75`, which lists the
vocabulary and is wrong about two of six entries.

### Out of scope

Pooled placement. `dask_jobqueue`. Any change to how `max_jobs` becomes cluster
shape — that shipped on 2026-08-16 and is correct.

---

## WP-2 — Stop admitting on failure, and never strand a job (review points 4 + 5)

**Owner's call on 4:** *"stop on failure is ok to be exposed, but we want it to
be True by default since often users want to stop scheduling new tasks and
debug the failure, solve it and resubmit."*
**Owner's call on 5:** ☑ both.

These are one primitive and must be built as one change: "stop admitting new
work" is exactly what a failure needs and exactly what an escaping exception
needs.

### The defect, in two halves

**Half one.** `run/src/hedloom_run/driver.py:110` has `stop_on_failure=True`
and blocks everything after the first failure in plan order.
`run/src/hedloom_run/graph.py` blocks only genuine dependents. `src/hedloom/study.py:107-114`
exposes neither, so the façade — the only supported way to run a study —
hardwires whichever the kernel happens to do.

**Half two.** Both kernels catch exactly `(AttemptError, TransportError)`
(`driver.py:202`, `graph.py:176`). `ConcurrentClaim` is a
`JournalError(RuntimeError)` (`journal.py:57,61`), raised when another caller
holds the attempt — an operator rerunning a study while the first run is still
in flight, which on a farm is a thing people do. It escapes. In the Dask kernel
`future.result()` re-raises inside the `as_completed` loop (`graph.py:437-441`),
so the `RunReport` is lost including everything that already succeeded, the
outstanding futures are never cancelled, and their tasks still hold live
`bsub -I` clients that owner-bound lifetime will not reap unless the *process*
dies.

### What to build

**1. `Study.submit(..., stop_on_failure: bool = True)`**, threaded to both
kernels and to the module-level `submit()`. Sequential semantics are unchanged
— pass the flag through; the owner did not ask for the driver to be rewritten
to block dependents instead of successors, and it should not be.

**2. In the Dask kernel, make the flag mean *stop admitting*:** on the first
failing outcome, cancel the tasks that have not started, let the in-flight ones
finish, and return a partial report.

The distinction between "not started" and "in flight" is available and must be
used: `client.processing()` returns `{worker_address: (task keys, …)}` for what
is executing right now. Everything outstanding and *not* in that set has not
begun, costs nothing to cancel, and is cancelled with
`client.cancel(futures, force=False)`. Everything in it is already spending farm
time and holding a `bsub -I` client — **wait for it.** Killing it here would
strand exactly what this package exists to stop stranding.

Cancelled invocations are reported in plan order with
`disposition="skipped"`, `outcome="blocked"` — the vocabulary the sequential
driver already uses (`driver.py:150-160`), so the two kernels describe a
stopped run the same way.

**3. Wrap the completion loop in `try/finally`** so that *anything* escaping —
`ConcurrentClaim`, `KeyboardInterrupt`, a bug — cancels the unstarted futures
and re-raises with a report that names what was left in flight. A run that dies
must say which jobs it left behind. Do not swallow the exception.

**4. Make a contended attempt an ordinary refusal.** `ConcurrentClaim` should
be catchable as an `AttemptError`, so one invocation is recorded `refused` and
the run continues rather than the whole sweep dying. `attempt.py` imports
`journal.py`, so `journal.py` cannot import `AttemptError` from it: move
`AttemptError` into a small `exec/src/hedloom_exec/errors.py`, re-export it from
`attempt.py` so every existing import still works, and declare
`class ConcurrentClaim(JournalError, AttemptError)`. If that proves worse in
practice than widening both kernels' `except` clauses, say so and take the
simpler road — but say so.

### Acceptance

* A four-invocation plan with a failure in the middle, run through the Dask
  kernel with `stop_on_failure=True`, returns a report covering **all four**:
  the failure, whatever completed, and the rest as `skipped`/`blocked`.
* The same plan with `stop_on_failure=False` runs every independent branch, as
  it does today.
* A task in flight when the stop is triggered still appears in the report with
  its real outcome — it was waited for, not cancelled.
* A `ConcurrentClaim` raised by one invocation leaves the other invocations'
  outcomes in the report, and that invocation reported rather than raised.
* An exception with no home still cancels outstanding futures before it
  propagates. Assert on cancellation, not on timing.
* `Study.submit` defaults to `True` and both kernels honour it.

### Out of scope

`bkill`, and any signal to the substrate — see `docs/cancellation-plan.md`,
which is a draft and stays one. This package cancels *futures*, never jobs.
Cancellation is a signal to the substrate and never a write to the record; this
package writes no records at all.

---

## WP-3 — Make `watch=True` show the queue (review point 3)

**Owner's call:** *"i would be fine with an improvement on watcher reporting
since i imagine its very basic for now, keep it simple. (is wiring the real
watcher a good option?)"* — yes, and it is small now that point 2's substrate
bug is fixed.

**Do not start this before WP-2 is committed.** Both edit `Study.submit`.

### The defect

Two different things in this tree are called "watch", and the weaker one owns
the public keyword.

* `Study.submit(watch=True)` (`study.py:183-196`) prints one line per
  **completed** invocation. It knows nothing about a farm.
* `hedloom_exec.watch` polls: one `bjobs -o "job_name stat"` for every live
  attempt, transitions appended to `observations.jsonl` beside each record. It
  is the only thing in the tree that can tell `PEND` from `RUN`.

`bsub -I` blocks from submission to completion, so on a farm `watch=True` prints
nothing at all for the whole queue wait and then a burst — with no way to
distinguish "pending behind a licence" from "running" from "hung".

### What to build

Keep it as small as the owner asked. **Do not rename the flag, do not change its
default, do not add a concept.** `watch=True` additionally starts a background
poller for the duration of the run:

* a daemon thread calling `hedloom_exec.watch.observe(site.root)` every few
  seconds (10 s is a sensible default; make it a module constant, not an
  argument);
* printing **only on transitions**, in the shape
  `corner … pending → running (48s queued)` — `AttemptStatus.queue_seconds` is
  already computed and is the number the pooled-versus-direct question has been
  waiting on;
* stopped and joined when `submit` returns, with a bounded join so a wedged
  poller cannot hold the process;
* **never able to fail a run.** A `TransportError` — an LSF too old for
  `bjobs -o`, which the reader deliberately refuses rather than parsing — must
  print once, disable the poller, and leave the run alone. The watcher is
  evidence about a run, never part of it.

A study with no LSF-substrate attempts must cost nothing: `observe` already
returns early without calling `bjobs` when no live attempt names that substrate.

Make the reader injectable for tests (a private argument, or a small
`start_watcher(root, reader=None)` helper in the façade) — do not reach for
monkeypatching in the test.

### Acceptance

* A study whose attempts are on an LSF substrate prints one line per transition
  and nothing between transitions.
* A local-only study starts no thread that calls `bjobs`, and prints what it
  prints today.
* A reader that raises `TransportError` prints once and the run still completes
  and reports normally.
* `submit` returns with no non-daemon thread left running.
* The existing completion reporter still prints its per-invocation line —
  this is *additional* output, not a replacement.

### Out of scope

A curses view, a web view, a rewrite of `render`. `live_attempts` doing a full
directory scan per refresh is a known cost recorded in review point 8 and is
not this package's to fix.

---

## WP-4 — Remove one allocation from the post-fork window (review point 7)

**Owner's call:** *"this is too complex for me to review now and therefore I
want to avoid inserting new issues or bugs, you can work on it if you simplify
it and u are sure to not introduce further issues."*

This package is therefore deliberately, almost absurdly, small. **Take only
this. Do not take the rest of the finding.**

`exec/src/hedloom_exec/lsf.py:85-93` loads libc at import rather than in the
child, which is the right call and already removed the worst hazard. What is
left: bind `_LIBC.prctl.argtypes` and `_LIBC.prctl.restype` **at import**, so
the forked child performs no ctypes attribute resolution and no argument
marshalling by ctypes' own defaults inside the fork-to-exec window.

Two lines. No new code path, no behaviour change on Linux. Add the one-sentence
comment saying which window this closes and that it does not close the
`preexec_fn` race itself.

**Explicitly out of scope:** the native launcher taking an expected parent PID,
which the existing `DEVNOTE/TODO` at `lsf.py:108-134` describes. That finding
stays open, at lower rank, unscheduled. If while reading you conclude the two
lines are unsafe, change nothing and report — that is a valid outcome here.

---

## WP-5 — Retire the placement doc's stale conclusion (review point 1)

**Owner's call:** *"did we solve this already? do i need to actually read
this?"* — solved; they do not.

`docs/placement-clustering-scheduling.md` closes by arguing that *"concurrency
is global"*, proposing a `max_in_flight`, and saying the design needs *"another
admission-control mechanism"*. Per-placement caps **are** that mechanism and
shipped on 2026-08-16 as `max_jobs`, so that section is now simply wrong, and
wrong in the direction that costs most: it reads as an open design question that
has been closed.

Keep sections 1–3 — the chain diagram and the placement / clustering /
scheduling split are the clearest prose in the tree on the subject, better than
the register's. Replace the final section with a two-line pointer to the
register entry *Two concurrency limits, not one* in
`analog-sim-studies/docs/vision/open-concepts.md`, so the doc is the explanation
and the register stays the decision.

**Run this after WP-1**, which also corrects the vocabulary list at lines 72-75
of the same file.

Documentation only. No code, no tests.

---

## WP-6 — A farm smoke test that actually runs concurrently (review point 8, last bullet)

**Requested directly by the owner, 2026-08-16:** *"develop a hedloom farm smoke
test with cluster spec and dask."*

**Do this last**, and after WP-1.

### Why it is worth a package of its own

`examples/farm_smoke.py` reached a real farm and proved `bsub -I`, artifact
chaining, failure recording and reuse — through the **sequential** kernel. The
combination that has never run anywhere is *graph kernel × LSF-shaped transport
× `BoundTransport`*: the three parts each have coverage and nothing crosses
them. `exec/tests/fakefarm/` has a fake `bsub`/`bjobs`/`bkill`;
`run/tests/test_graph.py` has a real client fixture; they have never met. That
combination is what the next real farm run will be, and it is the cheapest
de-risking available in the tree.

### What to build

**An example, runnable against the real farm.** Either a `--dask` mode on
`examples/farm_smoke.py` or a sibling `examples/farm_smoke_dask.py` — take
whichever leaves less duplicated authoring; the plan, the operations and the
points should be written once. It must build its cluster with
`cluster_for(site)` rather than constructing a `LocalCluster` by hand: the whole
point is that the worker shape and the task annotations come from one reading
of the profile. Print the dashboard link as `ota_pvt_clean.py` does.

**A test that runs it against `fakefarm`**, in the shape of
`tests/test_farm_smoke_example.py`. It must assert more than "it passed":

* every `bsub` argv carries `-J <identity>` and the identity is the attempt's;
* the sweep's invocations ran under the Dask kernel with placement annotations
  — a farm-placement task must not be executable on the local worker;
* **`max_jobs` actually bounds concurrency.** Set it to 2 with four jobs and
  show that no more than two were in flight at once. The fake `bsub` does not
  record time; teach it to write a start and end timestamp into its per-job
  JSON (it already writes `name`, `options` and `command`) and assert no three
  intervals overlap. This is the assertion that proves the placement resource is
  doing its job, and nothing in the suite proves it today.
* a second submission reuses all four invocations and launches no new job;
* the run reports in plan order regardless of completion order.

Keep the fake honest: it reproduces LSF's command-line shape and exit-status
behaviour, not its scheduling. Adding timestamps is within that; adding a queue
is not.

### Acceptance

Green under the standard command, with the concurrency assertion demonstrably
failing if `resources=` is removed from `client.submit` in `graph.py` — check
that and report what it looked like. That is the test this package exists to
produce.

### Out of scope

Running against the real farm — the owner does that. Pooled placement. Any
change to `graph.py` beyond what a genuine bug found here demands.

---

## Not scheduled, and why

* **Results tooling — `run.table()`, `by_point()`, run diffing (review point 9).**
  The owner's call: build it as a **separate library**, not in hedloom. It needs
  nothing from here: every outcome already carries `authored_key`, `operation`
  and `input_digest`, and `StudyRun` holds both the report and the document, so
  the join keys are public. **No hedloom change is required or wanted.**
* **`sweep` moving to that library.** Also needs nothing. `.options(key=...)`
  is the primitive and is already public and already used
  (`rc_corners.py:127`, `ota_pvt_clean.py:298`); `authoring.py:764` reads it
  before consulting the `_SWEEP_KEY` contextvar, which is an ergonomic, not a
  mechanism. An outside `sweep` can pass the key explicitly today. Do not add a
  `keyed()` helper — that proposal was made and withdrawn.
* **The native `preexec_fn` launcher** — see WP-4. Open, ranked below
  everything here.
* **`live_attempts` rescanning every journal per refresh** (point 8). Real, and
  it bites at exactly the sweep size the farm exists for. Checking for a
  published manifest before folding the journal would cover it. Not scheduled
  because nothing has been slow yet.
* **`_IMPLEMENTATIONS` being process-global** and **`StudyRun.value` meaning
  "last in plan order" rather than the plan's declared outputs** (point 8).
  Both are second definitions of something the ontology says has one. Neither
  has cost anything yet.
* **NFS and the claim lock.** The largest open risk in the durability argument
  and deliberately not a work package: it is unreachable until a second
  controller or pooled placement exists. Written up in full in the
  `DEVNOTE/TODO` on `AttemptJournal.claim` and in the register. **Read it
  before writing anything that journals from more than one process.**

---

## Sequencing

```
WP-1  profile vocabulary  ─┬─────────────────────────────► WP-5  doc trim
                           └──────────────┐
WP-4  prctl (independent)                 │
                                          ▼
WP-2  stop admitting ────► WP-3  watcher  ─► WP-6  dask farm smoke
```

WP-1 and WP-4 both live in `lsf.py` but in different functions; concurrent is
fine, sequential is safer. WP-2 before WP-3 is mandatory — both rewrite
`Study.submit`'s signature and body.
