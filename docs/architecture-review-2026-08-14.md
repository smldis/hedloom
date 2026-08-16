# Architecture review — 2026-08-14

Reviewing the current hedloom tree plus `docs/placement-clustering-scheduling.md`.
Written to be answered inline: each numbered point ends with a **Your call** slot.
Leave a verdict there and I will pick it up from this file.

Context assumed: `examples/farm_smoke.py` passed on the real farm. Note what that
did and did not cover — it runs **sequentially** (`subject.submit(site=site,
watch=True)`, no client), so it validated `bsub -I`, artifact chaining, failure
recording and reuse, and validated nothing about the Dask kernel, concurrency,
or observability during a queue wait.

---

## Verdict up front

The engineering is unusually good in the places most projects are bad: the
durable record, identity, reuse, and the refusal to guess. Every finding below
is at a **seam**, not in a unit — the wrapper hides the substrate from the
watcher (2), the façade hides a kernel's failure policy (4), the profile cannot
express what the plan can (6). That is the expected failure shape for a design
whose whole thesis is composition, and it is the cheap kind to fix.

The one thing I would push back on is priority. The register frames the next
move as concurrency. I think the next move is **observability**, and concurrency
second — because on a farm a `bsub -I` sweep is currently a black box for the
entire queue wait, and because the number that decides the pooled-versus-direct
question (queue dispatch latency) is only obtainable once the watcher works.
Concurrency without observability buys you more invisible jobs.

---

## 1. The codex doc re-derives a solved problem and lands on a weaker answer

`docs/placement-clustering-scheduling.md` is accurate as a description. Its
final section ("Current limitation: concurrency is global") is not new: it is
already in `../docs/vision/open-concepts.md` under **"Two concurrency limits,
not one (2026-08-05)"**, which states the same defect *and* its resolution,
*and* the measurements that settled it:

| | codex doc | register (2026-08-05) |
| --- | --- | --- |
| defect | one thread pool serves all placements | same |
| mechanism | "requires scheduling metadata or another admission-control mechanism" — unresolved | **decided**: two in-process Dask workers (`local` at 1 thread, `farm` at many), farm tasks routed by `resources={"lsf": 1}` |
| evidence | none | measured: 8 jobs × 0.5 s at caps 2/4/8 → 2.06 / 1.05 / 0.53 s |
| TOML key | `max_in_flight` | `max_jobs` |
| still to build | unstated | `Site.cluster_spec()`, `resources=` on `client.submit`, refusal when a caller-supplied client lacks the declared resources |

Two concrete problems with letting the new doc stand as written:

- It invents a second name (`max_in_flight`) for a key the register already
  named (`max_jobs`), and a second, vaguer account of the mechanism. The
  register stops being the single place a decision lives, which is exactly the
  failure mode `open-concepts.md` exists to prevent ("nothing from the original
  study is dropped by silence rather than by decision").
- Its example profile includes `[placement.local] kind = "in-process"` with a
  cap. `run/src/hedloom_run/site.py:266-278` deliberately **skipped** `in-process`
  placements when building from TOML (they need callables), so that row could
  not be constructed in the reviewed tree. The example promised a shape the
  loader refused.

Suggested resolution: keep the doc's first three sections — the chain diagram
and the placement/clustering/scheduling split are the clearest statement of that
model anywhere in the tree, better than the register's prose — and replace its
final section with a pointer to the register entry. Then the doc is the
*explanation* and the register stays the *decision*.

**Status 2026-08-16.** The unbuildable-profile claim above is stale, although
the broader documentation-consolidation call remains open. A profile may now
declare `kind = "in-process"`; `_placements_from` gives it `max_jobs`, or
defaults it to `[kernel] threads` or 1, and `Study.submit` supplies its
`BoundTransport` from the authored implementations.

**Your call:** ☐ fold as described ☐ keep both, reconcile the key name only ☑ other

**Notes:** did we solve this already? do i need to actually read this?

> **Reply — 2026-08-16.** No, you don't need to read it, and yes it's solved.
> Its closing section — *"concurrency is global"*, proposing `max_in_flight`,
> saying it needs *"another admission-control mechanism"* — is now simply wrong:
> per-placement caps **are** that mechanism, and they shipped as `max_jobs` on
> 2026-08-16. Nothing else in it is load-bearing that the register doesn't
> already hold. Its first three sections are still the clearest prose in the
> tree on placement vs clustering vs scheduling, so I'd keep those, replace the
> last with a two-line pointer, and you never open it again. Say go and it's
> done in five minutes.

---

## 2. Bug: the watcher is blind to every job the façade submits

> **Fixed 2026-08-16, in a parallel session — not by this review's own work.**
> `watch.py` now filters on an attempt's *substrate* rather than on the name of
> whatever submitted it (`_LSF_SUBSTRATE`, `AttemptStatus.substrate`), the
> journal records it, `hedloom_exec.transport` grew `substrate_of`, and
> `tests/test_watch_sees_the_facade.py` closes the gap this point named: the
> old tests built journals directly and so could never have caught it. 19 tests
> pass across `exec/tests/test_watch.py` and that new file. Recommendation (2)
> below — *journal the substrate, not the wrapper* — is what shipped.
>
> The finding is kept in full because two other documents reason from it, and
> because the *class* of bug outlived the instance: `concurrency-two-workers`
> §3 shows the same mistake waiting in the graph kernel, where matching on a
> transport name would have silently dropped the placement annotation.

Highest-value finding in the review.

- `exec/src/hedloom_exec/watch.py:50` — `_LSF_TRANSPORT = "lsf-interactive"`
- `exec/src/hedloom_exec/watch.py:285` — `watched = [item for item in live if item.transport == _LSF_TRANSPORT]`
- `exec/src/hedloom_exec/attempt.py:242` — the journal records `transport.name` at `submit_intent`
- `src/hedloom/binding.py:106` — through the façade the transport is a
  `BoundTransport`, whose name is `f"bound:{delegate.name}"`

So every attempt a hedloom study submits records `transport = "bound:lsf-interactive"`,
the exact-match filter drops all of them, `observe()` returns early, and
`exec/examples/watch_sweep.py` shows an empty farm for a study with fifty jobs
in flight.

Why this matters more than a one-line filter bug suggests: the register lists
"it does not deliver the view that was asked for — Dask can show tasks; it
cannot show `PEND` versus `RUN`" as a standing argument, and the `watch` module
*is* the answer to it. That answer has been disconnected since the façade was
introduced — which is since the only supported way to run a study. And the
by-product the register wants most — *"queue latency per job becomes measurable,
which is the number the pooled-versus-direct question has always lacked"* — is
unobtainable until this is fixed.

Two fixes; I would argue for the second:

1. Match by suffix in `watch.py`. Two lines, hides the modelling error.
2. Make the journal's `transport` field name **the substrate that holds the
   job**, not the wrapper. `BoundTransport` would report its delegate's name and
   carry its own wrapping as a separate fact (it already sets `"bound": True` on
   the delegated handle at `binding.py:149`). This is the truer record: the
   journal's job is to say what accepted the work, and `bound:` is a fact about
   how the body was called, not about the substrate.

Either way it needs a test that runs a study through `BoundTransport` and asserts
`live_attempts` sees it — `exec/tests/test_watch.py` builds journals directly,
which is why this never showed.

**Your call:** ☐ fix (2) — journal names the substrate ☐ fix (1) — suffix match ☑ other

**Notes:** i was fine watching just the tasks, but this is a nice improvement, how does the fix u landed recently work? point me at the key points in the code and open them in vscode.

> **Reply — 2026-08-16.** In one line: the journal was writing **one name for
> two facts**. `submit_intent` now records both — `transport`, what was asked to
> submit (`bound:lsf-interactive`), and `substrate`, where the job actually
> lives (`lsf-interactive`) — and the watcher matches the second.
>
> The five places, in reading order:
>
> 1. `exec/src/hedloom_exec/transport.py` — the optional `substrate` declaration
>    on the `Transport` protocol, and `substrate_of()`. A transport that submits
>    its own work needs no change; only a wrapper does.
> 2. `src/hedloom/binding.py:116` — `BoundTransport` declares the substrate it
>    delegates to, or `in-process` when there is no delegate. **This is the fix.**
> 3. `exec/src/hedloom_exec/attempt.py:253` — both facts recorded.
> 4. `exec/src/hedloom_exec/journal.py:270` — folds it, falling back to
>    `transport` so records written before the split are still watched.
> 5. `exec/src/hedloom_exec/watch.py:296` — matches on substrate.
>
> Proof it was real: I reverted that one line and the new test
> `tests/test_watch_sees_the_facade.py` fails with `assert [None] == ['pending']`
> — the observer returned rows with nothing observed, which is indistinguishable
> from a finished sweep. That is why it survived: `exec/tests/test_watch.py`
> writes its journals directly, so it could only assert what it had just written.
>
> On *"i was fine watching just the tasks"* — the thing you gain is
> `queue_seconds`, and it matters more than it did last week: `max_jobs` is now a
> **required** profile field, and queue latency is the only evidence for choosing
> it. All six files are open in VS Code.

---

## 3. `watch=True` on the façade means much less than the word promises

`src/hedloom/study.py:112,177-190`: `watch=True` installs a reporter that prints
one line per **completed** invocation. The README (line 27) presents it as the
operator gesture.

Locally that is fine — things complete in seconds. On the farm it is the wrong
shape: `bsub -I` blocks from submission to completion, so `watch=True` prints
nothing for the whole queue wait, then prints a burst. In the sequential kernel —
the one the farm smoke test used — that is one line every few minutes with total
silence between, and no way to tell "pending behind a licence" from "running"
from "hung".

The pieces to fix it exist. `hedloom_exec.watch.observe(root)` does one `bjobs`
call for all live attempts; `render(rows)` prints the table. What is missing is
~30 lines in `study.submit` starting that poll on a background thread against
`site.root` for the duration of the run, and a `watch` argument that means the
thing the module is named after. Depends on finding 2.

Worth noting independently: two different mechanisms in this tree are called
"watch", and the weaker one owns the public keyword.

**Your call:** ☐ wire the real watcher into `submit(watch=True)` ☐ rename the façade flag, leave watching external ☑ other

**Notes:** what are the two mechanisms? i would be fine with an improvement on watcher  reporting since i imagine its very basic for now, keep it simple. (is wiring the real watcher a good option?)

> **Reply — 2026-08-16.** The two mechanisms:
>
> * **`Study.submit(watch=True)`** — a reporter that prints one line per
>   *completed* invocation. Knows nothing about the farm. Owns the public keyword.
> * **`hedloom_exec.watch`** — an external poller: one `bjobs -o "job_name stat"`
>   for all live attempts, transitions appended to `observations.jsonl` beside
>   each record. This is the only thing that can tell PEND from RUN.
>
> Is wiring the second in a good option? **Yes, and it is now small** — it was
> blocked on finding 2, which is fixed. Keeping it simple, as you asked: don't
> rename the flag, don't change its default, don't add a concept. `watch=True`
> additionally starts a background thread calling `observe(site.root)` every few
> seconds and printing only on transitions, so a farm wait reads as
> `corner … pending → running (48s queued)` instead of silence followed by a
> burst. About 30 lines, no new vocabulary, and it is what makes the queue wait
> legible. I'd do that rather than anything cleverer.

---

## 4. The two kernels disagree about failure, and the façade hides it

`run/src/hedloom_run/graph.py` blocks only genuine dependents — its docstring is
explicit and right: *"one corner failing does not abandon the other forty-nine,
which is what a sweep wants and what the sequential driver could not offer."*

`run/src/hedloom_run/driver.py:110,151` blocks everything **after the first
failure in plan order**, independent branches included, because `stop_on_failure`
defaults to `True` and the loop is linear.

`src/hedloom/study.py:107-114` does not expose `stop_on_failure` at all. So the
sequential path is hardwired to the harsher rule — and the sequential path is
what the farm smoke test used and what anyone gets by default.

Concretely: a 50-corner farm sweep without a client, corner 3 fails on a licence
timeout, corners 4–50 reported `blocked` having never been attempted. On the
same plan with a client they would all have run. That is a difference in *what a
run does*, not in how long it takes, and it sits just outside the invariant
`run/src/hedloom_run/binding.py:11-13` states.

The driver already has `item.depends_on` from `plan_bundles`. Making the
sequential kernel block dependents rather than successors is ~15 lines and would
close the gap in the honest direction — then the two kernels agree and the flag
can stay for the "stop everything, something is systemically wrong" case.

**Your call:** ☐ sequential blocks dependents (kernels agree) ☐ just expose the flag on `submit` ☐ leave, document the divergence

**Notes:** stop on failure is ok to be exposed, but we want it to be True by default since often users want to stop scheduling new tasks and debug the failure, solve it and resubmit.

> **Reply — 2026-08-16.** Taking that as: expose `stop_on_failure` on
> `Study.submit`, default `True`. Worth naming what it has to *mean* in the Dask
> kernel, because today it means nothing there — dependents are already blocked,
> but the other forty-nine independent corners keep going.
>
> "Stop scheduling new tasks so I can debug" is precisely **stop admitting**:
> cancel the futures that have not started, let the in-flight ones finish,
> return a partial report naming what was skipped. That is the same primitive as
> level 1 in `cancellation-plan.md` §3(a) — so it gets built once and both
> features get it, which also answers point 5. Your resubmit story is already
> safe: content-addressed reuse means the corners that completed are reused and
> only the failure re-runs.

---

## 5. An exception outside two classes aborts the whole run and strands jobs

Both kernels catch exactly `(AttemptError, TransportError)` —
`driver.py:202`, `graph.py:176`.

`ConcurrentClaim` is a `JournalError(RuntimeError)`
(`exec/src/hedloom_exec/journal.py:57,61`), not an `AttemptError`. It is raised
by `journal.claim()` under `LOCK_NB` when another caller holds the attempt — the
ordinary case being an operator rerunning a study while the first run is still in
flight, which on a farm is a thing people do. It escapes the kernel.

In the Dask kernel that is worse than a traceback. `future.result()` re-raises
inside the `as_completed` loop (`graph.py:317-318`), so:

- the `RunReport` is lost, including outcomes of everything already succeeded;
- the remaining futures are never cancelled and their tasks still hold live
  `bsub -I` clients;
- owner-bound lifetime only fires when the **process** dies, so if the caller
  catches and continues, those farm jobs keep running with nothing watching them
  and no record naming them.

Two independent fixes, both small:

1. Make `ConcurrentClaim` an `AttemptError` (or widen the catch), so a contended
   attempt is recorded as one invocation `refused` and the run continues. This is
   arguably the correct modelling anyway — "another caller holds this attempt" is
   a fact about the attempt.
2. Wrap the `as_completed` loop so anything escaping cancels outstanding futures
   and still returns a partial report naming what was in flight. A run that dies
   should say which jobs it left behind.

**Your call:** ☑ both ☐ (1) only ☐ (2) only ☐ neither, out of scope

**Notes:** _

> **Reply — 2026-08-16.** Left open, but your answer to point 4 already implies
> at least **(2)** — a run that stops must say which jobs it left behind. I'd
> fold this into the same change: it is one `try/finally` around the completion
> loop, plus `client.cancel` for what hasn't started. Nothing extra to decide
> unless you want (1) as well.

---

## 6. The site profile cannot express what a plan can — and this blocks the concurrency work

`exec/src/hedloom_exec/lsf.py:229` — the per-invocation vocabulary is
`cores, licences, memory_mb, queue, resources, walltime`.

`exec/src/hedloom_exec/lsf.py:364-373` — the constructor accepts
`walltime, queue, resources, cores, timeout`. No `memory_mb`, no `licences`.

`run/src/hedloom_run/site.py:269` — `LSFInteractiveTransport(**settings)`, raw.

So a profile writing `[placement.lsf] memory_mb = 4096` raises a bare `TypeError`
from a constructor, not a `SiteError` naming the placement — in a loader whose
own docstring (`site.py:255-259`) says an unbuildable placement must be refused
by name rather than blamed on the Plan later. And
`docs/placement-clustering-scheduling.md:72-75` states site values are transport
defaults "for LSF they include queue, cores, memory, walltime, licences, and a
raw resource expression". Two of those six are not true.

Why this was on the critical path rather than a papercut: the register's
concurrency shape put `max_jobs = 200` inside `[placement.lsf]`. In the reviewed
tree that key reached the constructor and hit the same raw `TypeError`, so it
had to be split out before `Site.cluster_spec()` could ship.

Shape I would build: give the transport one `defaults: Mapping[str, Any]`
resolved through the existing `settings_for` (which already does exactly this
merge for the per-invocation half), and have `_transports_from` split
kernel-level keys (`max_jobs`) from transport keys, refusing anything in neither
set by name. ~30 lines, and it deletes the asymmetry rather than patching it.

**Partly addressed 2026-08-16; point 6 remains open.** `max_jobs` is now
profile vocabulary: `_placements_from` validates it, requires it for an
`lsf-interactive` placement, and `_transports_from` removes it before calling
`LSFInteractiveTransport`. But `memory_mb` and `licences` are still forwarded
raw to `LSFInteractiveTransport(**settings)`, whose constructor accepts
neither, so either declaration still raises a bare `TypeError` rather than a
placement-naming `SiteError`. The defaults-vocabulary asymmetry is not fixed.

**Your call:** ☑ build as described ☐ narrower: just add memory_mb/licences to the constructor ☐ defer

**Notes:** yep refactor this with care, we want things simple and clean. Also in the side consider that i dont use the license keyword in the farm (licenses are managed in some other way), that i use a --app bsub option sometimes and i use a -Rrh80 that can be merged with rusage etc.. for specifying the selection of the machine based on its os. maybe we can plug this in now as a sidequest. find a fix for the asymmetry.

> **Reply — 2026-08-16.** Building it. On the sidequest, those are three
> separate facts and only two need work:
>
> * **`-R rh80` is already expressible.** A profile can set
>   `resources = "select[rh80] span[hosts=1]"` today, and
>   `lsf.py:329 _resource_arguments` merges it with a composed `rusage[...]` into
>   one `-R`, refusing only if your string already contains its own `rusage`
>   section — because merging two rusage sections would be guessing. Nothing to
>   build.
> * **`memory_mb` is the real gap**, and it is this finding: a valid placement
>   option that the transport constructor cannot accept, so a profile declaring
>   it dies with a bare `TypeError`.
> * **`-app` is new.** I'll add `app` to the constructor, to `PLACEMENT_OPTIONS`
>   and to `build_argv`. Note LSF spells it `-app NAME`, single dash — I'll use
>   that unless your site differs.
> * **`licences`:** since you don't use it, I'll leave the existing code path
>   alone (it's written and tested) but stop treating it as part of the profile
>   vocabulary, and won't add it to the constructor. Say if you'd rather I
>   delete it outright.

---

## 7. `preexec_fn` under the recommended farm cluster — re-ranking a known risk

`exec/src/hedloom_exec/lsf.py:108-134` already records this, honestly and in
detail: a fork-to-`prctl` race, a `DEVNOTE/TODO` naming the fix (a native
launcher taking the expected parent PID), and the observation that the
replacement *"should also remove Python's general `preexec_fn` hazard in the
threaded Dask kernel."*

I am not adding information — I am arguing it is ranked too low. The recommended
farm configuration at review time was `LocalCluster(processes=False,
threads_per_worker=N)` with N threads each calling `subprocess.run(preexec_fn=...)`,
and CPython documents `preexec_fn` as unsafe in the presence of threads. The
module already removed the worst hazard by loading libc at import rather than in
the child (`lsf.py:85-93` — that reasoning is exactly right), so this is a tail
risk, not a likely one. But it is a tail risk on the *only* path that spends farm
time, and it presents as an intermittent hang in a child between fork and exec,
with a submitted job and a blocked thread. That is the most expensive possible
debugging session, and it can only happen at scale, on the farm, far from a
debugger.

Cheap partial mitigation if the native launcher is too much right now: bind
`_LIBC.prctl.argtypes` / `.restype` at import so the child performs no ctypes
attribute resolution or argument conversion. Does not close the race; removes one
allocation path from the post-fork window.

**Status 2026-08-16: still open.** The current topology is now an in-process
`SpecCluster` with one `Worker` per placement, but its farm-placement threads
still call the same `subprocess.run(preexec_fn=...)` path. The topology changed;
the risk did not.

**Your call:** ☐ schedule the native launcher before the parallel farm run ☐ cheap mitigation now, launcher later ☐ accept, revisit if it bites

**Notes:** this is too complex for me to review now and therefore I want to avoid inserting new issues or bugs, you can work on it if you simplify it and u are sure to not introduce further issues.

> **Reply — 2026-08-16.** Agreed, and I'll take only the part that cannot
> introduce a new failure: bind `_LIBC.prctl.argtypes` and `.restype` at import.
> Two lines, no new code path, no behaviour change on Linux — it only stops
> ctypes from marshalling the arguments by its own defaults, which is the single
> way this can go wrong without warning. The native launcher stays unscheduled
> and this finding stays open at lower rank. If that still feels like moving
> something you can't review, say so and I'll leave it entirely.

---

## 8. Smaller observations, no call needed

- **`_IMPLEMENTATIONS` is process-global** (`src/hedloom/__init__.py:84`). Two
  studies imported into one process share one registry. Benign today, and
  `study(plan, implementations=...)` is the escape hatch, but it is the second
  notion of "what an operation is" that `ONTOLOGY.md:50-54` says does not exist.
- **`StudyRun.value` is "the last invocation in plan order"**
  (`src/hedloom/study.py:49-53`). The plan declares `outputs`, and `_plan_id`
  already reads them (`study.py:169-174`). Reading `value` from the declared
  outputs would make it a property rather than a convention.
- **`live_attempts` is a full directory scan per refresh**
  (`exec/src/hedloom_exec/watch.py:197-215`), and each `status_of` re-reads a
  whole journal. The file says so ("honest at prototype scale and obviously wrong
  at any other"). Worth noting only because it bites at exactly the sweep size
  the farm exists for: 50 corners × several reruns × a 10 s refresh is a lot of
  re-reading of terminal attempts. Checking for a published manifest before
  folding the journal would cover it.
- **No test runs the Dask kernel against an LSF-shaped transport.**
  `exec/tests/fakefarm/` has fake `bsub`/`bjobs`/`bkill` and
  `run/tests/test_graph.py` has a real client fixture, but nothing crosses them.
  That combination — graph kernel × `bsub -I` × `BoundTransport` — is the one the
  farm has never met and the smoke test does not cover. Cheapest available
  de-risking in the tree.

---

## 9. Direction: what I would do next, and one push-back

### The ordered path, if the goal is "a parallel farm sweep you can watch"

1. **6** — profile vocabulary. Unblocks everything else and is pure deletion of
   an asymmetry.
2. **2 + 3** — the watcher, wired into `submit`. The one that changes what the
   system is like to use, and it produces the queue-latency number the
   pooled-versus-direct decision has been waiting on since it was recorded.
3. **8, last bullet** — graph kernel × fakefarm integration test. Cheap, and it
   covers the exact untested combination before it is pointed at a real farm.
4. `Site.cluster_spec()` and the resource-bounded farm worker, per the register.
   Design and measurements already exist; this is implementation.
5. **4, 5** — failure semantics and exception containment. Before a long
   unattended sweep, not after one strands jobs.

Concurrency is fourth, not first. That is the argument, not an oversight.

**Status 2026-08-16.** Step 4 is built, together with the concurrency slice of
step 1: `max_jobs`, placement caps, `cluster_spec`, task annotations and the
admission preflight now ship. Step 1 is only partial because `memory_mb` and
`licences` still fail as described in point 6. Steps 2, 3 and 5 remain open:
the watcher bug and façade integration, the graph-kernel × fakefarm coverage,
and failure containment have not been built.

### The push-back

Everything in this system is *per invocation*. The record, identity, reuse,
placement, observation — all of it beautifully engineered around one attempt. But
nobody runs a study to obtain an attempt. They run it to answer a question across
a sweep: does this design close over PVT, and where is the margin.

`examples/ota_pvt_clean.py` already knows this — it writes `report.md`, a real
deliverable with a corner table and a verdict, and that is the right place for it
(domain reporting is not the framework's to own; the ontology is correct to
exclude it). But the *reason* that example is 600 lines is that it has to
reassemble, by hand, a structure the system already has and does not expose.

`sweep(points, key=...)` keys every invocation `<point>:<operation>`. A
`StudyRun` therefore already contains a table — point down the rows, operation
across the columns — and offers no way to read it. The public surface is
`run["cold:simulate"]`, one cell at a time, plus `run.value`, the last row of the
last column.

The smallest thing that would change what hedloom is *for*:

```python
run.by_point("measure")            # {"tt_1v80_27c": {...}, "ss_1v62_125c": {...}}
run.table(["measure", "evaluate"])
```

Derived entirely from data already present, owns no domain vocabulary, invents
nothing about what an operation is — and turns a study's report operation from
200 lines of reassembly into 20 lines of formatting. It also makes the obvious
next thing possible: **diffing two runs of the same plan**, which is the actual
daily question ("what changed when I resized M3?") and which the content-addressed
identity model already makes exact — reuse tells you precisely which invocations
two runs share, so the diff is nearly free.

I would rank that above the concurrency work too. Concurrency makes a sweep
finish sooner. A result table and a run diff make it *answer something* — and the
register's own tripwire for whether Dask was the right call at all
(*"if the OTA/PVT study turns out to be a few dozen corners of several minutes
each, a thread pool and a watcher are enough"*) is a question about what the
studies actually look like, which you will learn faster by making results legible
than by making them faster.

**Your call:** ☐ agree, put `run.table()`/run-diff on the roadmap ☐ concurrency first, results later ☐ this belongs in studies, not the framework — argue it back

**Notes:** i would rather build a separate library of this kind of tooling, is it possible without having to patch the heloom current framework? even sweep i feel like it shoudnt be here. We need to elaborate this, since its easy to fall into domain specific tooling that we want to avoid in hedloom.

> **Reply — 2026-08-16.** The best question in the review, and the answer is
> yes — **without patching hedloom**. The test is whether the join keys are
> already public, and they are: every outcome carries `authored_key`,
> `operation` and `input_digest`, and `StudyRun` holds both the report and the
> document. So everything `run.table()` would have done can be computed by an
> outside library from public data, with the framework unchanged. That settles
> it: results tooling is a **consumer**, not a feature, and I withdraw the
> push-back in this point's favour of yours.
>
> **On `sweep` — I misread you, corrected 2026-08-16.** You are not proposing to
> delete it, you are proposing it be *reintroduced by the outside library*. That
> is a different and better claim, and I checked what it would cost.
>
> `sweep` is twelve lines (`flow/src/hedloom_flow/authoring.py:443`) over a
> single private `ContextVar`, `_SWEEP_KEY`. That var is **set in exactly one
> place and read in exactly one place** — line 463 and line 768, where the
> authored key becomes `<point>:<operation>`. Nothing else in the tree touches
> it.
>
> **And on a second look, it needs nothing at all — corrected again.** You asked
> what the primitive is, and `keyed` is not it. The primitive is
> **`.options(key=...)`** — a call naming its own authored key — and it is
> already public and already used all over the examples
> (`rc_corners.py:127`, `ota_pvt_clean.py:298`). `authoring.py:764` reads it
> first and only falls back to `_SWEEP_KEY` when a call named nothing.
>
> So `_SWEEP_KEY` is not a primitive, it is an **ergonomic**: ambient naming so
> the author does not repeat the key at every call. An outside library can
> resolve the point key itself and pass `.options(key=f"{point}:{op}")`
> explicitly, which means **`sweep` can move out today with zero changes to
> hedloom** — not six lines, none. My `keyed()` proposal would have added public
> API to hedloom for something only the domain library needs, which is precisely
> what you are guarding against. Withdrawn.
>
> The cost is real but it is the right cost: without the contextvar, an outside
> `sweep` has to route each call through its own wrapper to inject the key
> instead of getting it ambiently. That friction belongs to the domain library,
> and it can solve it with a contextvar of its own.
>
> The test this suggests is sharper than the one I gave, and it is two questions,
> not one: **what is the primitive under this convenience, and can anything but
> hedloom enact it?** `sweep` fails the second — anyone can build it on
> `.options(key=...)`. Points 4 and 5 pass it: "stop admitting new work" acts on
> futures only the kernel holds, so no caller can enact it from outside, and half
> of it is already in `driver.py` as `stop_on_failure`. That is the line, and it
> is a better one than "creates keys vs interprets them".

---

## How to answer

**A written note supersedes the boxes.** Where the notes answer the
question, that is the decision, whether or not an option is ticked — the
boxes are a shortcut for the cases where one of them happens to be right.


Mark the boxes, add notes under any point, and tell me — I will pick it up from
this file. Where you disagree, say why rather than just ☐ other; several of these
are judgement calls about priority rather than defects, and the argument is the
useful part.
