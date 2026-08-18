# Two workers, one resource: what the decision actually buys, and what it still lacks

Companion to `architecture-review-2026-08-14.md`, point 6 and point 9. This
elaborates the one line in the register you asked about:

> Two in-process workers, `local` at one thread and `farm` at many, with farm
> jobs routed by `resources={"lsf": 1}`.
> — `docs/vision/open-concepts.md`, *Two concurrency limits, not one (2026-08-05)*

Same convention as the review: every section ends with a `**Your call:**` slot.
Mark boxes, write underneath, and I pick it up from the file.

---

## 0. Where the code actually was when this was written

Worth pinning down before arguing, because the register's amendment of
2026-08-06 was half-built and it was easy to over-credit it.

At review time:

* `hedloom_run.cluster.cluster_for(site)` built **one** worker:
  `LocalCluster(processes=False, n_workers=1, threads_per_worker=site.threads)`.
* `Site.threads` was parsed from `[kernel] threads`. Its own docstring said
  *"size it from the site's MAX JOB policy and per-user process limits"* — i.e.
  the field then meant *farm* concurrency, which is exactly the conflation
  the register calls a defect.
* `run_plan_graph` called `client.submit(...)` with `key=` and `pure=False` and
  **no** `resources=`.
* Nothing anywhere read a `max_jobs`.

So the reviewed tree had one number, one worker and no resources. The decision
was recorded and measured; none of it was in the tree yet.

**Built 2026-08-16.** `Site.placements` now reads one cap per placement,
`cluster_for(site)` builds a `SpecCluster` with one in-process worker per
placement, and `run_plan_graph` requests that placement's resource on every
task the run can serve. `[kernel] threads` now means local concurrency only.

---

## 1. Three claims, only one of which is about concurrency

The decision bundles three separate things. They have different strengths and
it is worth not defending them as a unit.

**(a) Farm concurrency and local concurrency are different numbers.**
Unarguable. Local parallelism is a property of the submit host's CPU; farm
parallelism is the site's MAX JOB policy for your user. At review time one
integer meant both, so "two hundred farm jobs, little local parallelism" was
unsayable.

**(b) A Dask resource is the right way to cap the farm number.**
Strong, and stronger than the register argues. The register's case is
*measurement* (2/4/8 -> 2.06/1.05/0.53 s) and *observability* (a farm worker
holding live `bsub -I` clients reads as busy, where `secede()` would report it
idle). Both are true, but the load-bearing reason is neither:

> A Dask resource gates admission **at the scheduler**, so a task waiting for
> farm capacity occupies nothing at all. A `threading.Semaphore` inside the
> transport gates **inside the task**, so a task waiting for farm capacity
> occupies a worker thread for its whole wait.

With a semaphore you still need `nthreads >= max_jobs`, so it caps LSF
submissions without capping anything on the submit host — it buys nothing and
costs a lock. That argument holds regardless of what the measurements said, and
it is the one to write into the code comment.

**(c) There should be a second, dedicated worker.**
This is the weakest of the three as stated, and §2 is why. Resources alone
already cap the farm on a single worker. The second worker exists for one
reason only: to give in-process work a thread pool that a farm backlog cannot
saturate. That justification is correct but incomplete as recorded, because
Dask will not honour it without more than the register says.

**Your call:** ☐ split them like this in the register ☐ leave as one entry
☐ (b)'s real reason is the scheduler-vs-thread point ☐ other:

---

## 2. The gap: nothing pins local work to the local worker

This is the one I would most want you to look at.

Dask resource semantics: a resource restricts *where a task carrying that
requirement may run*. It says nothing about tasks that carry no requirement —
**a resource-free task may be scheduled onto any worker, including the farm
worker.** The scheduler prefers less-occupied workers, but that is a heuristic
over duration statistics, not a guarantee, and the duration statistics here are
poisoned: a `bsub -I` task's measured duration includes its queue wait, so the
farm worker's occupancy estimate is wildly non-representative of what a local
task would cost there.

Consequence: with the decision implemented exactly as recorded — farm tasks
carry `{"lsf": 1}`, local tasks carry nothing — a local invocation can land on
the farm worker and sit behind blocked `bsub -I` threads. The isolation that
justifies the second worker is not actually purchased.

The fix is small and it changes the shape of the whole design:

> Annotate **every** task, not only farm tasks. Local placements carry
> `{"placement:local": 1}`; the local worker declares that resource with
> capacity `[kernel] threads`. Routing then becomes exact rather than
> heuristic, and the cluster shape is fully described by the profile.

Once you accept that, the natural key is per placement rather than one global
`"lsf"` — see §3 and §5, which both fall out of it.

**Built 2026-08-16.** Every task whose placement has a transport now requests
`{"placement:<name>": 1}`. Scenarios 4 and 10 in the teaching app are the
shipped shape; the unrestricted shapes discussed above remain as the failure
modes it was built to exclude.

**Your call:** ☐ real gap, pin every task ☐ real but tolerable, rely on Dask's
preference ☐ you have measured otherwise ☐ other:

---

## 3. What carries the annotation: placement name, never transport name

The kernel has to decide, at submit time, which resource a task requests. There
are two candidate sources and the difference is not cosmetic.

**Transport name — do not.** `Study.submit` wraps every site transport in a
`BoundTransport`, whose `name` is `f"bound:{delegate.name}"`, or plain
`"bound"` when there is no delegate. Your own records show it:

```
examples/_runs/ota-dask2/attempts/.../events.jsonl
  {"event":"placement","data":{"resolved":{"placement":"local","transport":"bound"}}}
```

So `transport.name == "lsf-interactive"` is false for every job the façade
submits. That is the identical mistake as review point 2 — since **fixed**, on
2026-08-16, by filtering on an attempt's substrate instead (`watch.py:285`
matching `_LSF_TRANSPORT = "lsf-interactive"` against a journal that recorded
`"bound:lsf-interactive"`) — but here the consequence is worse than a blank
watcher: the annotation silently disappears, and depending on §2 you get either
no cap at all or every farm task stranded.

**Placement name — yes.** `select_transport` already resolves
`(item.policy or {}).get("name") or "local"`, it is a pure function of Plan data
available on the submitting side, and the façade preserves placement names
exactly (`{name: BoundTransport(self.implementations, delegate)}`). Deciding
admission from the placement name keeps the kernel speaking Plan vocabulary and
never asks it what a substrate is:

```python
# hedloom_run/graph.py
def _admission(
    item: PlannedInvocation, transports: Mapping[str, Transport]
) -> dict[str, float]:
    """What this invocation must be admitted against, from the Plan alone.

    Placement, not transport: the façade wraps every substrate in a
    BoundTransport, so a transport's name is 'bound:lsf-interactive' and any
    rule written against 'lsf-interactive' silently matches nothing.
    """
    name = (item.policy or {}).get("name") or "local"
    if transports.get(name) is None:
        return {}
    return {f"placement:{name}": 1}
```

The exception is load-bearing: a placement the run cannot serve at all is left
unannotated so `select_transport` refuses that invocation exactly as the
sequential kernel does and unrelated branches can still run. Annotating it
would strand it in Dask's `no-worker` state forever and make the kernels
disagree about the plan. For placements the run *can* serve, the cluster's
declared capacities come from the site profile — see §4.

**Built 2026-08-16.** `PLACEMENT_RESOURCE = "placement:"` now has one definition
in `site.py`, imported by both the cluster and graph kernels, and admission is
derived from the Plan's placement name rather than any wrapped transport name.

**Your call:** ☐ key on placement name ☐ key on a `Transport.admission`
property instead (and make `BoundTransport` delegate it) ☐ other:

---

## 4. The property worth protecting is one writer, not two workers

The register's original complaint was not really "one number means two things".
It was:

> `Site.threads` … is read by nothing, so the number is written once in the
> profile and again in the operator's `LocalCluster(...)` call with nothing
> comparing them.

That is a *single source of truth* complaint, and it is the thing to protect
when implementing. If the per-task annotation and the worker's declared
capacity come from different places, the same defect returns one level up — and
in a nastier form, because a mismatch is not a wrong number, it is a task that
is never scheduled at all.

So the implementation invariant I would write down:

> `Site.cluster_spec()` and the kernel's `resources=` annotation must be derived
> from the same profile reading, in one function, or the cluster and the plan
> can disagree about what a placement is called.

Profile shape that satisfies it:

```toml
[kernel]
threads = 4                    # local concurrency; nothing to do with the farm

[placement.lsf]
kind = "lsf-interactive"
queue = "reg"
walltime = "1"
max_jobs = 200                 # this user's MAX JOB policy for this queue

[placement.local]
kind = "in-process"            # see §6 — this must become buildable
```

-> workers `{"local": nthreads=4, resources={"placement:local": 4}}`,
`{"lsf": nthreads=200, resources={"placement:lsf": 200}}`; tasks annotated by
`_admission` above using the same placement names.

Two implementation notes that will bite:

* **`LocalCluster` cannot do this.** It applies one worker configuration to all
  workers; heterogeneous workers with different resources need `SpecCluster`.
  So `cluster_for` grows a second construction path, and the `dashboard="none"`
  seam (`_without_http_servers`, which suppresses `ServerNode.start_http_server`
  only *while the cluster is being built*) must be re-verified against it —
  `SpecCluster` may start workers outside that window, in which case a "silent"
  cluster quietly listens. `tests/test_cluster.py` asserts the behaviour rather
  than the seam, so it will catch this; make sure it runs against the new path.
* **`nthreads` must be derived, not configured** — the register already says
  this, and it is right: an `nthreads` below the declared cap binds first and
  silently. With per-placement caps the arithmetic is *sum of the caps on that
  worker*, and if you also keep a global `lsf` cap (§5) it is
  `min(sum(per-placement), global)`.

**Built 2026-08-16.** `Site.cluster_spec()` is the one writer of worker thread
counts and placement capacities; `cluster_for(site)` consumes that spec, and
the optional `threads=` override on `cluster_for` is gone. The shipped design
has one capacity per placement, so each worker's `nthreads` is that cap; the
composed global-cap variant in §5 remains only a possible extension.

**Your call:** ☐ one function, profile -> (cluster spec, caps) ☐ shape above is
right ☐ `max_jobs` belongs elsewhere: ☐ other:

---

## 5. The per-queue cap the codex doc wanted is free

`placement-clustering-scheduling.md` closes with *"Current limitation:
concurrency is global"*, proposes `max_in_flight` per placement, and says
implementing it *"requires scheduling metadata or another admission-control
mechanism"*.

It does not. It is the same mechanism, keyed per placement — which is exactly
what §2 and §3 push you toward anyway. And Dask takes multiple resources per
task, so a global user cap and a per-queue cap compose without any new
machinery:

```python
resources = {"lsf": 1, "placement:lsf_bigmem": 1}
```

The task needs both, so the tighter one binds, and both are enforced by the
scheduler. That is worth stating plainly in the register, because it converts
the codex doc's open problem into a naming decision.

**Your call:** ☐ per-placement caps, no global ☐ both, composed
☐ global only for now ☐ other:

---

## 6. A two-worker cluster needs something to put on the local worker

At review time, `examples/farm-smoke.site.toml` declared one placement,
`[placement.lsf]`, and
`farm_smoke.py` places both operations there. That works, and it is also why
the following has never been hit:

`site._transports_from` **skips** `kind = "in-process"` (it needs callables no
TOML holds), so an LSF-only profile produces `site.transports == {"lsf": …}`.
`Study.submit` only supplies its `{"local": BoundTransport(...)}` fallback when
that mapping is *empty*. So on a real farm profile, any invocation that does not
name a placement resolves to `"local"`, finds nothing, and dies as
`UnsupportedPlacement` — for a placement the façade could have built itself,
since `BoundTransport(implementations, delegate=None)` **is** the in-process
substrate.

Two consequences, both relevant here:

1. The two-worker design has no local work to route until this is fixed — every
   mixed study (farm sims, local post-processing) was then refused at the
   first local invocation.
2. It is roughly a two-line fix in `Study.submit`: always include `"local"` if
   the site does not define it. That also makes the codex doc's
   `[placement.local] kind = "in-process"` example buildable, which review
   point 1 flagged as a lie in the reviewed tree.

Note the interaction with §2: an unannotated fallback placement reintroduces the
free-floating task. Whatever supplies `local` must also supply its cap.

**Built 2026-08-16.** `Site.__post_init__` always supplies a `local` cap,
defaulting to `[kernel] threads` or 1, and `Study.submit` supplies a
`BoundTransport` for every name in `site.placements` that has no configured
transport. A declared `kind = "in-process"` placement is therefore buildable
even though TOML itself cannot hold its callables.

**Your call:** ☐ façade always provides `local` ☐ profile must declare it and
`_transports_from` should build it ☐ other:

---

## 7. The failure modes, and the refusal that is not optional

At review time the register listed three unbuilt pieces, and the third read
like a nicety:
*"a refusal when a caller-supplied client does not offer the declared
resources."* It is not a nicety, it is the design's whole failure mode.

* **No worker declares the resource** -> Dask never schedules those tasks. The
  sweep hangs forever with an idle cluster and no message. This is what happens
  the first time someone passes a plain `LocalCluster` to `Study.submit`, which
  is precisely what `examples/ota_pvt_clean.py:645` did then.
* **Annotation silently absent** (§3, transport-name matching) -> the cap does
  not exist and, per §2, farm work may pile onto the local worker's one thread.

The first is loud-but-mute, the second is quiet-and-wrong. Both are cheap to
close at submit time, before anything runs, in the same spirit as
`_require_shippable`:

```python
def _require_resources(client, needed: Mapping[str, int]) -> None:
    """Refuse a cluster that cannot admit the plan, before anything runs.

    A task asking for a resource no worker declares is not slow, it is never
    scheduled: the sweep hangs with an idle cluster and says nothing.
    """
    offered: dict[str, float] = {}
    for worker in client.scheduler_info()["workers"].values():
        for name, amount in (worker.get("resources") or {}).items():
            offered[name] = offered.get(name, 0) + amount
    missing = sorted(name for name in needed if name not in offered)
    if missing:
        raise SiteError(...)
```

**Built 2026-08-16, with a kernel-invariant qualification.**
`_require_admission` refuses a caller-supplied cluster that lacks capacity for
any placement the run can actually serve, before submitting anything. It does
not demand capacity for a placement with no transport: that invocation must
reach `select_transport` and be refused individually, so other branches run
just as they do under the sequential kernel.

**Your call:** ☐ mandatory, ship with the annotation ☐ separate step
☐ hang is acceptable, log instead ☐ other:

---

## 8. The cap is a courtesy rail, not a correctness requirement

Worth being explicit, because it lowers the stakes on getting `max_jobs` right
and changes what "tuning" means.

LSF is the real authority. Declaring more than the site's MAX JOB policy is
benign: the excess jobs pend, `bsub -I` waits longer, nothing breaks. Declaring
fewer costs throughput and nothing else. So the number can ship conservative and
be tuned by measurement rather than negotiated in advance.

But it is only *tunable* if you can see queue latency — and that is
`AttemptStatus.queue_seconds`, which is computed from an observation the watcher
never writes, because of the transport-name bug in review point 2. So:

> `max_jobs` cannot be chosen from evidence until the watcher works. Review
> point 2 is a prerequisite for tuning this decision, not an unrelated bug.

**Unblocked 2026-08-16.** The watcher now sees façade-submitted jobs, so
`queue_seconds` is obtainable on the next farm run. The number that decides
`max_jobs` — and, per `pooled-placement-plan.md` §6, pooled versus direct — is
now a measurement waiting on farm access rather than a bug waiting on a fix.

That is an independent reason for the ordering §9 of the review proposed
(profile vocabulary -> watcher -> integration test -> `cluster_spec`), and a
better one than "it is cheap".

**Your call:** ☐ agreed, watcher first ☐ pick a number now and tune later
☐ other:

---

## 9. The reframe: the limit is processes, not threads

This is the part I think is genuinely under-examined, and it is the "change of
direction" question you asked for.

Every argument in the register about the cost of a waiting job is denominated in
**threads**: a waiter is ~16 KiB, two hundred jobs is ~3 MB, therefore
concurrency here is "a safety rail, not a scarce resource", therefore the async
transport is deferred until "thread count actually hurts".

But a `bsub -I` waiter is not just a thread. It is a thread **plus a live `bsub`
client process** on the submit host, held for the whole queue wait and the whole
run, and that is not incidental — it is what buys owner-bound lifetime
(`preexec_fn`, `PR_SET_PDEATHSIG`, process group), which is how this design
avoids leases, heartbeats and a reaper. Two hundred in-flight jobs is two
hundred processes on a shared login host.

Two things follow:

1. **The async transport buys less than the register claims.** `asyncio`
   removes the thread; it does not remove the `bsub` child. If the binding
   constraint on the submit host is client processes — RSS, PID limits,
   `ulimit -u`, the sysadmin's patience — then the async path, which the
   register calls "the cleanest end-state" and prices at a rewrite of the
   synchronous durable-record machinery, moves the wrong number.
2. **The real ceiling is what re-opens pooled-versus-direct.** The only ways
   past one-client-per-job are: a detached `bsub` plus polling (the watcher is
   already half of that mechanism — it polls `bjobs -o "job_name stat"`
   today), or a pooled placement where one job holds N slots, or a DRMAA/API
   binding. Each of those trades away owner-bound lifetime or adds a reaper,
   which is the trade the register has been deferring to measurement.

So the tripwire is measurable and nobody has measured it. On the farm, run a
sweep of ~8 concurrent jobs and sum the RSS of the client processes:

```sh
ps --ppid $(pgrep -f farm_smoke) -o rss=,comm= | awk '{k+=$1} END {print k/1024 " MB"}'
```

Multiply by the `max_jobs` you actually want. If that number is uncomfortable on
your login host, the async path is answering the wrong question and the next
architectural move is detached submission plus the watcher, not asyncio.

I would put this in the register as an amendment to *"Deferred, wanted: an async
LSF transport"* — its "what would make it live" criterion is stated in threads
and should be stated in processes.

**Your call:** ☐ amend the register, processes not threads ☐ measure first, then
decide ☐ threads really are the binding constraint, here is why: ☐ other:

---

## 10. Ordering, if you take all of the above

1. **§6** — façade always provides `local`. Two lines, unblocks mixed studies,
   and without it there is no local work to isolate.
2. **§4** — profile vocabulary in one function: `max_jobs` per placement,
   `[kernel] threads` re-documented as local-only, `caps` and `cluster_spec`
   from one reading. This is review point 6 with the concurrency piece attached.
3. ~~**Review point 2** — the watcher's transport matching, which is the same
   bug class as §3 and the prerequisite for tuning (§8).~~ **Done 2026-08-16**,
   in a parallel session.
4. **§3 + §2 + §7** — annotate every task by placement name, `SpecCluster` with
   two workers, refuse a client that cannot admit the plan. These are one
   change; splitting them ships a hang.
5. **§9** — measure client-process cost on the farm; amend the register.

Note what this ordering says: the concurrency work lands *fourth*, and the
argument for that is unchanged from the review — a cap that is wrong costs
throughput, and every item above it costs correctness or costs you the evidence
to choose the cap.

**Status 2026-08-16.** Steps 1, 2 and 4 are built: the façade supplies local,
profiles carry placement caps, and the cluster/annotation/preflight bundle
landed as one change. Step 3, the watcher's transport-name bug, is still open,
as is step 5's farm measurement.

**Your call:** ☐ this order ☐ concurrency first anyway ☐ other:

---

Mark the boxes, write underneath any section, and tell me. Where you disagree,
the argument is the useful part — §2, §9 and the §4 invariant are the three I
would most like a second opinion on, because each of them changes what gets
built rather than only when.

---

## Correction (2026-08-15)

§4 flagged that the `dashboard = "none"` seam might not survive a move to
`SpecCluster`, because `_without_http_servers` only covers cluster
*construction*. Checked against the pinned `distributed==2026.7.1`:
`SpecCluster.__init__` starts the scheduler and the workers inside `__init__`
(`deploy/spec.py:290-292`, `self.sync(self._start)` then
`self.sync(self._correct_state)`), so the seam does cover them. Not a hazard.

**Verified 2026-08-16.** A passing behavioural test now builds a real silent
two-worker `SpecCluster`, asserts that neither scheduler nor workers hold an
HTTP server, and runs a resource-annotated task through it successfully. The
conclusion above is therefore exercised behaviour, not only source reasoning.

The scheduling rules the rest of this document reasons about are now written
down schematically, with source citations, in `dask-scheduling-rules.md`.

---

## Is the lockout real, and how disruptive is the fix? (2026-08-16)

### Real, but prospective at review time — it was not yet a live bug

Checked against the tree:

* `cluster.py:131` hardcodes `"n_workers": 1`. There is always exactly one worker.
* `resources=` appears nowhere in `run/` or `src/`.

So at review time there was no second worker for anything to be locked out
*of*. The then-current shape was scenario 1/2 on the concepts page — one thread
pool shared by everything — which was the conflation this work existed to fix,
not the lockout.

**The lockout is a defect in the design as recorded**, not in the code. Build
the register's sentence literally — two workers, farm tasks carrying
`{"lsf": 1}`, local tasks carrying nothing — and you introduce it on day one.

That is the good case. It costs nothing to avoid now and a farm run to diagnose
later.

### What the fix does not touch

| | Change |
| --- | --- |
| `hedloom_exec` | none. Identity, journal, reuse, transports untouched |
| Plan documents / schema | none. Placement is already in the Plan — that is the A1 answer |
| `hedloom_run.binding` | none. Placement resolution is already correct |
| the sequential driver | none. No scheduler, concurrency 1 |
| identity and reuse | none. Resources never reach the input digest; `build_bundle` already keeps placement out of it |

The last row is the acceptance test worth writing: **a plan run before and after
the change produces identical attempt identities, and the second run reuses
everything the first produced.** If that holds, the change did what the kernel
invariant says it may do — alter how long a plan takes and nothing else.

### What it does touch, precisely

* `run/src/hedloom_run/graph.py` — `_admission()` and `resources=` on submit, plus
  the pre-flight refusal. ~25 lines.
* `run/src/hedloom_run/site.py` — `max_jobs` per placement, the caps mapping,
  `cluster_spec()`. ~15 lines.
* `run/src/hedloom_run/cluster.py` — a `SpecCluster` path for heterogeneous
  workers. ~30 lines, and the largest single piece.
* `src/hedloom/study.py` — always provide `local`, pass caps through. ~2 lines.

Roughly 90 lines in four files, all inside `hedloom-run` and the façade.

### The three places it genuinely disrupts

1. **`run/tests/test_cluster.py` — 6 of its 8 tests.** They monkeypatch
   `distributed.LocalCluster` with a recorder and assert the kwargs it was
   called with, so a `cluster_for` that returns a `SpecCluster` breaks all six.
   The two that build a real cluster and assert behaviour —
   `test_a_silent_cluster_holds_no_http_server` and
   `test_the_seam_is_restored_for_whatever_is_built_next` — survive unchanged
   and are the ones that were guarding anything.
2. **Site profiles change meaning.** `Site.threads` documented itself then as
   *"size it from the site's MAX JOB policy"* — farm concurrency. It becomes
   local concurrency. An existing profile saying `threads = 32` would quietly
   become "32 local threads and no farm cap at all". **Mitigation: make
   `max_jobs` required on any `lsf-interactive` placement**, so an old profile
   is refused loudly instead of reinterpreted silently. That is the house style
   already — `SiteError`, `UnsupportedPlacement`, `_require_shippable` all
   refuse rather than guess.
3. **`examples/ota_pvt_clean.py:645` builds a bare `LocalCluster`** and hands it
   to `submit`. Once tasks are annotated that is an R7 permanent hang. So the
   pre-flight refusal is not a separate nicety — it must land in the same
   commit, and the example must move to `cluster_for(site)`.

**What actually changed (2026-08-16).** The recorder-based cluster tests were
rewritten around `spec_cluster`/`cluster_for`; the behavioural silent-cluster
test now covers the real two-worker resource path. LSF profiles now require
`max_jobs`, so `examples/farm-smoke.site.toml` and the profile generated by
`tests/test_farm_smoke_example.py` declare it. `examples/ota_pvt_clean.py`
moved from a bare `LocalCluster` to `cluster_for(site)`. These were the three
predicted disruptions; no Plan, identity, journal or transport code changed.

### Sequencing, because partial adoption is the trap

Annotation without capacities is a hang. Two workers without pinning is the
lockout. So:

1. **A3 keys** — one line, no behaviour change, improves Dask's estimates
   immediately even with the then-current single worker. Independent, ship anytime.
2. **Façade always provides `local`** — two lines, fixed mixed studies in the
   then-current shape.
   Independent, ship anytime.
3. **One commit:** `max_jobs` in the profile, `cluster_spec()`, annotate every
   task, refuse a cluster that cannot admit the plan, refuse a profile with an
   uncapped LSF placement. All or nothing.
4. Rewrite the six recorder tests; keep the two behavioural ones as the guard.

### One shortcut that does not work

"Just add resources to the single worker" looks like a cheap first step. It
gives a correct farm cap, but it does **not** solve the original defect: a farm
waiter still holds a thread, so `threads` must be at least the farm cap, which
re-authorises that many concurrent local tasks. One worker lets you set farm
concurrency *below* local concurrency, never far above it — and "two hundred
farm jobs, little local parallelism" is the sentence the whole entry exists to
make sayable. Only the two-worker split separates them.

**Your call:** ☐ sequencing above ☐ `max_jobs` required, old profiles refused
☐ other:
