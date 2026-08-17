# Reading the code before it runs on a real farm

**Written 2026-08-16.** Everything below is between `58d0764` (the ass→hedloom
rename) and `HEAD`. Sixteen source files, about 1100 added lines, plus tests
and documents that are excluded here because they cannot submit a job.

The order is by **what reaches the farm**, not by file. The first three slices
are the ones where a mistake spends real time; the last three cannot, and can
be skimmed or skipped.

To see everything at once, and to know how much is left:

```sh
git diff 58d0764..HEAD -- '*/src/*' 'src/*' examples exec/tests/fakefarm
git diff --stat 58d0764..HEAD -- '*/src/*' 'src/*' examples exec/tests/fakefarm
```

---

## Slice 1 — the command line your farm actually sees (~80 lines)

```sh
git diff 58d0764..HEAD -- exec/src/hedloom_exec/lsf.py
```

This is the only file that builds a `bsub` line, so it is the only file where
being wrong costs farm time rather than a traceback.

What changed: the constructor takes one `defaults` mapping instead of five
named arguments, `settings_for` resolves an invocation over it, `-app` is new,
and `_LIBC.prctl` gained argtypes at import.

**Ask of it:** does `build_argv` produce the line you would have typed? Read it
against a profile in your head — `-J`, `-W`, then `-app`, `-q`, `-n`, `-R`, then
your command. Check the `-R` merge in `_resource_arguments` in particular: a
raw `select[…] span[…]` string and a composed `rusage[…]` become one argument,
and two `rusage` sections refuse rather than merge. That refusal is deliberate;
decide whether you agree with it.

The `prctl` change is two lines and does not alter behaviour on Linux. The
fork-to-`prctl` race it does *not* fix is documented in the `DEVNOTE/TODO`
directly below it, and is the one known hazard on this path.

## Slice 2 — what a profile means (~135 lines)

```sh
git diff 58d0764..HEAD -- run/src/hedloom_run/site.py examples/farm-smoke.site.toml
```

`max_jobs` is new and **required** for an LSF placement, and it is the number
this review most needs your judgement on. It is the share of the farm this
study may spend — not your MAX JOB policy, which counts every job running under
your user from any source. Set it too high and your own submissions and
hedloom's queue behind each other; when it is hedloom that waits, its worker
threads are held by `bsub -I` clients that have not started, so the placement
spends its budget on queueing.

**Ask of it:** what number is right for how you actually use the farm, and does
`_placements_from`'s refusal message tell a future you that?

## Slice 3 — the readiness kernel (~370 lines, the largest)

```sh
git diff 58d0764..HEAD -- run/src/hedloom_run/graph.py
```

Three separate changes live here and are worth reading as three:

1. **Every task is annotated** with `resources={"placement:<name>": 1}`. This
   is the defect fix: an unannotated task is legal on *every* worker, so Dask
   would place — and steal — local work onto the worker whose threads are the
   farm's budget. `_admission` and `_placement_of`.
2. **`_require_admission`** refuses a cluster that declares no capacity for a
   placement the plan uses, before submitting anything, because Dask holds such
   a task unrunnable forever with no exception and an idle-looking cluster.
3. **Stop-admitting** (`_stop_admitting`, `_collect_preserved`, the
   `try/finally`). Cancels what has not started, waits for what has, reports
   the rest as blocked.

**Ask of it:** the deliberate exception in `_admission` — a placement *this run*
cannot serve is left unannotated on purpose, so it is refused per invocation
exactly as the sequential kernel refuses it. Annotating it would hang it
instead. Convince yourself that is right, because it is the one place the two
kernels are kept in step by an omission rather than by a symmetry.

## Slice 4 — the cluster (~115 lines)

```sh
git diff 58d0764..HEAD -- run/src/hedloom_run/cluster.py
```

`SpecCluster` with one in-process `Worker` per placement, because
`LocalCluster` applies one recipe to every worker and cannot express two that
differ. No nanny, deliberately: a nanny restarting a worker under memory
pressure would take its live `bsub -I` clients with it.

**Ask of it:** the dashboard exposure. `spec_cluster` binds the scheduler to
port 0 and the three exposure modes decide what listens where. If this host is
reachable from your network, that is a decision, not a default.

## Slice 5 — the durable record (~120 lines)

```sh
git diff 58d0764..HEAD -- exec/src/hedloom_exec/attempt.py \
    exec/src/hedloom_exec/journal.py exec/src/hedloom_exec/transport.py \
    exec/src/hedloom_exec/errors.py src/hedloom/binding.py
```

`submit_intent` now records `transport` (who submitted) and `substrate` (where
the job lives) as two facts. `ConcurrentClaim` became an `AttemptError`, which
moved to `errors.py` because `attempt` imports `journal` and the dependency
cannot run the other way.

**Read the `DEVNOTE/TODO` on `AttemptJournal.claim` even if you skip the rest
of this slice.** It is the NFS question, it is the weakest load-bearing
assumption in the durability argument, and it is untested. If your study root
is on NFS, read it before the first farm run, not after.

## Slice 6 — the façade (~110 lines)

```sh
git diff 58d0764..HEAD -- src/hedloom/study.py src/hedloom/__init__.py
```

`stop_on_failure` (defaulting to `True`) and the watcher thread that
`watch=True` now starts.

**Ask of it:** the `finally` that stops and joins the watcher. It is bounded and
the thread is a daemon behind that bound, so a wedged `bjobs` cannot hold the
process — check you agree with the bound.

## Slice 7 — the watcher, and what you would run (~220 lines)

```sh
git diff 58d0764..HEAD -- exec/src/hedloom_exec/watch.py examples/farm_smoke.py \
    exec/tests/fakefarm/bsub examples/ota_pvt_clean.py
```

`watch.py` is a fifteen-line change (match on substrate rather than transport).
`examples/farm_smoke.py` is the thing you would actually run, and is new since
the rename rather than modified — read it as a file, not as a diff:

```sh
git show HEAD:examples/farm_smoke.py
```

Four points, two chained operations each, eight `bsub -I` jobs. Independence
between points, chaining within one.

---

## The smallest first run

If the reading leaves you willing, the least you can spend to learn the most:

1. `python exec/examples/lsf_preflight.py --queue <queue>` — checks the
   assumptions this unit makes about LSF, including that `bjobs -o` exists
   (the watcher refuses to work without it) and, in its last and most important
   check, that killing the `bsub` client takes the job with it. Nothing in the
   test suite can establish that one.
2. `python examples/farm_smoke.py <profile>` — **sequential**, no `--dask`.
   Eight jobs one at a time. Proves `bsub -I`, argv, identity, chaining,
   artifacts and reuse against your real LSF, with no concurrency and no
   scheduler involved. This is the run that would catch a wrong `bsub` line.
3. `python examples/farm_smoke.py <profile> --dask` — adds the graph kernel and
   `max_jobs`. Start with `max_jobs = 2` regardless of what you intend to use,
   so a mistake queues nothing.
4. Add `watch=True` last. It is the only part that has never met a real `bjobs`,
   it refuses rather than guesses when the output is not what it expects, and
   it cannot fail a run — but there is no reason to debug it and the kernel at
   the same time.

## What has never met a real farm

Stated plainly, because everything above passed against a *fake* `bsub`:

- The `bjobs` output parser. Both call shapes are now exercised — `-J <name>`
  for discovery and `-o "job_name stat"` for the watcher — and the fake answers
  with LSF's active-queue semantics, reporting PEND and RUN and treating a
  finished or ownerless job as absent. But it still emits what the reader
  expects, so a real `bjobs` whose format differs would not be caught here.
  `exec/examples/lsf_preflight.py` is what checks that.
- The `-R` merge, `-app`, and `memory_mb` as real `bsub` arguments. The fake now
  at least parses `-app` rather than mistaking its value for the first word of
  the command, which is a bug it had while nothing exercised that option.
- Concurrency: `max_jobs` bounding jobs is proven against fake timestamps, not
  against a queue. `FAKE_LSF_PEND_SECONDS` makes a job pend, but because a
  number said so rather than because the farm was busy.
- `flock` on a study root over NFS. See slice 5.

**Owner-bound lifetime is a special case, and worth being exact about.** The
attempt protocol's crash window rests on it, and a TLA+ model
(`docs/attempt-claim-protocol.md`) found that it — not
`discovery_is_authoritative` — is what closes that window. It is now tested, by
killing a submitter for real and asking the farm what became of its job:
`exec/tests/test_fake_farm.py` covers the job dying with its client, the crash
window resubmitting rather than attaching, and the watcher seeing `PEND → RUN`.

What that verifies is *our* half of the chain: that this process binds its `bsub`
client with `PR_SET_PDEATHSIG`, that a client's death is propagated to the work,
and that hedloom then reads the record and the queue correctly. Whether a real
`bsub -I` ends its job when its client dies remains LSF's promise, and a fake
cannot check somebody else's promise. Both tests are mutation-checked — removing
the binding, or letting the fake report a dead owner's job as running, fails
them — so the coverage is real as far as it reaches.
