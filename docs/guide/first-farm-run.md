# Your first run on a real farm

Everything else in this guide works the same whether a placement is `local` or
`lsf`. This page is about the one transition where that stops being true: the
first time a study spends queue time on a farm nobody here can test against.

Read it before that run, not after.

## The ladder

The least you can spend to learn the most, in order. Each step is the previous
one with exactly one thing added, so a failure names its own cause.

### 1. Preflight the site, before any study

```console
python exec/examples/lsf_preflight.py --queue <queue>
```

This checks the assumptions `hedloom_exec` makes about your LSF: that the
commands exist, that interactive jobs are admitted, that `bjobs -J` finds a job
by name, that `bjobs -o` exists at all — the watcher refuses to work without it
— and, last and most important, **that killing the `bsub` client takes the job
with it**.

That final check is the one nothing in the test suite can establish. Owner-bound
lifetime is LSF's promise, not ours, and the whole direct-submission design
rests on it. If it fails, stop: the design premise is wrong for your site, and
the answer is a change to the transport rather than a tuning of the study.

Add `--licence <name>` to also submit one job requesting a licence you actually
use, which is the only way to learn whether the site parses the `rusage` term
this code composes.

### 2. One job at a time, against a real queue

```console
python examples/farm_smoke.py examples/farm-smoke.site.toml
```

with **`max_jobs = 1`** in the profile. Eight `bsub -I` jobs over four points,
one in flight at a time, no real tool involved. This proves argv, `-J`
identity, an artifact chaining out of one job and into the next, failure
recording, and reuse on a second submission — against your real LSF, with
nothing concurrent to confuse a failure.

This is the run that catches a wrong `bsub` line, and it is cheap.

Concurrency is the profile's, so `max_jobs = 1` is the honest spelling of
"one at a time under the real scheduler". If you want *genuinely no scheduler* —
worth doing once, if `bsub` itself is what you suspect — pass `sequential=True`
to the example's `session(...)` call. That builds no cluster at all.

If you would rather debug the kernel before the watcher, drop `watch=True` from
the same call for this first pass. The watcher is on for the whole run
otherwise; it is not something added at the end.

### 3. The same command, with concurrency

Raise `max_jobs` to 2 in the profile, then higher. No second command and no
flag — which is the point of concurrency being a site fact rather than a mode.

Raise it slowly. A mistake at `max_jobs = 2` queues nothing worth apologising
for, and the number you are looking for is measured from queue latency rather
than derived.

`max_jobs` is deliberately **not** your site's MAX JOB policy; see
[the two numbers](sites.md#the-two-numbers-which-are-about-two-different-machines)
for why setting it that high makes the placement spend its budget on queueing.

### 4. Pooling, only if the shape calls for it

```console
python examples/farm_smoke_pooled.py examples/farm-smoke-pooled.site.toml
```

A pool trades per-invocation visibility for paying the queue once per worker.
Do not reach for it until step 3 has told you what your queue wait actually is;
[the trade is described where placement is](sites.md#placement-kinds).

## What has never met a real farm

Stated plainly, because the ladder above is the only thing that tests any of
it — everything in the test suite passes against a *fake* `bsub`.

**Has met a real farm.** `examples/farm_smoke.py` has run against a real LSF
installation through the **sequential** kernel: the `shell` launcher reaching a
real `bsub -I` job, argv, `-J` identity, an artifact chaining from one job into
the next, failure recording, and reuse on resubmission.

**Has not.** Everything that needs more than one job in flight:

- **The graph kernel itself.** Nothing has yet put it in front of a real queue,
  so concurrency, `max_jobs` as a real bound, and failure isolation between
  branches are exercised only against a fake `bsub` and a real client fixture.
- **The `bjobs` output parser.** Both call shapes are exercised — `-J <name>`
  for discovery and `-o "job_name stat"` for the watcher — and the fake answers
  with LSF's active-queue semantics, reporting `PEND` and `RUN` and treating a
  finished or ownerless job as absent. But the fake emits what the reader
  expects. A real `bjobs` whose format differs would not be caught here;
  `lsf_preflight.py` is what checks that.
- **The `-R` merge, `-app`, and `memory_mb`** as real `bsub` arguments. Local
  tests fix the string this code builds — one `-R` holding whitespace-separated
  sections, with memory and licences in a single `rusage` — and can say nothing
  about whether your site parses it that way or knows those licence names.
- **`flock` on a study root over NFS.** The claim in
  `hedloom_exec.journal.AttemptJournal.claim` is the weakest load-bearing
  assumption in the durability argument, and it is untested. Its `DEVNOTE/TODO`
  says so. **If your study root is on NFS, read that note before the first farm
  run, not after.**
- **Every domain study.** `../studies/rc_corners.py` and both `ota_pvt`
  variants run entirely at `local` placement. `ota_pvt.py` carries the one-line
  policy change that would put each point on its own job as a comment, not as a
  claim.

## Owner-bound lifetime, exactly

Worth being precise about, because it carries more than it looks like it does.
The attempt protocol's crash window rests on it, and the TLA+ model in
[the attempt claim](../internals/attempt-claim-protocol.md) found that it — not
`discovery_is_authoritative` — is what closes that window.

Our half of the chain is tested for real: `exec/tests/test_fake_farm.py` kills a
submitter and asks the farm what became of its job, covering the job dying with
its client, the crash window resubmitting rather than attaching, and the watcher
seeing `PEND → RUN`. Both tests are mutation-checked — removing the
`PR_SET_PDEATHSIG` binding, or letting the fake report a dead owner's job as
running, fails them.

What that verifies is that *this* process binds its `bsub` client, that the
client's death propagates to the work, and that hedloom then reads the record
and the queue correctly. **Whether a real `bsub -I` ends its job when its client
dies remains LSF's promise**, and a fake cannot check somebody else's promise.
That is what step 1 of the ladder is for.
