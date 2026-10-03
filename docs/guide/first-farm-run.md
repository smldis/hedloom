# Your first run on a real farm

Everything else in this guide works the same whether a placement is `local` or
`lsf`. This page is about the one transition where that stops being true: the
first time a study spends queue time on a farm nobody here can test against.

Read it before that run, not after.

## First, the records directory

The ladder below is about your queue. This is about the shared filesystem that
holds `records_dir`, and it comes first because nothing on the queue can repair
a root that cannot hold a lock.

**What Hedloom assumes.** A records directory on a filesystem where `flock()` both
reaches the server and stays owned by the open file description that took it.
The one deployment this prototype has been measured against is NFSv3 mounted
`local_lock=none` with `nlockmgr` registered, and on that root both hold. If
your root is local disk, all of this is true by construction and you can skip
to the ladder.

**Why it matters.** The claim on `attempts/<record>/claim.lock` is what makes
"exactly one writer" true. A lock that silently fails to exclude does not just
produce two `bsub` jobs for one identity — `events.jsonl` is appended with
`O_APPEND`, which NFS does not make atomic, so the durable record every reuse
and recovery decision is read back from can interleave. It starts lying.

**If you move to a different farm, or a different root, check three things.**
None needs anything installed.

```console
findmnt -T /path/to/records -o FSTYPE,OPTIONS | tr ',' '\n' |
    grep -E 'nfs|vers|local_lock|acdir'
```

`local_lock=flock`, `local_lock=all` or `nolock` means the lock never leaves
the host: two submit hosts will both acquire it and neither is told. On NFSv3,
`rpcinfo -p <server> | grep nlockmgr` should find the lock manager.

```console
cd /path/to/records
exec 8>probe.lock ; exec 9>probe.lock
flock -x -n 8 && echo "fd 8 acquired"
flock -x -n 9 && echo "BAD : fd 9 acquired too" || echo "GOOD: fd 9 refused"
exec 8>&- ; exec 9>&- ; rm -f probe.lock
```

Two descriptions in one process must exclude each other, or the claim stops
separating threads of one runner. Read this asymmetrically: `GOOD` settles it,
`BAD` may also be an artifact of the probe rather than a real defect.

```console
# host A
flock -x /path/to/records/probe.lock -c 'echo held on $(hostname); sleep 30'
# host B, while A is holding
flock -x -n /path/to/records/probe.lock -c 'echo ACQUIRED'; echo "exit=$?"
```

Host B must print nothing and exit 1. This is the expensive one to arrange and
the only one that tests what actually costs money, so do it before two people
share a root. **It has never been run here** — see
*What has never met a real farm* below.

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

Concurrency is the profile's, so `max_jobs = 1` means one active wrapper at a
time. To separate controller problems from `bsub` problems, use the transport
preflight. The async Runtime
has no sequential mode; use the standalone Exec preflight to inspect the direct
transport, then the maintained smoke example to test the whole path.

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

Pools require authenticated Dask execution connections by default. Before this
step, make sure `records_dir` is visible at the same path on the submit host and
every farm node, and that private directories and files enforce the owner's
permissions across hosts. `records_dir` and its ancestors must also prevent
another user replacing those paths; check filesystem ACLs and UID mapping as
well as the Unix permission bits. Hedloom generates a private temporary credential
directory (`0700`) and credential files (`0600`) there. Install the `pooled`
extra on the submit host; no manual certificate setup is needed. The worker
environment must support Dask TLS connections.

Keep the default `authentication = "tls"` in the pooled placement. Failure to
establish it must fail startup. `authentication = "none"` is an explicit
per-pool opt-out allowing unauthenticated network execution. Dashboard exposure
is separate and does not change authentication. See [pool authentication](sites.md#pool-authentication)
and [the security boundary](../internals/execution-security.md).

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

- **The async Runtime and concurrent executor.** Nothing has yet put this
  replacement in front of a real queue,
  so concurrency, `max_jobs` as a real bound, and failure isolation between
  branches are exercised only against a fake `bsub` and a real client fixture.
- **Authenticated pools across real farm hosts.** Local/fake-farm security checks
  do not establish that the farm preserves private credential permissions,
  shares paths consistently or admits the required TLS connections. The
  pooled step above checks those deployment assumptions.
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
- **`flock` on a records directory over NFS, between two hosts.** The claim in
  `hedloom_exec.journal.AttemptJournal.claim` is the weakest load-bearing
  assumption in the durability argument. Part of it was measured on 2026-08-28
  — on NFSv3 mounted `local_lock=none` with `nlockmgr` up, two open file
  descriptions in one process do exclude each other — and that root is what
  *First, the records directory* above assumes. **Two hosts contending has still
  never been run**, and a mount that permits cross-host locking is not
  evidence that it happens. Its `DEVNOTE/TODO` carries the detail.
- **Attribute caching against a shared root.** A record is published complete
  rather than built in place, so no caller on one host can meet a half-built
  one. Nothing has measured how long a *second* host may keep serving a stale
  directory listing, which can produce the same symptom from a different
  cause.
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


For pooled placement, local fake-farm probes verified normal process-exit
reclamation. The initial SIGTERM/SIGKILL probes saw active commands and batch
workers alive after five seconds. Extended probes on 2026-10-02, using
Dask/distributed 2026.7.1, observed automatic reclamation after 32.45 seconds for
SIGTERM and 32.50 seconds for SIGKILL, without issuing `bkill`. The entered
command would otherwise have run for 60 seconds.

Worker logs show immediate detection of scheduler loss and the start of shutdown.
Dask's `Worker.close` defaults to a 30-second grace while joining active executor
threads; worker exit then reclaims the command through its Linux parent-death
binding. The earlier five-second check ended before that grace expired. This is
measured delayed cleanup, rather than evidence of permanent orphaning.

Batch allocations still have no direct OS parent-death binding to the submitter.
These timings establish one local fake-farm case, not a deadline under network
partitions or a real-farm guarantee. Connectivity, worker shutdown and farm
walltime remain part of that boundary.
