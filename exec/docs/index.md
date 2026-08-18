# Hedloom Exec

Hedloom Exec owns the durable lifecycle of one attempt at one planned invocation.
It is the first unit in this repository to own any part of *execute*, and it
deliberately owns only the part that must survive a crash.

The design follows from one asymmetry. After a batch system accepts a job, that
job outlives the worker, executor, scheduler, and client that submitted it. A
transient handle therefore cannot be the authority for it. Attempt identity is
chosen from planning facts *before* submission, written durably before the
substrate is touched, and used afterwards to find work whose receipt was lost.

## What the record guarantees

Each attempt is a plain directory containing an append-only `events.jsonl` and,
once terminal, an atomically published `manifest.json`. State is always derived
by folding that record. Two orderings carry the recovery argument: submission
intent is flushed before any transport call, and the terminal record is written
only after the manifest is visible.

`launch_or_attach(...)` resolves to `claimed`, `attached`, or `completed` — or
raises `UnrecoverableAttempt`, which reports a substrate that cannot say
whether it accepted work. That exception is a supported result. Guessing in its
place is what produces duplicate farm jobs.

## Transports declare what they can answer

A transport moves one attempt to a substrate and reports observations. It never
decides readiness or releases successors. Its `discovery_is_authoritative` flag
governs the *negative* answer only: a positive match is always usable, because
the identity predates the submission that created it.

## What each transport is, and what it has met

| Transport | What it does | Evidence |
| --- | --- | --- |
| in-process | The honest degenerate case: accepted work cannot outlive its caller, so discovery is trivially authoritative. | Every test, and every domain study in this repository. |
| `hedloom_exec.lsf.LSFInteractiveTransport` | One `bsub -I` job per attempt, owner-bound through the `bsub` client. | Has reached a **real LSF installation**, through the sequential kernel: argv, `-J` identity, an artifact chaining between jobs, failure recording and reuse. Concurrency and the `bjobs` parser remain fake-only. |
| `hedloom_exec.lsf.LSFPooledTransport` | Nothing. A refusing boundary that names the seam rather than letting a caller reach a half-implementation. | — |

The name collision is worth stating plainly: **the pooled transport that works
is `hedloom_run.pooled.LSFPooledTransport`**, not this unit's. It adopts
`dask_jobqueue.LSFCluster` rather than reimplementing worker lifetime, and it
lives in `hedloom-run` because holding a second cluster open is a readiness
concern. This unit keeps only the refusal.

A Dask *transport* — one that submits attempts as Dask work — does not exist.
That is a different thing from `hedloom-run`'s Dask *kernel*
(`hedloom_run.graph`), which decides readiness over whatever transports this
unit provides and does not belong to this unit at all. See `DECISIONS.md` and
`hedloom-run`'s own documentation.

## Evidence

`tests/test_failure_injection.py` reproduces the two failure injections the
architecture named as decisive, locally, against a fake substrate whose state
outlives its caller. Both must resolve to exactly one job and no rerun. A third
test holds the boundary: reconciliation succeeds from a bundle carrying no
dependency information, so this unit has not absorbed graph scheduling
authority.

See [`DECISIONS.md`](../DECISIONS.md) for what is settled, what is open, and
what would change our minds.

```{toctree}
:hidden:
:maxdepth: 1

../DECISIONS
```
