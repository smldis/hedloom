# Hedloom

Author a study, see what it will do, and run it — from one file.

```python
from hedloom import Site, artifact, file, flow, local, operation, shell, study, sweep

DECK = artifact("spice-deck")

@operation(config={"temp_c": parameter(int)}, outputs={"deck": file("corner.cir")})
def write_deck(out, *, temp_c):
    out.deck.write_text(render(temp_c))          # the body really runs

@operation(inputs={"deck": DECK},
           outputs={"raw": file("corner.raw")},
           policy=lsf(queue="normal", cores=4, licences={"ngspice": 1}))
def simulate(deck, out):
    return shell("ngspice", "-b", "-r", out.raw, deck)   # runs at its placement

@flow
def sweep_corners(points):
    for point in sweep(points, key="key"):        # keyed scope per corner
        yield simulate(write_deck(temp_c=point["temp_c"]))

@study
def corners(points):
    return sweep_corners.named("corners")(points)  # records; nothing runs

subject = corners(POINTS)                         # planning, not spending
print(subject.summary())                          # nothing spent yet
run = subject.submit(site=Site.from_file("site.toml"), watch=True)
print(run["cold:simulate"].artifacts["raw"]["address"])
```

`examples/rc_corners.py` is the whole of that, runnable, against real ngspice:
three RC corners whose -3 dB frequency is analytic, so the answer can be
checked rather than believed.

## What this unit adds

Nothing that the three units below it could not already do — it removes the
seam between them. Before it, an operation body was dead code, and a study
needed a second file supplying implementations, command lines, output paths,
transports and roots; for the OTA reference that file is six hundred lines
whose only job is to agree with the first one.

- **The body is the implementation.** `@operation` here is `hedloom_flow`'s,
  wrapped so the function it already kept is remembered as callable. The Plan
  records `module:qualname` and a fingerprint of the source, so an edited body
  reruns the work it produced instead of relying on someone bumping `version`.
- **`out` is the attempt's own workspace.** A body writing `out.deck` writes
  where the executor will look, because both read one declaration.
- **`shell(...)` is a launcher.** Returning a command instead of running one is
  what lets it reach a placement: locally it is a subprocess, on `lsf` it is one
  `bsub -I` job with that corner's queue, cores and licence.
- **`@study` is the plan.** The decorated function *is* the study: calling it
  records the work and hands back something inspectable, and `submit` is the
  only thing that spends. A `@flow` is the same shape one level down, which is
  why there is one thing to learn rather than two.
- **`sweep(points, key=...)`** names every call inside the loop, so reuse cannot
  be lost to renumbering — the trap that made unnamed invocations dangerous.
  `.named("...")` does it by hand for a single call.
- **`Site`** holds what is not the study: placements, roots, address spaces,
  threads. From TOML, with relative paths anchored to the profile.

## What it does not change

`hedloom-exec` still owns one attempt's durable record and imports neither this
package nor Dask. `hedloom-run` still owns binding and readiness and both of its
kernels, so a session is the same run with a scheduler deciding readiness rather
than a loop. Reuse, identity, placement, licences, the watcher — untouched. This
unit composes; it does not reimplement.

```console
PYTHONPATH=src:flow/src:exec/src:run/src python -m pytest -q
python examples/rc_corners.py
```

## Farm sweep test

After `hedloom/exec/examples/lsf_preflight.py --queue reg` passes, exercise the
complete plan-to-record path without a simulator:

```console
python examples/farm_smoke.py examples/farm-smoke.site.toml
```

The Plan sweeps four points, each with explicit `start` and `count` parameters.
For each point one `/bin/sh` command generates a numeric file and a second
POSIX-shell command consumes it, producing eight visible `bsub -I` jobs and four
deterministic summaries. It then submits the same Plan again and
requires all eight invocations to be reused without new jobs. Results live under
`examples/_runs/farm-smoke/`. The profile explicitly requests queue `reg`, one
core per job, and a one-minute walltime; copy the TOML and change those site
facts when needed.

Both submissions run inside one session, which is the whole of what a study
author has to hold:

```python
with session(site, watch=True) as farm:
    first = farm.submit(subject)
    second = farm.submit(subject)      # reuse, same cluster, same watcher
```

The session owns the cluster, the client and one queue watcher, and gives them
back when the block ends — which matters, because under owner-bound lifetime
leaving it ends any farm job still in flight. There is no kernel to choose:
capacity is the site's, and a site that declares none has capacity one. Ask for
`sequential=True` to run one at a time with no scheduler at all (this is what
keeps `distributed` an optional extra), or `locally=True` to debug the whole
study in this process without touching the farm. Either can be narrowed for a
single run without a second profile:

```python
with session(site, {"placement": {"lsf": {"max_jobs": 1, "queue": "express"}}}) as farm:
    ...
```

An override changes how a run executes and never what it means, so an overridden
run lands on the same attempt identities and the two reuse each other's work.

## Two studies at once

What the first test does not ask is what happens when something else is already
running. `examples/farm_multi_client.py` does, using the same operations:

```console
python examples/farm_multi_client.py --queue reg --max-jobs 2
```

Its site is built in Python rather than read from a profile — the other half of
the smoke test. A profile is right when a queue, a walltime and a farm share
belong to an installation and get copied per site; here the site *is* the
experiment, three arrangements differing in one declared number, so arguments
are both shorter and more honest than three TOML files.

Each arrangement is measured from the attempt journals rather than from the
process that started the work — one interval per job, between the
`submit_intent` written before the transport is touched and the receipt written
when `bsub -I` returns:

* **One session, two studies.** A session is one cluster, and a placement's
  budget belongs to that cluster's workers, so `submit_all` cannot put more on
  the farm than the site declared however many studies it is given. Eight jobs
  are wanted, `max_jobs` is two, and no more than two are ever in flight.
* **One session, the same study twice.** Dask keys belong to the scheduler, so
  identical work submitted twice is one task: four jobs, not eight. The attempt
  claim is never consulted here — there is only ever one caller — so this is
  Dask's idempotence, not hedloom's. Both reports say `claimed`.
* **Two sessions, the same study.** Different key namespaces, so both callers
  really do reach the attempt protocol and the journal claim is what prevents
  the duplicate. The loser is refused by name rather than made to wait. This is
  also the arrangement where the cap does *not* hold: each session has its own
  cluster and therefore its own budget, so two controllers can put twice
  `max_jobs` on the farm.

A fourth pass resubmits all of it from one session and must spend nothing.
`tests/test_farm_multi_client_example.py` runs the whole thing against the fake
`bsub`, checking the same numbers from the submission records rather than from
the journals, so the two instruments have to agree.
