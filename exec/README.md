# Hedloom Exec

Hedloom Exec owns one record for one planned invocation and its declared inputs,
with one or more tries beneath it. It is the durable half of
execution: the part that must survive the process, worker, and scheduler that
happened to start the work.

The problem it exists for is narrow and specific. Once a batch system accepts a
job, that job outlives whatever submitted it. If the submitting process dies
before learning the job's identity, a naive retry creates a duplicate and a
naive abandon loses it. Neither is acceptable on shared compute, so identity
has to live somewhere durable and be chosen *before* submission.

```python
from hedloom_exec import (
    AttemptJournal, InProcessTransport, attempt_identity,
    launch_or_attach, reconcile,
)

identity = attempt_identity(plan_id="plan-1", invocation_id="inv-a")
journal = AttemptJournal("attempts", identity.rendered)
transport = InProcessTransport({"double": lambda value: value * 2})

bundle = {"operation": "double", "arguments": {"value": 21}}
result = launch_or_attach(journal, transport, bundle)   # 'claimed'
state = reconcile(journal, transport)                   # 'succeeded'

# Running the same call again attaches or completes; it never reruns.
launch_or_attach(AttemptJournal("attempts", identity.rendered), transport, bundle)
```

Each record declares layout version 1 and holds `events.jsonl`, immutable
per-try manifests under `manifest/<n>.json`, and `standing.json` when evidence
has been selected for reuse. Try workspaces and batch jobs are named
`<record>-<n>`. All of these are readable without this package.

Storage policy is inspectable before it is destructive. A
`RetentionPolicy` contains named rules whose conditions narrow one another;
`prune.survey(record_root, policy, workspace_root=...)` reports candidate
tries, excluded tries, reasons, and measured reclaimable bytes without
creating or deleting anything. It never selects the standing result, a live
alias, a non-terminal try, or `unreconciled` evidence.

## Rerunning without repeating work

Declare what an invocation depends on and let the identity be derived from it.
Rerunning then skips work whose inputs are unchanged, and reruns work whose
inputs moved:

```python
from hedloom_exec.durability import Durability, execute
from hedloom_exec.reuse import input_digest, stale_attempts

bundle = {
    "operation": "solve",
    "command": ["solve", "-b", "tt.in"],
    "inputs": {"model": "sha256:aaa"},         # what the result depends on
    "identity_env": {"TOOL_ROOT": "/opt/toolchain/2026.1"},
}

execute(lsf, bundle, durability=Durability.RECORDED, root="attempts",
        plan_id="plan-1", invocation_id="inv-tt")
```

Queue, walltime, cores, and general `env` deliberately do **not** participate,
so retuning resources never invalidates a result. Change the model, and the
invocation lands on a new identity and reruns; the previous result stays on
disk and `stale_attempts(...)` can name it as superseded rather than having
quietly overwritten it.

This trusts your declaration. An operation that reads an undeclared file is not
honestly reusable, and no digest will notice.

Run the evidence with:

```console
python -m pip install -e .
python -m pytest -q
```

`tests/test_failure_injection.py` holds the observations that matter: a
substrate that accepts work and then loses the receipt, a controller that dies
between terminal status and recording it, and a site that cannot be asked
whether it accepted anything. The first two must resolve to exactly one job and
no rerun; the third must fail loudly rather than guess.

## The end-to-end slice

`plan_bundles(...)` turns an Hedloom Flow Plan document into content-addressed
bundles, so a rerun skips what is unchanged. Run it:

```console
PYTHONPATH=src:../flow/src python examples/planned_refinement.py
```

```
First run — nothing is published yet
  ran     point-medium   a6f0cf46bcb7
  ran     point-coarse   fc7add2a366f
  ran     point-fine     b9f4de332091
  ran     compare        634f63ac1fb4

Third run — the fine grid refined to 512 steps
  reused  point-medium   a6f0cf46bcb7
  reused  point-coarse   fc7add2a366f
  ran     point-fine     4d4cfd81fd6a
  ran     compare        6dc68ab1d541
```

The coupling is to the Plan *document*, not the package: nothing imports
`hedloom_flow`, so the base distribution stays dependency-free and any producer of
the same document works — schema 2 or 3, both of which
`plan_bundles` accepts. An invocation's digest changes exactly when
its own declaration or any ancestor's does, which is what makes the edited
point and its reduction rerun while the untouched points are reused.

## Outputs that are files

Most real commands write their answer to disk and print progress. Declare which
files matter; the rest stays as unnamed evidence:

```python
execute(
    lsf,
    {
        "command": ["solve", "-b", "point_tt.in"],
        "outputs": {"raw": {"path": "point_tt.out"}},
    },
    durability=Durability.RECORDED,
    root="attempts",
    workspace_root="/nfs/studies/sweep",
    plan_id="sweep",
    invocation_id="point-tt",
)
```

The attempt runs in its own directory under `workspace_root`, and the manifest
records each declared output's address, size, and modification time. On a shared
filesystem that is the whole of materialization — the next invocation opens the
same path, and nothing is copied. `result.address("raw")` gives it back.

Standard output always lands in `stdout.log` as diagnostics. It becomes a result
only if an operation declares `{"stream": "stdout"}`. A declared output that
never appears fails the invocation rather than publishing a manifest that points
at nothing.

For a planned invocation with an authored key, execution also maintains
`<attempt-root>/latest/<plan>/<authored-key>/<output>` as an atomic symlink to
each declared file output. It points at the workspace before launch, which
makes a fresh open useful while the file grows. The link may dangle until the
work writes; Hedloom never touches the target in advance. This stable view is
additional to the content-addressed record and does not change its identity.

The `created` event records the authored key, try, prior different-digest record,
and a digest for each identity key. `lineage()` uses those facts to explain
creation order while taking currentness from the alias, so edit → revert can
correctly make an older reusable record current again.

## Running on LSF

One selected invocation becomes one `bsub -I` job with its own name, resource
request, and exit status:

```python
from hedloom_exec.durability import Durability, execute
from hedloom_exec.lsf import LSFInteractiveTransport

lsf = LSFInteractiveTransport(
    defaults={"walltime": "30", "queue": "normal", "cores": 4}
)
execute(
    lsf,
    {"command": ["solve", "-b", "point_tt.in"], "cwd": "run/tt"},
    durability=Durability.RECORDED,
    root="attempts",
    plan_id="sweep",
    invocation_id="point-tt",
)
```

The constructor sets site defaults. What an individual job asks for comes from
the placement the Plan resolved for it, which the driver puts on the bundle:

```python
{
    "command": ["solve", "-b", "point_ss.in"],
    "placement": {
        "requested": {
            "name": "lsf-direct",
            "options": {
                "queue": "bigmem",
                "cores": 16,
                "memory_mb": 32000,
                "licences": {"spectre": 1},
            },
        }
    },
}
```

That job, and no other, is submitted with
`-q bigmem -n 16 -R rusage[mem=32000,spectre=1]`. A licence is *declared*, never
counted here: LSF knows how many exist and who holds them, so the request goes
to the scheduler that can arbitrate it — use the resource name your site
configured, and check it with `lsf_preflight.py --licence <name>`. An option
this transport cannot express as a `bsub` argument refuses before submission
rather than being dropped, because running the work without a resource it asked
for is not the same experiment. None of it reaches the input digest: retuning a
point's memory reuses the result it already produced.

Interactive submission is the mechanism, not a concession to human use: LSF
ties the job to the submitting client, so it cannot outlive the work that
wanted it. The client stays in this process's group and asks the kernel to
signal it if we die, which closes the one gap LSF does not. `-W` is mandatory
as the bound that survives everything else failing.

One job per invocation costs one queue dispatch, which is negligible for work
that runs for minutes and ruinous for work that runs for seconds. It buys a
per-invocation resource request, `bkill`, logs, accounting, licence arbitration by
LSF, and failure isolation. Short repetitive steps are what belong on a pooled
`dask_jobqueue.LSFCluster` instead; `LSFPooledTransport` marks that boundary and
currently refuses.

Concurrency is a separate matter: each simultaneously running job holds a
blocked client on the submit host, so the practical limit is your site's
per-user process and pending-job policy.

The subprocess layer runs for real against a fake `bsub`/`bjobs`/`bkill` on
PATH, and `tests/test_owner_bound.py` proves with real signals that a spawned
child dies when its owner is `SIGKILL`ed. What no local test can establish is
LSF's own guarantee that an interactive job dies with its client. Check that on
a submit host when you have one:

```console
python examples/lsf_preflight.py --queue normal
```

It verifies command availability, interactive admission, `bjobs -J` lookup, that
a composed resource requirement is admitted (add `--licence <name>` to include
one of your site's licence resources), and whether a running job actually
disappears once its client is killed. If the last check fails, the direct
mode's premise is wrong.

Worker pools, placement enforcement, retries, and graph scheduling are outside
this unit — see
[`ONTOLOME.md`](ONTOLOME.md) for the owned boundary and
[`DECISIONS.md`](DECISIONS.md) for what is settled, what is open, and what
would change our minds.
