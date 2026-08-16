# Developer note: placement, clustering, and scheduling

Status: description of the current implementation. The final section points to
the register entry that records the placement-aware concurrency decision.

## The three concepts

```text
placement   where an operation runs and what resources it requests
clustering  the controller workers available to launch ready operations
scheduling  which ready operation starts next, and when
```

They form this chain:

```text
                              Plan
                operations + dependencies + policy
                                |
                                v
                       readiness kernel
                   +------------+------------+
                   |                         |
             sequential loop            Dask client
             one at a time          several ready tasks
                   |                         |
                   +------------+------------+
                                |
                                v
                      placement selection
                 placement name -> Site transport
                       |                  |
                       v                  v
                     local         lsf-interactive
                       |                  |
                       v                  v
                local execution        bsub -I
                                          |
                                          v
                                    LSF scheduler
                                          |
                                          v
                                    compute node
```

## Placement

Placement is authored into the Plan. It is data, not an act of submission:

```python
@operation(policy=lsf(queue="reg", cores=1, walltime="30"))
def simulate(...):
    ...
```

The Plan records the policy name (`lsf`) and its options. Policy precedence is:

```text
call override -> operation default -> plan default -> local
```

The Site maps the resolved name to a concrete transport:

```toml
[placement.lsf]
kind = "lsf-interactive"
queue = "reg"
cores = 1
walltime = "30"
```

Site values are transport defaults. Supported options authored on an
invocation override those defaults. The LSF vocabulary is `app`, `cores`,
`licences`, `memory_mb`, `queue`, `resources`, and `walltime`. The transport
renders them as `bsub` arguments; for example, `app = "spectre"` becomes
`-app spectre` and `cores = 1` becomes `-n 1`.

### Named placement routes

Different operations can name different transports:

```python
regular = named_policy("regular")
large_memory = named_policy("large_memory")

@operation(policy=regular(cores=1))
def prepare(...):
    ...

@operation(policy=large_memory(cores=8))
def solve(...):
    ...
```

```toml
[placement.regular]
kind = "lsf-interactive"
queue = "reg"
walltime = "30"

[placement.large_memory]
kind = "lsf-interactive"
queue = "bigmem"
walltime = "120"
```

`lsf(...)` is convenience syntax for the policy name `lsf`;
`named_policy(...)` provides additional route names. At execution time both
kernels call the same `select_transport` function. An unavailable placement is
refused rather than silently run elsewhere.

Placement routing currently chooses transports. It does not route a task to a
separate Dask cluster or Dask worker class.

## Clustering

In this codebase, the Dask cluster is a controller cluster. It is not the LSF
compute farm. A Dask task prepares one invocation, selects its transport, and
may then wait in `bsub -I`; LSF still chooses and manages the compute node on
which the payload runs.

```text
Dask worker thread
    -> prepare invocation
    -> select placement transport
    -> build command
    -> call bsub -I when the transport is LSF
    -> wait for and record the result
```

Hedloom never silently creates a Dask cluster. The caller must provide a
`distributed.Client`:

```python
with cluster, Client(cluster) as client:
    result = subject.submit(site=site, client=client)
```

The current helper builds one local worker with multiple threads. A profile may
declare its global controller capacity and dashboard exposure:

```toml
[kernel]
threads = 10
dashboard = "none"
```

This thread count limits all Dask tasks on that worker. It is not a
placement-specific LSF limit.

## Scheduling

There are two scheduling decisions.

1. The Hedloom kernel, or Dask on its behalf, determines dependency readiness.
2. After an LSF invocation is submitted, LSF determines when and where the job
   runs and arbitrates queues, cores, memory, licences, and user limits.

### Sequential kernel

```python
subject.submit(site=site)
```

No Dask client is present. Hedloom walks the Plan one invocation at a time,
waits for each `bsub -I`, records its result, and only then considers the next
invocation. With the default failure behavior, work after the first failure is
reported as blocked.

The current farm smoke test uses this kernel. It validates LSF submission,
artifact chaining, failure recording, and reuse. It does not validate Dask or
parallel farm submission.

### Dask graph kernel

```python
subject.submit(site=site, client=client)
```

Hedloom creates one Dask task per invocation and dependency edges matching the
Plan. Dask may run independent ready branches concurrently. A failed task
blocks its dependants while independent branches may continue.

Placement is selected inside each Dask task. All tasks submitted to the client
currently share that client's worker and thread limits, regardless of their
placement.

## Concurrency limits

The decision and current mechanism are recorded under *Two concurrency limits,
not one* in [`open-concepts.md`](../../docs/vision/open-concepts.md).
