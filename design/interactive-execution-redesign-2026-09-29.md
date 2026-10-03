# Interactive execution: a replacement concept

Date: 2026-09-29. Status: concept development; no replacement implemented.

## Accepted direction

The operator needs to launch studies at different times from an interactive
Python environment while earlier runs continue. Resource ownership and limits
must be explicit.

The user explicitly wants to redevelop this concept without maintaining support
for the current blocking implementation. This removes backward compatibility
with that implementation as a design requirement. There is no obligation to
retain its API, a blocking adapter, the direct sequential backend, or two engines.
The existing code remains evidence to inspect, rather than the shape the
replacement must preserve. This direction does not authorize deleting saved
study evidence.

Alternative 3 (a runtime in a separate process) is deferred to future
consideration by user direction and excluded from the current discussion.

The user discarded alternative 1 (caller-owned event-loop integration) for
complexity. Alternative 2, a runtime-owned async event loop on a dedicated
background thread, is consequently the working direction. Executor choice,
API spelling, admission rules, and shutdown policy remain open. Questions about
those details are not decisions against them. This document develops the concept;
it does not change the currently available execution behavior.

## Proposed model

Start with three concepts, whose names are provisional:

- A **study** describes the planned work and its implementations.
- A **runtime** owns execution resources and the runs admitted to them.
- A **run** identifies one accepted submission and exposes its progress,
  evidence, completion, and result.

Submission has an acceptance boundary: either preparation fails visibly, or the
caller receives an identifiable run that the runtime owns. Acceptance does not
mean that an invocation has started. Later submissions can join the same runtime
and share its declared execution capacity.

Waiting is an explicit operation on a run. A script can wait for completion;
an interactive caller can retain the run and continue working. Neither requires
retaining the old blocking engine or its calling convention. Interrupting a
wait, requesting that a run stop, closing a runtime, and losing its host are
different events whose effects need to be specified.

The leading architectural hypothesis is one cooperative async orchestration
engine. Each run has state and yields while waiting for execution, rather than
occupying a controller thread for its whole lifetime. Preparation, admission,
completion, shared execution, withdrawal, and reporting must have one account.
The runtime owns a fixed controller thread hosting the async engine. Individual
runs have async tasks; their number does not determine the controller thread count.

## Resource and lifetime questions

| Resource or boundary | What the design must make explicit |
| --- | --- |
| Runtime host | Who opens and closes it; what keeps it progressing between commands; behavior when the interpreter exits. |
| Active runs | Admission limit or an explicit unbounded policy, memory cost, and whether excess submissions queue or refuse. |
| Ready invocations | Capacity per execution placement, shared across runs, and any outstanding-work queue. |
| Blocking work | Where synchronous bodies, transports, fingerprinting, and persistence run; their concurrency and effect on controller responsiveness. |
| Shared computation | Which runs consume it; stopping one run must account for remaining consumers. |
| Shutdown | Whether admitted work drains, withdraws, or is stopped; when resource release is complete. |

Async tasks still consume resources. Moving blocking work to an implicit default
thread pool would leave part of the resource model unspecified. Conversely,
using an explicitly owned, bounded thread resource is not inherently inconsistent
with this direction. The need and the cost must justify it.

## Runtime hosting direction

**Alternative 2: runtime-owned event loop on a fixed background thread.** A bounded
controller resource can progress independently of prompt input, while sharing the
process and GIL. This requires explicit startup, shutdown, cross-thread
interaction, and treatment of blocking controller work.

Alternative 1 was discarded by the user for integration complexity. Supporting
the caller's event loop is therefore outside the current design.

Dask is an existing candidate for execution capacity and completion delivery,
not an automatic requirement for the new concept. Evaluate whether its existing
runtime can host coordination safely before introducing another loop. Replacing
Dask, making it mandatory, and retaining a direct executor are separate choices;
none is justified merely by the existence of the current APIs.

The current thread-per-study approach is useful as a measurement baseline, but
building a supported compatibility layer around it is outside the chosen
redesign direction.

## Evidence to carry into the inquiry

At Hedloom `e9dbe70`, both `run_plan` and `run_plan_graph` call the same synchronous
`_run_ready` controller. There are not two independent orchestration algorithms
to replace. Direct execution calls the invocation inline; Dask execution
dispatches it with placement resources. `Session.submit_all` creates a thread
per submission and joins them.

Existing tests provide scenarios for staggered launches, sharing compatible
live executions, per-consumer history, generation-safe release, and withdrawal.
They supply behavioral questions for the new concept. Their API details are
not compatibility requirements, and observed behavior is not automatically a
requirement to preserve.

Useful proposed commitments are inspectable plans, explicit placement limits,
truthful execution evidence, understandable reuse, and independent attribution
for each submission. Current boundaries can change if the design explains how
it still serves those needs. Runtime implementation replacement does not by
itself call for changing authoring or computation identity.

## First experiment

Build one temporary vertical slice around the chosen controller candidate:
accept run A, hold an invocation, accept run B later, and observe both while the
prompt is idle. Then compare behavior during a blocking foreground command.
Measure controller resources and check that the shared placement budget holds.

Exercise a shared invocation, an independent failure, an interrupted wait,
withdrawal by one consumer, and shutdown with outstanding work. Include a slow
preparation or persistence operation to reveal whether it stalls other runs.
Use controlled barriers and recorded execution events rather than timing alone.

The experiment should decide whether the candidate earns adoption and identify
its limitations. A subsequent implementation plan should explicitly retire the
old execution path and update maintained callers, tests, docs, and ontologies.
No permanent dual-engine support or legacy API adapter is planned by default.

## Sources and verification

- User direction, 2026-09-29: staggered interactive launches; open brainstorming;
  clean concept redevelopment without support for the current blocking implementation.
- [Current Session](../src/hedloom/session.py),
  [shared controller](../run/src/hedloom_run/graph.py),
  [direct entry point](../run/src/hedloom_run/driver.py), and
  [staggered-consumer tests](../tests/test_shared_execution.py).
- [AiiDA process launching](https://aiida.readthedocs.io/projects/aiida-core/en/stable/topics/processes/usage.html):
  daemon submission returns a persistent process node; blocking local execution
  and daemon ownership are distinct contracts.
- [IPython autoawait](https://ipython.readthedocs.io/en/stable/interactive/autoawait.html)
  and [event-loop integration](https://ipython.readthedocs.io/en/stable/config/eventloops.html):
  terminal and IPykernel loop behavior differ.

Source and documentation inspection only. No new runtime, local IPython
experiment, or real-farm concurrency verification accompanies this concept.
