# Discoverability architecture reassessment — 2026-09-06

Status: architectural proposal with local probes; not a replacement implementation.
PR #20 remains unchanged. This follows the review of its per-submission Dask UUID
and the user's authorization to reconsider any Hedloom layer.

## Recommendation

Make an execution handle an explicit object owned by one running Session. Let
compatible, overlapping consumers of that Session refer to the same handle.
Persist each consumer's binding before admitting its work, and publish the
actual Exec selection once beneath the handle. Discovery follows this recorded
relationship on demand, without a scheduler connection or background observer.

Implement the executor/handle mechanism in Run; the facade Session owns its
lifetime and exposes consumer history. Flow remains static. Exec retains
computation identity, try selection, claims and transport ownership.

This is a recommendation for the next implementation slice, not a claim that
the probe has settled cancellation, graph binding, or crash durability. Those
are explicit acceptance gates below.

## Requirements that must survive simplification

- Essential capabilities work for scripts, CI and agents without an interactive
  session: composition-root `MANIFESTO.md`, “Keep engineering work under the
  operator's control”.
- Exact live record/try and workspace lookup works from a fresh process. An
  unavailable result has an explicit reason; an active candidate found by
  scanning is never substituted for the selected try.
- Already supported overlapping identical submissions in one Session should
  retain shared execution and both obtain their results. Enabling history must
  not turn that arrangement into additional `ConcurrentClaim` failures.
- A later submission can reuse successful evidence. A previous run's reference
  does not change when a subsequent submission selects a new try.
- History failure remains distinct from computation failure. Initial history
  setup refuses before execution; later observation failures report degradation.
- No mandatory observation tasks or polling loop for every cheap operation.

The manifesto establishes automation, not a specific distributed joining
protocol. The implementation plan supplies the exact live-discovery requirement.
Exec's maintained decision ledger separately leaves cross-request joining open.
These are different commitments.

## What the current code actually does

`exec/planned.py:plan_bundles` determines the computation digest before scheduling.
Upstream declarations contribute identities; actual upstream values and paths
are bound later. The record can therefore be named before a body runs.

`exec/attempt.py:launch_or_attach` holds the record claim while selecting a
reusable try, recovering an accepted submission, or allocating and launching a
new try. Both the facade's bound transport and direct `bsub -I` can block inside
`submit` until the body/job finishes. Try selection cannot be moved ahead merely
by moving a read: that would replace an authoritative decision with a snapshot.

`run/graph.py:_run_one_here` sends the selection to the sink in `_RunConfig`.
`history.py:SelectionSink` writes directly into one consumer's history. Dask
sharing executes one task configuration, not every consumer's side effects.
Final results can reach both consumers while live history reaches only one.
The PR's UUID workaround avoids sharing and exposes the second Exec claim.

The current readable `_task_key` is also not a complete execution-equivalence
contract: it includes operation, authored key and eight digest characters, but
not all roots, transport configuration, placement or dependency bindings. A
redesign must not simply promote that key into an authoritative sharing key.

There is an older proposal to bind attempt identity before scheduling. It
predates the present shared-record model and relies on obsolete selection
machinery. Its diagnosis is useful; moving its old code is not a current fix.

## The proposed objects and information path

There are three distinct references, not three competing computation identities:

| Reference | Meaning | Authority |
| --- | --- | --- |
| Run + invocation | A consumer request | Facade history |
| Execution handle H | One admitted call into Exec, possibly shared by compatible consumers | Run executor, owned by Session |
| Record R + try n | The execution evidence selected by that call | Exec |

H exists before the selected try does. Allocating H does not reserve an Exec
try, create a workspace, submit a transport job, or promise that dependencies
will succeed. An internal opaque ID for H is not the operator-facing run name.

```text
A.1 / work.1 -- durable binding --> H <-- durable binding -- B.1 / work.1
                                   |
                        one admitted call to Exec
                                   |
                        actual selection R#3
                                   |
                      H/selection + H/workspace

fresh-process query: A.1/work.1 -> H -> R#3 -> recorded workspace
```

The controller contains one synchronized active-handle table, owned explicitly
by the Session's executor. It is not a table that separate controllers must
agree to update, and there is no rendezvous requiring simultaneous arrival.
The table is ephemeral execution bookkeeping; durable bindings and receipts
remain sufficient for read-only discovery after it disappears.

1. A requests work. The executor creates H, persists A's binding, and arranges
   its Dask task and dependencies. The worker receives H's publication location,
   not A's private sink.
2. Exec selects R#3. The worker records that selection and its workspace beneath H
   before invoking the body. No controller needs to relay the notice.
3. B arrives later while H remains shareable. The executor returns H and persists
   B's binding. B's query reads the already-published notice immediately.
4. The shared result produces a separate invocation outcome for each consumer.
   Consumer invocation IDs must be applied here, not copied from the first
   consumer's `_Step`.
5. Once H is terminal, a new request creates a new handle. Exec then decides
   reuse or a new try. Successful evidence remains reusable; failed evidence is
   not accidentally cached forever by a retained Dask key.

Sharing must require compatible bound execution, not merely the same record.
For the first slice, restrict it to identical bound requests and dependency
handles within one Session. Different placements, roots, source bindings or
transport configurations must not share by accident. Conservative nonsharing
outside that defined set retains Exec's existing contention behavior.

## Deliberate scope reductions

- No live joining between independent Sessions or controllers. Their completed
  result reuse and existing claim refusals remain. Merely using the same Dask
  client does not confer shared ownership: advanced callers need an explicit
  executor context or isolated task namespaces.
- No preallocation of every downstream try. A dependency-blocked invocation has
  no selected try. H may exist as a request/dispatch reference without a try.
- No scheduler reconstruction or automatic worker replay in the first slice.
  Re-entering an already-started handle must fail before another call to Exec;
  it must never change the meaning of an existing H-to-try reference. A new
  explicit submission gets a new handle and uses normal Exec policy.
- No automatic retries, cross-host leases, independent consumer failover or
  new result-dependent planning.
- No operation opt-in initially: workspace discovery is naturally unavailable
  when no workspace exists. Value-only work can still need exact provenance.
  A future lightweight/ephemeral path must expose its capability explicitly.

“No worker replay” needs an enforced entry guard, not just `retries=0`:
Dask may recompute work after losing a worker. The guard is execution control
owned by Run, separate from the best-effort selection observer. Ordinary
observer exceptions must not be repurposed as the guard; Exec catches them.

## Cancellation is a required part of this slice

A run withdrawing interest must not blindly call `Client.cancel` on a shared
future: two Python Future objects on the same Client do not represent two
independent owners. Withdrawal changes the consumer's accounting. If other
consumers still need H, execution continues for them.

Last-consumer cancellation and the start/cancel race need a durable admission
decision coordinated with the worker entry guard. They must distinguish an
unstarted request, an execution that actually entered Exec, and a consumer
that stopped waiting. The existing `_stop_admitting` stack-snapshot race is
relevant; a cancelled Dask future is not proof that the body never ran.

The first implementation must test this interaction; the probe below only
demonstrates withdrawal while another consumer remains. A request binding alone
does not establish final consumption or success, and history must preserve that
distinction when a consumer stops early.

## Alternatives reconsidered

| Alternative | What it buys | Why it is not the recommendation |
| --- | --- | --- |
| Scan record tries on demand | Almost no runtime machinery | Cannot establish an exact consumer-to-try relationship in general. Useful secondary browser only. |
| Preserve UUIDs and wait on record claims | Keeps per-consumer sinks | Changes contention and failure semantics; blocking waiters can consume placement slots and interact with nesting. Controller-side waiting would itself add admission machinery. |
| Shared selection-allocation task in Dask | Ordinary dependencies distribute a common reference | Extra tasks; reference recomputation and cancellation still need ownership rules. Successful delivery alone does not settle lifecycle. |
| Split Exec into reserve/select and launch | Exact try before launch | New durable reservation, launch ownership and crash/recovery states; premature downstream reservations. Worth doing if execution semantics independently need it, not just to route history. |
| Nonblocking transport API | Could shorten claims and expose accepted handles earlier | Reworks local bodies, Shell delegation, LSF lifetime and resource accounting. Early selection still needs consumer attribution. |
| Session-owned execution handles | One place already owning scheduling lifetime can own sharing and references | Moderate Run/history rework; cancellation and replay guards must be explicit. This is the preferred bounded direction. |

Moving responsibilities is allowed, but the least speculative boundary change
is to make the Session's execution ownership explicit in Run. It does not require
moving readiness to Flow or putting Dask inside Exec.

## Evidence and its limits

The companion probe uses real `distributed==2026.8.0`, real local Exec and an
in-process cluster. It is deliberately not an implementation of Hedloom's graph
kernel. Run from the Hedloom directory:

```sh
PYTHONPATH=src:flow/src:exec/src:run/src python design/probes/discoverability_reference_probe_2026_09_06.py
```

Recorded output is in `design/probes/discoverability_reference_probe_2026_09_06.json`.
Assertions verify ten observations:

- Three successful reference-task cases: late arrival finds the same exact try
  before completion; one body serves both consumers; fully released tasks get
  a new reference while Exec reuses the successful try.
- Three counterexamples: replaying allocation changes its generated reference;
  cancelling one same-client future cancels its peer; replaying failed execution
  selects a new try while an observer-only immutable receipt retains the old one.
- Four bounded-owner cases: late consumers share exact live evidence and A can
  withdraw without cancelling B; the entry guard refuses replay before Exec;
  a later successful request uses a new handle and reuses the old try; a later
  failed request receives a new try without changing either historical binding.

The replay tests explicitly call `Client.retry` to exercise re-entry; they are
not worker-loss simulations. Expected body failures and one guarded replay
emit diagnostics. All ten observation assertions pass.

The probe's local exclusive file is not a production durability protocol. Its
compatibility key is only valid for the fixed probe function. It omits actual
graph dependencies, facade integration, last-consumer cancellation, entry/cancel
races, multi-host storage and killing a real controller/worker. No full-suite
pass, farm validation or performance benchmark is claimed by this reassessment.

Official Dask behavior checked alongside the installed source:
[future dependencies](https://distributed.dask.org/en/stable/manage-computation.html),
[future retention](https://distributed.dask.org/en/latest/memory.html), and
[recomputation after worker loss](https://distributed.dask.org/en/latest/resilience.html).

## Cost and implementation gates

This is a moderate architectural change, chiefly in Run's graph submission,
execution result mapping, cancellation and the facade's history reader/writer.
Flow's Plan schema and Exec's computation identity need not change. Sequential
execution should use the same handle/publication contract without requiring Dask.

Per unique active execution: one normal execution task, a handle allocation and
small durable metadata writes. Per consumer: a binding and normal final
accounting. Per query: direct bounded metadata reads plus the requested Exec
journal. There is no additional observer worker, perpetual poller or copy of
computation payloads. Entry guards add filesystem coordination; registration
serializes briefly on the controller. Actual latency and metadata cost remain
unmeasured. The probe's speed is not a benchmark.

Before replacing the UUID workaround, require an integrated vertical slice:

1. Two overlapping identical Plans, including downstream dependencies, both
   succeed with one body/job per shared invocation. Fresh-process CLI queries
   find both exact workspaces before a barrier is released.
2. Staggered arrivals before selection, after selection and after completion;
   later failures and successful reuse preserve all earlier references.
3. Per-consumer IDs/outcomes remain correct; incompatible bindings do not share.
4. Consumer withdrawal, last-consumer cancellation and start/cancel races produce
   truthful reports and do not cancel work another consumer needs.
5. Controller loss leaves readable exact references and incomplete consumer
   accounting. Worker re-entry cannot select a second try for an existing handle.
6. Both kernels, nested capacity, placement budgets, history failure behavior,
   retention and named output resolution retain their contracts.

Stop if meeting those gates requires distributed joining or speculative recovery.
Those would be a separate architecture decision, not a hidden dependency of
discoverability. The current PR should not merge with the UUID behavior while
this replacement is unresolved.
