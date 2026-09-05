# Hedloom Flow Ontology

This is the ongoing self-study of the component rooted here. Briefly inhabit
its perspective as you work: what are you learning about what it is, why it
exists, and what it might become? Help this account evolve when you have
something useful to add.

## Purpose and scope

Hedloom Flow owns generic, Python-authored definitions of operations and reusable
static flows, plus the immutable normalized Plan IR produced by explicit
planning scopes. It makes planned invocations, dependencies, nested flow
boundaries, policies, artifact contracts, and named outputs inspectable before
any execution boundary. It also owns data-only declarations for addressed
external sources: an opaque address, an artifact contract, and fixed
source/output reference value classes.
It also owns one non-reexported experimental instrument that lowers a validated
Plan, explicit implementation registry, and injected decoded source mapping to
inspectable Dask Delayed values. This instrument tests Plan sufficiency; it is
not a public or general execution surface.

## Mode of being

**Development state:** `prototype`

The current runnable API studies whether ordinary Python authoring can produce
one deterministic, executor-neutral graph while retaining explicit contracts
and nested flow structure. Its tests and tool-free example provide
evidence for ordered collection fan-in, scoped authored Plan identity, the
static distinction between addressed artifact sources and ephemeral operation
outputs, and a bounded local Delayed lowering of those contracts.

Inspectability, immutability, early validation, and the separation between
planning and runtime authority are chosen commitments. Repeatable graph
construction also relies on the author's flow-building Python making the same
choices from the same declared context; the API does not enforce that discipline.
The Delayed instrument supports Plan sufficiency for its bounded case. Whether
the same declarations remain a useful handoff as consumers' needs evolve is an
open hypothesis, not settled by retaining the instrument inside this unit.

## Current contracts

- Distribution: `hedloom-flow`, independently installable on Python 3.10 or newer;
  the base package has no dependencies, and the optional `dask` extra pins
  `dask==2026.7.1` for the experimental instrument.
- Python API: `hedloom_flow` exposes immutable planning model values and the
  `@operation`, `@flow`, `plan(...)`, `input_artifact(...)`, policy, and
  contract-authoring surfaces.
- `hedloom_flow.authoring.file(...)` and `directory(...)` declare filesystem
  output shape separately from their `kind=` artifact-contract label. The
  shape is preserved in the Plan output binding, so an executor can require
  what was authored without giving filesystem meaning to a semantic artifact
  kind.
- Authored operation calls are legal only in an explicit planning scope, and
  operation bodies do not execute during planning.
- Flow bodies are ordinary authored Python that constructs a static graph;
  avoiding external side effects in those bodies is an authoring responsibility.
- A planning handle carries a reference and an artifact kind, never a value.
  Reading one as a value is refused: `HandleUsedAsValue` — both an
  `AuthoringError` and a `TypeError` — is raised for truth-testing and for
  equality, the two readings Python would otherwise answer silently. Ordering,
  arithmetic, iteration and attribute access already raise. Handles remain
  hashable by identity, which is the identity under which repeated source
  declarations are shared.
- Plan IR is immutable, validates operation bindings and artifact dependencies,
  preserves nested flow boundaries, and provides deterministic plain-data and
  JSON inspection.
- The emitted Plan schema is **3**, and it is the one version a `Plan` will
  validate: a document declaring anything else is refused by version rather
  than read as though the difference did not matter. Consumers may be wider —
  `hedloom_exec.plan_bundles` accepts 2 and 3 — because a durable record
  outlives the schema the document that produced it was written at.
- `address(...)` declares an opaque source address as canonical data.
  `input_artifact(address, artifact=...)` records an external source without
  resolving, reading, or decoding it. A source is identified by exactly those
  two declarations, so declaring the same artifact at the same address twice
  is one source rather than two.
- External source references have inspectable value class `artifact`; ordinary
  operation-output references have value class `ephemeral`.
- `artifacts(kind)` declares a required, non-empty ordered collection input.
  Its binding retains member order and its dependencies contain one edge per
  member with an explicit zero-based position.
- Operation and flow call views may carry an explicit key. Keys share one
  operation/flow namespace within their containing boundary and may be reused
  only in distinct scopes. Keyed invocation and boundary IDs, and edges between
  keyed invocations, derive from that scoped authored identity.
- Keys are Plan identity only. They are never cache keys, scheduler keys,
  attempt identities, runtime identities, or sequential slots.
- Cross-edit stability is conditional: a keyed call beneath an unkeyed
  enclosing boundary inherits that counter-derived boundary's instability.
  External source IDs, unkeyed sources/invocations/boundaries, and fallback
  edges involving an external source or unkeyed endpoint remain deterministic
  authored-order identities and can change after earlier insertions.
- Explicit module `hedloom_flow.experimental.local_dask` consumes a validated Plan,
  exact `OperationIdentity`-keyed callables, and a complete source-ID mapping of
  already-decoded values. Each lowering has a fresh Dask-key namespace and
  returns immutable mappings for invocation tasks, named output projections,
  and pre-optimization invocation keys. Neither package initializer imports or
  reexports it.
- The experimental lowerer accepts only option-free `local` policy and
  resource-free used operations, builds no second readiness graph, performs no
  source I/O, and has no compute, submit, persistence,
  cancellation, publication, or scheduling method. Callers explicitly choose
  whether and how to compute returned Delayed values.
- `submit(...)` is a refusing boundary that raises `NotImplementedError`; it
  grants no executor contract.

## Contribution to the parent

The unit contributes static operation/flow planning and inspectable Plan IR to
the repository's broader author-plan-execute-evaluate vision. Only the planning
contract is promoted through the parent composition node.

## Exclusions

Hedloom Flow does not own execution meaning, public or general operation
execution, local or remote scheduling, placement enforcement, a working
`submit(...)`, general Dask lowering, Dask Distributed/Futures, LSF transport,
retries or attempts, persistence, address resolution, real
accessibility checking, artifact publication, materialized operation outputs,
runtime artifact values, recovery, plugins, dynamic or result-dependent
replanning, production hardening, runtime study ownership, or the complete
study lifecycle. It does not provide sequential editing helpers. The archived
sequential convenience is inactive historical material, not an API or backlog.

## Child composition

There are currently no child units.
