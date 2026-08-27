# Internals

For working *on* this package rather than with it. Nothing here is needed to
author or run a study — that is [the guide](../index.md).

## Why this unit exists at all

`hedloom_flow` owns authoring and the Plan. `hedloom_exec` owns one durable
record and its tries, and imports neither this package nor Dask. `hedloom_run` owns
binding and readiness. This package composes them and adds the one thing none of
them could own alone: a `submit` that runs what was authored, because it holds
both halves.

Before it, an `@operation` body was dead code — `hedloom_flow` kept the function
but nothing ever called it — and a study needed a second file supplying real
implementations, command lines and output paths. For this project's reference
that file was six hundred lines whose only job was to agree with the first
one. Every seam between the two was a place where the study could mean
something other than what was authored.

## Where a change belongs

`hedloom` owns no attempt record, no identity, no reuse policy, no transport, no
readiness, and no Plan validation. It composes those responsibilities into one
operator gesture; each of them remains exactly where it was, and `hedloom_exec`
in particular still imports neither `hedloom_flow` nor Dask, which is what keeps
the durable record and this façade independently replaceable.

| A bug in | Belongs to | Documented at |
| --- | --- | --- |
| what the Plan says, authoring, keys | `hedloom_flow` | `hedloom-flow`'s pages |
| reuse, identity, the journal, a transport | `hedloom_exec` | `hedloom-exec`'s pages |
| readiness, binding, placement selection, `Site` | `hedloom_run` | `hedloom-run`'s pages |
| binding authored bodies to planned invocations, `submit`, `session` | `hedloom` | here |

That is the whole of what belongs in this package: the composition, and the one
`submit` that joins the halves.

## Staged plans

An invocation may itself author and submit an inner Plan — see
`../studies/ota_pvt_clean_nested.py`. Nothing here is result-dependent control:
no plan branches on its own result. Plans are *staged* instead — each one is
fully determined at the moment it is authored, and a later stage is authored
only after the earlier stage has already produced the ordinary Python values it
needs. The invariant ("a Plan predicts what will run before anything runs")
holds per plan, exactly where it was always stated; it is just that "a study"
may now be more than one plan, submitted in sequence by ordinary Python code
inside one invocation.

This is recorded as "already demonstrated: staged plans" in
`docs/vision/open-concepts.md` at the repository root, along with the open
questions it leaves: a coarse source fingerprint that invalidates every point
when only the point list changed, and an inner run's records root arriving as
authored config, which bakes a machine path into the outer plan's identity.

## The pages here

- [**How `@study`, `@flow` and `sweep` work, and how that compares**](mechanism.md)
  — the mechanism under the three decorators, traced through the source:
  planning as tracing, the flow boundary as a naming scope, the four-step
  identity chain from authored key to attempt directory, and what is
  deliberately *not* in the input digest. Then the same axes against pure Dask,
  TensorFlow graph mode, LibreLane and CACE — useful for arguing about a
  proposed change, because most of them amount to moving authority across one of
  those three layers.
- [**Placement, clustering and scheduling**](placement-and-scheduling.md)
  — the three concepts, how a placement name becomes a transport, and why each
  placement becomes a worker of its own. Start here before either Dask page.
- [**How Dask decides where work runs**](dask-scheduling-concepts.md)
  — the same subject as prose: cluster versus farm, placement versus worker, why
  a thread is expensive, and the lockout that annotating every task exists to
  prevent.
- [**Dask scheduling and resources: the rules**](dask-scheduling-rules.md)
  — ten rules, each cited to a line of `distributed` 2026.7.1 so an upgrade can
  be re-checked. The extra takes a floor rather than a pin, so those citations
  are a dated reading rather than a description of whatever you have installed.
- [**The attempt claim, model-checked**](attempt-claim-protocol.md)
  — the claim, the journal and the two durable writes that publish a result,
  with a TLA+ model. Says which assumptions are load-bearing by denying them one
  at a time: the lock, the publish-before-record order, owner-bound lifetime.
  Records one finding — publication runs unlocked — and one surprise:
  `discovery_is_authoritative` is not carrying what it looks like it carries.
- [**Stopping a sweep, model-checked**](stop-admitting-protocol.md)
  — the same treatment one layer up, for what `graph.py` does with the rest of
  a sweep when one invocation fails. The "bounded loss" its own comment admits
  is a false report line, and the record has to answer for both what ran and
  what it produced.

Neighbouring units document themselves: `hedloom-flow`'s architecture page has
the authoring model and the Plan IR, `hedloom-exec`'s page has attempt identity
and the reuse decision this package defers to, and `hedloom-run`'s page has the
two kernels, the binding rules and `Site`.

## Two things that are not documentation

`hedloom/ONTOLOME.md` states this unit's current contracts, its exclusions, and
what its examples have and have not demonstrated. It is the contract surface for
contributors and agents, kept in the repository rather than published here.

`hedloom/design/` holds reviews, plans and proposals written on a date. They are
not maintained against the code and are deliberately not built — but two of them
are live proposals (cancellation, and binding the attempt identity before
submission), and `design/README.md` says which is which.

```{toctree}
:maxdepth: 1
:caption: Internals

mechanism
placement-and-scheduling
dask-scheduling-concepts
dask-scheduling-rules
attempt-claim-protocol
stop-admitting-protocol
```
