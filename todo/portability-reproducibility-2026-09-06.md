# Portability and reproducibility: what is worth preserving?

Date: 2026-09-06. Status: deferred concept; revisit through a concrete use case.

Recorded at the user's request after discussing point 3 of the
[philosophical review](../../design/philosophical-review-2026-09-05.md).
This is a backlog note, not an adopted architecture or implementation task.

## Motivation

Finding earlier work should let an operator understand what they can still do
with it. Discoverability provides a starting point: locate a submission and
follow its outputs to the exact computation records and tries it used.
Preservation must then explain which evidence remains usable and what would
be needed to repeat the work.

The [manifesto](../../MANIFESTO.md#make-evidence-trustworthy-and-reusable)
already keeps full reproducibility as an ambition while allowing lightweight
experiments. The open question is how to make that ambition useful in a real
handoff without requiring every experiment to preserve everything.

## Distinctions to retain

| Intended use | What it requires |
| --- | --- |
| Inspect what happened | A readable account of the request, execution, and outcomes. |
| Retrieve an original result | Its payload still exists, is identifiable, and can be reached. |
| Repeat a procedure elsewhere | Recoverable inputs, implementations, tools, and relevant environment, with enough instructions to use them. |
| Recreate exact artifacts | Conditions sufficient for byte equality, with evidence that the recreated artifacts match. |
| Reproduce an engineering conclusion | Evidence, criteria, assumptions, and interpretation sufficient to assess the conclusion under stated conditions. |

These are different capabilities. Retaining a manifest does not retain the
payload it describes. Retaining output bytes does not necessarily retain the
procedure or the reasoning that made those outputs significant. Measurements
and an evaluation may remain useful after a large intermediate dataset is
removed, while a new analysis requiring that dataset becomes impossible.

An "explicit object" initially means naming what is to be preserved or
reproduced: a particular output, execution, procedure, or conclusion. Whether
this needs a new software object or package format remains open.

## Connection to discoverability

The ongoing discovery work can provide the route from a run to its evidence.
Exact record/try references matter because several runs can use the same
computation, and later reusable evidence need not be the try an earlier run
used. A saved run account would preserve that relationship; it would not by
itself make the referenced dependencies portable.

Keep historical execution outcomes separate from present payload availability.
An invocation having succeeded does not establish that its output can be
opened now. Known removal, an unreachable location, and availability that has
not been checked convey different information. A pin protects its stated
workspace scope; it does not establish that every external dependency needed
for reproduction has been preserved.

These distinctions should inform discovery where useful. This note does not
make the wider preservation question a prerequisite for that work.

## Trigger and first useful investigation

Revisit when someone needs to hand off a selected run, return to important
evidence after cleanup, or repeat a procedure in another environment.

Use one real case: a permitted collaborator in a fresh environment receives
the recorded material and tries to inspect a chosen result and repeat a
chosen part of the work without relying on the original author's memory.
Agree first whether success means retrieving the original bytes, repeating
the procedure, recreating exact artifacts, or supporting the conclusion.

Record what was supplied, what could be inspected or reproduced, what was
missing, and what still depended on the original environment or explanation.
Missing dependencies may include source data, implementation versions, tools,
external resources, access, or evaluation criteria. Distinguish a dependency
being identified from its being recoverable.

Use that evidence to choose the smallest useful improvement and its owner.
Possible outcomes include clearer availability reporting, preserving an
additional dependency, or a concrete export/handoff capability. No particular
storage system, new component, universal reproducibility flag, or packaging
format is selected here. No reconstruction probe was performed for this note.
