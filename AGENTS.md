# Hedloom agent guidance

Inherit the project guidance from `../AGENTS.md`. Read `../MANIFESTO.md`, the
root `ONTOLOME.md`, this unit's `ONTOLOME.md`, and
`../docs/vision/open-concepts.md` before working here.

## What this unit owns

One thing: the join between an authored study and its execution. It composes
the three units beneath it and adds no second notion of anything they already
own.

| Concern | Unit | Package |
| --- | --- | --- |
| authoring, keys, Plan IR, validation | `hedloom-flow` | `flow/src/hedloom_flow` |
| attempt identity, journal, transports, reuse, artifact recording | `hedloom-exec` | `exec/src/hedloom_exec` |
| traversal, readiness, binding, placement selection, `Site`, cluster | `hedloom-run` | `run/src/hedloom_run` |
| binding authored bodies to planned invocations, `submit`, `session` | `hedloom` | `src/hedloom` |

`hedloom-exec` must keep importing neither this package nor Dask. That
independence is what makes the kernel and this façade separately replaceable,
and a Dask import there is the first thing that would make the choice
irreversible.

## The invariants every change is measured against

    A body decides what runs; it never decides *whether* it runs.

Reuse, identity, ordering and placement are settled before a body is called.
Anything that would let writing Python acquire scheduling authority belongs
somewhere else, or nowhere.

    Changing which kernel decides readiness changes how long a plan takes
    and nothing else — the same results, under the same identities.

The two kernels share `hedloom_run.binding` rather than restating it. A change
that makes them disagree about a plan is a defect even if both halves pass.

**Do not add result-dependent control.** `submit` is the surface where it would
most plausibly arrive disguised as convenience — a `retry=`, `max_iterations=`
or `until=` argument is the tripwire, not a feature. Planning handles refuse to
be read as values for the same reason; if a study needs to branch on a result,
raise it rather than implementing it.

**Incompleteness may refuse; it may not be silently wrong.** A surface that
cannot yet be correct declines. Documenting a limitation does not make it safe,
because a caller reads the return value and not the ontology.

## Where to read, and what to trust

Three surfaces, deliberately separate. Know which one you are in.

| Surface | Where | Maintained against the code? |
| --- | --- | --- |
| **Contracts** — what each unit guarantees now, and its exclusions | `ONTOLOME.md` in each unit; `exec/DECISIONS.md` | **Yes.** Update it in the same change that alters the contract. |
| **Documentation** — how to use it and how it works | `docs/` in each unit; built by `python composition.py docs` from the repository root | **Yes.** Everything under a `docs/` directory is published to the Sphinx site. |
| **Design record** — reviews, plans, proposals, phase trackers | `design/` in `hedloom`, `flow` and `exec`; `flow/PLANNING.md`; `flow/IMPLEMENTATION.md` | **No.** Written on a date, never edited to stay true, never published. |

The rule that follows: **do not cite a `design/` file as evidence of current
behaviour, and do not update one to match the code.** If something in there is
still right and load-bearing, promote it into `docs/` or an `ONTOLOME.md`.
`design/README.md` says which of the proposals are still live — today,
cancellation, binding the attempt identity, and reclaiming produced files
are; everything else is delivered or superseded.

Two files in `design/` are also *unanswered correspondence*: the 2026-08-14
architecture review and its concurrency companion still carry blank
`**Your call**` slots. They are not instructions.

## Evidence, and how to talk about it

The examples in `examples/` are evidence, not demonstrations: they run real
external tools and their numbers are checkable. Keep them runnable, and do not
let one acquire a fixture the other would need.

**Nothing in this package may name a domain.** No simulator, no circuit, no
process corner — not in code, not in a docstring, not in an example. The
examples run `awk` and `/bin/sh` precisely so that the package cannot quietly
acquire a domain it does not own. Studies that *do* name a simulator belong in
`../studies/`, one level up, and are linked from here rather than moved in.

Be exact about what has met a farm, because the honest split matters more than
the total. `examples/farm_smoke.py` has run against a real LSF installation
through the **sequential** kernel. The **graph kernel** has not, so concurrency,
`max_jobs` as a real bound, and the `bjobs` parser are fake-only. Every domain
study in `../studies/` still runs entirely at `local`.
`docs/guide/first-farm-run.md` is where that split is maintained; keep it true.

## Checks

```console
PYTHONPATH=src:flow/src:exec/src:run/src python -m pytest -q \
    tests exec/tests run/tests flow/tests
python ../composition.py docs          # from the repository root: python composition.py docs
```

**Name all four unit test directories.** Every unit's `pyproject.toml` sets
`testpaths = ["tests"]`, so a bare `pytest -q` from here collects only the
façade's 58 tests and silently skips the other 400. It exits zero either way,
which is the worst shape a check can have.

A successful Sphinx build can still report missing toctree entries and
unresolved cross-references, so read the warnings rather than the exit status.
