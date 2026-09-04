"""One file: a study that reads something which changes outside it.

    python examples/live_source.py

The question this answers is the one every study asks eventually: *the thing I
read is served by something else, and it may have changed since last time. How
do I re-read it every run, without recomputing work that the change did not
touch?*

Three runs, in one process, against one set of workers:

    first run    the document is new         both inner steps ran
    second run   the document is unchanged   both inner steps reused
    third run    the document changed        both inner steps ran again

The service here is a dict, so the example needs no network. Everything else —
the fetch, the fingerprint, the reuse decision, the shared session — is the
real machinery.

Three facts, in the order you meet them.

**A declared source is the only input identified by its content.** Every source
is read exactly once per submission, *before anything runs*, and fingerprinted.
Identical bytes give an identical fingerprint, so the whole plan below it is
reused; different bytes invalidate it transitively. That is what makes "check
whether it changed" cheap.

**An operation cannot do that job.** Every invocation's identity is computed
from the plan document before the first body is called, and a consumer is keyed
on its producer's *identity*, never on the bytes that producer wrote. So an
operation that fetches on every run must carry something that changes on every
run — and that change propagates to everything downstream, whether the fetched
document moved or not. Freshness and reuse cannot come from the same node.

They can come from two *stages*. `refresh` carries a nonce, so it reruns every
submission and the fetch really happens; it then authors and submits an inner
study whose source fingerprint decides what that fetch invalidated. Each plan
is still fully determined before it spends anything, which is the invariant
staging keeps and result-dependent control would break.

**A `Site` is a declaration; a `Session` is the live compute.** Sharing the
`Site` object shares roots and address spaces — not workers. Every `submit`
that is not given a session opens its own cluster, and would open its own farm
pools beside the ones already held. Reusing the session is what makes the inner
run spend the budget already open rather than asking for a second one.

One mechanical caveat, which `live_source_state.py` carries in full: a body is
copied to the worker that runs it, so a live session cannot be named as a
plain global of the module that defines the operation. It is reached through an
imported module instead, which travels as a reference.

Keep the nonce-bearing stage narrow. Anything authored downstream of it in the
outer plan reruns every submission too, for the same reason the fetch does.
"""

from __future__ import annotations

from pathlib import Path
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
for unit in ("flow", "exec", "run"):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / unit / "src"))

import live_source_state as state  # noqa: E402

from hedloom import (  # noqa: E402
    Session,
    Site,
    address,
    artifact,
    file,
    flow,
    input_artifact,
    local,
    operation,
    parameter,
    returned,
    session,
    study,
)

DOCUMENT = artifact("served-document")
TALLY = artifact("word-tally")

_HERE = Path(__file__).resolve().parent
_WORK = _HERE / "_runs" / "live-source"
SERVED_DIR = _WORK / "served"
SERVED_FILE = "document.txt"

# Stands in for whatever is outside the study: a service, a database, a
# generated file someone else owns. The study never reads this directly.
SERVICE = {"document": "alpha beta gamma beta alpha beta\n"}


# The site is built here, at import, and not inside `main`. An operation body
# runs in the submitting process, so a module-level declaration is what a body
# can reach — and keeping roots out of `config` keeps a machine path out of the
# plan's identity.
SITE = Site(
    root=str(_WORK / "attempts"),
    workspace_root=str(_WORK / "work"),
    address_spaces={"served": str(SERVED_DIR)},
    # Headroom on purpose: `refresh` holds one unit of `local` for as long as
    # the inner run takes, because it is blocked waiting on it. Declaring one
    # would leave the inner plan nothing to run on, which is refused as
    # `NestedCapacityExhausted` rather than hung — but refused is not run, so
    # the headroom is what makes this example work rather than explain itself.
    placements={"local": 4},
)

def live_session() -> Session:
    """The session an inner run must share, or a refusal naming what is missing.

    Read through `state` rather than from a global here, and that indirection is
    load-bearing: see `live_source_state`. Returning `None` would be found much
    later, as an `AttributeError` inside a body, blaming the operation for what
    is a wiring mistake.
    """

    if state.SESSION is None:
        raise RuntimeError(
            "no session is open: an inner study must run on the session its "
            "caller already holds, so open one with `with session(SITE)` and "
            "publish it before submitting"
        )
    return state.SESSION


def fetch(destination: Path) -> None:
    """Read the service and land its answer, writing only a real change.

    Two things are load-bearing. The write is atomic, because the fingerprint
    is taken on the submitting host at a moment this function does not choose,
    and half a file is not a version of anything. And identical content is left
    alone: above 64 MiB a source is identified by size and mtime rather than by
    hash, so rewriting the same bytes would invalidate the study by touching
    the clock.
    """

    destination.parent.mkdir(parents=True, exist_ok=True)
    served = SERVICE["document"].encode()
    if destination.exists() and destination.read_bytes() == served:
        return
    temporary = destination.with_suffix(destination.suffix + ".partial")
    temporary.write_bytes(served)
    temporary.replace(destination)


# ---------------------------------------------------------------------------
# Stage two: the work that must be reused when the document did not move.
# ---------------------------------------------------------------------------


@operation(inputs={"document": DOCUMENT},
           outputs={"tally": file("tally.txt", kind="word-tally")})
def tally(document, out) -> None:
    """Count each word once. A declared file, written where the executor looks."""

    counts: dict[str, int] = {}
    for word in Path(document).read_text(encoding="utf-8").split():
        counts[word] = counts.get(word, 0) + 1
    out.tally.write_text(
        "".join(f"{word} {counts[word]}\n" for word in sorted(counts)),
        encoding="utf-8",
    )


@operation(inputs={"tally": TALLY},
           outputs={"summary": returned(kind="tally-summary")})
def summarise(tally) -> dict:
    """A value-returning body: the number is the result, nothing is written."""

    lines = Path(tally).read_text(encoding="utf-8").split("\n")
    entries = [line.split() for line in lines if line]
    return {
        "distinct": len(entries),
        "total": sum(int(count) for _, count in entries),
        "commonest": max(entries, key=lambda entry: int(entry[1]))[0],
    }


@flow(name="live_source.reading")
def reading(document):
    """Ordinary authoring. Nothing here knows the document was just fetched."""

    return {"summary": summarise.named("summarise")(tally.named("tally")(document)).summary}


@study(name="live-source-reading", default_policy=local())
def reading_study():
    """Stage two: one declared source, and the work that depends on its content."""

    return reading.named("reading")(
        input_artifact(address("served", SERVED_FILE), artifact=DOCUMENT)
    )


# ---------------------------------------------------------------------------
# Stage one: fetch on every submission, then author and run stage two.
# ---------------------------------------------------------------------------


@operation(
    config={"nonce": parameter(str)},
    outputs={"result": returned(kind="refresh-result")},
)
def refresh(*, nonce: str) -> dict:
    """Fetch, then submit the inner study on the session already open.

    `nonce` is never read. It exists so that this invocation's identity differs
    every submission, which is the only way a body runs unconditionally — the
    cost being that anything authored downstream of it reruns too. Everything
    that must be reused therefore lives in the inner plan, not here.

    The inner run goes through `live_session()` rather than `submit(site=SITE)`.
    Both would reach the same records; only the first spends the compute that
    is already open instead of starting a second cluster beside it.
    """

    fetch(SERVED_DIR / SERVED_FILE)
    run = live_session().submit(reading_study())
    if not run.succeeded:
        raise RuntimeError(f"the reading study failed:\n{run.summary()}")
    return {
        "summary": run.value,
        "inner": [
            {"key": outcome.authored_key, "reused": outcome.reused}
            for outcome in run.report.outcomes
        ],
    }


@study(name="live-source", default_policy=local())
def live_source(nonce: str):
    """Stage one. It declares no source: what it reads does not exist yet."""

    return {"result": refresh.named("refresh")(nonce=nonce).result}


def main() -> int:
    subject = live_source(uuid.uuid4().hex)
    print(subject.summary())
    print(
        "\nThe document does not appear above. Stage one cannot name it: it is\n"
        "fetched by an invocation of this plan, and only the plan authored\n"
        "afterwards can declare it as a source.\n"
    )

    with session(SITE) as farm:
        state.SESSION = farm
        try:
            for label, served in (
                ("first  (new document)     ", None),
                ("second (unchanged)        ", None),
                ("third  (document changed) ", "alpha beta gamma delta delta\n"),
            ):
                if served is not None:
                    SERVICE["document"] = served
                # A fresh nonce each time, which is what "every run" means.
                run = farm.submit(live_source(uuid.uuid4().hex))
                if not run.succeeded:
                    print(run.summary())
                    return 1
                result = run.value
                inner = "  ".join(
                    f"{item['key']}:{'reused' if item['reused'] else 'ran   '}"
                    for item in result["inner"]
                )
                print(f"{label} inner: {inner}   summary: {result['summary']}")
        finally:
            state.SESSION = None

    print(
        "\nThe second run refetched and found the same bytes, so the inner\n"
        "plan's source fingerprint was unchanged and both steps were reused.\n"
        "The third found different bytes, and reused nothing below them."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
