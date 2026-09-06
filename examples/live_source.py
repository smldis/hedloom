"""Fetch on every submission; reuse analysis when the fetched bytes are equal.

Run with ``python examples/live_source.py``. A local dictionary stands in for
an external service. Acquisition is an ordinary operation in one static Plan;
content identity stops unchanged observations invalidating downstream work.
"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
for unit in ("flow", "exec", "run"):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / unit / "src"))

from hedloom import Site, artifact, file, flow, local, operation, returned, session, study

DOCUMENT = artifact("served-document")
TALLY = artifact("word-tally")
_WORK = Path(__file__).resolve().parent / "_runs" / "live-source"
SERVICE = {"document": "alpha beta gamma beta alpha beta\n"}
SITE = Site(root=str(_WORK / "attempts"), workspace_root=str(_WORK / "work"),
            history_root=str(_WORK / "history"), threads=1)


@operation(execution="each_submission",
           outputs={"document": file("document.txt", kind="served-document", identity="content")})
def refresh(out):
    out.document.write_text(SERVICE["document"], encoding="utf-8")


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
    return {'summary': {
        "distinct": len(entries),
        "total": sum(int(count) for _, count in entries),
        "commonest": max(entries, key=lambda entry: int(entry[1]))[0],
    }}


@flow(name="live_source.reading")
def reading(document):
    """Ordinary authoring. Nothing here knows the document was just fetched."""

    return {"summary": summarise.named("summarise")(tally.named("tally")(document)).summary}


@study(name="live-source", default_policy=local())
def live_source():
    return reading.named("reading")(refresh.named("refresh")().document)


def main():
    print(live_source().summary())
    with session(SITE) as live:
        for label, document in (("new", SERVICE["document"]),
                                ("unchanged", SERVICE["document"]),
                                ("changed", "alpha beta gamma delta delta\n")):
            SERVICE["document"] = document
            run = live.submit(live_source(), name="live-source")
            print(label, run.summary(), sep="\n")
            if not run.succeeded:
                return 1
            print(run.outputs["summary"].value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
