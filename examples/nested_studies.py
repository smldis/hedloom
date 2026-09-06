"""Submit an inner study from an operation, using the Session already open.

Run with ``python examples/nested_studies.py``. The wrapper executes on every
submission and calls an independently authored word-analysis study. Repeating
the text reuses both inner operations; changing it runs them again. Their
identities come from ordinary configuration and producer identity. Existing
records also survive script launches; use a new ``--work-dir`` to start fresh.

Use nesting when integrating a study that is submitted at execution time. Each
Plan is static when authored; inspecting the outer Plan does not reveal the
inner invocations. If the complete graph can be authored together, composing a
flow keeps that work visible in one Plan. In particular, freshness and output
identity now fit one Plan: see ``live_source.py``.

A Site declares resources and storage. The live Session owns workers and their
budget. The inner submission must use that same Session to share the compute;
calling ``subject.submit(site=...)`` would open another Session. The imported
``nested_studies_state`` module carries the live reference without serializing
the Session into a worker task. This example uses only in-process workers.

The outer operation holds one local slot while waiting. Two slots leave one
for the inner work; with only one, the graph kernel refuses nesting with
``NestedCapacityExhausted``. A separate placement for the wrapper is another
way to provide headroom. Concurrent wrappers need enough headroom collectively.
"""

import argparse
from pathlib import Path
import sys

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))
for unit in ("flow", "exec", "run"):
    sys.path.insert(0, str(_ROOT / unit / "src"))

from examples import nested_studies_state as state
from hedloom import Session, Site, artifact, local, operation, parameter, returned, session, study


def live_session() -> Session:
    if state.SESSION is None:
        raise RuntimeError("open a Session and set state.SESSION before submitting the outer study")
    return state.SESSION


@operation(config={"text": parameter(str)},
           outputs={"counts": returned(kind="word-counts")})
def count_words(text):
    counts = {}
    for word in text.split():
        counts[word] = counts.get(word, 0) + 1
    return {"counts": counts}


@operation(inputs={"counts": artifact("word-counts")},
           outputs={"summary": returned(kind="word-summary")})
def summarise(counts):
    return {"summary": {"distinct": len(counts), "total": sum(counts.values())}}


@study(name="word-analysis", default_policy=local())
def word_analysis(text):
    counts = count_words.named("count")(text=text).counts
    return {"summary": summarise.named("summarise")(counts).summary}


@operation(execution="each_submission", config={"text": parameter(str)},
           outputs={"result": returned(kind="nested-result")})
def run_analysis(text):
    # Author and submit the inner Plan here. The fresh wrapper ensures each
    # outer submission reaches it; the inner operations retain normal reuse.
    run = live_session().submit(word_analysis(text), name="inner-analysis")
    if not run.succeeded:
        raise RuntimeError(f"inner study failed:\n{run.summary()}")
    return {"result": {
        "summary": run.outputs["summary"].value,
        "inner": [
            {"key": outcome.authored_key, "reused": outcome.reused}
            for outcome in run.report.outcomes
        ],
    }}


@study(name="nested-studies", default_policy=local())
def nested_studies(text):
    return {"result": run_analysis.named("run-analysis")(text=text).result}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, default=_ROOT / "examples" / "_runs" / "nested-studies")
    args = parser.parse_args(argv)
    site = Site(root=str(args.work_dir / "attempts"),
                workspace_root=str(args.work_dir / "work"),
                history_root=str(args.work_dir / "history"), placements={"local": 2})
    text = "alpha beta gamma beta alpha beta"
    print(nested_studies(text).summary())
    print("Only the wrapper is in this Plan; it authors the inner Plan when it runs.")
    with session(site) as live:
        state.SESSION = live
        try:
            for label, document in (("first", text), ("unchanged", text),
                                    ("changed", "alpha beta gamma delta delta")):
                run = live.submit(nested_studies(document), name=label)
                if not run.succeeded:
                    print(run.summary())
                    return 1
                result = run.outputs["result"].value
                steps = " ".join(
                    f"{item['key']}:{'reused' if item['reused'] else 'ran'}"
                    for item in result["inner"]
                )
                print(f"{label}: {steps} summary={result['summary']}")
        finally:
            state.SESSION = None
    print("One Session served the outer and inner studies, then closed their workers.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
