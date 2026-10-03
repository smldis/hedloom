"""Stage independently authored word analysis from the caller.

Run with ``python examples/nested_studies.py``. Repeating the text reuses both
operations; changing it runs them again. A single Runtime serves each stage
with capacity one. No worker holds a slot while submitting or waiting for a
child study. This file retains its historical name to show the migration from
worker-held nesting; hierarchical submissions remain deferred.
"""

import argparse
from pathlib import Path
import sys

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))
for unit in ("flow", "exec", "run"):
    sys.path.insert(0, str(_ROOT / unit / "src"))

from hedloom import Site, artifact, local, operation, parameter, returned, runtime, study


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


def analyse(live, text, *, name):
    """Caller-level stage: return its terminal evidence and summary together."""
    run = live.submit(word_analysis(text), name=name).wait()
    if not run.succeeded:
        raise RuntimeError(f"word analysis failed:\n{run.summary()}")
    return run, {
        "summary": run.outputs["summary"].value,
        "inner": [{"key": item.authored_key, "reused": item.reused}
                  for item in run.report.outcomes],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, default=_ROOT / "examples" / "_runs" / "nested-studies")
    args = parser.parse_args(argv)
    site = Site(records_dir=str(args.work_dir / "records"),
                work_dir=str(args.work_dir / "work"),
                runs_dir=str(args.work_dir / "runs"), placements={"local": 1})
    text = "alpha beta gamma beta alpha beta"
    print(word_analysis(text).summary())
    print("Each caller-authored Plan exposes both operations before submission.")
    with runtime(site) as live:
        for label, document in (("first", text), ("unchanged", text),
                                ("changed", "alpha beta gamma delta delta")):
            run, result = analyse(live, document, name=label)
            steps = " ".join(
                f"{item['key']}:{'reused' if item['reused'] else 'ran'}"
                for item in result["inner"]
            )
            print(f"{label}: {steps} summary={result['summary']}")
    print("One Runtime served the caller stages, then released their workers.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
