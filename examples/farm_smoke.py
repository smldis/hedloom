"""Run a two-point, two-command Hedloom sweep on an LSF farm.

    python examples/farm_smoke.py examples/farm-smoke.site.toml

Each point supplies two authored parameters, ``start`` and ``count``. The
first farm command generates that integer range; the second consumes the
generated file and records its row count and sum. Two points therefore produce
four visible ``bsub -I`` jobs and two deterministic summary artifacts. An
identical second submission must reuse all four invocations.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
for unit in ("flow", "exec", "run"):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / unit / "src"))

from hedloom import (  # noqa: E402
    Site,
    artifact,
    file,
    flow,
    lsf,
    operation,
    parameter,
    plan,
    shell,
    study,
    sweep,
)

NUMBER_LIST = artifact("number-list")
POINTS = (
    {"key": "small", "start": 1, "count": 4},
    {"key": "offset", "start": 10, "count": 3},
)


@operation(
    config={"start": parameter(int), "count": parameter(int)},
    outputs={"numbers": file("numbers.txt", kind="number-list")},
    policy=lsf(),
)
def generate_numbers(out, *, start: int, count: int):
    """Command one: materialize a range from the two Plan parameters."""

    return shell(
        "/bin/sh",
        "-c",
        'start=$1; count=$2; output=$3; i=0; '
        ': > "$output"; while [ "$i" -lt "$count" ]; do '
        'printf "%s\\n" "$((start + i))" >> "$output"; i=$((i + 1)); done',
        "hedloom-generate",
        start,
        count,
        out.numbers,
    )


@operation(
    config={"start": parameter(int), "count": parameter(int)},
    inputs={"numbers": NUMBER_LIST},
    outputs={"summary": file("summary.txt", kind="text-file")},
    policy=lsf(),
)
def summarize_numbers(numbers, out, *, start: int, count: int):
    """Command two: consume command one's artifact and summarize it."""

    return shell(
        "/bin/sh",
        "-c",
        'input=$1; start=$2; count=$3; output=$4; '
        'rows=0; sum=0; '
        'while IFS= read -r value; do '
        'rows=$((rows + 1)); sum=$((sum + value)); done < "$input"; '
        'printf "start=%s\\ncount=%s\\nrows=%s\\nsum=%s\\n" '
        '"$start" "$count" "$rows" "$sum" > "$output"',
        "hedloom-summarize",
        numbers,
        start,
        count,
        out.summary,
    )


@flow
def range_sweep(points):
    summaries = {}
    for point in sweep(points, key="key"):
        numbers = generate_numbers(start=point["start"], count=point["count"])
        result = summarize_numbers(
            numbers,
            start=point["start"],
            count=point["count"],
        )
        summaries[point["key"]] = result.summary
    return summaries


def build():
    with plan() as draft:
        outputs = range_sweep.options(key="farm-sweep")(POINTS)
    return draft.finish(outputs=outputs)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("site", type=Path, help="farm Site profile")
    args = parser.parse_args()

    site = Site.from_file(args.site)
    subject = study(build())
    print(subject.summary(), "\n")

    print("first submission (must launch four LSF jobs):")
    first = subject.submit(site=site, watch=True)
    if not first.succeeded:
        print(first.summary())
        return 1

    for point in POINTS:
        key = f"{point['key']}:summarize_numbers"
        summary = Path(first[key].artifacts["summary"]["address"])
        print(f"\n{point['key']} summary ({summary}):")
        print(summary.read_text(), end="")

    print("\nsecond submission (must reuse all four; no new LSF jobs):")
    second = subject.submit(site=site, watch=True)
    if not second.succeeded or len(second.report.reused) != 4:
        print(second.summary())
        return 1

    print("\nfarm sweep passed: four jobs launched, chained, recorded, and reused")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
