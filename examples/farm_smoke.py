"""Run four independent, two-job Hedloom chains on an LSF farm.

    python examples/farm_smoke.py examples/farm-smoke.site.toml

Each point first generates an integer range, then passes that declared artifact
to a summarizing farm job after a different authored delay. The four chains are
independent of each other, making the placement's concurrency budget measurable;
the delays make completion order observably differ from Plan order. An identical
second submission must reuse all eight invocations.
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
    session,
    shell,
    study,
    sweep,
)

NUMBER_LIST = artifact("number-list")
POINTS = (
    {"key": "slow", "start": 1, "count": 4, "delay_s": 2.5},
    {"key": "fast", "start": 10, "count": 3, "delay_s": 0.2},
    {"key": "middle", "start": 20, "count": 2, "delay_s": 0.3},
    {"key": "single", "start": 100, "count": 1, "delay_s": 0.4},
)


@operation(
    config={"start": parameter(int), "count": parameter(int)},
    outputs={"numbers": file("numbers.txt", kind="number-list")},
    policy=lsf(),
)
def generate_numbers(out, *, start: int, count: int):
    """Materialize one point's range for its consumer farm job."""

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
    config={
        "start": parameter(int),
        "count": parameter(int),
        "delay_s": parameter(float),
    },
    inputs={"numbers": NUMBER_LIST},
    outputs={"summary": file("summary.txt", kind="text-file")},
    policy=lsf(),
)
def summarize_numbers(
    numbers,
    out,
    *,
    start: int,
    count: int,
    delay_s: float,
):
    """Consume the producer farm job's declared artifact."""

    return shell(
        "/bin/sh",
        "-c",
        'numbers=$1; start=$2; count=$3; delay=$4; output=$5; '
        'sleep "$delay"; '
        'rows=0; sum=0; '
        'while IFS= read -r value; do '
        'rows=$((rows + 1)); sum=$((sum + value)); done < "$numbers"; '
        'printf "start=%s\\ncount=%s\\nrows=%s\\nsum=%s\\n" '
        '"$start" "$count" "$rows" "$sum" > "$output"',
        "hedloom-summarize",
        numbers,
        start,
        count,
        delay_s,
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
            delay_s=point["delay_s"],
        )
        summaries[point["key"]] = result.summary
    return summaries


@study
def farm_sweep():
    """The study: one flow over the four points, and nothing else.

    Decorated, so calling it plans. Nothing here runs — an operation call
    records itself and hands back a handle — which is why the body can be read
    as the shape of the work rather than as work.
    """

    return range_sweep.named("farm-sweep")(POINTS)


def run(subject, farm) -> int:
    """Submit twice, proving the second pass spends no farm work.

    Both submissions go to one session, so they share one cluster, one farm
    budget and one queue watcher — and the second pass proves reuse without
    paying to start any of that again.
    """

    print("first submission (must launch eight LSF jobs):")
    first = farm.submit(subject)
    if not first.succeeded:
        print(first.summary())
        return 1

    for point in POINTS:
        key = f"{point['key']}:summarize_numbers"
        summary = Path(first[key].artifacts["summary"]["address"])
        print(f"\n{point['key']} summary ({summary}):")
        print(summary.read_text(), end="")

    print("\nsecond submission (must reuse all eight; no new LSF jobs):")
    second = farm.submit(subject)
    if not second.succeeded or len(second.report.reused) != 8:
        print(second.summary())
        return 1

    print("\nfarm sweep passed: eight jobs launched, chained, recorded, and reused")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("site", type=Path, help="farm Site profile")
    args = parser.parse_args()

    site = Site.from_file(args.site)
    subject = farm_sweep()
    print(subject.summary(), "\n")

    # There is no kernel to choose. The site says how much farm this study may
    # spend and the session opens exactly that, for as long as the two runs
    # below need it; a site that declares nothing has capacity one, which is
    # one invocation at a time. Add `sequential=True` for that without a
    # scheduler, or `locally=True` to debug the whole thing on this host.
    with session(site, watch=True) as farm:
        return run(subject, farm)


if __name__ == "__main__":
    raise SystemExit(main())
