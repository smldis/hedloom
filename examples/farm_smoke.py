"""Run a four-job Hedloom sweep on an LSF farm.

    python examples/farm_smoke.py examples/farm-smoke.site.toml [--dask]

Each of four independently ready commands generates and summarizes an integer
range after a different authored delay. The delays make completion order
observably differ from Plan order, while readiness makes the placement's
concurrency budget measurable. An identical second submission must reuse all
four invocations.
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
from hedloom_run.cluster import cluster_for  # noqa: E402

POINTS = (
    {"key": "slow", "start": 1, "count": 4, "delay_s": 2.5},
    {"key": "fast", "start": 10, "count": 3, "delay_s": 0.2},
    {"key": "middle", "start": 20, "count": 2, "delay_s": 0.3},
    {"key": "single", "start": 100, "count": 1, "delay_s": 0.4},
)


@operation(
    config={
        "start": parameter(int),
        "count": parameter(int),
        "delay_s": parameter(float),
    },
    outputs={
        "numbers": file("numbers.txt", kind="number-list"),
        "summary": file("summary.txt", kind="text-file"),
    },
    policy=lsf(),
)
def summarize_numbers(
    out,
    *,
    start: int,
    count: int,
    delay_s: float,
):
    """Materialize and summarize one range in one visible farm job."""

    return shell(
        "/bin/sh",
        "-c",
        'start=$1; count=$2; delay=$3; numbers=$4; output=$5; i=0; '
        ': > "$numbers"; while [ "$i" -lt "$count" ]; do '
        'printf "%s\\n" "$((start + i))" >> "$numbers"; i=$((i + 1)); done; '
        'sleep "$delay"; '
        'rows=0; sum=0; '
        'while IFS= read -r value; do '
        'rows=$((rows + 1)); sum=$((sum + value)); done < "$numbers"; '
        'printf "start=%s\\ncount=%s\\nrows=%s\\nsum=%s\\n" '
        '"$start" "$count" "$rows" "$sum" > "$output"',
        "hedloom-summarize",
        start,
        count,
        delay_s,
        out.numbers,
        out.summary,
    )


@flow
def range_sweep(points):
    summaries = {}
    for point in sweep(points, key="key"):
        result = summarize_numbers(
            start=point["start"],
            count=point["count"],
            delay_s=point["delay_s"],
        )
        summaries[point["key"]] = result.summary
    return summaries


def build():
    with plan() as draft:
        outputs = range_sweep.options(key="farm-sweep")(POINTS)
    return draft.finish(outputs=outputs)


def run(subject, site, *, client=None) -> int:
    """Submit twice, proving the second pass spends no farm work."""

    print("first submission (must launch four LSF jobs):")
    first = subject.submit(site=site, client=client, watch=True)
    if not first.succeeded:
        print(first.summary())
        return 1

    for point in POINTS:
        key = f"{point['key']}:summarize_numbers"
        summary = Path(first[key].artifacts["summary"]["address"])
        print(f"\n{point['key']} summary ({summary}):")
        print(summary.read_text(), end="")

    print("\nsecond submission (must reuse all four; no new LSF jobs):")
    second = subject.submit(site=site, client=client, watch=True)
    if not second.succeeded or len(second.report.reused) != 4:
        print(second.summary())
        return 1

    print("\nfarm sweep passed: four jobs launched, recorded, and reused")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("site", type=Path, help="farm Site profile")
    parser.add_argument(
        "--dask",
        action="store_true",
        help="give readiness and concurrency to the site's Dask cluster",
    )
    args = parser.parse_args()

    site = Site.from_file(args.site)
    subject = study(build())
    print(subject.summary(), "\n")

    if not args.dask:
        return run(subject, site)

    from distributed import Client

    cluster = cluster_for(site)
    try:
        with Client(cluster) as client:
            print(f"dashboard: {client.dashboard_link}")
            return run(subject, site, client=client)
    finally:
        cluster.close()


if __name__ == "__main__":
    raise SystemExit(main())
