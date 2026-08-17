from __future__ import annotations

import json
import os
from pathlib import Path

from distributed import Client, get_task_stream

from hedloom import Site
from hedloom_exec.planned import plan_bundles
from hedloom_run.cluster import cluster_for

from examples import farm_smoke


ROOT = Path(__file__).resolve().parents[1]


def maximum_overlap(records) -> int:
    """Count half-open job intervals; a handoff at one instant is not overlap."""

    events = sorted(
        [
            event
            for record in records
            for event in (
                (record["started_at_ns"], 1),
                (record["ended_at_ns"], -1),
            )
        ],
        key=lambda event: (event[0], event[1]),
    )
    current = 0
    peak = 0
    for _, change in events:
        current += change
        peak = max(peak, current)
    return peak


def test_dask_farm_smoke_honours_placement_capacity_and_plan_order(
    tmp_path: Path, monkeypatch
) -> None:
    """The real crossing is graph × `bsub -I` × authored-body binding.

    Eight jobs alone prove none of those seams. The placement resource earns
    its existence only if the same profile both confines four independent
    producer-consumer chains to the farm worker and keeps them below
    ``max_jobs``.
    """

    profile = tmp_path / "site.toml"
    profile.write_text(
        "[study]\n"
        'root = "attempts"\n'
        'workspace_root = "work"\n'
        "\n[placement.lsf]\n"
        'kind = "lsf-interactive"\n'
        'queue = "reg"\n'
        'walltime = "1"\n'
        "max_jobs = 2\n"
        "cores = 1\n"
        "timeout = 30\n"
        "\n[kernel]\n"
        "threads = 2\n"
        'dashboard = "none"\n',
        encoding="utf-8",
    )
    fake_state = tmp_path / "fake-lsf"
    fake_bin = ROOT / "exec" / "tests" / "fakefarm"
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_LSF_STATE", str(fake_state))

    site = Site.from_file(profile)
    subject = farm_smoke.farm_sweep()
    plan_order = [item.authored_key for item in plan_bundles(subject.document)]
    completion_order = []
    cluster = cluster_for(site)
    try:
        with Client(cluster) as client:
            workers = {
                address: detail["name"]
                for address, detail in client.scheduler_info()["workers"].items()
            }
            with get_task_stream(client) as stream:
                first = subject.submit(
                    site=site,
                    client=client,
                    on_event=lambda outcome: completion_order.append(
                        outcome.authored_key
                    ),
                )

            assert first.succeeded, first.summary()
            assert [item.authored_key for item in first.report.outcomes] == plan_order
            assert completion_order != plan_order
            assert completion_order[-1] == "slow:summarize_numbers"
            assert len(stream.data) == 8
            task_workers = {workers[item["worker"]] for item in stream.data}

            before_reuse = {
                item.name: item.read_bytes() for item in fake_state.glob("*.json")
            }
            second = subject.submit(site=site, client=client)
    finally:
        cluster.close()

    submitted = sorted(fake_state.glob("*.json"))
    records = [json.loads(item.read_text(encoding="utf-8")) for item in submitted]
    assert len(records) == 8
    for path, record in zip(submitted, records):
        assert record["name"] == path.stem
        assert record["options"]["-J"] == record["name"]
        assert (Path(site.root) / record["name"] / "manifest.json").is_file()

    assert maximum_overlap(records) == 2
    assert task_workers == {"lsf"}
    assert second.succeeded
    assert len(second.report.reused) == 8
    assert {
        item.name: item.read_bytes() for item in fake_state.glob("*.json")
    } == before_reuse

    summaries = sorted((tmp_path / "work").glob("*/summary.txt"))
    assert len(summaries) == 4
    assert {item.read_text(encoding="utf-8") for item in summaries} == {
        "start=1\ncount=4\nrows=4\nsum=10\n",
        "start=10\ncount=3\nrows=3\nsum=33\n",
        "start=20\ncount=2\nrows=2\nsum=41\n",
        "start=100\ncount=1\nrows=1\nsum=100\n",
    }, "each consumer must report rows and sum from its producer's artifact"
