from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def test_farm_smoke_sweeps_chains_publishes_and_reuses(tmp_path: Path) -> None:
    profile = tmp_path / "site.toml"
    profile.write_text(
        "[study]\n"
        'root = "attempts"\n'
        'workspace_root = "work"\n'
        "\n[placement.lsf]\n"
        'kind = "lsf-interactive"\n'
        'queue = "reg"\n'
        'walltime = "1"\n'
        "max_jobs = 4\n"
        "cores = 1\n"
        "timeout = 30\n",
        encoding="utf-8",
    )
    fake_state = tmp_path / "fake-lsf"
    fake_bin = ROOT / "exec" / "tests" / "fakefarm"
    env = {
        **os.environ,
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "FAKE_LSF_STATE": str(fake_state),
    }

    completed = subprocess.run(
        [sys.executable, "examples/farm_smoke.py", str(profile)],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert (
        "farm sweep passed: four jobs launched, chained, recorded, and reused"
        in completed.stdout
    )
    assert len(list(fake_state.glob("*.json"))) == 4
    summaries = sorted((tmp_path / "work").glob("*/summary.txt"))
    assert len(summaries) == 2
    assert {item.read_text(encoding="utf-8") for item in summaries} == {
        "start=1\ncount=4\nrows=4\nsum=10\n",
        "start=10\ncount=3\nrows=3\nsum=33\n",
    }
