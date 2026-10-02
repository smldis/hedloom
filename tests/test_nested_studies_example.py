from pathlib import Path
import subprocess
import sys

from examples import nested_studies
from hedloom import Site, runtime


def test_staged_example_reuses_analysis_with_one_slot(tmp_path):
    site = Site(records_dir=str(tmp_path / "records"), runs_dir=str(tmp_path / "history"),
                placements={"local": 1})
    with runtime(site) as live:
        stages = [nested_studies.analyse(live, text, name=f"run-{index}")
                  for index, text in enumerate(("alpha beta beta", "alpha beta beta", "alpha gamma"))]
    runs = [run for run, _ in stages]
    results = [result for _, result in stages]
    assert all(run.succeeded for run in runs)
    assert [[item["reused"] for item in result["inner"]] for result in results] == [
        [False, False], [True, True], [False, False],
    ]
    assert results[0]["summary"] == results[1]["summary"] == {"distinct": 2, "total": 3}
    assert results[2]["summary"] == {"distinct": 2, "total": 2}


def test_nested_example_runs_as_script(tmp_path):
    # Exercise the maintained caller-staging script in a fresh interpreter.
    script = Path(nested_studies.__file__)
    result = subprocess.run([sys.executable, str(script), "--work-dir", str(tmp_path)],
                            capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "first: count:ran summarise:ran" in result.stdout
    assert "unchanged: count:reused summarise:reused" in result.stdout
    assert "changed: count:ran summarise:ran" in result.stdout
