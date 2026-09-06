from pathlib import Path
import subprocess
import sys

import pytest

from examples import nested_studies, nested_studies_state as state
from hedloom import Site, session


@pytest.mark.parametrize("sequential", [True, False])
def test_nested_example_uses_open_session_and_reuses_inner_work(tmp_path, monkeypatch, sequential):
    site = Site(root=str(tmp_path / "records"), history_root=str(tmp_path / "history"),
                placements={"local": 2})
    with session(site, sequential=sequential) as live:
        monkeypatch.setattr(state, "SESSION", live)
        # A nested Study.submit would open a second cluster. Once ours is open,
        # refuse any such creation so this exercises actual Session sharing.
        from hedloom_run import cluster as cluster_module

        def unexpected_cluster(*args, **kwargs):
            pytest.fail("nested submission opened another cluster")

        monkeypatch.setattr(cluster_module, "cluster_for", unexpected_cluster)
        runs = [live.submit(nested_studies.nested_studies(text), name=f"run-{index}")
                for index, text in enumerate(("alpha beta beta", "alpha beta beta", "alpha gamma"))]
        monkeypatch.setattr(state, "SESSION", None)
    assert all(run.succeeded for run in runs)
    assert all(run["run-analysis"].ran for run in runs)
    results = [run.outputs["result"].value for run in runs]
    assert [[item["reused"] for item in result["inner"]] for result in results] == [
        [False, False], [True, True], [False, False],
    ]
    assert results[0]["summary"] == results[1]["summary"] == {"distinct": 2, "total": 3}
    assert results[2]["summary"] == {"distinct": 2, "total": 2}


def test_nested_example_runs_as_script(tmp_path):
    # __main__ bodies are serialized by value: exercise the imported state
    # indirection with a real fresh interpreter, not just an imported fixture.
    script = Path(nested_studies.__file__)
    result = subprocess.run([sys.executable, str(script), "--work-dir", str(tmp_path)],
                            capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "first: count:ran summarise:ran" in result.stdout
    assert "unchanged: count:reused summarise:reused" in result.stdout
    assert "changed: count:ran summarise:ran" in result.stdout
