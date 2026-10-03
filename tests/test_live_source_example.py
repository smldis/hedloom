from hedloom import Site, runtime
from examples import live_source


def test_fresh_download_reuses_only_unchanged_analysis(tmp_path, monkeypatch):
    site = Site(records_dir=str(tmp_path / "records"), runs_dir=str(tmp_path / "history"), threads=1)
    with runtime(site) as live:
        first = live.submit(live_source.live_source(), name="first").wait()
        same = live.submit(live_source.live_source(), name="same").wait()
        monkeypatch.setitem(live_source.SERVICE, "document", "alpha beta gamma delta delta\n")
        changed = live.submit(live_source.live_source(), name="changed").wait()
    assert all(run.succeeded for run in (first, same, changed))
    assert len(first.report.outcomes) == 3
    assert all(run["refresh"].ran for run in (first, same, changed))
    assert len(same.report.reused) == 2
    assert len(changed.report.reused) == 0
    assert first.outputs["summary"].value == same.outputs["summary"].value
    assert changed.outputs["summary"].value["commonest"] == "delta"
