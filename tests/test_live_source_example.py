from hedloom import Site, session
from examples import live_source


def test_fresh_download_reuses_only_unchanged_analysis(tmp_path, monkeypatch):
    site = Site(root=str(tmp_path / "records"), history_root=str(tmp_path / "history"), threads=1)
    with session(site) as live:
        first = live.submit(live_source.live_source(), name="first")
        same = live.submit(live_source.live_source(), name="same")
        monkeypatch.setitem(live_source.SERVICE, "document", "alpha beta gamma delta delta\n")
        changed = live.submit(live_source.live_source(), name="changed")
    assert all(run.succeeded for run in (first, same, changed))
    assert len(first.report.outcomes) == 3
    assert all(run["refresh"].ran for run in (first, same, changed))
    assert len(same.report.reused) == 2
    assert len(changed.report.reused) == 0
    assert first.outputs["summary"].value == same.outputs["summary"].value
    assert changed.outputs["summary"].value["commonest"] == "delta"
