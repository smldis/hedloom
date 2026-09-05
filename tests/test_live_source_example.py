from __future__ import annotations

import uuid
from pathlib import Path

from hedloom import Site, session

from examples import live_source


# The example puts its own directory on `sys.path` so it runs as a script, so
# its state module is the top-level `live_source_state` rather than
# `examples.live_source_state` — two module objects, and only the one the body
# actually reads is worth patching. `live_source.state` is that one, by
# construction.
def site_for(tmp_path: Path) -> Site:
    return Site(
        root=str(tmp_path / "attempts"),
        workspace_root=str(tmp_path / "work"),
        address_spaces={"served": str(tmp_path / "served")},
        # The waiter holds one unit of `local` for the whole inner run, so the
        # inner plan needs units the waiter is not sitting on.
        placements={"local": 4},
    )


def outcomes_of(run) -> dict[str, bool]:
    """Which inner invocations were reused, by authored key."""

    result = run.outputs["result"].value
    return {item["key"]: item["reused"] for item in result["inner"]}


def test_an_unchanged_document_reuses_the_inner_plan(tmp_path, monkeypatch) -> None:
    """The example's whole claim, checked from the report rather than the clock.

    The fetch runs on every submission — that is what the nonce buys — so a
    second run that reuses nothing would mean the source fingerprint was moving
    when the bytes were not, and every study reading a served document would
    recompute forever without saying why.
    """

    monkeypatch.setattr(live_source, "SERVED_DIR", tmp_path / "served")
    site = site_for(tmp_path)

    with session(site) as farm:
        monkeypatch.setattr(live_source.state, "SESSION", farm)

        first = farm.submit(live_source.live_source(uuid.uuid4().hex))
        assert first.succeeded, first.summary()
        assert outcomes_of(first) == {"tally": False, "summarise": False}

        second = farm.submit(live_source.live_source(uuid.uuid4().hex))
        assert second.succeeded, second.summary()
        assert outcomes_of(second) == {"tally": True, "summarise": True}
        assert (
            second.outputs["result"].value["summary"]
            == first.outputs["result"].value["summary"]
        )


def test_a_changed_document_invalidates_everything_below_it(
    tmp_path, monkeypatch
) -> None:
    """The other half: reuse must not survive a change it cannot see."""

    monkeypatch.setattr(live_source, "SERVED_DIR", tmp_path / "served")
    site = site_for(tmp_path)

    with session(site) as farm:
        monkeypatch.setattr(live_source.state, "SESSION", farm)

        first = farm.submit(live_source.live_source(uuid.uuid4().hex))
        assert first.succeeded, first.summary()

        monkeypatch.setitem(
            live_source.SERVICE, "document", "alpha beta gamma delta delta\n"
        )
        second = farm.submit(live_source.live_source(uuid.uuid4().hex))
        assert second.succeeded, second.summary()

        assert outcomes_of(second) == {"tally": False, "summarise": False}
        changed = second.outputs["result"].value["summary"]
        assert changed != first.outputs["result"].value["summary"]
        assert changed["commonest"] == "delta"
