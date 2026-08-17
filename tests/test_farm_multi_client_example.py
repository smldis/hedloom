from __future__ import annotations

import json
import os
from pathlib import Path

from examples import farm_multi_client

from tests.test_farm_smoke_example import maximum_overlap


ROOT = Path(__file__).resolve().parents[1]


def jobs(state: Path) -> list[dict]:
    return [
        json.loads(item.read_text(encoding="utf-8"))
        for item in sorted(state.glob("*.json"))
    ]


def test_two_studies_share_one_budget_one_keyspace_and_one_record(
    tmp_path: Path, monkeypatch
) -> None:
    """What a second run shares with the first, arrangement by arrangement.

    Each act is checked twice over, by two instruments that could disagree: the
    example counts farm jobs by folding the attempt journals, and this test
    counts them from the fake `bsub`'s own submission records. A defect in
    either the protocol or the example's arithmetic shows up as the two
    disagreeing about how much farm was spent.
    """

    state = tmp_path / "fake-lsf"
    monkeypatch.setenv(
        "PATH", f"{ROOT / 'exec' / 'tests' / 'fakefarm'}{os.pathsep}{os.environ['PATH']}"
    )
    monkeypatch.setenv("FAKE_LSF_STATE", str(state))

    # The example's own site, with one thing overridden for a test run: no
    # dashboard, since a test has nobody to watch it. That an override is the
    # way to say so — rather than a second site — is itself the point of it.
    site = farm_multi_client.site_for(tmp_path, queue="reg", cap=2).overridden(
        {"kernel": {"dashboard": "none"}}
    )

    # One session, two studies: eight jobs wanted, and the placement cap is the
    # session's rather than each study's.
    assert farm_multi_client.shared_budget(site, 2)
    submitted = jobs(state)
    assert len(submitted) == 8
    assert maximum_overlap(submitted) == 2, (
        "two studies on one session must draw on one budget"
    )

    # One session, the same study twice: Dask keys belong to the scheduler, so
    # identical work submitted twice is one task.
    assert farm_multi_client.same_work_twice(site)
    assert len(jobs(state)) == 12, "the second submission must add four jobs, not eight"

    # Two sessions: different key namespaces, so both callers really reach
    # `execute` and the journal claim is what prevents the duplicate.
    assert farm_multi_client.two_controllers(site)
    assert len(jobs(state)) == 16, "the claim must refuse the second caller, not queue it"

    # And the concurrent record still reuses cleanly from a single session.
    assert farm_multi_client.all_reused(site)
    assert len(jobs(state)) == 16, "a reused invocation must reach no substrate"

    # Every attempt's folded journal agrees with its published manifest.
    # `publish_terminal` runs outside the claim, so this is the observable form
    # of the race `docs/attempt-claim-protocol.md` models.
    assert farm_multi_client.disagreements(site.root) == []

    # The identities are the record's, not the report's: one manifest per job.
    for record in jobs(state):
        assert (Path(site.root) / record["name"] / "manifest.json").is_file()
