import inspect
import importlib

import pytest

from hedloom import Site, operation, returned, study
from hedloom.session import Session
from hedloom.study import Study, _apply_automatic_retention, submit

study_module = importlib.import_module("hedloom.study")


def _site(tmp_path):
    return Site(
        root=str(tmp_path / "records"), workspace_root=str(tmp_path / "work"),
        retention={
            "floor": "0s",
            "rule": [
                {"name": "failures", "outcome": ["failed"]},
                {"name": "successes", "outcome": ["succeeded"]},
            ],
            "automatic": {"after_run": ["failures"]},
        },
    )


def test_a_run_applies_only_the_rules_the_site_names(tmp_path, monkeypatch):
    captured = {}

    class Found:
        def apply(self, **options):
            captured["options"] = options

    def fake_survey(root, policy, *, workspace_root):
        captured["root"] = root
        captured["workspace_root"] = workspace_root
        captured["rules"] = tuple(rule.name for rule in policy.rules)
        return Found()

    monkeypatch.setattr(study_module, "survey", fake_survey)
    _apply_automatic_retention(_site(tmp_path))
    assert captured["rules"] == ("failures",)
    assert captured["options"] == {"actor": "automatic-after-run"}


def test_a_completed_run_reaches_the_post_run_trigger(tmp_path, monkeypatch):
    called = []

    @operation(outputs={"answer": returned()})
    def answer():
        return 42

    @study
    def subject():
        return answer()

    site = _site(tmp_path)
    monkeypatch.setattr(
        study_module, "_apply_automatic_retention", lambda selected: called.append(selected)
    )
    run = subject().submit(site=site, sequential=True)
    assert run.succeeded
    assert called == [site]


def test_no_automatic_rules_means_no_post_run_pass(tmp_path, monkeypatch):
    site = Site(root=str(tmp_path / "records"),
                workspace_root=str(tmp_path / "work"))
    monkeypatch.setattr(
        study_module, "survey",
        lambda *args, **kwargs: pytest.fail("survey should not run"),
    )
    _apply_automatic_retention(site)


def test_a_prune_failure_warns_and_does_not_fail_the_run(tmp_path, monkeypatch):
    monkeypatch.setattr(
        study_module, "survey",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("disk busy")),
    )
    with pytest.warns(RuntimeWarning, match="disk busy"):
        assert _apply_automatic_retention(_site(tmp_path)) is None


def test_submit_has_no_prune_argument():
    assert "prune" not in inspect.signature(Study.submit).parameters
    assert "prune" not in inspect.signature(submit).parameters
    assert "prune" not in inspect.signature(Session.submit).parameters
