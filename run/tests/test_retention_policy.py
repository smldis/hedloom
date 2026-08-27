import pytest

from hedloom_run.site import Site, SiteError


def _profile(tmp_path):
    profile = tmp_path / "site.toml"
    profile.write_text(
        """
[study]
root = "records"
workspace_root = "work"

[retention]
floor = "7d"

[[retention.rule]]
name = "spent failures"
outcome = ["failed", "cancelled"]
older_than = "14d"
keep_latest = 1
keep_logs = true

[retention.automatic]
after_run = ["spent failures"]
"""
    )
    return profile


def test_retention_rules_parse_from_a_site_profile(tmp_path):
    site = Site.from_file(_profile(tmp_path))
    assert site.retention["floor"] == "7d"
    assert site.retention["rule"][0]["name"] == "spent failures"
    assert site.retention["automatic"]["after_run"] == ["spent failures"]


def test_retention_survives_every_site_derivation(tmp_path):
    site = Site.from_file(_profile(tmp_path))
    assert site.with_transports().retention == site.retention
    assert site.overridden({"kernel": {"threads": 1}}).retention == site.retention
    assert site.served_in_process().retention == site.retention


def test_an_unknown_retention_key_is_refused_at_site_load(tmp_path):
    profile = tmp_path / "site.toml"
    profile.write_text(
        '[study]\nroot = "records"\n[retention]\nmystery = true\n'
    )
    with pytest.raises(SiteError, match="unknown key"):
        Site.from_file(profile)


def test_automatic_retention_must_name_an_existing_rule(tmp_path):
    profile = _profile(tmp_path)
    text = profile.read_text().replace('"spent failures"]', '"missing"]')
    profile.write_text(text)
    with pytest.raises(SiteError, match="unknown rule"):
        Site.from_file(profile)
