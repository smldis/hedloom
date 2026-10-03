"""Authentication defaults and explicit operator overrides stay visible."""
import pytest

from hedloom_run.site import Site, SiteError


def pool(**options):
    return {"kind": "lsf-pooled", "max_jobs": 1, **options}


def test_pool_authentication_default_and_scoped_override(tmp_path):
    secure = Site(records_dir=str(tmp_path), placements={"first": pool(), "second": pool()})
    assert secure.placements["first"]["authentication"] == "tls"
    insecure = secure.overridden({"placement": {"first": {"authentication": "none"}}})
    assert insecure.placements["first"]["authentication"] == "none"
    assert insecure.placements["second"]["authentication"] == "tls"
    assert secure.placements["first"]["authentication"] == "tls"
    assert "authentication" not in insecure.transports["first"].settings


@pytest.mark.parametrize("authentication", ["tls", "none"])
def test_profile_authentication_and_local_debug(tmp_path, authentication):
    profile = tmp_path / "site.toml"
    profile.write_text('[study]\nrecords_dir="records"\n'
                       '[placement.pool]\nkind="lsf-pooled"\nmax_jobs=1\n'
                       f'authentication="{authentication}"\n')
    site = Site.from_file(profile)
    assert site.placements["pool"]["authentication"] == authentication
    local = site.served_in_process()
    assert local.placements["pool"]["kind"] == "in-process"
    assert "authentication" not in local.placements["pool"]
    assert site.placements["pool"]["authentication"] == authentication


@pytest.mark.parametrize("authentication", [False, True, None, "tcp", "off"])
def test_invalid_authentication_refuses_instead_of_weakening(tmp_path, authentication):
    with pytest.raises(SiteError, match="authentication"):
        Site(records_dir=str(tmp_path), placements={"pool": pool(authentication=authentication)})


def test_network_authentication_setting_requires_network_placement(tmp_path):
    with pytest.raises(SiteError, match="only to lsf-pooled"):
        Site(records_dir=str(tmp_path), placements={"local": {"authentication": "none"}})
