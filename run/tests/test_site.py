"""Sources are identified by what is at the address, not only by the address.

The first test here is the one that matters. Before it, editing an input
input file in place changed no declared fact, so every downstream invocation was
reused and a study reported results computed from a file that no longer existed
in that form — a plausible answer that could be false, which this project
treats as a defect rather than a limitation.
"""

from pathlib import Path

import pytest

from hedloom_exec.identity import attempt_identity, try_name

from hedloom_exec.transport import InProcessTransport
from hedloom_run.driver import run_plan
from hedloom_run.site import Site, SiteError, fingerprint_file


def read(model=None, **kwargs):
    """A source reference resolves to nothing here; the run still reads it."""

    return "read"


def transport():
    return InProcessTransport({"read": read})


def document(source_id="source:1"):
    return {
        "schema_version": 2,
        "sources": [
            {
                "id": source_id,
                "artifact": {"kind": "input file"},
                "address": {"address_space": "repo", "locator": "ota.cir"},
                "materialized_as": {"codec": {"name": "solve", "version": "1"}},
            }
        ],
        "operations": [
            {"identity": {"name": "read", "version": "1"},
             "outputs": [{"name": "out"}]}
        ],
        "invocations": [
            {
                "id": "invoke:a",
                "authored_key": "reader",
                "operation": {"name": "read", "version": "1"},
                "config": [],
                "inputs": [
                    {
                        "cardinality": "scalar",
                        "name": "model",
                        "reference": {"type": "source", "source_id": source_id},
                    }
                ],
                "policy": {"name": "local", "options": {}},
            }
        ],
    }


@pytest.fixture
def study(tmp_path):
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    (inputs / "ota.cir").write_text("* ota\nV1 vdd 0 1.8\n")
    return Site(root=str(tmp_path / "attempts"),
                address_spaces={"repo": str(inputs)})


def run(document, site, tmp_path):
    return run_plan(
        document,
        transport(),
        plan_id="study",
        root=site.root,
        source_fingerprints=site.fingerprints(document),
    )


def test_editing_a_source_in_place_invalidates_the_work_that_read_it(
    study, tmp_path
):
    """The defect this module exists to close."""

    first = run(document(), study, tmp_path)
    (tmp_path / "inputs" / "ota.cir").write_text("* ota\nV1 vdd 0 1.62\n")
    second = run(document(), study, tmp_path)

    assert first.outcomes[0].ran
    assert not second.outcomes[0].reused, "an edited input file must rerun the study"
    assert first.outcomes[0].input_digest != second.outcomes[0].input_digest


def test_without_a_fingerprint_an_edit_is_invisible(study, tmp_path):
    """Documents the behaviour a run gets by declaring nothing. Not a feature."""

    first = run_plan(document(), transport(), plan_id="study", root=study.root)
    (tmp_path / "inputs" / "ota.cir").write_text("* edited\n")
    second = run_plan(document(), transport(), plan_id="study", root=study.root)

    assert first.outcomes[0].ran
    assert second.outcomes[0].reused, "declaration-only identity cannot see this"


def test_rewriting_identical_content_still_reuses(study, tmp_path):
    """Why content, not mtime: a checkout must not invalidate a sweep."""

    first = run(document(), study, tmp_path)
    source = tmp_path / "inputs" / "ota.cir"
    source.write_text(source.read_text())          # same bytes, new mtime

    second = run(document(), study, tmp_path)

    assert first.outcomes[0].input_digest == second.outcomes[0].input_digest
    assert second.outcomes[0].reused


def test_an_unrelated_source_still_invalidates_nothing(study, tmp_path):
    """Source identity remains positional-free: adding one changes no digest."""

    first = run(document(), study, tmp_path)
    second = run(document(source_id="source:9"), study, tmp_path)

    assert first.outcomes[0].input_digest == second.outcomes[0].input_digest


def test_an_address_space_this_site_does_not_define_is_refused(tmp_path):
    site = Site(root=str(tmp_path), address_spaces={})
    with pytest.raises(SiteError) as raised:
        site.fingerprints(document())
    assert "repo" in str(raised.value)


def test_a_declared_source_that_is_not_there_is_refused_early(tmp_path):
    site = Site(root=str(tmp_path), address_spaces={"repo": str(tmp_path)})
    with pytest.raises(SiteError) as raised:
        site.fingerprints(document())
    assert "does not exist" in str(raised.value)


def test_a_directory_source_covers_everything_under_it(tmp_path):
    tree = tmp_path / "base"
    (tree / "sub").mkdir(parents=True)
    (tree / "sub" / "a.cir").write_text("one")
    site = Site(root=str(tmp_path), address_spaces={"repo": str(tmp_path)})
    plan = document()
    plan["sources"][0]["address"]["locator"] = "base"

    before = site.fingerprints(plan)
    (tree / "sub" / "a.cir").write_text("two")
    after = site.fingerprints(plan)

    assert before != after, "editing a file inside a tree source must show"


def test_an_implausibly_large_source_says_how_it_was_identified(tmp_path):
    small = tmp_path / "small.cir"
    small.write_text("x")
    assert fingerprint_file(small).startswith("blake2b:")


def test_a_profile_anchors_relative_paths_to_itself(tmp_path):
    """A study run from elsewhere must mean the same thing."""

    (tmp_path / "site.toml").write_text(
        "[study]\n"
        'root = "_runs/attempts"\n'
        'workspace_root = "/nfs/studies/ota"\n'
        "\n[address_space]\n"
        'repository-relative = "."\n'
        "\n[placement.lsf]\n"
        'kind = "lsf-interactive"\n'
        'walltime = "240"\n'
        'queue = "normal"\n'
        'app = "spectre"\n'
        'memory_mb = 4096\n'
        'licences = { spectre = 1 }\n'
        'resources = "select[rh80] span[hosts=1]"\n'
        "max_jobs = 200\n"
        "\n[kernel]\n"
        "threads = 32\n"
    )
    site = Site.from_file(tmp_path / "site.toml")

    assert site.root == str(tmp_path / "_runs" / "attempts")
    assert site.workspace_root == "/nfs/studies/ota"
    assert site.address_spaces["repository-relative"] == str(tmp_path)
    assert site.threads == 32
    assert site.dashboard == "none"
    lsf = site.transports["lsf"]
    job = try_name(attempt_identity(plan_id="site", invocation_id="default").rendered, 0)
    argv = lsf.build_argv(job, {"command": ["simulate"]})
    assert argv[argv.index("-W") + 1] == "240"
    assert argv[argv.index("-app") + 1] == "spectre"
    assert argv.count("-R") == 1
    assert (
        argv[argv.index("-R") + 1]
        == "select[rh80] span[hosts=1] rusage[mem=4096,spectre=1]"
    )
    # Two numbers about two machines: local concurrency on the submit host, and
    # how many jobs this user may have in flight on the farm.
    assert site.capacity == {"lsf": 200, "local": 32}


def test_a_farm_placement_without_a_cap_is_refused(tmp_path):
    """No safe default exists, so the profile has to say.

    A small guess silently throttles a sweep; a large one authorises more
    concurrent jobs than the site permits and more live `bsub` clients than the
    submit host will carry. Both are expensive to discover on a farm.
    """

    (tmp_path / "site.toml").write_text(
        '[study]\nroot = "attempts"\n\n[placement.lsf]\n'
        'kind = "lsf-interactive"\nwalltime = "10"\n'
    )
    with pytest.raises(SiteError, match="max_jobs"):
        Site.from_file(tmp_path / "site.toml")


def test_local_exists_even_where_a_profile_never_mentions_it(tmp_path):
    """An operation declaring no policy resolves to `local` when the Plan is
    built, so the commonest plan there is names a placement no farm profile
    bothers to declare. The capacity has to be there anyway."""

    site = Site(root=str(tmp_path), placements={"lsf": 8}, threads=3)

    assert site.capacity["local"] == 3
    spec = site.cluster_spec()
    assert spec["lsf"] == {"nthreads": 8, "resources": {"placement:lsf": 8}}
    assert spec["local"] == {"nthreads": 3, "resources": {"placement:local": 3}}


def test_max_jobs_is_profile_vocabulary_not_a_bsub_argument(tmp_path):
    """It sizes the cluster; it must never reach the transport as a setting."""

    (tmp_path / "site.toml").write_text(
        '[study]\nroot = "attempts"\n\n[placement.lsf]\n'
        'kind = "lsf-interactive"\nwalltime = "10"\nmax_jobs = 4\n'
    )
    site = Site.from_file(tmp_path / "site.toml")

    assert site.capacity["lsf"] == 4
    assert not hasattr(site.transports["lsf"], "max_jobs")
    # It stays in the declaration, because a run may override it there; what it
    # must never do is reach the transport as a `bsub` setting.
    assert site.placements["lsf"]["max_jobs"] == 4
    assert "max_jobs" not in site.transports["lsf"].defaults


def test_an_invocation_overrides_a_profile_memory_default(tmp_path):
    """One placement vocabulary serves both the profile and the Plan."""

    (tmp_path / "site.toml").write_text(
        '[study]\nroot = "attempts"\n\n[placement.lsf]\n'
        'kind = "lsf-interactive"\nwalltime = "10"\nmemory_mb = 4096\n'
        "max_jobs = 4\n"
    )
    lsf = Site.from_file(tmp_path / "site.toml").transports["lsf"]
    bundle = {
        "command": ["simulate"],
        "placement": {
            "requested": {
                "name": "lsf",
                "options": {"memory_mb": 8192},
            }
        },
    }

    job = try_name(attempt_identity(plan_id="site", invocation_id="override").rendered, 0)
    argv = lsf.build_argv(job, bundle)
    assert argv[argv.index("-R") + 1] == "rusage[mem=8192]"


def test_a_misspelled_profile_option_names_the_placement_and_key(tmp_path):
    """A profile typo is a site error, never a constructor TypeError."""

    (tmp_path / "site.toml").write_text(
        '[study]\nroot = "attempts"\n\n[placement.lsf]\n'
        'kind = "lsf-interactive"\nwalltime = "10"\nqueeu = "reg"\n'
        "max_jobs = 4\n"
    )

    with pytest.raises(SiteError) as raised:
        Site.from_file(tmp_path / "site.toml")
    assert "lsf" in str(raised.value)
    assert "queeu" in str(raised.value)


def test_an_invalid_transport_default_is_a_named_site_error(tmp_path):
    """Profile validation belongs at the profile boundary, with its route name."""

    (tmp_path / "site.toml").write_text(
        '[study]\nroot = "attempts"\n\n[placement.farm]\n'
        'kind = "lsf-interactive"\nwalltime = "10"\nmemory_mb = -1\n'
        "max_jobs = 4\n"
    )

    with pytest.raises(SiteError) as raised:
        Site.from_file(tmp_path / "site.toml")
    assert "farm" in str(raised.value)
    assert "memory_mb" in str(raised.value)


def test_a_placement_kind_this_site_cannot_build_is_refused(tmp_path):
    (tmp_path / "site.toml").write_text(
        '[study]\nroot = "attempts"\n\n[placement.gpu]\nkind = "cuda-farm"\n'
    )
    with pytest.raises(SiteError) as raised:
        Site.from_file(tmp_path / "site.toml")
    assert "cuda-farm" in str(raised.value)


def test_implementations_are_added_where_configuration_cannot_reach(tmp_path):
    site = Site(root=str(tmp_path)).with_transports(local=transport())
    assert site.transports["local"].name == "in-process"


def test_a_relative_root_is_anchored_before_anything_uses_it(tmp_path, monkeypatch):
    """A relative root is silently wrong, so it never survives construction.

    A command runs *in* its attempt's workspace and is told where to write by a
    path built from that same workspace. Absolute, those agree. Relative, the
    command resolves the path again against the directory it was just placed
    in and writes nowhere — surfacing as a tool that could not open its
    own output file. `from_file` already anchors; this closes the same gap for
    a Site built in Python.
    """

    monkeypatch.chdir(tmp_path)
    site = Site(
        root="records",
        workspace_root="scratch",
        address_spaces={"here": "inputs"},
    )

    assert Path(site.root).is_absolute()
    assert Path(site.workspace_root).is_absolute()
    assert Path(site.address_spaces["here"]).is_absolute()
    assert Path(site.root) == tmp_path / "records"


def test_a_python_site_declares_a_placement_once(tmp_path):
    """The budget and the substrate are one declaration, not two that must agree.

    A caller used to pass `transports=` and `placements=` separately, naming the
    placement twice; a typo produced a budget with no way to reach it, found
    much later as `UnsupportedPlacement`.
    """

    site = Site(
        root=str(tmp_path),
        placements={
            "lsf": {
                "kind": "lsf-interactive",
                "queue": "reg",
                "walltime": "1",
                "max_jobs": 2,
            }
        },
        threads=3,
    )

    assert site.capacity == {"lsf": 2, "local": 3}
    assert site.transports["lsf"].name == "lsf-interactive"
    assert site.transports["lsf"].defaults["queue"] == "reg"


def test_a_profile_and_a_python_site_are_the_same_declaration(tmp_path):
    """One vocabulary, two notations. The refusals have to reach both."""

    (tmp_path / "site.toml").write_text(
        '[study]\nroot = "attempts"\n\n[placement.lsf]\n'
        'kind = "lsf-interactive"\nqueue = "reg"\nwalltime = "1"\nmax_jobs = 2\n'
        "\n[kernel]\nthreads = 3\n",
        encoding="utf-8",
    )
    from_file = Site.from_file(tmp_path / "site.toml")
    in_python = Site(
        root=str(tmp_path / "attempts"),
        placements={
            "lsf": {
                "kind": "lsf-interactive",
                "queue": "reg",
                "walltime": "1",
                "max_jobs": 2,
            }
        },
        threads=3,
    )

    assert from_file.capacity == in_python.capacity
    assert from_file.placements == in_python.placements
    assert (
        from_file.transports["lsf"].defaults == in_python.transports["lsf"].defaults
    )


def test_an_override_changes_how_a_run_executes_and_nothing_it_declares(tmp_path):
    """`reuse.py` settles that placement is not identity-bearing, so this is safe."""

    site = Site(
        root=str(tmp_path),
        placements={
            "lsf": {
                "kind": "lsf-interactive",
                "queue": "reg",
                "walltime": "1",
                "cores": 1,
                "max_jobs": 8,
            }
        },
        threads=4,
        dashboard="network",
    )
    thrifty = site.overridden(
        {"placement": {"lsf": {"max_jobs": 1, "queue": "express"}},
         "kernel": {"dashboard": "none"}}
    )

    assert thrifty.capacity["lsf"] == 1
    assert thrifty.transports["lsf"].defaults["queue"] == "express"
    assert thrifty.dashboard == "none"
    assert thrifty.root == site.root, "an override must not move the record"
    # The original is untouched: an override derives a site, it does not edit one.
    assert site.capacity["lsf"] == 8
    assert site.transports["lsf"].defaults["queue"] == "reg"


def test_an_override_refuses_what_would_change_meaning(tmp_path):
    site = Site(root=str(tmp_path), placements={"local": 2})

    with pytest.raises(SiteError, match="never what it means"):
        site.overridden({"study": {"root": "/somewhere/else"}})
    with pytest.raises(SiteError, match="does not offer"):
        site.overridden({"placement": {"lsf": {"max_jobs": 1}}})


def test_served_in_process_keeps_the_placements_and_drops_the_substrate(tmp_path):
    """For debugging a farm study here: same names, same budgets, no farm."""

    site = Site(
        root=str(tmp_path),
        placements={
            "lsf": {
                "kind": "lsf-interactive",
                "walltime": "1",
                "max_jobs": 4,
            }
        },
        threads=2,
    )
    here = site.served_in_process()

    assert here.capacity == site.capacity, "the Plan still resolves to these names"
    assert "lsf" not in here.transports, "nothing may leave the process"
    assert here.placements["lsf"]["kind"] == "in-process"
