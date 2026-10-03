"""One file authors a study and runs it.

What these hold is the seam that used to be hand-written: that the bodies which
run are the ones the Plan names, that a declared output lands where the
operation said it would, and that nothing is spent before `submit`.
"""

from runtime_helpers import run_study

import importlib
import inspect
import os
from pathlib import Path

import pytest

from hedloom import (
    Site,
    Study,
    address,
    artifact,
    directory,
    file,
    flow,
    input_artifact,
    local,
    operation,
    parameter,
    plan,
    returned,
    shell,
    study,
    sweep,
)
from hedloom.binding import BoundTransport, Shell, Workspace
from hedloom_exec.transport import RECORDED_TEXT_LIMIT, SubmissionRefused
from hedloom_exec.reuse import scan_attempts

TEXT = artifact("text-file")
COUNT = artifact("count")


@operation(config={"word": parameter(str)}, outputs={"note": file("note.txt",
                                                                 kind="text-file")})
def write_note(out, *, word: str) -> None:
    out.note.write_text(word * 3)


@operation(inputs={"note": TEXT}, outputs={"size": returned(kind="count")})
def measure(note) -> int:
    return {'size': len(Path(note).read_text())}


@operation(config={"word": parameter(str)},
           outputs={"copy": file("copy.txt", kind="text-file")})
def copy_via_shell(out, *, word: str):
    return shell("sh", "-c", f"printf %s {word} > {out.copy}")




@operation(inputs={"given": TEXT}, outputs={"size": returned(kind="count")})
def measure_source(given) -> int:
    """A body whose input is a file this study did not create."""

    return {'size': len(Path(given).read_text())}


@flow
def notes(words):
    sizes = []
    for word in sweep(words, key=lambda item: item):
        sizes.append(measure(write_note(word=word)))
    return {"sizes": sizes[-1]}


@study(default_policy=local())
def build(words=("ab", "cde")):
    return notes.named("notes")(words)


@pytest.fixture
def site(tmp_path):
    return Site(records_dir=str(tmp_path / "attempts"),
                work_dir=str(tmp_path / "work"), runs_dir=str(tmp_path / "attempts") + "-history")


def test_the_plan_is_complete_before_anything_is_spent(tmp_path):
    subject = build()
    document = subject.document

    assert subject.name == f"{__name__}.build"
    assert document["schema_version"] == 4
    assert len(document["invocations"]) == 4
    assert not (tmp_path / "attempts").exists(), "summary must spend nothing"
    assert "write_note" in subject.summary()


def test_the_body_that_runs_is_the_one_the_plan_names(site):
    run = run_study(build(), site=site, name="test-run")

    assert run.study_name == f"{__name__}.build"
    assert run.succeeded, run.summary()
    assert run["ab:measure"].value["size"] == 6
    assert run["cde:measure"].value == {"size": 9}


def test_stop_on_failure_defaults_true_and_can_allow_independent_branches(site):
    from hedloom import Runtime
    assert inspect.signature(Runtime.submit).parameters["stop_on_failure"].default is True
    run = run_study(_one_failing_branch(), site=site, name="continue", stop_on_failure=False)
    assert run["bad:refuses_one_word"].outcome == "failed"
    assert run["good:measure"].outcome == "succeeded"


def test_runtime_replaces_blocking_submission_surfaces(site):
    import hedloom
    from hedloom import runtime
    assert not hasattr(hedloom, "submit")
    assert not hasattr(Study, "submit")
    with runtime(site) as live:
        with pytest.raises(TypeError):
            live.submit(object(), name="invalid")


def test_a_declared_file_lands_where_the_operation_said(site):
    run = run_study(build(), site=site, name="test-run")

    address = run["ab:write_note"].artifacts["note"]["address"]
    assert Path(address).name == "note.txt"
    assert Path(address).read_text() == "ababab"


def test_the_plan_carries_what_implements_each_operation(tmp_path):
    document = build().document
    # Named from this module, whatever pytest imported it as.
    definitions = {
        item["identity"]["name"]: item for item in document["operations"]
    }
    name = f"{write_note.identity.name}"
    implementation = definitions[name]["implementation"]

    assert name.endswith(".write_note"), name
    assert implementation["entry_point"].endswith(":write_note")
    assert implementation["fingerprint"]


def test_a_second_run_reuses_everything(site):
    run_study(build(), site=site, name="test-run")
    again = run_study(build(), site=site, name="test-run")

    assert all(item.reused for item in again.report.outcomes), again.summary()


def test_one_edited_point_reruns_only_its_own_branch(site):
    run_study(build(), site=site, name="test-run")
    edited = run_study(build(words=("ab", "xyz")), site=site, name="test-run")

    outcomes = {item.authored_key: item for item in edited.report.outcomes}
    assert outcomes["ab:write_note"].reused
    assert outcomes["ab:measure"].reused
    assert not outcomes["xyz:write_note"].reused
    assert not outcomes["xyz:measure"].reused


def test_a_sweep_keys_every_call_inside_it(tmp_path):
    keys = {
        item["authored_key"] for item in build().document["invocations"]
    }
    assert {"ab:write_note", "ab:measure", "cde:write_note", "cde:measure"} == keys


def test_a_body_may_ask_for_a_command_to_be_run(site):
    @study(default_policy=local())
    def copies():
        return {"copy": copy_via_shell.named("copy")(word="hello").copy}

    run = run_study(copies(), site=site, name="test-run")

    assert run.succeeded, run.summary()
    address = run["copy"].artifacts["copy"]["address"]
    assert Path(address).read_text() == "hello"


def test_a_study_captures_a_declared_directory_output(site):
    @operation(outputs={"bundle": directory("bundle", kind="text-bundle")})
    def write_bundle(out):
        out.bundle.mkdir()
        (out.bundle / "first.txt").write_text("abc")
        nested = out.bundle / "nested"
        nested.mkdir()
        (nested / "second.txt").write_text("de")

    @study(default_policy=local())
    def bundles():
        return write_bundle.named("bundle")()

    run = run_study(bundles(), site=site, name="test-run")

    assert run.succeeded, run.summary()
    captured = run["bundle"].artifacts["bundle"]
    assert captured["kind"] == "directory"
    assert captured["size"] == 5
    assert Path(captured["address"]).is_dir()


def test_an_operation_with_no_bound_body_refuses(tmp_path):
    transport = BoundTransport({})
    with pytest.raises(SubmissionRefused):
        transport.submit("hedloom-abc", {"operation": "nobody.implements.this"})


def test_bound_body_failure_uses_the_exec_recording_limit(monkeypatch, capsys):
    binding_module = importlib.import_module("hedloom.binding")
    formatted = "traceback-prefix\n" + "x" * (RECORDED_TEXT_LIMIT + 1)
    monkeypatch.setattr(binding_module.traceback, "format_exc", lambda: formatted)
    transport = BoundTransport(
        {"explode": lambda: (_ for _ in ()).throw(ValueError("failure"))}
    )

    observation = transport.poll(
        transport.submit("attempt", {"operation": "explode"})
    )

    assert observation.detail["error"] == "ValueError: failure"
    assert observation.detail["traceback"] == formatted[-RECORDED_TEXT_LIMIT:]
    assert capsys.readouterr().err == formatted


def test_a_workspace_offers_only_declared_file_outputs(tmp_path):
    workspace = Workspace(tmp_path, {"raw": {"path": "point.raw"},
                                     "directory": {"path": "directory.txt"},
                                     "value": {"value": True}})

    assert Path(workspace) == tmp_path
    assert os.fspath(workspace) == str(tmp_path)
    assert str(workspace) == str(tmp_path)
    assert f"{workspace}" == str(tmp_path)
    assert workspace.raw == tmp_path / "point.raw"
    assert workspace.directory == tmp_path / "directory.txt"
    with pytest.raises(AttributeError):
        workspace.value
    with pytest.raises(AttributeError):
        workspace.undeclared


@study(default_policy=local())
def _reads_a_source():
    given = input_artifact(
        address("fixtures", "given.txt"),
        artifact=TEXT,
    )
    return {"size": measure_source.named("read")(given).size}


@pytest.fixture
def fixtures(tmp_path):
    directory = tmp_path / "fixtures"
    directory.mkdir()
    (directory / "given.txt").write_text("abcde")
    return directory


@pytest.fixture
def reading_site(tmp_path, fixtures):
    return Site(
        records_dir=str(tmp_path / "attempts"),
        work_dir=str(tmp_path / "work"),
        address_spaces={"fixtures": str(fixtures)},
        runs_dir=str(tmp_path / "attempts") + "-history",
    )


def test_a_declared_source_reaches_the_body_that_asked_for_it(reading_site):
    """A study may start from a file it did not write.

    Every real study does: a input file, a model card, a point file someone else
    owns. Until sources were seeded this body was handed None, and the only way
    to read an external file was to write its path into a second file by hand.
    """

    run = run_study(_reads_a_source(), site=reading_site, name="test-run")

    assert run.succeeded, run.summary()
    assert run["read"].value["size"] == 5


def test_editing_a_source_reruns_the_work_that_read_it(reading_site, fixtures):
    """Delivery and staleness read the same file, so they cannot disagree."""

    first = run_study(_reads_a_source(), site=reading_site, name="test-run")
    assert first["read"].value["size"] == 5

    (fixtures / "given.txt").write_text("abcdefgh")
    again = run_study(_reads_a_source(), site=reading_site, name="test-run")

    assert not again.report.outcomes[0].reused, again.summary()
    assert again["read"].value["size"] == 8


def test_an_unedited_source_reuses_what_read_it(reading_site):
    run_study(_reads_a_source(), site=reading_site, name="test-run")
    again = run_study(_reads_a_source(), site=reading_site, name="test-run")

    assert all(item.reused for item in again.report.outcomes), again.summary()


def test_a_command_renders_as_something_an_operator_can_read():
    assert str(shell("awk", "-f", Path("/tmp/x.awk"))) == "awk -f /tmp/x.awk"
    assert isinstance(shell("true"), Shell)


def test_an_overridden_run_lands_on_the_same_attempts(site):
    """The safety claim under `runtime(site, override)`, end to end.

    Placement is a scheduling concern and identity a semantic one, so a run that
    spent less of the site must be reusable by one that did not. If an override
    ever reached identity, this reruns instead of reusing.
    """

    thrifty = run_study(build(), site=site, override={"kernel": {"threads": 1}}, name="test-run")
    plain = run_study(build(), site=site, name="test-run")

    assert thrifty.succeeded, thrifty.summary()
    assert all(item.reused for item in plain.report.outcomes), plain.summary()


def test_locally_runs_a_farm_study_here_without_contacting_the_farm(tmp_path):
    """The debugging pair: local execution and the same identities."""

    farm_site = Site(
        records_dir=str(tmp_path / "attempts"),
        work_dir=str(tmp_path / "work"),
        placements={
            "lsf": {"kind": "lsf-interactive", "walltime": "1", "max_jobs": 4}
        },
        runs_dir=str(tmp_path / "attempts") + "-history",
    )
    subject = build(("ab",))

    # No farm submission should be reached; the Runtime executes bodies locally.
    debugged = run_study(subject, site=farm_site, locally=True, name="test-run")

    assert debugged.succeeded, debugged.summary()
    assert debugged["ab:measure"].value["size"] == 6


def test_a_runtime_holds_one_controller_for_several_runs(site):
    from hedloom import runtime
    subject = build()
    with runtime(site) as live:
        live.ready()
        thread = live._thread
        first = live.submit(subject, name="first").wait()
        second = live.submit(subject, name="second").wait()
        assert live._thread is thread and thread.is_alive()
    assert first.succeeded, first.summary()
    assert all(item.reused for item in second.report.outcomes), second.summary()


def test_runtime_accepts_several_receipts_and_retires_sequential_mode(site):
    from hedloom import runtime
    with pytest.raises(TypeError):
        runtime(site, sequential=True)
    with runtime(site) as live:
        receipts = {name: live.submit(build(), name=name) for name in ("one", "two")}
        runs = {name: receipt.wait() for name, receipt in receipts.items()}
    assert set(runs) == {"one", "two"}
    assert all(run.succeeded for run in runs.values())


def test_dask_globals_survive_a_runtime(site):
    dask = pytest.importorskip("dask")
    from hedloom import runtime
    before = dask.config.get("scheduler", None)
    with runtime(site) as live:
        assert live.submit(build(), name="test-run").wait().succeeded
    assert dask.config.get("scheduler", None) == before


@operation(config={"word": parameter(str)},
           outputs={"note": file("note.txt", kind="text-file")})
def refuses_one_word(out, *, word: str) -> None:
    if word == "bad":
        raise RuntimeError("this point fails")
    out.note.write_text(word)


@flow
def pairs(words):
    return {
        word: measure(refuses_one_word(word=word))
        for word in sweep(words, key=lambda item: item)
    }


@study(default_policy=local())
def _one_failing_branch():
    return pairs.named("pairs")(("bad", "good"))


@pytest.mark.parametrize("threads", [1, 2])
def test_executor_capacities_block_dependents_and_let_others_finish(tmp_path, threads):
    """A failure blocks what named its result, whatever stop_on_failure says.

    The sequential kernel used to *run* the dependent: its input did not exist,
    so it spent an attempt and published `failed` with a TypeError blaming the
    operation for an absent upstream artifact. On a farm that is a real `bsub -I`
    spent on work that could not succeed. The graph kernel always refused it, and
    a study has to mean the same thing under either.
    """

    site = Site(
        records_dir=str(tmp_path / f"attempts-{threads}"),
        work_dir=str(tmp_path / f"work-{threads}"),
        runs_dir=str(tmp_path / f"attempts-{threads}") + "-history",
        threads=threads,
    )
    run = run_study(_one_failing_branch(), site=site, stop_on_failure=False, name="test-run")

    outcomes = {item.authored_key: item for item in run.report.outcomes}
    assert outcomes["bad:refuses_one_word"].outcome == "failed"
    assert outcomes["bad:measure"].outcome == "blocked"
    assert outcomes["bad:measure"].disposition == "skipped"
    assert outcomes["bad:measure"].error is None, (
        "a blocked invocation never ran, so it has no error of its own"
    )
    # The branch that has nothing to do with the failure still finishes.
    assert outcomes["good:refuses_one_word"].outcome == "succeeded"
    assert outcomes["good:measure"].value["size"] == 4


def test_a_study_is_a_function_and_calling_it_spends_nothing(tmp_path):
    """What `@study` leaves behind, and what it costs to call.

    The decorated name is a family rather than one study, because the arguments
    are what distinguish its members. Calling it plans — which reads the
    registry, records invocations, and touches no site.
    """

    subject = build(("ab", "cde"))

    assert isinstance(subject, Study)
    assert len(subject.document["invocations"]) == 4
    assert not (tmp_path / "attempts").exists()
    assert build(("ab", "cde")).document == subject.document


def test_the_draft_form_and_the_decorated_form_are_one_plan():
    """`plan()` remains the escape hatch, and must not mean anything else."""

    with plan(default_policy=local()) as draft:
        outputs = notes.named("notes")(("ab", "cde"))
    explicit = study(draft.finish(outputs=outputs), name="explicit-notes")

    assert explicit.document == build().document


def test_a_finished_plan_requires_its_study_name():
    with plan(default_policy=local()) as draft:
        outputs = notes.named("notes")(("ab",))

    with pytest.raises(TypeError, match="needs name="):
        study(draft.finish(outputs=outputs))


def test_an_explicit_study_name_is_the_record_and_cli_namespace(site, capsys):
    @study(name="short-study", default_policy=local())
    def explicitly_named():
        return write_note.named("write")(word="named")

    subject = explicitly_named()
    run = run_study(subject, site=site, name="test-run")
    records = scan_attempts(site.records_dir)

    assert subject.name == "short-study"
    assert run.study_name == "short-study"
    assert "study short-study" in subject.summary().splitlines()[0]
    # The name is the study's, for authoring and run context. It is not on the
    # record, which is named by the computation and shared with anyone who
    # declares the same work.
    assert len(records) == 1
    assert run["write"].record == records[0].identity


def test_exported_output_names_do_not_rename_or_invalidate_a_study(site):
    @study(name="stable-study", default_policy=local())
    def renamed_output(output_name):
        note = write_note.named("write")(word="same").note
        return {output_name: note}

    first = run_study(renamed_output("first"), site=site, name="test-run")
    second = run_study(renamed_output("second"), site=site, name="test-run")

    assert first.study_name == second.study_name == "stable-study"
    assert second.report.outcomes[0].reused
    assert len(scan_attempts(site.records_dir)) == 1


def test_different_declarations_get_their_own_records(site):
    """Different words are different computations, hence different records.

    The study name separates nothing: two studies declaring the same word
    would share one record — `tests/test_shared_computation_identity.py` holds
    that half. Here the *words* differ, and that is what makes two records.
    """

    @study(name="first-study", default_policy=local())
    def first():
        return {"note": write_note.named("write")(word="one").note}

    @study(name="second-study", default_policy=local())
    def second():
        return {"note": write_note.named("write")(word="two").note}

    one = run_study(first(), site=site, name="test-run")
    two = run_study(second(), site=site, name="test-run")

    records = {item.identity for item in scan_attempts(site.records_dir)}
    assert records == {one["write"].record, two["write"].record}
    assert len(records) == 2


def test_two_study_definitions_cannot_claim_one_name():
    @study(name="conflicting-study-name")
    def first():
        return {}

    assert first.name == "conflicting-study-name"
    with pytest.raises(ValueError, match="already used by a different"):

        @study(name="conflicting-study-name")
        def second():
            return {}


@pytest.mark.parametrize("name", ["", " leading", "trailing ", "has:colon"])
def test_invalid_study_names_are_refused(name):
    with pytest.raises(ValueError, match="study name"):

        @study(name=name)
        def invalid():
            return {}


def test_two_operation_bodies_cannot_claim_one_facade_identity():
    @operation(name="tests.conflicting-operation")
    def first():
        return None

    assert first.identity.name == "tests.conflicting-operation"
    with pytest.raises(ValueError, match="already bound to a different body"):

        @operation(name="tests.conflicting-operation")
        def second():
            return None


def test_calling_the_family_is_what_produces_a_study():
    """The mistake this shape invites, answered with what to type instead."""

    with pytest.raises(AttributeError, match=r"call it first, then use live\.submit\(build\(\.\.\.\), name="):
        build.submit

    with pytest.raises(AttributeError):
        build.no_such_attribute


def test_a_finished_plan_cannot_be_given_a_default_policy():
    """It has already been authored; the policies in it are settled."""

    with plan(default_policy=local()) as draft:
        outputs = notes.named("notes")(("ab",))
    finished = draft.finish(outputs=outputs)

    with pytest.raises(TypeError, match="already carries its policies"):
        study(finished, default_policy=local())


def test_study_refuses_what_it_can_neither_plan_nor_pair():
    with pytest.raises(TypeError, match="a strategy to plan or a finished Plan"):
        study(42)
