"""What a study exported, read back by the names its author gave.

A run's outputs are the ones the Plan exports. That is the whole claim these
hold, and every case here is a way it used to be wrong: `StudyRun.value`
answered with the last invocation in report order, so a study whose conclusion
was authored in the middle got a neighbour's number, and appending an unrelated
step silently changed what the study appeared to conclude.

The other half is that an output nobody produced must say so. A failed or
blocked producer has no value, and `None` is a value a body may legitimately
return, so unavailability is raised rather than returned.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from hedloom import (
    OutputUnavailable,
    Site,
    StudyOutput,
    StudyRun,
    address,
    artifact,
    directory,
    file,
    flow,
    input_artifact,
    local,
    operation,
    parameter,
    returned,
    study,
    sweep,
)
from hedloom_flow import AuthoringError

TEXT = artifact("text-file")
COUNT = artifact("count")
VERDICT = artifact("verdict")


@pytest.fixture
def site(tmp_path):
    return Site(
        root=str(tmp_path / "attempts"),
        workspace_root=str(tmp_path / "work"),
    )


@operation(config={"word": parameter(str)},
           outputs={"note": file("note.txt", kind="text-file")})
def o_write(out, *, word: str) -> None:
    out.note.write_text(word * 3)


@operation(inputs={"note": TEXT}, outputs={"size": returned(kind="count")})
def o_measure(note) -> int:
    return len(Path(note).read_text())


@operation(inputs={"size": COUNT}, outputs={"verdict": returned(kind="verdict")})
def o_evaluate(size) -> dict:
    """An evaluation. Returning a failing verdict is a successful execution."""

    return {"passes": size > 100, "measured": size}


@study(default_policy=local())
def _measured_and_evaluated():
    """The conclusion is authored in the middle, and exported by name."""

    measured = o_measure.named("measure")(o_write.named("write")(word="ab"))
    verdict = o_evaluate.named("evaluate")(measured)
    return {"measurements": measured, "verdict": verdict}


@operation(inputs={"verdict": VERDICT}, outputs={"note": returned(kind="text")})
def o_record(verdict) -> str:
    """A step after the conclusion, which the study does not export."""

    return "filed {} verdict".format("passing" if verdict["passes"] else "failing")


@study(default_policy=local())
def _with_a_later_step():
    """The same study, plus one invocation that must run last and is not exported.

    It consumes the verdict, so dependency order puts it at the end of the
    report whatever the invocation identities hash to. That is what makes this
    the case the deleted aggregate got wrong rather than a case it might.
    """

    measured = o_measure.named("measure")(o_write.named("write")(word="ab"))
    verdict = o_evaluate.named("evaluate")(measured)
    o_record.named("record")(verdict)
    return {"measurements": measured, "verdict": verdict}


def test_an_exported_output_is_addressed_by_name_and_by_its_producer(site):
    """The author says what a study produced, and which invocation produced it."""

    run = _measured_and_evaluated().submit(site=site)
    assert run.succeeded, run.summary()

    assert set(run.outputs) == {"measurements", "verdict"}
    assert run.outputs["measurements"].value == 6
    assert run.outputs["verdict"].value == {"passes": False, "measured": 6}
    assert run.outputs["verdict"].authored_key == "evaluate"
    assert run.outputs["measurements"].authored_key == "measure"


def test_a_later_invocation_changes_no_exported_result(site):
    """Appending a step must not change what the study says it produced.

    `record` consumes the verdict, so it is last in the report by dependency
    rather than by chance — and the exported conclusion is still `evaluate`'s.
    """

    plain = _measured_and_evaluated().submit(site=site)
    extended = _with_a_later_step().submit(site=site)

    assert extended.succeeded, extended.summary()
    assert extended.report.outcomes[-1].authored_key == "record"
    assert extended.report.outcomes[-1].value == "filed failing verdict"
    assert extended.outputs["verdict"].value == plain.outputs["verdict"].value
    assert (
        extended.outputs["measurements"].value
        == plain.outputs["measurements"].value
    )
    assert set(extended.outputs) == {"measurements", "verdict"}, (
        "an unexported step is not an output"
    )


def test_several_exports_stay_several(site):
    """No unwrapping, no preferred entry, no verdict-shaped magic."""

    run = _measured_and_evaluated().submit(site=site)

    assert len(run.outputs) == 2
    assert sorted(run.outputs) == ["measurements", "verdict"]
    assert not hasattr(run.outputs, "value")


@operation(outputs={"nothing": returned()})
def o_returns_none() -> None:
    return None


@study(default_policy=local())
def _exports_none():
    return {"nothing": o_returns_none.named("none")()}


def test_a_successful_none_is_a_value_and_not_an_absence(site):
    """The distinction the old aggregate could not make."""

    run = _exports_none().submit(site=site)

    assert run.succeeded, run.summary()
    assert run.outputs["nothing"].available
    assert run.outputs["nothing"].value is None


@study(default_policy=local())
def _exports_nothing():
    """Work with no declared conclusion. It has outputs: none of them."""

    o_measure.named("measure")(o_write.named("write")(word="ab"))
    return None


def test_a_study_that_exports_nothing_has_an_empty_mapping(site):
    run = _exports_nothing().submit(site=site)

    assert run.succeeded, run.summary()
    assert dict(run.outputs) == {}
    assert len(run.outputs) == 0


def test_a_name_the_study_never_exported_raises_key_error(site):
    run = _measured_and_evaluated().submit(site=site)

    with pytest.raises(KeyError) as raised:
        run.outputs["conclusion"]

    assert "conclusion" in str(raised.value)
    assert "measurements" in str(raised.value), "say what it does export"


@operation(config={"word": parameter(str)},
           outputs={"note": file("note.txt", kind="text-file")})
def o_refuses(out, *, word: str) -> None:
    if word == "bad":
        raise RuntimeError("this point fails")
    out.note.write_text(word)


@study(default_policy=local())
def _exports_a_failing_branch():
    failed = o_refuses.named("write")(word="bad")
    return {"note": failed, "size": o_measure.named("measure")(failed)}


def test_a_failed_producer_refuses_rather_than_answering_none(site):
    """An output nobody produced is not `None`; it is not there."""

    run = _exports_a_failing_branch().submit(site=site, stop_on_failure=False)

    assert not run.succeeded
    note = run.outputs["note"]
    assert not note.available
    assert note.outcome.outcome == "failed"
    with pytest.raises(OutputUnavailable) as raised:
        note.value
    assert "note" in str(raised.value)
    assert "this point fails" in str(raised.value), "say why, from the record"


def test_a_blocked_producer_refuses_and_says_which_invocation(site):
    run = _exports_a_failing_branch().submit(site=site, stop_on_failure=False)

    size = run.outputs["size"]
    assert not size.available
    assert size.outcome.outcome == "blocked"
    assert size.authored_key == "measure"
    with pytest.raises(OutputUnavailable) as raised:
        size.value
    assert "blocked" in str(raised.value)


@operation(
    config={"word": parameter(str)},
    outputs={
        "note": file("note.txt", kind="text-file"),
        "length": returned(kind="count"),
    },
)
def o_writes_and_returns(out, *, word: str) -> int:
    out.note.write_text(word * 3)
    return len(word) * 3


@study(default_policy=local())
def _exports_both_ports():
    both = o_writes_and_returns.named("both")(word="ab")
    return {"note": both.note, "length": both.length}


def test_each_exported_port_resolves_to_its_own_output(site):
    """The port, not the producer's whole result.

    One invocation with two declared outputs: the file port resolves to the
    recorded artifact's address, the returned port to what the body returned.
    Answering both with the producer's return value would make the file export
    a number.
    """

    run = _exports_both_ports().submit(site=site)
    assert run.succeeded, run.summary()

    note = run.outputs["note"]
    length = run.outputs["length"]

    assert note.invocation_id == length.invocation_id, "one producer, two ports"
    assert note.output_name == "note"
    assert length.value == 6
    assert note.value == note.artifact["address"]
    assert Path(note.value).read_text() == "ababab"
    assert note.artifact["kind"] == "file"
    assert length.artifact["kind"] == "value"


def test_a_file_output_exposes_its_reference_and_not_its_bytes(site):
    """Reading the artifact is the caller's decision, never an implied one."""

    run = _exports_both_ports().submit(site=site)
    note = run.outputs["note"]

    assert isinstance(note.value, str)
    assert note.artifact["size"] == 6
    assert "modified_ns" in note.artifact


@operation(config={"count": parameter(int)},
           outputs={"pages": directory("pages", kind="bundle")})
def o_writes_a_directory(out, *, count: int) -> None:
    out.pages.mkdir(parents=True)
    for index in range(count):
        (out.pages / f"page{index}.txt").write_text("x" * (index + 1))


@study(default_policy=local())
def _exports_a_directory():
    return {"pages": o_writes_a_directory.named("pages")(count=3)}


def test_a_directory_output_exposes_its_recorded_tree(site):
    run = _exports_a_directory().submit(site=site)
    assert run.succeeded, run.summary()

    pages = run.outputs["pages"]
    assert pages.artifact["kind"] == "directory"
    assert Path(pages.value).is_dir()
    assert sorted(item.name for item in Path(pages.value).iterdir()) == [
        "page0.txt",
        "page1.txt",
        "page2.txt",
    ]
    assert pages.artifact["size"] == 1 + 2 + 3, "the payload, not the inode"


@operation(inputs={"given": TEXT}, outputs={"size": returned(kind="count")})
def o_measure_source(given) -> int:
    return len(Path(given).read_text())


def test_exporting_a_declared_source_is_refused_where_it_is_authored():
    """Hedloom Flow permits no source export, so no run has to resolve one.

    Recorded as a test rather than assumed: the façade's refusal to invent a
    producing invocation for a non-output reference is only unreachable while
    this holds.
    """

    @study(default_policy=local(), name="tests.exports-a-source")
    def _exports_a_source():
        given = input_artifact(address("fixtures", "given.txt"), artifact=TEXT)
        o_measure_source.named("read")(given)
        return {"given": given}

    with pytest.raises(AuthoringError) as raised:
        _exports_a_source()

    assert "not an input source" in str(raised.value)


def test_a_reference_the_run_cannot_resolve_is_kept_and_refused():
    """An export is never dropped, and never given a producer it did not have."""

    exported = StudyOutput(
        name="given",
        reference={"type": "source", "source_id": "source:0"},
        outcome=None,
    )

    assert not exported.available
    with pytest.raises(OutputUnavailable) as raised:
        exported.value
    assert "'source'" in str(raised.value)


def test_reused_outputs_are_the_recorded_ones_and_say_they_were_reused(site):
    """A second submission answers from the record, not from a fresh number."""

    first = _measured_and_evaluated().submit(site=site)
    second = _measured_and_evaluated().submit(site=site)

    assert len(second.report.reused) == len(second.report.outcomes)
    assert second.outputs["verdict"].value == first.outputs["verdict"].value
    assert second.outputs["verdict"].outcome.reused
    assert not first.outputs["verdict"].outcome.reused


@pytest.mark.parametrize("kernel", [{"sequential": True}, {}])
def test_both_kernels_export_the_same_outputs(tmp_path, kernel):
    """Which kernel decides readiness changes how long a plan takes, not this."""

    site = Site(
        root=str(tmp_path / f"attempts-{'seq' if kernel else 'graph'}"),
        workspace_root=str(tmp_path / f"work-{'seq' if kernel else 'graph'}"),
    )

    run = _measured_and_evaluated().submit(site=site, **kernel)

    assert run.succeeded, run.summary()
    assert {name: item.value for name, item in run.outputs.items()} == {
        "measurements": 6,
        "verdict": {"passes": False, "measured": 6},
    }
    assert {name: item.authored_key for name, item in run.outputs.items()} == {
        "measurements": "measure",
        "verdict": "evaluate",
    }


def test_a_run_has_no_aggregate_value(site):
    """The property this change deleted, and why it cannot come back quietly.

    "The last invocation's value" is not the study's conclusion. It was one, by
    coincidence, for plans whose conclusion happened to be authored last.
    """

    run = _measured_and_evaluated().submit(site=site)

    assert not hasattr(run, "value")
    assert not hasattr(StudyRun, "value")
    assert "value" not in dir(run)


def test_execution_success_is_not_the_verdict_it_carried(site):
    """A successful computation returning a failing verdict stays successful."""

    run = _measured_and_evaluated().submit(site=site)

    assert run.succeeded, run.summary()
    assert run.outputs["verdict"].value["passes"] is False
    assert run.outputs["verdict"].available, (
        "an evaluation that ran produced its verdict, whatever the verdict says"
    )
