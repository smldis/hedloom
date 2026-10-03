"""Fresh observations retain their evidence while equal artifacts reuse work."""

from runtime_helpers import run_study
import pytest
import json
import subprocess
from pathlib import Path

from hedloom import Site, artifact, directory, located, operation, parameter, returned, study
from hedloom import runtime, RunHistory
import time
import os
import shutil
from hedloom import file


@operation(execution="each_submission", config={"repository": parameter(str)},
           outputs={"repo": directory(kind="repository", external=True, identity="declared")})
def acquire_revision(*, repository):
    commit = subprocess.check_output(["git", "-C", repository, "rev-parse", "HEAD"], text=True).strip()
    return {"repo": located(repository, identity={"repository": repository, "commit": commit})}


@operation(inputs={"repo": artifact("repository")}, outputs={"report": returned()})
def read_revision(repo):
    return {"report": Path(repo, "payload").read_text()}


@study
def revision_study(repository):
    return {"report": read_revision.named("analysis")(acquire_revision.named("pull")(repository=repository))}


def git(path, *args):
    return subprocess.check_output(["git", "-C", str(path), *args], text=True).strip()


def test_latest_a_a_b_a_observations(tmp_path):
    repository = tmp_path / "repository"
    repository.mkdir()
    git(repository, "init", "-q")
    (repository / "payload").write_text("A")
    git(repository, "add", "payload")
    git(repository, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "A")
    a = git(repository, "rev-parse", "HEAD")
    (repository / "payload").write_text("B")
    git(repository, "add", "payload")
    git(repository, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "B")
    b = git(repository, "rev-parse", "HEAD")
    site = Site(records_dir=str(tmp_path / "records"), work_dir=str(tmp_path / "work"), runs_dir=str(tmp_path / "history"))
    runs = []
    for commit in (a, a, b, a):
        git(repository, "checkout", "-q", commit)
        run = run_study(revision_study(str(repository)), site=site, name="observe")
        assert run.succeeded, run.summary()
        runs.append(run)
    assert [run.outputs["report"].value for run in runs] == ["A", "A", "B", "A"]
    assert len({run["pull"].record for run in runs}) == 4
    assert [run["analysis"].reused for run in runs] == [False, True, False, True]
    assert all(run["pull"].try_number == 0 for run in runs)
    events_path = Path(site.records_dir) / runs[0]["analysis"].record / "events.jsonl"
    events = [json.loads(line) for line in events_path.read_text().splitlines()]
    bound = [event for event in events if event["event"] == "inputs_bound"]
    assert len(bound) == 1
    assert bound[0]["data"]["inputs"]["repo"]["producer"]["record"] == runs[0]["pull"].record
    assert [e["event"] for e in events].index("inputs_bound") < [e["event"] for e in events].index("submit_intent")
    bindings = [json.loads(path.read_text()) for path in (tmp_path / "history").rglob("execution.json")]
    candidates = [row["inputs"]["repo"] for row in bindings if "repo" in row.get("inputs", {})]
    assert {row["producer"]["record"] for row in candidates} == {run["pull"].record for run in runs}


@operation(execution="each_submission", config={"path": parameter(str)},
           outputs={"repo": directory(kind="repository", external=True, identity="declared")})
def borrow_copy(path):
    return {"repo": located(path, identity={"repository": "fixture", "commit": "A"})}


@operation(inputs={"repo": artifact("repository")}, config={"markers": parameter(str)}, outputs={"report": returned()})
def held_analysis(repo, markers):
    marker = Path(markers)
    with (marker / "calls").open("a") as stream:
        stream.write(repo + "\n")
    deadline = time.monotonic() + 10
    while not (marker / "release").exists():
        if time.monotonic() > deadline:
            raise TimeoutError("test release did not arrive")
        time.sleep(.01)
    return {"report": Path(repo, "payload").read_text()}


@study
def shared_revision(path, markers):
    return held_analysis.named("analysis")(borrow_copy.named("pull")(path=path), markers=markers)


@pytest.mark.parametrize("threads", [1, 2])
def test_independent_observations_join_analysis(tmp_path, threads):
    for name in ("one", "two"):
        (tmp_path / name).mkdir()
        (tmp_path / name / "payload").write_text("A")
    site = Site(records_dir=str(tmp_path / "records"), runs_dir=str(tmp_path / "history"), threads=threads)
    def wait(predicate):
        deadline = time.monotonic() + 10
        while not predicate():
            assert time.monotonic() < deadline, "barrier timeout"
            time.sleep(.01)
    with runtime(site) as live:
        a = live.submit(shared_revision(str(tmp_path / "one"), str(tmp_path)), name="a")
        try:
            wait(lambda: (tmp_path / "calls").exists())
            b = live.submit(shared_revision(str(tmp_path / "two"), str(tmp_path)), name="b")
            # One graph slot cannot acquire B while A's analysis occupies it;
            # allow two slots there to demonstrate the converging boundary.
            if threads == 1:
                (tmp_path / "release").touch()
            else:
                wait(lambda: len(list((tmp_path / "history" / "b.1" / "selections").glob("*/execution.json"))) == 2)
        finally:
            (tmp_path / "release").touch()
        first, second = a.wait(), b.wait()
    assert first.succeeded and second.succeeded
    assert first["pull"].record != second["pull"].record
    assert first["analysis"].record == second["analysis"].record
    assert (tmp_path / "calls").read_text().splitlines() == [str(tmp_path / "one")]
    history = RunHistory(site.runs_dir)
    first_inputs = history.invocation(first.run_id, "analysis")
    second_inputs = history.invocation(second.run_id, "analysis")
    assert second_inputs.requested_inputs["repo"]["producer"]["record"] == second["pull"].record
    assert second_inputs.executed_inputs["repo"]["producer"]["record"] == first["pull"].record
    if threads == 2:
        assert first_inputs.execution_id == second_inputs.execution_id
    bindings = [json.loads(p.read_text()) for p in (tmp_path / "history").rglob("execution.json")]
    assert {row["inputs"]["repo"]["value"] for row in bindings if "repo" in row.get("inputs", {})} == {str(tmp_path / "one"), str(tmp_path / "two")}


@operation(execution="each_submission", config={"path": parameter(str)},
           outputs={"document": file("document", kind="document", identity="content")})
def acquire_file(path, out):
    shutil.copyfile(path, out.document)
    os.utime(out.document, ns=(123456789, 123456789))


@operation(inputs={"document": artifact("document")}, outputs={"tail": returned()})
def read_tail(document):
    with open(document, "rb") as stream:
        stream.seek(-1, 2)
        return {"tail": stream.read(1).decode()}


@study
def file_study(path):
    return read_tail.named("read")(acquire_file.named("pull")(path=path))


@pytest.mark.parametrize("size", [1024, 65 * 1024 * 1024])
def test_full_file_identity(tmp_path, size):
    source = tmp_path / "source"
    with source.open("wb") as stream:
        stream.truncate(size)
        stream.seek(size - 1)
        stream.write(b"A")
    site = Site(records_dir=str(tmp_path / "records"), runs_dir=str(tmp_path / "history"))
    with runtime(site) as live:
        a = live.submit(file_study(str(source)), name="a").wait()
        os.utime(source, None)
        same = live.submit(file_study(str(source)), name="same").wait()
        with source.open("r+b") as stream:
            stream.seek(size - 1)
            stream.write(b"B")
        changed = live.submit(file_study(str(source)), name="changed").wait()
    assert all(run.succeeded for run in (a, same, changed))
    assert same["read"].reused and not changed["read"].reused
    assert a["pull"].artifacts["document"]["modified_ns"] == changed["pull"].artifacts["document"]["modified_ns"]
    assert a["pull"].artifacts["document"]["identity"] != changed["pull"].artifacts["document"]["identity"]
    assert changed["read"].value == {"tail": "B"}


@operation(execution="each_submission", config={"path": parameter(str), "mode": parameter(str)},
           outputs={"repo": directory(kind="repository", external=True, identity="declared")})
def invalid_acquisition(path, mode):
    if mode == "raise":
        raise ValueError("acquisition failed")
    if mode == "missing_return":
        return {}
    return {"repo": located(path, identity=float("nan") if mode == "invalid_identity" else "A")}


@study
def failed_acquisition(path, mode):
    return read_revision.named("analysis")(invalid_acquisition.named("pull")(path=path, mode=mode))


@pytest.mark.parametrize("mode", ["raise", "missing_return", "invalid_identity", "missing_path"])
def test_invalid_acquisition_blocks_consumers(tmp_path, mode):
    directory = tmp_path / "repo"
    if mode != "missing_path":
        directory.mkdir()
    site = Site(records_dir=str(tmp_path / "records"), runs_dir=str(tmp_path / "history"))
    run = run_study(failed_acquisition(str(directory), mode), site=site, name="failed")
    assert run["pull"].outcome == "failed"
    assert run["analysis"].outcome == "blocked"
    assert run["analysis"].input_digest is None
    assert run["analysis"].record is None


def test_producer_outputs_do_not_hash_payloads(tmp_path, monkeypatch):
    from hedloom_exec import artifacts as capture
    (tmp_path / "payload").write_text("data")
    monkeypatch.setattr(capture, "fingerprint_content", lambda _: pytest.fail("unrequested hashing"))
    ref, = capture.capture_outputs({"data": {"path": "payload"}}, workdir=tmp_path, producer_digest="producer")
    assert ref.identity == "output:producer:data"


def test_borrowed_payload_survives_pin_and_prune(tmp_path):
    from hedloom_exec.artifacts import capture_outputs
    from hedloom_exec.journal import AttemptJournal
    from hedloom_exec.identity import try_name
    from hedloom_exec.pins import pin, unpin
    from hedloom_exec.prune import RetentionPolicy, RetentionRule, survey
    borrowed = tmp_path / "borrowed"
    borrowed.mkdir()
    payload = borrowed / "payload"
    payload.write_text("externally owned")
    before = payload.stat()
    ref, = capture_outputs({"repo": {"external": True, "identity": "declared", "filesystem_kind": "directory"}},
                           workdir=None, value={"repo": {"location": str(borrowed), "identity": "A"}})
    # A failed try can retain output references as diagnostic evidence.
    journal = AttemptJournal(tmp_path / "records", "hedloom-" + "a" * 20)
    with journal.claim():
        number = journal.begin_try()
        journal.publish_terminal(try_number=number, outcome="failed", manifest={"artifacts": [ref.as_data()]})
    work = tmp_path / "work" / try_name(journal.identity, number)
    work.mkdir(parents=True)
    (work / "diagnostic").write_text("owned")
    made = pin(journal, try_number=number, work_dir=tmp_path / "work", reason="inspect", freeze=True)
    assert payload.stat().st_mode == before.st_mode
    unpin(journal, pin_id=made.pin_id, reason="done")
    policy = RetentionPolicy((RetentionRule("spent", outcome=("failed",), keep_latest=0, keep_logs=False),), floor="0s")
    result = survey(tmp_path / "records", policy, work_dir=tmp_path / "work").apply()
    assert len(result.removed) == 1 and not work.exists()
    assert payload.read_text() == "externally owned"
    assert payload.stat().st_mode == before.st_mode
    assert journal.read_manifest(number)["result"]["artifacts"][0]["identity"] == ref.identity


@operation(execution="each_submission", config={"path": parameter(str)},
           outputs={"left": file("left", kind="document", identity="content"),
                    "right": file("right", kind="document", identity="content")})
def pair_files(path, out):
    out.left.write_text("A")
    out.right.write_text(Path(path).read_text())


@study
def pair_study(path):
    pair = pair_files.named("pair")(path=path)
    pair_files.named("other-observation")(path=path)
    return {"left": read_tail.named("left")(pair.left), "right": read_tail.named("right")(pair.right)}


def test_named_outputs_invalidate_only_the_changed_port(tmp_path):
    source = tmp_path / "source"
    source.write_text("B")
    site = Site(records_dir=str(tmp_path / "records"), runs_dir=str(tmp_path / "history"))
    with runtime(site) as live:
        first = live.submit(pair_study(str(source)), name="first").wait()
        source.write_text("C")
        second = live.submit(pair_study(str(source)), name="second").wait()
    assert first.succeeded and second.succeeded
    assert len({run[name].record for run in (first, second) for name in ("pair", "other-observation")}) == 4
    assert second["left"].reused and not second["right"].reused
    assert second.outputs["left"].value == "A" and second.outputs["right"].value == "C"


@operation(config={"path": parameter(str)}, outputs={"repo": directory(external=True, identity="declared")})
def stable_borrow(path):
    return {"repo": located(path, identity="A")}


@study
def stable_borrow_study(path):
    return stable_borrow(path=path)


def test_historical_identity_is_separate_from_current_access(tmp_path):
    location = tmp_path / "external"
    location.mkdir()
    site = Site(records_dir=str(tmp_path / "records"), runs_dir=str(tmp_path / "history"))
    subject = stable_borrow_study(str(location))
    first = run_study(subject, site=site, name="first")
    output = first.outputs["output"]
    assert output.available and output.accessible
    location.rename(tmp_path / "moved")
    assert output.available and not output.accessible
    saved = RunHistory(site.runs_dir).outputs(first.run_id)["output"]
    assert saved["available"] and not saved["accessible"]
    assert saved["artifact"]["identity"]["value"] == "A"
    next_run = run_study(subject, site=site, name="missing")
    assert not next_run.succeeded
    assert "no longer accessible" in next_run.report.outcomes[0].error


def test_a_completed_generation_is_replaced_without_redirecting_history(tmp_path):
    (tmp_path / 'release').touch()
    (tmp_path / 'one').mkdir()
    (tmp_path / 'one' / 'payload').write_text('A')
    site = Site(records_dir=str(tmp_path / 'records'), runs_dir=str(tmp_path / 'history'))
    with runtime(site) as live:
        first = live.submit(shared_revision(str(tmp_path / 'one'), str(tmp_path)), name='first').wait()
        second = live.submit(shared_revision(str(tmp_path / 'one'), str(tmp_path)), name='second').wait()
    history = RunHistory(site.runs_dir)
    a, b = (history.invocation(run.run_id, 'analysis') for run in (first, second))
    assert a.execution_id != b.execution_id
    assert a.record == b.record and second['analysis'].reused
    assert history.invocation(first.run_id, 'analysis').execution_id == a.execution_id


@study
def two_consumers(path, markers):
    borrowed = borrow_copy.named("pull")(path=path)
    return {"one": held_analysis.named("one")(borrowed, markers=markers),
            "two": held_analysis.named("two")(borrowed, markers=markers)}


def test_one_completion_projects_multiple_invocations(tmp_path):
    (tmp_path / "payload").write_text("A")
    site = Site(records_dir=str(tmp_path / "records"), runs_dir=str(tmp_path / "history"), threads=2)
    with runtime(site) as live:
        future = live.submit(two_consumers(str(tmp_path), str(tmp_path)), name="two")
        try:
            deadline = time.monotonic() + 10
            while len(list((tmp_path / "history" / "two.1" / "selections").glob("*/execution.json"))) != 3:
                assert time.monotonic() < deadline
                time.sleep(.01)
        finally:
            (tmp_path / "release").touch()
        run = future.wait()
    assert run.succeeded
    assert run["one"].record == run["two"].record
    assert run["one"].invocation_id != run["two"].invocation_id
    assert run.outputs["one"].value == run.outputs["two"].value == "A"
    history = RunHistory(site.runs_dir)
    assert history.invocation(run.run_id, "one").execution_id == history.invocation(run.run_id, "two").execution_id
    assert (tmp_path / "calls").read_text().splitlines() == [str(tmp_path)]


@operation(execution="each_submission", config={"path": parameter(str)},
           outputs={"document": file(kind="document", external=True, identity="content")})
def acquire_external_file(path):
    return {"document": located(path)}


@study
def external_file_study(path):
    return read_tail.named("read")(acquire_external_file.named("pull")(path=path))


@pytest.mark.parametrize("size", [1024, 65 * 1024 * 1024])
def test_external_content_identity(tmp_path, size):
    source = tmp_path / "source"
    with source.open("wb") as stream:
        stream.truncate(size)
        stream.seek(size - 1)
        stream.write(b"A")
    stamp = source.stat()
    site = Site(records_dir=str(tmp_path / "records"), runs_dir=str(tmp_path / "history"))
    with runtime(site) as live:
        a = live.submit(external_file_study(str(source)), name="a").wait()
        same = live.submit(external_file_study(str(source)), name="same").wait()
        with source.open("r+b") as stream:
            stream.seek(size - 1)
            stream.write(b"B")
        os.utime(source, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
        changed = live.submit(external_file_study(str(source)), name="changed").wait()
    assert all(run.succeeded for run in (a, same, changed))
    assert len({run["pull"].record for run in (a, same, changed)}) == 3
    assert same["read"].reused and not changed["read"].reused
    assert changed["read"].value == {"tail": "B"}
    for run in (a, same, changed):
        ref = run["pull"].artifacts["document"]
        assert ref["address"] == str(source)
        assert ref["ownership"] == "borrowed"
    assert a["pull"].artifacts["document"]["identity"] != changed["pull"].artifacts["document"]["identity"]


@pytest.mark.parametrize("shape", ["missing", "directory"])
def test_external_content_requires_existing_file(tmp_path, shape):
    source = tmp_path / "source"
    if shape == "directory":
        source.mkdir()
    site = Site(records_dir=str(tmp_path / "records"), runs_dir=str(tmp_path / "history"))
    run = run_study(external_file_study(str(source)), site=site, name="invalid")
    assert not run.succeeded


def test_declared_external_identity_is_still_required(tmp_path):
    from hedloom.binding import _named_data
    from hedloom_exec.artifacts import capture_outputs, MissingOutput
    source = tmp_path / "source"
    source.write_text("payload")
    with pytest.raises(MissingOutput, match="explicit identity"):
        capture_outputs(
            {"document": {"external": True, "identity": "declared", "filesystem_kind": "file"}},
            workdir=None, value=_named_data({"document": located(source)}),
        )
