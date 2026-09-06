"""Fresh observations retain their evidence while equal artifacts reuse work."""
import pytest
import json
import subprocess
from pathlib import Path

from hedloom import Site, artifact, directory, located, operation, parameter, returned, study
from concurrent.futures import ThreadPoolExecutor
from hedloom import session, RunHistory
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


@pytest.mark.parametrize("sequential", [True, False])
def test_latest_a_a_b_a_sequential(tmp_path, sequential):
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
    site = Site(root=str(tmp_path / "records"), workspace_root=str(tmp_path / "work"), history_root=str(tmp_path / "history"))
    runs = []
    for commit in (a, a, b, a):
        git(repository, "checkout", "-q", commit)
        run = revision_study(str(repository)).submit(site=site, name="observe", sequential=sequential)
        assert run.succeeded, run.summary()
        runs.append(run)
    assert [run.outputs["report"].value for run in runs] == ["A", "A", "B", "A"]
    assert len({run["pull"].record for run in runs}) == 4
    assert [run["analysis"].reused for run in runs] == [False, True, False, True]
    assert all(run["pull"].try_number == 0 for run in runs)
    events_path = Path(site.root) / runs[0]["analysis"].record / "events.jsonl"
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


@pytest.mark.parametrize("sequential, threads", [(True, 1), (False, 1), (False, 2)])
def test_independent_observations_join_analysis(tmp_path, sequential, threads):
    for name in ("one", "two"):
        (tmp_path / name).mkdir()
        (tmp_path / name / "payload").write_text("A")
    site = Site(root=str(tmp_path / "records"), history_root=str(tmp_path / "history"), threads=threads)
    def wait(predicate):
        deadline = time.monotonic() + 10
        while not predicate():
            assert time.monotonic() < deadline, "barrier timeout"
            time.sleep(.01)
    with session(site, sequential=sequential) as live, ThreadPoolExecutor(2) as pool:
        a = pool.submit(live.submit, shared_revision(str(tmp_path / "one"), str(tmp_path)), name="a")
        try:
            wait(lambda: (tmp_path / "calls").exists())
            b = pool.submit(live.submit, shared_revision(str(tmp_path / "two"), str(tmp_path)), name="b")
            # One graph slot cannot acquire B while A's analysis occupies it;
            # allow two slots there to demonstrate the converging boundary.
            if not sequential and threads == 1:
                (tmp_path / "release").touch()
            else:
                wait(lambda: len(list((tmp_path / "history" / "runs" / "b.1" / "selections").glob("*/execution.json"))) == 2)
        finally:
            (tmp_path / "release").touch()
        first, second = a.result(), b.result()
    assert first.succeeded and second.succeeded
    assert first["pull"].record != second["pull"].record
    assert first["analysis"].record == second["analysis"].record
    assert (tmp_path / "calls").read_text().splitlines() == [str(tmp_path / "one")]
    history = RunHistory(site.history_root)
    first_inputs = history.invocation(first.run_id, "analysis")
    second_inputs = history.invocation(second.run_id, "analysis")
    assert second_inputs.requested_inputs["repo"]["producer"]["record"] == second["pull"].record
    assert second_inputs.executed_inputs["repo"]["producer"]["record"] == first["pull"].record
    if sequential or threads == 2:
        assert first_inputs.execution_id == second_inputs.execution_id
    bindings = [json.loads(p.read_text()) for p in (tmp_path / "history" / "runs").rglob("execution.json")]
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


@pytest.mark.parametrize("sequential", [True, False])
@pytest.mark.parametrize("size", [1024, 65 * 1024 * 1024])
def test_full_file_identity(tmp_path, sequential, size):
    source = tmp_path / "source"
    with source.open("wb") as stream:
        stream.truncate(size)
        stream.seek(size - 1)
        stream.write(b"A")
    site = Site(root=str(tmp_path / "records"), history_root=str(tmp_path / "history"))
    with session(site, sequential=sequential) as live:
        a = live.submit(file_study(str(source)), name="a")
        os.utime(source, None)
        same = live.submit(file_study(str(source)), name="same")
        with source.open("r+b") as stream:
            stream.seek(size - 1)
            stream.write(b"B")
        changed = live.submit(file_study(str(source)), name="changed")
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


@pytest.mark.parametrize("sequential", [True, False])
@pytest.mark.parametrize("mode", ["raise", "missing_return", "invalid_identity", "missing_path"])
def test_invalid_acquisition_blocks_consumers(tmp_path, sequential, mode):
    directory = tmp_path / "repo"
    if mode != "missing_path":
        directory.mkdir()
    site = Site(root=str(tmp_path / "records"), history_root=str(tmp_path / "history"))
    run = failed_acquisition(str(directory), mode).submit(site=site, name="failed", sequential=sequential)
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
    made = pin(journal, try_number=number, workspace_root=tmp_path / "work", reason="inspect", freeze=True)
    assert payload.stat().st_mode == before.st_mode
    unpin(journal, pin_id=made.pin_id, reason="done")
    policy = RetentionPolicy((RetentionRule("spent", outcome=("failed",), keep_latest=0, keep_logs=False),), floor="0s")
    result = survey(tmp_path / "records", policy, workspace_root=tmp_path / "work").apply()
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


@pytest.mark.parametrize("sequential", [True, False])
def test_named_outputs_invalidate_only_the_changed_port(tmp_path, sequential):
    source = tmp_path / "source"
    source.write_text("B")
    site = Site(root=str(tmp_path / "records"), history_root=str(tmp_path / "history"))
    with session(site, sequential=sequential) as live:
        first = live.submit(pair_study(str(source)), name="first")
        source.write_text("C")
        second = live.submit(pair_study(str(source)), name="second")
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
    site = Site(root=str(tmp_path / "records"), history_root=str(tmp_path / "history"))
    subject = stable_borrow_study(str(location))
    first = subject.submit(site=site, name="first", sequential=True)
    output = first.outputs["output"]
    assert output.available and output.accessible
    location.rename(tmp_path / "moved")
    assert output.available and not output.accessible
    saved = RunHistory(site.history_root).outputs(first.run_id)["output"]
    assert saved["available"] and not saved["accessible"]
    assert saved["artifact"]["identity"]["value"] == "A"
    next_run = subject.submit(site=site, name="missing", sequential=True)
    assert not next_run.succeeded
    assert "no longer accessible" in next_run.report.outcomes[0].error


def test_releasing_a_completed_generation_keeps_its_replacement(tmp_path):
    from concurrent.futures import Future
    from hedloom_run.execution import ExecutionOwner
    owner = ExecutionOwner(tmp_path)
    with owner.lock:
        previous = owner.entry("same")
        previous.consumers.add("first")
        previous.future = Future()
        previous.future.set_result(None)
        replacement = owner.entry("same")
        replacement.consumers.add("second")
    owner.release(previous, "first")
    assert owner.groups["same"] is replacement
    assert owner.withdraw(replacement, "second") == "blocked"
    owner.release(replacement, "second")
    assert not owner.groups


@study
def two_consumers(path, markers):
    borrowed = borrow_copy.named("pull")(path=path)
    return {"one": held_analysis.named("one")(borrowed, markers=markers),
            "two": held_analysis.named("two")(borrowed, markers=markers)}


def test_one_completion_projects_multiple_invocations(tmp_path):
    (tmp_path / "payload").write_text("A")
    site = Site(root=str(tmp_path / "records"), history_root=str(tmp_path / "history"), threads=2)
    with session(site) as live, ThreadPoolExecutor(1) as pool:
        future = pool.submit(live.submit, two_consumers(str(tmp_path), str(tmp_path)), name="two")
        try:
            deadline = time.monotonic() + 10
            while len(list((tmp_path / "history" / "runs" / "two.1" / "selections").glob("*/execution.json"))) != 3:
                assert time.monotonic() < deadline
                time.sleep(.01)
        finally:
            (tmp_path / "release").touch()
        run = future.result()
    assert run.succeeded
    assert run["one"].record == run["two"].record
    assert run["one"].invocation_id != run["two"].invocation_id
    assert run.outputs["one"].value == run.outputs["two"].value == "A"
    history = RunHistory(site.history_root)
    assert history.invocation(run.run_id, "one").execution_id == history.invocation(run.run_id, "two").execution_id
    assert (tmp_path / "calls").read_text().splitlines() == [str(tmp_path)]
