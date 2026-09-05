from __future__ import annotations

import os
import stat

import pytest

from hedloom_exec.identity import attempt_identity, try_name
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.pins import PinError, pin as make_pin, unpin, verify


def _pinned(tmp_path, *, empty=False):
    identity = attempt_identity(computation_digest="plan/point").rendered
    journal = AttemptJournal(tmp_path / "records", identity)
    with journal.claim():
        number = journal.begin_try()
        journal.append(
            "created", **{"try": number, "plan": "plan", "invocation": "point",
                          "operation": "work", "input_digest": "digest"},
        )
        journal.publish_terminal(try_number=number, outcome="failed", manifest={})
    workspace = tmp_path / "work" / try_name(identity, number)
    workspace.mkdir(parents=True)
    if not empty:
        (workspace / "result.bin").write_bytes(b"content")
    made = make_pin(journal, try_number=number, workspace_root=tmp_path / "work",
                    reason="reference", actor="engineer")
    return journal, workspace, made


def test_pinning_freezes_the_workspace_read_only(tmp_path):
    _journal, workspace, made = _pinned(tmp_path)
    assert made.froze
    assert stat.S_IMODE(workspace.stat().st_mode) & 0o222 == 0
    assert stat.S_IMODE((workspace / "result.bin").stat().st_mode) & 0o222 == 0


def test_pinning_records_a_digest_for_every_file_it_froze(tmp_path):
    _journal, _workspace, made = _pinned(tmp_path)
    assert [(item.relpath, len(item.digest)) for item in made.contents] == [
        ("result.bin", 64)
    ]


def test_the_inventory_is_taken_before_the_chmod(tmp_path, monkeypatch):
    import hedloom_exec.pins as pins_module
    seen = {}
    original = pins_module._freeze

    def freeze(workspace, modes):
        seen["writable"] = bool(workspace.stat().st_mode & 0o200)
        return original(workspace, modes)

    monkeypatch.setattr(pins_module, "_freeze", freeze)
    _journal, _workspace, made = _pinned(tmp_path)
    assert seen["writable"]
    assert made.contents


def test_verify_reports_no_drift_for_an_untouched_pin(tmp_path):
    _journal, _workspace, made = _pinned(tmp_path)
    assert verify(made, layout=1).outcome == "intact"


def test_verify_names_exactly_the_files_that_drifted(tmp_path):
    _journal, workspace, made = _pinned(tmp_path)
    path = workspace / "result.bin"
    path.chmod(0o644)
    path.write_bytes(b"changed")
    assert verify(made, layout=1).drifted == ("result.bin",)


def test_verify_detects_a_file_added_to_a_pinned_workspace(tmp_path):
    _journal, workspace, made = _pinned(tmp_path)
    workspace.chmod(0o755)
    (workspace / "added").write_text("new")
    assert verify(made, layout=1).drifted == ("added",)


def test_verify_detects_a_file_removed_from_a_pinned_workspace(tmp_path):
    _journal, workspace, made = _pinned(tmp_path)
    workspace.chmod(0o755)
    (workspace / "result.bin").unlink()
    assert verify(made, layout=1).drifted == ("result.bin",)


def test_verify_detects_content_changed_at_the_same_size_and_mtime(tmp_path):
    _journal, workspace, made = _pinned(tmp_path)
    path = workspace / "result.bin"
    before = path.stat()
    path.chmod(0o644)
    path.write_bytes(b"CONTENT")
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert path.stat().st_size == before.st_size
    assert path.stat().st_mtime_ns == before.st_mtime_ns
    assert verify(made, layout=1).drifted == ("result.bin",)


def test_unpin_thaws_the_workspace_and_says_so_in_the_record(tmp_path):
    journal, workspace, made = _pinned(tmp_path)
    released = unpin(journal, pin_id=made.pin_id, reason="done", actor="engineer")
    assert not released.is_active
    assert workspace.stat().st_mode & 0o200
    assert (workspace / "result.bin").stat().st_mode & 0o200
    event = next(item for item in journal.events() if item.event == "unpinned")
    assert event.data["thaw"] is True


def test_an_empty_workspace_can_still_be_pinned(tmp_path):
    _journal, _workspace, made = _pinned(tmp_path, empty=True)
    assert made.contents == ()
    assert verify(made, layout=1).outcome == "intact"


def test_a_crash_between_chmod_and_the_event_leaves_an_explainable_state(
    tmp_path, monkeypatch
):
    identity = attempt_identity(computation_digest="plan/point").rendered
    journal = AttemptJournal(tmp_path / "records", identity)
    with journal.claim():
        number = journal.begin_try()
        journal.publish_terminal(try_number=number, outcome="failed", manifest={})
    workspace = tmp_path / "work" / try_name(identity, number)
    workspace.mkdir(parents=True)
    (workspace / "result.bin").write_bytes(b"content")
    original = journal.append

    def crash(event, **data):
        if event == "pinned":
            raise RuntimeError("injected crash")
        return original(event, **data)

    monkeypatch.setattr(journal, "append", crash)
    with pytest.raises(RuntimeError, match="injected"):
        make_pin(journal, try_number=number, workspace_root=tmp_path / "work",
                 reason="reference")
    assert workspace.stat().st_mode & 0o222 == 0
    assert journal.fold().pins == ()


def test_a_thaw_failure_is_reported_after_the_release(tmp_path, monkeypatch):
    journal, _workspace, made = _pinned(tmp_path)
    import hedloom_exec.pins as pins_module

    monkeypatch.setattr(pins_module, "_thaw", lambda workspace, modes: False)
    with pytest.raises(PinError, match="was released"):
        unpin(journal, pin_id=made.pin_id, reason="done")
    assert not journal.fold().pins[0].is_active
