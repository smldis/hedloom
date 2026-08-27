"""Record attribution remains outside the Phase 1 content identity."""

from hedloom_exec.durability import Durability, execute
from hedloom_exec.identity import attempt_identity
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.reuse import IDENTITY_KEYS, input_digest, input_digests, scan_attempts
from hedloom_exec.transport import InProcessTransport


BUNDLE = {
    "operation": "op",
    "operation_version": "1",
    "implementation": {"fingerprint": "abc"},
    "command": ["tool", "x"],
    "arguments": {"x": 1},
    "cwd": "run",
    "inputs": {"a": "sha256:a"},
    "outputs": {"raw": {"path": "x.raw"}},
    "identity_env": {"X": "1"},
}


def transport():
    return InProcessTransport({"op": lambda **_kwargs: "ok"})


def run(tmp_path, bundle=None, **kwargs):
    return execute(
        transport(),
        bundle or {"operation": "op"},
        durability=Durability.RECORDED,
        root=str(tmp_path),
        plan_id="plan",
        invocation_id="invoke:key:abc",
        **kwargs,
    )


def created(result):
    return next(
        event for event in result.journal.events() if event.event == "created"
    )


def test_created_is_written_once_and_only_for_a_new_record(tmp_path):
    first = run(tmp_path)
    second = run(tmp_path)

    events = AttemptJournal(tmp_path, first.journal.identity).events()
    assert first.journal.identity == second.journal.identity
    assert [event.event for event in events].count("created") == 1


def test_created_records_the_try_number(tmp_path):
    assert created(run(tmp_path)).data["try"] == 0


def test_created_records_the_try_for_a_caller_supplied_record_identity(tmp_path):
    supplied = attempt_identity(plan_id="supplied", invocation_id="record").rendered
    result = run(tmp_path, identity=supplied)

    assert created(result).data["try"] == 0


def test_created_records_the_authored_key(tmp_path):
    result = run(tmp_path, authored_key="point:op")

    assert created(result).data["authored_key"] == "point:op"


def test_created_omits_the_authored_key_when_the_plan_did_not_name_one(tmp_path):
    assert "authored_key" not in created(run(tmp_path)).data


def test_created_records_the_identity_it_supersedes(tmp_path):
    first = run(tmp_path, {"operation": "op", "inputs": {"a": "one"}})
    second = run(tmp_path, {"operation": "op", "inputs": {"a": "two"}})

    assert created(second).data["supersedes"] == first.journal.identity


def test_created_records_no_supersedes_for_a_first_record(tmp_path):
    assert "supersedes" not in created(run(tmp_path)).data


def test_created_records_a_digest_for_every_identity_key(tmp_path):
    evidence = created(run(tmp_path, BUNDLE)).data["input_digests"]

    assert set(evidence) == set(IDENTITY_KEYS)
    assert all(len(digest) == 32 for digest in evidence.values())


def test_recording_the_key_digests_leaves_the_input_digest_unchanged(tmp_path):
    before = input_digest(BUNDLE)
    result = run(tmp_path, BUNDLE)

    assert before == "11c1a0b0731fb2108d7c4dc97abc1baa"
    assert created(result).data["input_digest"] == before
    assert input_digest({**BUNDLE, "input_digests": input_digests(BUNDLE)}) == before


def test_an_identity_computed_before_phase_zero_is_unchanged_after_it(tmp_path):
    digest = input_digest(BUNDLE)
    before = attempt_identity(
        plan_id="plan", invocation_id="invoke:key:abc", input_digest=digest
    )
    result = run(tmp_path, BUNDLE)

    # Phase 1 intentionally removed the sequence slot from the hash material.
    assert before.rendered == "hedloom-f8985a4150657953e7cf"
    assert result.journal.identity == before.rendered


def test_no_created_field_participates_in_the_input_digest():
    decorated = {
        **BUNDLE,
        "plan": "plan",
        "invocation": "invoke:key:abc",
        "try": 4,
        "authored_key": "point:op",
        "supersedes": "hedloom-prior",
        "input_digests": input_digests(BUNDLE),
    }

    assert input_digest(decorated) == input_digest(BUNDLE)


def test_a_layout_one_record_missing_optional_attribution_still_scans(tmp_path):
    identity = attempt_identity(plan_id="old", invocation_id="record").rendered
    journal = AttemptJournal(tmp_path, identity)
    with journal.claim():
        number = journal.begin_try()
        journal.append(
            "created",
            **{
                "try": number,
                "plan": "plan",
                "invocation": "invoke:key:abc",
                "operation": "op",
                "input_digest": "old",
            },
        )

    records = scan_attempts(tmp_path)
    assert len(records) == 1
    assert records[0].identity == identity
    assert records[0].try_number == 0
    assert records[0].authored_key is None
    assert records[0].supersedes is None
    assert records[0].input_digests == {}
