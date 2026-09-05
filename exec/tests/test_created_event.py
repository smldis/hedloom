"""What a record says about itself when it is created.

The created event is the record's own account of the computation it holds. It
names the try that made it, the operation, and the digest of the declaration.
It does not name a requester, because a record has none: the same declaration
from a second study is the same record, so any name stored here could only be
whichever caller happened to arrive first.
"""

from hedloom_exec.durability import Durability, execute
from hedloom_exec.identity import attempt_identity
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.reuse import input_digest, scan_attempts
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


def test_created_states_the_try_the_operation_and_the_declaration(tmp_path):
    data = created(run(tmp_path, BUNDLE)).data

    assert data["try"] == 0
    assert data["operation"] == "op"
    assert data["input_digest"] == input_digest(BUNDLE)


def test_created_names_no_requester_and_no_superseded_record(tmp_path):
    """Nothing here may answer "whose record is this?" or "what did it replace?"."""

    first = run(tmp_path, {"operation": "op", "inputs": {"a": "one"}})
    second = run(tmp_path, {"operation": "op", "inputs": {"a": "two"}})

    assert first.journal.identity != second.journal.identity
    for result in (first, second):
        assert set(created(result).data) == {"try", "operation", "input_digest"}


def test_the_record_is_derivable_from_the_declaration_alone(tmp_path):
    """A caller holding only the bundle can name the record a run will use."""

    before = attempt_identity(computation_digest=input_digest(BUNDLE))
    result = run(tmp_path, BUNDLE)

    assert input_digest(BUNDLE) == "11c1a0b0731fb2108d7c4dc97abc1baa"
    assert before.rendered == "hedloom-8f9ace4493c90af8ebee"
    assert result.journal.identity == before.rendered
    assert result.record == before.rendered
    assert result.try_number == 0


def test_no_execution_detail_participates_in_the_input_digest():
    decorated = {**BUNDLE, "try": 4, "workdir": "/tmp/somewhere", "placement": "lsf"}

    assert input_digest(decorated) == input_digest(BUNDLE)


def test_a_layout_one_record_written_before_this_contract_still_scans(tmp_path):
    """Old renderings are not selected by the new hash; they remain readable.

    A record written by an earlier identity contract carries fields this one
    no longer writes. Layout 1 has not changed, so reading it is unaffected:
    the scan ignores what it does not model rather than refusing the record.
    """

    identity = attempt_identity(computation_digest="old/record").rendered
    journal = AttemptJournal(tmp_path, identity)
    with journal.claim():
        number = journal.begin_try()
        journal.append(
            "created",
            **{
                "try": number,
                "plan": "plan",
                "invocation": "invoke:key:abc",

                "supersedes": "hedloom-prior",
                "operation": "op",
                "input_digest": "old",
            },
        )

    records = scan_attempts(tmp_path)
    assert len(records) == 1
    assert records[0].identity == identity
    assert records[0].try_number == 0
    assert records[0].input_digest == "old"
