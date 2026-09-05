"""One declared computation selects one record, whoever asks for it.

The contract these hold, stated before the code:

    A record is selected by the declared computation digest alone. Study
    name, authored key, Plan ID, placement and try number describe who asked
    or where it ran, and none of them may reach the selection or be stored on
    the record as its owner.

What that buys is reuse across studies: a second study declaring the same work
finds the first study's evidence instead of recomputing it. What it costs is
that renaming something is no longer a way to ask for the work again — an
intentional repetition has to declare a computational distinction, and one of
these tests is exactly that.

Execution is counted from the store rather than from a Python counter, because
a body may run in a worker: one record with one published try is one execution,
and that is the evidence these tests assert on. Which execution an invocation
got is read from the outcome, which states its record and try rather than
leaving a caller to infer them.

Two *simultaneous* requesters of one shared record are deliberately out of
scope for this pass. That is the claim refusal, asserted here as a refusal
rather than papered over.
"""

import json
from pathlib import Path

import pytest

from hedloom import (
    Site,
    artifact,
    file,
    flow,
    local,
    operation,
    parameter,
    returned,
    study,
    sweep,
)
from hedloom.cli import main as hedloom_cli
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.reuse import scan_attempts

TEXT = artifact("text-file")


@operation(
    config={"word": parameter(str)},
    outputs={"note": file("note.txt", kind="text-file")},
)
def write_note(out, *, word: str) -> None:
    out.note.write_text(word * 3)


@operation(
    config={"word": parameter(str), "seed": parameter(int)},
    outputs={"note": file("note.txt", kind="text-file")},
)
def write_note_repeated(out, *, word: str, seed: int) -> None:
    out.note.write_text(word * 3)


@operation(inputs={"note": TEXT}, outputs={"size": returned(kind="count")})
def measure(note) -> int:
    return len(Path(note).read_text())


@pytest.fixture
def site(tmp_path):
    return Site(
        root=str(tmp_path / "attempts"), workspace_root=str(tmp_path / "work")
    )


@pytest.fixture
def records(tmp_path):
    return tmp_path / "attempts"


def executions(records):
    """How many times work actually ran: one published try is one execution."""

    return sum(
        len(list((record.directory / "manifest").glob("*.json")))
        for record in scan_attempts(records)
    )


def selected(run):
    """Every (record, try) an invocation actually landed on."""

    return {(item.record, item.try_number) for item in run.report.outcomes}


def one_note(key="point"):
    @flow
    def notes():
        for _ in sweep(["only"], key=lambda item: key):
            written = write_note(word="ab")
        return {"note": written}

    return notes


def test_two_studies_declaring_the_same_work_share_one_record(site, records):
    """A study rename is not a request for the work to happen again."""

    notes = one_note()

    @study(name="shared-first-study", default_policy=local())
    def first():
        return notes.named("notes")()

    @study(name="shared-second-study", default_policy=local())
    def second():
        return notes.named("notes")()

    ran = first().submit(site=site)
    reran = second().submit(site=site)

    assert ran.succeeded and reran.succeeded, reran.summary()
    assert len(scan_attempts(records)) == 1
    assert executions(records) == 1, "equal declarations execute once"
    assert selected(ran) == selected(reran) != {(None, None)}
    assert all(item.reused for item in reran.report.outcomes)


def test_a_renamed_authored_key_selects_the_same_record(site, records):
    """The authored key names the request, not the computation."""

    @study(name="shared-keyed-first", default_policy=local())
    def before():
        return one_note("original").named("notes")()

    @study(name="shared-keyed-second", default_policy=local())
    def after():
        return one_note("renamed").named("notes")()

    first = before().submit(site=site)
    second = after().submit(site=site)

    assert first.succeeded and second.succeeded, second.summary()
    assert len(scan_attempts(records)) == 1
    assert executions(records) == 1
    assert selected(first) == selected(second)
    assert all(item.reused for item in second.report.outcomes)


def test_two_equal_invocations_in_one_plan_share_one_record(site, records):
    """One record, and still one outcome per authored invocation."""

    @flow
    def twice():
        for _ in sweep(["left", "right"], key=lambda item: item):
            written = write_note(word="ab")
        return {"note": written}

    @study(name="shared-twice", default_policy=local())
    def build():
        return twice.named("twice")()

    # Sequential on purpose: two equal declarations are one record, and this
    # pass answers two simultaneous requesters with the claim refusal rather
    # than by coalescing them. The refusal is asserted separately, below.
    run = build().submit(site=site, sequential=True)

    assert run.succeeded, run.summary()
    assert len(scan_attempts(records)) == 1
    assert executions(records) == 1

    keys = sorted(item.authored_key for item in run.report.outcomes)
    assert keys == ["left:write_note", "right:write_note"]
    assert len({item.input_digest for item in run.report.outcomes}) == 1
    # Both authored invocations keep their own outcome and name, and both say
    # plainly that they landed on the same execution.
    assert len(selected(run)) == 1
    assert all(item.record is not None for item in run.report.outcomes)


def test_a_renamed_producer_leaves_downstream_identity_unchanged(site, records):
    """Upstream references normalize to digests, never to producer names."""

    def chain(producer_key):
        @flow
        def linked():
            for _ in sweep(["only"], key=lambda item: producer_key):
                written = write_note(word="ab")
            for _ in sweep(["only"], key=lambda item: "consumer"):
                size = measure(written)
            return {"size": size}

        return linked

    @study(name="shared-chain-first", default_policy=local())
    def before():
        return chain("producer").named("linked")()

    @study(name="shared-chain-second", default_policy=local())
    def after():
        return chain("renamed-producer").named("linked")()

    first = before().submit(site=site)
    second = after().submit(site=site)

    assert first.succeeded and second.succeeded, second.summary()

    def by_operation(run):
        return {item.operation: (item.input_digest, item.record)
                for item in run.report.outcomes}

    assert by_operation(first) == by_operation(second), (
        "renaming a producer must not invalidate it or anything downstream"
    )
    assert executions(records) == 2, "one producer and one consumer, once each"
    assert all(item.reused for item in second.report.outcomes)


def test_an_intentional_repetition_must_declare_a_distinction(site, records):
    """Two seeds are two computations; two names are not."""

    @flow
    def seeded(seed):
        for _ in sweep(["only"], key=lambda item: f"seed-{seed}"):
            written = write_note_repeated(word="ab", seed=seed)
        return {"note": written}

    @study(name="shared-repetition", default_policy=local())
    def build(seed=1):
        return seeded.named("seeded")(seed)

    first = build(1).submit(site=site)
    second = build(2).submit(site=site)
    again = build(1).submit(site=site)

    assert first.succeeded and second.succeeded and again.succeeded
    assert len(scan_attempts(records)) == 2, "a declared seed is a distinction"
    assert executions(records) == 2, "a renamed key would not have been one"
    assert selected(first) != selected(second)
    assert selected(again) == selected(first)
    assert all(item.reused for item in again.report.outcomes)


def test_a_second_caller_keeps_its_own_name_and_reads_the_same_execution(
    site, records
):
    """Sharing a record does not merge the callers that reached it."""

    notes = one_note("point")

    @study(name="shared-alpha", default_policy=local())
    def alpha():
        return notes.named("notes")()

    @study(name="shared-beta", default_policy=local())
    def beta():
        return notes.named("notes")()

    first = alpha().submit(site=site)
    second = beta().submit(site=site)

    assert first["point:write_note"].outcome == "succeeded"
    assert second["point:write_note"].outcome == "succeeded"
    assert first["point:write_note"].record == second["point:write_note"].record
    assert (
        first["point:write_note"].try_number
        == second["point:write_note"].try_number
        == 0
    )
    # The same file, reached by both, through the try workspace rather than a
    # per-requester view. There is no per-study name anywhere in the store.
    address = second["point:write_note"].artifacts["note"]["address"]
    assert Path(address).read_text() == "ababab"
    assert len(scan_attempts(records)) == 1
    assert not (records / "latest").exists(), "no derived per-requester view"


def test_a_record_carries_no_requester_names(site, records):
    """The store must not be able to answer 'whose record is this?'."""

    @study(name="shared-anonymous", default_policy=local())
    def build():
        return one_note("point").named("notes")()

    assert build().submit(site=site).succeeded
    (record,) = scan_attempts(records)

    assert not hasattr(record, "plan_id")
    assert not hasattr(record, "authored_key")
    assert not hasattr(record, "invocation_id")
    assert not hasattr(record, "supersedes")

    created = next(
        event for event in AttemptJournal(records, record.identity).events()
        if event.event == "created"
    )
    assert set(created.data) == {"try", "operation", "input_digest"}


def test_a_competing_claim_reaches_the_same_record_and_is_refused(site, records):
    """Exclusion, not completed result sharing. The limitation is asserted.

    A second study declaring the same work reaches the *same* record — which
    is what makes this evidence about sharing — and, while that record's claim
    is held, is refused rather than launching a second execution. Waiting for
    the holder, or coalescing the two requests, is deferred to the scheduler
    work and is deliberately not implemented here. The consequence, stated
    plainly: a simultaneous equivalent requester does not get a result.
    """

    notes = one_note()

    @study(name="shared-creator", default_policy=local())
    def creator():
        return notes.named("notes")()

    @study(name="shared-contender", default_policy=local())
    def contender():
        return notes.named("notes")()

    made = creator().submit(site=site)
    assert made.succeeded, made.summary()
    (record,) = scan_attempts(records)

    journal = AttemptJournal(records, record.identity)
    with journal.claim():
        run = contender().submit(site=site)

    refused = [
        outcome
        for outcome in run.report.outcomes
        if outcome.operation.endswith("write_note")
    ]
    assert refused, run.summary()
    assert refused[0].disposition == "refused"
    assert "ConcurrentClaim" in (refused[0].error or "")
    assert refused[0].input_digest == record.input_digest, (
        "the refusal must be about the shared record, not a private one"
    )
    # A request that selected no try must not name one.
    assert refused[0].record is None and refused[0].try_number is None
    assert len(scan_attempts(records)) == 1
    assert executions(records) == 1, "a held claim must not launch a second run"


def test_the_operator_cli_addresses_records_and_tries(site, records, capsys):
    """Pin, list and prune reach the exact execution a run reported."""

    @study(name="shared-cli", default_policy=local())
    def build():
        return one_note("point").named("notes")()

    run = build().submit(site=site)
    assert run.succeeded, run.summary()
    outcome = run["point:write_note"]
    reference = f"{outcome.record}#{outcome.try_number}"

    roots = ["--root", str(records), "--workspace-root", str(site.workspace_root)]
    assert hedloom_cli(["pin", *roots, reference, "--reason", "keep"]) == 0
    pin_id = capsys.readouterr().out.split()[0]

    assert hedloom_cli(["pins", *roots]) == 0
    assert reference in capsys.readouterr().out

    assert hedloom_cli(["unpin", *roots, pin_id, "--reason", "done"]) == 0
    assert "released" in capsys.readouterr().out

    # Reclamation addresses the record too, with no owner-shaped filter.
    assert hedloom_cli(
        ["prune", *roots, "--record", outcome.record, "--failed", "--json"]
    ) == 0
    surveyed = json.loads(capsys.readouterr().out)
    assert surveyed["candidates"] == [], "the only try is standing evidence"
    assert {item["identity"] for item in surveyed["skipped"]} == {outcome.record}

    # An unknown record prefix is refused, not silently surveyed as everything.
    assert hedloom_cli(["prune", *roots, "--record", "hedloom-nope"]) == 2


@pytest.mark.parametrize("removed", ["where", "check", "log"])
def test_the_ownership_commands_are_gone(removed, capsys):
    """Removed, not softened: the parser must not know these names."""

    with pytest.raises(SystemExit):
        hedloom_cli([removed, "--root", "/nonexistent", "anything"])


@pytest.mark.parametrize("removed", ["--study", "--invocation"])
def test_prune_has_no_creator_filters(removed):
    with pytest.raises(SystemExit):
        hedloom_cli(["prune", "--root", "/nonexistent", removed, "x"])
