from hedloom_exec.identity import attempt_identity, try_name
from hedloom_exec.journal import AttemptJournal
from hedloom_exec.lsf import CommandResult
from hedloom_exec.watch import LSFStatusReader, ObservationLog, live_attempts, observe, status_of


class Reader:
    def __init__(self, rows):
        self.rows = rows

    def __call__(self, _argv, **_kwargs):
        return CommandResult(
            returncode=0,
            stdout="".join(f"{name} {state}\n" for name, state in self.rows),
        )


def live_record(tmp_path):
    identity = attempt_identity(computation_digest="watch-keys/one").rendered
    journal = AttemptJournal(tmp_path, identity)
    with journal.claim():
        number = journal.begin_try()
        journal.append(
            "created", **{"try": number, "invocation": "one", "operation": "work"}
        )
        journal.append(
            "submit_intent",
            **{"try": number, "transport": "lsf-interactive", "substrate": "lsf-interactive"},
        )
        journal.append(
            "submit_receipt",
            **{"try": number, "handle": {"identity": try_name(identity, number)}},
        )
    return journal


def test_the_watcher_matches_the_job_name_not_the_record_identity(tmp_path):
    journal = live_record(tmp_path)
    job = try_name(journal.identity, 0)
    rows = observe(tmp_path, LSFStatusReader(Reader([(job, "RUN")])))
    assert rows[0].identity == journal.identity
    assert rows[0].job_name == job
    assert rows[0].observed == "running"


def test_a_sweep_with_live_jobs_is_distinguishable_from_a_finished_one(tmp_path):
    journal = live_record(tmp_path)
    assert [item.job_name for item in live_attempts(tmp_path)] == [
        try_name(journal.identity, 0)
    ]
    with journal.claim():
        journal.publish_terminal(try_number=0, outcome="succeeded", manifest={})
    assert live_attempts(tmp_path) == ()


def test_an_observation_for_one_try_does_not_attach_to_another(tmp_path):
    journal = live_record(tmp_path)
    log = ObservationLog(tmp_path, journal.identity)
    log.record(0, "running")
    with journal.claim():
        journal.publish_terminal(try_number=0, outcome="failed", manifest={})
        one = journal.begin_try()
        journal.append(
            "submit_intent",
            **{"try": one, "transport": "lsf-interactive", "substrate": "lsf-interactive"},
        )
    assert status_of(tmp_path, journal.identity).observed is None


def test_removing_the_try_suffix_from_the_key_fails_this_test(tmp_path):
    journal = live_record(tmp_path)
    rows = observe(
        tmp_path,
        LSFStatusReader(Reader([(journal.identity, "RUN")])),
    )
    assert rows[0].observed is None
