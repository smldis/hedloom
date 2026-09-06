import pytest
from hedloom_exec.durability import execute, Durability
from hedloom_exec.transport import InProcessTransport, SubmissionRefused
from hedloom_exec.journal import AttemptJournal


def test_selection_and_binding_arrive_before_submit_and_match_reuse(tmp_path):
    notices = []
    class Transport(InProcessTransport):
        def submit(self, identity, bundle):
            assert len(notices) == 2
            assert notices[0].workspace_known is False
            assert notices[1].workspace_known is True
            return super().submit(identity, bundle)
    transport = Transport({'op': lambda: 42})
    first = execute(transport, {'operation': 'op'}, durability=Durability.RECORDED,
                    root=str(tmp_path), publish_selection=notices.append)
    assert (notices[0].record, notices[0].try_number) == (first.record, first.try_number)
    notices.clear()
    second = execute(transport, {'operation': 'op'}, durability=Durability.RECORDED,
                     root=str(tmp_path), publish_selection=notices.append)
    assert notices[0].disposition == 'completed'
    assert (second.record, second.try_number) == (first.record, first.try_number)


def test_publication_failure_is_diagnostic_only(tmp_path):
    def fail(selection): raise OSError('publisher cannot write')
    with pytest.warns(RuntimeWarning, match='selection publication failed'):
        result = execute(InProcessTransport({'op': lambda: 42}), {'operation': 'op'},
                         durability=Durability.RECORDED, root=str(tmp_path), publish_selection=fail)
    assert result.value == 42
    assert result.selection.try_number == result.try_number
    assert result.publication_errors == ("OSError: publisher cannot write",) * 2


def test_snapshot_torn_tail_does_not_repair_or_claim(tmp_path):
    result = execute(InProcessTransport({'op': lambda: 42}), {'operation': 'op'},
                     durability=Durability.RECORDED, root=str(tmp_path))
    journal = result.journal
    original = journal.log_path.read_bytes()
    journal.log_path.write_bytes(original + b'{')
    before = journal.log_path.stat().st_mtime_ns
    state, diagnostics = journal.snapshot()
    assert state.outcome == 'succeeded' and diagnostics == ('incomplete event tail',)
    assert journal.log_path.stat().st_mtime_ns == before
    with pytest.raises(Exception): journal.fold()


def test_attach_and_standing_older_than_current_are_observed(tmp_path):
    from hedloom_exec.attempt import launch_or_attach, reconcile
    from hedloom_exec.identity import attempt_identity
    transport = InProcessTransport({'op': lambda: 42})
    journal = AttemptJournal(tmp_path, attempt_identity(computation_digest='observer').rendered)
    bundle = {'operation': 'op'}
    launch_or_attach(journal, transport, bundle)
    notices = []
    attached = launch_or_attach(journal, transport, bundle, publish_selection=notices.append)
    assert attached.disposition == 'attached'
    assert notices[0].try_number == 0 and notices[0].disposition == 'attached'
    reconcile(journal, transport)
    with journal.claim():
        number = journal.begin_try()
        journal.publish_terminal(try_number=number, outcome='failed', manifest={})
    notices.clear()
    reused = launch_or_attach(journal, transport, bundle, publish_selection=notices.append)
    assert reused.state.current_try == 1
    assert notices[0].try_number == 0 and notices[0].disposition == 'completed'


@pytest.mark.parametrize('broken_publisher', [False, True])
def test_submission_failure_keeps_selection_when_publication_is_absent_or_fails(tmp_path, broken_publisher):
    import warnings
    class Refusing(InProcessTransport):
        def submit(self, identity, bundle):
            raise SubmissionRefused('allocation happened, acceptance did not')
    def fail(selection):
        raise OSError('publication unavailable')
    with warnings.catch_warnings():
        warnings.simplefilter('error', RuntimeWarning)
        with pytest.raises(SubmissionRefused) as caught:
            execute(Refusing({}), {'operation': 'op'}, durability=Durability.RECORDED,
                    root=str(tmp_path), publish_selection=fail if broken_publisher else None)
    error = caught.value
    assert error.selection.try_number == 0
    assert error.selection.workspace_known
    journal = AttemptJournal(tmp_path, error.selection.record)
    assert journal.fold().current_try == error.selection.try_number
    assert bool(error.publication_errors) is broken_publisher
    if broken_publisher:
        assert all('publication unavailable' in message for message in error.publication_errors)


def test_reconciliation_failure_retains_exec_selection(tmp_path):
    from hedloom_exec.transport import TransportError
    class Unreachable(InProcessTransport):
        def poll(self, handle):
            raise TransportError('poll unavailable')
    with pytest.raises(TransportError) as caught:
        execute(Unreachable({'op': lambda: 42}), {'operation': 'op'},
                durability=Durability.RECORDED, root=str(tmp_path))
    assert caught.value.selection.try_number == 0
    assert caught.value.selection.record
    assert caught.value.publication_errors == ()


def test_result_cannot_follow_a_rival_to_a_newer_try(tmp_path, monkeypatch):
    """A rival wins immediately after claim release; each caller keeps its try."""
    from contextlib import contextmanager
    from hedloom_exec.attempt import reconcile
    original_claim = AttemptJournal.claim
    rival_results = []
    released = False
    def fail():
        raise ValueError('first computation fails')
    first_transport = InProcessTransport({'op': fail})
    rival_transport = InProcessTransport({'op': lambda: 42})
    bundle = {'operation': 'op'}

    @contextmanager
    def release_to_rival(journal):
        nonlocal released
        with original_claim(journal):
            yield
        if not released:
            released = True
            reconcile(journal, first_transport)
            rival_results.append(execute(rival_transport, bundle,
                durability=Durability.RECORDED, root=str(tmp_path)))

    monkeypatch.setattr(AttemptJournal, 'claim', release_to_rival)
    first = execute(first_transport, bundle, durability=Durability.RECORDED,
                    root=str(tmp_path))
    assert first.outcome == 'failed' and first.try_number == 0
    assert first.selection.try_number == 0
    assert rival_results[0].outcome == 'succeeded' and rival_results[0].try_number == 1
    assert first.record == rival_results[0].record
