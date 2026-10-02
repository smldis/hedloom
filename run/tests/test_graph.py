"""Worker helper diagnostics independent of readiness hosting."""
import threading
import pytest
from hedloom_exec.planned import prepare_invocations
from hedloom_exec.transport import InProcessTransport
from hedloom_run.graph import _require_shippable, _task_key, _occupying, nested_submission_context
from test_async_controller import document


def test_transport_serialization_refusal_names_placement():
    transport = InProcessTransport({})
    transport.unshippable = threading.Lock()
    with pytest.raises(TypeError, match="placement 'local'"):
        _require_shippable({'local': transport})


def test_task_key_preserves_operation_and_authored_name():
    from dataclasses import replace
    item = replace(prepare_invocations(document(('point', {}, None)))[0], input_digest='1234567890')
    assert _task_key(item) == 'operation-point-12345678'


def test_body_occupancy_is_a_nested_submission_guard():
    assert not nested_submission_context()
    with _occupying('local'):
        assert nested_submission_context()
    assert not nested_submission_context()
