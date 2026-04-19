import pytest

from wool import protocol

EXPECTED_MESSAGE_EXPORTS = [
    "Ack",
    "Message",
    "Nack",
    "Request",
    "Response",
    "RuntimeContext",
    "StopRequest",
    "Task",
    "TaskEnvelope",
    "Void",
    "WorkerMetadata",
]

EXPECTED_GRPC_EXPORTS = [
    "WorkerServicer",
    "WorkerStub",
    "add_WorkerServicer_to_server",
]


class TestExports:
    """Tests for :py:mod:`wool.protocol` export completeness."""

    @pytest.mark.parametrize("name", EXPECTED_MESSAGE_EXPORTS + EXPECTED_GRPC_EXPORTS)
    def test_exports_accessible(self, name):
        """Test that each expected name is importable from protocol.

        Given:
            A name from the expected public API.
        When:
            Accessed as an attribute of the protocol module.
        Then:
            It should resolve without error.
        """
        assert hasattr(protocol, name)

    def test___all___complete(self):
        """Test __all__ contains every expected public name.

        Given:
            The protocol module.
        When:
            Comparing __all__ against the expected exports.
        Then:
            Every expected name should be present.
        """
        all_names = set(protocol.__all__)
        expected = set(EXPECTED_MESSAGE_EXPORTS + EXPECTED_GRPC_EXPORTS)
        assert expected <= all_names


class TestMessageConstruction:
    """Tests for constructing wire protocol messages."""

    def test_task_fields(self):
        """Test Task message field assignment.

        Given:
            Protobuf field values for a Task message.
        When:
            A Task message is constructed.
        Then:
            All fields should round-trip correctly.
        """
        task = protocol.Task(
            version="1.0.0",
            id="abc-123",
            caller="parent-id",
            tag="gpu",
            proxy_id="proxy-1",
            proxy=b"proxy-bytes",
            callable=b"fn-bytes",
            args=b"args-bytes",
            kwargs=b"kwargs-bytes",
            timeout=30,
        )
        assert task.version == "1.0.0"
        assert task.id == "abc-123"
        assert task.caller == "parent-id"
        assert task.tag == "gpu"
        assert task.proxy_id == "proxy-1"
        assert task.proxy == b"proxy-bytes"
        assert task.callable == b"fn-bytes"
        assert task.args == b"args-bytes"
        assert task.kwargs == b"kwargs-bytes"
        assert task.timeout == 30

    def test_runtime_context_fields(self):
        """Test RuntimeContext optional dispatch_timeout semantics.

        Given:
            A ``dispatch_timeout`` value for the backcompat
            RuntimeContext submessage.
        When:
            RuntimeContext is constructed with and without the value.
        Then:
            The value round-trips and ``HasField`` reflects presence.
        """
        populated = protocol.RuntimeContext(dispatch_timeout=5.5)
        assert populated.dispatch_timeout == 5.5
        assert populated.HasField("dispatch_timeout") is True

        empty = protocol.RuntimeContext()
        assert empty.HasField("dispatch_timeout") is False

    def test_task_envelope_fields(self):
        """Test TaskEnvelope message is a strict subset of Task.

        Given:
            Fields 1-4 of a Task message.
        When:
            A TaskEnvelope is constructed.
        Then:
            It should contain only the metadata fields.
        """
        envelope = protocol.TaskEnvelope(
            version="1.0.0",
            id="abc-123",
            caller="parent-id",
            tag="gpu",
        )
        assert envelope.version == "1.0.0"
        assert envelope.id == "abc-123"

    def test_worker_metadata_fields(self):
        """Test WorkerMetadata message construction.

        Given:
            Worker metadata field values.
        When:
            A WorkerMetadata message is constructed.
        Then:
            All fields should be set correctly.
        """
        wm = protocol.WorkerMetadata(
            uid="uid-1",
            address="localhost:50051",
            pid=1234,
            version="1.0.0",
            tags=["gpu", "ml"],
            extra={"region": "us-west"},
            secure=True,
        )
        assert wm.uid == "uid-1"
        assert wm.address == "localhost:50051"
        assert wm.pid == 1234
        assert list(wm.tags) == ["gpu", "ml"]
        assert dict(wm.extra) == {"region": "us-west"}
        assert wm.secure is True

    def test_ack_with_version(self):
        """Test Ack message carries a version string.

        Given:
            A version string.
        When:
            An Ack message is constructed.
        Then:
            The version field should be set.
        """
        ack = protocol.Ack(version="1.0.0")
        assert ack.version == "1.0.0"

    def test_nack_with_reason(self):
        """Test Nack message carries a reason string.

        Given:
            A rejection reason.
        When:
            A Nack message is constructed.
        Then:
            The reason field should be set.
        """
        nack = protocol.Nack(reason="version mismatch")
        assert nack.reason == "version mismatch"


class TestOneofBehavior:
    """Tests for Request/Response oneof semantics."""

    def test_request_task_oneof(self):
        """Test Request oneof selects 'task' when a Task is provided.

        Given:
            A Request message with a Task payload.
        When:
            WhichOneof is queried.
        Then:
            It should return 'task'.
        """
        req = protocol.Request(task=protocol.Task(id="1"))
        assert req.WhichOneof("payload") == "task"

    def test_request_next_oneof(self):
        """Test Request oneof selects 'next' when a Void is provided.

        Given:
            A Request message with a Void next payload.
        When:
            WhichOneof is queried.
        Then:
            It should return 'next'.
        """
        req = protocol.Request(next=protocol.Void())
        assert req.WhichOneof("payload") == "next"

    def test_request_send_oneof(self):
        """Test Request oneof selects 'send' when a Message is provided.

        Given:
            A Request message with a send Message payload.
        When:
            WhichOneof is queried.
        Then:
            It should return 'send'.
        """
        req = protocol.Request(send=protocol.Message(dump=b"data"))
        assert req.WhichOneof("payload") == "send"

    def test_request_throw_oneof(self):
        """Test Request oneof selects 'throw' when a Message is provided.

        Given:
            A Request message with a throw Message payload.
        When:
            WhichOneof is queried.
        Then:
            It should return 'throw'.
        """
        req = protocol.Request(throw=protocol.Message(dump=b"exc"))
        assert req.WhichOneof("payload") == "throw"

    def test_response_ack_oneof(self):
        """Test Response oneof selects 'ack'.

        Given:
            A Response message with an Ack payload.
        When:
            WhichOneof is queried.
        Then:
            It should return 'ack'.
        """
        resp = protocol.Response(ack=protocol.Ack(version="1.0.0"))
        assert resp.WhichOneof("payload") == "ack"

    def test_response_nack_oneof(self):
        """Test Response oneof selects 'nack'.

        Given:
            A Response message with a Nack payload.
        When:
            WhichOneof is queried.
        Then:
            It should return 'nack'.
        """
        resp = protocol.Response(nack=protocol.Nack(reason="denied"))
        assert resp.WhichOneof("payload") == "nack"

    def test_response_result_oneof(self):
        """Test Response oneof selects 'result'.

        Given:
            A Response message with a result Message payload.
        When:
            WhichOneof is queried.
        Then:
            It should return 'result'.
        """
        resp = protocol.Response(result=protocol.Message(dump=b"ok"))
        assert resp.WhichOneof("payload") == "result"

    def test_response_exception_oneof(self):
        """Test Response oneof selects 'exception'.

        Given:
            A Response message with an exception Message payload.
        When:
            WhichOneof is queried.
        Then:
            It should return 'exception'.
        """
        resp = protocol.Response(exception=protocol.Message(dump=b"err"))
        assert resp.WhichOneof("payload") == "exception"

    def test_empty_request_oneof_is_none(self):
        """Test empty Request has no oneof selection.

        Given:
            An empty Request message.
        When:
            WhichOneof is queried.
        Then:
            It should return None.
        """
        req = protocol.Request()
        assert req.WhichOneof("payload") is None

    def test_empty_response_oneof_is_none(self):
        """Test empty Response has no oneof selection.

        Given:
            An empty Response message.
        When:
            WhichOneof is queried.
        Then:
            It should return None.
        """
        resp = protocol.Response()
        assert resp.WhichOneof("payload") is None


class TestProtobufImportError:
    """Tests for ProtobufImportError fallback."""

    def test_protobuf_import_error_class_exists(self):
        """Test ProtobufImportError is importable.

        Given:
            The exception module.
        When:
            ProtobufImportError is imported.
        Then:
            It should be a subclass of ImportError.
        """
        from wool.protocol.exception import ProtobufImportError

        assert issubclass(ProtobufImportError, ImportError)
