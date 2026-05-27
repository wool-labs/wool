"""Unit tests for the Frame subtype hierarchy in :mod:`wool.protocol.frame`."""

from uuid import uuid4

import cloudpickle
import pytest

from tests.helpers import scoped_context
from wool import protocol
from wool.protocol.frame import AckResponseFrame
from wool.protocol.frame import ExceptionResponseFrame
from wool.protocol.frame import Frame
from wool.protocol.frame import NackResponseFrame
from wool.protocol.frame import NextRequestFrame
from wool.protocol.frame import RequestFrame
from wool.protocol.frame import ResponseFrame
from wool.protocol.frame import ResultResponseFrame
from wool.protocol.frame import SendRequestFrame
from wool.protocol.frame import TaskRequestFrame
from wool.protocol.frame import ThrowRequestFrame
from wool.runtime.context.manifest import _ContextManifest


class TestFrameHierarchy:
    """Tests for the Frame / RequestFrame / ResponseFrame / leaf layout."""

    def test_request_frame_wire_type(self):
        """Test RequestFrame declares the Request wire envelope.

        Given:
            The RequestFrame intermediate class.
        When:
            ``_wire_type_name`` is inspected.
        Then:
            It should resolve to the ``Request`` protobuf message class.
        """
        # Act
        wire_cls = getattr(protocol, RequestFrame._wire_type_name)

        # Assert
        assert wire_cls is protocol.Request

    def test_response_frame_wire_type(self):
        """Test ResponseFrame declares the Response wire envelope.

        Given:
            The ResponseFrame intermediate class.
        When:
            ``_wire_type_name`` is inspected.
        Then:
            It should resolve to the ``Response`` protobuf message class.
        """
        # Act
        wire_cls = getattr(protocol, ResponseFrame._wire_type_name)

        # Assert
        assert wire_cls is protocol.Response

    @pytest.mark.parametrize(
        ("leaf", "parent", "field"),
        [
            (TaskRequestFrame, RequestFrame, "task"),
            (NextRequestFrame, RequestFrame, "next"),
            (SendRequestFrame, RequestFrame, "send"),
            (ThrowRequestFrame, RequestFrame, "throw"),
            (AckResponseFrame, ResponseFrame, "ack"),
            (NackResponseFrame, ResponseFrame, "nack"),
            (ResultResponseFrame, ResponseFrame, "result"),
            (ExceptionResponseFrame, ResponseFrame, "exception"),
        ],
    )
    def test_leaf_inherits_intermediate_and_declares_payload_field(
        self, leaf, parent, field
    ):
        """Test each leaf declares its payload-oneof field and inherits its envelope.

        Given:
            A concrete frame leaf class.
        When:
            Its ``_payload_field`` and parentage are inspected.
        Then:
            ``_payload_field`` should match the protobuf oneof field
            name, and ``parent`` should be one of the two abstract
            intermediates (``RequestFrame`` / ``ResponseFrame``).
        """
        # Assert
        assert leaf._payload_field == field
        assert issubclass(leaf, parent)
        assert issubclass(leaf, Frame)


class TestFrameFromProtobuf:
    """Tests for :meth:`Frame.from_protobuf` leaf dispatch."""

    def test_dispatches_request_to_next_request_frame(self):
        """Test from_protobuf returns a NextRequestFrame for a ``next`` request.

        Given:
            A wire :class:`protocol.Request` carrying the ``next`` payload.
        When:
            :meth:`Frame.from_protobuf` decodes it.
        Then:
            The returned frame should be a :class:`NextRequestFrame` instance.
        """
        # Arrange
        wire = protocol.Request(next=protocol.Void())

        # Act
        frame = Frame.from_protobuf(wire)

        # Assert
        assert isinstance(frame, NextRequestFrame)
        assert frame.payload is None

    def test_dispatches_response_to_result_response_frame(self):
        """Test from_protobuf returns a ResultResponseFrame for a ``result`` response.

        Given:
            A wire :class:`protocol.Response` carrying a ``result`` payload.
        When:
            :meth:`Frame.from_protobuf` decodes it.
        Then:
            The returned frame should be a :class:`ResultResponseFrame`
            carrying the deserialised payload.
        """
        # Arrange
        wire = protocol.Response(
            result=protocol.Message(dump=cloudpickle.dumps("value"))
        )

        # Act
        frame = Frame.from_protobuf(wire)

        # Assert
        assert isinstance(frame, ResultResponseFrame)
        assert frame.payload == "value"


class TestChainsDecodeErrorOntoPayload:
    """Tests for the mixin's chain-walk into the payload exception."""

    def _bad_manifest(self) -> _ContextManifest:
        from wool.runtime.context import ContextDecodeError
        from wool.runtime.context import ContextDecodeWarning

        return _ContextManifest(
            chain_id=uuid4(),
            decoded_vars={},
            reset_vars=frozenset(),
            stub_pins=frozenset(),
            decode_error=ContextDecodeError(
                ContextDecodeWarning("synthetic decode failure")
            ),
        )

    def test_exception_frame_chains_decode_error_onto_payload(self):
        """Test ExceptionResponseFrame.mount chains decode_error onto payload.__context__.

        Given:
            An :class:`ExceptionResponseFrame` whose context manifest
            carries a strict-mode :class:`ContextDecodeError`.
        When:
            ``frame.mount()`` runs.
        Then:
            The decode error should land at the end of the payload
            exception's ``__context__`` chain rather than propagating
            out — the routine exception remains the primary signal.
        """
        # Arrange
        payload = RuntimeError("worker failed")
        frame = ExceptionResponseFrame(payload=payload, context=self._bad_manifest())

        # Act
        with scoped_context():
            frame.mount()

        # Assert
        chained = payload.__context__
        from wool.runtime.context import ContextDecodeError as _CDE

        assert isinstance(chained, _CDE)

    def test_nack_frame_chains_decode_error_onto_payload(self):
        """Test NackResponseFrame.mount chains decode_error onto payload.__context__.

        Given:
            A :class:`NackResponseFrame` whose context manifest carries
            a strict-mode :class:`ContextDecodeError`.
        When:
            ``frame.mount()`` runs.
        Then:
            The decode error should land at the end of the rejection
            exception's ``__context__`` chain.
        """
        # Arrange
        rejection = RuntimeError("rejected pre-routine")
        frame = NackResponseFrame(payload=rejection, context=self._bad_manifest())

        # Act
        with scoped_context():
            frame.mount()

        # Assert
        chained = rejection.__context__
        from wool.runtime.context import ContextDecodeError as _CDE

        assert isinstance(chained, _CDE)

    def test_throw_frame_chains_decode_error_onto_payload(self):
        """Test ThrowRequestFrame.mount chains decode_error onto payload.__context__.

        Given:
            A :class:`ThrowRequestFrame` whose context manifest carries
            a strict-mode :class:`ContextDecodeError`.
        When:
            ``frame.mount()`` runs.
        Then:
            The decode error should land at the end of the thrown
            exception's ``__context__`` chain so the worker-side
            routine sees both signals on the eventual ``athrow``.
        """
        # Arrange
        thrown = ValueError("caller-side throw payload")
        frame = ThrowRequestFrame(payload=thrown, context=self._bad_manifest())

        # Act
        with scoped_context():
            frame.mount()

        # Assert
        chained = thrown.__context__
        from wool.runtime.context import ContextDecodeError as _CDE

        assert isinstance(chained, _CDE)

    def test_non_exception_leaf_raises_decode_error(self):
        """Test ResultResponseFrame.mount raises decode_error (no mixin).

        Given:
            A :class:`ResultResponseFrame` (no chain-walk mixin) whose
            context manifest carries a strict-mode
            :class:`ContextDecodeError`.
        When:
            ``frame.mount()`` runs.
        Then:
            The decode error should raise raw — non-exception-bearing
            leaves don't chain on payload.
        """
        # Arrange
        from wool.runtime.context import ContextDecodeError

        frame = ResultResponseFrame(payload="value", context=self._bad_manifest())

        # Act & assert
        with scoped_context():
            with pytest.raises(ContextDecodeError):
                frame.mount()
