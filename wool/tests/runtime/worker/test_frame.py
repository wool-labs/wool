"""Unit tests for the Frame subtype hierarchy in :mod:`wool.runtime.worker.frame`."""

import threading
import warnings
from uuid import uuid4

import cloudpickle
import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

import wool
from tests.helpers import scoped_context
from wool import protocol
from wool.runtime.context.exceptions import SerializationWarning
from wool.runtime.worker.frame import AckResponseFrame
from wool.runtime.worker.frame import ExceptionResponseFrame
from wool.runtime.worker.frame import Frame
from wool.runtime.worker.frame import NackResponseFrame
from wool.runtime.worker.frame import NextRequestFrame
from wool.runtime.worker.frame import RequestFrame
from wool.runtime.worker.frame import ResponseFrame
from wool.runtime.worker.frame import ResultResponseFrame
from wool.runtime.worker.frame import SendRequestFrame
from wool.runtime.worker.frame import TaskRequestFrame
from wool.runtime.worker.frame import ThrowRequestFrame
from wool.runtime.worker.frame import _safely_serialize_exception


def _duplicate_key_wire(key: tuple[str, str]) -> protocol.ChainManifest:
    """Build a wire ChainManifest carrying *key* twice.

    Duplicate ``(namespace, name)`` keys are undefined behaviour Wool's own
    encoder never emits; this fabricates the malformed wire a buggy or
    hostile peer could send so the strict-mode decode path raises.
    """
    return protocol.ChainManifest(
        id=uuid4().hex,
        vars=[
            protocol.ContextVar(namespace=key[0], name=key[1]),
            protocol.ContextVar(namespace=key[0], name=key[1]),
        ],
    )


def _duplicate_key_response(key: tuple[str, str]) -> protocol.Response:
    """Build a wire Response whose chain manifest carries *key* twice."""
    response = protocol.Response(
        result=protocol.Message(dump=wool.__serializer__.dumps("value"))
    )
    response.context.CopyFrom(_duplicate_key_wire(key))
    return response


class TestSafelySerializeException:
    """Tests for the type-preserving exception serialization fallback."""

    def test_safely_serialize_exception_should_ship_bare_type_when_unpicklable(self):
        """Test the fallback ships the bare type when only attachments fail.

        Given:
            A StopAsyncIteration whose __cause__ drags an un-picklable
            object, so the primary serialization and the
            attachment-carrying fallback both fail.
        When:
            It is serialized via the exception serialization helper.
        Then:
            The wire payload should deserialize to a StopAsyncIteration
            with its original args — the bare reconstructed type is
            shipped rather than demoted to a generic RuntimeError, so the
            caller's except clause still matches.
        """

        # Arrange
        class _Unpicklable(Exception):
            def __init__(self):
                super().__init__()
                self._lock = threading.Lock()

        exc = StopAsyncIteration("from coroutine")
        exc.__cause__ = _Unpicklable()

        # Act
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            dump = _safely_serialize_exception(wool.__serializer__, exc)
        restored = wool.__serializer__.loads(dump)

        # Assert
        assert isinstance(restored, StopAsyncIteration)
        assert restored.args == ("from coroutine",)

    def test_safely_serialize_exception_should_demote_to_runtime_error(self):
        """Test the fallback demotes to RuntimeError when args cannot pickle.

        Given:
            An exception whose own args carry an un-picklable object, so
            no reconstruction of its type can be serialized.
        When:
            It is serialized via the exception serialization helper.
        Then:
            The wire payload should deserialize to a RuntimeError naming
            the original class — the always-picklable last resort when
            the bare type itself cannot survive.
        """
        # Arrange
        exc = ValueError(threading.Lock())

        # Act
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            dump = _safely_serialize_exception(wool.__serializer__, exc)
        restored = wool.__serializer__.loads(dump)

        # Assert
        assert type(restored) is RuntimeError
        assert "ValueError" in str(restored)

    def test_safely_serialize_exception_should_warn_fidelity_loss(self):
        """Test the fallback reports fidelity loss for an unreconstructible cause.

        Given:
            An un-picklable exception (so the primary serialization
            fails) whose __cause__ is an exception that raises when its
            type is re-constructed.
        When:
            It is serialized via the exception serialization helper.
        Then:
            The wire payload should still deserialize to the original
            top-level type with the un-walkable cause level truncated
            rather than taking the whole exception down, and a
            SerializationWarning should report the fidelity loss. The
            warning should name the original exception class and
            carry the primary serialization failure as cause, with no
            var_key or direction (the failure is exception-fidelity
            loss, not a chain-manifest hop).
        """

        # Arrange
        class _OnceConstructible(Exception):
            _made = False

            def __init__(self, *args):
                if type(self)._made:
                    raise RuntimeError("cannot rebuild")
                type(self)._made = True
                super().__init__(*args)

        bad_cause = _OnceConstructible("c")
        bad_cause.__cause__ = ValueError("deeper")
        bad_cause.__cause__.__cause__ = RuntimeError("deepest")
        exc = ValueError("top")
        exc._lock = threading.Lock()  # un-picklable __dict__ → primary fails
        exc.__cause__ = bad_cause

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            dump = _safely_serialize_exception(wool.__serializer__, exc)
        restored = wool.__serializer__.loads(dump)

        # Assert
        assert type(restored) is ValueError
        assert restored.args == ("top",)
        assert restored.__cause__ is None  # chain truncated at the bad level
        fidelity = [
            w.message for w in caught if isinstance(w.message, SerializationWarning)
        ]
        assert fidelity
        assert fidelity[0].original_type is ValueError
        assert fidelity[0].cause is not None
        assert fidelity[0].var_key is None
        assert fidelity[0].direction is None

    def test_strict_mode_should_still_ship_exception_when_warnings_are_errors(self):
        """Test fidelity loss stays non-fatal even when warnings are errors.

        Given:
            An un-picklable exception (so the fallback runs) serialized
            while SerializationWarning is promoted to an error.
        When:
            It is serialized via the exception serialization helper.
        Then:
            The promoted warning should be swallowed and the bare type
            still shipped — fidelity loss must never deprive the caller
            of the primary exception signal.
        """
        # Arrange
        exc = ValueError("strict")
        exc._lock = threading.Lock()  # un-picklable __dict__ → primary fails

        # Act
        with warnings.catch_warnings():
            warnings.simplefilter("error", SerializationWarning)
            dump = _safely_serialize_exception(wool.__serializer__, exc)
        restored = wool.__serializer__.loads(dump)

        # Assert
        assert type(restored) is ValueError
        assert restored.args == ("strict",)

    def test_safely_serialize_exception_should_cap_cause_chain(self):
        """Test the fallback bounds an over-deep __cause__ chain.

        Given:
            An un-picklable exception whose __cause__ chain is deeper
            than the walk's depth limit.
        When:
            It is serialized via the exception serialization helper.
        Then:
            The wire payload should deserialize to the original
            top-level type with the reconstructed cause chain capped
            at the walk bound, and a SerializationWarning should
            report the fidelity loss.
        """
        # Arrange
        top = ValueError("L0")
        top._lock = threading.Lock()  # un-picklable __dict__ → primary fails
        node = top
        for i in range(70):  # exceeds the 64-level walk bound
            nxt = ValueError(f"L{i + 1}")
            node.__cause__ = nxt
            node = nxt

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            dump = _safely_serialize_exception(wool.__serializer__, top)
        restored = wool.__serializer__.loads(dump)

        # Assert
        assert type(restored) is ValueError
        assert restored.args == ("L0",)
        depth = 0
        link = restored.__cause__
        while link is not None:
            depth += 1
            link = link.__cause__
        assert depth == 64  # capped at the walk bound, tail dropped
        fidelity = [
            w.message for w in caught if isinstance(w.message, SerializationWarning)
        ]
        assert fidelity

    @given(
        exc_type=st.sampled_from(
            [ValueError, RuntimeError, KeyError, TypeError, StopAsyncIteration]
        ),
        message=st.text(),
    )
    @settings(max_examples=100, deadline=None)
    def test_exception_payload_should_round_trip_through_the_send_path(
        self, exc_type, message
    ):
        """Test an exception round-trips through the exception-frame wire path.

        Given:
            Any standard exception type constructed with arbitrary
            message text.
        When:
            It is shipped via ``ExceptionResponseFrame.for_send`` and
            decoded back with :meth:`Frame.from_protobuf`.
        Then:
            The restored payload should preserve the exception type and
            args — the type-preserving serializer survives the wire
            round trip through the public frame path.
        """
        # Arrange
        exc = exc_type(message)

        # Act
        wire = ExceptionResponseFrame.for_send(
            exc, wire_chain_manifest=None
        ).to_protobuf()
        restored = Frame.from_protobuf(wire)

        # Assert
        assert isinstance(restored, ExceptionResponseFrame)
        assert type(restored.payload) is exc_type
        assert restored.payload.args == exc.args


class TestFrameHierarchy:
    """Tests for the Frame / RequestFrame / ResponseFrame / leaf layout."""

    def test_request_frame_should_declare_request_wire_type(self):
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

    def test_response_frame_should_declare_response_wire_type(self):
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
    def test_leaf_should_declare_payload_field_and_inherit_intermediate(
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

    def test_duplicate_payload_field_should_raise(self):
        """Test a second leaf claiming an in-use payload field raises.

        Given:
            A concrete leaf already registered under a payload-oneof field
            (``TaskRequestFrame`` under ``"task"``).
        When:
            Another concrete subclass claims the same ``field``.
        Then:
            __init_subclass__ should raise TypeError naming the conflict,
            so schema drift fails loudly instead of silently shadowing the
            original leaf.
        """
        # Act & assert
        with pytest.raises(TypeError, match="already registered"):

            class _DuplicateTask(RequestFrame, field="task"):
                pass

    def test_fieldless_subclass_should_not_register(self):
        """Test a subclass that declares no field is not a dispatch target.

        Given:
            A Frame subclass defined without the ``field=`` class keyword,
            as the abstract intermediates are.
        When:
            The class body is evaluated.
        Then:
            __init_subclass__ should neither raise nor register it in the
            leaf dispatch table.
        """
        # Act

        class _Intermediate(Frame):
            pass

        # Assert
        assert _Intermediate not in Frame._frame_by_field.values()

    def test_abstract_payload_hooks_should_raise_not_implemented(self):
        """Test the base Frame payload hooks raise when not overridden.

        Given:
            The abstract Frame base, whose ``_decode_payload`` /
            ``_encode_payload`` are not overridden.
        When:
            Each default hook is invoked directly.
        Then:
            Both should raise NotImplementedError naming the hook — a
            mistyped or abstract frame fails loudly rather than
            silently mis-encoding.
        """
        # Act & assert
        with pytest.raises(NotImplementedError, match="_decode_payload"):
            Frame._decode_payload(object(), serializer=wool.__serializer__)
        with pytest.raises(NotImplementedError, match="_encode_payload"):
            Frame(payload=None)._encode_payload(serializer=wool.__serializer__)


class TestFrameFromProtobuf:
    """Tests for :meth:`Frame.from_protobuf` leaf dispatch."""

    def test_from_protobuf_should_return_next_request_frame(self):
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

    def test_from_protobuf_should_return_result_response_frame(self):
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

    def test_empty_request_should_raise_value_error(self):
        """Test from_protobuf rejects a wire envelope with no payload.

        Given:
            A wire :class:`protocol.Request` with no payload oneof set.
        When:
            :meth:`Frame.from_protobuf` decodes it.
        Then:
            It should raise ValueError — an envelope must carry exactly
            one payload variant.
        """
        # Act & assert
        with pytest.raises(ValueError, match="no payload"):
            Frame.from_protobuf(protocol.Request())

    def test_from_protobuf_should_return_ack_response_frame(self):
        """Test from_protobuf returns an AckResponseFrame for an ``ack`` response.

        Given:
            A wire :class:`protocol.Response` carrying the ``ack`` payload.
        When:
            :meth:`Frame.from_protobuf` decodes it.
        Then:
            The returned frame should be an :class:`AckResponseFrame`
            with no payload — Ack is a pure boundary signal.
        """
        # Arrange
        wire = protocol.Response(ack=protocol.Ack())

        # Act
        frame = Frame.from_protobuf(wire)

        # Assert
        assert isinstance(frame, AckResponseFrame)
        assert frame.payload is None

    def test_from_protobuf_should_return_nack_response_frame(self):
        """Test from_protobuf returns a NackResponseFrame carrying the exception.

        Given:
            A wire :class:`protocol.Response` carrying a ``nack`` payload
            whose serialized message is a rejection exception.
        When:
            :meth:`Frame.from_protobuf` decodes it.
        Then:
            The returned frame should be a :class:`NackResponseFrame`
            carrying the deserialized exception.
        """
        # Arrange
        wire = protocol.Response(
            nack=protocol.Nack(
                exception=protocol.Message(
                    dump=wool.__serializer__.dumps(ValueError("rejected"))
                )
            )
        )

        # Act
        frame = Frame.from_protobuf(wire)

        # Assert
        assert isinstance(frame, NackResponseFrame)
        assert isinstance(frame.payload, ValueError)
        assert frame.payload.args == ("rejected",)

    def test_from_protobuf_should_leave_chain_manifest_none_when_no_wire_context(self):
        """Test from_protobuf leaves chain_manifest None when no wire context.

        Given:
            A wire :class:`protocol.Response` carrying a result payload
            but no ``context`` field.
        When:
            :meth:`Frame.from_protobuf` decodes it.
        Then:
            The frame's ``chain_manifest`` should be ``None`` — there is
            no chain state to mount.
        """
        # Arrange
        wire = protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("v")))

        # Act
        frame = Frame.from_protobuf(wire)

        # Assert
        assert frame.chain_manifest is None

    def test_from_protobuf_should_capture_error_when_strict_decode_fails(self):
        """Test from_protobuf captures a strict-mode decode failure on the frame.

        Given:
            A wire :class:`protocol.Response` whose context carries a
            duplicated ``(namespace, name)`` key, decoded under strict
            mode (SerializationWarning promoted to error).
        When:
            :meth:`Frame.from_protobuf` decodes it.
        Then:
            It should capture the :class:`ChainSerializationError` as the
            frame's ``chain_manifest`` value rather than raising — the
            failure is deferred to mount, and the aggregated warning
            carries the duplicated var_key.
        """
        # Arrange
        from wool.runtime.context.exceptions import ChainSerializationError

        key = ("dup_ns", "dup_var")

        # Act
        with warnings.catch_warnings():
            warnings.simplefilter("error", SerializationWarning)
            frame = Frame.from_protobuf(_duplicate_key_response(key))

        # Assert
        assert isinstance(frame.chain_manifest, ChainSerializationError)
        assert frame.chain_manifest.warnings[0].var_key == key

    @given(
        payload=st.one_of(st.text(), st.integers(), st.binary(), st.lists(st.integers()))
    )
    @settings(max_examples=100, deadline=None)
    def test_result_payload_should_round_trip_through_the_send_path(self, payload):
        """Test a result payload round-trips through the result-frame wire path.

        Given:
            Any picklable result payload.
        When:
            It is shipped via ``ResultResponseFrame.for_send`` and
            decoded back with :meth:`Frame.from_protobuf`.
        Then:
            The restored frame should carry an equal payload.
        """
        # Arrange & act
        wire = ResultResponseFrame.for_send(
            payload, wire_chain_manifest=None
        ).to_protobuf()
        restored = Frame.from_protobuf(wire)

        # Assert
        assert isinstance(restored, ResultResponseFrame)
        assert restored.payload == payload


class TestFrameToProtobuf:
    """Tests for :meth:`Frame.to_protobuf` wire-envelope emission."""

    def test_to_protobuf_should_copy_captured_wire_chain_manifest_into_envelope(self):
        """Test to_protobuf copies a captured chain manifest into the envelope.

        Given:
            A response frame built via ``for_send`` with an explicit
            non-None chain manifest.
        When:
            :meth:`Frame.to_protobuf` encodes it.
        Then:
            The emitted wire envelope should carry the ``context`` field
            — the armed chain rides out on the frame.
        """
        # Arrange
        wire_chain_manifest = protocol.ChainManifest(id=uuid4().hex)
        frame = ResultResponseFrame.for_send(
            "value", wire_chain_manifest=wire_chain_manifest
        )

        # Act
        wire = frame.to_protobuf()

        # Assert
        assert wire.HasField("context")
        assert wire.context.id == wire_chain_manifest.id

    def test_for_send_should_default_serializer_when_omitted(self):
        """Test for_send falls back to the package serializer when none is given.

        Given:
            A boundary frame (AckResponseFrame) that does not override
            for_send.
        When:
            ``for_send`` is called with no serializer argument.
        Then:
            The frame should encode and decode intact — the omitted
            serializer falls back to the package default, so the send
            path still round-trips.
        """
        # Act
        wire = AckResponseFrame.for_send().to_protobuf()
        restored = Frame.from_protobuf(wire)

        # Assert
        assert isinstance(restored, AckResponseFrame)


class TestChainsDecodeErrorOntoPayload:
    """Tests for the mixin's chain-walk into the payload exception."""

    def _decode_failure(self):
        """Build a synthetic strict-mode decode failure — the union's error arm.

        ``Frame.chain_manifest`` is typed
        ``ChainManifest | ChainSerializationError | None``, so a failed
        decode is represented by the error itself; the chaining tests pass
        this directly as ``chain_manifest=``.
        """
        from wool.runtime.context.exceptions import ChainSerializationError
        from wool.runtime.context.exceptions import SerializationWarning

        return ChainSerializationError(SerializationWarning("synthetic decode failure"))

    def test_exception_frame_should_chain_decode_error_onto_payload(self):
        """Test ExceptionResponseFrame.mount chains decode_error onto the payload.

        Given:
            An :class:`ExceptionResponseFrame` whose chain manifest
            carries a strict-mode :class:`ChainSerializationError`.
        When:
            ``frame.mount()`` runs.
        Then:
            The decode error should land at the end of the payload
            exception's ``__context__`` chain rather than propagating
            out — the routine exception remains the primary signal.
        """
        # Arrange
        payload = RuntimeError("worker failed")
        frame = ExceptionResponseFrame(
            payload=payload, chain_manifest=self._decode_failure()
        )

        # Act
        with scoped_context():
            frame.mount()

        # Assert
        chained = payload.__context__
        from wool.runtime.context.exceptions import ChainSerializationError as _CSE

        assert isinstance(chained, _CSE)

    def test_nack_frame_should_chain_decode_error_onto_payload(self):
        """Test NackResponseFrame.mount chains decode_error onto payload.__context__.

        Given:
            A :class:`NackResponseFrame` whose context manifest carries
            a strict-mode :class:`ChainSerializationError`.
        When:
            ``frame.mount()`` runs.
        Then:
            The decode error should land at the end of the rejection
            exception's ``__context__`` chain.
        """
        # Arrange
        rejection = RuntimeError("rejected pre-routine")
        frame = NackResponseFrame(
            payload=rejection, chain_manifest=self._decode_failure()
        )

        # Act
        with scoped_context():
            frame.mount()

        # Assert
        chained = rejection.__context__
        from wool.runtime.context.exceptions import ChainSerializationError as _CSE

        assert isinstance(chained, _CSE)

    def test_throw_frame_should_chain_decode_error_onto_payload(self):
        """Test ThrowRequestFrame.mount chains decode_error onto payload.__context__.

        Given:
            A :class:`ThrowRequestFrame` whose context manifest carries
            a strict-mode :class:`ChainSerializationError`.
        When:
            ``frame.mount()`` runs.
        Then:
            The decode error should land at the end of the thrown
            exception's ``__context__`` chain so the worker-side
            routine sees both signals on the eventual ``athrow``.
        """
        # Arrange
        thrown = ValueError("caller-side throw payload")
        frame = ThrowRequestFrame(payload=thrown, chain_manifest=self._decode_failure())

        # Act
        with scoped_context():
            frame.mount()

        # Assert
        chained = thrown.__context__
        from wool.runtime.context.exceptions import ChainSerializationError as _CSE

        assert isinstance(chained, _CSE)

    def test_non_exception_leaf_should_raise_decode_error(self):
        """Test ResultResponseFrame.mount raises decode_error (no mixin).

        Given:
            A :class:`ResultResponseFrame` (no chain-walk mixin) whose
            context manifest carries a strict-mode
            :class:`ChainSerializationError`.
        When:
            ``frame.mount()`` runs.
        Then:
            The decode error should raise raw — non-exception-bearing
            leaves don't chain on payload.
        """
        # Arrange
        from wool.runtime.context.exceptions import ChainSerializationError

        frame = ResultResponseFrame(
            payload="value", chain_manifest=self._decode_failure()
        )

        # Act & assert
        with scoped_context():
            with pytest.raises(ChainSerializationError):
                frame.mount()

    def test_mount_should_append_decode_error_past_existing_context_chain(self):
        """Test mount appends the decode error past a pre-existing __context__.

        Given:
            An :class:`ExceptionResponseFrame` whose payload exception
            already has a ``__context__`` chain, plus a strict-mode
            decode error.
        When:
            ``frame.mount()`` runs.
        Then:
            The decode error should land at the *end* of the existing
            chain — the pre-existing context is preserved, and the
            decode error chains beyond it.
        """
        # Arrange
        from wool.runtime.context.exceptions import ChainSerializationError as _CSE

        inner = ValueError("pre-existing context")
        payload = RuntimeError("worker failed")
        payload.__context__ = inner
        frame = ExceptionResponseFrame(
            payload=payload, chain_manifest=self._decode_failure()
        )

        # Act
        with scoped_context():
            frame.mount()

        # Assert
        assert payload.__context__ is inner  # preserved, not overwritten
        assert isinstance(inner.__context__, _CSE)  # decode error chained past it
