"""Unified wire-frame abstraction for dispatch.

A :class:`Frame` couples a wire payload with an optional
:class:`~wool.runtime.context.manifest._ContextManifest` — the
decoded-but-unmounted view of a wire :class:`~wool.protocol.Context`.
The single :meth:`Frame.mount` entry point applies the manifest onto
the active chain, collapsing the four ad-hoc receive paths into one.

Frames are three-tiered:

* :class:`Frame` — abstract base; owns unified
  :meth:`from_protobuf` / :meth:`to_protobuf` dispatch and the
  canonical :meth:`mount` entry point.
* :class:`RequestFrame` / :class:`ResponseFrame` — abstract
  intermediates carrying the wire-envelope class name
  (:class:`~wool.protocol.Request` / :class:`~wool.protocol.Response`).
* Eight concrete leaves (:class:`TaskRequestFrame`,
  :class:`NextRequestFrame`, :class:`SendRequestFrame`,
  :class:`ThrowRequestFrame`, :class:`AckResponseFrame`,
  :class:`NackResponseFrame`, :class:`ResultResponseFrame`,
  :class:`ExceptionResponseFrame`) — each declares its
  ``_payload_field`` (the protobuf oneof name) and provides
  ``_decode_payload`` / ``_encode_payload``. The exception-bearing
  leaves additionally flip ``_chains_decode_onto_payload = True``
  so a :class:`ContextDecodeError` from :meth:`mount` chains onto
  the payload exception's ``__context__`` rather than propagating.

Send-side encoding uses each leaf's ``for_send`` classmethod, which
auto-captures the active wire context.
"""

from __future__ import annotations

import contextvars
import pickle
import warnings
from dataclasses import dataclass
from dataclasses import field
from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar
from typing import Generic
from typing import TypeVar

# Q7 — Frame is generic in its payload type. Each concrete leaf binds
# the type variable to its semantic payload type (Task, BaseException,
# Any for arbitrary values, None for boundary frames). Drops the
# ``cast(...)`` / ``assert isinstance(...)`` calls every encode path
# previously needed to narrow ``payload: Any`` back to its true type.
_P = TypeVar("_P")

# Runtime imports for ``wool``, ``wool.protocol``, and
# ``wool.runtime.context`` are deferred to function bodies — the
# ``wool.protocol`` package re-exports the Frame types via its
# ``__init__``, and a top-level import here would cycle with
# ``wool.runtime.context.base``'s ``from wool import protocol``.

if TYPE_CHECKING:
    from wool import protocol as _protocol
    from wool.runtime.context.manifest import _ContextManifest
    from wool.runtime.routine.task import Task
    from wool.runtime.serializer import Serializer as _Serializer


# Sentinel used by response-leaf ``for_send`` classmethods to
# distinguish "explicit ``wire_context=None`` (do not auto-capture)"
# from "no argument passed (auto-capture via
# ``current_wire_context``)". The worker driver's terminal-exception
# path passes ``wire_context=session._final_wire_context`` which may
# legitimately be ``None`` (the worker stayed unarmed) — that is NOT
# a request to auto-capture the dispatch-handler's own chain on the
# main loop.
_AUTO_CAPTURE: Any = object()


@dataclass
class Frame(Generic[_P]):
    """Abstract base for the wire frames a dispatch exchanges.

    Three-tier hierarchy: :class:`Frame` (this base) →
    :class:`RequestFrame` / :class:`ResponseFrame` (intermediate
    envelopes) → one concrete leaf per protobuf payload oneof.

    A frame is the deserialised view of one
    :class:`~wool.protocol.Request` or :class:`~wool.protocol.Response`
    envelope: the application *payload* plus an optional
    :class:`~wool.runtime.context.manifest._ContextManifest`.

    Generic in the payload type ``_P`` (Q7). Each concrete leaf binds
    the type variable: ``TaskRequestFrame(RequestFrame[Task])``,
    ``ExceptionResponseFrame(ResponseFrame[BaseException])``, etc.
    Drops the ``cast(...)`` / ``assert isinstance(...)`` calls every
    encode path previously needed to narrow ``payload: Any`` back to
    its leaf's true type.

    :meth:`mount` is the single "receive this frame" entry point.
    The base body no-ops when ``context`` is None and otherwise
    routes the manifest through :meth:`Context._update` (live-
    receiver branch) or :meth:`Context._from_manifest` plus
    :meth:`Context.mount` (unarmed-receiver branch). The three
    exception-bearing leaves flip the
    :attr:`_chains_decode_onto_payload` ClassVar so a
    :class:`ContextDecodeError` chains onto the payload exception's
    ``__context__`` rather than propagating.
    """

    payload: _P = None  # type: ignore[assignment]
    context: "_ContextManifest | None" = None
    # Outbound side: ``for_send`` populates these so ``to_protobuf``
    # can emit the captured wire context and use the correct
    # serializer. On inbound frames they stay None — receive-side
    # use goes through ``context`` instead.
    _wire_context: "_protocol.Context | None" = field(
        default=None, init=False, repr=False
    )
    _serializer: "_Serializer | None" = field(default=None, init=False, repr=False)

    # Class-level discriminators. Intermediates set
    # ``_wire_type_name``; concrete leaves set ``_payload_field``.
    # The base values are empty strings; only concrete leaves are
    # ever constructed, and they always override both.
    _wire_type_name: ClassVar[str] = ""
    _payload_field: ClassVar[str] = ""
    # F31 — opt-in marker for intermediate subclasses
    # (``RequestFrame``, ``ResponseFrame``).
    # ``__init_subclass__`` below requires a concrete leaf to override
    # ``_payload_field`` unless this flag is set.
    _frame_intermediate: ClassVar[bool] = True
    # Q5 — leaves whose payload is an exception flip this flag to
    # ``True``. :meth:`mount` then catches a :class:`ContextDecodeError`
    # raised by :meth:`_do_mount` and chains it onto the payload's
    # ``__context__`` rather than propagating. Replaces the previous
    # :class:`_ChainsDecodeErrorOntoPayload` mixin (which required a
    # specific MRO ordering and made the gate hard to discover); a
    # ClassVar flag surfaces the contract at the leaf's own definition
    # site with no inheritance subtlety.
    _chains_decode_onto_payload: ClassVar[bool] = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # An intermediate subclass (RequestFrame / ResponseFrame)
        # explicitly sets ``_frame_intermediate = True`` on its own
        # body. A concrete leaf inherits ``False`` (set in its body)
        # or relies on this default; either way it must provide
        # ``_payload_field``.
        is_intermediate = cls.__dict__.get("_frame_intermediate", False)
        if not is_intermediate and not cls.__dict__.get("_payload_field"):
            raise TypeError(
                f"{cls.__qualname__} is a non-intermediate Frame subclass but "
                f"does not set ``_payload_field``. Concrete leaves must declare "
                f"their protobuf oneof field name as a ClassVar; intermediate "
                f"subclasses must set ``_frame_intermediate = True``."
            )

    @classmethod
    def from_protobuf(
        cls,
        wire: "_protocol.Request | _protocol.Response",
        *,
        serializer: "_Serializer | None" = None,
    ) -> Frame:
        """Decode an incoming wire envelope into the matching leaf.

        Matches ``wire.WhichOneof("payload")`` against the leaf
        registry; the matched leaf class's :meth:`_decode_payload`
        deserialises the per-variant sub-message, and the wire
        context decodes via
        :func:`~wool.runtime.context.manifest._parse_context`.

        :raises ValueError:
            If the payload oneof is unset or unknown.
        """
        import wool
        from wool.runtime.context.manifest import _parse_context

        if serializer is None:
            serializer = wool.__serializer__
        field_name = wire.WhichOneof("payload")
        if field_name is None:
            raise ValueError("wire envelope has no payload set")
        leaf_cls = _LEAVES_BY_FIELD.get(field_name)
        if leaf_cls is None:
            raise ValueError(f"unknown payload oneof: {field_name!r}")
        payload = leaf_cls._decode_payload(wire, serializer=serializer)
        context = _parse_context(
            wire.context if wire.HasField("context") else None,
            serializer=serializer,
        )
        return leaf_cls(payload=payload, context=context)

    def to_protobuf(self) -> "_protocol.Request | _protocol.Response":
        """Encode this frame as its wire envelope.

        Resolves the wire envelope class via the intermediate's
        ``_wire_type_name``, builds it with the leaf's
        :meth:`_encode_payload` kwargs, then copies in
        :attr:`_wire_context` if non-None.

        Lazy-wire-frame: the optional ``context`` field is omitted
        entirely when :attr:`_wire_context` is ``None`` (the active
        chain was unarmed at :meth:`for_send` time).
        """
        import wool
        from wool import protocol

        serializer = self._serializer or wool.__serializer__
        wire_cls = getattr(protocol, self._wire_type_name)
        wire = wire_cls(**self._encode_payload(serializer=serializer))
        if self._wire_context is not None:
            wire.context.CopyFrom(self._wire_context)
        return wire

    @classmethod
    def _decode_payload(cls, wire: Any, *, serializer: "_Serializer") -> Any:
        """Deserialise this leaf's payload sub-message from *wire*.

        Override on each concrete leaf. The default raises so an
        abstract / mistyped frame surfaces loudly at decode time.
        """
        raise NotImplementedError(f"{cls.__name__}._decode_payload must be overridden")

    def _encode_payload(self, *, serializer: "_Serializer") -> dict[str, Any]:
        """Return a kwargs dict for the wire-envelope constructor.

        The dict has exactly one key — the leaf's ``_payload_field``
        — mapped to the encoded sub-message. Override on each
        concrete leaf.
        """
        raise NotImplementedError(
            f"{type(self).__name__}._encode_payload must be overridden"
        )

    @classmethod
    def _build_for_send(
        cls,
        payload: Any,
        *,
        serializer: "_Serializer | None",
        wire_context: "_protocol.Context | None | object" = _AUTO_CAPTURE,
    ) -> Frame:
        """Shared scaffold for the leaves' ``for_send`` classmethods."""
        import wool
        from wool.runtime.context.base import current_wire_context

        if serializer is None:
            serializer = wool.__serializer__
        frame = cls(payload=payload, context=None)
        if wire_context is _AUTO_CAPTURE:
            wire_context = current_wire_context(serializer=serializer)
        frame._wire_context = wire_context  # type: ignore[assignment]
        frame._serializer = serializer
        return frame

    def mount(self, ctx: contextvars.Context | None = None) -> None:
        """Apply the frame's context manifest onto the active chain.

        Gates in order: no-op when ``context`` is None; raise the
        deferred :class:`ContextDecodeError` first so an all-empty
        manifest carrying a strict-mode decode failure still
        surfaces; then short-circuit when the manifest has no
        observable state. Otherwise route through the unified
        :func:`~wool.runtime.context.wire._install_manifest`
        pipeline:

        * Caller-side mount — ``ctx`` is ``None`` (the default).
          The install runs in the current
          :class:`contextvars.Context`, stamps the calling thread /
          task as owners, ensures Wool's task factory is on the
          loop, and merges with the active :class:`Context` when
          armed.
        * Worker-side mount — ``ctx`` is the chain's cached
          :class:`contextvars.Context`. The install runs inside
          ``ctx.run(...)`` so backing writes land in the chain's
          context, with no owner stamping (the cached context
          already owns the chain) and no factory install (the
          worker driver bypasses the factory entirely via
          :func:`~wool.runtime.worker.session._create_step_task`).
          Q4 — replaces the hand-rolled ``ctx.run(_apply_manifest,
          manifest)`` block in the worker driver.

        Leaves whose payload is an exception
        (:class:`ThrowRequestFrame`, :class:`NackResponseFrame`,
        :class:`ExceptionResponseFrame`) flip the
        :attr:`_chains_decode_onto_payload` class flag. Any
        :class:`ContextDecodeError` raised by the mount is appended
        onto :attr:`payload`'s ``__context__`` chain rather than
        propagating — the caller (or routine, in the throw case)
        still gets to handle the primary exception, with the decode
        failure surfaced as a chained cause.

        :param ctx:
            Optional :class:`contextvars.Context` to install into.
            When provided, the install runs via ``ctx.run(...)``.
            Pass ``None`` (the default) for caller-side mounts
            running in the current context.

        :raises ContextDecodeError:
            When ``self.context.decode_error`` is set on a leaf that
            does not chain onto its payload.
        """
        from wool.runtime.context import ContextDecodeError

        try:
            self._do_mount(ctx)
        except ContextDecodeError as decode_err:
            if not self._chains_decode_onto_payload:
                raise
            # The ``_chains_decode_onto_payload`` flag is only set on
            # leaves whose payload is a BaseException
            # (ThrowRequestFrame, NackResponseFrame,
            # ExceptionResponseFrame); pyright can't narrow ``_P``
            # through the ClassVar correlation.
            payload: BaseException = self.payload  # type: ignore[assignment]
            # F25 — cycle guard on the ``__context__`` walk. An
            # adversarial / malformed payload whose ``__context__``
            # eventually points back into the chain previously spun
            # this walk indefinitely; the visited-set short-circuits
            # the first time we'd revisit a node and attaches the
            # decode error there.
            node = payload
            seen: set[int] = {id(node)}
            while node.__context__ is not None and id(node.__context__) not in seen:
                node = node.__context__
                seen.add(id(node))
            node.__context__ = decode_err

    def _do_mount(self, ctx: contextvars.Context | None) -> None:
        """The actual manifest-apply logic, called from :meth:`mount`."""
        if self.context is None:
            return
        if self.context.decode_error is not None:
            raise self.context.decode_error
        # Inline the "manifest has observable state" check (Q2):
        # empty wire frames decode to ``None`` and are filtered at
        # the constructor in :func:`_parse_context`, but a manifest
        # decoded with only a strict-mode failure (raised above) or
        # a manifest that survives decoding but binds no vars and
        # records no resets — neither needs the mount routing below.
        if not (self.context.decoded_vars or self.context.reset_vars):
            return
        from wool.runtime.context.base import current_context
        from wool.runtime.context.wire import _install_manifest

        if ctx is None:
            # Caller-side: stamp owner, ensure factory installed,
            # merge with current Context (``None`` falls through to
            # the unarmed-receiver fresh-install path inside
            # :func:`_install_manifest`).
            _install_manifest(
                self.context,
                stamp_owner=True,
                install_factory=True,
                merge_with=current_context(),
            )
        else:
            # Worker-side: install inside the chain's cached
            # contextvars.Context. The cached context already owns
            # the chain (no owner restamp) and the driver bypasses
            # the loop's task factory (no factory install).
            ctx.run(
                _install_manifest,
                self.context,
                stamp_owner=False,
                install_factory=False,
            )


@dataclass
class RequestFrame(Frame[_P]):
    """Abstract intermediate for worker-bound dispatch frames.

    Each concrete leaf (:class:`TaskRequestFrame`,
    :class:`NextRequestFrame`, :class:`SendRequestFrame`,
    :class:`ThrowRequestFrame`) inherits this intermediate's
    ``_wire_type_name = "Request"`` and declares its own
    ``_payload_field``.
    """

    _wire_type_name: ClassVar[str] = "Request"
    _frame_intermediate: ClassVar[bool] = True


@dataclass
class ResponseFrame(Frame[_P]):
    """Abstract intermediate for caller-bound dispatch frames.

    Each concrete leaf (:class:`AckResponseFrame`,
    :class:`NackResponseFrame`, :class:`ResultResponseFrame`,
    :class:`ExceptionResponseFrame`) inherits this intermediate's
    ``_wire_type_name = "Response"`` and declares its own
    ``_payload_field``.
    """

    _wire_type_name: ClassVar[str] = "Response"
    _frame_intermediate: ClassVar[bool] = True


# ----- Concrete request leaves -----


@dataclass
class TaskRequestFrame(RequestFrame["Task"]):
    """Initial dispatch request carrying a :class:`~wool.routine.Task`."""

    _payload_field: ClassVar[str] = "task"

    @classmethod
    def _decode_payload(
        cls, wire: "_protocol.Request", *, serializer: "_Serializer"
    ) -> Any:
        from wool.runtime.routine.task import Task

        return Task.from_protobuf(wire.task)

    def _encode_payload(self, *, serializer: "_Serializer") -> dict[str, Any]:
        # ``self.payload`` is typed as ``Task`` via ``Frame[Task]``;
        # the runtime assert that previously narrowed ``Any`` to
        # ``Task`` is no longer needed.
        return {"task": self.payload.to_protobuf()}

    @classmethod
    def for_send(
        cls,
        task: Any,
        *,
        serializer: "_Serializer | None" = None,
    ) -> TaskRequestFrame:
        """Build an outbound :class:`TaskRequestFrame`.

        Under the per-frame worker architecture, the initial Task
        frame carries no wire context — it is pure dispatch metadata
        (routine identity, args, kwargs, runtime options). Subsequent
        request frames (:class:`NextRequestFrame`,
        :class:`SendRequestFrame`, :class:`ThrowRequestFrame`) ship
        the full per-step manifest, which the worker driver applies
        inside the chain's cached
        :class:`contextvars.Context`. Auto-capturing context on the
        Task frame would forfeit the per-frame discriminator the
        worker uses to route subsequent frames onto their owning
        chains.
        """
        return cls._build_for_send(  # type: ignore[return-value]
            task, serializer=serializer, wire_context=None
        )


@dataclass
class NextRequestFrame(RequestFrame[None]):
    """Mid-stream pull request (no payload)."""

    _payload_field: ClassVar[str] = "next"

    @classmethod
    def _decode_payload(
        cls, wire: "_protocol.Request", *, serializer: "_Serializer"
    ) -> Any:
        return None

    def _encode_payload(self, *, serializer: "_Serializer") -> dict[str, Any]:
        from wool import protocol

        return {"next": protocol.Void()}

    @classmethod
    def for_send(
        cls,
        *,
        serializer: "_Serializer | None" = None,
    ) -> NextRequestFrame:
        """Build an outbound :class:`NextRequestFrame`. Auto-captures
        the active wire context."""
        return cls._build_for_send(None, serializer=serializer)  # type: ignore[return-value]


@dataclass
class SendRequestFrame(RequestFrame[Any]):
    """Mid-stream ``asend`` request carrying an arbitrary payload."""

    _payload_field: ClassVar[str] = "send"

    @classmethod
    def _decode_payload(
        cls, wire: "_protocol.Request", *, serializer: "_Serializer"
    ) -> Any:
        return serializer.loads(wire.send.dump)

    def _encode_payload(self, *, serializer: "_Serializer") -> dict[str, Any]:
        from wool import protocol

        return {"send": protocol.Message(dump=serializer.dumps(self.payload))}

    @classmethod
    def for_send(
        cls,
        payload: Any,
        *,
        serializer: "_Serializer | None" = None,
    ) -> SendRequestFrame:
        """Build an outbound :class:`SendRequestFrame`. Auto-captures
        the active wire context."""
        return cls._build_for_send(payload, serializer=serializer)  # type: ignore[return-value]


@dataclass
class ThrowRequestFrame(RequestFrame[BaseException]):
    """Mid-stream ``athrow`` request carrying an exception.

    Flips :attr:`_chains_decode_onto_payload` so a wire-context
    decode error chains onto the throw payload's ``__context__``
    rather than abandoning the throw entirely (Q5).
    """

    _payload_field: ClassVar[str] = "throw"
    _chains_decode_onto_payload: ClassVar[bool] = True

    @classmethod
    def _decode_payload(
        cls, wire: "_protocol.Request", *, serializer: "_Serializer"
    ) -> Any:
        return serializer.loads(wire.throw.dump)

    def _encode_payload(self, *, serializer: "_Serializer") -> dict[str, Any]:
        from wool import protocol

        return {"throw": protocol.Message(dump=serializer.dumps(self.payload))}

    @classmethod
    def for_send(
        cls,
        payload: BaseException,
        *,
        serializer: "_Serializer | None" = None,
    ) -> ThrowRequestFrame:
        """Build an outbound :class:`ThrowRequestFrame`. Auto-captures
        the active wire context."""
        return cls._build_for_send(payload, serializer=serializer)  # type: ignore[return-value]


# ----- Concrete response leaves -----


@dataclass
class AckResponseFrame(ResponseFrame[None]):
    """Worker's protocol-version ack for the initial dispatch frame."""

    _payload_field: ClassVar[str] = "ack"

    @classmethod
    def _decode_payload(
        cls, wire: "_protocol.Response", *, serializer: "_Serializer"
    ) -> Any:
        return None

    def _encode_payload(self, *, serializer: "_Serializer") -> dict[str, Any]:
        from wool import protocol

        return {"ack": protocol.Ack(version=protocol.__version__)}

    @classmethod
    def for_send(
        cls,
        *,
        serializer: "_Serializer | None" = None,
    ) -> AckResponseFrame:
        """Build an outbound :class:`AckResponseFrame`.

        Under the per-frame worker architecture, the boundary
        Ack frame carries no wire context — it is pure handshake
        metadata (protocol version). Wire context lives on
        mid-stream payload frames (``Result``/``Exception``), not
        on boundary frames. Symmetric with
        :meth:`TaskRequestFrame.for_send`.
        """
        return cls._build_for_send(  # type: ignore[return-value]
            None, serializer=serializer, wire_context=None
        )


@dataclass
class NackResponseFrame(ResponseFrame[BaseException]):
    """Dispatch-handler rejection response carrying an exception.

    Flips :attr:`_chains_decode_onto_payload` so a wire-context
    decode error chains onto the payload's ``__context__`` (Q5).
    The payload is serialised via :func:`_safely_serialize_exception`
    so an un-picklable exception instance degrades to a
    type-preserving fallback.
    """

    _payload_field: ClassVar[str] = "nack"
    _chains_decode_onto_payload: ClassVar[bool] = True

    @classmethod
    def _decode_payload(
        cls, wire: "_protocol.Response", *, serializer: "_Serializer"
    ) -> Any:
        return serializer.loads(wire.nack.exception.dump)

    def _encode_payload(self, *, serializer: "_Serializer") -> dict[str, Any]:
        from wool import protocol

        return {
            "nack": protocol.Nack(
                exception=protocol.Message(
                    dump=_safely_serialize_exception(serializer, self.payload),
                ),
            )
        }

    @classmethod
    def for_send(
        cls,
        payload: BaseException,
        *,
        serializer: "_Serializer | None" = None,
    ) -> NackResponseFrame:
        """Build an outbound :class:`NackResponseFrame`.

        Under the per-frame worker architecture, the boundary Nack
        frame carries no wire context — it is pure dispatch-rejection
        metadata. Wire context lives on mid-stream payload frames
        (``Result``/``Exception``), not on boundary frames. Symmetric
        with :meth:`TaskRequestFrame.for_send`.
        """
        return cls._build_for_send(  # type: ignore[return-value]
            payload, serializer=serializer, wire_context=None
        )


@dataclass
class ResultResponseFrame(ResponseFrame[Any]):
    """Routine yield / return value response."""

    _payload_field: ClassVar[str] = "result"

    @classmethod
    def _decode_payload(
        cls, wire: "_protocol.Response", *, serializer: "_Serializer"
    ) -> Any:
        return serializer.loads(wire.result.dump)

    def _encode_payload(self, *, serializer: "_Serializer") -> dict[str, Any]:
        from wool import protocol

        return {"result": protocol.Message(dump=serializer.dumps(self.payload))}

    @classmethod
    def for_send(
        cls,
        payload: Any,
        *,
        serializer: "_Serializer | None" = None,
        wire_context: "_protocol.Context | None | object" = _AUTO_CAPTURE,
    ) -> ResultResponseFrame:
        """Build an outbound :class:`ResultResponseFrame`. Accepts an
        explicit *wire_context* (use ``None`` to suppress the field)
        or auto-captures when omitted."""
        return cls._build_for_send(  # type: ignore[return-value]
            payload, serializer=serializer, wire_context=wire_context
        )


@dataclass
class ExceptionResponseFrame(ResponseFrame[BaseException]):
    """Routine-raised exception response.

    Flips :attr:`_chains_decode_onto_payload` so a wire-context
    decode error chains onto the payload's ``__context__`` (Q5).
    The payload is serialised via :func:`_safely_serialize_exception`
    so an un-picklable exception instance degrades to a
    type-preserving fallback.
    """

    _payload_field: ClassVar[str] = "exception"
    _chains_decode_onto_payload: ClassVar[bool] = True

    @classmethod
    def _decode_payload(
        cls, wire: "_protocol.Response", *, serializer: "_Serializer"
    ) -> Any:
        return serializer.loads(wire.exception.dump)

    def _encode_payload(self, *, serializer: "_Serializer") -> dict[str, Any]:
        from wool import protocol

        return {
            "exception": protocol.Message(
                dump=_safely_serialize_exception(serializer, self.payload),
            )
        }

    @classmethod
    def for_send(
        cls,
        payload: BaseException,
        *,
        serializer: "_Serializer | None" = None,
        wire_context: "_protocol.Context | None | object" = _AUTO_CAPTURE,
    ) -> ExceptionResponseFrame:
        """Build an outbound :class:`ExceptionResponseFrame`. Accepts
        an explicit *wire_context* (use ``None`` to suppress the
        field) or auto-captures when omitted."""
        return cls._build_for_send(  # type: ignore[return-value]
            payload, serializer=serializer, wire_context=wire_context
        )


# Dispatch registry — populated after every leaf is defined so
# :meth:`Frame.from_protobuf` can resolve a payload-oneof field name
# back to its concrete leaf class without re-importing the module.
_LEAVES_BY_FIELD: dict[str, type[Frame]] = {
    "task": TaskRequestFrame,
    "next": NextRequestFrame,
    "send": SendRequestFrame,
    "throw": ThrowRequestFrame,
    "ack": AckResponseFrame,
    "nack": NackResponseFrame,
    "result": ResultResponseFrame,
    "exception": ExceptionResponseFrame,
}


def _safely_serialize_exception(
    serializer: "_Serializer",
    exc: BaseException,
) -> bytes:
    """Serialize *exc*, preserving the exception class when the
    original instance carries un-picklable state.

    Prevents un-picklable exception state from converting a
    wool-class failure on the wire into a generic gRPC stream
    error on the caller side. The serializer is
    :class:`CloudpickleSerializer`, which fails on un-picklable
    input via :class:`pickle.PickleError`, :class:`TypeError`
    for un-picklable C types, :class:`AttributeError` for
    un-picklable local closures, or :class:`RecursionError` for
    deeply self-referential graphs.

    **Type-preserving fallback.** Stdlib exception pickling
    round-trips ``(type, args, __dict__)``, dropping
    :attr:`__traceback__`, :attr:`__cause__`, :attr:`__context__`,
    and :attr:`__suppress_context__` — those four are not part of
    the exception's identity. :attr:`__notes__` and other
    ``__dict__`` attributes survive the round-trip; cloudpickle (with
    tblib) preserves :attr:`__cause__` so the dispatch handler's
    ``raise routine_exc from encode_err`` chaining for strict-mode
    context-encode failures survives the wire. When a routine-level
    exception accumulates state that drags an un-picklable C-level
    object into the graph (e.g. a worker-thread frame on
    :attr:`__traceback__` reachable via :attr:`__cause__`), the
    first ``dumps`` raises but the exception's *class* and *args*
    are still picklable on their own. Reconstruct a clean instance
    and reship — the caller's ``except RoutineError`` still
    matches, mirroring the stdlib pickle contract for exceptions.
    Side-channel attachments (notes, the ``__cause__`` chain) are
    lost on the reconstructed instance because the fallback builds
    a fresh ``cls(*exc.args)``; the wire-survival guarantee holds
    only on the primary path.

    If even ``cls(*exc.args)`` cannot be constructed or pickled
    (constructor side effects, unpicklable args), demote to a
    stdlib :class:`RuntimeError` carrying the original class
    name and message — always picklable, so the third ``dumps``
    cannot fail.
    """
    try:
        return serializer.dumps(exc)
    except (pickle.PickleError, TypeError, AttributeError, RecursionError) as primary:
        primary_err = primary
    try:
        cls = type(exc)
        clean = cls(*exc.args)
        # F26 — walk the ``__cause__`` chain recursively, depth-bounded
        # and cycle-guarded. The pre-F26 fallback preserved only one
        # level; deeper chains were silently truncated. Reconstruct
        # each level via the ``cls(*args)`` shape so tracebacks (a
        # frequent source of cloudpickle failures) don't take the
        # chain down with them.
        _MAX_CAUSE_DEPTH = 64
        seen_ids: set[int] = {id(exc)}
        head = clean
        original = exc.__cause__
        lost_levels = 0
        depth = 0
        while (
            original is not None
            and id(original) not in seen_ids
            and depth < _MAX_CAUSE_DEPTH
        ):
            seen_ids.add(id(original))
            depth += 1
            try:
                cause_cls = type(original)
                clean_args = tuple(
                    type(a)(*a.args) if isinstance(a, BaseException) else a
                    for a in original.args
                )
                clean_cause = cause_cls(*clean_args)
                head.__cause__ = clean_cause
                head.__suppress_context__ = True
                head = clean_cause
                original = original.__cause__
            except Exception:
                # Reconstruction failed at this level; stop walking
                # and count the rest as lost.
                lost_levels = 1  # at least this one
                # Count the remaining chain length (bounded) so the
                # warning can report fidelity loss accurately.
                probe = original.__cause__ if original is not None else None
                while (
                    probe is not None
                    and id(probe) not in seen_ids
                    and lost_levels < _MAX_CAUSE_DEPTH
                ):
                    seen_ids.add(id(probe))
                    lost_levels += 1
                    probe = probe.__cause__
                break
        # If the loop exited due to depth bound or detected cycle,
        # surface that via the warning as well.
        if depth >= _MAX_CAUSE_DEPTH and original is not None:
            # Bound the unwalked tail.
            lost_levels = max(lost_levels, 1)
        # Surface exception-fidelity loss via a typed warning. The
        # warning is also attached to the reconstructed exception's
        # ``__context__`` so the caller's diagnostic stack carries
        # the fidelity signal alongside the (degraded) exception
        # they actually catch.
        from wool.runtime.context.errors import SerializationWarning

        fidelity_warning = SerializationWarning(
            f"Exception {type(exc).__qualname__!r} required fallback "
            f"reconstruction; some side-channel state was lost.",
            cause=primary_err,
            original_class=type(exc),
            lost_args=None,
            lost_cause_chain=lost_levels or None,
        )
        try:
            warnings.warn(fidelity_warning, stacklevel=2)
        except SerializationWarning:
            # Strict-mode: the warning was promoted to an error. The
            # F26 contract is that fidelity loss is non-fatal — even
            # under strict mode the caller's primary signal (the
            # routine exception) must reach them. Swallow and continue
            # to ship the degraded copy with the warning attached
            # via ``__context__`` below.
            pass
        # Attach the warning via ``__context__`` so the diagnostic
        # rides on the wire alongside the reconstructed exception
        # without replacing the caller's primary catchable type. Only
        # attach if no ``__context__`` was already set; preserves
        # whatever the caller's exception machinery established.
        if clean.__context__ is None:
            clean.__context__ = fidelity_warning
        return serializer.dumps(clean)
    except Exception:
        # Honor the docstring's "third dumps cannot fail" promise:
        # any reconstruction failure (over-eager ``__init__``
        # validation, custom ``__new__``, un-picklable args, etc.)
        # — not just the narrow pickle/type/recursion set — must
        # fall through to the always-picklable ``RuntimeError``
        # demotion. ``KeyboardInterrupt``/``SystemExit`` still
        # propagate since they are ``BaseException``-only.
        cls_name = type(exc).__name__
        # Guard the f-string. ``__str__`` is user-overridable and
        # can raise (touches per-instance state, delegates to a
        # buggy ``__repr__``, etc.). The bare class name is a
        # string attribute lookup and cannot raise — last resort
        # so the safety net always succeeds.
        try:
            message = f"{cls_name}: {exc!s}"
        except Exception:
            message = cls_name
        return serializer.dumps(RuntimeError(message))


__all__ = [
    "AckResponseFrame",
    "ExceptionResponseFrame",
    "Frame",
    "NackResponseFrame",
    "NextRequestFrame",
    "RequestFrame",
    "ResponseFrame",
    "ResultResponseFrame",
    "SendRequestFrame",
    "TaskRequestFrame",
    "ThrowRequestFrame",
]
