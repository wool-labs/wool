"""Unified wire-frame abstraction for dispatch.

Provides the `Frame` hierarchy: deserialised views of the
`~wool.protocol.Request` / `~wool.protocol.Response` envelopes a
dispatch exchanges, with `Frame.from_protobuf` decoding an inbound
envelope to its frame and `Frame.mount` applying the decoded chain
state onto the active chain.

The hierarchy is three-tiered: the abstract `Frame` base, the
`RequestFrame` / `ResponseFrame` envelope intermediates, and one
concrete frame per protobuf payload oneof.
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
from typing import Self
from typing import TypeVar
from typing import cast
from typing import get_args

import wool
from wool import protocol
from wool.runtime.context.chain import Chain
from wool.runtime.context.exceptions import ChainSerializationError
from wool.runtime.context.exceptions import SerializationWarning
from wool.runtime.context.manifest import ChainManifest
from wool.runtime.routine.task import Task
from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

if TYPE_CHECKING:
    from wool.runtime.serializer import Serializer


T = TypeVar("T")


@dataclass
class Frame(Generic[T]):
    """Abstract base class for the frames a dispatch exchanges.

    A frame is the deserialised view of one
    `~wool.protocol.Request` or `~wool.protocol.Response`
    envelope: the application ``payload`` plus an optional
    `~wool.runtime.context.manifest.ChainManifest` — or the
    `~wool.runtime.context.exceptions.ChainSerializationError` raised
    when its wire context failed to decode.

    `mount` is the single "receive this frame" entry point,
    collapsing the receive paths into one; its docstring owns the
    caller-side / worker-side install and the exception-frame
    chaining contract.
    """

    payload: T

    _wire_type_name: ClassVar[str]
    _payload_field: ClassVar[str]

    chain_manifest: ChainManifest | ChainSerializationError | None = None

    _wire_chain_manifest: protocol.ChainManifest | None = field(
        default=None, init=False, repr=False
    )
    _serializer: Serializer | None = field(default=None, init=False, repr=False)
    _frame_by_field: ClassVar[dict[str, type[Frame]]] = {}
    _chain_exceptions: ClassVar[bool] = False

    def __init_subclass__(
        cls, *, field: str | None = None, wire_type: str | None = None, **kwargs: Any
    ) -> None:
        super().__init_subclass__(**kwargs)
        if wire_type is not None:
            cls._wire_type_name = wire_type
        # A frame whose payload is a BaseException chains a chain-manifest
        # decode error onto that payload rather than propagating (see
        # `mount`). Derive the flag from the bound payload type so it
        # cannot drift from the payload, and a new exception frame gets
        # the behaviour for free.
        for base in getattr(cls, "__orig_bases__", ()):
            args = get_args(base)
            if args and isinstance(args[0], type) and issubclass(args[0], BaseException):
                cls._chain_exceptions = True
                break
        # A concrete frame declares its protobuf payload-oneof name via
        # ``field=`` and is registered for dispatch. Subclasses that
        # pass no ``field`` — the abstract intermediates
        # (``RequestFrame`` / ``ResponseFrame``) — are not dispatch
        # targets and are skipped.
        if field is None:
            return
        cls._payload_field = field
        registered = Frame._frame_by_field.setdefault(field, cls)
        if registered is not cls:
            raise TypeError(
                f"{cls.__qualname__} claims payload field {field!r}, already "
                f"registered to {registered.__qualname__}; each concrete frame "
                f"must map to a distinct protobuf oneof field."
            )

    @classmethod
    def from_protobuf(
        cls,
        wire: protocol.Request | protocol.Response,
        *,
        serializer: Serializer | None = None,
    ) -> Frame:
        """Decode an incoming wire envelope into the matching frame.

        Matches ``wire.WhichOneof("payload")`` against the frame
        registry; the matched frame class's `_decode_payload`
        deserialises the per-variant sub-message, and the wire context
        decodes through `ChainManifest.from_protobuf`. A strict-mode
        decode failure is not raised here — it is captured as the
        `~wool.runtime.context.exceptions.ChainSerializationError` value
        of ``chain_manifest`` and deferred to `mount`, which raises it or
        chains it onto an exception payload.

        :raises ValueError:
            If the payload oneof is unset.
        """

        if serializer is None:
            serializer = wool.__serializer__
        field_name = wire.WhichOneof("payload")
        if field_name is None:
            raise ValueError("wire envelope has no payload set")
        # ``_frame_by_field`` is exhaustive over the protobuf payload
        # oneof, so a direct lookup is total; a missing key is an
        # invariant violation (registry/schema drift) and fails loudly
        # via ``KeyError`` rather than being a reachable input error.
        frame_cls = cls._frame_by_field[field_name]
        payload = frame_cls._decode_payload(wire, serializer=serializer)
        chain_manifest: ChainManifest | ChainSerializationError | None
        if wire.HasField("context"):
            try:
                chain_manifest = ChainManifest.from_protobuf(
                    wire.context, serializer=serializer
                )
            except ChainSerializationError as decode_err:
                # Defer the strict-mode failure: carry it as the manifest
                # value so `mount` raises it (or chains it onto an
                # exception payload) instead of preempting the payload here.
                chain_manifest = decode_err
        else:
            chain_manifest = None
        return frame_cls(payload=payload, chain_manifest=chain_manifest)

    def to_protobuf(self) -> protocol.Request | protocol.Response:
        """Encode this frame as its wire envelope.

        Resolves the wire envelope class via the intermediate's
        `_wire_type_name`, builds it with the frame's
        `_encode_payload` kwargs, then copies in
        `_wire_chain_manifest` if non-None.

        Lazy-wire-frame: the optional ``context`` field is omitted
        entirely when `_wire_chain_manifest` is ``None`` (the active
        chain was unarmed at `for_send` time).
        """

        serializer = self._serializer or wool.__serializer__
        wire_cls = getattr(protocol, self._wire_type_name)
        wire = wire_cls(**self._encode_payload(serializer=serializer))
        if self._wire_chain_manifest is not None:
            wire.context.CopyFrom(self._wire_chain_manifest)
        return wire

    @classmethod
    def _decode_payload(cls, wire: Any, *, serializer: Serializer) -> Any:
        """Deserialise this frame's payload sub-message from ``wire``.

        Override on each concrete frame. The default raises so an
        abstract / mistyped frame surfaces loudly at decode time.
        """
        raise NotImplementedError(f"{cls.__name__}._decode_payload must be overridden")

    def _encode_payload(self, *, serializer: Serializer) -> dict[str, Any]:
        """Return a kwargs dict for the wire-envelope constructor.

        The dict has exactly one key — the frame's `_payload_field`
        — mapped to the encoded sub-message. Override on each
        concrete frame.
        """
        raise NotImplementedError(
            f"{type(self).__name__}._encode_payload must be overridden"
        )

    @classmethod
    def for_send(
        cls,
        payload: T = None,  # type: ignore[assignment]
        *,
        serializer: Serializer | None = None,
    ) -> Self:
        """Build an outbound boundary frame ready for `to_protobuf`.

        Boundary frames carry no chain manifest; mid-stream frames mix in
        `_ChainManifestFrame` to capture or accept one.
        """
        if serializer is None:
            serializer = wool.__serializer__
        frame = cls(payload=payload, chain_manifest=None)
        frame._serializer = serializer
        return frame

    def mount(self, ctx: contextvars.Context | None = None) -> None:
        """Apply the frame's chain manifest onto the active chain.

        Gates in order: no-op when ``chain_manifest`` is None; raise the
        deferred `ChainSerializationError` first so an all-empty
        manifest carrying a strict-mode decode failure still
        surfaces; then short-circuit when the manifest has no
        observable state. Otherwise route through
        `~wool.runtime.context.chain.Chain.from_manifest`:

        * Caller-side mount — ``ctx`` is ``None`` (the default).
          The install runs in the current
          `contextvars.Context`, stamps the calling thread /
          task as owners, ensures Wool's task factory is on the
          loop, and merges with the active `Chain` when
          armed.
        * Worker-side mount — ``ctx`` is the chain's cached
          `contextvars.Context`. The install runs inside
          ``ctx.run(...)`` so backing writes land in the chain's
          context, with no task-owner stamp (the cached context is
          driven by a succession of step-tasks, so the chain is owned
          thread-wise rather than by any one task). The task factory is
          still ensured — a no-op on the already-equipped worker loop,
          but the site that surfaces a displaced factory; the per-step
          driver separately bypasses the factory when it constructs
          step tasks (see
          `~wool.runtime.worker.session._create_step_task`).

        Frames whose payload is an exception
        (`ThrowRequestFrame`, `NackResponseFrame`,
        `ExceptionResponseFrame`) carry `_chain_exceptions`. Any
        `ChainSerializationError` raised by the mount is appended
        onto `payload`'s ``__context__`` chain rather than
        propagating — the caller (or routine, in the throw case)
        still gets to handle the primary exception, with the decode
        failure surfaced as a chained cause.

        :param ctx:
            Optional `contextvars.Context` to install into.
            When provided, the install runs via ``ctx.run(...)``.
            Pass ``None`` (the default) for caller-side mounts
            running in the current context.

        :raises ChainSerializationError:
            When ``chain_manifest`` is a `ChainSerializationError` on a
            frame that does not chain onto its payload.
        """

        try:
            self._do_mount(ctx)
        except ChainSerializationError as decode_err:
            if not self._chain_exceptions:
                raise
            # The ``_chain_exceptions`` flag is only set on
            # frames whose payload is a BaseException
            # (ThrowRequestFrame, NackResponseFrame,
            # ExceptionResponseFrame); pyright can't narrow ``T``
            # through the ClassVar correlation.
            payload: BaseException = self.payload  # type: ignore[assignment]
            # Cycle guard on the ``__context__`` walk. An
            # adversarial / malformed payload whose ``__context__``
            # eventually points back into the chain would spin this
            # walk indefinitely; the visited-set short-circuits
            # the first time we'd revisit a node and attaches the
            # decode error there.
            node = payload
            seen: set[int] = {id(node)}
            while node.__context__ is not None and id(node.__context__) not in seen:
                node = node.__context__
                seen.add(id(node))
            node.__context__ = decode_err

    def _do_mount(self, ctx: contextvars.Context | None) -> None:
        """The actual manifest-apply logic, called from `mount`."""
        if self.chain_manifest is None:
            return
        if isinstance(self.chain_manifest, ChainSerializationError):
            # A deferred strict-mode wire-decode failure captured by
            # `from_protobuf`. Raising routes it through `mount`'s
            # raise-or-chain handler.
            raise self.chain_manifest
        # Short-circuit a manifest that decoded successfully but binds
        # no vars and records no resets — nothing to install.
        if not (self.chain_manifest.vars or self.chain_manifest.resets):
            return

        if ctx is None:
            # Caller-side: stamp the owning task, ensure the factory is
            # installed, merge with the current Chain (``None`` falls
            # through to the unarmed-receiver fresh-install path inside
            # `Chain.from_manifest`).
            Chain.from_manifest(
                self.chain_manifest,
                owned=True,
                merge_with=wool.__chain__.get(None),
            )
        else:
            # Worker-side: install inside the chain's cached
            # contextvars.Context. The cached context is driven by
            # successive step-tasks, so the chain is armed
            # task-agnostically (``owned=False``); the factory is still
            # ensured (a no-op on the worker loop, but the displacement
            # tripwire).
            ctx.run(
                Chain.from_manifest,
                self.chain_manifest,
                owned=False,
            )


@dataclass
class RequestFrame(Frame[T], wire_type="Request"):
    """Abstract intermediate for worker-bound dispatch frames.

    Each concrete request frame inherits this intermediate's
    ``_wire_type_name = "Request"`` and declares its own
    `_payload_field`.
    """

    def to_protobuf(self) -> protocol.Request:
        """Encode this request frame as a `protocol.Request`.

        Narrows `Frame.to_protobuf`'s ``Request | Response``
        union return to ``Request`` so call sites that already know
        they hold a ``RequestFrame`` (e.g., ``call.write(...)``) can
        consume the wire envelope without an extra ``cast`` /
        ``isinstance`` step.
        """
        return cast(protocol.Request, super().to_protobuf())


@dataclass
class ResponseFrame(Frame[T], wire_type="Response"):
    """Abstract intermediate for caller-bound dispatch frames.

    Each concrete response frame inherits this intermediate's
    ``_wire_type_name = "Response"`` and declares its own
    `_payload_field`.
    """

    def to_protobuf(self) -> protocol.Response:
        """Encode this response frame as a `protocol.Response`.

        Narrows `Frame.to_protobuf`'s ``Request | Response``
        union return to ``Response`` so async-generator yield sites
        (e.g., ``yield response.to_protobuf()`` into a stream typed
        ``AsyncGenerator[Response, ...]``) type-check without an
        extra ``cast`` / ``isinstance`` step.
        """
        return cast(protocol.Response, super().to_protobuf())


@dataclass
class _ChainManifestFrame(Frame[T]):
    """Mixin for mid-stream frames that carry a chain manifest.

    Extends `for_send` with `wire_chain_manifest`: auto-captures the active
    chain when omitted, or accepts an explicit chain manifest (``None``
    suppresses the field). The worker's result / terminal-exception
    paths pass a chain manifest captured inside the routine's cached
    `contextvars.Context`; auto-capturing on the worker's main loop
    would observe the wrong scope.
    """

    @classmethod
    def for_send(
        cls,
        payload: T = None,  # type: ignore[assignment]
        *,
        serializer: Serializer | None = None,
        wire_chain_manifest: protocol.ChainManifest | None | UndefinedType = Undefined,
    ) -> Self:
        """Build an outbound frame carrying the active (or given) chain manifest."""
        if serializer is None:
            serializer = wool.__serializer__
        frame = super().for_send(payload, serializer=serializer)
        if wire_chain_manifest is Undefined:
            try:
                wire_chain_manifest = (
                    wool.__chain__.get().to_manifest().to_protobuf(serializer=serializer)
                )
            except LookupError:
                wire_chain_manifest = None
        frame._wire_chain_manifest = wire_chain_manifest
        return frame


@dataclass
class TaskRequestFrame(RequestFrame[Task], field="task"):
    """Initial dispatch request carrying a `wool.Task`."""

    @classmethod
    def _decode_payload(cls, wire: protocol.Request, *, serializer: Serializer) -> Any:

        return Task.from_protobuf(wire.task)

    def _encode_payload(self, *, serializer: Serializer) -> dict[str, Any]:
        return {"task": self.payload.to_protobuf()}


@dataclass
class NextRequestFrame(RequestFrame[None], _ChainManifestFrame[None], field="next"):
    """Mid-stream pull request (no payload)."""

    @classmethod
    def _decode_payload(cls, wire: protocol.Request, *, serializer: Serializer) -> Any:
        return None

    def _encode_payload(self, *, serializer: Serializer) -> dict[str, Any]:

        return {"next": protocol.Void()}


@dataclass
class SendRequestFrame(RequestFrame[Any], _ChainManifestFrame[Any], field="send"):
    """Mid-stream ``asend`` request carrying an arbitrary payload."""

    @classmethod
    def _decode_payload(cls, wire: protocol.Request, *, serializer: Serializer) -> Any:
        return serializer.loads(wire.send.dump)

    def _encode_payload(self, *, serializer: Serializer) -> dict[str, Any]:

        return {"send": protocol.Message(dump=serializer.dumps(self.payload))}


@dataclass
class ThrowRequestFrame(
    RequestFrame[BaseException], _ChainManifestFrame[BaseException], field="throw"
):
    """Mid-stream ``athrow`` request carrying an exception.

    Its `BaseException` payload makes `mount` chain a chain-manifest
    decode error onto the throw payload's ``__context__`` rather than
    abandoning the throw entirely.
    """

    @classmethod
    def _decode_payload(cls, wire: protocol.Request, *, serializer: Serializer) -> Any:
        return serializer.loads(wire.throw.dump)

    def _encode_payload(self, *, serializer: Serializer) -> dict[str, Any]:

        return {"throw": protocol.Message(dump=serializer.dumps(self.payload))}


@dataclass
class AckResponseFrame(ResponseFrame[None], field="ack"):
    """Worker's protocol-version ack for the initial dispatch frame."""

    @classmethod
    def _decode_payload(cls, wire: protocol.Response, *, serializer: Serializer) -> Any:
        return None

    def _encode_payload(self, *, serializer: Serializer) -> dict[str, Any]:

        return {"ack": protocol.Ack(version=protocol.__version__)}


@dataclass
class NackResponseFrame(ResponseFrame[BaseException], field="nack"):
    """Dispatch-handler rejection response carrying an exception.

    Its `BaseException` payload makes `mount` chain a chain-manifest
    decode error onto the payload's ``__context__``.
    The payload is serialised via `_safely_serialize_exception`
    so an un-picklable exception instance degrades to a
    type-preserving fallback.
    """

    @classmethod
    def _decode_payload(cls, wire: protocol.Response, *, serializer: Serializer) -> Any:
        return serializer.loads(wire.nack.exception.dump)

    def _encode_payload(self, *, serializer: Serializer) -> dict[str, Any]:

        return {
            "nack": protocol.Nack(
                exception=protocol.Message(
                    dump=_safely_serialize_exception(serializer, self.payload),
                ),
            )
        }


@dataclass
class ResultResponseFrame(ResponseFrame[Any], _ChainManifestFrame[Any], field="result"):
    """Routine yield / return value response."""

    @classmethod
    def _decode_payload(cls, wire: protocol.Response, *, serializer: Serializer) -> Any:
        return serializer.loads(wire.result.dump)

    def _encode_payload(self, *, serializer: Serializer) -> dict[str, Any]:

        return {"result": protocol.Message(dump=serializer.dumps(self.payload))}


@dataclass
class ExceptionResponseFrame(
    ResponseFrame[BaseException], _ChainManifestFrame[BaseException], field="exception"
):
    """Routine-raised exception response.

    Its `BaseException` payload makes `mount` chain a chain-manifest
    decode error onto the payload's ``__context__``.
    The payload is serialised via `_safely_serialize_exception`
    so an un-picklable exception instance degrades to a
    type-preserving fallback.
    """

    @classmethod
    def _decode_payload(cls, wire: protocol.Response, *, serializer: Serializer) -> Any:
        return serializer.loads(wire.exception.dump)

    def _encode_payload(self, *, serializer: Serializer) -> dict[str, Any]:

        return {
            "exception": protocol.Message(
                dump=_safely_serialize_exception(serializer, self.payload),
            )
        }


def _safely_serialize_exception(
    serializer: Serializer,
    exc: BaseException,
) -> bytes:
    """Serialize ``exc``, preserving the exception class when the
    original instance carries un-picklable state.

    Prevents un-picklable exception state from converting a
    wool-class failure on the wire into a generic gRPC stream
    error on the caller side. The serializer is
    `CloudpickleSerializer`, which fails on un-picklable
    input via `pickle.PickleError`, `TypeError`
    for un-picklable C types, `AttributeError` for
    un-picklable local closures, or `RecursionError` for
    deeply self-referential graphs.

    **Type-preserving fallback.** Stdlib exception pickling
    round-trips ``(type, args, __dict__)``, dropping
    `__traceback__`, `__cause__`, `__context__`,
    and `__suppress_context__` — those four are not part of
    the exception's identity. `__notes__` and other
    ``__dict__`` attributes survive the round-trip; cloudpickle (with
    tblib) preserves `__cause__` so the dispatch handler's
    ``raise routine_exc from encode_err`` chaining for strict-mode
    chain-encode failures survives the wire. When a routine-level
    exception accumulates state that drags an un-picklable C-level
    object into the graph (e.g., a worker-thread frame on
    `__traceback__` reachable via `__cause__`), the
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
    stdlib `RuntimeError` carrying the original class
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
        # Walk the ``__cause__`` chain recursively, depth-bounded
        # and cycle-guarded. Reconstruct each level via the
        # ``cls(*args)`` shape so tracebacks (a frequent source of
        # cloudpickle failures) don't take the chain down with them.
        _MAX_CAUSE_DEPTH = 64
        seen_ids: set[int] = {id(exc)}
        head = clean
        original = exc.__cause__
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
                # Reconstruction failed at this level; stop walking.
                # The rest of the chain is lost — the fidelity warning
                # below reports the loss.
                break
        # Surface exception-fidelity loss via a typed warning. The
        # warning is also attached to the reconstructed exception's
        # ``__context__`` so the caller's diagnostic stack carries
        # the fidelity signal alongside the (degraded) exception
        # they actually catch.
        fidelity_warning = SerializationWarning(
            f"Exception {type(exc).__qualname__!r} required fallback "
            f"reconstruction; some side-channel state was lost.",
            cause=primary_err,
            original_type=type(exc),
        )
        try:
            warnings.warn(fidelity_warning, stacklevel=2)
        except SerializationWarning:
            # Strict-mode: the warning was promoted to an error. The
            # contract is that fidelity loss is non-fatal — even
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
        try:
            return serializer.dumps(clean)
        except Exception:
            # The attached fidelity warning's ``cause`` (the primary
            # pickle failure) — or a reconstructed ``__cause__`` level —
            # can itself carry the same un-picklable state that broke the
            # primary dumps (e.g., an env-dependent worker-thread
            # traceback). Strip the side-channel attachments and ship the
            # bare reconstructed exception so the caller still catches the
            # original type rather than a demoted ``RuntimeError``: the
            # class and args are picklable on their own — the primary
            # failure was the attachments, not the identity. This also
            # makes the shipped type deterministic across environments
            # where traceback picklability differs.
            clean.__cause__ = None
            clean.__context__ = None
            clean.__suppress_context__ = True
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
