from __future__ import annotations

import contextvars
import logging
import weakref
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import ClassVar
from typing import Final
from typing import Generic
from typing import TypeVar
from typing import cast
from typing import overload
from uuid import UUID
from uuid import uuid4

import cloudpickle

from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

if TYPE_CHECKING:
    from wool import protocol

T = TypeVar("T")

_SENTINEL: Final = object()


class _UnsetType:
    """Picklable singleton representing a context variable with no value.

    Used as the internal default for the stdlib ``ContextVar`` backing
    each :class:`ContextVar`, so that the "no value" state is
    representable as a real value and survives cloudpickle roundtrips.
    """

    _instance: _UnsetType | None = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __reduce__(self):
        return (_UnsetType, ())

    def __repr__(self):
        return "UNSET"

    def __bool__(self):
        return False


_UNSET: Final = _UnsetType()


def _reconstruct_token(
    id: UUID,
    var_name: str,
    old_value: Any,
) -> Token:
    """Unpickle helper for :class:`Token`."""
    tok = object.__new__(Token)
    tok._id = id
    tok._var_name = var_name
    tok._old_value = old_value
    tok._stdlib_token = None
    tok._used = False
    return tok


# public
class Token(Generic[T]):
    """Picklable token for reverting a :class:`ContextVar` mutation.

    Mirrors :class:`contextvars.Token` with the addition of
    cross-process serializability. Each token carries a UUID so its
    identity survives pickling — two tokens created by separate
    ``set()`` calls are never equal, even across workers.

    On the local process the wrapped stdlib token is used for
    :meth:`ContextVar.reset`, giving the same guarantees as the stdlib
    (same-var, same-context, single-use). After deserialization the
    stdlib token is unavailable; :meth:`ContextVar.reset` falls back
    to restoring ``_old_value`` directly.
    """

    __slots__ = ("_id", "_var_name", "_old_value", "_stdlib_token", "_used")

    _id: UUID
    _var_name: str
    _old_value: T | _UnsetType
    _stdlib_token: contextvars.Token[T | _UnsetType] | None
    _used: bool

    def __init__(
        self,
        var_name: str,
        old_value: T | _UnsetType,
        stdlib_token: contextvars.Token[T | _UnsetType],
    ):
        self._id = uuid4()
        self._var_name = var_name
        self._old_value = old_value
        self._stdlib_token = stdlib_token
        self._used = False

    def __reduce__(self):
        return (_reconstruct_token, (self._id, self._var_name, self._old_value))

    def __repr__(self) -> str:
        return f"<wool.Token var={self._var_name!r} id={self._id}>"

    @property
    def var_name(self) -> str:
        return self._var_name


# public
class ContextVar(Generic[T]):
    """Propagating context variable that crosses worker boundaries.

    Mirrors :class:`contextvars.ContextVar` with the addition of
    automatic propagation across the dispatch chain. Each instance
    self-registers in a class-level :class:`weakref.WeakSet`.

    Two construction modes:

    - ``ContextVar(name)`` — no default; ``get()`` raises
      :class:`LookupError` until a value is set.
    - ``ContextVar(name, *, default=...)`` — ``get()`` returns the
      default when the variable has no value in the current context.

    The internal stdlib ``ContextVar`` always defaults to
    :data:`_UNSET` so that the "no value" state is representable as a
    real, picklable value — this enables :class:`Token`-based reset
    across serialization boundaries.

    Values propagated across the wire MUST be picklable (or
    serializable via the serializer supplied at dispatch time).
    Non-serializable values surface a :class:`TypeError` at dispatch
    time naming the offending variable.
    """

    _registry: ClassVar["weakref.WeakSet[ContextVar[Any]]"] = weakref.WeakSet()

    _var: contextvars.ContextVar[T | _UnsetType]
    _default: Any  # T or _SENTINEL — wool-level default

    @overload
    def __init__(self, name: str, /) -> None: ...

    @overload
    def __init__(self, name: str, /, *, default: T) -> None: ...

    def __init__(self, name: str, /, *, default=_SENTINEL):
        self._var = cast(
            "contextvars.ContextVar[T | _UnsetType]",
            contextvars.ContextVar(name, default=_UNSET),
        )
        self._default = default
        type(self)._registry.add(self)

    def __reduce__(self):
        """Reduce to (reconstructor, args) so cloudpickle can serialize.

        The underlying stdlib :class:`contextvars.ContextVar` cannot be
        pickled, so default object pickling fails. Reducing to a
        constructor call sidesteps that — the receiving process
        reconstructs a fresh var with the same name and default, which
        self-registers. :class:`_Context` resolves vars by name
        against the full registry, so both siblings receive the
        propagated value.
        """

        def _reconstruct(name, default=_SENTINEL):
            if default is _SENTINEL:
                return ContextVar(name)
            return ContextVar(name, default=default)

        if self._default is _SENTINEL:
            return (_reconstruct, (self.name,))
        return (_reconstruct, (self.name, self._default))

    @property
    def name(self) -> str:
        """The variable's name.

        :returns:
            The underlying :class:`contextvars.ContextVar` name.
        """
        return self._var.name

    @overload
    def get(self) -> T: ...

    @overload
    def get(self, default: T, /) -> T: ...

    def get(self, *args):
        """Return the current value in the active context.

        :param default:
            Optional fallback returned when the variable has no value
            and no class-level default.
        :returns:
            The current value, the supplied fallback, or the
            class-level default.
        :raises LookupError:
            If the variable has no value, no fallback, and no default.
        """
        value = self._var.get()
        if value is _UNSET:
            if args:
                return args[0]
            if self._default is not _SENTINEL:
                return self._default
            raise LookupError(self)
        return value

    def set(self, value: T) -> Token[T]:
        """Set the variable's value in the active context.

        :param value:
            The new value.
        :returns:
            A :class:`Token` usable with :meth:`reset` to restore
            the previous value. The token is picklable and carries a
            UUID identity for cross-process use.
        """
        raw = self._var.get()
        old_value: T | _UnsetType = cast("T", raw) if raw is not _UNSET else _UNSET
        stdlib_token = self._var.set(value)
        return Token(self.name, old_value, stdlib_token)

    def reset(self, token: Token[T]) -> None:
        """Restore the variable to the value it had before ``token``.

        :param token:
            A token previously returned by :meth:`set`.
        :raises RuntimeError:
            If the token has already been used.
        :raises ValueError:
            If the token was created by a different
            :class:`ContextVar`.
        """
        if token._used:
            raise RuntimeError("Token has already been used")
        if token._var_name != self.name:
            raise ValueError(
                f"Token was created by ContextVar {token._var_name!r}, not {self.name!r}"
            )
        token._used = True
        if token._stdlib_token is not None:
            self._var.reset(token._stdlib_token)
        else:
            # Deserialized token — restore old value directly
            if isinstance(token._old_value, _UnsetType):
                self._var.set(_UNSET)
            else:
                self._var.set(token._old_value)

    def __repr__(self) -> str:
        return f"<wool.ContextVar name={self.name!r}>"


class _Context:
    """Internal serialization wrapper for context variable snapshots.

    Handles the conversion between the in-process
    :class:`ContextVar` registry state and the ``map<string, bytes>``
    protobuf representation used on the ``Request.context`` and
    ``Response.context`` fields.
    """

    @staticmethod
    def snapshot(
        *,
        dumps: Callable[[Any], bytes] = cloudpickle.dumps,
    ) -> dict[str, bytes]:
        """Snapshot all registered :class:`ContextVar` values.

        :param dumps:
            Serializer for values. Defaults to
            :func:`cloudpickle.dumps`.
        :returns:
            A ``{name: serialized_value}`` dict ready for the proto
            map. Only vars with an explicit value (not ``_UNSET``) are
            included.
        :raises TypeError:
            If a value fails to serialize.
        """
        result: dict[str, bytes] = {}
        for var in list(ContextVar._registry):
            raw = var._var.get()
            if raw is _UNSET:
                continue
            try:
                result[var.name] = dumps(raw)
            except Exception as e:
                raise TypeError(
                    f"Failed to serialize wool.ContextVar {var.name!r}: {e}"
                ) from e
        return result

    @staticmethod
    def snapshot_from(
        ctx: contextvars.Context,
        *,
        dumps: Callable[[Any], bytes] = cloudpickle.dumps,
    ) -> dict[str, bytes]:
        """Snapshot registered :class:`ContextVar` values from a specific context.

        Like :meth:`snapshot` but reads from *ctx* instead of the
        current context. Used on the worker side to read the final
        state of wool vars after a routine completes inside a
        ``contextvars.Context`` created with ``create_task``.

        :param ctx:
            The :class:`contextvars.Context` to read from.
        :param dumps:
            Serializer for values.
        :returns:
            A ``{name: serialized_value}`` dict.
        """
        result: dict[str, bytes] = {}
        for var in list(ContextVar._registry):
            if var._var in ctx:
                raw = ctx[var._var]
                if raw is _UNSET:
                    continue
                try:
                    result[var.name] = dumps(raw)
                except Exception as e:
                    raise TypeError(
                        f"Failed to serialize wool.ContextVar {var.name!r}: {e}"
                    ) from e
        return result

    @staticmethod
    def apply(
        vars: dict[str, bytes],
        *,
        loads: Callable[[bytes], Any] = cloudpickle.loads,
    ) -> None:
        """Apply a context snapshot to the current context.

        Deserializes each entry and calls ``var.set(value)`` on the
        matching registered :class:`ContextVar`. Unknown names are
        logged and skipped.

        :param vars:
            A ``{name: serialized_value}`` dict from the proto map.
        :param loads:
            Deserializer for values. Defaults to
            :func:`cloudpickle.loads`.
        """
        if not vars:
            return
        by_name: dict[str, list[ContextVar[Any]]] = {}
        for var in list(ContextVar._registry):
            by_name.setdefault(var.name, []).append(var)
        for name, data in vars.items():
            matches = by_name.get(name)
            if not matches:
                logging.warning(
                    "wool.ContextVar %r not registered on this process; "
                    "propagated value dropped",
                    name,
                )
                continue
            value = loads(data)
            for var in matches:
                var.set(value)


dispatch_timeout: Final[contextvars.ContextVar[float | None]] = contextvars.ContextVar(
    "dispatch_timeout", default=None
)


# public — deprecated
class RuntimeContext:
    """Runtime context for configuring Wool behavior.

    .. deprecated::
        Use :class:`_Context` for bidirectional context propagation.
        ``RuntimeContext`` is retained for backwards compatibility with
        the ``Task.context`` protobuf field and will be removed in a
        future release.

    Provides context-managed configuration for dispatch timeout and
    propagated :class:`wool.ContextVar` values, allowing defaults to
    be set for all workers within a context and automatically shipped
    across worker boundaries at dispatch time.

    :param dispatch_timeout:
        Default timeout for task dispatch operations.
    :param vars:
        Mapping of :class:`wool.ContextVar` names to values to restore
        on :meth:`__enter__`. Normally populated by
        :meth:`get_current` or :meth:`from_protobuf`; direct
        construction is intended for tests and internal use.
    """

    _dispatch_timeout: float | None | UndefinedType
    _dispatch_timeout_token: contextvars.Token[float | None] | UndefinedType
    _vars: dict[str, Any]

    def __init__(
        self,
        *,
        dispatch_timeout: float | None | UndefinedType = Undefined,
        vars: dict[str, Any] | None = None,
    ):
        self._dispatch_timeout = dispatch_timeout
        self._vars = dict(vars) if vars else {}

    def __enter__(self):
        if self._dispatch_timeout is not Undefined:
            self._dispatch_timeout_token = dispatch_timeout.set(self._dispatch_timeout)
        else:
            self._dispatch_timeout_token = Undefined

        if self._vars:
            by_name: dict[str, list[ContextVar[Any]]] = {}
            for var in list(ContextVar._registry):
                by_name.setdefault(var.name, []).append(var)
            for name, value in self._vars.items():
                matches = by_name.get(name)
                if not matches:
                    logging.warning(
                        "wool.ContextVar %r not registered on this worker; "
                        "propagated value dropped",
                        name,
                    )
                    continue
                for var in matches:
                    var.set(value)

        return self

    def __exit__(self, *_):
        if self._dispatch_timeout_token is not Undefined:
            dispatch_timeout.reset(self._dispatch_timeout_token)

    @classmethod
    def get_current(cls) -> RuntimeContext:
        """Get the current runtime context.

        Returns a :class:`RuntimeContext` that captures the current
        ``dispatch_timeout`` and snapshots any registered
        :class:`wool.ContextVar` values that have been explicitly set
        in the current context. Variables at their class-level default
        are not snapshotted — only explicit ``set`` calls propagate.

        :returns:
            A :class:`RuntimeContext` with current context values.
        """
        snapshot: dict[str, Any] = {}
        for wool_var in list(ContextVar._registry):
            raw = wool_var._var.get()
            if raw is not _UNSET:
                snapshot[wool_var.name] = raw
        return cls(
            dispatch_timeout=dispatch_timeout.get(),
            vars=snapshot,
        )

    def to_protobuf(
        self,
        *,
        dumps: Callable[[Any], bytes] = cloudpickle.dumps,
    ) -> protocol.RuntimeContext:
        """Serialize the wire-safe subset of this context to protobuf.

        ``dispatch_timeout`` and any snapshotted
        :class:`wool.ContextVar` values are propagated over the wire.

        :param dumps:
            Callable used to serialize propagated context variable
            values. Defaults to :func:`cloudpickle.dumps`. Callers
            (e.g. :meth:`Task.to_protobuf`) may pass a custom
            serializer so propagated values honor the same serialization
            strategy as the task payload.
        :returns:
            A protobuf ``RuntimeContext`` message.
        :raises TypeError:
            If a propagated value fails to serialize; the offending
            variable name is included in the error message.
        """
        from wool import protocol

        dt = self._dispatch_timeout
        if dt is Undefined:
            dt = dispatch_timeout.get()
        pb = protocol.RuntimeContext()
        if dt is not None:
            pb.dispatch_timeout = dt
        for name, value in self._vars.items():
            try:
                pb.vars[name] = dumps(value)
            except Exception as e:
                raise TypeError(
                    f"Failed to serialize wool.ContextVar {name!r}: {e}"
                ) from e
        return pb

    @classmethod
    def from_protobuf(
        cls,
        context: protocol.RuntimeContext,
        *,
        loads: Callable[[bytes], Any] = cloudpickle.loads,
    ) -> RuntimeContext:
        """Reconstruct a :class:`RuntimeContext` from a protobuf message.

        No runtime import of ``protocol`` is needed here — the message
        is received as a parameter rather than constructed.

        :param context:
            A protobuf ``RuntimeContext`` message.
        :param loads:
            Callable used to deserialize propagated context variable
            values. Defaults to :func:`cloudpickle.loads`. Callers
            (e.g. :meth:`Task.from_protobuf`) may pass a custom
            deserializer matching the one used at
            :meth:`to_protobuf`.
        :returns:
            A :class:`RuntimeContext` instance.
        """
        return cls(
            dispatch_timeout=context.dispatch_timeout
            if context.HasField("dispatch_timeout")
            else None,
            vars={name: loads(data) for name, data in context.vars.items()},
        )
