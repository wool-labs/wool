from __future__ import annotations

import contextvars
import logging
import threading
import weakref
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import ClassVar
from typing import Final
from typing import Generic
from typing import TypeVar
from typing import overload

import cloudpickle

from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

if TYPE_CHECKING:
    from wool import protocol

T = TypeVar("T")


class _SentinelType:
    """Singleton sentinel marking "no default provided" on a ContextVar.

    Distinct from :class:`_UnsetType`, which represents "no value set
    in the current context" on the internal stdlib ContextVar.
    Picklable by identity so classmethod ``__defaults__`` round-trip
    through cloudpickle without creating distinct sentinel instances.
    """

    _instance: _SentinelType | None = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __reduce__(self):
        return (_SentinelType, ())

    def __repr__(self):
        return "_SENTINEL"


_SENTINEL: Final = _SentinelType()


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


# public
class Token(Generic[T]):
    """Token for reverting a :class:`ContextVar` mutation.

    Mirrors :class:`contextvars.Token` semantics: single-use,
    same-var rejection, LIFO ordering enforced by the underlying
    stdlib token. Not picklable — :meth:`ContextVar.reset` must be
    called on the same process that minted the token.
    """

    __slots__ = ("_var", "_old_value", "_stdlib_token", "_used")

    _var: ContextVar[T]
    _old_value: T | _UnsetType
    _stdlib_token: contextvars.Token[T | _UnsetType]
    _used: bool

    def __init__(
        self,
        var: ContextVar[T],
        old_value: T | _UnsetType,
        stdlib_token: contextvars.Token[T | _UnsetType],
    ):
        self._var = var
        self._old_value = old_value
        self._stdlib_token = stdlib_token
        self._used = False

    def __reduce__(self):
        raise TypeError(
            "wool.Token is not picklable; reset must happen in the "
            "process that minted the token"
        )

    def __repr__(self) -> str:
        return f"<wool.Token var={self._var.name!r}>"


class _ContextVarMeta(type):
    """Metaclass enforcing singleton-by-name construction semantics.

    :class:`ContextVar` instances are cached by name in a
    :class:`weakref.WeakValueDictionary`. Public construction via
    ``ContextVar(name)`` raises :exc:`ValueError` on duplicate name;
    use :meth:`ContextVar.get_or_create` for idempotent lookup.

    The pickle reconstruction path uses
    :meth:`ContextVar._get_or_create`, which bypasses the raising
    ``__call__`` override by constructing via ``object.__new__``
    directly.
    """

    _cache: ClassVar[weakref.WeakValueDictionary[str, ContextVar]] = (
        weakref.WeakValueDictionary()
    )
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def __call__(cls, name: str, /, *, default: Any = _SENTINEL) -> ContextVar:
        # Fast path — lock-free dict lookup. dict.get is atomic under
        # the GIL, so a cache hit can return the existing instance
        # without acquiring the lock.
        existing = cls._cache.get(name)
        if existing is not None:
            if default != existing._default:
                raise ValueError(
                    f"wool.ContextVar {name!r} already exists with a "
                    f"different declaration: cached "
                    f"default={existing._default!r}, incoming "
                    f"default={default!r}. Both sides must agree on "
                    f"the var's declaration."
                )
            return existing
        with cls._lock:
            # Re-check under lock to close the race between two
            # concurrent first-time constructions of the same name.
            existing = cls._cache.get(name)
            if existing is not None:
                if default != existing._default:
                    raise ValueError(
                        f"wool.ContextVar {name!r} already exists "
                        f"with a different declaration: cached "
                        f"default={existing._default!r}, incoming "
                        f"default={default!r}. Both sides must agree "
                        f"on the var's declaration."
                    )
                return existing
            instance = super().__call__(name, default=default)
            cls._cache[name] = instance
            return instance


# public
class ContextVar(Generic[T], metaclass=_ContextVarMeta):
    """Propagating context variable that crosses worker boundaries.

    Mirrors :class:`contextvars.ContextVar` with the addition of
    automatic propagation across the dispatch chain. Instances are
    singletons by name within a process: constructing
    ``ContextVar("same_name")`` twice raises :exc:`ValueError`. Use
    :meth:`get_or_create` for idempotent lookup when names are
    generated dynamically.

    Two construction modes:

    - ``ContextVar(name)`` — no default; ``get()`` raises
      :class:`LookupError` until a value is set.
    - ``ContextVar(name, *, default=...)`` — ``get()`` returns the
      default when the variable has no value in the current context.

    The internal stdlib ``ContextVar`` always defaults to
    :data:`_UNSET` so that the "no value" state is representable as
    a real, picklable value.

    Values propagated across the wire MUST be cloudpicklable.
    Non-serializable values surface a :class:`TypeError` at dispatch
    time naming the offending variable.
    """

    _var: contextvars.ContextVar[T | _UnsetType]
    _default: Any  # T or _SENTINEL — wool-level default

    @overload
    def __init__(self, name: str, /) -> None: ...

    @overload
    def __init__(self, name: str, /, *, default: T) -> None: ...

    def __init__(self, name: str, /, *, default: Any = _SENTINEL):
        self._var = contextvars.ContextVar(name, default=_UNSET)
        self._default = default

    def __reduce__(self):
        # The stdlib contextvars.ContextVar is a C type that blocks
        # pickling outright; without this reducer, default pickling
        # of wool.ContextVar tries to pickle self._var and fails.
        # Routing through _get_or_create ensures the worker resolves
        # to the existing singleton (if any) or constructs a new one
        # under the same name.
        if self._default is _SENTINEL:
            return (type(self)._get_or_create, (self.name,))
        return (type(self)._get_or_create, (self.name, self._default))

    @classmethod
    def _lookup(cls, name: str) -> ContextVar | None:
        """Internal: return the cached instance for ``name``, or None.

        Used by :class:`_Context` to resolve wire-propagated var
        entries. Lock-free; ``dict.get`` is atomic under the GIL.
        """
        return _ContextVarMeta._cache.get(name)

    @classmethod
    def _get_or_create(
        cls,
        name: str,
        default: Any = _SENTINEL,
    ) -> ContextVar:
        """Internal: lookup-or-construct bypass for the unpickle path.

        Used by :meth:`__reduce__`. Raises :exc:`ValueError` when the
        incoming declaration disagrees with a cached instance's
        default (strict mode).
        """
        meta = _ContextVarMeta
        with meta._lock:
            existing = meta._cache.get(name)
            if existing is not None:
                if default != existing._default:
                    raise ValueError(
                        f"wool.ContextVar {name!r} already exists with "
                        f"a different declaration: cached "
                        f"default={existing._default!r}, incoming "
                        f"default={default!r}. Both sides must agree "
                        f"on the var's declaration."
                    )
                return existing
            # Bypass the metaclass's raising __call__ by constructing
            # via object.__new__ + __init__ directly.
            instance = object.__new__(cls)
            instance.__init__(name, default=default)
            meta._cache[name] = instance
            return instance

    @classmethod
    def get_or_create(
        cls,
        name: str,
        *,
        default: Any = _SENTINEL,
    ) -> ContextVar:
        """Public idempotent lookup-or-construct.

        Use when the var name is dynamic (factory, config-driven,
        parametrized test). Returns the cached instance if one
        exists with a matching ``default``, otherwise constructs a
        new one. Raises :exc:`ValueError` on default conflict.

        :param name:
            The variable's name.
        :param default:
            The class-level default. Must match an existing cached
            instance's default if one exists.
        :returns:
            The cached or newly-constructed :class:`ContextVar`.
        :raises ValueError:
            If a cached instance has a different default.
        """
        return cls._get_or_create(name, default=default)

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
            the previous value.
        """
        raw = self._var.get()
        old_value: T | _UnsetType = raw if raw is not _UNSET else _UNSET
        stdlib_token = self._var.set(value)
        return Token(self, old_value, stdlib_token)

    def reset(self, token: Token[T]) -> None:
        """Restore the variable to the value it had before ``token``.

        Matches :meth:`contextvars.ContextVar.reset` semantics: the
        var is restored to the value it had before the :meth:`set`
        call that produced ``token``. Out-of-order reset is not
        rejected — resetting an older token while a newer set is
        still active silently restores the pre-older-set state,
        discarding the newer mutation. This matches stdlib
        behavior.

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
        if token._var is not self:
            raise ValueError("Token was created by a different ContextVar")
        token._used = True
        self._var.reset(token._stdlib_token)

    def __repr__(self) -> str:
        return f"<wool.ContextVar name={self.name!r}>"


class _Context:
    """Internal serialization wrapper for context variable snapshots.

    Handles the conversion between the in-process
    :class:`ContextVar` cache and the ``map<string, bytes>`` protobuf
    representation used on the ``Request.vars`` and ``Response.vars``
    fields.
    """

    @staticmethod
    def snapshot(
        *,
        dumps: Callable[[Any], bytes] = cloudpickle.dumps,
    ) -> dict[str, bytes]:
        """Snapshot all cached :class:`ContextVar` values.

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
        for var in list(_ContextVarMeta._cache.values()):
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
        """Snapshot cached :class:`ContextVar` values from a specific context.

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
        for var in list(_ContextVarMeta._cache.values()):
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
        cached :class:`ContextVar` with the matching name. Names that
        don't resolve to a cached instance are logged and skipped.

        :param vars:
            A ``{name: serialized_value}`` dict from the proto map.
        :param loads:
            Deserializer for values. Defaults to
            :func:`cloudpickle.loads`.
        """
        if not vars:
            return
        for name, data in vars.items():
            var = ContextVar._lookup(name)
            if var is None:
                # Happens when the caller has set a var that the
                # target routine doesn't reference — the wire map
                # carries it, but the worker never saw the name
                # (routines only register vars they actually use).
                logging.warning(
                    "wool.ContextVar %r not registered on this process; "
                    "propagated value dropped",
                    name,
                )
                continue
            var.set(loads(data))


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
        Mapping of :class:`wool.ContextVar` names to the restored
        VALUES (any cloudpicklable object — not
        :class:`wool.ContextVar` instances themselves). The values
        are applied via ``var.set(value)`` on :meth:`__enter__`.
        Normally populated by :meth:`get_current` or
        :meth:`from_protobuf`; direct construction is intended for
        tests and internal use.
    """

    _dispatch_timeout: float | None | UndefinedType
    _dispatch_timeout_token: contextvars.Token[float | None] | UndefinedType
    # Maps ContextVar names to restored values (not ContextVar instances).
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

        for name, value in self._vars.items():
            var = ContextVar._lookup(name)
            if var is None:
                logging.warning(
                    "wool.ContextVar %r not registered on this worker; "
                    "propagated value dropped",
                    name,
                )
                continue
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
        for var in list(_ContextVarMeta._cache.values()):
            raw = var._var.get()
            if raw is not _UNSET:
                snapshot[var.name] = raw
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
