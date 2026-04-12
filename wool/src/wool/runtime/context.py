from __future__ import annotations

import contextvars
import logging
import sys
import types
import weakref
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import ClassVar
from typing import Final
from typing import Generic
from typing import TypeVar
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
    var: ContextVar,
    old_value: Any,
) -> Token:
    """Unpickle helper for :class:`Token`."""
    tok = object.__new__(Token)
    tok._var = var
    tok._old_value = old_value
    tok._stdlib_token = None
    tok._used = False
    return tok


# public
class Token(Generic[T]):
    """Picklable token for reverting a :class:`ContextVar` mutation.

    Mirrors :class:`contextvars.Token` semantics: single-use,
    same-var rejection. On the local process the wrapped stdlib
    token is used for :meth:`ContextVar.reset`, giving the same
    guarantees as the stdlib. After deserialization the stdlib token
    is unavailable; :meth:`ContextVar.reset` falls back to restoring
    ``_old_value`` directly, matching the transparent-dispatch
    semantic where routines behave as if they were local coroutines.
    """

    __slots__ = ("_var", "_old_value", "_stdlib_token", "_used")

    _var: ContextVar[T]
    _old_value: T | _UnsetType
    _stdlib_token: contextvars.Token[T | _UnsetType] | None
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
        # The _var field is a ContextVar (ModuleType subclass), which
        # pickles via its own __reduce__. On the worker, _var
        # reconstructs to the same sys.modules entry as the library's
        # import-constructed instance — so token._var is self passes
        # for reset.
        return (_reconstruct_token, (self._var, self._old_value))

    def __repr__(self) -> str:
        return f"<wool.Token var={self._var.name!r}>"


def _reconstruct_context_var(
    synthetic_name: str,
    state: dict[str, Any],
) -> ContextVar:
    """Unpickle helper for :class:`ContextVar`.

    Checks ``sys.modules`` first for an existing instance under the
    synthetic name. If found, returns it — this is the unification
    path that makes pickle-reconstructed instances and library-
    import-constructed instances converge to the same Python object.
    If not found, constructs a fresh instance and registers it.
    """
    existing = sys.modules.get(synthetic_name)
    if existing is not None and isinstance(existing, ContextVar):
        return existing
    mod = ContextVar.__new__(ContextVar, state["_wool_name"])
    types.ModuleType.__init__(mod, synthetic_name)
    mod._wool_name = state["_wool_name"]
    mod._uuid = state["_uuid"]
    mod._default = state["_default"]
    mod._var = contextvars.ContextVar(state["_wool_name"], default=_UNSET)
    sys.modules[synthetic_name] = mod
    ContextVar._registry.add(mod)
    return mod


# public
class ContextVar(types.ModuleType, Generic[T]):
    """Propagating context variable that crosses worker boundaries.

    A :class:`types.ModuleType` subclass whose instances live in
    ``sys.modules`` under a deterministic key
    (``wool._vars.<name>``). This enables cross-process identity via
    ``sys.modules`` unification: both the pickle reconstructor and
    the library's import-time constructor produce the same key, so
    they converge on a single Python object.

    Mirrors :class:`contextvars.ContextVar` API: ``get``, ``set``,
    ``reset``, ``name``. Name is cosmetic — used for the stdlib
    backing var and for repr, matching the stdlib API. Identity is
    the ``sys.modules`` key.

    Two construction modes:

    - ``ContextVar(name)`` — no default; ``get()`` raises
      :class:`LookupError` until a value is set.
    - ``ContextVar(name, *, default=...)`` — ``get()`` returns the
      default when the variable has no value in the current context.

    Values propagated across the wire MUST be cloudpicklable.
    Non-serializable values surface a :class:`TypeError` at dispatch
    time naming the offending variable.
    """

    _registry: ClassVar[weakref.WeakSet[ContextVar[Any]]] = weakref.WeakSet()

    _wool_name: str
    _uuid: UUID
    _default: Any
    _var: contextvars.ContextVar[T | _UnsetType]

    def __new__(cls, name: str, /, *, default: Any = _SENTINEL):
        synthetic = f"wool._vars.{name}"
        existing = sys.modules.get(synthetic)
        if existing is not None and isinstance(existing, cls):
            return existing
        return super().__new__(cls, synthetic)

    @overload
    def __init__(self, name: str, /) -> None: ...

    @overload
    def __init__(self, name: str, /, *, default: T) -> None: ...

    def __init__(self, name: str, /, *, default: Any = _SENTINEL):
        if hasattr(self, "_uuid"):
            return  # already initialized (from __new__ unification)
        synthetic = f"wool._vars.{name}"
        super().__init__(synthetic)
        self._wool_name = name
        self._uuid = uuid4()
        self._default = default
        self._var = contextvars.ContextVar(name, default=_UNSET)
        sys.modules[synthetic] = self
        type(self)._registry.add(self)

    def __reduce__(self):
        # The stdlib contextvars.ContextVar is a C type that blocks
        # pickling. This reducer ships the wool-level state and
        # reconstructs via _reconstruct_context_var, which checks
        # sys.modules for an existing instance (unification with
        # the library's import-constructed version).
        state = {
            "_wool_name": self._wool_name,
            "_uuid": self._uuid,
            "_default": self._default,
        }
        return (_reconstruct_context_var, (self.__name__, state))

    @property
    def name(self) -> str:
        """The variable's name.

        :returns:
            The user-provided name (cosmetic, matching stdlib API).
        """
        return self._wool_name

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
            the previous value. The token is picklable and carries
            a reference to this ContextVar for cross-process use.
        """
        raw = self._var.get()
        old_value: T | _UnsetType = raw if raw is not _UNSET else _UNSET
        stdlib_token = self._var.set(value)
        return Token(self, old_value, stdlib_token)

    def reset(self, token: Token[T]) -> None:
        """Restore the variable to the value it had before ``token``.

        Matches :meth:`contextvars.ContextVar.reset` semantics. On
        the local process the stdlib token is used for the reset. On
        a remote process (after deserialization) the token's captured
        ``_old_value`` is restored directly, bypassing stdlib's
        context-scoping check — matching the transparent-dispatch
        semantic where routines behave as if they were local
        coroutines.

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
        if token._stdlib_token is not None:
            self._var.reset(token._stdlib_token)
        else:
            # Deserialized token — restore old value directly.
            if isinstance(token._old_value, _UnsetType):
                self._var.set(_UNSET)
            else:
                self._var.set(token._old_value)

    def __repr__(self) -> str:
        return f"<wool.ContextVar name={self._wool_name!r}>"


class _Context:
    """Internal serialization wrapper for context variable snapshots.

    Handles the conversion between the in-process
    :class:`ContextVar` registry and the ``map<string, bytes>``
    protobuf representation used on the ``Request.vars`` and
    ``Response.vars`` fields. The map key is the synthetic
    ``sys.modules`` name (``wool._vars.<name>``), which enables
    cross-process identity via ``sys.modules`` unification.
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
            A ``{synthetic_name: serialized_value}`` dict ready for
            the proto map. Only vars with an explicit value (not
            ``_UNSET``) are included.
        :raises TypeError:
            If a value fails to serialize.
        """
        result: dict[str, bytes] = {}
        for var in list(ContextVar._registry):
            raw = var._var.get()
            if raw is _UNSET:
                continue
            try:
                result[var.__name__] = dumps(raw)
            except Exception as e:
                raise TypeError(
                    f"Failed to serialize wool.ContextVar {var._wool_name!r}: {e}"
                ) from e
        return result

    @staticmethod
    def snapshot_from(
        ctx: contextvars.Context,
        *,
        dumps: Callable[[Any], bytes] = cloudpickle.dumps,
    ) -> dict[str, bytes]:
        """Snapshot registered :class:`ContextVar` values from a context.

        Like :meth:`snapshot` but reads from *ctx* instead of the
        current context. Used on the worker side to read the final
        state of wool vars after a routine completes inside a
        ``contextvars.Context`` created with ``create_task``.

        :param ctx:
            The :class:`contextvars.Context` to read from.
        :param dumps:
            Serializer for values.
        :returns:
            A ``{synthetic_name: serialized_value}`` dict.
        """
        result: dict[str, bytes] = {}
        for var in list(ContextVar._registry):
            if var._var in ctx:
                raw = ctx[var._var]
                if raw is _UNSET:
                    continue
                try:
                    result[var.__name__] = dumps(raw)
                except Exception as e:
                    raise TypeError(
                        f"Failed to serialize wool.ContextVar {var._wool_name!r}: {e}"
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
        :class:`ContextVar` found via ``sys.modules[synthetic_name]``.
        Unknown names are logged and skipped.

        :param vars:
            A ``{synthetic_name: serialized_value}`` dict from the
            proto map.
        :param loads:
            Deserializer for values. Defaults to
            :func:`cloudpickle.loads`.
        """
        if not vars:
            return
        for synthetic_name, data in vars.items():
            var = sys.modules.get(synthetic_name)
            if var is None or not isinstance(var, ContextVar):
                logging.warning(
                    "wool.ContextVar %r not registered on this process; "
                    "propagated value dropped",
                    synthetic_name,
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
            # Look up by synthetic name in sys.modules
            synthetic = f"wool._vars.{name}"
            var = sys.modules.get(synthetic)
            if var is None or not isinstance(var, ContextVar):
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
        for var in list(ContextVar._registry):
            raw = var._var.get()
            if raw is not _UNSET:
                snapshot[var._wool_name] = raw
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
