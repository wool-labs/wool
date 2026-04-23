from __future__ import annotations

import asyncio
import threading
import weakref
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Final
from typing import NoReturn
from typing import SupportsIndex
from uuid import UUID

from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

if TYPE_CHECKING:
    from wool.runtime.context.base import Context
    from wool.runtime.context.token import Token
    from wool.runtime.context.var import ContextVar


lock: Final[threading.Lock] = threading.Lock()


var_registry: Final[weakref.WeakValueDictionary[tuple[str, str], ContextVar[Any]]] = (
    weakref.WeakValueDictionary()
)


class _TokenRegistry(weakref.WeakValueDictionary[UUID, "Token[Any]"]):
    """Process-wide registry of live :class:`Token` instances that
    pickles by module-attribute reference under Wool's pickler.

    Plain :class:`weakref.WeakValueDictionary` is unpicklable
    (weakrefs reject pickle), and cloudpickle serializes bound
    classmethods like :meth:`Token._reconstitute` by walking the
    function's globals and capturing each name by value. Without
    this override the by-value walk crashes on the registry; with
    it, the registry reduces to a zero-arg lookup of this module's
    :data:`token_registry` attribute, keeping the actual contents
    process-local.

    The reduction is exposed only to Wool's pickler via
    :meth:`__wool_reduce__`; vanilla :func:`pickle.dumps` and
    :func:`cloudpickle.dumps` are rejected by :meth:`__reduce_ex__`
    so the registry never silently leaves the dispatch path.
    """

    def __wool_reduce__(self) -> tuple[Callable[..., _TokenRegistry], tuple[()]]:
        return (_resolve_token_registry, ())

    def __reduce_ex__(self, _protocol: SupportsIndex) -> NoReturn:
        raise TypeError(
            "_TokenRegistry cannot be pickled via vanilla pickle/cloudpickle; "
            "it is serialized automatically when dispatched through Wool's "
            "runtime."
        )


# Process-wide weak registry of live :class:`Token` instances keyed
# by ID. Preserves pickle identity within a process and resolves
# incoming wire IDs to live tokens so their ``_used`` flag can be
# flipped on merge. Weak values auto-prune when a token is GC'd, so
# transient tokens from a ``set``/``reset`` loop do not accumulate.
token_registry: Final[_TokenRegistry] = _TokenRegistry()


def _resolve_token_registry() -> _TokenRegistry:
    """Return the process-wide :class:`Token` registry.

    Module-level shim used by :meth:`_TokenRegistry.__wool_reduce__`
    so cloudpickle's lookup-and-qualname path can pickle the registry
    reference by name instead of by value. MUST stay at module level;
    cloudpickle's by-reference lookup requires a stable qualname.
    """
    return token_registry


class _ContextToken:
    """Restore cookie returned by :meth:`_ContextRegistry.set` and
    consumed by :meth:`_ContextRegistry.reset` to undo a
    :class:`Context` installation.

    Mirrors :class:`contextvars.Token` in spirit: opaque to the
    caller, carrying enough state to return the registry slot to
    exactly where it was (including the "was-unset" case, which
    :meth:`_ContextRegistry.reset` pops rather than rewriting to
    ``None``).
    """

    __slots__ = ("_key", "_previous", "_used")

    _key: asyncio.Task[Any] | threading.Thread
    _previous: Context | None
    _used: bool

    def __init__(
        self,
        key: asyncio.Task[Any] | threading.Thread,
        previous: Context | None,
    ) -> None:
        self._key = key
        self._previous = previous
        self._used = False


class _ContextRegistry(
    weakref.WeakKeyDictionary["asyncio.Task[Any] | threading.Thread", "Context"]
):
    """WeakKey registry of per-scope :class:`Context` bindings.

    Implements the standard :class:`MutableMapping` protocol —
    ``__getitem__`` raises :class:`KeyError` on miss (no auto-create).
    Overrides :meth:`get` and :meth:`setdefault` to default the *key*
    argument to :func:`scope_key` when omitted (or passed as
    :data:`Undefined`), so callers asking about "the current scope"
    do not have to spell out the lookup. Adds two wool-specific
    methods: :meth:`set` returns a :class:`_ContextToken` that
    captures the previous slot state for later :meth:`reset`
    restoration.

    Pickles by module-attribute reference under Wool's pickler.
    Plain :class:`weakref.WeakKeyDictionary` is unpicklable
    (weakrefs reject pickle), and cloudpickle serializes bound
    classmethods like :meth:`Token._reconstitute` by walking the
    function's globals and capturing each name by value. Without
    the reduce hooks the by-value walk crashes on the registry; with
    them, the registry reduces to a zero-arg lookup of this module's
    :data:`context_registry` attribute, keeping the actual contents
    process-local.
    """

    def get(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        key: asyncio.Task[Any] | threading.Thread | UndefinedType = Undefined,
        default: Context | None = None,
    ) -> Context | None:
        if key is Undefined:
            key = scope_key()
        return super().get(key, default)  # pyright: ignore[reportArgumentType]

    def setdefault(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        key: asyncio.Task[Any] | threading.Thread | UndefinedType = Undefined,
        default: Context | None = None,
    ) -> Context | None:
        if key is Undefined:
            key = scope_key()
        return super().setdefault(key, default)  # pyright: ignore[reportArgumentType]

    def set(self, ctx: Context) -> _ContextToken:
        with lock:
            key = scope_key()
            previous = self.get(key)
            self[key] = ctx
        return _ContextToken(key, previous)

    def reset(self, token: _ContextToken) -> None:
        if token._used:
            raise RuntimeError("token already consumed by reset")
        token._used = True
        with lock:
            if token._previous is None:
                self.pop(token._key, None)
            else:
                self[token._key] = token._previous

    def __wool_reduce__(self) -> tuple[Callable[..., _ContextRegistry], tuple[()]]:
        return (_resolve_context_registry, ())

    def __reduce_ex__(self, _protocol: SupportsIndex) -> NoReturn:
        raise TypeError(
            "_ContextRegistry cannot be pickled via vanilla pickle/cloudpickle; "
            "it is serialized automatically when dispatched through Wool's "
            "runtime."
        )


context_registry: Final[_ContextRegistry] = _ContextRegistry()


def _resolve_context_registry() -> _ContextRegistry:
    """Return the process-wide :class:`Context` registry.

    Module-level shim used by :meth:`_ContextRegistry.__wool_reduce__`
    so cloudpickle's lookup-and-qualname path can pickle the registry
    reference by name instead of by value. MUST stay at module level;
    cloudpickle's by-reference lookup requires a stable qualname.
    """
    return context_registry


def scope_key() -> asyncio.Task[Any] | threading.Thread:
    """Identify the current execution scope (asyncio task, or
    thread for sync callers).
    """
    try:
        return asyncio.current_task() or threading.current_thread()
    except RuntimeError:
        return threading.current_thread()
