from __future__ import annotations

import asyncio
import logging
import sys
import threading
import weakref
from typing import Any
from typing import Callable
from typing import ClassVar
from typing import Coroutine
from typing import Final
from typing import Generic
from typing import TypeVar
from typing import overload
from uuid import UUID
from uuid import uuid4

import cloudpickle

T = TypeVar("T")

_SENTINEL: Final = object()

_log = logging.getLogger(__name__)

_THIS_MODULE: Final = __name__


# public
class ContextVarCollision(Exception):
    """Raised when two distinct :class:`ContextVar` instances are
    constructed with the same ``(namespace, name)`` key.

    Keys must be unique within the inferred package namespace. Library
    authors should pass ``namespace=`` explicitly when constructing
    vars from shared factory code; application code can rely on the
    implicit package-name inference.
    """


class _UnsetType:
    """Picklable singleton representing a context variable with no value.

    Exposed via :attr:`Token.MISSING` to mirror the stdlib
    ``Token.MISSING`` sentinel. Used internally to distinguish "var
    has no value in this Context" from "var is set to None."
    """

    _instance: _UnsetType | None = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __reduce__(self):
        return (_UnsetType, ())

    def __repr__(self):
        return "<Token.MISSING>"

    def __bool__(self):
        return False


_UNSET: Final = _UnsetType()


# ---------------------------------------------------------------
# Task-keyed context store
# ---------------------------------------------------------------

# Per-task wool.Context, keyed by the live asyncio.Task object.
# Entries vanish when the task is garbage-collected.
task_contexts: weakref.WeakKeyDictionary[asyncio.Task[Any], Context] = (
    weakref.WeakKeyDictionary()
)
task_contexts_lock: threading.Lock = threading.Lock()

# Sync/no-task fallback: one wool.Context per thread.
_thread_context: threading.local = threading.local()


def resolve_context() -> Context:
    """Return the wool.Context for the current execution scope.

    Inside an asyncio task, looks up the task's Context in the
    process-wide :data:`task_contexts` dict. Outside a task (sync
    code), uses a per-thread fallback. If no Context exists for the
    current scope, one is created lazily and registered.
    """
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    if task is not None:
        with task_contexts_lock:
            ctx = task_contexts.get(task)
            if ctx is not None:
                return ctx
            ctx = Context()
            task_contexts[task] = ctx
            return ctx
    ctx = getattr(_thread_context, "ctx", None)
    if ctx is None:
        ctx = Context()
        _thread_context.ctx = ctx
    return ctx


def swap_context(new: Context) -> Context:
    """Replace the current scope's Context and return the previous one.

    Used by :meth:`Context.run` and :func:`activate` to temporarily
    install a specific Context.
    """
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    if task is not None:
        with task_contexts_lock:
            prev = task_contexts.get(task)
            task_contexts[task] = new
        if prev is None:
            prev = Context()
        return prev
    prev = getattr(_thread_context, "ctx", None)
    _thread_context.ctx = new
    if prev is None:
        prev = Context()
    return prev


# ---------------------------------------------------------------
# Namespace inference
# ---------------------------------------------------------------


def _infer_namespace() -> str:
    """Infer the namespace for a :class:`ContextVar` constructor call.

    Walks up the call stack from the current frame, skipping frames
    whose ``__name__`` matches this module, and returns the top-level
    package of the first user frame encountered. Falls back to
    ``"__main__"`` if the walk reaches the top of the stack.
    """
    frame = sys._getframe(1)
    while frame is not None:
        mod = frame.f_globals.get("__name__", "")
        if mod and mod != _THIS_MODULE:
            return mod.partition(".")[0]
        frame = frame.f_back
    return "__main__"  # pragma: no cover — stack always has a caller


# ---------------------------------------------------------------
# Unpickle helpers
# ---------------------------------------------------------------


def reconstruct(
    key: str,
    has_default: bool,
    default: Any,
    current_value: Any = _UNSET,
) -> ContextVar:
    """Unpickle helper: get-or-create a :class:`ContextVar` by key.

    Normal flow: the worker has already imported the routine's module
    (via cloudpickle), which constructed the var at module scope and
    registered it. The lookup hits and returns the existing instance.

    Defensive fallback: if no instance is registered under *key*,
    :func:`_register_unchecked` builds and registers one.

    If *current_value* is not :data:`_UNSET`, the value is applied
    to the current wool.Context so the unpickle target carries the
    sender's state. Defaults to :data:`_UNSET` so callers
    reconstructing a var without a known value can omit the argument.
    """
    with ContextVar._registry_lock:
        existing = ContextVar._registry.get(key)
    if existing is None:
        existing = _register_unchecked(key, has_default, default)
    if current_value is not _UNSET:
        ctx = resolve_context()
        ctx._data[existing] = current_value
    return existing


def _register_unchecked(key: str, has_default: bool, default: Any) -> ContextVar:
    """Sallyport: build and register a :class:`ContextVar` by key
    without running the public constructor's duplicate-key check.

    Intended exclusively for the unpickle path
    (:func:`reconstruct`). The produced instance is flagged
    ``_stub=True`` so a later authoritative module-scope
    ``ContextVar(name, namespace=...)`` call can promote it in place.
    """
    ns, _, name = key.partition(":")
    instance: ContextVar = object.__new__(ContextVar)
    instance._name = name
    instance._namespace = ns
    instance._key = key
    instance._default = default if has_default else _SENTINEL
    instance._stub = True
    with ContextVar._registry_lock:
        ContextVar._registry[key] = instance
        ContextVar._stub_pins[key] = instance
    return instance


def reconstruct_token(
    var: ContextVar,
    old_value: Any,
    context_id: UUID,
) -> Token:
    """Unpickle helper for :class:`Token`."""
    token = object.__new__(Token)
    token._var = var
    token._old_value = old_value
    token._context = None
    token._context_id = context_id
    token._used = False
    return token


# ---------------------------------------------------------------
# Token
# ---------------------------------------------------------------


# public
class Token(Generic[T]):
    """Picklable token for reverting a :class:`ContextVar` mutation.

    Mirrors :class:`contextvars.Token`: single-use, same-var
    rejection, and scoped to the wool.Context in which it was
    created. Attempting to :meth:`ContextVar.reset` with a token
    minted in a different wool.Context raises :class:`ValueError`.

    In-process, the Context is checked by object identity.
    Cross-process (unpickled token), the Context's UUID is compared
    as a fallback.
    """

    __slots__ = (
        "_var",
        "_old_value",
        "_context",
        "_context_id",
        "_used",
    )

    MISSING: ClassVar[_UnsetType] = _UNSET

    _var: ContextVar[T]
    _old_value: T | _UnsetType
    _context: Context | None
    _context_id: UUID
    _used: bool

    def __init__(
        self,
        var: ContextVar[T],
        old_value: T | _UnsetType,
        context: Context,
    ):
        self._var = var
        self._old_value = old_value
        self._context = context
        self._context_id = context._id
        self._used = False

    @property
    def var(self) -> ContextVar[T]:
        """The :class:`ContextVar` this token was created for."""
        return self._var

    @property
    def old_value(self) -> T | _UnsetType:
        """The prior value the var held before the :meth:`ContextVar.set`
        call that produced this token. Returns :attr:`Token.MISSING`
        if the var had no value set.
        """
        return self._old_value

    def __reduce__(self):
        return (
            reconstruct_token,
            (self._var, self._old_value, self._context_id),
        )

    def __repr__(self) -> str:
        used_marker = " used" if self._used else ""
        return f"<wool.Token var={self._var._key!r}{used_marker}>"


# ---------------------------------------------------------------
# ContextVar
# ---------------------------------------------------------------


# public
class ContextVar(Generic[T]):
    """Propagating context variable that crosses worker boundaries.

    Mirrors :class:`contextvars.ContextVar` at the surface: construct
    with a name and optional default; call :meth:`get`, :meth:`set`,
    :meth:`reset`. Unlike the stdlib class, instances pickle across
    process boundaries and their values propagate through
    ``@wool.routine`` dispatches.

    **Identity model:** every :class:`ContextVar` has a unique
    ``(namespace, name)`` key. The ``name`` is the first positional
    argument; the ``namespace`` is inferred from the top-level package
    of the calling frame or provided explicitly via ``namespace=``.
    Two distinct instances constructed under the same key raise
    :class:`ContextVarCollision`.

    **Storage model:** values are stored in the current
    :class:`Context` (one per asyncio.Task, one per thread for sync
    code). A task factory seeds each new task with a copy of the
    parent's Context, providing stdlib-like copy-on-fork semantics.

    Values propagated across the wire MUST be cloudpicklable.
    """

    __slots__ = (
        "_name",
        "_namespace",
        "_key",
        "_default",
        "_stub",
        "__weakref__",
    )

    _registry: ClassVar[weakref.WeakValueDictionary[str, ContextVar[Any]]] = (
        weakref.WeakValueDictionary()
    )
    _stub_pins: ClassVar[dict[str, ContextVar[Any]]] = {}
    _registry_lock: ClassVar[threading.RLock] = threading.RLock()

    _name: str
    _namespace: str
    _key: str
    _default: Any
    _stub: bool

    def __new__(
        cls,
        name: str,
        /,
        *,
        namespace: str | None = None,
        default: Any = _SENTINEL,
    ) -> ContextVar[T]:
        ns = namespace if namespace is not None else _infer_namespace()
        key = f"{ns}:{name}"
        with cls._registry_lock:
            existing = cls._registry.get(key)
            if existing is not None:
                if existing._stub:
                    existing._default = default
                    existing._stub = False
                    cls._stub_pins.pop(key, None)
                    return existing
                raise ContextVarCollision(
                    f"wool.ContextVar {key!r} is already registered "
                    f"({existing!r}). Keys must be unique within a "
                    f"namespace."
                )
            instance = super().__new__(cls)
            instance._namespace = ns
            return instance

    @overload
    def __init__(self, name: str, /, *, namespace: str | None = None) -> None: ...

    @overload
    def __init__(
        self,
        name: str,
        /,
        *,
        namespace: str | None = None,
        default: T,
    ) -> None: ...

    def __init__(
        self,
        name: str,
        /,
        *,
        namespace: str | None = None,
        default: Any = _SENTINEL,
    ):
        if getattr(self, "_key", None) is not None:
            return
        ns = self._namespace
        key = f"{ns}:{name}"
        self._name = name
        self._key = key
        self._default = default
        self._stub = False
        type(self)._registry[key] = self

    @property
    def name(self) -> str:
        """The variable's name (cosmetic, matching stdlib API)."""
        return self._name

    @property
    def namespace(self) -> str:
        """The namespace this var belongs to."""
        return self._namespace

    @property
    def key(self) -> str:
        """The full ``"namespace:name"`` key that identifies this var
        on the wire.
        """
        return self._key

    @overload
    def get(self) -> T: ...

    @overload
    def get(self, default: T, /) -> T: ...

    def get(self, *args):
        """Return the current value in the active Context.

        :param default:
            Optional fallback returned when the variable has no value
            and no constructor default.
        :returns:
            The current value, the supplied fallback, or the
            constructor default.
        :raises LookupError:
            If the variable has no value, no fallback, and no default.
        """
        ctx = resolve_context()
        try:
            return ctx._data[self]
        except KeyError:
            if args:
                return args[0]
            if self._default is not _SENTINEL:
                return self._default
            raise LookupError(self)

    def set(self, value: T) -> Token[T]:
        """Set the variable's value in the active Context.

        :param value:
            The new value.
        :returns:
            A :class:`Token` usable with :meth:`reset` to restore
            the previous value.
        """
        ctx = resolve_context()
        old_value = ctx._data.get(self, _UNSET)
        ctx._data[self] = value
        return Token(self, old_value, ctx)

    def reset(self, token: Token[T]) -> None:
        """Restore the variable to the value it had before *token*.

        Matches :meth:`contextvars.ContextVar.reset` semantics,
        scoped to the wool.Context: the token must have been created
        in the same wool.Context as the one currently active.

        In-process, the Context is checked by object identity.
        Cross-process (unpickled tokens), the Context UUID is
        compared as a fallback.

        :param token:
            A token previously returned by :meth:`set`.
        :raises RuntimeError:
            If the token has already been used.
        :raises ValueError:
            If the token was created by a different
            :class:`ContextVar` or in a different wool.Context.
        """
        if token._used:
            raise RuntimeError("Token has already been used")
        if token._var is not self:
            raise ValueError("Token was created by a different ContextVar")
        ctx = resolve_context()
        if token._context is not None:
            if token._context is not ctx:
                raise ValueError("Token was created in a different wool.Context")
        elif token._context_id != ctx._id:
            raise ValueError("Token was created in a different wool.Context")
        token._used = True
        if isinstance(token._old_value, _UnsetType):
            ctx._data.pop(self, None)
        else:
            ctx._data[self] = token._old_value

    def __reduce__(self):
        has_default = self._default is not _SENTINEL
        ctx = resolve_context()
        current_value = ctx._data.get(self, _UNSET)
        return (
            reconstruct,
            (
                self._key,
                has_default,
                self._default if has_default else None,
                current_value,
            ),
        )

    def __repr__(self) -> str:
        default_part = (
            f" default={self._default!r}" if self._default is not _SENTINEL else ""
        )
        return f"<wool.ContextVar {self._key!r}{default_part} at 0x{id(self):x}>"


# ---------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------


def dumps(value: Any) -> bytes:
    """Serialize *value* via cloudpickle.

    wool.ContextVar instances in the object graph are reduced via
    their ``__reduce__`` method, which embeds the var's current
    value from the active Context.
    """
    return cloudpickle.dumps(value)


def loads(data: bytes) -> Any:
    """Deserialize *data* produced by :func:`dumps`.

    Thin wrapper retained as the default for :func:`apply_vars`'s
    ``loads`` parameter — a future self-dispatch passthrough can
    swap in a no-op loader without changing call sites.
    """
    return cloudpickle.loads(data)


# ---------------------------------------------------------------
# Wire-path helpers
# ---------------------------------------------------------------


def snapshot_vars(
    ctx: Context | None = None,
    *,
    dumps_param: Callable[..., bytes] | None = None,
) -> dict[str, bytes]:
    """Snapshot :class:`ContextVar` values to wire form.

    Iterates every var→value pair in the given (or current)
    :class:`Context` and serializes each value.

    :param ctx:
        Optional :class:`Context` to read from. When ``None``,
        reads from the current Context.
    :param dumps_param:
        Serializer for values. When ``None`` (default), uses
        :func:`dumps`. Pass a ``PassthroughSerializer.dumps`` on
        self-dispatch to avoid cloudpickle overhead.
    :raises TypeError:
        If a value fails to serialize.
    """
    if ctx is None:
        ctx = resolve_context()
    _ser = dumps_param if dumps_param is not None else dumps
    result: dict[str, bytes] = {}
    for var, value in ctx._data.items():
        try:
            pickled = _ser(value)
        except Exception as e:
            raise TypeError(
                f"Failed to serialize wool.ContextVar {var._key!r}: {e}"
            ) from e
        result[var._key] = pickled
    return result


def apply_vars(
    wire_vars: dict[str, bytes],
    *,
    loads: Callable[[bytes], Any] = loads,
) -> None:
    """Apply a wire-form context snapshot to the current Context.

    For each ``{"namespace:name": serialized_value}`` entry, looks
    up the key in the process-wide registry and writes the value
    directly into the current :class:`Context`'s data dict. Keys
    unknown on this process log at debug and are skipped.

    :param wire_vars:
        A ``{key: serialized_value}`` dict from the proto map.
    :param loads:
        Deserializer for values. Defaults to :func:`loads`.
    """
    if not wire_vars:
        return
    ctx = resolve_context()
    for key, data in wire_vars.items():
        with ContextVar._registry_lock:
            var = ContextVar._registry.get(key)
        if var is None:
            _log.debug(
                "wool.ContextVar %r not registered on this process; "
                "propagated value dropped",
                key,
            )
            continue
        ctx._data[var] = loads(data)


# ---------------------------------------------------------------
# Context unpickle
# ---------------------------------------------------------------


def reconstruct_context(
    id: UUID,
    vars: dict[ContextVar[Any], Any],
) -> Context:
    """Unpickle helper for :class:`Context`."""
    return Context._create(id, vars)


# ---------------------------------------------------------------
# Context
# ---------------------------------------------------------------


# public
class Context:
    """Snapshot of wool.ContextVar state and context id, scoped to a
    single task at a time.

    Mirrors :class:`contextvars.Context`: supports the stdlib
    container protocol (``__iter__``, ``__getitem__``,
    ``__contains__``, ``__len__``, ``keys``, ``values``, ``items``)
    and scopes mutations via :meth:`Context.run` /
    :meth:`Context.run_async`.

    Beyond the snapshot of :class:`ContextVar` values, a
    :class:`Context` carries a ``UUID`` id that identifies the
    logical execution chain it belongs to.

    **Storage model:** each :class:`Context` holds a ``_data`` dict
    mapping :class:`ContextVar` instances to their values. This is
    the live store — :meth:`ContextVar.get` / :meth:`ContextVar.set`
    read and write directly into the current task's Context.

    **Single-task invariant:** at most one task may run inside a
    given :class:`Context` at a time. :meth:`run` / :meth:`run_async`
    raise :class:`RuntimeError` on re-entry.
    """

    __slots__ = ("_id", "_data", "_running", "_running_lock")

    _id: UUID
    _data: dict[ContextVar[Any], Any]
    _running: bool
    _running_lock: threading.Lock

    def __new__(cls) -> Context:
        return cls._create(uuid4(), {})

    @classmethod
    def _create(
        cls,
        id: UUID,
        data: dict[ContextVar[Any], Any],
    ) -> Context:
        """Allocate and initialize a :class:`Context`."""
        instance: Context = object.__new__(cls)
        instance._id = id
        instance._data = data
        instance._running = False
        instance._running_lock = threading.Lock()
        return instance

    @property
    def id(self) -> UUID:
        """The UUID that identifies this Context's logical chain."""
        return self._id

    def copy(self, *, fork: bool = False) -> Context:
        """Return a shallow copy of this Context's data.

        :param fork:
            If ``True``, the copy gets a fresh UUID (new execution
            chain — used by the task factory for user-created tasks).
            If ``False`` (default), the copy preserves this Context's
            id (same chain — used by the worker dispatch path).
        """
        new_id = uuid4() if fork else self._id
        return Context._create(new_id, dict(self._data))

    def _enter(self) -> None:
        """Acquire the single-task flag or raise. Thread-safe."""
        with self._running_lock:
            if self._running:
                raise RuntimeError(
                    "wool.Context is already running; at most one "
                    "task may run inside a given Context at a time"
                )
            self._running = True

    def _exit(self) -> None:
        """Release the single-task flag. Thread-safe."""
        with self._running_lock:
            self._running = False

    def run(self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        """Run *fn* in this Context (stdlib parity).

        Installs this Context as the current scope's active Context,
        runs *fn*, then restores the previous Context. Mutations
        made by *fn* go directly into ``self._data``.

        :raises RuntimeError:
            If this :class:`Context` is already running a task.
        """
        self._enter()
        prev = swap_context(self)
        try:
            return fn(*args, **kwargs)
        finally:
            swap_context(prev)
            self._exit()

    async def run_async(self, coro: Coroutine[Any, Any, T], /) -> T:
        """Run *coro* in this Context (async analog of :meth:`run`).

        Creates an :class:`asyncio.Task` for *coro* and registers
        this Context as the task's active Context. Mutations during
        the task go directly into ``self._data``.

        :raises RuntimeError:
            If this :class:`Context` is already running a task.
        """
        self._enter()
        try:
            # Bypass the task factory to avoid a wasted fork
            # allocation that we'd immediately overwrite.
            loop = asyncio.get_running_loop()
            task = asyncio.Task(coro, loop=loop)
            with task_contexts_lock:
                task_contexts[task] = self
            result = await task
        finally:
            self._exit()
        return result

    def __iter__(self):
        return iter(self._data)

    def __getitem__(self, var: ContextVar[T]) -> T:
        return self._data[var]

    def __contains__(self, var: Any) -> bool:
        return var in self._data

    def __len__(self) -> int:
        return len(self._data)

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()

    def items(self):
        return self._data.items()

    def __repr__(self) -> str:
        return f"<wool.Context id={self._id} vars={len(self._data)}>"

    def __reduce__(self):
        return (reconstruct_context, (self._id, dict(self._data)))


# ---------------------------------------------------------------
# Public API
# ---------------------------------------------------------------


# public
def current_context() -> Context:
    """Return a snapshot of the current wool.Context.

    Mirrors :func:`contextvars.copy_context` — returns a new
    :class:`Context` whose ``_data`` is a shallow copy of the
    current scope's data and whose ``id`` matches the current
    execution chain.
    """
    ctx = resolve_context()
    return Context._create(ctx._id, dict(ctx._data))


def build_frame_payload(
    *,
    dumps_param: Callable[..., bytes] | None = None,
) -> tuple[dict[str, bytes], str]:
    """Assemble the wire payload for a dispatch or streaming frame.

    Returns ``(vars, context_id)`` for populating the protobuf
    ``vars`` map and ``context_id`` string on any Request.

    :param dumps_param:
        Optional serializer for var values. Pass
        ``PassthroughSerializer.dumps`` on self-dispatch to avoid
        cloudpickle overhead. When ``None``, uses cloudpickle.
    """
    ctx = resolve_context()
    vars_dict = snapshot_vars(ctx, dumps_param=dumps_param)
    context_hex = ctx._id.hex
    return vars_dict, context_hex


# ---------------------------------------------------------------
# Task factory
# ---------------------------------------------------------------


def _wool_task_factory(
    loop: asyncio.AbstractEventLoop,
    coro: Coroutine[Any, Any, Any],
    **kwargs: Any,
) -> asyncio.Task[Any]:
    """Task factory that seeds each new Task with a wool.Context.

    The parent's Context is found by looking up
    ``asyncio.current_task()`` in :data:`task_contexts`. The child
    gets a shallow copy with a fresh UUID (copy-on-fork). No stdlib
    ContextVar is needed — the factory runs synchronously inside
    ``create_task``, so ``current_task()`` is the parent.

    If no parent Context is registered (e.g., tasks created before
    wool is initialized), the child starts with an empty Context.
    """
    task: asyncio.Task[Any] = asyncio.Task(coro, loop=loop, **kwargs)
    try:
        parent_task = asyncio.current_task()
    except RuntimeError:
        parent_task = None
    with task_contexts_lock:
        parent_ctx = task_contexts.get(parent_task) if parent_task else None
        child_ctx = parent_ctx.copy(fork=True) if parent_ctx is not None else Context()
        task_contexts[task] = child_ctx
    return task


def install_task_factory(
    loop: asyncio.AbstractEventLoop | None = None,
) -> None:
    """Install wool's task factory on the given (or running) loop.

    Composes with an existing factory if one is set. Safe to call
    multiple times — subsequent calls are no-ops if the factory is
    already installed.
    """
    if loop is None:
        loop = asyncio.get_running_loop()
    existing = loop.get_task_factory()
    if existing is _wool_task_factory:
        return
    if existing is not None and getattr(existing, "_wool_wrapped", False):
        return
    if existing is not None:

        def composed(
            loop: asyncio.AbstractEventLoop,
            coro: Coroutine[Any, Any, Any],
            **kwargs: Any,
        ) -> asyncio.Task[Any]:
            task: asyncio.Task[Any] = existing(loop, coro, **kwargs)  # type: ignore[assignment]
            try:
                parent_task = asyncio.current_task()
            except RuntimeError:
                parent_task = None
            with task_contexts_lock:
                parent_ctx = task_contexts.get(parent_task) if parent_task else None
                child_ctx = (
                    parent_ctx.copy(fork=True) if parent_ctx is not None else Context()
                )
                task_contexts[task] = child_ctx
            return task

        composed._wool_wrapped = True  # type: ignore[attr-defined]
        loop.set_task_factory(composed)
    else:
        loop.set_task_factory(_wool_task_factory)
