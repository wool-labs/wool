from __future__ import annotations

import asyncio
import contextvars
import io
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

# Module name used by :func:`_infer_namespace` to distinguish the
# library's own frames from user frames during the stack walk.
_THIS_MODULE: Final = __name__


# public
class ContextVarCollision(Exception):
    """Raised when two distinct :class:`ContextVar` instances are
    constructed with the same ``(namespace, name)`` key.

    Keys must be unique within the inferred package namespace. Library
    authors SHOULD pass ``namespace=`` explicitly when constructing
    vars from shared factory code; application code can rely on the
    implicit package-name inference.
    """


class _UnsetType:
    """Picklable singleton representing a context variable with no value.

    Exposed via :attr:`Token.MISSING` to mirror the stdlib ``Token.MISSING``
    sentinel. Used as the internal default for the stdlib ``ContextVar``
    backing each :class:`ContextVar`, so that the "no value" state is
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
        return "<Token.MISSING>"

    def __bool__(self):
        return False


_UNSET: Final = _UnsetType()


def _lineage_id_now() -> UUID:
    """Return the current lineage UUID, lazily imported to avoid cycles."""
    from wool.runtime.worker.namespace import _current_lineage

    return _current_lineage()


def _infer_namespace() -> str:
    """Infer the namespace for a :class:`ContextVar` constructor call.

    Walks up the call stack from the current frame, skipping frames
    whose ``__name__`` matches this module, and returns the top-level
    package of the first user frame encountered. Falls back to
    ``"__main__"`` if the walk reaches the top of the stack.

    The walk (rather than a fixed ``_getframe(n)``) is resilient to
    decorators, subclass ``__init__`` chains, and intermediate helpers
    inside this module.
    """
    frame = sys._getframe(1)
    while frame is not None:
        mod = frame.f_globals.get("__name__", "")
        if mod and mod != _THIS_MODULE:
            return mod.partition(".")[0]
        frame = frame.f_back
    return "__main__"  # pragma: no cover — stack always has a caller


def _reconstruct(key: str, has_default: bool, default: Any) -> ContextVar:
    """Unpickle helper: get-or-create a :class:`ContextVar` by key.

    Normal flow: the worker has already imported the routine's module
    (via cloudpickle), which constructed the var at module scope and
    registered it. The lookup hits and returns the existing instance.

    Defensive fallback: if no instance is registered under *key*,
    :func:`_register_unchecked` builds and registers one. Uses the
    sallyport so the duplicate-key check in the public constructor
    doesn't fire here — the whole point of the unpickle path is to
    adopt the caller's identity on this process.
    """
    existing = ContextVar._registry.get(key)
    if existing is not None:
        return existing
    return _register_unchecked(key, has_default, default)


def _register_unchecked(key: str, has_default: bool, default: Any) -> ContextVar:
    """Sallyport: build and register a :class:`ContextVar` by key without
    running the public constructor's duplicate-key check.

    Intended exclusively for the unpickle path (:func:`_reconstruct`).
    The caller MUST verify the key is unregistered before calling — no
    internal check is done. Allocating via :func:`object.__new__`
    bypasses :meth:`ContextVar.__new__` entirely, so a key collision
    would silently overwrite rather than raise.

    The produced instance is flagged ``_stub=True`` so a later
    authoritative module-scope ``ContextVar(name, namespace=...)``
    call can promote it in place rather than collide.
    """
    ns, _, name = key.partition(":")
    instance: ContextVar = object.__new__(ContextVar)
    instance._name = name
    instance._namespace = ns
    instance._key = key
    instance._default = default if has_default else _SENTINEL
    instance._stdlib = contextvars.ContextVar(key, default=_UNSET)
    instance._stub = True
    with ContextVar._registry_lock:
        ContextVar._registry[key] = instance
        # Paired strong pin: registry is a WeakValueDictionary and the
        # sallyport has no other caller keeping the instance alive.
        # Cleared on promotion in :meth:`ContextVar.__new__`.
        ContextVar._stub_pins[key] = instance
    return instance


def _reconstruct_token(
    var: ContextVar,
    old_value: Any,
    lineage_id: UUID,
) -> Token:
    """Unpickle helper for :class:`Token`."""
    token = object.__new__(Token)
    token._var = var
    token._old_value = old_value
    token._stdlib_token = None
    token._used = False
    token._lineage_id = lineage_id
    return token


# public
class Token(Generic[T]):
    """Picklable token for reverting a :class:`ContextVar` mutation.

    Mirrors :class:`contextvars.Token`: single-use, same-var rejection,
    and (per wool) scoped to the lineage in which it was created.
    Attempting to :meth:`ContextVar.reset` with a token minted in a
    different wool lineage raises :class:`ValueError`.

    Within the same lineage, the wrapped stdlib token is used for
    :meth:`ContextVar.reset` when valid. In cross-process dispatch
    scenarios the stdlib token is invalid on the receiver; the token's
    captured :attr:`old_value` is restored directly as a fallback.
    """

    __slots__ = (
        "_var",
        "_old_value",
        "_stdlib_token",
        "_used",
        "_lineage_id",
    )

    # Class-level alias for the stdlib-compatible missing sentinel.
    MISSING: ClassVar[_UnsetType] = _UNSET

    _var: ContextVar[T]
    _old_value: T | _UnsetType
    _stdlib_token: contextvars.Token[T | _UnsetType] | None
    _used: bool
    _lineage_id: UUID

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
        self._lineage_id = _lineage_id_now()

    @property
    def var(self) -> ContextVar[T]:
        """The :class:`ContextVar` this token was created for."""
        return self._var

    @property
    def old_value(self) -> T | _UnsetType:
        """The prior value the var held before the :meth:`ContextVar.set`
        call that produced this token. Returns :attr:`Token.MISSING` if
        the var had no value set.
        """
        return self._old_value

    def __reduce__(self):
        return (_reconstruct_token, (self._var, self._old_value, self._lineage_id))

    def __repr__(self) -> str:
        used_marker = " used" if self._used else ""
        return f"<wool.Token var={self._var._key!r}{used_marker}>"


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
    of the calling frame (e.g., code in ``foo.bar.baz`` yields
    namespace ``"foo"``) or provided explicitly via the
    ``namespace=`` keyword. Two distinct instances constructed under
    the same key raise :class:`ContextVarCollision`.

    The key travels on the wire and identifies the var on every worker
    in the cluster.

    **Drop-in stdlib compatibility:** ``wool.ContextVar("foo")`` can
    be replaced with ``contextvars.ContextVar("foo")`` without code
    changes outside of imports.

    Values propagated across the wire MUST be cloudpicklable.
    Non-serializable values surface a :class:`TypeError` at dispatch
    time naming the offending variable.
    """

    __slots__ = (
        "_name",
        "_namespace",
        "_key",
        "_default",
        "_stdlib",
        "_stub",
        "__weakref__",
    )

    _registry: ClassVar[weakref.WeakValueDictionary[str, ContextVar[Any]]] = (
        weakref.WeakValueDictionary()
    )
    # Strong-ref companion to _registry for sallyport-created stubs.
    # Instances created via :func:`_register_unchecked` (the unpickle
    # path) have no user-visible owner — the weak _registry alone
    # can't keep them alive. They stay pinned here until a
    # module-scope ``ContextVar(...)`` promotes them (at which point
    # the pin is dropped and lifetime defers to the user's module
    # globals) or the process ends.
    _stub_pins: ClassVar[dict[str, ContextVar[Any]]] = {}
    # Serializes check-then-act on _registry / _stub_pins across the
    # __new__ promotion path and the _register_unchecked sallyport.
    # Concurrent importer and gRPC-handler threads can otherwise race
    # on the stub flag, torn-read _default, or register duplicate
    # stubs under the same key.
    _registry_lock: ClassVar[threading.RLock] = threading.RLock()

    _name: str
    _namespace: str
    _key: str
    _default: Any
    _stdlib: contextvars.ContextVar[T | _UnsetType]
    # True when this instance was registered via :func:`_register_unchecked`
    # (the unpickle sallyport) but has not yet been "promoted" by an
    # authoritative module-scope construction. Stubs behave identically
    # to real vars at runtime; the flag exists so a subsequent
    # module-body ``ContextVar(key, ...)`` call can promote the stub
    # rather than collide with it.
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
                    # Promote a sallyport-registered stub in place. The
                    # module-scope construction is the authoritative
                    # source for default; adopt it, clear the stub
                    # flag, and return the existing instance so
                    # reference identity (and any wire values already
                    # applied to ``_stdlib``) is preserved.
                    existing._default = default
                    existing._stub = False
                    # Drop the sallyport pin now that the user's module
                    # globals (or equivalent) will hold the strong ref.
                    cls._stub_pins.pop(key, None)
                    return existing
                raise ContextVarCollision(
                    f"wool.ContextVar {key!r} is already registered "
                    f"({existing!r}). Keys must be unique within a "
                    f"namespace."
                )
            return super().__new__(cls)

    @overload
    def __init__(self, name: str, /, *, namespace: str | None = None) -> None: ...

    @overload
    def __init__(
        self, name: str, /, *, namespace: str | None = None, default: T
    ) -> None: ...

    def __init__(
        self,
        name: str,
        /,
        *,
        namespace: str | None = None,
        default: Any = _SENTINEL,
    ):
        # Promotion path: __new__ returned an already-initialized stub
        # whose fields were updated in place. Skip re-init so we don't
        # clobber its ``_stdlib`` (which may already hold wire values).
        if getattr(self, "_key", None) is not None:
            return
        ns = namespace if namespace is not None else _infer_namespace()
        key = f"{ns}:{name}"
        self._name = name
        self._namespace = ns
        self._key = key
        self._default = default
        self._stdlib = contextvars.ContextVar(key, default=_UNSET)
        self._stub = False
        type(self)._registry[key] = self

    @property
    def name(self) -> str:
        """The variable's name (cosmetic, matching stdlib API)."""
        return self._name

    @property
    def namespace(self) -> str:
        """The namespace this var belongs to. Defaults to the top-level
        package of the construction site.
        """
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
        value = self._stdlib.get()
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
        old_value = self._stdlib.get()
        stdlib_token = self._stdlib.set(value)
        return Token(self, old_value, stdlib_token)

    def reset(self, token: Token[T]) -> None:
        """Restore the variable to the value it had before *token*.

        Matches :meth:`contextvars.ContextVar.reset` semantics, scoped
        to the wool lineage: the token must have been created in the
        same lineage as the one currently active, else
        :class:`ValueError`.

        Within a single lineage, the wrapped stdlib token is used for
        the reset when valid. Cross-stdlib-Context dispatch (same
        lineage on caller and worker, different stdlib Contexts) falls
        back to restoring the token's captured ``_old_value``.

        :param token:
            A token previously returned by :meth:`set`.
        :raises RuntimeError:
            If the token has already been used.
        :raises ValueError:
            If the token was created by a different
            :class:`ContextVar` or in a different wool lineage.
        """
        if token._used:
            raise RuntimeError("Token has already been used")
        if token._var is not self:
            raise ValueError("Token was created by a different ContextVar")
        if token._lineage_id != _lineage_id_now():
            raise ValueError("Token was created in a different wool.Context lineage")
        token._used = True
        if token._stdlib_token is not None:
            try:
                self._stdlib.reset(token._stdlib_token)
                return
            except ValueError:
                # Same lineage but different stdlib Context (e.g.,
                # across a dispatch boundary on the worker-side run).
                # Restore via _old_value.
                pass
        if isinstance(token._old_value, _UnsetType):
            # Note: explicitly setting _UNSET as a value leaves
            # ``var._stdlib in ctx`` reporting True for a var that is
            # semantically unset. Every reader in this module guards on
            # ``is _UNSET`` so the public API surface is consistent;
            # don't rely on ``in ctx`` as a proxy for "was set".
            self._stdlib.set(_UNSET)
        else:
            self._stdlib.set(token._old_value)

    def __repr__(self) -> str:
        default_part = (
            f" default={self._default!r}" if self._default is not _SENTINEL else ""
        )
        return f"<wool.ContextVar {self._key!r}{default_part} at 0x{id(self):x}>"


class _ContextPickler(cloudpickle.CloudPickler):
    """Cloudpickle pickler that reduces :class:`ContextVar` by key.

    Overrides :meth:`reducer_override` to emit every
    :class:`ContextVar` encountered in the object graph as a reduce
    tuple carrying its full ``"namespace:name"`` key plus default. On
    the receiver, :func:`_reconstruct` resolves the key to a local
    instance via the process-wide registry, or constructs a fresh
    instance under the same key if unknown.
    """

    def reducer_override(self, obj: Any) -> Any:
        if isinstance(obj, ContextVar):
            has_default = obj._default is not _SENTINEL
            return (
                _reconstruct,
                (
                    obj._key,
                    has_default,
                    obj._default if has_default else None,
                ),
            )
        return super().reducer_override(obj)


def _dumps(value: Any) -> bytes:
    """Serialize *value* with wool's ContextVar-aware pickler."""
    buf = io.BytesIO()
    _ContextPickler(buf).dump(value)
    return buf.getvalue()


def _loads(data: bytes) -> Any:
    """Deserialize *data* produced by :func:`_dumps`."""
    return cloudpickle.loads(data)


def _snapshot_vars(  # pragma: no cover
    ctx: contextvars.Context | None = None,
    *,
    dumps: Callable[[Any], bytes] = _dumps,
) -> dict[str, bytes]:
    """Snapshot all registered :class:`ContextVar` values to wire form.

    Iterates the process-wide registry, reads each var's current value
    (from *ctx* if supplied, else the current stdlib Context), and
    emits ``{"namespace:name": serialized_value}`` for every var with
    a set value.

    :param ctx:
        Optional :class:`contextvars.Context` to read from. When
        ``None``, reads from the current context.
    :param dumps:
        Serializer for values. Defaults to :func:`_dumps`.
    :raises TypeError:
        If a value fails to serialize.
    """
    # Covered by the integration dispatch suite, not by unit tests;
    # the whole function body is pragma'd on the def line above.
    result: dict[str, bytes] = {}
    for var in list(ContextVar._registry.values()):
        if ctx is not None:
            if var._stdlib not in ctx:
                continue
            raw = ctx[var._stdlib]
        else:
            raw = var._stdlib.get()
        if raw is _UNSET:
            continue
        try:
            pickled = dumps(raw)
        except Exception as e:
            raise TypeError(
                f"Failed to serialize wool.ContextVar {var._key!r}: {e}"
            ) from e
        result[var._key] = pickled
    return result


def _apply_vars(  # pragma: no cover
    vars: dict[str, bytes],
    *,
    loads: Callable[[bytes], Any] = _loads,
) -> None:
    """Apply a wire-form context snapshot to the current context.

    For each ``{"namespace:name": serialized_value}`` entry, looks up
    the key in the process-wide registry and calls
    :meth:`ContextVar.set` on the resolved instance. Keys unknown on
    this process (e.g., a caller-side var whose defining module isn't
    imported on the worker) log a warning and are skipped — the value
    has no in-scope var to read it through, so propagating it would be
    useless.

    :param vars:
        A ``{key: serialized_value}`` dict from the proto map.
    :param loads:
        Deserializer for values. Defaults to :func:`_loads`.
    """
    # Covered by the integration dispatch suite, not by unit tests;
    # the whole function body is pragma'd on the def line above.
    if not vars:
        return
    for key, data in vars.items():
        var = ContextVar._registry.get(key)
        if var is None:
            _log.warning(
                "wool.ContextVar %r not registered on this process; "
                "propagated value dropped",
                key,
            )
            continue
        var.set(loads(data))


def _reconstruct_context(
    id: UUID,
    vars: dict[ContextVar[Any], Any],
) -> Context:
    """Unpickle helper for :class:`Context`."""
    return Context._from_vars(id, vars)


# public
class Context:
    """Snapshot of wool.ContextVar state and lineage identity, scoped
    to a single task at a time.

    Mirrors :class:`contextvars.Context`: supports the stdlib container
    protocol (``__iter__``, ``__getitem__``, ``__contains__``,
    ``__len__``, ``keys``, ``values``, ``items``) and scopes mutations
    via :meth:`Context.run` / :meth:`Context.run_async`.

    A :class:`Context` formalizes the task lineage concept: beyond
    the snapshot of :class:`ContextVar` values, it carries the
    lineage ``UUID`` that identifies a logical execution chain.

    Instances can be constructed directly (``wool.Context()`` returns
    an empty Context with a fresh lineage UUID) or via
    :func:`current_context` (snapshot of the current ambient state).

    Instances are picklable. A :class:`Context` captured on the
    caller can ride the wire as a routine argument, be unpickled on
    a worker, and scope further dispatches there via :meth:`run` /
    :meth:`run_async`.

    **Single-task invariant:** at most one task may run inside a
    given :class:`Context` at a time. :meth:`run` / :meth:`run_async`
    raise :class:`RuntimeError` on re-entry. This makes
    bidirectional value propagation coherent under the
    transparent-dispatch model.
    """

    __slots__ = ("_id", "_vars", "_running")

    _id: UUID
    _vars: dict[ContextVar[Any], Any]
    _running: bool

    def __new__(cls) -> Context:
        instance: Context = object.__new__(cls)
        instance._id = uuid4()
        instance._vars = {}
        instance._running = False
        return instance

    @classmethod
    def _from_vars(
        cls,
        id: UUID,
        vars: dict[ContextVar[Any], Any],
    ) -> Context:
        """Private factory used by :func:`current_context` and unpickle."""
        instance: Context = object.__new__(cls)
        instance._id = id
        instance._vars = vars
        instance._running = False
        return instance

    @property
    def id(self) -> UUID:
        """The UUID that identifies this context's lineage."""
        return self._id

    def _enter(self) -> None:
        """Acquire the single-task flag or raise."""
        if self._running:
            raise RuntimeError(
                "wool.Context is already running; at most one task may run "
                "inside a given Context at a time"
            )
        self._running = True

    def _exit(self) -> None:
        """Release the single-task flag."""
        self._running = False

    def run(self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        """Run *fn* in this context (stdlib parity).

        Seeds a fresh stdlib :class:`contextvars.Context` with this
        wool context's var values and active lineage, then invokes
        *fn* inside it via :meth:`contextvars.Context.run`. Mutations
        made during the call flow back into this :class:`Context`'s
        :attr:`_vars` on exit so that back-propagation can read them.

        :raises RuntimeError:
            If this :class:`Context` is already running a task.
        """
        from wool.runtime.worker.namespace import _intended_lineage
        from wool.runtime.worker.namespace import adopt_lineage

        self._enter()
        seeded = contextvars.copy_context()

        def _seed_and_call() -> T:
            for var, value in self._vars.items():
                var._stdlib.set(value)
            # Bind the sentinel if we're in an asyncio task (fast path
            # for async-in-sync nesting) and always prime
            # _intended_lineage so that sync callers — where
            # adopt_lineage is a no-op — still resolve the lineage
            # inside the seeded stdlib Context.
            adopt_lineage(self._id)
            _intended_lineage.set(self._id)
            return fn(*args, **kwargs)

        try:
            result = seeded.run(_seed_and_call)
        finally:
            try:
                self._vars = _snapshot_from(seeded)
            except Exception:
                # Never let a snapshot failure mask the user's
                # exception (if any). Log and drop the partial update;
                # the captured _vars stays at its prior value.
                _log.exception("wool.Context.run: failed to capture post-run snapshot")
            finally:
                self._exit()
        return result

    async def run_async(self, coro: Coroutine[Any, Any, T], /) -> T:
        """Run *coro* in this context (async analog of :meth:`run`).

        Seeds a fresh stdlib :class:`contextvars.Context` with this
        wool context's var values and active lineage, then runs *coro*
        as an :class:`asyncio.Task` scoped to that seeded context.
        Mutations during the task flow back into this Context's
        :attr:`_vars` on completion.

        :raises RuntimeError:
            If this :class:`Context` is already running a task.
        """
        from wool.runtime.worker.namespace import _intended_lineage

        self._enter()
        seeded = contextvars.copy_context()

        def _seed() -> None:
            for var, value in self._vars.items():
                var._stdlib.set(value)
            _intended_lineage.set(self._id)

        seeded.run(_seed)
        try:
            result = await asyncio.create_task(coro, context=seeded)
        finally:
            try:
                self._vars = _snapshot_from(seeded)
            except Exception:
                _log.exception(
                    "wool.Context.run_async: failed to capture post-run snapshot"
                )
            finally:
                self._exit()
        return result

    def __iter__(self):
        return iter(self._vars)

    def __getitem__(self, var: ContextVar[T]) -> T:
        return self._vars[var]

    def __contains__(self, var: Any) -> bool:
        return var in self._vars

    def __len__(self) -> int:
        return len(self._vars)

    def keys(self):
        return self._vars.keys()

    def values(self):
        return self._vars.values()

    def items(self):
        return self._vars.items()

    def __repr__(self) -> str:
        return f"<wool.Context lineage={self._id} vars={len(self._vars)}>"

    def __reduce__(self):
        return (_reconstruct_context, (self._id, dict(self._vars)))


def _snapshot_from(ctx: contextvars.Context) -> dict[ContextVar[Any], Any]:
    """In-process snapshot of set :class:`ContextVar` values in *ctx*.

    Used by :meth:`Context.run` / :meth:`Context.run_async` to capture
    post-run mutations back onto the :class:`Context`. Different from
    :func:`_snapshot_vars` — this one produces a dict keyed by
    :class:`ContextVar` instances and does not serialize values.
    """
    out: dict[ContextVar[Any], Any] = {}
    for var in list(ContextVar._registry.values()):
        if var._stdlib not in ctx:
            continue
        raw = ctx[var._stdlib]
        # The ``is _UNSET`` branch is only reachable via the cross-Context
        # reset fallback, which is exercised by the integration suite.
        if raw is _UNSET:  # pragma: no cover
            continue
        out[var] = raw
    return out


# public
def current_context() -> Context:
    """Return a snapshot of the current wool context.

    Mirrors :func:`contextvars.copy_context` with the additional
    semantic that the returned :class:`Context` carries the lineage
    UUID identifying the current execution chain. The returned
    Context captures every :class:`ContextVar` that has an explicit
    value set in the current context (unset vars and vars sitting at
    their class-level default are omitted) plus the active lineage
    UUID. When no lineage is active, a fresh UUID is minted — the
    returned context starts a new lineage.
    """
    vars_dict: dict[ContextVar[Any], Any] = {}
    for var in list(ContextVar._registry.values()):
        raw = var._stdlib.get()
        if raw is _UNSET:
            continue
        vars_dict[var] = raw
    return Context._from_vars(_lineage_id_now(), vars_dict)


def build_frame_payload() -> tuple[dict[str, bytes], str]:  # pragma: no cover
    """Assemble the wire payload for a dispatch or streaming frame.

    Returns ``(vars, lineage_id)`` for populating the protobuf
    ``vars`` map and ``lineage_id`` string on any Request — the
    initial dispatch or a subsequent ``next``/``send``/``throw``.
    Every frame re-ships the current var snapshot plus the active
    lineage UUID so forward-propagated caller mutations reach the
    worker and the worker confirms the lineage on return.

    ``lineage_id`` is the active lineage UUID as a hex string. When
    no lineage has been adopted (root dispatch), it falls back to
    the process-default lineage.
    """
    vars_dict = _snapshot_vars()
    lineage_hex = _lineage_id_now().hex
    return vars_dict, lineage_hex


dispatch_timeout: Final[contextvars.ContextVar[float | None]] = contextvars.ContextVar(
    "dispatch_timeout", default=None
)
