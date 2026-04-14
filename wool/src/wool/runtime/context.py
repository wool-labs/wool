from __future__ import annotations

import asyncio
import contextvars
import importlib
import io
import logging
import pickle
import sys
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

import cloudpickle

T = TypeVar("T")

_SENTINEL: Final = object()

_log = logging.getLogger(__name__)


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


def _reconstruct_context_var(
    locations: list[tuple[str, str]],
) -> ContextVar:
    """Unpickle helper: resolve a ContextVar by any of its locations.

    Tries each ``(module, attr)`` location in order, importing the
    module if absent. Returns the first match — all locations of a
    single caller-side instance share one instance on the receiver
    when the module graph is consistent, so any successful lookup is
    correct. The fallback list exists so that if one alias has been
    added only on the caller (runtime rebinding) the other aliases
    still resolve on the worker.
    """
    errors: list[str] = []
    for module_name, attr_name in locations:
        mod = sys.modules.get(module_name)
        if mod is None:
            try:
                mod = importlib.import_module(module_name)
            except ImportError as e:
                errors.append(f"{module_name}: {e}")
                continue
        var = getattr(mod, attr_name, None)
        if isinstance(var, ContextVar):
            return var
        errors.append(f"{module_name}.{attr_name}: not a wool.ContextVar")
    raise RuntimeError(
        "Cannot resolve wool.ContextVar from any bound location: " + "; ".join(errors)
    )


def _reconstruct_token(var: ContextVar, old_value: Any) -> Token:
    """Unpickle helper for :class:`Token`."""
    token = object.__new__(Token)
    token._var = var
    token._old_value = old_value
    token._stdlib_token = None
    token._used = False
    return token


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
        # The _var field pickles by-reference (cloudpickle's default
        # for module-level bindings); _old_value pickles normally.
        # _stdlib_token is dropped — the receiver uses _old_value to
        # restore via var.set(_old_value).
        return (_reconstruct_token, (self._var, self._old_value))

    def __repr__(self) -> str:
        return f"<wool.Token var={self._var._name!r}>"


# public
class ContextVar(Generic[T]):
    """Propagating context variable that crosses worker boundaries.

    Mirrors :class:`contextvars.ContextVar` at the surface: construct
    with a name and optional default; call :meth:`get`, :meth:`set`,
    :meth:`reset`. Unlike the stdlib class, instances pickle by
    reference to their module-level binding and their values can be
    propagated across ``@wool.routine`` dispatches via the
    :class:`Context` machinery.

    Identity is module-attribute based: a :class:`ContextVar`
    constructed at ``some_lib.tracing.trace_id`` is referenced across
    the wire as ``("some_lib.tracing", "trace_id")``. The worker's
    local instance at the same location receives propagated values.
    Instances MUST therefore be assigned to module-level attributes
    (same constraint as :class:`contextvars.ContextVar`, which also
    only supports module-level bindings in practice).

    Two construction modes:

    - ``ContextVar(name)`` — no default; :meth:`get` raises
      :class:`LookupError` until a value is set.
    - ``ContextVar(name, *, default=...)`` — :meth:`get` returns the
      default when the variable has no value in the current context.

    Values propagated across the wire MUST be cloudpicklable.
    Non-serializable values surface a :class:`TypeError` at dispatch
    time naming the offending variable.
    """

    __slots__ = ("_name", "_default", "_stdlib", "_locations", "__weakref__")

    _registry: ClassVar[weakref.WeakSet[ContextVar[Any]]] = weakref.WeakSet()

    _name: str
    _default: Any
    _stdlib: contextvars.ContextVar[T | _UnsetType]
    # Cached list of every ``(module_name, attr_name)`` binding this
    # instance is reachable from. Populated lazily on first pickling
    # or first snapshot-binding discovery; re-validated on each call
    # and re-scanned if any cached entry has become stale.
    _locations: list[tuple[str, str]] | None

    @overload
    def __init__(self, name: str, /) -> None: ...

    @overload
    def __init__(self, name: str, /, *, default: T) -> None: ...

    def __init__(self, name: str, /, *, default: Any = _SENTINEL):
        self._name = name
        self._default = default
        self._stdlib = contextvars.ContextVar(name, default=_UNSET)
        self._locations = None
        type(self)._registry.add(self)

    def _resolve_locations(self) -> list[tuple[str, str]]:
        """Return every ``(module, attr)`` binding of this instance.

        Walks :data:`sys.modules` to find every module-level attribute
        that holds ``self`` — a ContextVar aliased under multiple
        names (``foo = bar = wool.ContextVar(...)`` or a
        ``from lib import trace as other`` re-export) produces one
        entry per binding. The list is cached on the instance;
        subsequent calls re-validate the cache and re-scan only if
        any cached entry has become stale.

        :returns:
            A list of ``(module_name, attr_name)`` tuples, or an empty
            list if the instance is not bound at module scope.
        """
        if self._locations is not None:
            still_valid = True
            for mod_name, attr_name in self._locations:
                mod = sys.modules.get(mod_name)
                if mod is None or getattr(mod, attr_name, None) is not self:
                    still_valid = False
                    break
            if still_valid:
                return self._locations
            self._locations = None
        found: list[tuple[str, str]] = []
        for mod_name, mod in list(sys.modules.items()):
            if mod is None:
                continue
            try:
                mod_dict = mod.__dict__
            except AttributeError:
                continue
            for attr_name, value in mod_dict.items():
                if value is self:
                    found.append((mod_name, attr_name))
        self._locations = found
        return found

    @property
    def name(self) -> str:
        """The variable's name (cosmetic, matching stdlib API)."""
        return self._name

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
            try:
                self._stdlib.reset(token._stdlib_token)
                return
            except ValueError:
                # Token was created in a different stdlib Context
                # (e.g., the user invoked a nested wool.routine that
                # runs in a copy-on-inherit context per
                # :func:`routine._execute`). Fall through to the
                # _old_value path, which restores the pre-set value
                # without consulting the stdlib Context identity.
                pass
        if isinstance(token._old_value, _UnsetType):
            self._stdlib.set(_UNSET)
        else:
            self._stdlib.set(token._old_value)

    def __repr__(self) -> str:
        return f"<wool.ContextVar name={self._name!r}>"


class _ContextPickler(cloudpickle.CloudPickler):
    """Cloudpickle pickler that reduces :class:`ContextVar` by location.

    Overrides :meth:`reducer_override` to emit every
    :class:`ContextVar` encountered in the object graph — whether the
    top-level value or nested in a closure, argument, or attribute —
    as a ``(module_name, attr_name)`` reference via
    :func:`_reconstruct_context_var`. This is the single authoritative
    entry point for wool ContextVar serialization; the class itself
    carries no ``__reduce__`` method.

    Instances MUST be used anywhere wool serializes user data that
    might transitively reference a :class:`ContextVar` (routine
    closures, args/kwargs, return values, thrown exceptions, var
    values). :func:`_dumps` is the canonical helper.
    """

    def reducer_override(self, obj: Any) -> Any:
        if isinstance(obj, ContextVar):
            locations = obj._resolve_locations()
            if not locations:
                raise pickle.PicklingError(
                    f"wool.ContextVar {obj._name!r} is not bound to any "
                    "module attribute; instances must be assigned at module "
                    "level to be picklable across worker boundaries"
                )
            return (_reconstruct_context_var, (locations,))
        # Delegate to cloudpickle's own reducer_override so local
        # functions, closures, and lambdas continue to pickle.
        return super().reducer_override(obj)


def _dumps(value: Any) -> bytes:
    """Serialize *value* with wool's ContextVar-aware pickler."""
    buf = io.BytesIO()
    _ContextPickler(buf).dump(value)
    return buf.getvalue()


def _loads(data: bytes) -> Any:
    """Deserialize *data* produced by :func:`_dumps`."""
    return cloudpickle.loads(data)


def _snapshot_vars(
    ctx: contextvars.Context | None = None,
    *,
    dumps: Callable[[Any], bytes] = _dumps,
) -> dict[str, bytes]:
    """Snapshot all registered :class:`ContextVar` values to wire form.

    :param ctx:
        Optional :class:`contextvars.Context` to read from. When
        ``None``, reads from the current context via
        :meth:`ContextVar.get`.
    :param dumps:
        Serializer for values. Defaults to :func:`_dumps`.
    :returns:
        A ``{"<module>:<attr>": serialized_value}`` dict ready for
        the proto map. Only vars with an explicit value (not
        ``_UNSET``) are included. Vars with no module binding are
        skipped with a log warning.
    :raises TypeError:
        If a value fails to serialize.
    """
    result: dict[str, bytes] = {}
    for var in list(ContextVar._registry):
        if ctx is not None:
            if var._stdlib not in ctx:
                continue
            raw = ctx[var._stdlib]
        else:
            raw = var._stdlib.get()
        if raw is _UNSET:
            continue
        locations = var._resolve_locations()
        if not locations:
            _log.warning(
                "wool.ContextVar %r is not bound to a module attribute; "
                "value dropped from snapshot",
                var._name,
            )
            continue
        try:
            pickled = dumps(raw)
        except Exception as e:
            raise TypeError(
                f"Failed to serialize wool.ContextVar {var._name!r}: {e}"
            ) from e
        # Emit one entry per binding so aliased locations on the
        # receiver (direct aliases, re-exports) each resolve. All
        # entries share the same pickled value; the receiver applies
        # them onto the same underlying instance.
        for mod_name, attr_name in locations:
            result[f"{mod_name}:{attr_name}"] = pickled
    return result


def _apply_vars(
    vars: dict[str, bytes],
    *,
    loads: Callable[[bytes], Any] = _loads,
) -> None:
    """Apply a wire-form context snapshot to the current context.

    For each entry ``"<module>:<attr>": value``, looks up the
    :class:`ContextVar` via ``getattr(sys.modules[module], attr)``
    and calls :meth:`ContextVar.set` on it. Unknown or non-ContextVar
    locations log-warn and are skipped.

    :param vars:
        A ``{"<module>:<attr>": serialized_value}`` dict from the
        proto map.
    :param loads:
        Deserializer for values. Defaults to :func:`_loads`.
    """
    if not vars:
        return
    for key, data in vars.items():
        mod_name, _, attr_name = key.partition(":")
        if not mod_name or not attr_name:
            _log.warning("Malformed wool.ContextVar key %r; skipping", key)
            continue
        mod = sys.modules.get(mod_name)
        if mod is None:
            _log.warning(
                "wool.ContextVar %r.%s not registered on this process; "
                "propagated value dropped",
                mod_name,
                attr_name,
            )
            continue
        var = getattr(mod, attr_name, None)
        if not isinstance(var, ContextVar):
            _log.warning(
                "wool.ContextVar %r.%s not a ContextVar on this process; "
                "propagated value dropped",
                mod_name,
                attr_name,
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
    """Immutable snapshot of wool.ContextVar state and lineage identity.

    Mirrors :class:`contextvars.Context`: not directly instantiable,
    immutable, supports the stdlib container protocol (``__iter__``,
    ``__getitem__``, ``__contains__``, ``__len__``, ``keys``,
    ``values``, ``items``), and scopes mutations via
    :meth:`Context.run` / :meth:`Context.run_async`.

    A :class:`Context` formalizes the task lineage concept: beyond
    the snapshot of :class:`ContextVar` values, it carries the
    lineage ``UUID`` that identifies a logical execution chain. Two
    dispatches scoped to the same :class:`Context` share the same
    lineage.

    Instances are picklable. A :class:`Context` captured on the
    caller (via :func:`current_context`) can ride the wire as a
    routine argument, be unpickled on a worker, and scope further
    dispatches there via :meth:`run` / :meth:`run_async`.

    **Single-task invariant:** at most one task may run inside a
    given :class:`Context` at a time. :meth:`run` / :meth:`run_async`
    raise :class:`RuntimeError` on re-entry. This makes
    bidirectional value propagation coherent under the
    transparent-dispatch model.

    Obtain a :class:`Context` via :func:`current_context`; direct
    instantiation raises :class:`TypeError` (stdlib parity).
    """

    __slots__ = ("_id", "_vars", "_running")

    _id: UUID
    _vars: dict[ContextVar[Any], Any]
    _running: bool

    def __new__(cls, *args: Any, **kwargs: Any) -> Context:
        raise TypeError(
            f"{cls.__name__} cannot be instantiated directly; use wool.current_context()"
        )

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
        made during the call — to wool or stdlib contextvars — are
        scoped to the seeded context and flow back into this
        :class:`Context`'s :attr:`_vars` on exit so that
        back-propagation can read them.

        :raises RuntimeError:
            If this :class:`Context` is already running a task
            (single-task invariant) or if the current event loop is
            running and *fn* returns an awaitable. Use
            :meth:`run_async` for coroutines.
        """
        from wool.runtime.worker.namespace import adopt_lineage

        self._enter()
        seeded = contextvars.copy_context()

        def _seed_and_call() -> T:
            for var, value in self._vars.items():
                var._stdlib.set(value)
            adopt_lineage(self._id)
            return fn(*args, **kwargs)

        try:
            result = seeded.run(_seed_and_call)
        finally:
            try:
                self._vars = _snapshot_from(seeded)
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
            If this :class:`Context` is already running a task
            (single-task invariant).
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
        # vars keys are ContextVar instances that pickle by module
        # reference; values pickle normally. Receiver reconstructs via
        # _from_vars and gets a fresh _running flag.
        return (_reconstruct_context, (self._id, dict(self._vars)))


def _snapshot_from(ctx: contextvars.Context) -> dict[ContextVar[Any], Any]:
    """In-process snapshot of set :class:`ContextVar` values in *ctx*.

    Used by :meth:`Context.run` / :meth:`Context.run_async` to capture
    post-run mutations back onto the :class:`Context`. Different from
    :func:`_snapshot_vars` — this one produces a dict keyed by
    :class:`ContextVar` instances (not wire keys), and does not
    serialize values.
    """
    out: dict[ContextVar[Any], Any] = {}
    for var in list(ContextVar._registry):
        if var._stdlib not in ctx:
            continue
        raw = ctx[var._stdlib]
        if raw is _UNSET:
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

    Lineage identity follows stdlib asyncio fork semantics: the same
    :class:`asyncio.Task` sees a stable lineage across sequential
    awaits; :func:`asyncio.create_task` / :func:`asyncio.gather`
    children mint fresh lineages on their first wool interaction.

    The snapshot is independent of the current context: subsequent
    mutations to wool vars do not affect the returned Context, and
    scoping a call via :meth:`Context.run` does not leak mutations
    back to the current context.
    """
    from wool.runtime.worker.namespace import _current_lineage

    vars_dict: dict[ContextVar[Any], Any] = {}
    for var in list(ContextVar._registry):
        raw = var._stdlib.get()
        if raw is _UNSET:
            continue
        vars_dict[var] = raw
    return Context._from_vars(_current_lineage(), vars_dict)


def build_task_frame_payload() -> tuple[dict[str, bytes], str]:
    """Assemble the wire payload for the initial Task dispatch frame.

    Returns ``(vars, lineage_id)`` for populating the protobuf
    ``vars`` map and ``lineage_id`` string on the first Request of a
    dispatch stream.

    ``lineage_id`` is the active lineage UUID as a hex string, or
    empty when no lineage is active (root dispatch — the worker
    assigns one).
    """
    from wool.runtime.worker.namespace import _current_lineage

    vars_dict = _snapshot_vars()
    lineage_hex = _current_lineage().hex
    return vars_dict, lineage_hex


def build_stream_frame_payload() -> tuple[dict[str, bytes], str]:
    """Assemble the wire payload for streaming frames (next/send/throw).

    Streaming frames re-ship the current var snapshot plus the
    lineage ID so back-propagated values reach the caller and the
    worker confirms the lineage.
    """
    from wool.runtime.worker.namespace import _current_lineage

    vars_dict = _snapshot_vars()
    lineage_hex = _current_lineage().hex
    return vars_dict, lineage_hex


dispatch_timeout: Final[contextvars.ContextVar[float | None]] = contextvars.ContextVar(
    "dispatch_timeout", default=None
)
