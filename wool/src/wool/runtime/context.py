from __future__ import annotations

import asyncio
import contextvars
import importlib
import io
import logging
import operator
import sys
import weakref
from typing import Any
from typing import Callable
from typing import ClassVar
from typing import Coroutine
from typing import Final
from typing import Generic
from typing import Iterator
from typing import TypeVar
from typing import overload
from uuid import UUID
from uuid import uuid4
from uuid import uuid5

import cloudpickle

T = TypeVar("T")

_SENTINEL: Final = object()

_log = logging.getLogger(__name__)

# Fixed namespace UUID for deriving deterministic UUID5 identity from
# a wool.ContextVar's module-attribute location. Generated once and
# never changed — changing it would break wire compatibility.
_NAMESPACE_UUID: Final = UUID("7d3e81b9-3f7c-4e2a-9a47-56c82e1f0aa3")


# public
class ContextVarIdentityError(Exception):
    """Raised when a wool.ContextVar's wire IDs resolve to different local
    instances on the receiver, indicating deployment-level drift between
    caller and worker module state.
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


def _reconstruct(
    ids: list[UUID],
    name: str,
    has_default: bool,
    default: Any,
) -> ContextVar:
    """Unpickle helper: resolve a ContextVar by any of its shipped UUIDs.

    Resolution strategy, in order:
    1. Look up each ID in the process-wide ``_uuid_registry``.
    2. For IDs not in the registry, search ``sys.modules`` (and class
       bodies) for a wool.ContextVar whose UUID5 matches.

    Trichotomy:
    - Zero resolved: construct a fresh instance, register under all
      supplied IDs.
    - One distinct instance resolved: return it; register unresolved
      IDs under it (enrichment for future lookups).
    - Multiple distinct instances resolved: raise
      :class:`ContextVarIdentityError` — real module-level drift.
    """
    resolved: list[ContextVar] = []
    unresolved: list[UUID] = []
    for uid in ids:
        existing = ContextVar._uuid_registry.get(uid)
        if existing is None:
            existing = _find_var_by_uuid(uid)
        if existing is not None:
            resolved.append(existing)
        else:
            unresolved.append(uid)

    distinct = {id(r) for r in resolved}

    if not distinct:
        instance = ContextVar.__new__(ContextVar)
        instance._name = name
        instance._default = default if has_default else _SENTINEL
        instance._stdlib = contextvars.ContextVar(name, default=_UNSET)
        instance._ids = list(ids)
        instance._uuid4 = _pick_uuid4(ids)
        ContextVar._instance_registry.add(instance)
        for uid in ids:
            ContextVar._uuid_registry[uid] = instance
        return instance

    if len(distinct) == 1:
        first = resolved[0]
        for uid in unresolved:
            ContextVar._uuid_registry[uid] = first
        return first

    raise ContextVarIdentityError(
        f"wool.ContextVar {name!r} identity divergence: the caller's "
        f"bindings resolve to {len(distinct)} different instances on "
        f"this process. Deployment drift between caller and worker. "
        f"IDs: {[str(u) for u in ids]}"
    )


def _pick_uuid4(ids: list[UUID]) -> UUID | None:
    """Pick the UUID4 from an id list, if present. Used on reconstruction."""
    for uid in ids:
        if uid.version == 4:
            return uid
    return None


def _find_var_by_uuid(target: UUID) -> ContextVar | None:
    """Search ``sys.modules`` and class bodies for a wool.ContextVar
    whose UUID5 of location matches *target*.

    Only meaningful for version-5 UUIDs. Returns ``None`` for
    version-4 UUIDs without a walk (no location to compute).
    """
    if target.version != 5:
        return None
    visited_classes: set[int] = set()
    for mod_name, mod in list(sys.modules.items()):
        if mod is None:
            continue
        try:
            mod_dict = mod.__dict__
        except AttributeError:
            continue
        for attr_name, value in list(mod_dict.items()):
            if isinstance(value, ContextVar):
                uid = uuid5(_NAMESPACE_UUID, f"{mod_name}:{attr_name}")
                if uid == target:
                    return value
            elif isinstance(value, type):
                found = _find_var_in_class(
                    mod_name, attr_name, value, target, visited_classes
                )
                if found is not None:
                    return found
    return None


def _find_var_in_class(
    mod_name: str,
    class_path: str,
    cls: type,
    target: UUID,
    visited: set[int],
) -> ContextVar | None:
    """Recurse into a class body searching for a ContextVar whose UUID5
    matches *target*.
    """
    key = id(cls)
    if key in visited:
        return None
    visited.add(key)
    try:
        class_dict = cls.__dict__
    except AttributeError:
        return None
    for attr_name, value in list(class_dict.items()):
        if isinstance(value, ContextVar):
            uid = uuid5(_NAMESPACE_UUID, f"{mod_name}:{class_path}.{attr_name}")
            if uid == target:
                return value
        elif isinstance(value, type) and _class_defined_in(value, mod_name):
            nested = _find_var_in_class(
                mod_name, f"{class_path}.{attr_name}", value, target, visited
            )
            if nested is not None:
                return nested
    return None


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
        return f"<wool.Token var={self._var._name!r}{used_marker}>"


def _iter_locations(target: ContextVar) -> Iterator[tuple[str, str]]:
    """Yield ``(module_name, dotted_attr_path)`` for each binding of *target*.

    Walks ``sys.modules`` for module-level attributes and recurses into
    class bodies (with cycle protection) for nested bindings. Covers:

    - Module-level: ``lib.foo = ContextVar(...)``.
    - Class body: ``class C: foo = ContextVar(...)``.
    - Nested class body: ``class Outer: class Inner: foo = ContextVar(...)``.

    Instance attributes, closures, defaults, and container values are
    not reachable via this walk; those are handled by the lazy UUID4
    fallback on the instance.
    """
    visited_classes: set[int] = set()
    for mod_name, mod in list(sys.modules.items()):
        if mod is None:
            continue
        try:
            mod_dict = mod.__dict__
        except AttributeError:
            continue
        for attr_name, value in list(mod_dict.items()):
            if value is target:
                yield (mod_name, attr_name)
            elif isinstance(value, type):
                yield from _walk_class(
                    mod_name, attr_name, value, target, visited_classes
                )


def _class_defined_in(cls: type, mod_name: str) -> bool:
    """Return True iff *cls* appears to be defined in module *mod_name*.

    Avoids chasing re-exported/imported classes back into their
    defining module on every scan — which would cause redundant work
    and, if class graphs cycle across modules, infinite recursion.
    """
    return getattr(cls, "__module__", None) == mod_name


def _walk_class(
    mod_name: str,
    class_path: str,
    cls: type,
    target: ContextVar,
    visited: set[int],
) -> Iterator[tuple[str, str]]:
    """Recurse into *cls*'s __dict__ for bindings of *target*.

    Uses *visited* (keyed by ``id(cls)``) to break cycles from
    self-referential class attributes.
    """
    key = id(cls)
    if key in visited:
        return
    visited.add(key)
    try:
        class_dict = cls.__dict__
    except AttributeError:
        return
    for attr_name, value in list(class_dict.items()):
        if value is target:
            yield (mod_name, f"{class_path}.{attr_name}")
        elif isinstance(value, type) and _class_defined_in(value, mod_name):
            yield from _walk_class(
                mod_name, f"{class_path}.{attr_name}", value, target, visited
            )


# public
class ContextVar(Generic[T]):
    """Propagating context variable that crosses worker boundaries.

    Mirrors :class:`contextvars.ContextVar` at the surface: construct
    with a name and optional default; call :meth:`get`, :meth:`set`,
    :meth:`reset`. Unlike the stdlib class, instances pickle across
    process boundaries and their values propagate through
    ``@wool.routine`` dispatches.

    Identity across processes is derived from the instance's bindings:

    - **Module-level binding** (``lib.foo = wool.ContextVar(...)``) —
      the var is identified across processes by its
      ``(module, attribute_path)`` location. Caller and worker both
      compute the same UUID5 from that location, so the wire looks up
      the same logical var on either side.
    - **Class-level binding** (``class C: foo = wool.ContextVar(...)``)
      — the dotted class path is included in the canonical location,
      e.g. ``"lib:C.foo"``. Nested classes work the same way.
    - **Unbound** (function local, closure cell, default argument,
      container value) — the instance is assigned a lazy UUID4 at first
      pickle. Identity is preserved for pickle roundtrips within one
      process; across processes, each side constructs fresh instances
      unless the caller's UUID4 is shipped and the receiver
      reconstructs against the registry.

    Two construction modes:

    - ``ContextVar(name)`` — no default; :meth:`get` raises
      :class:`LookupError` until a value is set.
    - ``ContextVar(name, *, default=...)`` — :meth:`get` returns the
      default when the variable has no value in the current context.

    Values propagated across the wire MUST be cloudpicklable.
    Non-serializable values surface a :class:`TypeError` at dispatch
    time naming the offending variable.
    """

    __slots__ = (
        "_name",
        "_default",
        "_stdlib",
        "_ids",
        "_uuid4",
        "__weakref__",
    )

    _instance_registry: ClassVar[weakref.WeakSet[ContextVar[Any]]] = weakref.WeakSet()
    _uuid_registry: ClassVar[weakref.WeakValueDictionary[UUID, ContextVar[Any]]] = (
        weakref.WeakValueDictionary()
    )

    _name: str
    _default: Any
    _stdlib: contextvars.ContextVar[T | _UnsetType]
    # Cached list of UUIDs identifying this instance. Populated lazily
    # at pickle time via :meth:`_resolve_ids`. UUID5 per module- or
    # class-level location, plus a lazy UUID4 once one has been minted.
    _ids: list[UUID] | None
    # Lazy UUID4 minted on first pickle if the instance has no
    # discoverable locations. Cached so repeated pickles converge on a
    # stable identity. Retained even after the instance gains a
    # location binding, so in-flight wire data still resolves.
    _uuid4: UUID | None

    @overload
    def __init__(self, name: str, /) -> None: ...

    @overload
    def __init__(self, name: str, /, *, default: T) -> None: ...

    def __init__(self, name: str, /, *, default: Any = _SENTINEL):
        self._name = name
        self._default = default
        self._stdlib = contextvars.ContextVar(name, default=_UNSET)
        self._ids = None
        self._uuid4 = None
        type(self)._instance_registry.add(self)

    def _resolve_ids(self) -> list[UUID]:
        """Return every UUID identifying this instance, refreshed from
        current module state.

        Walks ``sys.modules`` (and class bodies) for bindings of
        ``self``, computes a UUID5 per location using
        :data:`_NAMESPACE_UUID`, and unions with any previously-minted
        lazy UUID4. If the instance has no bindings, mints a UUID4
        lazily on first call and retains it.

        Runs only at pickle time, not on every ``get``/``set``/``reset``.
        The returned list is cached on the instance; the cache is
        refreshed unconditionally to pick up runtime alias additions.

        :returns:
            A non-empty list of UUIDs. Module/class locations produce
            UUID5s (sorted for determinism); the lazy UUID4 appended
            last if present.
        """
        found_ids: list[UUID] = []
        seen: set[UUID] = set()
        for mod_name, dotted in _iter_locations(self):
            uid = uuid5(_NAMESPACE_UUID, f"{mod_name}:{dotted}")
            if uid not in seen:
                found_ids.append(uid)
                seen.add(uid)
        found_ids.sort()
        if not found_ids and self._uuid4 is None:
            self._uuid4 = uuid4()
        if self._uuid4 is not None:
            found_ids.append(self._uuid4)
        cls = type(self)
        for uid in found_ids:
            cls._uuid_registry[uid] = self
        self._ids = found_ids
        return found_ids

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
            self._stdlib.set(_UNSET)
        else:
            self._stdlib.set(token._old_value)

    def __repr__(self) -> str:
        default_part = (
            f" default={self._default!r}" if self._default is not _SENTINEL else ""
        )
        return f"<wool.ContextVar name={self._name!r}{default_part} at 0x{id(self):x}>"


class _ContextPickler(cloudpickle.CloudPickler):
    """Cloudpickle pickler that reduces :class:`ContextVar` by identity.

    Overrides :meth:`reducer_override` to emit every
    :class:`ContextVar` encountered in the object graph as a reduce
    tuple carrying its full id list plus name/default. On the receiver,
    :func:`_reconstruct` resolves the IDs to a local instance via the
    process-wide UUID registry (or sys.modules walk with UUID5 match),
    or constructs a fresh instance if unknown.
    """

    def reducer_override(self, obj: Any) -> Any:
        if isinstance(obj, ContextVar):
            ids = obj._resolve_ids()
            has_default = obj._default is not _SENTINEL
            return (
                _reconstruct,
                (
                    ids,
                    obj._name,
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


def _snapshot_vars(
    ctx: contextvars.Context | None = None,
    *,
    dumps: Callable[[Any], bytes] = _dumps,
) -> dict[str, bytes]:
    """Snapshot all registered :class:`ContextVar` values to wire form.

    Iterates the process-wide registry, reads each var's current value
    (from *ctx* if supplied, else the current stdlib Context), and
    emits ``{uuid_hex: serialized_value}`` for every var with a set
    value.

    Wire keys use the first UUID from ``_resolve_ids`` (sorted
    deterministically), so caller and worker agree on the canonical
    key for a given var.

    :param ctx:
        Optional :class:`contextvars.Context` to read from. When
        ``None``, reads from the current context.
    :param dumps:
        Serializer for values. Defaults to :func:`_dumps`.
    :raises TypeError:
        If a value fails to serialize.
    """
    result: dict[str, bytes] = {}
    for var in list(ContextVar._instance_registry):
        if ctx is not None:
            if var._stdlib not in ctx:
                continue
            raw = ctx[var._stdlib]
        else:
            raw = var._stdlib.get()
        if raw is _UNSET:
            continue
        ids = var._resolve_ids()
        if not ids:
            continue
        try:
            pickled = dumps(raw)
        except Exception as e:
            raise TypeError(
                f"Failed to serialize wool.ContextVar {var._name!r}: {e}"
            ) from e
        # Canonical wire key = first (sorted) UUID5, or lazy UUID4 if
        # that's all we have. Stable across caller and worker.
        result[ids[0].hex] = pickled
    return result


def _apply_vars(
    vars: dict[str, bytes],
    *,
    loads: Callable[[bytes], Any] = _loads,
) -> None:
    """Apply a wire-form context snapshot to the current context.

    For each entry ``{uuid_hex: serialized_value}``, resolves the UUID
    to a local :class:`ContextVar` via the registry (or sys.modules
    walk as fallback) and calls :meth:`ContextVar.set` on it in the
    current stdlib Context. Unknown UUIDs log-warn and are skipped.

    :param vars:
        A ``{uuid_hex: serialized_value}`` dict from the proto map.
    :param loads:
        Deserializer for values. Defaults to :func:`_loads`.
    """
    if not vars:
        return
    for key, data in vars.items():
        try:
            uid = UUID(hex=key)
        except (ValueError, TypeError):
            _log.warning("Malformed wool.ContextVar wire key %r; skipping", key)
            continue
        var = ContextVar._uuid_registry.get(uid) or _find_var_by_uuid(uid)
        if var is None:
            _log.warning(
                "wool.ContextVar UUID %s not resolvable on this process; "
                "propagated value dropped",
                uid,
            )
            continue
        ContextVar._uuid_registry[uid] = var
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
    for var in list(ContextVar._instance_registry):
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
    """
    vars_dict: dict[ContextVar[Any], Any] = {}
    for var in list(ContextVar._instance_registry):
        raw = var._stdlib.get()
        if raw is _UNSET:
            continue
        vars_dict[var] = raw
    return Context._from_vars(_lineage_id_now(), vars_dict)


def build_task_frame_payload() -> tuple[dict[str, bytes], str]:
    """Assemble the wire payload for the initial Task dispatch frame.

    Returns ``(vars, lineage_id)`` for populating the protobuf
    ``vars`` map and ``lineage_id`` string on the first Request of a
    dispatch stream.

    ``lineage_id`` is the active lineage UUID as a hex string, or
    empty when no lineage is active (root dispatch — the worker
    assigns one).
    """
    vars_dict = _snapshot_vars()
    lineage_hex = _lineage_id_now().hex
    return vars_dict, lineage_hex


def build_stream_frame_payload() -> tuple[dict[str, bytes], str]:
    """Assemble the wire payload for streaming frames (next/send/throw).

    Streaming frames re-ship the current var snapshot plus the
    lineage ID so back-propagated values reach the caller and the
    worker confirms the lineage.
    """
    vars_dict = _snapshot_vars()
    lineage_hex = _lineage_id_now().hex
    return vars_dict, lineage_hex


dispatch_timeout: Final[contextvars.ContextVar[float | None]] = contextvars.ContextVar(
    "dispatch_timeout", default=None
)


# Silence unused-import warnings for names retained as re-exportable
# internals / API surface that static analysis may miss.
_ = (operator, importlib)
