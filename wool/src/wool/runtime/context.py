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
    UUID-keyed synthetic name. If found, returns it — this path is
    purely multi-unpickle dedup within one process. A single
    pickle-reconstructed instance resolves to the same Python object
    on every subsequent unpickle of the same UUID.

    Construction-by-name on the worker never collides with a
    reconstructed instance, because each user construction mints a
    fresh UUID.
    """
    existing = sys.modules.get(synthetic_name)
    if existing is not None and isinstance(existing, ContextVar):
        return existing
    mod = types.ModuleType.__new__(ContextVar, synthetic_name)  # type: ignore[call-arg]
    types.ModuleType.__init__(mod, synthetic_name)
    mod._wool_name = state["_wool_name"]
    mod._uuid = state["_uuid"]
    mod._default = state["_default"]
    mod._var = contextvars.ContextVar(state["_wool_name"], default=_UNSET)
    sys.modules[synthetic_name] = mod
    ContextVar._registry.add(mod)
    ContextVar._construction_count += 1
    return mod


# public
class ContextVar(types.ModuleType, Generic[T]):
    """Propagating context variable that crosses worker boundaries.

    A :class:`types.ModuleType` subclass. Each construction mints a
    fresh UUID and registers the instance in ``sys.modules`` under a
    UUID-keyed synthetic name (``wool._vars.<uuid>``).

    Identity semantics mirror the stdlib :class:`contextvars.ContextVar`:
    each ``wool.ContextVar(name, ...)`` call produces a distinct
    instance, regardless of name. Two calls with the same name return
    two independent vars. Name is cosmetic — used for the stdlib
    backing var and for repr.

    Cross-process propagation rides the manifest machinery
    (:class:`Manifest`), not name-based unification. At dispatch time
    a manifest is built by walking ``sys.modules`` to discover
    ``(module, attr) → ContextVar`` bindings; on the worker, task-
    local module clones get those bindings substituted via
    :meth:`Manifest.apply`.

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

    # Counter of live constructions. Incremented in __init__ and in
    # _reconstruct_context_var; used by Manifest.build() as an
    # early-exit upper bound.
    _construction_count: ClassVar[int] = 0

    _wool_name: str
    _uuid: UUID
    _default: Any
    _var: contextvars.ContextVar[T | _UnsetType]

    def __new__(cls, name: str, /, *, default: Any = _SENTINEL):
        # Mint a fresh UUID-keyed synthetic name. No name-based
        # sys.modules lookup — each construction is a distinct var.
        uid = uuid4()
        synthetic = f"wool._vars.{uid}"
        instance = super().__new__(cls, synthetic)  # type: ignore[call-arg]
        # Stash the pre-minted UUID so __init__ can reuse it without
        # generating a second one.
        instance._pending_uuid = uid  # type: ignore[attr-defined]
        return instance

    @overload
    def __init__(self, name: str, /) -> None: ...

    @overload
    def __init__(self, name: str, /, *, default: T) -> None: ...

    def __init__(self, name: str, /, *, default: Any = _SENTINEL):
        uid = self.__dict__.pop("_pending_uuid", None) or uuid4()
        synthetic = f"wool._vars.{uid}"
        super().__init__(synthetic)
        self._wool_name = name
        self._uuid = uid
        self._default = default
        self._var = contextvars.ContextVar(name, default=_UNSET)
        sys.modules[synthetic] = self
        type(self)._registry.add(self)
        type(self)._construction_count += 1

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
    """Internal serialization wrapper for context variable *values*.

    Handles the conversion between the in-process
    :class:`ContextVar` registry and the ``map<string, bytes>``
    protobuf representation used on the ``Request.vars`` and
    ``Response.vars`` fields. The map key is the UUID-keyed synthetic
    ``sys.modules`` name (``wool._vars.<uuid>``).

    This class handles VALUES only. Instance identity and the
    ``(module, attr) → ContextVar`` binding map is the concern of
    :class:`Manifest`. Values apply successfully only after the
    corresponding manifest entries have been applied (so that
    ``sys.modules[synthetic_name]`` resolves on the worker).
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


class Manifest:
    """Instance-binding manifest for cross-process ContextVar propagation.

    A manifest records which module-level ``(module_name, attr_name)``
    bindings in a process refer to which :class:`ContextVar`
    instances. On dispatch the caller builds a manifest by walking
    :data:`sys.modules` for bindings to live ContextVars in the
    :attr:`ContextVar._registry`. The manifest ships alongside the
    task payload (via the proto ``manifest`` field) and is applied on
    the worker by substituting the reconstructed ContextVar instances
    into the corresponding task-local module clones.

    Manifest entries are distinct from value snapshots
    (:class:`_Context`). The manifest conveys *which instance lives at
    which module attribute*; the value snapshot conveys *what value
    each instance currently holds*. Values apply after manifests
    because value lookups resolve through the instance registered in
    :data:`sys.modules`, which the manifest establishes.
    """

    @staticmethod
    def build() -> dict[tuple[str, str], ContextVar]:
        """Discover ``(module, attr) → ContextVar`` bindings.

        Walks :data:`sys.modules`, inspecting each module's
        ``__dict__`` for attributes that are live :class:`ContextVar`
        instances.

        Every distinct binding is recorded. A single ContextVar that
        is aliased under multiple modules (e.g., via ``from lib_code
        import trace_id``) appears once for each binding. This is
        intentional: on the worker, each binding location needs its
        substitution so library helpers resolving ``LOAD_GLOBAL``
        against *their* module's dict see the wire-shipped instance.
        Since all aliased bindings map to the same var, the wire
        ships that var's state once and ``_reconstruct_context_var``
        dedupes on the worker via its UUID-keyed sys.modules entry.

        Returns an empty dict when no ContextVars exist in the
        process (``ContextVar._construction_count == 0``), skipping
        the sys.modules walk entirely.

        Modules whose name starts with ``wool._vars.`` (the synthetic
        registrations for ContextVar instances themselves) are
        skipped.
        """
        if ContextVar._construction_count == 0:
            return {}
        manifest: dict[tuple[str, str], ContextVar] = {}

        for mod_name, mod in list(sys.modules.items()):
            if mod is None or mod_name.startswith("wool._vars."):
                continue
            try:
                mod_dict = mod.__dict__
            except AttributeError:
                continue
            for attr_name, attr_value in list(mod_dict.items()):
                if isinstance(attr_value, ContextVar):
                    manifest[(mod_name, attr_name)] = attr_value
        return manifest

    @staticmethod
    def apply(
        manifest: dict[tuple[str, str], ContextVar],
        *,
        target_modules: dict[str, types.ModuleType] | None = None,
    ) -> None:
        """Substitute manifest entries into target modules.

        Writes each ``manifest[(mod_name, attr_name)] = instance``
        binding into ``target_modules[mod_name].__dict__[attr_name]``,
        overwriting any freshly-constructed instance that lived there
        (typically a worker-side import of the library that defined
        the ContextVar).

        :param manifest:
            ``(module_name, attr_name) → ContextVar`` bindings
            (usually the output of :meth:`build` on the caller side).
        :param target_modules:
            Mapping of module name to module instance to write into.
            Defaults to :data:`sys.modules`. Worker-side invocations
            typically pass a task-local dict of cloned modules so the
            mutation is isolated from other concurrent tasks.
        """
        targets = target_modules if target_modules is not None else sys.modules
        for (mod_name, attr_name), instance in manifest.items():
            mod = targets.get(mod_name)
            if mod is None:
                continue
            setattr(mod, attr_name, instance)

    @staticmethod
    def to_wire(
        manifest: dict[tuple[str, str], ContextVar],
        *,
        dumps: Callable[[Any], bytes] = cloudpickle.dumps,
    ) -> list[tuple[str, str, bytes]]:
        """Serialize a manifest for the proto ``manifest`` field.

        Each ContextVar is pickled via its :meth:`ContextVar.__reduce__`
        which ships the UUID + state.

        :returns:
            List of ``(module_name, attr_name, pickled_var)`` triples.
        """
        return [
            (mod_name, attr_name, dumps(var))
            for (mod_name, attr_name), var in manifest.items()
        ]

    @staticmethod
    def from_wire(
        entries: list[tuple[str, str, bytes]],
        *,
        loads: Callable[[bytes], Any] = cloudpickle.loads,
    ) -> dict[tuple[str, str], ContextVar]:
        """Reconstruct a manifest from wire entries.

        Unpickling each entry routes through
        :func:`_reconstruct_context_var`, which either returns an
        already-registered instance (multi-unpickle dedup) or
        constructs and registers a new one in :data:`sys.modules`.
        The returned manifest is ready to pass to :meth:`apply`.
        """
        manifest: dict[tuple[str, str], ContextVar] = {}
        for mod_name, attr_name, blob in entries:
            var = loads(blob)
            if isinstance(var, ContextVar):
                manifest[(mod_name, attr_name)] = var
        return manifest


def build_task_frame_payload() -> tuple[
    dict[str, bytes],
    list[tuple[str, str, bytes]],
    str,
]:
    """Assemble the wire payload for the initial Task dispatch frame.

    Returns ``(vars, manifest_entries, lineage_id)`` for populating
    the protobuf ``vars``, ``manifest``, and ``lineage_id`` fields on
    the first Request of a dispatch stream. The manifest ships the
    full ``(module, attr) → ContextVar`` binding snapshot so the
    worker can clone the referenced modules and substitute the
    wire-reconstructed instances into them.

    ``lineage_id`` is the active lineage UUID as a hex string, or
    empty when no lineage is active (root dispatch — the worker
    assigns one).
    """
    from wool.runtime.worker.namespace import _active_lineage

    vars_dict = _Context.snapshot()
    manifest = Manifest.build()
    manifest_entries = Manifest.to_wire(manifest)
    lineage = _active_lineage.get()
    lineage_hex = lineage.hex if lineage is not None else ""
    return vars_dict, manifest_entries, lineage_hex


def build_stream_frame_payload() -> tuple[dict[str, bytes], str]:
    """Assemble the wire payload for streaming frames (next/send/throw).

    Streaming frames do not re-ship the manifest: the worker has
    already activated the lineage's module clones from the initial
    Task frame. Only the current var value snapshot and the lineage
    ID are propagated so back-propagated values reach the caller and
    the worker confirms the lineage.
    """
    from wool.runtime.worker.namespace import _active_lineage

    vars_dict = _Context.snapshot()
    lineage = _active_lineage.get()
    lineage_hex = lineage.hex if lineage is not None else ""
    return vars_dict, lineage_hex


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

        # Var propagation is now handled by :class:`Manifest` +
        # :class:`_Context` via the proto ``manifest`` and ``vars``
        # fields. RuntimeContext.vars is retained for backwards
        # compatibility only and is NOT applied here — attempting to
        # resolve name-keyed vars would be lossy under the new
        # UUID-keyed identity model. The dispatch pipeline applies
        # wire values via :meth:`_Context.apply` after
        # :func:`namespace.activate` has registered the
        # wire-reconstructed instances.
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
