from __future__ import annotations

import asyncio
import contextvars
import logging
import threading
import warnings
import weakref
from contextlib import contextmanager
from contextlib import nullcontext
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Coroutine
from typing import Final
from typing import ItemsView
from typing import Iterator
from typing import KeysView
from typing import NoReturn
from typing import SupportsIndex
from typing import TypeVar
from typing import ValuesView
from uuid import UUID
from uuid import uuid4

import wool
from wool import protocol
from wool.runtime.context.registry import context_registry
from wool.runtime.context.registry import lock
from wool.runtime.context.registry import scope_key
from wool.runtime.context.registry import token_registry
from wool.runtime.context.registry import var_registry
from wool.runtime.context.stub import resolve_stub
from wool.runtime.serializer import Serializer
from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

if TYPE_CHECKING:
    from wool.runtime.context.stub import StubPin
    from wool.runtime.context.token import Token
    from wool.runtime.context.var import ContextVar


# Ambient per-Context dispatch timeout in seconds. ``None`` means no
# timeout. The value scopes to whichever execution chain is currently
# active and rides through nested dispatches until reset or overridden.
dispatch_timeout: Final[contextvars.ContextVar[float | None]] = contextvars.ContextVar(
    "dispatch_timeout", default=None
)

_log = logging.getLogger(__name__)

_loops_with_factory: weakref.WeakSet[asyncio.AbstractEventLoop] = weakref.WeakSet()

T = TypeVar("T")


# public
class ContextDecodeWarning(RuntimeWarning):
    """Emitted when a wire :class:`protocol.Context` fails to decode.

    Wool's wire protocol treats context propagation as ancillary
    state — failures to decode incoming context never preempt the
    primary signal (the routine's return value or raised exception).
    Instead a :class:`ContextDecodeWarning` is emitted so callers
    that depend on context state can detect the inconsistency.

    Callers that prefer strict semantics — treat any decode failure
    as fatal — can opt in by promoting the warning to an exception::

        import warnings
        import wool

        warnings.filterwarnings("error", category=wool.ContextDecodeWarning)

    Under strict mode, per-var failures inside :meth:`Context.to_protobuf`
    and :meth:`Context.from_protobuf` aggregate into a single
    :class:`BaseExceptionGroup` raised after the loop completes — every
    bad var surfaces, not just the first.
    """


# public
class ContextAlreadyBound(RuntimeError):
    """Raised when a task is bound to a :class:`Context` more than once.

    Enforces the one-shot contract: a task is bound exactly once,
    at creation time. A second binding attempt would silently stomp
    the prior binding, masking bugs where the wrong :class:`Context`
    (and thus the wrong chain ID) rides through nested dispatches.
    """


# public
class RuntimeContext:
    """Block-scoped runtime option overrides for wool routines.

    Used as a context manager to override runtime options (currently
    only :data:`dispatch_timeout`) for the duration of a block. Auto-
    captured on every :class:`Task` at construction time, which ships
    the caller's snapshot across the wire so the worker restores it
    before running the routine.

    :param dispatch_timeout:
        Default timeout for task dispatch operations. ``None`` means
        no timeout. Leaving this argument out (the default sentinel)
        has two effects: ``__enter__`` skips setting the stdlib var
        — useful for "no-override" usage as a context manager — and
        :meth:`to_protobuf` substitutes the current scope's live
        :data:`dispatch_timeout` at encode time, so a bare
        ``RuntimeContext()`` constructed for wire transport still
        propagates the encoder's effective timeout to the receiver.
    """

    _dispatch_timeout: float | None | UndefinedType
    _dispatch_timeout_token: contextvars.Token[float | None] | None

    def __init__(
        self,
        *,
        dispatch_timeout: float | None | UndefinedType = Undefined,
    ) -> None:
        self._dispatch_timeout = dispatch_timeout
        self._dispatch_timeout_token = None

    def __enter__(self) -> RuntimeContext:
        if self._dispatch_timeout is not Undefined:
            self._dispatch_timeout_token = dispatch_timeout.set(self._dispatch_timeout)
        return self

    def __exit__(self, *_):
        if self._dispatch_timeout_token is not None:
            dispatch_timeout.reset(self._dispatch_timeout_token)
            self._dispatch_timeout_token = None

    @classmethod
    def get_current(cls) -> RuntimeContext:
        """Capture the current stdlib :data:`dispatch_timeout` value."""
        return cls(dispatch_timeout=dispatch_timeout.get())

    @classmethod
    def from_protobuf(cls, context: protocol.RuntimeContext) -> RuntimeContext:
        """Reconstruct from a :class:`protocol.RuntimeContext` message."""
        return cls(
            dispatch_timeout=(
                context.dispatch_timeout
                if context.HasField("dispatch_timeout")
                else None
            )
        )

    def to_protobuf(self) -> protocol.RuntimeContext:
        """Serialize to a :class:`protocol.RuntimeContext` message.

        When the instance was constructed without an explicit
        ``dispatch_timeout`` (i.e., the default sentinel), the live
        :data:`dispatch_timeout` value from the current scope is
        captured at encode time and rides the wire. An explicit
        :data:`None` skips emission, so the receiver inherits its
        own scope's default.
        """
        message = protocol.RuntimeContext()
        timeout = self._dispatch_timeout
        if timeout is Undefined:
            timeout = dispatch_timeout.get()
        if timeout is not None:
            message.dispatch_timeout = timeout
        return message


# public
class Context:
    """Snapshot of :class:`wool.ContextVar` state and context ID,
    scoped to a single task at a time.

    Mirrors :class:`contextvars.Context` at the surface — supports
    the mapping and container protocols and scopes mutations via
    :meth:`Context.run` — but is a parallel mechanism with no shared
    state. A :class:`wool.Context` and a :class:`contextvars.Context`
    never interact: stdlib :meth:`contextvars.Context.run` does not
    fork or clear the :class:`wool.Context`, and vice versa. The
    Wool task factory is the boundary where Wool's fork-on-task
    semantics engage.

    Beyond the snapshot of :class:`ContextVar` values, a
    :class:`Context` carries a ``UUID`` that identifies the
    logical chain it belongs to.

    .. caution::
       At most one task may run inside a given :class:`Context` at
       a time. :meth:`run` raises :class:`RuntimeError` on
       re-entry.
    """

    __slots__ = (
        "_id",
        "_data",
        "_lock",
        "_running",
        "_stub_pins",
        "_used_tokens",
        "_external_used_tokens",
        "_bound_task",
        "__weakref__",
    )

    _id: UUID
    _data: dict[ContextVar[Any], Any]
    _lock: threading.Lock
    _running: bool
    _stub_pins: set[StubPin]
    # Tokens consumed locally that still have a live in-process
    # instance. Auto-prunes via :class:`weakref.WeakSet` when the
    # last reference to a Token is dropped: a consumed Token whose
    # only role was double-reset detection has nothing left to
    # block once it is unreachable, so its ID can be reclaimed
    # along with the instance.
    _used_tokens: weakref.WeakSet[Token]
    # Tokens known to be consumed but lacking a live in-process
    # :class:`Token` instance — typically wire-supplied entries that
    # arrived in :meth:`from_protobuf` without a same-process pickle
    # round-trip having registered the Token under
    # :data:`token_registry`. The map carries each consumed-token id
    # alongside the :class:`wool.ContextVar` key the token reset, so
    # the receiving :meth:`Context.update` can pop the var from
    # :attr:`_data` and propagate the reset signal even when the
    # token instance never materialized locally. When a matching
    # :class:`Token` later materializes (via
    # :meth:`Token._reconstitute`'s promotion hook) the entry
    # migrates from this map into :attr:`_used_tokens`.
    _external_used_tokens: dict[UUID, tuple[str, str]]
    # Weakref to the asyncio task currently scoped via
    # :func:`_wool_scoped`. Set on factory-routed task entry and
    # cleared on exit, giving "first task wins for the routine's
    # lifetime" semantics — a second factory-routed task targeting
    # the same Context while the first is still active raises before
    # acquiring :meth:`_guard`. ``None`` outside of a
    # :func:`_wool_scoped` block, or for sync-only callers.
    _bound_task: weakref.ref[asyncio.Task[Any]] | None

    def __init__(self) -> None:
        self._init_state(context_id=uuid4(), data={})

    def __iter__(self) -> Iterator[ContextVar[Any]]:
        return iter(self._data)

    def __getitem__(self, var: ContextVar[T]) -> T:
        return self._data[var]

    def __contains__(self, var: Any) -> bool:
        return var in self._data

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"<wool.Context id={self._id} vars={len(self)}>"

    def __reduce_ex__(self, _protocol: SupportsIndex) -> NoReturn:
        """Refuse pickle, ``copy.copy``, and ``copy.deepcopy``.

        Mirrors :class:`contextvars.Context`. A snapshot disconnected
        from live state is uniformly a footgun, so the rejection
        applies under both vanilla pickling and Wool's own pickler —
        intentionally no :meth:`__wool_reduce__` is defined. Callers
        wanting in-process duplication must use :meth:`Context.copy`
        explicitly; cross-process propagation rides
        :meth:`to_protobuf` and :meth:`from_protobuf` instead.
        """
        raise TypeError(
            "cannot pickle 'wool.Context' object — use Context.copy() "
            "for in-process duplication"
        )

    @property
    def id(self) -> UUID:
        """The UUID that identifies this :class:`Context`'s logical chain."""
        return self._id

    @classmethod
    def from_protobuf(
        cls,
        wire_context: protocol.Context,
        *,
        serializer: Serializer | None = None,
    ) -> Context:
        """Reconstruct a :class:`Context` from a wire :class:`protocol.Context`.

        Walks ``wire_context.vars`` once, materializing each entry into
        the receiver's :class:`Context`: the var identity resolves
        through the process-wide :class:`wool.ContextVar` registry
        (or pins a stub if undeclared), the optional serialized value
        is deserialized into the receiver's data dict, and any
        consumed-token IDs the entry carries promote to the live-Token
        slot or stash into :attr:`_external_used_tokens` for later
        reset propagation. Stub creation pins the stub onto the
        current :class:`Context`, so a lazy-import receiver sees the
        propagated value as soon as it later declares the var — the
        same stub-promotion path a pickled :class:`ContextVar`
        instance uses when it rides through ``__reduce__`` embedded
        in a routine argument.

        :param wire_context:
            The wire :class:`protocol.Context` to decode.
        :param serializer:
            Deserializer for values. ``None`` (default) selects
            :data:`wool.__serializer__`.

        Decode failures emit :class:`ContextDecodeWarning` and the
        offending entry is skipped — surviving entries decode
        normally. A malformed wire context id falls back to a fresh
        UUID with the failure recorded as the same warning class.
        Operators who want strict behavior promote the warning via
        ``PYTHONWARNINGS=error::wool.ContextDecodeWarning``; under
        strict mode the failures aggregate into a single
        :class:`BaseExceptionGroup` raised after the decode loop so
        callers learn about every failure, not just the first.

        :raises BaseExceptionGroup:
            Under strict mode, when one or more entries fail to
            decode; the group's peers are the per-failure
            :class:`ContextDecodeWarning` instances.
        """
        if serializer is None:
            serializer = wool.__serializer__
        failures: list[ContextDecodeWarning] = []
        try:
            ctx_id = UUID(hex=wire_context.id) if wire_context.id else uuid4()
        except ValueError as e:
            try:
                warnings.warn(
                    f"Failed to decode wire context id {wire_context.id!r}: {e}",
                    ContextDecodeWarning,
                    stacklevel=2,
                )
            except ContextDecodeWarning as raised:
                failures.append(raised)
            ctx_id = uuid4()
        data: dict[ContextVar[Any], Any] = {}
        ctx = cls._reconstitute(ctx_id, data)
        with attached(ctx, guarded=False):
            for entry in wire_context.vars:
                var_key = (entry.namespace, entry.name)
                var = resolve_stub(var_key, ctx)
                if entry.HasField("value"):
                    try:
                        data[var] = serializer.loads(entry.value)
                    except Exception as e:
                        try:
                            warnings.warn(
                                f"Failed to deserialize wool.ContextVar "
                                f"{var_key!r}: {e}",
                                ContextDecodeWarning,
                                stacklevel=2,
                            )
                        except ContextDecodeWarning as raised:
                            failures.append(raised)
                # Each consumed-token ID is either promoted to the
                # live-Token slot when the same-process registry has
                # an instance for it, or stashed in
                # :attr:`_external_used_tokens` keyed by the var so
                # a subsequent :meth:`Context.update` can pop the
                # corresponding var from the receiver's data — the
                # reset signal propagates even when the token
                # instance never materializes locally.
                for token_id_hex in entry.consumed_tokens:
                    try:
                        token_id = UUID(hex=token_id_hex)
                    except ValueError as e:
                        try:
                            warnings.warn(
                                f"Failed to decode consumed-token ID "
                                f"{token_id_hex!r} for var {var_key!r}: {e}",
                                ContextDecodeWarning,
                                stacklevel=2,
                            )
                        except ContextDecodeWarning as raised:
                            failures.append(raised)
                        continue
                    live = token_registry.get(token_id)
                    if live is not None:
                        if not live._used:
                            live._used = True
                        ctx._used_tokens.add(live)
                    else:
                        ctx._external_used_tokens[token_id] = var_key
        if failures:
            raise BaseExceptionGroup(
                "wool context decode failed",
                failures,
            )
        return ctx

    def to_protobuf(
        self,
        *,
        serializer: Serializer | None = None,
    ) -> protocol.Context:
        """Snapshot this :class:`Context` to a wire :class:`protocol.Context`.

        Each var observable in the snapshot — by carrying a value or
        by being the source of a consumed token — emits one
        :class:`protocol.ContextVar` entry. The entry's ``value`` is
        set when the var has a current binding in this :class:`Context`
        and unset otherwise (a var that was set and then reset to no
        prior value still rides the wire so its consumed-token IDs
        propagate). Default-only values — vars that have never been
        explicitly set in this :class:`Context` and have no consumed
        tokens — are absent from the snapshot.

        Per-var encode failures emit :class:`ContextDecodeWarning`
        and the offending key is skipped — surviving vars encode
        normally, mirroring the per-entry resilience of
        :meth:`from_protobuf`. Operators who want strict behavior
        promote the warning via
        ``PYTHONWARNINGS=error::wool.ContextDecodeWarning``; under
        strict mode the per-var failures aggregate into a single
        :class:`BaseExceptionGroup` raised after the loop so callers
        learn about every bad var, not just the first.

        :param serializer:
            Serializer for values. ``None`` (default) selects
            :data:`wool.__serializer__`.
        :raises BaseExceptionGroup:
            Under strict mode, when one or more vars fail to encode;
            the group's peers are the per-var
            :class:`ContextDecodeWarning` instances.
        """
        if serializer is None:
            serializer = wool.__serializer__
        wire_context = protocol.Context(id=self._id.hex)
        failures: list[ContextDecodeWarning] = []
        encoded_values: dict[tuple[str, str], bytes] = {}
        failed_keys: set[tuple[str, str]] = set()
        for var, value in self.items():
            try:
                encoded_values[var._key] = serializer.dumps(value)
            except Exception as e:
                failed_keys.add(var._key)
                try:
                    warnings.warn(
                        f"Failed to serialize wool.ContextVar {var._key!r}: {e}",
                        ContextDecodeWarning,
                        stacklevel=2,
                    )
                except ContextDecodeWarning as raised:
                    failures.append(raised)
        token_ids_by_key: dict[tuple[str, str], list[str]] = {}
        for token_id, var_key in self._consumed_entries():
            # A var whose value failed to serialize is suppressed
            # entirely — emitting consumed tokens without a value would
            # propagate a phantom reset on the receiver, which would
            # interpret the half-encoded entry as "reset and not
            # re-set" via consumed_keys minus sender_data_keys in
            # :meth:`update`.
            if var_key in failed_keys:
                continue
            token_ids_by_key.setdefault(var_key, []).append(token_id.hex)
        for var_key in set(encoded_values).union(token_ids_by_key):
            namespace, name = var_key
            entry = wire_context.vars.add(namespace=namespace, name=name)
            if var_key in encoded_values:
                entry.value = encoded_values[var_key]
            if var_key in token_ids_by_key:
                entry.consumed_tokens.extend(token_ids_by_key[var_key])
        if failures:
            raise BaseExceptionGroup(
                "wool context encode failed for one or more vars",
                failures,
            )
        return wire_context

    def has_state(self) -> bool:
        """Return True if this :class:`Context` carries any observable
        state — var bindings, locally-consumed tokens, or externally-
        supplied consumed-token entries awaiting promotion.

        Distinct from :meth:`__bool__` / :meth:`__len__`, which follow
        the mapping-container contract and report only on var
        bindings. Wire-side callers use :meth:`has_state` to skip
        no-op merges from empty wire frames.
        """
        return (
            bool(self._data)
            or bool(self._used_tokens)
            or bool(self._external_used_tokens)
        )

    def copy(self) -> Context:
        """Return a shallow copy of this :class:`Context` with a fresh ID.

        Mirrors :meth:`contextvars.Context.copy` — the copy is a
        new logical chain with its own UUID, so mutations to the
        copy do not affect this :class:`Context` and nested
        dispatches fired under the copy carry its fresh ID, not this
        :class:`Context`'s. The consumed-token set is not carried
        across: any :class:`Token` minted under this
        :class:`Context`'s UUID is already incompatible with the
        copy's UUID for :meth:`ContextVar.reset` purposes.
        """
        return Context._reconstitute(uuid4(), dict(self))

    def run(self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        """Run the specified callable in this :class:`Context`.

        Installs this :class:`Context` as the current scope's active
        :class:`Context`, runs the callable, then restores the
        previous :class:`Context`. Mutations made by the callable go
        directly into this :class:`Context`. Affects only Wool's
        per-scope :class:`Context` registry; the surrounding
        :class:`contextvars.Context` is untouched. Compose with
        :meth:`contextvars.Context.run` to scope both at once::

            wool_ctx.run(stdlib_ctx.run, fn, *args, **kwargs)

        :raises RuntimeError:
            If this :class:`Context` is already running a task.
        """
        with attached(self):
            return fn(*args, **kwargs)

    def get(self, var: ContextVar[T], default: Any = None) -> Any:
        """Return *var*'s value in this :class:`Context`, or *default* if unset."""
        return self._data.get(var, default)

    def keys(self) -> KeysView[ContextVar[Any]]:
        """Return a view of every :class:`ContextVar` bound in this :class:`Context`."""
        return self._data.keys()

    def values(self) -> ValuesView[Any]:
        """Return a view of every value bound in this :class:`Context`."""
        return self._data.values()

    def items(self) -> ItemsView[ContextVar[Any], Any]:
        """Return a view of every (var, value) pair bound in this :class:`Context`."""
        return self._data.items()

    def update(self, other: Context) -> None:
        """Apply *other*'s vars and used-token state to this :class:`Context`.

        One-way: *other* is the source of truth for overlapping
        keys. This :class:`Context`'s ID is unchanged. Mirrors
        :meth:`dict.update` semantics for the var map but extends it
        with reset propagation: a var consumed by *other* and absent
        from *other*'s data (i.e. reset and not subsequently re-set)
        is popped from this :class:`Context`'s data so the merge
        carries the reset signal, not just the post-set state. Live
        Tokens from *other*'s :attr:`_used_tokens` are added to this
        :class:`Context`'s :attr:`_used_tokens` as-is — their
        ``_used`` flag was already flipped by the originating
        :meth:`ContextVar.reset` call before they landed in
        *other*'s set; :meth:`update` does not re-flip them. External
        consumed-token entries from *other* not yet known to this
        :class:`Context` are resolved against :data:`token_registry`:
        a live match promotes directly into :attr:`_used_tokens` and
        ``_used`` is flipped if not already, otherwise the (id,
        var_key) entry joins :attr:`_external_used_tokens`.
        """
        self._data.update(other._data)
        sender_data_keys = {var._key for var in other._data}
        consumed_keys = {token._key for token in other._used_tokens}
        consumed_keys.update(other._external_used_tokens.values())
        for key in consumed_keys - sender_data_keys:
            receiver_var = var_registry.get(key)
            if receiver_var is not None:
                self._data.pop(receiver_var, None)
        for token in other._used_tokens:
            self._used_tokens.add(token)
            self._external_used_tokens.pop(token._id, None)
        known = self._consumed_token_ids()
        for token_id, var_key in other._external_used_tokens.items():
            if token_id in known:
                continue
            live = token_registry.get(token_id)
            if live is not None:
                if not live._used:
                    live._used = True
                self._used_tokens.add(live)
            else:
                self._external_used_tokens[token_id] = var_key

    @classmethod
    def _reconstitute(
        cls,
        context_id: UUID,
        data: dict[ContextVar[Any], Any],
    ) -> Context:
        """Rebuild a :class:`Context` from externally-supplied parts.

        Bypasses ``__init__`` to adopt an externally-supplied
        context ID and data dict, for callers that already hold the
        canonical identity and state to rebuild a :class:`Context`
        around. Not a copy — the dict reference is taken as-is. The
        consumed-token slots start empty; callers populate them via
        :meth:`update`, :meth:`from_protobuf`, or direct mutation
        as their wire shape dictates.
        """
        instance: Context = object.__new__(cls)
        instance._init_state(context_id=context_id, data=data)
        return instance

    def _consumed_entries(self) -> Iterator[tuple[UUID, tuple[str, str]]]:
        """Yield ``(token_id, var_key)`` for every consumed token
        tracked by this :class:`Context`, deduped across
        :attr:`_used_tokens` and :attr:`_external_used_tokens`.

        Iterating :attr:`_used_tokens` materializes only Tokens that
        are still alive — :class:`weakref.WeakSet` skips entries
        whose referents have been collected — so the resulting
        sequence elides IDs whose double-reset detection role is no
        longer load-bearing in this process. External entries cover
        IDs whose owning Token was never reconstituted locally; if
        an ID appears in both stores the live-token entry wins.
        """
        seen: set[UUID] = set()
        for token in self._used_tokens:
            seen.add(token._id)
            yield token._id, token._key
        for token_id, var_key in self._external_used_tokens.items():
            if token_id in seen:
                continue
            yield token_id, var_key

    def _consumed_token_ids(self) -> set[UUID]:
        """Return every consumed-token ID this :class:`Context`
        tracks, across both live and external stores."""
        return {token_id for token_id, _ in self._consumed_entries()}

    def _init_state(
        self,
        *,
        context_id: UUID,
        data: dict[ContextVar[Any], Any],
    ) -> None:
        self._id = context_id
        self._data = data
        self._lock = threading.Lock()
        self._running = False
        self._stub_pins = set()
        self._used_tokens = weakref.WeakSet()
        self._external_used_tokens = {}
        self._bound_task = None

    @contextmanager
    def _guard(self) -> Iterator[None]:
        """Enforce the single-task invariant for the wrapped block.

        Acquires the running flag under :attr:`_lock` on entry
        (raising :class:`RuntimeError` if another task is already
        running inside this :class:`Context`) and releases it on
        exit. Thread-safe.
        """
        with self._lock:
            if self._running:
                raise RuntimeError(
                    "wool.Context is already running; at most one "
                    "task may run inside a given Context at a time"
                )
            self._running = True
        try:
            yield
        finally:
            with self._lock:
                self._running = False


# public
def current_context() -> Context:
    """Return the live :class:`wool.Context` for the current execution scope.

    Inside an asyncio task, looks up the task's :class:`Context` in
    the process-wide registry. Outside a task (sync code), uses a
    per-thread fallback. If no :class:`Context` exists for the
    current scope, one is created lazily and registered.
    """
    _ensure_task_factory_installed()
    with lock:
        existing = context_registry.get()
        if existing is not None:
            return existing
        fresh = Context()
        context_registry[scope_key()] = fresh
        return fresh


@contextmanager
def attached(ctx: Context, *, guarded: bool = True) -> Iterator[None]:
    """Install *ctx* as the current scope's :class:`Context` for the
    duration of the ``with`` block.

    Scoped install/restore. Holds :meth:`Context._guard` for the
    block by default — the discipline that enforces the single-
    task-per-:class:`Context` invariant for user code running
    inside *ctx*. Pass ``guarded=False`` for framework-internal
    decode plumbing where the :class:`Context` only needs to be
    visible for transitive :class:`wool.ContextVar` /
    :class:`wool.Token` reconstitution inside ``serializer.loads``
    calls and the caller is not running user code in *ctx*.

    :raises RuntimeError:
        Under ``guarded=True``, if another task is already running
        inside *ctx*.
    """
    guard = ctx._guard() if guarded else nullcontext()
    with guard:
        token = context_registry.set(ctx)
        try:
            yield
        finally:
            context_registry.reset(token)


# public
def copy_context() -> Context:
    """Return a shallow copy of the current :class:`wool.Context`.

    Mirrors :func:`contextvars.copy_context` — returns a shallow
    copy of the current scope's context as a new :class:`Context`
    instance. The copy receives a fresh logical-chain ID, so it is
    independent of the source's chain for dispatch, tracing, and
    :class:`Token` scoping purposes.
    """
    return current_context().copy()


def install_task_factory(
    loop: asyncio.AbstractEventLoop | None = None,
) -> None:
    """Install Wool's task factory on the given (or running) loop.

    Composes with an existing factory if one is set, so that
    asyncio child tasks created via ``create_task`` inherit a
    forked :class:`Context`. Idempotent — a subsequent call on a
    loop that already has the Wool-wrapped factory installed is a
    no-op. :func:`current_context` self-installs the factory on
    first contact, so user code that touches Wool's API without
    first calling :func:`install_task_factory` still gets fork-on-
    task semantics for tasks created after the first contact.

    **Ordering contract** — If a user installs their own task factory
    *after* Wool's, Wool's wrapping of child coroutines is dropped
    and copy-on-fork breaks silently for subsequently-created tasks.
    Install Wool's factory last (or compose manually) when other
    libraries also want a factory on the same loop.
    """
    if loop is None:
        loop = asyncio.get_running_loop()

    existing = loop.get_task_factory()
    if existing is not None and getattr(existing, "__wool_wrapped__", False):
        _log.debug(f"wool-composed task factory already installed on {loop}")
        return
    inner = existing if existing is not None else _default_task_factory

    def wool_factory(
        loop: asyncio.AbstractEventLoop,
        coro: Coroutine[Any, Any, Any],
        *,
        context: Context | contextvars.Context | None = None,
        **kwargs: Any,
    ) -> asyncio.Task[Any]:
        if isinstance(context, Context):
            # Explicit wool.Context: hide it from asyncio (which would
            # call ctx.run(step_fn) per step and fragment _guard's
            # held-across-awaits semantics into per-step entries) and
            # instead wrap the coroutine so the guard + attach span
            # the whole routine. ``_wool_scoped`` installs the
            # :class:`Context` in the registry under the new task's
            # identity for the routine's lifetime.
            task = inner(loop, _wool_scoped(context, coro), **kwargs)
            _register(task, context)  # pyright: ignore[reportArgumentType]
            return task  # pyright: ignore[reportReturnType]
        # No wool.Context: forward stdlib ``context=`` if supplied
        # (so a third-party-supplied :class:`contextvars.Context` is
        # honored verbatim) and fork the parent's wool.Context at
        # task creation. The new task is registered with a fork (or
        # a fresh Context if no parent is bound), so wool inherits
        # the parent's bindings under a fresh chain id without
        # depending on stdlib Context boundaries.
        if context is not None:
            kwargs["context"] = context
        task = inner(loop, coro, **kwargs)
        child = (
            parent.copy()
            if (parent := context_registry.get()) is not None
            else Context()
        )
        _register(task, child)  # pyright: ignore[reportArgumentType]
        return task  # pyright: ignore[reportReturnType]

    wool_factory.__wool_wrapped__ = True  # pyright: ignore[reportFunctionMemberAccess]
    wool_factory.__wool_inner__ = inner  # pyright: ignore[reportFunctionMemberAccess]
    loop.set_task_factory(wool_factory)
    if existing is None:
        _log.debug(f"wool task factory installed on {loop}")
    else:
        _log.debug(
            f"wool task factory composed with existing factory {existing} on {loop}",
        )


# public
def create_task(
    coro: Coroutine[Any, Any, T],
    *,
    name: str | None = None,
    context: Context | None = None,
) -> asyncio.Task[T]:
    """Create a task on the running loop, optionally pre-bound to a
    :class:`wool.Context`.

    Mirrors :func:`asyncio.create_task` — uses the running loop, no
    explicit loop parameter, and accepts the same ``name``/``context``
    keywords in the same order. asyncio's stdlib ``context=`` kwarg
    is typed for :class:`contextvars.Context`, and :class:`wool.Context`
    cannot subclass it (the stdlib C type disallows subclassing); this
    helper hides the cast so callers do not need
    ``# pyright: ignore[reportArgumentType]`` at every call site. The wool task
    factory's interception of wool-Context-typed ``context`` is what
    actually does the binding — it wraps the coroutine so the
    single-task guard is held continuously across awaits and pins
    the :class:`Context` to the new task in the process-wide
    registry.

    Calling :func:`asyncio.create_task` directly with
    ``context=wool_ctx`` is functionally identical when wool's task
    factory is installed on the running loop; this helper exists
    purely as a typing shim. To schedule on a non-running loop, call
    :meth:`AbstractEventLoop.create_task` directly.

    :param coro:
        The coroutine to run.
    :param name:
        Optional task name (forwarded to the factory).
    :param context:
        Optional :class:`wool.Context` to bind. When supplied, the
        wool task factory wraps *coro* so :meth:`Context._guard` is
        held across the task's lifetime; concurrent attempts to enter
        the same Context from another task raise :class:`RuntimeError`
        immediately. When ``None``, the new task inherits a fork of
        the parent's :class:`wool.Context` via the factory's copy-on-
        fork path.
    :returns:
        The freshly created :class:`asyncio.Task`.
    """
    # stdlib's create_task is typed for contextvars.Context; the wool
    # factory accepts a duck-typed wool.Context.
    return asyncio.create_task(coro, name=name, context=context)  # pyright: ignore[reportArgumentType]


def _register(task: asyncio.Task[Any], ctx: Context) -> None:
    """Pin *ctx* to *task* in the process-wide :data:`context_registry`.

    Enforces the one-shot contract — a task is bound exactly once
    at creation — so duplicate bindings surface immediately as
    :class:`ContextAlreadyBound` rather than silently stomping
    prior state.

    :raises ContextAlreadyBound:
        If *task* is already bound to a :class:`Context` (one-shot
        contract — see :class:`ContextAlreadyBound`).
    """
    with lock:
        if task in context_registry:
            raise ContextAlreadyBound(
                f"task {task!r} is already bound to {context_registry[task]!r}"
            )
        context_registry[task] = ctx


async def _wool_scoped(ctx: Context, coro: Coroutine[Any, Any, T]) -> T:
    """Run *coro* with *ctx* attached, the single-task guard held,
    and the Context's ``_bound_task`` slot pinned to the current
    task for the coroutine's lifetime.

    Implements the "first-task-wins for the routine's lifetime"
    binding for ``loop.create_task(coro, context=wool_ctx)``: a
    second task targeting the same Context while the first is
    still mid-flight raises before acquiring :meth:`_guard`,
    catching the cross-task interleaving that :meth:`_guard`'s
    ``_running`` flag alone could miss for concurrent attempts
    spread across asyncio loop ticks. Holds :meth:`_guard`
    continuously across the coroutine's awaits so yield frames
    cannot be interleaved by another task.
    """
    current = asyncio.current_task()
    if current is not None:
        with ctx._lock:
            if ctx._bound_task is not None:
                bound = ctx._bound_task()
                if bound is not None and bound is not current and not bound.done():
                    # Close the un-awaited coroutine to suppress the
                    # "coroutine was never awaited" RuntimeWarning that
                    # would otherwise leak at GC.
                    coro.close()
                    raise RuntimeError(
                        "wool.Context is bound to another live task; "
                        "first-task-wins for the routine's lifetime"
                    )
            ctx._bound_task = weakref.ref(current)
    try:
        with attached(ctx):
            return await coro
    finally:
        if current is not None:
            with ctx._lock:
                if ctx._bound_task is not None and ctx._bound_task() is current:
                    ctx._bound_task = None


def _default_task_factory(
    loop: asyncio.AbstractEventLoop,
    coro: Coroutine[Any, Any, Any],
    **kwargs: Any,
) -> asyncio.Task[Any]:
    """Fall-back task factory matching :meth:`AbstractEventLoop.create_task`.

    Used when no user factory is installed, so wool's factory has
    a uniform inner layer to delegate to.
    """
    return asyncio.Task(coro, loop=loop, **kwargs)


def _ensure_task_factory_installed() -> None:
    """Self-install Wool's task factory on the running loop if absent.

    Lets user code that touches Wool without first calling
    :func:`install_task_factory` still get fork-on-task semantics
    for tasks created after the first Wool API contact. No-ops in
    sync contexts (no running loop). The :data:`_loops_with_factory`
    weak set short-circuits the lookup to a single membership check
    after the first install on a given loop.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    if loop in _loops_with_factory:
        return
    install_task_factory(loop)
    _loops_with_factory.add(loop)
