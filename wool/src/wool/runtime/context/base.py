from __future__ import annotations

import asyncio
import contextvars
import threading
import warnings
import weakref
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from typing import TYPE_CHECKING
from typing import Any
from typing import Final
from uuid import UUID
from uuid import uuid4

import wool
from wool import protocol
from wool.runtime.context.registry import var_registry
from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

if TYPE_CHECKING:
    from wool.runtime.context.manifest import _ContextManifest
    from wool.runtime.context.var import ContextVar
    from wool.runtime.serializer import Serializer


# Re-export the unified serialization hierarchy at this module's
# legacy import paths for back-compat. New code should import from
# :mod:`wool.runtime.context.errors`.
from wool.runtime.context.errors import ContextDecodeError  # noqa: F401
from wool.runtime.context.errors import ContextDecodeWarning  # noqa: F401
from wool.runtime.context.errors import ContextSerializationError  # noqa: F401
from wool.runtime.context.errors import SerializationError  # noqa: F401
from wool.runtime.context.errors import SerializationWarning  # noqa: F401
from wool.runtime.context.errors import WoolError  # noqa: F401

# Q9 — ``RuntimeContext`` and ``dispatch_timeout`` moved to
# :mod:`wool.runtime.context.runtime`. ``_apply_manifest`` and
# ``current_wire_context`` moved to :mod:`wool.runtime.context.wire`.
# This module retains the :class:`Context` chain-state model and the
# ``_install_context`` / ``current_context`` / ``_current_owner_ref`` /
# ``context_is_armed`` helpers — everything that touches the
# Wool-owned ``__wool_context__`` variable directly.
from wool.runtime.context.runtime import RuntimeContext  # noqa: F401
from wool.runtime.context.runtime import dispatch_timeout  # noqa: F401


@dataclass(frozen=True, eq=False)
class Context:
    """Immutable index of Wool chain state.

    Wool chain state — the set of bound :class:`wool.ContextVar`
    instances, the logical-chain UUID, and the reset-variable signals -
    rides in a single Wool-owned :class:`contextvars.ContextVar` as one
    of these contexts. The context is an *index*, not a value store:
    each :class:`wool.ContextVar`'s value lives in its own backing
    :class:`contextvars.ContextVar`, and ``data`` records the set of
    bound :class:`wool.ContextVar` instances.
    :class:`wool.ContextVar`'s ``get``, ``set``, and ``reset`` methods
    route through the backing variable. The context tracks which
    variables are bound and carries the cross-process reset signals.

    Because the values live in the :class:`contextvars.Context` rather
    than in the Wool context, callback write-isolation rests on native
    :class:`contextvars` copy-on-write: a callback runs in a
    :func:`contextvars.copy_context` copy, so its backing-variable
    writes (and the new context it installs) stay in that copy.

    The Wool-owned :class:`contextvars.ContextVar` becomes a permanent
    member of any :class:`contextvars.Context` once that context is
    armed. An *armed* context additionally carries one backing
    variable per bound :class:`wool.ContextVar`, so a
    :func:`contextvars.copy_context` of an armed context enumerates
    ``1 + N`` Wool-owned variables. An *unarmed* context holds none of
    them and is indistinguishable from a plain
    :class:`contextvars.Context`.

    **Lifecycle.** A :class:`Context` instance is strictly *live*:
    fresh (constructed with no arguments, via :meth:`fork`, or via
    the private :meth:`_from_manifest` factory) or mounted (installed
    in a :class:`contextvars.Context` via :meth:`mount`). Decoded-
    but-unmounted wire state lives on
    :class:`~wool.runtime.context.manifest._ContextManifest`; :meth:`_update`
    is the entry point that merges a manifest into a live Context
    and mounts it. :meth:`mount` is the single transition from pure
    data → installed; it stamps ``_owning_thread`` / ``_owning_task``
    from the calling scope, drains any pending decoded values into
    backing variables, and installs Wool's task factory on the
    running loop.

    **Indexing asymmetry.** ``data`` keys on :class:`wool.ContextVar`
    instance identity (safe because the process-wide var registry
    enforces a singleton per ``(namespace, name)`` key); ``reset_vars``
    keys on the ``(namespace, name)`` tuple directly so the reset
    signal survives a wire round-trip without requiring the receiver
    to have declared the variable. The two indices reference the same
    logical concept under different key spaces — by design.

    :param chain_id:
        UUID identifying the logical execution chain. Defaults to a
        freshly minted UUID. Re-minted on every task fork by the Wool
        task factory; an explicit value lets a worker preserve the
        caller's chain id when arming on an inbound wire frame.
    :param _owning_thread:
        :func:`threading.get_ident` of the OS thread that owns this
        chain — stamped by :meth:`mount`. Defaults to ``0`` (no owner)
        so construction is pure data; :meth:`mount` is the single
        owner-stamping site. The chain-contention guard compares it
        against the accessing thread; see :class:`wool.ChainContention`.
    :param _owning_task:
        Weak reference to the :class:`asyncio.Task` that owns this
        chain — stamped by :meth:`mount`. Defaults to ``None`` (no
        owner). ``None`` also marks a chain armed outside any task
        (synchronous code, or a :func:`wool.to_thread` worker thread).
        The chain-contention guard compares it against the running
        task so a second task entering the chain fails loud. This
        field is process-local and never crosses the wire.
    :param data:
        Set of bound :class:`wool.ContextVar` instances. Membership
        is the index of "bound in this chain": ``X in data`` is
        equivalent to ``X._backing`` resolving to a
        non-:data:`~wool.runtime.typing.Undefined` value in the active
        :class:`contextvars.Context`.
    :param reset_vars:
        ``(namespace, name)`` keys of variables reset to no prior
        value and not since re-set. The token-independent "drop this
        variable" signal for :meth:`update`; survives even if the
        resetting token is collected before the reset propagates.
    :param stub_pins:
        Undeclared-stub :class:`wool.ContextVar` instances observed
        while decoding a wire context, held strongly so a lazy-import
        receiver can still promote them when it declares the variable.
    """

    chain_id: UUID = field(default_factory=uuid4)
    # ``repr=False`` / ``compare=False`` keep the owner stamps out of
    # debug output and equality — they are runtime bookkeeping, not
    # part of the chain's identity. ``init=True`` is retained so
    # tests and worker-side construction can still pass them.
    _owning_thread: int = field(default=0, repr=False, compare=False)
    _owning_task: weakref.ref[asyncio.Future[Any]] | None = field(
        default=None, repr=False, compare=False
    )
    data: frozenset["ContextVar[Any]"] = field(default_factory=frozenset)
    reset_vars: frozenset[tuple[str, str]] = field(default_factory=frozenset)
    stub_pins: frozenset["ContextVar[Any]"] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        # Defensive coercion: ensure every container field is bound to
        # its immutable wrapper or a defensively-copied mutable view,
        # regardless of what callers passed. ``dataclasses.replace``
        # and the constructor go through this hook, so a caller
        # passing a plain ``set`` / ``list`` / shared dict cannot
        # smuggle a mutable container past the frozen-dataclass
        # facade. Bypasses the frozen guard via ``object.__setattr__``,
        # standard for ``__post_init__`` on a ``frozen=True``
        # dataclass.
        if not isinstance(self.data, frozenset):
            object.__setattr__(self, "data", frozenset(self.data))
        if not isinstance(self.reset_vars, frozenset):
            object.__setattr__(self, "reset_vars", frozenset(self.reset_vars))
        if not isinstance(self.stub_pins, frozenset):
            object.__setattr__(self, "stub_pins", frozenset(self.stub_pins))

    def _evolve(self, **changes: Any) -> Context:
        """Return a copy of this context with *changes* applied.

        Private — internal-only. All callers live inside the wool
        runtime (Q23 rename from ``evolve``).
        """
        return replace(self, **changes)

    def to_protobuf(
        self,
        *,
        serializer: "Serializer | None" = None,
    ) -> protocol.Context:
        """Encode this context to a wire :class:`protocol.Context`.

        Each variable observable in the context — carrying a value or
        in a reset-to-no-value state — emits one
        :class:`protocol.ContextVar` entry. Default-only variables are
        absent.

        **Value source.** :class:`Context` is strictly live, so the
        entry's ``value`` is read from the variable's backing
        :class:`contextvars.ContextVar`. This method **must run inside
        the chain's** :class:`contextvars.Context` for the read to
        observe the chain's bindings. Decoded-but-unmounted wire state
        lives on :class:`~wool.runtime.context.manifest._ContextManifest`; that
        path does not call this method.

        A variable reset to no prior value still rides the wire, with
        no ``value``, so the reset propagates regardless of source.

        Per-variable encode failures emit :class:`ContextDecodeWarning`
        and the offending key is skipped. Under strict mode
        (``PYTHONWARNINGS=error::wool.ContextDecodeWarning``) the
        failures aggregate into a single :class:`ContextDecodeError`
        raised after the loop.

        Emitted ``wire.vars`` entries are unique by
        ``(namespace, name)`` by construction: ``self.data`` is a
        ``frozenset[ContextVar[Any]]`` of singletons keyed by
        ``(namespace, name)`` (enforced at construction via
        ``var_registry`` and :class:`ContextVarCollision`), and each
        instance contributes exactly one entry. Receivers MAY treat
        duplicate keys on the wire as undefined behaviour — Wool's
        encoder will never produce them.

        :raises ContextDecodeError:
            Under strict mode, when one or more variables fail to encode.
        """
        # Singleton-invariant check: every ContextVar in ``data`` is
        # uniquely keyed by ``(namespace, name)`` via ``var_registry`` +
        # ``ContextVarCollision``. A violation here would propagate as
        # duplicate ``wire.vars`` entries and silently drop one of the
        # entries on the receiver. Stripped under ``-O`` as an invariant
        # debug check; operators running ``-O`` have opted out of debug
        # assertions.
        assert len({(v.namespace, v.name) for v in self.data}) == len(self.data), (
            "singleton invariant violated: duplicate var keys in Context.data"
        )
        if serializer is None:
            serializer = wool.__serializer__
        wire = protocol.Context(id=self.chain_id.hex)
        failures: list[ContextDecodeWarning] = []
        encoded_values: dict[tuple[str, str], bytes] = {}
        failed_keys: set[tuple[str, str]] = set()
        for var in self.data:
            # Read from the active backing variable. ``Context`` is
            # strictly live, so ``data`` membership always resolves to a
            # backing value in the current
            # :class:`contextvars.Context`. The residual ``Undefined``
            # skip is defence against a divergence that can't happen by
            # construction.
            value = var._backing.get(Undefined)
            if value is Undefined:
                continue
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
        # A variable whose value failed to serialize is suppressed
        # entirely — emitting its reset signal without the value would
        # let the receiver read a phantom reset.
        reset_keys = {key for key in self.reset_vars if key not in failed_keys}
        # Sort the emission iteration so identical Contexts encode to
        # byte-identical wire frames across processes. Set iteration
        # order is hash-randomised; without ``sorted`` the same chain
        # state produces different ``wire.vars`` orderings on each
        # encode, defeating content-addressed caching and replay-style
        # fingerprinting.
        for var_key in sorted(set(encoded_values) | reset_keys):
            namespace, name = var_key
            entry = wire.vars.add(namespace=namespace, name=name)
            if var_key in encoded_values:
                entry.value = encoded_values[var_key]
        if failures:
            raise ContextDecodeError(*failures)
        return wire

    def mount(self) -> Context:
        """Mount this context into the current (owning task's) Context.

        Installs an evolved copy of this context — re-stamped so the
        calling thread and task own the chain — and applies each
        :attr:`reset_vars` signal by setting the matching backing to
        :data:`~wool.runtime.typing.Undefined`. Must run inside the
        owning task's real :class:`contextvars.Context` so backing-
        variable state and any native tokens minted afterward bind
        to it.

        ``mount`` is the single "make a non-manifest Context live"
        operation — the path used by :meth:`wool.ContextVar.set` on
        first-arm and by :meth:`wool.ContextVar.reset`. Wire-ingress
        installs route through :func:`_install_manifest` (in
        :mod:`wool.runtime.context.wire`) so the manifest-shaped
        drain/apply pipeline lives in one place; ``mount`` carries
        only the no-drain (no-manifest) install path.

        ``mount`` is also the single point at which Wool ensures its
        :func:`asyncio.create_task` factory is installed on the
        running loop: every code path that arms a chain transits
        through here. The install call is idempotent per loop (a
        :class:`weakref.WeakSet` membership check) and silently
        no-ops when called from synchronous code with no running
        loop.

        :returns:
            The installed Context — the evolved copy with owner
            stamps re-applied. The return value lets callers chain
            off the installed instance; existing callers in
            :class:`~wool.ContextVar` discard it.
        """
        # Self-install the task factory if it is not already on the
        # running loop. This is the keystone boundary for arming —
        # every code path that arms a chain via ``Context.mount``
        # transits through here. The local import breaks the
        # ``base ↔ factory`` cycle; the call is idempotent per-loop.
        from wool.runtime.context.factory import _ensure_task_factory_installed

        _ensure_task_factory_installed()
        # Apply reset signals: a variable in ``reset_vars`` was reset
        # to no prior value upstream, so its backing rewinds to the
        # Undefined sentinel. Idempotent for resets that the chain
        # already carried — those backings were already Undefined.
        for key in self.reset_vars:
            receiver_var = var_registry.get(key)
            if receiver_var is not None:
                receiver_var._backing.set(Undefined)
        installed = self._evolve(
            _owning_thread=threading.get_ident(),
            _owning_task=_current_owner_ref(),
        )
        _install_context(installed)
        return installed

    def has_state(self) -> bool:
        """Return ``True`` if this context carries observable wire state.

        Wire-side callers use this to skip no-op merges from empty wire
        frames — a context with no variable bindings and no reset
        signals has nothing to propagate.
        """
        return bool(self.data) or bool(self.reset_vars)

    def _fork(self) -> Context:
        """Fork this context into a fresh logical chain.

        Private — internal-only. Called by ``_forked_scope`` and
        ``to_thread``; not part of the public surface (Q24 rename
        from ``fork``).

        The fork inherits the variable bindings (the ``data`` index)
        and stub pins but mints a new ``chain_id``, adopts the calling
        thread as ``_owning_thread``, and adopts the running task — if any —
        as ``_owning_task``. The reset-variable signals and cross-process
        used log are dropped: a :class:`wool.Token` minted in the
        parent chain is already incompatible with the fork's chain for
        :meth:`wool.ContextVar.reset`, so the fork starts clean. The
        backing variables' values ride the fork natively — the forked
        task runs in a :func:`contextvars.copy_context` copy. This is
        the copy-on-fork the task factory applies at every task
        creation.

        **Re-handoff is undefined behaviour.** A fork minted on one
        thread/task and then re-driven elsewhere — e.g. a
        :func:`wool.to_thread` worker's fork captured back into the
        loop thread and passed to :func:`asyncio.create_task` —
        is unsupported. It will fail loudly only at the next
        :class:`wool.ContextVar` access on the re-handed chain, via
        the chain-contention guard. Forks are intended to be owned and
        retired by the scope that created them.
        """
        return Context(
            chain_id=uuid4(),
            data=self.data,
            stub_pins=self.stub_pins,
        )


# The single Wool-owned stdlib variable that carries the Wool chain
# context. ``None`` (the default) means *unarmed*: the surrounding
# context carries no Wool state and behaves as a plain
# :class:`contextvars.Context`. The first :meth:`wool.ContextVar.set`
# arms it.
_context: Final[contextvars.ContextVar[Context | None]] = contextvars.ContextVar(
    "__wool_context__", default=None
)


def current_context() -> Context | None:
    """Return the context active in the current :class:`contextvars.Context`.

    ``None`` when the context is unarmed — no :class:`wool.ContextVar`
    has been set in it.
    """
    return _context.get()


def _install_context(context: Context | None) -> contextvars.Token:
    """Install *context* as the active Context, returning a restore token.

    Private — the public install entry point is :meth:`Context.mount`,
    which restamps owner, drains pending values, and ensures the task
    factory is installed. ``_install_context`` is the underlying
    primitive (a thin ``_context.set`` wrapper) reserved for test
    infrastructure that needs to inject a Context state without
    side effects.
    """
    return _context.set(context)


# Q9 — ``_apply_manifest`` and ``current_wire_context`` moved to
# :mod:`wool.runtime.context.wire`. Back-compat re-exports preserve
# the legacy ``wool.runtime.context.base`` import path while the
# wire-codec module owns the source.
from wool.runtime.context.wire import _apply_manifest  # noqa: F401, E402
from wool.runtime.context.wire import current_wire_context  # noqa: F401, E402


def context_is_armed(context: contextvars.Context) -> bool:
    """Return ``True`` if *context* carries a non-``None`` Wool context.

    A :class:`contextvars.Context` in which no :class:`wool.ContextVar`
    has been set never holds the Wool-owned context variable at all; a
    context whose Wool context was installed and later restored to
    ``None`` holds the variable with a ``None`` value. Both are
    *unarmed* — they behave as a plain :class:`contextvars.Context`.

    Distinct from ``current_context() is not None``: this predicate
    inspects an *explicit* :class:`contextvars.Context` (e.g., a
    ``copy_context()`` snapshot or a child task's materialised
    context), whereas :func:`current_context` reads the active
    :class:`contextvars.Context` in the calling scope. The two
    coincide when ``context`` is the currently-active one, but
    diverge whenever a caller holds a non-active reference (e.g.,
    the task factory inspecting a child's freshly copied context
    before scheduling it).
    """
    return _context in context and context[_context] is not None


def _current_owner_ref() -> weakref.ref[asyncio.Future[Any]] | None:
    """Return a weak reference to the running :class:`asyncio.Task`.

    ``None`` when there is no running event loop (synchronous code, or
    a :func:`wool.to_thread` worker thread) or when no task is running
    on the loop (a bare event-loop callback). The chain-contention
    guard treats a ``None`` owner task as "no task-level guard" and
    falls back to the thread-owner check.
    """
    # The two ``None`` cases are genuinely different runtime states
    # (no-loop vs loop-without-task) but the guard's behaviour for
    # them is identical, so collapsing them here is intentional.
    try:
        task = asyncio.current_task()
    except RuntimeError:
        return None
    else:
        return weakref.ref(task) if task else None
