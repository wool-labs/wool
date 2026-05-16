from __future__ import annotations

import asyncio
import contextlib
import contextvars
import threading
import warnings
import weakref
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from types import MappingProxyType
from typing import TYPE_CHECKING
from typing import Any
from typing import Final
from typing import Iterator
from typing import Mapping
from uuid import UUID
from uuid import uuid4

import wool
from wool import protocol
from wool.runtime.context.base import ContextDecodeWarning
from wool.runtime.context.registry import token_registry
from wool.runtime.context.registry import var_registry
from wool.runtime.context.stub import resolve_stub

if TYPE_CHECKING:
    from wool.runtime.context.var import ContextVar
    from wool.runtime.serializer import Serializer


@dataclass(frozen=True, eq=False)
class Snapshot:
    """Immutable snapshot of Wool chain state.

    Wool chain state — the map of :class:`wool.ContextVar` bindings,
    the logical-chain UUID, and the consumed-token log — rides in a
    single Wool-owned stdlib :class:`contextvars.ContextVar` as one
    of these snapshots. :meth:`wool.ContextVar.get` reads its key
    from the snapshot; :meth:`~wool.ContextVar.set` and
    :meth:`~wool.ContextVar.reset` rebuild the snapshot and reinstall
    it.

    The snapshot is immutable, so it can be shared by reference
    across event-loop callbacks, timers, and child tasks without
    intercepting the loop: a callback that mutates a variable
    produces a *new* snapshot scoped to its own
    :class:`contextvars.Context` copy, leaving the scheduling scope's
    snapshot untouched.

    :param chain_id:
        UUID identifying the logical execution chain. Minted when a
        context is first armed (the first :meth:`wool.ContextVar.set`)
        and freshly re-minted on every task fork by the Wool task
        factory.
    :param owner:
        :func:`threading.get_ident` of the OS thread that owns this
        chain. The concurrent-entry guard compares it against the
        accessing thread; see :class:`wool.ConcurrentChainEntry`.
    :param owner_task:
        Weak reference to the :class:`asyncio.Task` that owns this
        chain, or ``None`` when the chain was armed outside any task
        (synchronous code, or a :func:`wool.to_thread` worker thread).
        The concurrent-entry guard compares it against the running
        task so a second task entering the chain fails loud. This
        field is process-local and never crosses the wire.
    :param data:
        Map of :class:`wool.ContextVar` to its current value.
    :param consumed:
        Consumed-token log — :class:`wool.Token` UUID to the
        ``(namespace, name)`` key of the variable it reset. Carries
        the "this variable was reset" signal across worker
        boundaries.
    :param stub_pins:
        Undeclared-stub :class:`wool.ContextVar` instances observed
        while decoding a wire snapshot, held strongly so a lazy-import
        receiver can still promote them when it declares the variable.
    """

    chain_id: UUID
    owner: int
    owner_task: weakref.ref[asyncio.Future[Any]] | None = field(default=None)
    data: Mapping["ContextVar[Any]", Any] = field(default_factory=dict)
    consumed: Mapping[UUID, tuple[str, str]] = field(default_factory=dict)
    stub_pins: frozenset["ContextVar[Any]"] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        # ``frozen=True`` blocks attribute rebinding but not in-place
        # mutation of the underlying dicts. Wrap them so the
        # immutability the class promises is physical: a snapshot
        # shared by reference across callbacks and tasks cannot be
        # corrupted through an aliased dict reference.
        object.__setattr__(self, "data", MappingProxyType(dict(self.data)))
        object.__setattr__(self, "consumed", MappingProxyType(dict(self.consumed)))

    def evolve(self, **changes: Any) -> Snapshot:
        """Return a copy of this snapshot with *changes* applied."""
        return replace(self, **changes)


# The single Wool-owned stdlib variable that carries Wool chain
# state. ``None`` (the default) means *unarmed*: the surrounding
# context carries no Wool state and behaves as a plain
# :class:`contextvars.Context` — no chain UUID, no guard, no fork
# bookkeeping. The first :meth:`wool.ContextVar.set` arms it.
_snapshot: Final[contextvars.ContextVar[Snapshot | None]] = contextvars.ContextVar(
    "_wool_snapshot", default=None
)


def current_snapshot() -> Snapshot | None:
    """Return the snapshot active in the current :class:`contextvars.Context`.

    ``None`` when the context is unarmed — no :class:`wool.ContextVar`
    has been set in it.
    """
    return _snapshot.get()


def install_snapshot(snapshot: Snapshot | None) -> contextvars.Token:
    """Install *snapshot* as the active snapshot, returning a restore token."""
    return _snapshot.set(snapshot)


@contextlib.contextmanager
def with_snapshot(snapshot: Snapshot | None) -> Iterator[None]:
    """Install *snapshot* as the active snapshot for the ``with`` block.

    Scoped install/restore. Used by worker-side plumbing that must
    run code — a backpressure hook reading caller-shipped
    :class:`wool.ContextVar` values — under a decoded snapshot
    without permanently arming the surrounding context.
    """
    token = _snapshot.set(snapshot)
    try:
        yield
    finally:
        _snapshot.reset(token)


def context_is_armed(context: contextvars.Context) -> bool:
    """Return ``True`` if *context* carries a non-``None`` Wool snapshot.

    A :class:`contextvars.Context` in which no :class:`wool.ContextVar`
    has been set never holds the Wool-owned snapshot variable at all; a
    context whose snapshot was installed and later restored to ``None``
    holds the variable with a ``None`` value. Both are *unarmed* — they
    behave as a plain :class:`contextvars.Context`.
    """
    return _snapshot in context and context[_snapshot] is not None


def _current_owner_ref() -> weakref.ref[asyncio.Future[Any]] | None:
    """Return a weak reference to the running :class:`asyncio.Task`.

    ``None`` when there is no running event loop (synchronous code, or
    a :func:`wool.to_thread` worker thread) or when no task is running
    on the loop (a bare event-loop callback). The concurrent-entry
    guard treats a ``None`` owner task as "no task-level guard" and
    falls back to the thread-owner check.
    """
    try:
        task = asyncio.current_task()
    except RuntimeError:
        return None
    if task is None:
        return None
    return weakref.ref(task)


def fork_snapshot(snapshot: Snapshot) -> Snapshot:
    """Fork *snapshot* into a fresh logical chain.

    The fork inherits the variable bindings and stub pins but mints a
    new ``chain_id``, adopts the calling thread as ``owner``, and
    adopts the running task — if any — as ``owner_task``. The
    consumed-token log is dropped — a :class:`wool.Token` minted in
    the parent chain is already incompatible with the fork's chain
    for :meth:`wool.ContextVar.reset` purposes, so the fork starts
    with a clean log. This is the copy-on-fork the task factory
    applies at every task creation.
    """
    return Snapshot(
        chain_id=uuid4(),
        owner=threading.get_ident(),
        owner_task=_current_owner_ref(),
        data=dict(snapshot.data),
        consumed={},
        stub_pins=snapshot.stub_pins,
    )


def snapshot_has_state(snapshot: Snapshot | None) -> bool:
    """Return ``True`` if *snapshot* carries observable wire state.

    Wire-side callers use this to skip no-op merges from empty wire
    frames — a snapshot with neither variable bindings nor consumed
    tokens has nothing to propagate.
    """
    return snapshot is not None and (bool(snapshot.data) or bool(snapshot.consumed))


def encode_snapshot(
    snapshot: Snapshot | None,
    *,
    serializer: Serializer | None = None,
) -> protocol.Context:
    """Encode *snapshot* to a wire :class:`protocol.Context`.

    Each variable observable in the snapshot — by carrying a value or
    by being the source of a consumed token — emits one
    :class:`protocol.ContextVar` entry. The entry's ``value`` is set
    when the variable has a current binding and unset otherwise (a
    variable reset to no prior value still rides the wire so its
    consumed-token IDs propagate). Default-only variables are absent.

    A ``None`` snapshot (unarmed context) encodes to an empty
    :class:`protocol.Context`.

    Per-variable encode failures emit :class:`ContextDecodeWarning`
    and the offending key is skipped. Under strict mode
    (``PYTHONWARNINGS=error::wool.ContextDecodeWarning``) the
    failures aggregate into a single :class:`BaseExceptionGroup`
    raised after the loop.

    :raises BaseExceptionGroup:
        Under strict mode, when one or more variables fail to encode.
    """
    if serializer is None:
        serializer = wool.__serializer__
    if snapshot is None:
        return protocol.Context()
    wire = protocol.Context(id=snapshot.chain_id.hex)
    failures: list[ContextDecodeWarning] = []
    encoded_values: dict[tuple[str, str], bytes] = {}
    failed_keys: set[tuple[str, str]] = set()
    for var, value in snapshot.data.items():
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
    for token_id, var_key in snapshot.consumed.items():
        # A variable whose value failed to serialize is suppressed
        # entirely — emitting consumed tokens without a value would
        # propagate a phantom reset on the receiver, which would
        # interpret the half-encoded entry as "reset and not re-set".
        if var_key in failed_keys:
            continue
        token_ids_by_key.setdefault(var_key, []).append(token_id.hex)
    for var_key in set(encoded_values).union(token_ids_by_key):
        namespace, name = var_key
        entry = wire.vars.add(namespace=namespace, name=name)
        if var_key in encoded_values:
            entry.value = encoded_values[var_key]
        if var_key in token_ids_by_key:
            entry.consumed_tokens.extend(token_ids_by_key[var_key])
    if failures:
        raise BaseExceptionGroup(
            "wool context encode failed for one or more vars",
            failures,
        )
    return wire


def decode_snapshot(
    wire: protocol.Context,
    *,
    serializer: Serializer | None = None,
) -> Snapshot:
    """Decode a wire :class:`protocol.Context` into a :class:`Snapshot`.

    Walks ``wire.vars`` once: each variable identity resolves through
    the process-wide :class:`wool.ContextVar` registry (or registers
    a stub if undeclared, pinned into the resulting snapshot), the
    optional serialized value is deserialized, and any consumed-token
    IDs the entry carries are recorded in the snapshot's consumed log
    and flip the matching live :class:`wool.Token` to consumed.

    Decode failures emit :class:`ContextDecodeWarning` and the
    offending entry is skipped — surviving entries decode normally. A
    malformed wire chain ID falls back to a fresh UUID. Under strict
    mode the failures aggregate into a single
    :class:`BaseExceptionGroup` raised after the decode loop.

    :raises BaseExceptionGroup:
        Under strict mode, when one or more entries fail to decode.
    """
    if serializer is None:
        serializer = wool.__serializer__
    failures: list[ContextDecodeWarning] = []
    try:
        chain_id = UUID(hex=wire.id) if wire.id else uuid4()
    except ValueError as e:
        try:
            warnings.warn(
                f"Failed to decode wire context id {wire.id!r}: {e}",
                ContextDecodeWarning,
                stacklevel=2,
            )
        except ContextDecodeWarning as raised:
            failures.append(raised)
        chain_id = uuid4()
    data: dict[ContextVar[Any], Any] = {}
    consumed: dict[UUID, tuple[str, str]] = {}
    stub_pins: set[ContextVar[Any]] = set()
    failed_keys: set[tuple[str, str]] = set()
    for entry in wire.vars:
        var_key = (entry.namespace, entry.name)
        var = resolve_stub(var_key)
        if var._stub:
            stub_pins.add(var)
        if entry.HasField("value"):
            try:
                data[var] = serializer.loads(entry.value)
            except Exception as e:
                failed_keys.add(var_key)
                try:
                    warnings.warn(
                        f"Failed to deserialize wool.ContextVar {var_key!r}: {e}",
                        ContextDecodeWarning,
                        stacklevel=2,
                    )
                except ContextDecodeWarning as raised:
                    failures.append(raised)
        if var_key in failed_keys:
            # A variable whose value failed to deserialize is absent
            # from ``data``; recording its consumed tokens would let a
            # subsequent merge read the half-decoded entry as a
            # phantom reset and drop a live binding on the receiver.
            # Mirrors encode_snapshot's failed-key suppression above.
            continue
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
            # Flip any live Token instance to consumed so a later
            # in-process reset against it cannot double-reset, and
            # record the id in the consumed log for onward wire
            # propagation.
            live = token_registry.get(token_id)
            if live is not None and not live._used:
                live._used = True
            consumed[token_id] = var_key
    if failures:
        raise BaseExceptionGroup(
            "wool context decode failed",
            failures,
        )
    # ``owner_task`` is intentionally left at its ``None`` default: a
    # decoded wire snapshot has no owning task in this process. The
    # worker install (DispatchSession's worker driver) and
    # merge_snapshot stamp the owning task when they adopt the decoded
    # snapshot into a live chain.
    return Snapshot(
        chain_id=chain_id,
        owner=threading.get_ident(),
        data=data,
        consumed=consumed,
        stub_pins=frozenset(stub_pins),
    )


def merge_snapshot(incoming: Snapshot) -> None:
    """Merge *incoming* into the active snapshot and reinstall it.

    One-way: *incoming* is the source of truth for overlapping keys.
    The active snapshot's ``chain_id`` and ``owner`` are unchanged.
    Beyond a :meth:`dict.update`-style merge of the variable map this
    propagates resets: a variable consumed by *incoming* and absent
    from *incoming*'s data — reset and not subsequently re-set — is
    removed from the active snapshot so the merge carries the reset
    signal, not just the post-set state.

    When the active context is unarmed, this arms it on a freshly
    minted chain so subsequent :meth:`wool.ContextVar.get` calls
    observe the merged state.
    """
    current = current_snapshot()
    if current is None:
        # Arm the receiving context on a fresh chain — the merge is
        # one-way data, not a chain handoff — and self-install the
        # task factory so tasks forked afterwards inherit the state.
        from wool.runtime.context.factory import ensure_task_factory_installed

        ensure_task_factory_installed()
        current = Snapshot(
            chain_id=uuid4(),
            owner=threading.get_ident(),
            owner_task=_current_owner_ref(),
        )
    new_data = {**current.data, **incoming.data}
    sender_data_keys = {var._key for var in incoming.data}
    consumed_keys = set(incoming.consumed.values())
    for key in consumed_keys - sender_data_keys:
        receiver_var = var_registry.get(key)
        if receiver_var is not None:
            new_data.pop(receiver_var, None)
    for token_id in incoming.consumed:
        live = token_registry.get(token_id)
        if live is not None and not live._used:
            live._used = True
    merged = current.evolve(
        data=new_data,
        consumed={**current.consumed, **incoming.consumed},
        stub_pins=current.stub_pins | incoming.stub_pins,
    )
    _snapshot.set(merged)
