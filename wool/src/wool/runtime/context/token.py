from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import ClassVar
from typing import Generic
from typing import NoReturn
from typing import SupportsIndex
from typing import TypeVar
from uuid import UUID
from uuid import uuid4

from wool.runtime.context.registry import context_registry
from wool.runtime.context.registry import token_registry
from wool.runtime.context.registry import var_registry
from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

if TYPE_CHECKING:
    from wool.runtime.context.base import Context
    from wool.runtime.context.var import ContextVar

T = TypeVar("T")


# public
class Token(Generic[T]):
    """Picklable token for reverting a :class:`ContextVar` mutation.

    Mirrors :class:`contextvars.Token`: single-use, same-var
    rejection, and scoped to the :class:`wool.Context` in which it
    was created. Attempting to :meth:`ContextVar.reset` with a token
    minted in a different :class:`wool.Context` raises
    :class:`ValueError`. Same-Context identity is checked by
    comparing the :class:`wool.Context` UUID — the canonical chain
    identity that holds across in-process and cross-process
    boundaries.

    On construction the token registers itself in the process-wide
    :data:`token_registry` so a same-process pickle round-trip
    resolves back to this instance and wire-supplied consumed-token
    UUIDs can be matched against live tokens for used-flag promotion.

    :param var:
        The :class:`ContextVar` whose mutation this token reverts.
        Only its key is captured — the var instance itself is
        resolved on demand at :meth:`ContextVar.reset` time via
        :data:`var_registry`.
    :param old_value:
        The value to restore on :meth:`ContextVar.reset`.
        :data:`~wool.runtime.typing.Undefined` (also exposed as
        :attr:`Token.MISSING`) signals the var was unset before
        the corresponding :meth:`ContextVar.set`, so reset pops
        the var from the :class:`Context`'s data rather than
        restoring a value.
    :param context:
        The :class:`wool.Context` in which the corresponding
        :meth:`ContextVar.set` ran. Only its UUID is retained —
        the live Context reference is intentionally not held so
        long-lived tokens do not pin their originating Context.
    """

    __slots__ = (
        "_id",
        "_key",
        "_old_value",
        "_context_id",
        "_used",
        "__weakref__",
    )

    _id: UUID
    _key: tuple[str, str]
    _old_value: T | UndefinedType
    _context_id: UUID
    _used: bool

    MISSING: ClassVar[UndefinedType] = Undefined

    def __init__(
        self,
        var: ContextVar[T],
        old_value: T | UndefinedType,
        context: Context,
    ):
        self._id = uuid4()
        self._key = var._key
        self._old_value = old_value
        self._context_id = context._id
        self._used = False
        token_registry[self._id] = self

    def __wool_reduce__(self) -> tuple[Callable[..., Token[T]], tuple[Any, ...]]:
        """Return constructor args for unpickling via Wool's pickler.

        Token's transport carries identity (``_id``, ``_key``,
        ``_context_id``) plus the consumed-state bit and old value
        needed for cross-process reset. Token is guarded against
        vanilla pickling (see :meth:`__reduce_ex__`); this method
        is invoked only by Wool's own pickler.
        """
        return (
            Token._reconstitute,
            (
                self._id,
                self._key,
                self._old_value,
                self._context_id,
                self._used,
            ),
        )

    def __reduce_ex__(self, _protocol: SupportsIndex) -> NoReturn:
        """Reject vanilla pickling.

        Token reset semantics are scoped to a live :class:`wool.Context`
        and the process-wide :data:`token_registry`, neither of which
        is reconstructible from a vanilla pickle taken outside the
        dispatch path. Wool's own pickler consults
        ``reducer_override`` (and therefore :meth:`__wool_reduce__`)
        before ``__reduce_ex__``, so this guard is invisible to
        Wool's serialization.

        :func:`copy.copy` and :func:`copy.deepcopy` also route
        through ``__reduce_ex__`` and are rejected for the same
        reason — a registry-bound Token has no meaningful copy
        semantics.

        :raises TypeError:
            Always.
        """
        raise TypeError(
            "wool.Token cannot be pickled via vanilla pickle/cloudpickle; "
            "it is serialized automatically when dispatched through Wool's "
            "runtime."
        )

    def __repr__(self) -> str:
        used_marker = " used" if self._used else ""
        return f"<wool.Token var={self._key!r}{used_marker}>"

    @property
    def id(self) -> UUID:
        """The UUID that identifies this :class:`Token` on the wire."""
        return self._id

    @property
    def var(self) -> ContextVar[T]:
        """Return the :class:`ContextVar` this token was created for.

        Resolves the stored key against the process-wide
        :data:`var_registry`. Raises :class:`KeyError` if the key
        is not registered locally — typically a cross-process token
        whose owning :class:`ContextVar` was never declared on this
        side; the caller can declare the var first or use the
        wire-snapshot ingress path that pins a stub eagerly.
        """
        return var_registry[self._key]

    @property
    def old_value(self) -> T | UndefinedType:
        """The prior value the var held before the :meth:`ContextVar.set`
        call that produced this token. Returns :attr:`Token.MISSING`
        if the var had no value set.

        :attr:`Token.MISSING` is a singleton; check for it via
        identity (``token.old_value is Token.MISSING``) rather than
        :func:`isinstance` since the underlying sentinel type is
        internal and not part of the public API.
        """
        return self._old_value

    @property
    def used(self) -> bool:
        """Whether this :class:`Token` has been consumed by :meth:`ContextVar.reset`.

        ``True`` once a successful :meth:`ContextVar.reset` call has
        passed this token, whether the call occurred in this process
        or in another process running in a :class:`Context` whose
        used-token state has since been merged back (via pickle
        round-trip or back-propagation into this token via the
        process-wide token registry). Tokens are single-use across
        the logical chain; a second :meth:`ContextVar.reset` raises
        :class:`RuntimeError`.
        """
        return self._used

    @staticmethod
    def _promote_external_used(
        active: Context | None, instance: Token[T], token_id: UUID
    ) -> None:
        """Migrate *token_id* from :attr:`Context._external_used_tokens`
        into :attr:`Context._used_tokens` when *active* carries it under
        the former.

        The active :class:`Context`'s external map is authoritative for
        consumed-state in this chain — a wire-supplied entry only lands
        there when an upstream :meth:`Context.to_protobuf` listed it
        under ``consumed_tokens``. Promoting the entry into the auto-
        pruning :class:`weakref.WeakSet` engages lifetime cleanup once
        the live :class:`Token` is reclaimed, and brings :attr:`_used`
        up to date even when the pickle's wire bit is stale.
        """
        if active is None:
            return
        if token_id not in active._external_used_tokens:
            return
        if not instance._used:
            instance._used = True
        active._external_used_tokens.pop(token_id, None)
        active._used_tokens.add(instance)

    @staticmethod
    def _sync_state(token: Token[T], wire_used: bool) -> None:
        """Monotonically advance *token*'s ``_used`` flag to match a
        wire payload's used flag.

        :attr:`Token._used` is a one-way bit (``False → True``); a
        wire payload reporting ``used=True`` for an id whose
        registry instance still reads ``_used=False`` indicates the
        token was consumed in a snapshot taken after the registry
        instance was last updated. Bringing the live flag up to
        date keeps the registry coherent across out-of-order pickle
        round-trips — notably, the case where a user pickles a
        token before and after reset, the original is GC'd, and the
        older snapshot is unpickled first (registering a stub with
        ``_used=False``) before the newer snapshot is unpickled.
        Without this catch-up the registry would stay at
        ``_used=False`` and a subsequent :meth:`ContextVar.reset`
        would succeed against an already-consumed token.

        In the dispatch pipeline this is typically a no-op because
        :meth:`Context.update` runs before result decode and flips
        ``_used`` first via the wire's ``consumed_tokens`` field;
        by the time :meth:`_reconstitute` sees a token, the
        registry instance already reflects the consumed state.
        """
        if wire_used and not token._used:
            token._used = True

    @classmethod
    def _reconstitute(
        cls,
        token_id: UUID,
        key: tuple[str, str],
        old_value: T | UndefinedType,
        context_id: UUID,
        used: bool,
    ) -> Token[T]:
        """Rebuild a :class:`Token` from externally-supplied parts.

        Same-process pickle identity is preserved via
        :data:`token_registry`: if a live :class:`Token` already exists
        under *token_id*, it is returned with its ``_used`` flag synced
        via :meth:`_sync_state` so an out-of-order pickle round-trip
        does not strand the registry at a stale ``_used=False``.
        Cross-process callers see a fresh stub whose ``_used`` flag
        is adopted from the pickled tuple. The token stores the var
        *key* directly; the owning :class:`ContextVar` is resolved
        on demand via the :attr:`var` property.

        When the active :class:`Context` carries this token's UUID
        in :attr:`Context._external_used_tokens` — typically because
        the consumed UUID arrived on an earlier wire frame before
        the Token instance itself — the fresh instance is promoted
        into :attr:`Context._used_tokens` so subsequent emissions
        and merges go through the auto-pruning weak-set path.
        """
        existing: Token[T] | None = token_registry.get(token_id)
        if existing is not None:
            cls._sync_state(existing, used)
            instance = existing
        else:
            candidate: Token[T] = object.__new__(cls)
            candidate._id = token_id
            candidate._key = key
            candidate._old_value = old_value
            candidate._context_id = context_id
            candidate._used = used
            # Single-instance registry claim. Single-threaded
            # asyncio per worker plus the single-task-per-Context
            # invariant means only one task unpickles a given
            # token's bytes at a time, so concurrent ``setdefault``
            # calls cannot occur in steady state. ``setdefault`` is
            # used over plain assignment to express second-caller-
            # wins (first candidate wins the slot; later callers
            # receive the winner). Note that
            # ``WeakValueDictionary.setdefault`` is not GIL-atomic
            # — its check-then-insert is pure Python — so coherence
            # rests on the architectural invariant rather than on
            # dict atomicity.
            instance = token_registry.setdefault(token_id, candidate)
            if instance is not candidate:
                cls._sync_state(instance, used)
        cls._promote_external_used(context_registry.get(), instance, token_id)
        return instance
