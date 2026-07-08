"""The reset token minted by `~wool.runtime.context.var.ContextVar.set`.

Houses `Token`, the Wool counterpart to `contextvars.Token`, together
with the anchor (`anchor_tokens`) and collector (`token_sink`) machinery
that lets a token survive a Wool dispatch and reset on the receiver.
Unlike the stdlib token, a `wool.Token` may ride a `@wool.routine` dispatch
as an argument, kwarg, or return value (or nested inside a propagated
`~wool.runtime.context.var.ContextVar` value). Vanilla
``pickle``/``cloudpickle`` still reject it; only the dispatch path
serialises it.

The single-use enforcement and per-context reset semantics live on `wool.Token`
and `~wool.runtime.context.var.ContextVar.reset`. Cross-process use rides the
chain's ``spent_tokens`` ledger (see `~wool.runtime.context.chain.Chain`).

.. rubric:: Implementation notes

A token ID is recorded in the ``spent_tokens`` ledger on reset and propagated
on every frame; its retention is bounded to the lifespan of the `wool.Token`
instances carrying it, reaped by `~wool.runtime.context.chain.Chain.mount` once
none remains (see `~wool.runtime.context.chain.Chain`).
"""

from __future__ import annotations

import contextvars
import threading
import weakref
from collections.abc import Generator
from collections.abc import Iterable
from contextlib import contextmanager
from typing import TYPE_CHECKING
from typing import Any
from typing import Final
from typing import Generic
from typing import NoReturn
from typing import TypeVar

from wool.runtime.typing import UndefinedType

if TYPE_CHECKING:
    from wool.runtime.context.chain import Chain
    from wool.runtime.context.var import ContextVar


T = TypeVar("T")

_NO_TOKENS = frozenset()

#: Anchor for wire-reconstituted tokens. A live wire token, on the
#: receiver, mints a native stdlib token here inside its chain's context at
#: mount time; `~wool.runtime.context.var.ContextVar.reset` validates
#: through ``_token_anchor.reset(native)``, delegating "same Context as the
#: mount" to stdlib's own machinery. The value written (the token ID) is
#: never read — only the returned native token's context binding matters.
token_anchor: Final[contextvars.ContextVar[str]] = contextvars.ContextVar(
    "__wool_token_anchor__"
)

#: Wire tokens reconstituted within a `token_sink` context. ``_reconstitute``
#: runs deep in the cloudpickle arg/payload graph — a different context from the
#: eventual mount, with no handle on the frame — so it appends each live wire
#: token here rather than receiving a frame reference.
_token_sink: Final[contextvars.ContextVar[list["Token"] | None]] = (
    contextvars.ContextVar("__wool_token_sink__", default=None)
)

#: Count of live `Token` instances per ID, consulted by
#: `~wool.runtime.context.chain.Chain`'s ledger reap to bound ``spent_tokens``
#: to token lifespan (see `_register_token` and `dead_token_ids`).
_token_counts: Final[dict[str, int]] = {}

#: Serialises the read-modify-write on `_token_counts` so a register on one
#: thread and a `weakref.finalize` release on another (finalizers fire during
#: GC on whatever thread drops the token) cannot lose an increment — a lost
#: increment could zero a count early and reap an ID whose token is still live.
_token_lock: Final[threading.Lock] = threading.Lock()


@contextmanager
def token_sink() -> Generator[list[Token]]:
    """Collect the wire tokens reconstituted within the block into a fresh list.

    Wraps a single frame's decode: `Token.__wool_reduce__`'s reconstitution
    appends each live wire token to the yielded list, which the caller hands to
    the frame's mount for anchoring (see `anchor_tokens`).
    """
    collected: list[Token] = []
    reset = _token_sink.set(collected)
    try:
        yield collected
    finally:
        _token_sink.reset(reset)


def _register_token(token: Token) -> None:
    """Record *token* as a live instance of its ID for ledger reaping.

    Increments the ID's live-instance count and attaches a `weakref.finalize`
    that decrements it when this instance is collected. Counts each instance
    rather than a single canonical one, so coexisting copies of one ID (a
    returned clone alongside its origin, or a token both held and re-decoded)
    keep the ID live until the last is collected.
    """
    with _token_lock:
        _token_counts[token._id] = _token_counts.get(token._id, 0) + 1
    weakref.finalize(token, _release_token, token._id)


def _release_token(token_id: str) -> None:
    """Decrement *token_id*'s live-instance count, dropping the key at zero.

    The `weakref.finalize` callback `_register_token` attaches to each instance;
    it fires during garbage collection when that instance is reclaimed.
    """
    with _token_lock:
        remaining = _token_counts.get(token_id, 0) - 1
        if remaining > 0:
            _token_counts[token_id] = remaining
        else:
            _token_counts.pop(token_id, None)


def dead_token_ids(ids: Iterable[str]) -> frozenset[str]:
    """Return the *ids* whose tokens no longer live in this process.

    The reap oracle for `~wool.runtime.context.chain.Chain.mount`: an ID absent
    from `_token_counts` has no surviving instance, so nothing local can reset
    or forward it and its consumed-token entry can be dropped. Keeps the
    registry encapsulated here: callers pass the spent IDs and get back the
    dead ones.

    .. rubric:: Implementation notes

    The membership read needs no lock: a transient miscount can only retain an
    ID one mount longer, never reap one early.
    """
    return frozenset(id for id in ids if id not in _token_counts)


def anchor_tokens(wire_tokens: Iterable[Token], chain: Chain) -> None:
    """Mint anchors for the decoded wire tokens live on *chain*, in this context.

    Runs inside the chain's target context (the worker's cached context, or the
    caller's awaiting task) — where reset must be valid from. This is where the
    context-ownership gate is decided: a token earns an anchor, becoming
    resettable here, only if its ID is in the mounted chain's ``unspent_tokens``
    under the token's own var key, i.e., the manifest that carried it snapshots
    the context it was minted in. A token minted in a sibling context the sender
    does not continue is absent from that snapshot and is left an orphan
    (``_native`` unset), reclaimed with the frame.

    .. rubric:: Implementation notes

    The gate lives here rather than at serialization time because the payload may
    be pickled outside the context it ships (a worker serialises its result in the
    gRPC handler, not the routine's context). The manifest, captured inside the
    right context by ``to_manifest``, is the reliable witness. Idempotent: an
    already-bound token is skipped, so a streaming routine's per-frame re-mounts
    neither re-mint nor double-consume an anchor.
    """
    for token in wire_tokens:
        if token._native is None and token._id in chain.unspent_tokens.get(
            token._var._key, _NO_TOKENS
        ):
            token._native = token_anchor.set(token._id)


# public
class Token(Generic[T]):
    """Single-use authorization to `~wool.runtime.context.var.ContextVar.reset`.

    Returned by `~wool.runtime.context.var.ContextVar.set` and accepted only
    by that variable's `~wool.runtime.context.var.ContextVar.reset`. Direct
    construction raises `TypeError`; a token is minted only by `set` (and
    dispatch reconstitution). A `wool.Token` may be passed through a
    `@wool.routine` dispatch (arg, kwarg, return value, or nested in a
    propagated value) and reset on the receiver; passing, storing, returning,
    or re-dispatching a token in any state never raises. Only
    `~wool.runtime.context.var.ContextVar.reset` raises, and its
    `RuntimeError` / `ValueError` check order is documented there. Vanilla
    pickling is rejected; the token serialises only through Wool's dispatch
    pickler.
    """

    __slots__ = (
        "_var",
        "_old_value",
        "_native",
        "_id",
        "_used",
        "__weakref__",
    )

    _var: ContextVar[T]
    _old_value: T | UndefinedType
    _native: contextvars.Token[Any] | None
    _id: str
    _used: bool

    #: Sentinel returned by ``old_value`` when the variable had no prior value.
    # Aliased for stdlib muscle memory: ``tok.old_value is wool.Token.MISSING``
    # mirrors ``tok.old_value is contextvars.Token.MISSING``, though the value
    # is Wool's `~wool.runtime.typing.Undefined`, not stdlib's ``MISSING``.
    MISSING: Final = UndefinedType.Undefined

    def __init__(self, *_args: Any, **_kwargs: Any) -> NoReturn:
        """Reject direct construction; a `Token` is minted only by `set`.

        Mirrors `contextvars.Token`, which is likewise not user-constructible.

        :raises TypeError:
            Always.

        .. rubric:: Implementation notes

        A `Token`'s single-use authorization rests on wire and anchor state
        and the cross-process consumed-token ledger that only
        `~wool.runtime.context.var.ContextVar.set` (and dispatch
        reconstitution) populate correctly, so hand-construction — which
        could forge a token in any state and bypass that ledger — is refused.
        """
        raise TypeError("Tokens can only be created by wool.ContextVar.set")

    def __repr__(self) -> str:
        """Return a stdlib-shaped `Token` repr for canonical reset error strings."""
        # Mirror stdlib's ``<Token [used ]var=<...> at 0x...>``; the ``var`` is
        # this token's `wool.ContextVar` (the failure is about the wool token).
        used = "used " if self._used else ""
        return f"<Token {used}var={self._var!r} at 0x{id(self):x}>"

    def __eq__(self, other: object) -> bool:
        """Return whether *other* is a `Token` for the same underlying token.

        Two `wool.Token` handles are equal when they share an ID, so the handle
        a caller holds and the one returned from a dispatch round-trip compare
        equal — as a `contextvars.Token` compares equal to itself. Direct
        construction is refused and an ID is minted per `set`, so the ID is a
        faithful token identity.
        """
        return isinstance(other, Token) and other._id == self._id

    def __hash__(self) -> int:
        """Hash by token ID so equal tokens hash alike and survive a round-trip.

        The ID is fixed for the token's life, so the hash is stable even as the
        ``used`` flag settles.
        """
        return hash(self._id)

    def __reduce__(self) -> NoReturn:
        """Reject vanilla pickling; the dispatch pickler uses `__wool_reduce__`.

        :raises TypeError:
            Always.

        .. rubric:: Implementation notes

        A `Token`'s authorization is bound to a live context and the
        cross-process, single-use ledger; reconstructing one outside Wool's
        dispatch path would bypass that bookkeeping. Wool's pickler consults
        ``reducer_override`` (and therefore `__wool_reduce__`) before
        ``__reduce_ex__``, so this guard is invisible to dispatch while
        blocking ``pickle``/``cloudpickle``/``copy`` at every protocol.
        """
        raise TypeError(
            "wool.Token cannot be pickled via vanilla pickle/cloudpickle; "
            "it is serialized automatically when passed through a Wool dispatch."
        )

    def __wool_reduce__(self) -> tuple[Any, tuple[Any, ...]]:
        """Return constructor args for reconstitution via Wool's pickler.

        Reduction never raises for any token state; an orphan (``live=False``,
        or nominated but rejected at the mount gate) reconstitutes into a
        usable `Token` whose only restriction is that ``reset`` fails with the
        reason "token was created in a different Context".

        .. rubric:: Implementation notes

        Ships the token's captured old value, ID, and used flag, plus ``live``
        (whether a native token is bound at serialize time). ``live`` only
        nominates the token for an anchor; the actual context-ownership
        decision happens receiver-side at mount (see `anchor_tokens`), gated
        on the carrying manifest's per-var ``unspent_tokens`` rather than the
        ambient chain at reduce time.
        """
        live = self._native is not None
        return (
            _reconstitute,
            (
                self._var,
                self._old_value,
                self._id,
                live,
                self._used,
            ),
        )

    @property
    def var(self) -> ContextVar[T]:
        """The `~wool.runtime.context.var.ContextVar` this token resets.

        Mirrors `contextvars.Token.var`: this is the `ContextVar` the caller
        holds, so ``var.set(x).var is var`` holds as it does in stdlib.
        """
        return self._var

    @property
    def old_value(self) -> T | UndefinedType:
        """The value the variable held before this token's `set`.

        Mirrors `contextvars.Token.old_value`, except the "no prior value"
        case reports Wool's `~wool.runtime.typing.Undefined` sentinel rather
        than `contextvars.Token.MISSING` — consistent with the rest of the
        Wool context API, and picklable so it survives a dispatch.
        """
        return self._old_value

    @classmethod
    def _mint(
        cls,
        *,
        var: ContextVar[T],
        old_value: T | UndefinedType,
        native: contextvars.Token[Any] | None,
        id: str,
        used: bool = False,
    ) -> Token[T]:
        """Construct a `Token`, bypassing the public constructor guard.

        The sole mint path — `~wool.runtime.context.var.ContextVar.set` and
        dispatch reconstitution (`_reconstitute`) — building the token from its
        wire and anchor state directly rather than through the refusing
        ``__init__``. Every minted instance is counted for ledger reaping via
        `_register_token`, so a consumed ID is reclaimed once no instance of it
        remains live in this process.
        """
        token: Token[T] = object.__new__(cls)
        token._var = var
        token._old_value = old_value
        token._native = native
        token._id = id
        token._used = used
        _register_token(token)
        return token


def _reconstitute(
    var: ContextVar[Any],
    old_value: Any,
    id: str,
    live: bool,
    used: bool,
) -> Token:
    """Rebuild a wire token; collect it for anchoring when ``live``.

    The reduce callable for `Token.__wool_reduce__`. A ``live`` token appends
    itself to the active frame decode's collector (see `token_sink`) so that
    frame's mount decides — against its manifest-borne per-var
    ``unspent_tokens`` — whether to mint its anchor in the target context (see
    `anchor_tokens`). An orphan keeps ``_native`` unset. Reconstitution never
    raises: an orphan is a usable object whose only constraint is that reset
    fails.

    .. rubric:: Implementation notes

    A module-level function (not a `Token` classmethod) so it pickles by
    reference — a classmethod's dotted qualname resolves to a fresh bound
    method that fails cloudpickle's identity check, forcing a by-value pickle
    that would capture this module's `~wool.runtime.context.registry.lock`.

    `Token._mint` counts this instance in `_token_counts`, so while the
    reconstituted token lives its ID's consumed-token entry is retained for
    reaping (see `dead_token_ids`) and reclaimed once it is collected.
    """
    token = Token._mint(
        var=var,
        old_value=old_value,
        native=None,
        id=id,
        used=used,
    )
    if live:
        collected = _token_sink.get()
        if collected is not None:
            collected.append(token)
    return token
