from __future__ import annotations

import contextvars
import inspect
from typing import Any
from typing import Callable
from typing import Final
from typing import TypeVar
from typing import cast
from typing import overload
from uuid import uuid4

import wool
from wool.runtime.context.chain import Chain
from wool.runtime.context.exceptions import ContextVarCollision
from wool.runtime.context.guard import assert_chain_owner
from wool.runtime.context.manifest import ContextVarManifest
from wool.runtime.context.manifest import resolve_stub
from wool.runtime.context.registry import lock
from wool.runtime.context.registry import var_registry
from wool.runtime.context.token import Token
from wool.runtime.context.token import token_anchor
from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

_PACKAGE: Final = __name__.rpartition(".")[0]

T = TypeVar("T")


# public
class ContextVar(ContextVarManifest[T]):
    """Propagating context variable that crosses worker boundaries.

    Mirrors `contextvars.ContextVar` across the `get` / `set` /
    `reset` call shapes: construct with a name and optional default,
    then call `get`, `set`, `reset`. Two deliberate divergences bound
    that parity. First, `get`, `set`, and `reset` additionally raise
    `wool.ChainContention` when a chain is entered by a thread or
    asyncio task other than the one that owns it — stdlib
    `contextvars.ContextVar` never raises it; see
    `wool.ChainContention` for the full scenario catalogue. Second,
    `set` returns a `~wool.runtime.context.token.Token` rather than a
    `contextvars.Token`: it preserves stdlib's single-use and
    per-context reset semantics (including the canonical `RuntimeError`
    / `ValueError` messages) but, unlike the stdlib token, survives a
    Wool dispatch — it may be passed to a `@wool.routine` and reset on
    the receiver. `reset` accepts only that token type. Unlike
    `contextvars.ContextVar`, instances pickle across process
    boundaries and their values propagate through ``@wool.routine``
    dispatches.

    **Identity model** — Every `ContextVar` has a unique
    ``(namespace, name)`` key. The ``name`` is the first positional
    argument; the ``namespace`` is inferred from the top-level package
    of the calling frame or provided explicitly via ``namespace=``.
    Two distinct instances constructed under the same key raise
    `ContextVarCollision`.

    **Namespace stability** — The inferred namespace is the top-level
    package of the calling frame. This is deliberately coarse so that
    wire keys stay stable when a module is refactored deeper within
    its package — a rolling deploy that moves ``myapp.auth.tokens``
    to ``myapp.auth.credentials.tokens`` continues to propagate
    values between caller and worker. The trade-off is that two
    subpackages of the same library cannot define distinct variables
    with the same ``name`` without one of them passing ``namespace=``
    explicitly; the construction raises `ContextVarCollision`
    instead.

    **Storage model** — Wool state rides in stdlib ``contextvars`` (as a
    `~wool.runtime.context.chain.Chain`), so ``wool.ContextVar`` values
    propagate with the same visibility a `contextvars.ContextVar` has —
    across every conformant event loop and asyncio scheduling edge. The
    first `set` on a context *arms* it, engaging the chain-contention
    guard; an unset context is unarmed and behaves as a plain
    `contextvars.Context`. Once armed, the Wool-owned variable is a
    permanent member of the `contextvars.Context`: a
    `contextvars.copy_context` of it carries one extra variable, kept
    even after every `ContextVar` is reset. Child tasks fork a copy of
    the parent's context onto a fresh chain — the copy-on-fork applied
    at task creation.

    Values propagated across the wire must be cloudpicklable.
    Variable serialisation is dispatch-path-only — vanilla
    ``pickle.dumps`` / ``cloudpickle.dumps`` / `copy.copy` /
    `copy.deepcopy` raise `TypeError`; see
    `__reduce_ex__`.
    """

    # No instance slots of its own — the data layer (the key, default,
    # stub flag, and backing) lives on ``ContextVarManifest``. This
    # empty-slots invariant is load-bearing: it keeps a manifest layout-
    # compatible with ``ContextVar`` so ``promote`` can reassign
    # ``__class__`` in place. Adding a slot here breaks that swap (see
    # ``promote`` and its guard test).
    __slots__ = ()

    @overload
    def __new__(
        cls,
        name: str,
        /,
        *,
        namespace: str | None = None,
    ) -> ContextVar[T]: ...

    @overload
    def __new__(
        cls,
        name: str,
        /,
        *,
        namespace: str | None = None,
        default: T,
    ) -> ContextVar[T]: ...

    def __new__(
        cls,
        name: str,
        /,
        *,
        namespace: str | None = None,
        default: T | UndefinedType = Undefined,
    ) -> ContextVar[T]:
        """Resolve or construct the `ContextVar` for *namespace:name*.

        Three outcomes:

        * No prior registration — a fresh instance is constructed and
          registered.
        * A stub already registered by an earlier wire ingress, before
          any user declaration — promoted in place. An explicit
          ``default=`` wins; an implicit `~wool.runtime.typing.Undefined`
          keeps whatever default the stub already carries.
        * A non-stub registration already exists — `ContextVarCollision`
          raises; keys must be unique within a namespace.

        .. rubric:: Implementation notes

        The lookup, the registry insert, and the new instance's observable
        state are all serialized under the registry lock, so concurrent
        declarations of the same key cannot observe an intermediate
        registration. The implicit-default case mirrors `resolve_stub`'s
        "don't silently discard a known default" rule.
        """
        if not isinstance(name, str):
            raise TypeError("context variable name must be a str")
        if namespace is None:
            namespace = _infer_namespace()
        key = (namespace, name)
        with lock:
            existing = var_registry.get(key)
            if existing is not None:
                if existing._stub:
                    promoted = promote(existing)
                    if default is not Undefined:
                        promoted._default = default
                    return promoted
                raise ContextVarCollision(
                    f"wool.ContextVar {key!r} is already registered "
                    f"({existing!r}). Keys must be unique within a "
                    f"namespace."
                )
            instance = cast(ContextVar[T], cls._build(key, default, stub=False))
            var_registry[key] = instance
            return instance

    def __wool_reduce__(
        self,
    ) -> tuple[Callable[..., ContextVar[Any]], tuple[Any, ...]]:
        """Return constructor args for unpickling via Wool's pickler.

        A `ContextVar` is a key for resolving a value from the active
        `~wool.runtime.context.chain.Chain`; its pickled state is
        therefore the key plus the constructor default, never a captured
        value. The pickle path stays pure-identity: a reconstituted
        variable is a key only — ``var.get()`` on the receiver resolves
        through the receiver's context without the unpickle ever writing
        to it.

        .. rubric:: Implementation notes

        State propagation rides on the chain-manifest path, not the pickle:
        `~wool.runtime.context.chain.Chain.to_manifest` snapshots the
        sender's context and
        `~wool.runtime.context.manifest.ChainManifest.to_protobuf` encodes
        it; `~wool.runtime.context.manifest.ChainManifest.from_protobuf`
        decodes the wire frame and
        `~wool.runtime.context.chain.Chain.from_manifest` populates the
        receiver's context.

        The pickle surface lives on `ContextVar` rather than
        `~wool.runtime.context.manifest.ContextVarManifest` because
        unpickling reconstitutes a *usable* variable — the receiver calls
        `get`/`set`/`reset` on it — so `_reconstitute` upgrades a bare
        manifest to a behavioral `ContextVar`. The vanilla-pickle guard
        (`__reduce_ex__`) lives on the base, covering both flavors.
        """
        return (
            ContextVar._reconstitute,
            (self._namespace, self._name, self._default),
        )

    @overload
    def get(self) -> T: ...

    @overload
    def get(self, default: T, /) -> T: ...

    def get(self, *args: T) -> T:
        """Return the current value in the active context.

        :param default:
            Optional fallback returned when the variable has no value
            and no constructor default.
        :returns:
            The current value, the supplied fallback, or the
            constructor default.
        :raises TypeError:
            If more than one positional argument is supplied.
        :raises LookupError:
            If the variable has no value, no fallback, and no default.
        :raises ChainContention:
            If the active chain is being entered by a thread or asyncio
            task other than the one that owns it. See
            `wool.ChainContention` for full detail.

        .. rubric:: Implementation notes

        The ``*args`` signature mirrors `contextvars.ContextVar.get` so a
        `LookupError` for "no default supplied" stays distinct from a
        returned ``None`` for "default is ``None``"; the two ``@overload``
        declarations constrain the user-facing surface. Resolution falls
        through a ladder keyed on `~wool.runtime.typing.Undefined` — an
        `Undefined` backing (never set in this chain, or reset/merged to the
        sentinel) defers to the *default* argument, then the constructor
        default, then `LookupError`. The chain-ownership guard runs at this
        API boundary, as in `set`/`reset`.
        """
        if len(args) > 1:
            raise TypeError(f"get expected at most 1 argument, got {len(args)}")
        # Guard at the API boundary.
        assert_chain_owner(wool.__chain__.get(None))
        # An Undefined backing falls through to the default ladder.
        value = self._backing.get(Undefined)
        if value is not Undefined:
            return value
        if args:
            return args[0]
        if self._default is not Undefined:
            return self._default
        raise LookupError(self)

    def set(self, value: T) -> Token[T]:
        """Set the variable's value in the active context and return a reset `Token`.

        The first `set` in a context *arms* it (see the class **Storage
        model**) and installs Wool's task factory on the running loop.

        :param value:
            The new value.
        :returns:
            A `~wool.runtime.context.token.Token` usable with `reset`
            to restore the previous value. Unlike `contextvars.Token`,
            it survives a Wool dispatch — it may be passed to a
            `@wool.routine` and reset on the receiver — while still
            refusing vanilla pickling. Its ``var`` is this `ContextVar`.
        :raises ChainContention:
            If the active chain is being entered by a thread or asyncio
            task other than the one that owns it. See
            `wool.ChainContention` for full detail.
        :raises TaskFactoryDisplaced:
            If arming the context must install Wool's task factory but a
            third-party factory has displaced a Wool factory previously
            installed on the running loop.

        .. rubric:: Implementation notes

        The chain-ownership guard runs against the *currently installed*
        `~wool.runtime.context.chain.Chain` before any other work.
        `~wool.runtime.context.chain.Chain.mount` re-stamps the owning
        thread and task, so a guard placed after mount would silently
        transfer ownership to the calling thread — the exact corruption
        `wool.ChainContention` exists to surface. The guard sits at the
        user-facing boundary (`get`, `set`, `reset`); the backing
        `contextvars.ContextVar` is unguarded plumbing.

        The token ID is minted before the chain evolve so it rides the
        same evolve as the binding. Recording it under the variable's
        key in ``unspent_tokens`` marks that the token was minted while
        the chain was live in *this* stdlib `contextvars.Context`; the
        receiver's mount checks that per-variable set against the wire
        manifest to decide whether the token may anchor a reset (see
        `~wool.runtime.context.token.anchor_tokens`).

        Spent token IDs survive a fresh set. A token consumed on a
        remote worker leaves its local ``_used`` flag unset — the
        consumption is known here only through the chain's
        ``spent_tokens`` ledger — so pruning the key's spent IDs on a
        re-set would let the caller reset a token already consumed on
        another process a second time. The ledger therefore grows one
        entry per consumed token for the chain's lifetime; bounding that
        growth to token lifespan is issue #226's ownership model, not a
        prune that forgets consumptions. The freshly minted ID is never
        spent, so it always survives the subtraction that keeps
        ``unspent_tokens`` free of dead IDs.

        The bookkeeping delta folds into mount's single owner-stamp
        evolve — one `~wool.runtime.context.chain.Chain` construction,
        not two — and mount runs before the backing mutation so a
        mount-time raise (e.g., `wool.TaskFactoryDisplaced`) rolls back
        cleanly. Mount on the set path leaves this variable's backing
        untouched: the delta has already cleared the key from
        ``resets``, so mount's reset-drain skips it and
        ``self._backing.set`` writes to a freshly mounted-but-unmodified
        backing.

        The native token's "no prior value" sentinel
        (`contextvars.Token.MISSING`) is normalised to
        `~wool.runtime.typing.Undefined` so it survives the wire: a wire
        reset applies the captured old value directly, while the local
        path reads the native token instead.
        """
        # Guard the installed chain before mount re-stamps ownership.
        assert_chain_owner(wool.__chain__.get(None))
        chain = wool.__chain__.get(None)
        if chain is None:
            # First set arms the context (see the class Storage model).
            chain = Chain()
        # Mint the ID up front so it rides the binding's evolve.
        token_id = uuid4().hex
        # Bind the variable in the chain, clearing any prior reset signal.
        if self not in chain.vars or self._key in chain.resets:
            new_vars = chain.vars | {self}
            new_resets = chain.resets - {self._key}
        else:
            new_vars, new_resets = chain.vars, chain.resets
        # Record the fresh ID; drop any IDs already spent for this key.
        new_unspent = {
            **chain.unspent_tokens,
            self._key: (chain.unspent_tokens.get(self._key, frozenset()) | {token_id})
            - chain.spent_tokens.get(self._key, frozenset()),
        }
        # Fold the delta into mount's evolve; mount before the backing write.
        chain.mount(
            vars=new_vars,
            resets=new_resets,
            unspent_tokens=new_unspent,
        )
        native = self._backing.set(value)
        # Normalise the native "unset" sentinel to Undefined for the wire.
        old_value = native.old_value
        if old_value is contextvars.Token.MISSING:
            old_value = Undefined
        return Token._mint(
            var=self,
            old_value=old_value,
            native=native,
            id=token_id,
        )

    def reset(self, token: Token[T]) -> None:
        """Restore the variable to the value it had before *token*.

        Mirrors `contextvars.ContextVar.reset`, extended to accept a
        `~wool.runtime.context.token.Token` that may have crossed a Wool
        dispatch. Only a `~wool.runtime.context.token.Token` is accepted; a
        raw `contextvars.Token` raises `TypeError`. Enforcement follows
        stdlib's order and error types:

        * **Already used** — locally, or consumed elsewhere in the chain
          (this process or another) — raises `RuntimeError`.
        * **Wrong variable** — a token minted by a different `ContextVar`
          than the one being reset — raises `ValueError`.
        * **Different context** — a token minted in a different
          `contextvars.Context` than the reset runs in (a
          `contextvars.Context.run` sibling, a ``create_task`` fork, or a
          wire token that arrived orphaned) — raises `ValueError`.

        Reset is single-use everywhere the token's ID has reached: once a
        reset succeeds, any further reset of the same token raises
        `RuntimeError`, whether attempted in this process or another the
        chain reaches. Atomicity mirrors stdlib — the used and context
        checks run before any state mutation, so a rejected reset leaves
        the variable and chain unchanged.

        :param token:
            A `~wool.runtime.context.token.Token` returned by this
            variable's `set`.
        :raises TypeError:
            If *token* is not a `~wool.runtime.context.token.Token`.
        :raises ValueError:
            If the token was minted by a different `ContextVar`, or in a
            different `contextvars.Context` (or arrived orphaned across
            the wire).
        :raises RuntimeError:
            If the token has already been used, on this or any process.
        :raises ChainContention:
            If the active chain is being entered by a thread or asyncio
            task other than the one that owns it. See
            `wool.ChainContention` for full detail.

        .. rubric:: Implementation notes

        The chain-ownership guard runs against the currently installed
        `~wool.runtime.context.chain.Chain` first, as in `set`. That
        chain is stable through the reset — the backing mutations never
        touch ``wool.__chain__`` — so it is bound once and reused for the
        spent-ledger read and the closing evolve.

        The used-gate runs before the wrong-variable check to match
        stdlib's order (a token that is both used and for the wrong
        variable raises the used error). It consults the local ``_used``
        flag and the chain's ``spent_tokens`` ledger keyed on the
        *token's own* variable, so a token consumed elsewhere in the
        chain is rejected at its origin too; keying on the token's
        variable rather than ``self`` keeps a used token handed to the
        wrong variable tripping this gate first. When the ledger is the
        discoverer, the local flag is stamped so the canonical error repr
        still carries stdlib's ``used`` marker (``<Token used var=...>``)
        and later gates see the settled flag.

        Context validity is decided per token flavor, before any backing
        mutation. A local token (its native backing is this variable's)
        validates and restores through the native reset, delegating
        "same Context" to stdlib. A wire token validates through its
        receiver-side anchor (see `~wool.runtime.context.token.Token`)
        and then has its captured old value applied to the backing
        directly, the native reset being unavailable off-origin; an
        orphan wire token has no anchor and always raises "different
        Context". A single guarded reset translates stdlib's
        `RuntimeError` / `ValueError` into the canonical wool messages.

        Once the backing has committed, the chain bookkeeping mirrors it:
        the token's ID moves from the variable's ``unspent_tokens`` set
        (dropping the key once it empties) into ``spent_tokens`` — which
        rides the next frame so the consumption propagates across the
        wire — and the token is flagged used locally. The spent ID then
        persists for the chain's lifetime (see
        `~wool.runtime.context.chain.Chain`); a later set never prunes it,
        since an off-origin consumption is witnessed only by this ledger.
        """
        # Guard the installed chain first (see set); bind it once for reuse.
        context = wool.__chain__.get(None)
        assert_chain_owner(context)
        if not isinstance(token, Token):
            raise TypeError(f"expected an instance of Token, got {token!r}")
        # Used-gate first (stdlib order), keyed on the token's own var.
        consumed = context is not None and token._id in context.spent_tokens.get(
            token._var._key, frozenset()
        )
        if token._used or consumed:
            # Stamp _used so a ledger-discovered rejection reprs as "used".
            token._used = True
            raise RuntimeError(f"{token!r} has already been used once")
        # Then reject a token minted by a different variable.
        if token._var is not self:
            raise ValueError(f"{token!r} was created by a different ContextVar")
        # Context check before any backing mutation (stdlib atomicity); an
        # orphan wire token has no native and no anchor.
        if token._native is None:
            raise ValueError(f"{token!r} was created in a different Context")
        # Local token validates via the backing; a wire token via its anchor.
        local = token._native.var is self._backing
        try:
            (self._backing if local else token_anchor).reset(token._native)
        except RuntimeError:
            raise RuntimeError(f"{token!r} has already been used once") from None
        except ValueError:
            raise ValueError(f"{token!r} was created in a different Context") from None
        if local:
            native_old = token._native.old_value
            old_was_unset = (
                native_old is contextvars.Token.MISSING or native_old is Undefined
            )
        else:
            # Native reset is unavailable off-origin; apply old value directly.
            old_was_unset = token._old_value is Undefined
            self._backing.set(Undefined if old_was_unset else token._old_value)
        # Mirror the committed reset into the chain: move the ID from
        # unspent to spent, dropping the key once unspent empties.
        if context is not None:
            if old_was_unset:
                new_vars = context.vars - {self}
                new_resets = context.resets | {self._key}
            else:
                new_vars = context.vars | {self}
                new_resets = context.resets - {self._key}
            remaining = context.unspent_tokens.get(self._key, frozenset()) - {token._id}
            new_unspent = {**context.unspent_tokens}
            if remaining:
                new_unspent[self._key] = remaining
            else:
                new_unspent.pop(self._key, None)
            new_spent = {
                **context.spent_tokens,
                self._key: context.spent_tokens.get(self._key, frozenset())
                | {token._id},
            }
            context._evolve(
                vars=new_vars,
                resets=new_resets,
                unspent_tokens=new_unspent,
                spent_tokens=new_spent,
            ).mount()
        token._used = True

    @classmethod
    def _reconstitute(
        cls,
        namespace: str,
        name: str,
        default: Any,
    ) -> ContextVar[Any]:
        """Rebuild or resolve a usable `ContextVar` from pickled parts.

        Restores identity only — the receiver's context, populated via the
        chain-manifest path, is the source of truth for value lookup.

        .. rubric:: Implementation notes

        Routes through `~wool.runtime.context.manifest.resolve_stub` so
        the chain-manifest ingress and the pickle ingress (this method)
        converge on a single registry entry per key. Where the manifest
        ingress is content with a bare
        `~wool.runtime.context.manifest.ContextVarManifest`, the pickle
        ingress needs behavior — the receiver invokes `get`/`set`/`reset` —
        so `_as_contextvar` upgrades a freshly minted manifest to a
        functional `ContextVar` in place. The instance stays flagged as a
        stub (``_stub`` is untouched) so a later user declaration still
        promotes it without `ContextVarCollision`.
        """
        return _as_contextvar(resolve_stub((namespace, name), default=default))


def _infer_namespace() -> str:
    """Infer the namespace for a `ContextVar` constructor call.

    The namespace is the top-level package of the first non-Wool caller
    frame, or ``"__main__"`` when the walk finds none.

    .. rubric:: Implementation notes

    Walks up the call stack from the current frame, skipping frames from
    any ``wool.runtime.context`` submodule, and returns the top-level
    package of the first frame that remains.
    """
    frame = inspect.currentframe()
    while frame is not None:
        module = frame.f_globals.get("__name__", "")
        if module and module != _PACKAGE and not module.startswith(_PACKAGE + "."):
            return module.partition(".")[0]
        frame = frame.f_back
    return "__main__"  # pragma: no cover — stack always has a caller


def _as_contextvar(manifest: ContextVarManifest[T]) -> ContextVar[T]:
    """Retag a bare `ContextVarManifest` as a behavioral `ContextVar` in place.

    The upgrade preserves identity: ``manifest`` keeps its backing
    `contextvars.ContextVar` and its registry registration, so a value
    drained into the backing before the upgrade survives, and every
    immutable `~wool.runtime.context.chain.Chain` that already captured
    the manifest observes the upgrade without rebuild. ``_stub`` is left
    untouched — this only grants behavior, not declared status (`promote`
    is the declaration transition).

    .. rubric:: Implementation notes

    In-place is mandatory: a fresh object would leave those chains
    pointing at a stale manifest and break the one-instance-per-key
    invariant.

    Reassigning ``__class__`` is sound only because `ContextVar` adds no
    instance slots over `ContextVarManifest` (``__slots__ = ()``),
    keeping the two memory layouts compatible. A future slot on
    `ContextVar` would make this raise `TypeError` at the first upgrade;
    ``test_contextvar_should_declare_empty_slots`` guards the invariant.
    """
    if type(manifest) is ContextVarManifest:
        manifest.__class__ = ContextVar
    return cast(ContextVar[T], manifest)


def promote(manifest: ContextVarManifest[T]) -> ContextVar[T]:
    """Promote a stub-state manifest to a *declared* `ContextVar`, in place.

    The receiver-side counterpart to `resolve_stub`: when user code
    finally declares a variable whose key a wire ingress already
    registered as a stub, `ContextVar.__new__` calls this to upgrade the
    registered placeholder. Grants behavior via `_as_contextvar` and
    clears ``_stub`` so the variable counts as declared — a subsequent
    declaration of the same key then raises `ContextVarCollision`.
    """
    promoted = _as_contextvar(manifest)
    promoted._stub = False
    return promoted
