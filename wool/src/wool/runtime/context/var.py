from __future__ import annotations

import contextvars
import inspect
from typing import Any
from typing import Callable
from typing import Final
from typing import TypeVar
from typing import cast
from typing import overload

import wool
from wool.runtime.context.chain import Chain
from wool.runtime.context.exceptions import ContextVarCollision
from wool.runtime.context.guard import assert_chain_owner
from wool.runtime.context.manifest import ContextVarManifest
from wool.runtime.context.manifest import resolve_stub
from wool.runtime.context.registry import lock
from wool.runtime.context.registry import var_registry
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
    the `contextvars.Token` that `set` returns is process-local, and
    its ``var`` attribute references the variable's internal backing
    rather than this `ContextVar`, so ``wool_var.set(x).var is
    wool_var`` is `False` where stdlib's is `True` — the supported
    reset path is ``wool_var.reset(token)`` and ``token.var`` is not a
    supported attribute. Unlike `contextvars.ContextVar`, instances
    pickle across process boundaries and their values propagate through
    ``@wool.routine`` dispatches.

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

    **Storage model** — Values ride in a single immutable
    `~wool.runtime.context.chain.Chain` held in one
    Wool-owned stdlib `contextvars.ContextVar`. Because the
    Wool chain rides in stdlib ``contextvars``, ``wool.ContextVar``
    values propagate with stdlib visibility across every conformant
    event loop and every cooperative asyncio scheduling edge — task
    creation, ``call_soon``/``call_later``/``call_at``,
    ``add_reader``/``add_writer``/``add_signal_handler``,
    ``Future.add_done_callback``. The first `set` on a context
    *arms* it: a chain UUID is minted and the chain-contention guard
    engages. A context in which no `ContextVar` has been set
    is unarmed and behaves as a plain `contextvars.Context`.
    Once armed, the Wool-owned variable is a permanent member of the
    `contextvars.Context`: a `contextvars.copy_context`
    of an armed context carries one extra variable, and it stays even
    after every `ContextVar` is reset.

    Child tasks fork a copy of the parent's context under a fresh
    chain UUID when Wool's task factory is installed on the running
    loop.

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

        The lookup, the registry insert, and the new instance's
        observable state are all serialized under the registry lock
        so concurrent declarations of the same key cannot observe an
        intermediate registration.

        Three outcomes:

        * No prior registration — a fresh instance is constructed and
          registered.
        * A stub already registered (seeded by an earlier wire
          ingress — pickle-embedded or chain-manifest — before any user
          declaration) — promoted in place.
          An explicit ``default=`` wins; an implicit
          `~wool.runtime.typing.Undefined` preserves whatever
          default the stub already carries, mirroring
          `resolve_stub`'s
          "don't silently discard a known default" rule.
        * A non-stub registration already exists — `ContextVarCollision`
          raises; keys must be unique within a namespace.
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
        value. State propagation rides on the chain-manifest path
        (`~wool.runtime.context.chain.Chain.to_manifest` snapshots the
        sender's context and
        `~wool.runtime.context.manifest.ChainManifest.to_protobuf`
        encodes it;
        `~wool.runtime.context.manifest.ChainManifest.from_protobuf`
        decodes the wire frame and
        `~wool.runtime.context.chain.Chain.from_manifest` populates the
        receiver's context). The pickle path stays pure-identity so a
        reconstituted variable is a key only — ``var.get()`` on the
        receiver resolves through the receiver's context without the
        unpickle ever writing to it.

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

    # ``*args`` sentinel pattern mirrors `contextvars.ContextVar.get` —
    # distinguishes "no default supplied" (raise `LookupError`) from
    # "default is `None`" (return `None`). The user-facing surface
    # is constrained by the two ``@overload`` declarations above.
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
        """
        if len(args) > 1:
            raise TypeError(f"get expected at most 1 argument, got {len(args)}")
        # Resolve through the backing variable. ``Undefined`` — whether
        # the backing variable was never set in this Chain or was
        # reset/merged to the sentinel value — means "fall through to
        # the default ladder" (get argument, constructor default,
        # LookupError). Guard at the user-facing API boundary to
        # match `set` and `reset` — the storage layer
        # (raw stdlib contextvars.ContextVar) carries no guard, so
        # ``self._backing.get`` is direct stdlib plumbing.
        assert_chain_owner(wool.__chain__.get(None))
        value = self._backing.get(Undefined)
        if value is not Undefined:
            return value
        if args:
            return args[0]
        if self._default is not Undefined:
            return self._default
        raise LookupError(self)

    def set(self, value: T) -> contextvars.Token[T | UndefinedType]:
        """Set the variable's value in the active context.

        The first `set` on an unarmed context arms it — mints a
        fresh chain UUID, installs the first context, and self-installs
        Wool's task factory on the running loop (raising
        `~wool.TaskFactoryDisplaced` if Wool's factory was
        previously installed on the loop but has since been displaced
        by a third-party factory installed after it). The value rides
        in the variable's backing
        `contextvars.ContextVar`; the context's ``vars`` index
        gains an entry for this variable the first time it is bound in
        the chain. The factory install is performed by
        `~wool.runtime.context.chain.Chain.mount`, which this
        method routes through on every set, but the user-visible effect
        chain is the same.

        :param value:
            The new value.
        :returns:
            A `contextvars.Token` usable with `reset` to
            restore the previous value. The token is process-local, and
            its ``var`` attribute references the variable's internal
            backing `contextvars.ContextVar` rather than this
            `ContextVar` (see the class docstring); the supported reset
            path is ``wool_var.reset(token)``.
        :raises ChainContention:
            If the active chain is being entered by a thread or asyncio
            task other than the one that owns it. See
            `wool.ChainContention` for full detail.
        """
        # Enforce the chain-ownership invariant against the *currently
        # installed* `Chain` before doing anything else.
        # `Chain.mount` below unconditionally re-stamps the
        # owning thread/task, so a guard check that runs after mount
        # would silently transfer ownership to the calling thread —
        # the exact corruption `ChainContention` is meant to
        # surface. The guard lives at the user-facing API boundary
        # (here, plus `get` and `reset`); the storage
        # layer (raw stdlib contextvars.ContextVar) is plumbing.
        assert_chain_owner(wool.__chain__.get(None))
        chain = wool.__chain__.get(None)
        if chain is None:
            # First set on this chain: arm it with a fresh chain
            # UUID. ``Chain.mount`` below is the single point at
            # which Wool's task factory self-installs on the running
            # loop — every code path that arms a chain transits
            # through here.
            chain = Chain()
        # Add this variable to the chain's vars index the first time
        # it is bound in the chain, and clear any prior reset signal —
        # the variable now carries a value again. When it is already
        # indexed and not reset, the chain is unchanged.
        if self not in chain.vars or self._key in chain.resets:
            chain = chain._evolve(
                vars=chain.vars | {self},
                resets=chain.resets - {self._key},
            )
        # Mount before mutating the backing so that a mount-time
        # raise (e.g., ``TaskFactoryDisplaced``) rolls back cleanly
        # without leaving the backing in a state inconsistent with the
        # Wool Chain. Mount in the set path doesn't touch this
        # variable's backing — the evolve above removed ``self._key``
        # from ``resets``, so mount's reset-drain loop skips it, so
        # the later ``self._backing.set(value)`` operates on a freshly
        # mounted-but-unmodified backing.
        chain.mount()
        return self._backing.set(value)

    def reset(self, token: contextvars.Token[T | UndefinedType]) -> None:
        """Restore the variable to the value it had before *token*.

        Matches `contextvars.ContextVar.reset` semantics. The
        reset delegates to the backing variable's native
        `contextvars.ContextVar.reset`, so stdlib itself
        enforces single-use, rejects a token whose ``var`` is not this
        variable's backing, and rejects a token reset in a different
        `contextvars.Context` than the one it was minted in.

        :param token:
            A `contextvars.Token` previously returned by
            `set`. ``token.var`` references the variable's
            internal backing `contextvars.ContextVar` rather
            than this `ContextVar` instance — supported reset
            path is ``wool_var.reset(token)``, not direct stdlib
            reset.
        :raises ValueError:
            If the token was created by a different
            `ContextVar` or in a different
            `contextvars.Context` (surfaced by stdlib
            `contextvars.ContextVar.reset`).
        :raises RuntimeError:
            If the token has already been used (surfaced by stdlib
            single-use enforcement).
        :raises ChainContention:
            If the active chain is being entered by a thread or asyncio
            task other than the one that owns it. See
            `wool.ChainContention` for full detail.
        """
        # Enforce the chain-ownership invariant against the *currently
        # installed* `Chain` before doing anything else.
        # `Chain.mount` below unconditionally re-stamps the
        # owning thread/task, so a guard check that runs after mount
        # would silently transfer ownership to the calling thread.
        # See `set` for the analogous guard placement.
        assert_chain_owner(wool.__chain__.get(None))
        # Atomicity: run the native reset first. Stdlib
        # ``ContextVar.reset`` is atomic — if it rejects (wrong var,
        # wrong Context, already used) observable state is unchanged.
        # Mounting first would break that contract: by the time
        # stdlib raised, the Wool ``Chain`` would already have been
        # evolved + installed and (for the unset case) the backing
        # rewound to ``Undefined`` via the mount drain.
        # Native-reset-first preserves stdlib parity:
        # if the native reset raises, no Wool bookkeeping happens;
        # if it succeeds, evolve + mount commit the Wool view.
        self._backing.reset(token)
        # Below this point the native reset has succeeded; mutate Wool
        # bookkeeping. A subsequent mount-time raise (e.g.
        # ``TaskFactoryDisplaced``) does leave the backing in its
        # restored state — but mount-time raises on the reset path
        # are pathological (the chain was already armed; reset just
        # rewound it) and the diagnostic surface intent of mount
        # raising is unrelated to reset atomicity.
        context = wool.__chain__.get(None)
        if context is None:
            # Unarmed context — native reset already raised the
            # appropriate ValueError, so this branch is unreachable
            # except via test mocks. Safe to no-op.
            return
        # ``token.old_value`` is `contextvars.Token.MISSING` when
        # the variable was never set in this Chain (true first-set),
        # ``Undefined`` (Wool's sentinel) when the backing was
        # rewound by a prior ``mount`` drain, and the prior value
        # otherwise. Both ``MISSING`` and ``Undefined`` mean "reset to
        # no observable value" for the Wool ``Chain`` bookkeeping.
        old_value = token.old_value
        old_was_unset = old_value is contextvars.Token.MISSING or old_value is Undefined
        if old_was_unset:
            new_vars = context.vars - {self}
            new_resets = context.resets | {self._key}
        else:
            new_vars = context.vars | {self}
            new_resets = context.resets - {self._key}
        evolved = context._evolve(
            vars=new_vars,
            resets=new_resets,
        )
        evolved.mount()

    @classmethod
    def _reconstitute(
        cls,
        namespace: str,
        name: str,
        default: Any,
    ) -> ContextVar[Any]:
        """Rebuild or resolve a usable `ContextVar` from pickled parts.

        Routes through `~wool.runtime.context.manifest.resolve_stub` so
        the chain-manifest ingress and the pickle ingress (this method)
        converge on a single registry entry per key. Where the manifest
        ingress is content with a bare
        `~wool.runtime.context.manifest.ContextVarManifest`, the pickle
        ingress needs behavior — the receiver invokes `get`/`set`/`reset`
        — so `_as_contextvar` upgrades a freshly minted manifest to a
        functional `ContextVar` in place. The instance stays flagged as a
        stub
        (``_stub`` is untouched) so a later user declaration still
        promotes it without `ContextVarCollision`. Pickle restores
        identity only — the receiver's context, populated via the
        chain-manifest path, is the source of truth for value lookup.
        """
        return _as_contextvar(resolve_stub((namespace, name), default=default))


def _infer_namespace() -> str:
    """Infer the namespace for a `ContextVar` constructor call.

    Walks up the call stack from the current frame, skipping frames
    from any ``wool.runtime.context`` submodule, and returns the
    top-level package of the first user frame encountered. Falls back
    to ``"__main__"`` if the walk reaches the top of the stack.
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
    the manifest observes the upgrade without rebuild. In-place is
    mandatory: a fresh object would leave those chains pointing at a
    stale manifest and break the one-instance-per-key invariant.

    ``_stub`` is left untouched — this only grants behavior, not
    declared status (`promote` is the declaration transition).

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
