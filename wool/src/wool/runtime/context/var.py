from __future__ import annotations

import contextvars
import inspect
from typing import Any
from typing import Callable
from typing import Final
from typing import Generic
from typing import NoReturn
from typing import SupportsIndex
from typing import TypeVar
from typing import overload

import wool
from wool.runtime.context.chain import Chain
from wool.runtime.context.exceptions import ContextVarCollision
from wool.runtime.context.guard import _assert_chain_owner
from wool.runtime.context.registry import lock
from wool.runtime.context.registry import var_registry
from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

_PACKAGE: Final = __name__.rpartition(".")[0]

T = TypeVar("T")


# public
class ContextVar(Generic[T]):
    """Propagating context variable that crosses worker boundaries.

    Mirrors `contextvars.ContextVar` at the surface: construct
    with a name and optional default; call `get`, `set`,
    `reset`. The one surface difference: `get`,
    `set`, and `reset` additionally raise
    `wool.ChainContention` when a chain is entered by a thread
    or asyncio task other than the one that owns it —
    stdlib `contextvars.ContextVar` never raises it. See
    `wool.ChainContention` for the full scenario catalogue.
    Unlike `contextvars.ContextVar`, instances pickle across
    process boundaries and their values propagate through
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

    __slots__ = (
        "_name",
        "_namespace",
        "_key",
        "_default",
        "_stub",
        "_backing",
        "__weakref__",
    )

    _name: str
    _namespace: str
    _key: tuple[str, str]
    _default: T | UndefinedType
    _stub: bool
    _backing: contextvars.ContextVar[T | UndefinedType]

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
                    if default is not Undefined:
                        existing._default = default
                    existing._stub = False
                    return existing
                raise ContextVarCollision(
                    f"wool.ContextVar {key!r} is already registered "
                    f"({existing!r}). Keys must be unique within a "
                    f"namespace."
                )
            instance = cls._build(key, default, stub=False)
            var_registry[key] = instance
            return instance

    def __wool_reduce__(
        self,
    ) -> tuple[Callable[..., ContextVar[Any]], tuple[Any, ...]]:
        """Return constructor args for unpickling via Wool's pickler.

        A `ContextVar` is a key for resolving a value from the
        active `~wool.runtime.context.chain.Chain`; its
        pickled state is therefore the key plus the constructor
        default, never a captured value. State propagation rides on
        the chain-manifest path
        (`~wool.runtime.context.chain.Chain.to_protobuf` walks
        the sender's context;
        `~wool.runtime.context.manifest.ChainManifest.from_protobuf`
        decodes the wire frame and
        `~wool.runtime.context.manifest.ChainManifest.mount` populates
        the receiver's context). The pickle path stays pure-identity
        so a reconstituted variable is a key only — ``var.get()`` on
        the receiver resolves through the receiver's context without
        the unpickle ever writing to it.

        ContextVar is guarded against vanilla pickling (see
        `__reduce_ex__`); this method is invoked only by Wool's
        own pickler.
        """
        return (
            ContextVar._reconstitute,
            (self._namespace, self._name, self._default),
        )

    def __reduce_ex__(self, _protocol: SupportsIndex) -> NoReturn:
        """Reject vanilla pickling.

        ContextVar identity is registered against the process-wide
        `var_registry`; restoring an instance outside Wool's
        dispatch path bypasses the stub-promotion and collision-
        detection that `_reconstitute` relies on. Wool's own
        pickler consults ``reducer_override`` (and therefore
        `__wool_reduce__`) before ``__reduce_ex__``, so this
        guard is invisible to Wool's serialization.

        `copy.copy` and `copy.deepcopy` also route
        through ``__reduce_ex__`` and are rejected for the same
        reason — a registry-bound ContextVar has no meaningful copy
        semantics.

        :raises TypeError:
            Always.
        """
        raise TypeError(
            "wool.ContextVar cannot be pickled via vanilla pickle/cloudpickle; "
            "it is serialized automatically when dispatched through Wool's "
            "runtime."
        )

    def __repr__(self) -> str:
        default_part = (
            f" default={self._default!r}" if self._default is not Undefined else ""
        )
        return (
            f"<wool.ContextVar name={self._name!r} "
            f"namespace={self._namespace!r}{default_part} at 0x{id(self):x}>"
        )

    @property
    def name(self) -> str:
        """The variable's name, matching the `contextvars.ContextVar` API."""
        return self._name

    @property
    def namespace(self) -> str:
        """The namespace this variable belongs to."""
        return self._namespace

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
        _assert_chain_owner(wool.__chain__.get(None))
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
            restore the previous value. The token's ``var`` attribute
            references the variable's internal backing
            `contextvars.ContextVar` (an implementation
            detail); the supported reset path is
            ``wool_var.reset(token)``. Tokens are process-local —
            cross-process token transport is deferred (see issue
            #231).
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
        _assert_chain_owner(wool.__chain__.get(None))
        context = wool.__chain__.get(None)
        if context is None:
            # First set on this context: arm it with a fresh chain
            # UUID. ``Chain.mount`` below is the single point at
            # which Wool's task factory self-installs on the running
            # loop — every code path that arms a chain transits
            # through here.
            context = Chain()
        # Add this variable to the context's vars index the first time
        # it is bound in the chain, and clear any prior reset signal —
        # the variable now carries a value again. When it is already
        # indexed and not reset, the context is unchanged.
        if self not in context.vars or self._key in context.resets:
            context = context._evolve(
                vars=context.vars | {self},
                resets=context.resets - {self._key},
            )
        # Mount before mutating the backing so that a mount-time
        # raise (e.g., ``TaskFactoryDisplaced``) rolls back cleanly
        # without leaving the backing in a state inconsistent with the
        # Wool Chain. Mount in the set path doesn't touch this
        # variable's backing — the evolve above removed ``self._key``
        # from ``resets``, so mount's reset-drain loop skips it, so
        # the later ``self._backing.set(value)`` operates on a freshly
        # mounted-but-unmodified backing.
        context.mount()
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
        _assert_chain_owner(wool.__chain__.get(None))
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
    def _build(
        cls,
        key: tuple[str, str],
        default: Any,
        *,
        stub: bool,
    ) -> ContextVar[Any]:
        """Construct a `ContextVar` instance with field assignment.

        Single source of truth for the
        ``object.__new__(ContextVar)`` + per-field-assignment idiom
        shared by `__new__` (the user-facing construction path)
        and `resolve_stub` (the
        wire-boundary stub path). The instance is *not* registered in
        `var_registry` — callers do that under the registry
        lock. The backing stdlib variable is created once and shared
        by every chain for the process lifetime; it carries no
        ``contextvars``-level default — `get` owns the default-
        resolution ladder, and "unset" is the
        `~wool.runtime.typing.Undefined` sentinel value (see
        `get`/`reset`).
        """
        namespace, name = key
        instance: ContextVar[Any] = object.__new__(cls)
        instance._name = name
        instance._namespace = namespace
        instance._key = key
        instance._default = default
        instance._stub = stub
        instance._backing = contextvars.ContextVar(
            f"__wool_var__:{namespace}:{name}", default=Undefined
        )
        return instance

    @classmethod
    def _reconstitute(
        cls,
        namespace: str,
        name: str,
        default: Any,
    ) -> ContextVar[Any]:
        """Rebuild or resolve a `ContextVar` from externally-
        supplied parts.

        Routes through `resolve_stub` for the lookup-or-stub
        path so the chain-manifest ingress (via
        `~wool.runtime.context.manifest.ChainManifest.from_protobuf`)
        and the pickle ingress (this method) converge on a single
        creation site. Pickle restores identity only — the receiver's
        context is the source of truth for value lookup, populated
        via the chain-manifest path rather than as a side-effect of
        unpickling.
        """
        return resolve_stub((namespace, name), default=default)


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


def resolve_stub(
    key: tuple[str, str],
    *,
    default: Any = Undefined,
) -> ContextVar[Any]:
    """Return the `ContextVar` registered under *key*, creating a
    stub if no authoritative declaration exists yet.

    Unifies the two ingress paths that may encounter an unregistered
    variable key on a receiving process: the pickle-embedded
    `ContextVar` instance path (via
    `ContextVar._reconstitute`) and the chain-manifest path (via
    `~wool.runtime.context.manifest.ChainManifest.from_protobuf`). Both
    route through this helper so a lazy-import receiver converges on
    a single `ContextVar` instance per key regardless of
    whether the value arrived as a bare wire entry or embedded in a
    pickled variable reference.

    A freshly created stub is registered in `var_registry` (a
    `weakref.WeakValueDictionary`, so it needs a strong
    referent to survive). It is held by the embedding object graph
    (the pickle-embedded ingress) or by the decoded
    `~wool.runtime.context.chain.Chain`'s ``stubs``
    (the chain-manifest ingress) until the receiver's user code
    declares the real variable, at which point
    `ContextVar.__new__` promotes the stub in place. A promoted
    variable remains in ``stubs`` for the rest of that chain's
    life — harmless, since a declared variable is a process-wide
    singleton anyway. If the receiver never declares the variable,
    the stub is collected with whatever held it and the propagated
    value is dropped.

    Pass *default* to seed the stub's constructor default before
    promotion when that information is available on the ingress side
    (the pickle path carries it; the chain-manifest path does not).
    """
    with lock:
        existing = var_registry.get(key)
        if existing is not None:
            # Fold the supplied default into a default-less stub: the
            # chain-manifest path supplies no default (wire bytes don't
            # carry it), but the pickle-embedded path does. Whichever
            # ingress encounters the key second must not silently
            # discard a known default.
            if (
                existing._stub
                and existing._default is Undefined
                and default is not Undefined
            ):
                existing._default = default
            return existing
        # The backing variable is created with the stub and preserved
        # across stub promotion (`ContextVar.__new__` returns the
        # same instance), so a value applied to the stub's backing
        # variable before the receiver declares the real variable
        # survives the promotion.
        stub = ContextVar._build(key, default, stub=True)
        var_registry[key] = stub
        return stub
