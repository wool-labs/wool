from __future__ import annotations

import inspect
import threading
from typing import Any
from typing import Callable
from typing import Final
from typing import Generic
from typing import NoReturn
from typing import SupportsIndex
from typing import TypeVar
from typing import overload
from uuid import uuid4

from wool.runtime.context.factory import ensure_task_factory_installed
from wool.runtime.context.guard import _resolve_owner_task
from wool.runtime.context.guard import assert_chain_owner
from wool.runtime.context.guard import assert_owner_task
from wool.runtime.context.registry import lock
from wool.runtime.context.registry import var_registry
from wool.runtime.context.snapshot import Snapshot
from wool.runtime.context.snapshot import _current_owner_ref
from wool.runtime.context.snapshot import current_snapshot
from wool.runtime.context.snapshot import install_snapshot
from wool.runtime.context.stub import resolve_stub
from wool.runtime.context.token import Token
from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

_PACKAGE: Final = __name__.rpartition(".")[0]

T = TypeVar("T")


# public
class ContextVarCollision(Exception):
    """Raised when two distinct :class:`ContextVar` instances are
    constructed with the same ``(namespace, name)`` key.

    Keys must be unique within the inferred package namespace. Library
    authors should pass ``namespace=`` explicitly when constructing
    variables from shared factory code; application code can rely on
    the implicit package-name inference.

    Detection is best-effort under garbage collection: the process-wide
    variable registry holds :class:`ContextVar` instances weakly, so a
    key frees up once its previous instance is collected and a later
    construction under that key then succeeds instead of colliding. In
    practice :class:`ContextVar` instances are module-level singletons
    held for the process lifetime, so a genuine collision always
    raises.
    """


# public
class ContextVar(Generic[T]):
    """Propagating context variable that crosses worker boundaries.

    Mirrors :class:`contextvars.ContextVar` at the surface: construct
    with a name and optional default; call :meth:`get`, :meth:`set`,
    :meth:`reset`. The one surface difference: :meth:`get`,
    :meth:`set`, and :meth:`reset` additionally raise
    :class:`wool.ConcurrentChainEntry` when an armed chain is accessed
    from a thread other than the one that owns it — stdlib
    :class:`contextvars.ContextVar` never raises it. Unlike
    :class:`contextvars.ContextVar`, instances pickle across process
    boundaries and their values propagate through ``@wool.routine``
    dispatches.

    **Identity model** — Every :class:`ContextVar` has a unique
    ``(namespace, name)`` key. The ``name`` is the first positional
    argument; the ``namespace`` is inferred from the top-level package
    of the calling frame or provided explicitly via ``namespace=``.
    Two distinct instances constructed under the same key raise
    :class:`ContextVarCollision`.

    **Namespace stability** — The inferred namespace is the top-level
    package of the calling frame. This is deliberately coarse so that
    wire keys stay stable when a module is refactored deeper within
    its package — a rolling deploy that moves ``myapp.auth.tokens``
    to ``myapp.auth.credentials.tokens`` continues to propagate
    values between caller and worker. The trade-off is that two
    subpackages of the same library cannot define distinct variables
    with the same ``name`` without one of them passing ``namespace=``
    explicitly; the construction raises :class:`ContextVarCollision`
    instead.

    **Storage model** — Values ride in a single immutable
    :class:`~wool.runtime.context.snapshot.Snapshot` held in one
    Wool-owned stdlib :class:`contextvars.ContextVar`. Because the
    snapshot rides in stdlib ``contextvars``, ``wool.ContextVar``
    values propagate with stdlib visibility across every conformant
    event loop and every cooperative asyncio scheduling edge — task
    creation, ``call_soon``/``call_later``/``call_at``,
    ``add_reader``/``add_writer``/``add_signal_handler``,
    ``Future.add_done_callback``. The first :meth:`set` on a context
    *arms* it: a chain UUID is minted and the concurrent-entry guard
    engages. A context in which no :class:`ContextVar` has been set
    is unarmed and behaves as a plain :class:`contextvars.Context`.

    Child tasks fork a copy of the parent's snapshot under a fresh
    chain UUID when Wool's task factory is installed on the running
    loop.

    Values propagated across the wire must be cloudpicklable.
    """

    __slots__ = (
        "_name",
        "_namespace",
        "_key",
        "_default",
        "_stub",
        "__weakref__",
    )

    _name: str
    _namespace: str
    _key: tuple[str, str]
    _default: T | UndefinedType
    _stub: bool

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
        """Resolve or construct the :class:`ContextVar` for *namespace:name*.

        The lookup, the registry insert, and the new instance's
        observable state are all serialized under the registry lock
        so concurrent declarations of the same key cannot observe an
        intermediate registration.

        Three outcomes:

        * No prior registration — a fresh instance is constructed and
          registered.
        * A stub already registered (seeded by an earlier pickle-path
          ingress before any user declaration) — promoted in place.
          An explicit ``default=`` wins; an implicit
          :data:`~wool.runtime.typing.Undefined` preserves whatever
          default the stub already carries, mirroring
          :func:`~wool.runtime.context.stub.resolve_stub`'s
          "don't silently discard a known default" rule.
        * A non-stub registration already exists — :class:`ContextVarCollision`
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
            instance = super().__new__(cls)
            instance._name = name
            instance._namespace = namespace
            instance._key = key
            instance._default = default
            instance._stub = False
            var_registry[key] = instance
            return instance

    def __wool_reduce__(
        self,
    ) -> tuple[Callable[..., ContextVar[Any]], tuple[Any, ...]]:
        """Return constructor args for unpickling via Wool's pickler.

        A :class:`ContextVar` is a key for resolving a value from the
        active :class:`~wool.runtime.context.snapshot.Snapshot`; its
        pickled state is therefore the key plus the constructor
        default, never a value snapshot. State propagation rides on
        the wire-context path
        (:func:`~wool.runtime.context.snapshot.encode_snapshot` walks
        the sender's snapshot;
        :func:`~wool.runtime.context.snapshot.decode_snapshot`
        populates the receiver's). The pickle path stays pure-identity
        so a reconstituted variable is a key only — ``var.get()`` on
        the receiver resolves through the receiver's snapshot without
        the unpickle ever writing to it.

        ContextVar is guarded against vanilla pickling (see
        :meth:`__reduce_ex__`); this method is invoked only by Wool's
        own pickler.
        """
        return (
            ContextVar._reconstitute,
            (self._namespace, self._name, self._default),
        )

    def __reduce_ex__(self, _protocol: SupportsIndex) -> NoReturn:
        """Reject vanilla pickling.

        ContextVar identity is registered against the process-wide
        :data:`var_registry`; restoring an instance outside Wool's
        dispatch path bypasses the stub-promotion and collision-
        detection that :meth:`_reconstitute` relies on. Wool's own
        pickler consults ``reducer_override`` (and therefore
        :meth:`__wool_reduce__`) before ``__reduce_ex__``, so this
        guard is invisible to Wool's serialization.

        :func:`copy.copy` and :func:`copy.deepcopy` also route
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
        """The variable's name, matching the :class:`contextvars.ContextVar` API."""
        return self._name

    @property
    def namespace(self) -> str:
        """The namespace this variable belongs to."""
        return self._namespace

    @overload
    def get(self) -> T: ...

    @overload
    def get(self, default: T, /) -> T: ...

    # ``*args`` sentinel pattern mirrors :meth:`contextvars.ContextVar.get` —
    # distinguishes "no default supplied" (raise :class:`LookupError`) from
    # "default is :data:`None`" (return :data:`None`). The user-facing surface
    # is constrained by the two ``@overload`` declarations above.
    def get(self, *args: T) -> T:
        """Return the current value in the active snapshot.

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
        :raises ConcurrentChainEntry:
            If the active chain is being entered from a thread other
            than the one that owns it.
        """
        if len(args) > 1:
            raise TypeError(f"get expected at most 1 argument, got {len(args)}")
        snapshot = current_snapshot()
        assert_chain_owner(snapshot)
        assert_owner_task(snapshot)
        if snapshot is not None and self in snapshot.data:
            return snapshot.data[self]
        if args:
            return args[0]
        if self._default is not Undefined:
            return self._default
        raise LookupError(self)

    def set(self, value: T) -> Token[T]:
        """Set the variable's value in the active snapshot.

        The first :meth:`set` on an unarmed context arms it — mints a
        fresh chain UUID and installs the first snapshot. Every
        :meth:`set` rebuilds the immutable snapshot with the new
        binding and reinstalls it.

        :param value:
            The new value.
        :returns:
            A :class:`Token` usable with :meth:`reset` to restore the
            previous value.
        :raises ConcurrentChainEntry:
            If the active chain is being entered from a thread other
            than the one that owns it.
        """
        snapshot = current_snapshot()
        assert_chain_owner(snapshot)
        assert_owner_task(snapshot)
        if snapshot is None:
            # First set on this context: arm it. Self-install the task
            # factory so tasks forked after this point inherit the
            # chain, then mint the chain UUID and stamp the running
            # task as the chain's owner.
            ensure_task_factory_installed()
            snapshot = Snapshot(
                chain_id=uuid4(),
                owner=threading.get_ident(),
                owner_task=_current_owner_ref(),
            )
        elif _resolve_owner_task(snapshot) is None:
            # Adoption: the chain is armed but has no live owner task
            # (the owner finished, was collected, or the chain was
            # armed outside any task). The running task adopts
            # ownership so a later concurrent task entering the same
            # context still fails loud via assert_owner_task. A live
            # foreign owner would already have raised above, so
            # reaching here means the owner is absent — or is the
            # current task, making the re-stamp a harmless no-op.
            snapshot = snapshot.evolve(owner_task=_current_owner_ref())
        old_value = snapshot.data.get(self, Undefined)
        install_snapshot(snapshot.evolve(data={**snapshot.data, self: value}))
        return Token(self, old_value, snapshot.chain_id)

    def reset(self, token: Token[T]) -> None:
        """Restore the variable to the value it had before *token*.

        Matches :meth:`contextvars.ContextVar.reset` semantics: the
        token must have been minted in the same logical chain as the
        one currently active. Chain identity is the snapshot
        ``chain_id`` UUID — the canonical identity that holds across
        in-process forks and cross-process boundaries.

        :param token:
            A token previously returned by :meth:`set`.
        :raises RuntimeError:
            If the token has already been used.
        :raises ValueError:
            If the token was created by a different
            :class:`ContextVar` or in a different chain.
        :raises ConcurrentChainEntry:
            If the active chain is being entered from a thread other
            than the one that owns it.
        """
        if not isinstance(token, Token):
            raise TypeError(
                f"reset expected an instance of wool.Token, got {type(token).__name__}"
            )
        if token._used:
            raise RuntimeError("Token has already been used")
        if token._key != self._key:
            raise ValueError("Token was created by a different ContextVar")
        snapshot = current_snapshot()
        assert_chain_owner(snapshot)
        assert_owner_task(snapshot)
        if snapshot is None or token._chain_id != snapshot.chain_id:
            raise ValueError("Token was created in a different chain")
        token._used = True
        new_data = dict(snapshot.data)
        if token._old_value is Undefined:
            new_data.pop(self, None)
        else:
            new_data[self] = token._old_value
        install_snapshot(
            snapshot.evolve(
                data=new_data,
                consumed={**snapshot.consumed, token._id: self._key},
            )
        )

    @classmethod
    def _reconstitute(
        cls,
        namespace: str,
        name: str,
        default: Any,
    ) -> ContextVar[Any]:
        """Rebuild or resolve a :class:`ContextVar` from externally-
        supplied parts.

        Routes through :func:`resolve_stub` for the lookup-or-stub
        path so the wire-snapshot ingress (via
        :func:`~wool.runtime.context.snapshot.decode_snapshot`) and
        the pickle ingress (this method) converge on a single
        creation site. Pickle restores identity only — the receiver's
        snapshot is the source of truth for value lookup, populated
        via the wire-context path rather than as a side-effect of
        unpickling.
        """
        return resolve_stub((namespace, name), default=default)


def _infer_namespace() -> str:
    """Infer the namespace for a :class:`ContextVar` constructor call.

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
