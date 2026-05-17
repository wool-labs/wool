from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any

from wool.runtime.context.registry import lock
from wool.runtime.context.registry import var_registry
from wool.runtime.typing import Undefined

if TYPE_CHECKING:
    from wool.runtime.context.var import ContextVar


def resolve_stub(
    key: tuple[str, str],
    *,
    default: Any = Undefined,
) -> ContextVar[Any]:
    """Return the :class:`ContextVar` registered under *key*, creating a
    stub if no authoritative declaration exists yet.

    Unifies the two ingress paths that may encounter an unregistered
    variable key on a receiving process: the pickle-embedded
    :class:`ContextVar` instance path (via
    :meth:`ContextVar._reconstitute`) and the wire-snapshot path (via
    :func:`~wool.runtime.context.snapshot.decode_snapshot`). Both
    route through this helper so a lazy-import receiver converges on
    a single :class:`ContextVar` instance per key regardless of
    whether the value arrived as a bare wire entry or embedded in a
    pickled variable reference.

    A freshly created stub is registered in :data:`var_registry` (a
    :class:`weakref.WeakValueDictionary`, so it needs a strong
    referent to survive). It is held by the embedding object graph
    (the pickle-embedded ingress) or by the decoded
    :class:`~wool.runtime.context.snapshot.Snapshot`'s ``stub_pins``
    (the wire-snapshot ingress) until the receiver's user code
    declares the real variable, at which point
    :meth:`ContextVar.__new__` promotes the stub in place. A promoted
    variable remains in ``stub_pins`` for the rest of that snapshot's
    life — harmless, since a declared variable is a process-wide
    singleton anyway. If the receiver never declares the variable,
    the stub is collected with whatever held it and the propagated
    value is dropped.

    Pass *default* to seed the stub's constructor default before
    promotion when that information is available on the ingress side
    (the pickle path carries it; the wire-snapshot path does not).
    """
    # Local import breaks the wool.runtime.context.var ↔
    # wool.runtime.context.stub cycle: var imports stub helpers at
    # module level, so stub must defer its ContextVar import until
    # both modules have finished loading.
    from wool.runtime.context.var import ContextVar

    with lock:
        existing = var_registry.get(key)
        if existing is not None:
            # Fold the supplied default into a default-less stub: the
            # wire-snapshot path supplies no default (wire bytes don't
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
        namespace, name = key
        stub: ContextVar[Any] = object.__new__(ContextVar)
        stub._name = name
        stub._namespace = namespace
        stub._key = key
        stub._default = default
        stub._stub = True
        var_registry[key] = stub
        return stub
