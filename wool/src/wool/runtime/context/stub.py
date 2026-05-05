from __future__ import annotations

import weakref
from typing import TYPE_CHECKING
from typing import Any

from wool.runtime.context.registry import lock
from wool.runtime.context.registry import var_registry
from wool.runtime.typing import Undefined

if TYPE_CHECKING:
    from wool.runtime.context.base import Context
    from wool.runtime.context.var import ContextVar


# Weakly indexes :class:`StubPin` instances by var key for O(1)
# release on promotion. Entries auto-prune when a pin has no strong
# refs (all pinning :class:`Context` instances have died without
# promotion occurring).
_stub_pin_anchors: weakref.WeakValueDictionary[tuple[str, str], StubPin] = (
    weakref.WeakValueDictionary()
)


class StubPin:
    """Severable anchor that keeps a stub :class:`ContextVar` alive.

    Held strongly by each :class:`Context` that observed the stub's
    creation (via :attr:`Context._stub_pins`), and weakly indexed by
    var key in :data:`_stub_pin_anchors`. Because the only path from
    a :class:`Context` to its pinned stub goes through this anchor,
    promotion can release the stub in O(1) by nulling :attr:`stub`
    — live :class:`Context` instances retain the gutted anchor until
    they are themselves collected, but the stub itself is free to
    be reclaimed as soon as user references drop.
    """

    __slots__ = ("stub", "__weakref__")

    stub: ContextVar[Any] | None

    def __init__(self, stub: ContextVar[Any]) -> None:
        self.stub = stub


def pin_stub(stub: ContextVar[Any], ctx: Context) -> None:
    """Pin a freshly reconstructed stub to *ctx* and the global index."""
    anchor = StubPin(stub)
    _stub_pin_anchors[stub._key] = anchor
    ctx._stub_pins.add(anchor)


def release_stub(key: tuple[str, str]) -> None:
    """Release the stub pin for *key* so the stub can be reclaimed.

    Pops the anchor from the global index and severs its strong
    reference to the stub; any live :class:`Context` still holding
    the (now gutted) anchor in its pin set drops it naturally when
    the :class:`Context` itself is collected.
    """
    anchor = _stub_pin_anchors.pop(key, None)
    if anchor is not None:
        anchor.stub = None


def resolve_stub(
    key: tuple[str, str],
    ctx: Context,
    *,
    default: Any = Undefined,
) -> ContextVar[Any]:
    """Return the :class:`ContextVar` registered under *key*, creating a
    stub pinned to *ctx* if no authoritative declaration exists yet.

    Unifies the two ingress paths that may encounter an unregistered
    var key on a receiving process: the pickle-embedded :class:`ContextVar`
    instance path (via :meth:`ContextVar._reconstitute`) and the wire
    snapshot path (via :meth:`Context.from_protobuf`). Both route
    through this helper so a lazy-import receiver converges on the
    same state regardless of whether the value arrived as a bare
    wire entry or embedded in a pickled var reference.

    Pass *default* to seed the stub's default before promotion when
    that information is available on the ingress side (the pickle
    path carries it; the wire-snapshot path does not).
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
        pin_stub(stub, ctx)
        return stub
