"""Isolated collision-probe routines for ST-002.

Lives in its own module (separate from :mod:`tests.integration.routines`)
so that collision state — the pinned :class:`wool.ContextVar`
instances — is scoped to this module's globals rather than polluting
the general routines module.

Both sibling coroutines construct distinct :class:`wool.ContextVar`
instances under the same ``namespace:name`` key. The first invocation
on a worker registers the key; the second raises
:class:`wool.ContextVarCollision` because the public constructor's
duplicate-key check fires on an already-registered (non-stub)
instance.

Each routine must preserve its constructed var between dispatches so
the registry's :class:`weakref.WeakValueDictionary` does not drop the
key before the sibling runs. ``cloudpickle`` transports routine
bodies via a snapshot of their ``__globals__``, so a write to a local
module-level dict lands in a throwaway dict on the worker and does
not persist across dispatches. The routines therefore reach for the
live :mod:`tests.integration._collision_fixtures` module via
:func:`importlib.import_module` and write the pin into the live
module dict, which does persist across dispatches on the same worker.
"""

from __future__ import annotations

import importlib

import wool

_COLLISION_KEY_NAMESPACE = "integration_collision_ns"
_COLLISION_KEY_NAME = "x"

# Strong-ref pin dict, populated on the worker side so constructed
# ContextVar instances outlive their routine's return and remain in
# :attr:`wool.ContextVar._registry` (a WeakValueDictionary) when the
# sibling routine runs.
_PINS: dict[str, wool.ContextVar] = {}


def _live_pins() -> dict[str, wool.ContextVar]:
    """Return the live module's ``_PINS`` dict.

    Cloudpickle ships the routine's ``__globals__`` as a snapshot; a
    direct ``_PINS[...]`` write in the routine body would land in a
    throwaway dict on the worker. Reaching the live module via
    :func:`importlib.import_module` pins the var into state that
    persists across dispatches on the same worker process.
    """
    mod = importlib.import_module(__name__)
    return mod._PINS


@wool.routine
async def sibling_a() -> str:
    """First-to-dispatch sibling that registers the collision key."""
    var = wool.ContextVar(
        _COLLISION_KEY_NAME,
        namespace=_COLLISION_KEY_NAMESPACE,
        default="sibling-a",
    )
    _live_pins()["a"] = var
    return var.get()


@wool.routine
async def sibling_b() -> str:
    """Second sibling whose construction collides with :func:`sibling_a`.

    Constructs a distinct :class:`wool.ContextVar` under the same key
    as :func:`sibling_a`. Raises :class:`wool.ContextVarCollision`
    when the two dispatch to the same worker (the key is already
    registered and non-stub after the first sibling ran).
    """
    var = wool.ContextVar(
        _COLLISION_KEY_NAME,
        namespace=_COLLISION_KEY_NAMESPACE,
        default="sibling-b",
    )
    _live_pins()["b"] = var
    return var.get()
