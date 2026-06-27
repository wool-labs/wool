"""Shared `ContextVar` registry, factored out so the context
modules can import it without forming an import cycle."""

from __future__ import annotations

import threading
import weakref
from typing import TYPE_CHECKING
from typing import Any
from typing import Final

if TYPE_CHECKING:
    from wool.runtime.context.var import ContextVar


# Serializes ``var_registry`` registration so concurrent declarations of
# the same ``(namespace, name)`` key cannot observe an intermediate
# state — see `ContextVar.__new__` and `resolve_stub`.
lock: Final[threading.Lock] = threading.Lock()


# Process-wide identity map for ``(namespace, name) → ContextVar``.
# Weak values so unreferenced stubs are reclaimed when the embedding
# object graph drops them; ``lock`` above guards concurrent
# registration so two threads cannot observe an intermediate state.
var_registry: Final[weakref.WeakValueDictionary[tuple[str, str], ContextVar[Any]]] = (
    weakref.WeakValueDictionary()
)
