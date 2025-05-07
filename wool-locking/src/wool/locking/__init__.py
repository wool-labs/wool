from contextvars import ContextVar
from typing import Final

from wool.locking._client import LockClient
from wool.locking._lock import lock
from wool.locking._pool import LockPool

__lock_client__: Final[ContextVar[LockClient]] = ContextVar("__wool_client__")

__all__ = [
    "LockClient",
    "LockPool",
    "lock",
    "__lock_client__",
]

for symbol in __all__:
    attribute = globals().get(symbol)
    try:
        if attribute and "wool" in attribute.__module__.split("."):
            # Set the module to reflect imports of the symbol
            attribute.__module__ = __name__
    except AttributeError:
        continue
