from contextvars import ContextVar
from typing import Final

import wool
from wool.locking._lock import lock
from wool.locking._pool import LockPool
from wool.locking._pool import pool
from wool.locking._session import LockPoolSession
from wool.locking._session import session

__locking_session__: Final[ContextVar[wool.WorkerPoolSession]] = ContextVar(
    "__locking_session__", default=wool.LocalSession()
)

__all__ = ["LockPool", "LockPoolSession", "lock", "pool", "session"]

for symbol in __all__:
    attribute = globals().get(symbol)
    try:
        if attribute and "wool" in attribute.__module__.split("."):
            # Set the module to reflect imports of the symbol
            attribute.__module__ = __name__
    except AttributeError:
        continue
