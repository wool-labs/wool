from contextvars import Token as Token

from wool.runtime.context.base import current_context as current_context
from wool.runtime.context.errors import ContextDecodeError as ContextDecodeError
from wool.runtime.context.errors import ContextDecodeWarning as ContextDecodeWarning
from wool.runtime.context.errors import (
    ContextSerializationError as ContextSerializationError,
)
from wool.runtime.context.errors import SerializationError as SerializationError
from wool.runtime.context.errors import SerializationWarning as SerializationWarning
from wool.runtime.context.errors import WoolError as WoolError
from wool.runtime.context.factory import TaskFactoryDisplaced as TaskFactoryDisplaced
from wool.runtime.context.factory import install_task_factory as install_task_factory
from wool.runtime.context.guard import ChainContention as ChainContention
from wool.runtime.context.runtime import RuntimeContext as RuntimeContext
from wool.runtime.context.threading import to_thread as to_thread
from wool.runtime.context.var import ContextVar as ContextVar
from wool.runtime.context.var import ContextVarCollision as ContextVarCollision

# Q6 — ``Context``, ``context_is_armed``, ``current_wire_context``,
# ``dispatch_timeout`` are no longer re-exported through this package.
# They live in :mod:`wool.runtime.context.base` and are package-
# internal — callers that need them import from ``base`` directly.
# Picks one signal per symbol: a name appearing on the package surface
# is documented as part of the public API; everything else is reached
# via its owning module.

__all__ = [
    "ChainContention",
    "ContextDecodeError",  # alias of ContextSerializationError (back-compat)
    "ContextDecodeWarning",  # alias of SerializationWarning (back-compat)
    "ContextSerializationError",
    "ContextVar",
    "ContextVarCollision",
    "RuntimeContext",
    "SerializationError",
    "SerializationWarning",
    "TaskFactoryDisplaced",
    "Token",
    "WoolError",
    "current_context",
    "install_task_factory",
    "to_thread",
]
