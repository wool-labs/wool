from __future__ import annotations

from contextvars import ContextVar
from typing import Any
from typing import Final

from wool.runtime.resourcepool import ResourcePool

__subscriber_pool__: Final[ContextVar[ResourcePool[Any] | None]] = ContextVar(
    "__subscriber_pool__", default=None
)
