from __future__ import annotations

from contextvars import ContextVar
from typing import Any
from typing import Final

from wool.runtime.cache import ReferenceCountedCache

__subscriber_pool__: Final[ContextVar[ReferenceCountedCache[Any] | None]] = ContextVar(
    "__subscriber_pool__", default=None
)
