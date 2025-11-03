from __future__ import annotations

from typing import AsyncContextManager
from typing import Awaitable
from typing import Callable
from typing import ContextManager
from typing import Final
from typing import TypeAlias
from typing import TypeVar

T_CO: Final = TypeVar("T_CO", covariant=True)


# public
Factory: TypeAlias = (
    Awaitable[T_CO]
    | AsyncContextManager[T_CO]
    | ContextManager[T_CO]
    | Callable[
        [], T_CO | Awaitable[T_CO] | AsyncContextManager[T_CO] | ContextManager[T_CO]
    ]
)
