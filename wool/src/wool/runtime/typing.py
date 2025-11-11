from __future__ import annotations

from enum import Enum
from typing import AsyncContextManager
from typing import Awaitable
from typing import Callable
from typing import ContextManager
from typing import Coroutine
from typing import Final
from typing import TypeAlias
from typing import TypeVar
from typing import final

F = TypeVar("F", bound=Callable)
W = TypeVar("W", bound=Callable)
Wrapper = Callable[[F], W]
PassthroughWrapper = Callable[[F], F]

AsyncCallable = Callable[..., Coroutine]


@final
class UndefinedType(Enum):
    Undefined = "Undefined"


Undefined: Final = UndefinedType.Undefined


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
