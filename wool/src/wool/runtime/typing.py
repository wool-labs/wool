from __future__ import annotations

from contextlib import asynccontextmanager
from enum import Enum
from typing import AsyncContextManager
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import ContextManager
from typing import Final
from typing import TypeAlias
from typing import TypeVar
from typing import cast
from typing import final

F = TypeVar("F", bound=Callable)
W = TypeVar("W", bound=Callable)
Wrapper = Callable[[F], W]
PassthroughWrapper = Callable[[F], F]


# public
@final
class UndefinedType(Enum):
    Undefined = "Undefined"


Undefined: Final = UndefinedType.Undefined


T_CO: Final = TypeVar("T_CO", covariant=True)
T = TypeVar("T")

# public
Factory: TypeAlias = (
    Awaitable[T_CO]
    | AsyncContextManager[T_CO]
    | ContextManager[T_CO]
    | Callable[
        [], T_CO | Awaitable[T_CO] | AsyncContextManager[T_CO] | ContextManager[T_CO]
    ]
)


@asynccontextmanager
async def resolved(dependency: T | Factory[T]) -> AsyncIterator[T]:
    """Enter a configured dependency and yield the live object.

    Accepts a bare instance, or any `Factory` form: an awaitable, a sync
    or async context manager, or a callable producing one of those.
    Forms are tried in that order — sync context manager, async context
    manager, callable, awaitable, bare instance — and the first that
    matches wins, so an instance that is also a context manager is
    entered here and exited when the block ends. A context manager's
    exit receives the block's exception info, and its return value is
    ignored, so a manager that suppresses cannot swallow the block's
    failure.
    """
    if isinstance(dependency, ContextManager):
        obj = dependency.__enter__()
        try:
            yield obj
        except BaseException as exc:
            dependency.__exit__(type(exc), exc, exc.__traceback__)
            raise
        else:
            dependency.__exit__(None, None, None)
    elif isinstance(dependency, AsyncContextManager):
        obj = await dependency.__aenter__()
        try:
            yield obj
        except BaseException as exc:
            await dependency.__aexit__(type(exc), exc, exc.__traceback__)
            raise
        else:
            await dependency.__aexit__(None, None, None)
    elif callable(dependency):
        async with resolved(dependency()) as obj:
            yield cast(T, obj)
    elif isinstance(dependency, Awaitable):
        yield cast(T, await dependency)
    else:
        yield cast(T, dependency)
