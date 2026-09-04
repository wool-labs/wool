"""Type aliases and the dependency resolver shared by the runtime."""

from __future__ import annotations

from contextlib import AsyncExitStack
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
    """The single-member enum whose member is the `Undefined` sentinel."""

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
"""The forms a configured dependency may take: an awaitable, a sync or
async context manager, or a zero-argument callable producing an instance
or any of those — see `resolved` for how one is entered.
"""


@asynccontextmanager
async def resolved(
    dependency: T | Factory[T], *, expect: type | tuple[type, ...] | None = None
) -> AsyncIterator[T]:
    """Enter a configured dependency and yield the live object.

    Accepts a bare instance or any `Factory` form. Forms are tried in a
    fixed order, i.e., sync context manager, async context manager,
    callable, awaitable, bare instance, and the first that matches wins,
    so an
    instance that is also a context manager is entered here and exited
    when the block ends. A context manager's exit receives the block's
    exception info, and its return value is ignored, so a manager that
    suppresses cannot swallow the block's failure.

    :param dependency:
        The instance, or the `Factory` producing it.
    :param expect:
        A type, or tuple of types, the resolved object must be an
        instance of. Checked once the object is in hand, so a factory
        that produces the wrong thing is exited with the failure.
    :yields:
        The resolved object, for the duration of the block.
    :raises TypeError:
        If ``expect`` is given and the resolved object is not an
        instance of it.
    """
    async with _resolved(dependency) as obj:
        if expect is not None and not isinstance(obj, expect):
            names = (
                " | ".join(t.__name__ for t in expect)
                if isinstance(expect, tuple)
                else expect.__name__
            )
            raise TypeError(f"Expected {names}, got: {type(obj)}")
        yield obj


@asynccontextmanager
async def _resolved(dependency: T | Factory[T]) -> AsyncIterator[T]:
    """Enter ``dependency`` by its first matching form — see `resolved`."""
    if isinstance(dependency, (ContextManager, AsyncContextManager)):
        stack = AsyncExitStack()
        if isinstance(dependency, ContextManager):
            obj = stack.enter_context(dependency)
        else:
            obj = await stack.enter_async_context(dependency)
        try:
            yield cast(T, obj)
        except BaseException as exc:
            # Verdict discarded — see resolved's docstring.
            await stack.__aexit__(type(exc), exc, exc.__traceback__)
            raise
        else:
            await stack.__aexit__(None, None, None)
    elif callable(dependency):
        async with _resolved(dependency()) as obj:
            yield cast(T, obj)
    elif isinstance(dependency, Awaitable):
        yield cast(T, await dependency)
    else:
        yield cast(T, dependency)
