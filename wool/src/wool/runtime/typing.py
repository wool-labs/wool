from __future__ import annotations

from enum import Enum
from typing import AsyncContextManager
from typing import Awaitable
from typing import Callable
from typing import ContextManager
from typing import Final
from typing import TypeAlias
from typing import TypeVar
from typing import final

F = TypeVar("F", bound=Callable)
W = TypeVar("W", bound=Callable)
Wrapper = Callable[[F], W]
PassthroughWrapper = Callable[[F], F]


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
"""Union of forms accepted wherever wool needs a lazily-resolved service.

Accepted forms:

- ``T`` — a direct instance.
- ``Callable[[], T]`` — a callable returning an instance.
- ``Callable[[], ContextManager[T]]`` — a callable returning a sync CM.
- ``Callable[[], AsyncContextManager[T]]`` — a callable returning an async CM.
- ``ContextManager[T]`` — a pre-called sync context manager.
- ``AsyncContextManager[T]`` — a pre-called async context manager.
- ``Awaitable[T]`` — an awaitable resolving to an instance.

.. caution::

   The two pre-called context manager forms (``ContextManager[T]`` and
   ``AsyncContextManager[T]``) are **not picklable**.  If the resulting
   object is serialized — as happens during nested routine dispatch —
   ``cloudpickle`` will raise a ``TypeError``.  Wrap the context manager
   in a zero-argument callable instead::

       # Bad — pre-called CM, not picklable
       WorkerProxy(discovery=my_discovery_cm())

       # Good — callable returning a CM, picklable
       WorkerProxy(discovery=my_discovery_cm)
"""
