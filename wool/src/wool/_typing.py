from typing import Callable
from typing import TypeVar

F = TypeVar("F", bound=Callable)
W = TypeVar("W", bound=Callable)
Decorator = Callable[[F], W]
PassthroughDecorator = Callable[[F], F]
