from typing import Annotated, Callable, Literal, SupportsInt, TypeVar

from annotated_types import Gt

T = TypeVar("T", bound=SupportsInt)
Positive = Annotated[T, Gt(0)]

Zero = Literal[0]

F = TypeVar("F", bound=Callable)
W = TypeVar("W", bound=Callable)
Decorator = Callable[[F], W]
PassthroughDecorator = Callable[[F], F]
