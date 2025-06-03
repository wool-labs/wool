from typing import Annotated
from typing import Callable
from typing import Literal
from typing import SupportsInt
from typing import TypeVar

from annotated_types import Gt

T = TypeVar("T", bound=SupportsInt)
Positive = Annotated[T, Gt(0)]

Zero = Literal[0]

F = TypeVar("F", bound=Callable)
W = TypeVar("W", bound=Callable)
Decorator = Callable[[F], W]
PassthroughDecorator = Callable[[F], F]
