from queue import Queue
from time import time
from typing import Generic, TypeVar

T = TypeVar("T")


class TaskQueue(Queue, Generic[T]):
    def __init__(self, maxsize: int = 0, maxidle: int | None = None) -> None:
        if maxidle is not None:
            if maxidle <= -1:
                raise ValueError(f"Max idle must be positive, got {maxidle}")
        super().__init__(maxsize)
        self.last_cleared: float | None = time()
        self.maxidle: int | None = maxidle

    def put(self, item: T, *args, **kwargs) -> None:
        return super().put(item, *args, **kwargs)

    def get(self, *args, **kwargs) -> T:
        result = super().get(*args, **kwargs)
        if self.empty():
            self.last_cleared = time()
        return result

    def idle(self) -> bool:
        return (
            self.last_cleared is not None
            and self.maxidle is not None
            and (time() - self.last_cleared) >= self.maxidle
        )
