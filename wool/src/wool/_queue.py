from queue import Queue
from time import time
from typing import Generic
from typing import TypeVar

T = TypeVar("T")


class TaskQueue(Queue, Generic[T]):
    """
    A task queue with support for tracking idle time.

    TaskQueue extends the standard Python `Queue` to include functionality 
    for determining whether the queue has been idle for a specified duration.

    :param maxsize: The maximum number of items allowed in the queue. Defaults 
        to 0 (unlimited size).
    :param maxidle: The maximum idle time in seconds before the queue is 
        considered idle. If None, idle time is not tracked.
    :raises ValueError: If `maxidle` is not positive.
    """

    def __init__(self, maxsize: int = 0, maxidle: int | None = None) -> None:
        """
        Initialize the TaskQueue.

        :param maxsize: The maximum number of items allowed in the queue. 
            Defaults to 0 (unlimited size).
        :param maxidle: The maximum idle time in seconds before the queue is 
            considered idle. If None, idle time is not tracked.
        :raises ValueError: If `maxidle` is not positive.
        """
        if maxidle is not None:
            if maxidle <= -1:
                raise ValueError(f"Max idle must be positive, got {maxidle}")
        super().__init__(maxsize)
        self.last_cleared: float | None = time()
        self.maxidle: int | None = maxidle

    def put(self, item: T, *args, **kwargs) -> None:
        """
        Add an item to the queue.

        :param item: The item to add to the queue.
        :param args: Additional positional arguments for the `Queue.put` 
            method.
        :param kwargs: Additional keyword arguments for the `Queue.put` 
            method.
        """
        return super().put(item, *args, **kwargs)

    def get(self, *args, **kwargs) -> T:
        """
        Remove and return an item from the queue.

        If the queue becomes empty after this operation, the `last_cleared` 
        timestamp is updated.

        :param args: Additional positional arguments for the `Queue.get` 
            method.
        :param kwargs: Additional keyword arguments for the `Queue.get` 
            method.
        :return: The item removed from the queue.
        """
        result = super().get(*args, **kwargs)
        if self.empty():
            self.last_cleared = time()
        return result

    def idle(self) -> bool:
        """
        Check if the queue has been idle for longer than the specified 
        `maxidle` time.

        :return: True if the queue is idle, False otherwise.
        """
        return (
            self.last_cleared is not None
            and self.maxidle is not None
            and (time() - self.last_cleared) >= self.maxidle
        )
