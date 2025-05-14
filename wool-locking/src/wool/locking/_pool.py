import logging
from contextvars import ContextVar
from typing import Callable
from typing import Coroutine

import wool
import wool.locking
from wool.locking._session import LockPoolSession
from wool.locking._worker import LockScheduler


# PUBLIC
def pool(
    host: str = "localhost",
    port: int = 48800,
    *,
    authkey: bytes | None = None,
    breadth: int = 0,
    log_level: int = logging.INFO,
) -> Callable[[Callable[..., Coroutine]], Callable[..., Coroutine]]:
    """
    Convenience function to create a lock pool.

    :param address: The address of the worker pool (host, port).
    :param authkey: Optional authentication key for the pool.
    :param breadth: Number of worker processes in the pool. Defaults to CPU
        count.
    :param log_level: Logging level for the pool.
    :return: A decorator that wraps the function to execute within the pool.
    """
    return LockPool(
        address=(host, port),
        authkey=authkey,
        breadth=breadth,
        log_level=log_level,
    )


# PUBLIC
class LockPool(wool.WorkerPool):
    """
    A specialized worker pool for managing distributed locking tasks.

    This class extends the base `wool.Pool` to provide functionality
    specific to distributed locking. It uses a single worker (`breadth=1`)
    and integrates with `LockPoolSession` and `LockScheduler` to manage
    locking tasks effectively.

    :param args: Positional arguments passed to the base `wool.Pool`.
    :param kwargs: Keyword arguments passed to the base `wool.Pool`.
    """

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize the LockPool.

        :param args: Positional arguments passed to the base `wool.Pool`.
        :param kwargs: Keyword arguments passed to the base `wool.Pool`.
        """
        super().__init__(*args, breadth=1, **kwargs)

    @property
    def session_type(self) -> type[LockPoolSession]:
        """
        Get the session type associated with this pool.

        :return: The `LockPoolSession` class, which defines the session
                 behavior for this pool.
        """
        return LockPoolSession

    @property
    def session_context(self) -> ContextVar[wool.WorkerPoolSession]:
        """
        Get the context variable used to manage the session state.

        :return: The context variable for the current locking session.
        """
        return wool.locking.__locking_session__

    @property
    def scheduler_type(self) -> type[LockScheduler]:
        """
        Get the scheduler type associated with this pool.

        :return: The `LockScheduler` class, which defines the scheduling
                 behavior for this pool.
        """
        return LockScheduler
