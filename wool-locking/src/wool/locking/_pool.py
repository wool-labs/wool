from contextvars import ContextVar

import wool
import wool.locking
from wool.locking._session import LockPoolSession
from wool.locking._worker import LockScheduler


class LockPool(wool.Pool):
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
    def session_context(self) -> ContextVar[wool.PoolSession]:
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
