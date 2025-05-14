from __future__ import annotations

from contextvars import ContextVar
from typing import Callable
from typing import Coroutine

import wool
import wool.locking


# PUBLIC
def session(
    host: str = "localhost",
    port: int = 48800,
    *,
    authkey: bytes | None = None,
) -> Callable[[Callable[..., Coroutine]], Callable[..., Coroutine]]:
    """
    Convenience function to declare a lock pool session context. Usage is
    identical to that of ``wool.session``.

    :param host: The hostname of the worker pool.
    :param port: The port of the worker pool.
    :param authkey: Optional authentication key for the worker pool.
    :return: A decorator that wraps the function to execute within the session.

    .. seealso:: `wool.session`
    """
    return LockPoolSession((host, port), authkey=authkey)


# PUBLIC
class LockPoolSession(wool.WorkerPoolSession):
    """
    A session for managing distributed locking tasks within a `LockPool`.

    This class extends `wool.PoolSession` to provide session-specific
    behavior for distributed locking. It integrates with the context
    variable used to manage the current locking session.

    :seealso: `wool.PoolSession` for the base session implementation.
    """

    @property
    def session(self) -> ContextVar[wool.WorkerPoolSession]:
        """
        Get the context variable for the current locking session.

        This property provides access to the context variable that
        manages the state of the current session. It ensures that
        session-specific data is isolated and accessible within the
        appropriate context.

        :return: The context variable for the current locking session.
        """
        return wool.locking.__locking_session__
