from __future__ import annotations

from contextvars import ContextVar

import wool
import wool.locking


class LockPoolSession(wool.PoolSession):
    """
    A session for managing distributed locking tasks within a `LockPool`.

    This class extends `wool.PoolSession` to provide session-specific
    behavior for distributed locking. It integrates with the context
    variable used to manage the current locking session.

    :seealso: `wool.PoolSession` for the base session implementation.
    """

    @property
    def session(self) -> ContextVar[wool.PoolSession]:
        """
        Get the context variable for the current locking session.

        This property provides access to the context variable that
        manages the state of the current session. It ensures that
        session-specific data is isolated and accessible within the
        appropriate context.

        :return: The context variable for the current locking session.
        """
        return wool.locking.__locking_session__
