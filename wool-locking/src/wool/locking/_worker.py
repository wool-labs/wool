from contextvars import ContextVar

import wool
import wool.locking


class LockScheduler(wool.Scheduler):
    """
    A scheduler for managing task execution in a `LockPool`.

    This class extends `wool.Scheduler` to provide scheduling behavior
    specific to distributed locking tasks. It integrates with the context
    variable used to manage the current locking session.

    :seealso: `wool.Scheduler` for the base scheduler implementation.
    """

    @property
    def session_context(self) -> ContextVar[wool.PoolSession]:
        """
        Get the context variable for the current locking session.

        This property provides access to the context variable that
        manages the state of the current session. It ensures that
        session-specific data is isolated and accessible within the
        appropriate context.

        :return: The context variable for the current locking session.
        """
        return wool.locking.__locking_session__
