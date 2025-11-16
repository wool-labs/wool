from __future__ import annotations

from contextvars import ContextVar
from contextvars import Token
from typing import TYPE_CHECKING
from typing import Final

from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

if TYPE_CHECKING:
    from wool.runtime.worker.auth import WorkerCredentials

dispatch_timeout: Final[ContextVar[float | None]] = ContextVar(
    "dispatch_timeout", default=None
)
credentials: Final[ContextVar[WorkerCredentials | None]] = ContextVar(
    "credentials", default=None
)


# public
class RuntimeContext:
    """Runtime context for configuring Wool behavior.

    Provides context-managed configuration for dispatch timeout and
    worker credentials, allowing defaults to be set for all workers
    within a context.

    :param dispatch_timeout:
        Default timeout for task dispatch operations.
    :param credentials:
        Default WorkerCredentials for secure worker connections.
    """

    _dispatch_timeout: float | None | UndefinedType
    _dispatch_timeout_token: Token | UndefinedType
    _credentials: WorkerCredentials | None | UndefinedType
    _credentials_token: Token | UndefinedType

    def __init__(
        self,
        *,
        dispatch_timeout: float | None | UndefinedType = Undefined,
        credentials: WorkerCredentials | None | UndefinedType = Undefined,
    ):
        self._dispatch_timeout = dispatch_timeout
        self._credentials = credentials

    def __enter__(self):
        if self._dispatch_timeout is not Undefined:
            self._dispatch_timeout_token = dispatch_timeout.set(self._dispatch_timeout)
        else:
            self._dispatch_timeout_token = Undefined

        if self._credentials is not Undefined:
            self._credentials_token = credentials.set(self._credentials)
        else:
            self._credentials_token = Undefined

        return self

    def __exit__(self, *_):
        if self._dispatch_timeout_token is not Undefined:
            dispatch_timeout.reset(self._dispatch_timeout_token)

        if self._credentials_token is not Undefined:
            credentials.reset(self._credentials_token)

    @classmethod
    def get_current(cls) -> "RuntimeContext":
        """Get the current runtime context.

        Returns a RuntimeContext instance with the current context values.

        :returns:
            RuntimeContext with current context variable values.
        """
        ctx = cls()
        ctx._dispatch_timeout = dispatch_timeout.get()
        ctx._credentials = credentials.get()
        return ctx

    @property
    def credentials(self) -> WorkerCredentials | None:
        """Get the current worker credentials.

        :returns:
            The WorkerCredentials instance, or None if not set.
        """
        return credentials.get() if self._credentials is Undefined else self._credentials
