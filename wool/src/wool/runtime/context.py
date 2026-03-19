from __future__ import annotations

from contextvars import ContextVar
from contextvars import Token
from typing import Final

from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

dispatch_timeout: Final[ContextVar[float | None]] = ContextVar(
    "dispatch_timeout", default=None
)


# public
class RuntimeContext:
    """Runtime context for configuring Wool behavior.

    Provides context-managed configuration for dispatch timeout,
    allowing defaults to be set for all workers within a context.

    :param dispatch_timeout:
        Default timeout for task dispatch operations.
    """

    _dispatch_timeout: float | None | UndefinedType
    _dispatch_timeout_token: Token | UndefinedType

    def __init__(
        self,
        *,
        dispatch_timeout: float | None | UndefinedType = Undefined,
    ):
        self._dispatch_timeout = dispatch_timeout

    def __enter__(self):
        if self._dispatch_timeout is not Undefined:
            self._dispatch_timeout_token = dispatch_timeout.set(self._dispatch_timeout)
        else:
            self._dispatch_timeout_token = Undefined

        return self

    def __exit__(self, *_):
        if self._dispatch_timeout_token is not Undefined:
            dispatch_timeout.reset(self._dispatch_timeout_token)

    @classmethod
    def get_current(cls) -> "RuntimeContext":
        """Get the current runtime context.

        Returns a RuntimeContext instance with the current context values.

        :returns:
            RuntimeContext with current context variable values.
        """
        ctx = cls()
        ctx._dispatch_timeout = dispatch_timeout.get()
        return ctx
