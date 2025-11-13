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
    _dispatch_timeout: float | None | UndefinedType
    _dispatch_timeout_token: Token | UndefinedType

    def __init__(self, *, dispatch_timeout: float | None | UndefinedType = Undefined):
        self._dispatch_timeout = dispatch_timeout

    def __enter__(self):
        if self._dispatch_timeout is not Undefined:
            self._dispatch_timeout_token = dispatch_timeout.set(self._dispatch_timeout)
        else:
            self._dispatch_timeout_token = Undefined

    def __exit__(self, *_):
        if self._dispatch_timeout_token is not Undefined:
            dispatch_timeout.reset(self._dispatch_timeout_token)
