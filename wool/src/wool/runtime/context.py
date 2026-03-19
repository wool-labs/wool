from __future__ import annotations

from contextvars import ContextVar
from contextvars import Token
from typing import TYPE_CHECKING
from typing import Final

from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

if TYPE_CHECKING:
    from wool import protocol

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
    def get_current(cls) -> RuntimeContext:
        """Get the current runtime context.

        Returns a RuntimeContext instance with the current context values.

        :returns:
            RuntimeContext with current context variable values.
        """
        return cls(dispatch_timeout=dispatch_timeout.get())

    def to_protobuf(self) -> protocol.RuntimeContext:
        """Serialize the wire-safe subset of this context to protobuf.

        Only ``dispatch_timeout`` is propagated over the wire.

        :returns:
            A protobuf ``RuntimeContext`` message.
        """
        from wool import protocol

        dt = self._dispatch_timeout
        if dt is Undefined:
            dt = dispatch_timeout.get()
        pb = protocol.RuntimeContext()
        if dt is not None:
            pb.dispatch_timeout = dt
        return pb

    @classmethod
    def from_protobuf(cls, context: protocol.RuntimeContext) -> RuntimeContext:
        """Reconstruct a ``RuntimeContext`` from a protobuf message.

        No runtime import of ``protocol`` is needed here — the message
        is received as a parameter rather than constructed.

        :param context:
            A protobuf ``RuntimeContext`` message.
        :returns:
            A ``RuntimeContext`` instance.
        """
        return cls(
            dispatch_timeout=context.dispatch_timeout
            if context.HasField("dispatch_timeout")
            else None,
        )
