from __future__ import annotations

import contextvars
from typing import Final

from wool import protocol
from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

# Ambient per-chain dispatch timeout in seconds. ``None`` means no
# timeout. The value scopes to whichever execution chain is currently
# active and rides through nested dispatches until reset or
# overridden. This is a plain stdlib :class:`contextvars.ContextVar` —
# distinct from the Wool-owned snapshot variable that carries
# :class:`wool.ContextVar` state.
dispatch_timeout: Final[contextvars.ContextVar[float | None]] = contextvars.ContextVar(
    "dispatch_timeout", default=None
)


# public
class ContextDecodeWarning(RuntimeWarning):
    """Emitted when a wire :class:`protocol.Context` fails to decode.

    Wool's wire protocol treats context propagation as ancillary
    state — failures to decode incoming context never preempt the
    primary signal (the routine's return value or raised exception).
    Instead a :class:`ContextDecodeWarning` is emitted so callers
    that depend on context state can detect the inconsistency.

    Callers that prefer strict semantics — treat any decode failure
    as fatal — can opt in by promoting the warning to an exception::

        import warnings
        import wool

        warnings.filterwarnings("error", category=wool.ContextDecodeWarning)

    Under strict mode, per-variable failures inside
    :func:`~wool.runtime.context.snapshot.encode_snapshot` and
    :func:`~wool.runtime.context.snapshot.decode_snapshot` aggregate
    into a single :class:`BaseExceptionGroup` raised after the loop
    completes — every bad variable surfaces, not just the first.
    """


# public
class RuntimeContext:
    """Block-scoped runtime option overrides for wool routines.

    Used as a context manager to override runtime options (currently
    only :data:`dispatch_timeout`) for the duration of a block. Auto-
    captured on every :class:`Task` at construction time, which ships
    the caller's snapshot across the wire so the worker restores it
    before running the routine.

    :param dispatch_timeout:
        Default timeout for task dispatch operations. ``None`` means
        no timeout. Leaving this argument out (the default sentinel)
        has two effects: ``__enter__`` skips setting the stdlib
        variable — useful for "no-override" usage as a context
        manager — and :meth:`to_protobuf` substitutes the current
        scope's live :data:`dispatch_timeout` at encode time, so a
        bare ``RuntimeContext()`` constructed for wire transport still
        propagates the encoder's effective timeout to the receiver.
    """

    _dispatch_timeout: float | None | UndefinedType
    _dispatch_timeout_token: contextvars.Token[float | None] | None

    def __init__(
        self,
        *,
        dispatch_timeout: float | None | UndefinedType = Undefined,
    ) -> None:
        self._dispatch_timeout = dispatch_timeout
        self._dispatch_timeout_token = None

    def __enter__(self) -> RuntimeContext:
        if self._dispatch_timeout is not Undefined:
            self._dispatch_timeout_token = dispatch_timeout.set(self._dispatch_timeout)
        return self

    def __exit__(self, *_):
        if self._dispatch_timeout_token is not None:
            dispatch_timeout.reset(self._dispatch_timeout_token)
            self._dispatch_timeout_token = None

    @classmethod
    def get_current(cls) -> RuntimeContext:
        """Capture the current stdlib :data:`dispatch_timeout` value."""
        return cls(dispatch_timeout=dispatch_timeout.get())

    @classmethod
    def from_protobuf(cls, context: protocol.RuntimeContext) -> RuntimeContext:
        """Reconstruct from a :class:`protocol.RuntimeContext` message."""
        return cls(
            dispatch_timeout=(
                context.dispatch_timeout
                if context.HasField("dispatch_timeout")
                else None
            )
        )

    def to_protobuf(self) -> protocol.RuntimeContext:
        """Serialize to a :class:`protocol.RuntimeContext` message.

        When the instance was constructed without an explicit
        ``dispatch_timeout`` (i.e., the default sentinel), the live
        :data:`dispatch_timeout` value from the current scope is
        captured at encode time and rides the wire. An explicit
        :data:`None` skips emission, so the receiver inherits its
        own scope's default.
        """
        message = protocol.RuntimeContext()
        timeout = self._dispatch_timeout
        if timeout is Undefined:
            timeout = dispatch_timeout.get()
        if timeout is not None:
            message.dispatch_timeout = timeout
        return message
