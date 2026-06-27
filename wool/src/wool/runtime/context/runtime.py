"""Block-scoped runtime option overrides.

Houses `RuntimeContext` — the user-facing context manager
that overrides per-chain runtime options (currently `dispatch_timeout`)
for the duration of a ``with`` block — and the ambient
`dispatch_timeout` `contextvars.ContextVar` it scopes.

Distinct from `wool.runtime.context.chain` (the
`Chain` chain-state model) and
`wool.runtime.context.manifest` (the chain-manifest codec): runtime
options are a separate concern from Wool's chain-context machinery.
`dispatch_timeout` is a plain stdlib `contextvars.ContextVar`
— it does *not* ride the Wool-owned `wool.__chain__` variable
and is therefore unaffected by `Chain.mount`. Each dispatch
encodes its current value via `RuntimeContext` and the worker
re-installs that value before running the routine.
"""

from __future__ import annotations

import contextvars
from typing import Final

from wool import protocol
from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

# Ambient per-chain dispatch timeout in seconds. ``None`` means no
# timeout. The value scopes to whichever execution chain is currently
# active and rides through nested dispatches until reset or
# overridden. This is a plain stdlib `contextvars.ContextVar` —
# distinct from the Wool-owned context variable that carries
# `wool.ContextVar` state.
dispatch_timeout: Final[contextvars.ContextVar[float | None]] = contextvars.ContextVar(
    "dispatch_timeout", default=None
)


# public
class RuntimeContext:
    """Block-scoped runtime option overrides for wool routines.

    Used as a context manager to override runtime options (currently
    only `dispatch_timeout`) for the duration of a block. Auto-
    captured on every `Task` at construction time and serialised
    onto each dispatch so the worker restores the caller's effective
    `dispatch_timeout` before running the routine.

    :param dispatch_timeout:
        Default timeout for task dispatch operations as a positive
        ``float``. Omit (or pass ``None``) to inherit the surrounding
        scope's `dispatch_timeout`. ``None`` and ``Undefined`` are
        observationally equivalent on the wire: `to_protobuf`
        substitutes the live `dispatch_timeout` at encode time on
        inherit, and omits
        the field when the resolved value is ``None``. So a bare
        ``RuntimeContext()`` constructed for wire transport still
        propagates the encoder's effective timeout to the receiver.
    """

    _dispatch_timeout: float | UndefinedType
    _dispatch_timeout_token: contextvars.Token[float | None] | None

    def __init__(
        self,
        *,
        dispatch_timeout: float | None | UndefinedType = Undefined,
    ) -> None:
        # Collapse ``None`` to ``Undefined`` on input. The two
        # were already observationally equivalent on the wire; this
        # extends the equivalence to ``__enter__`` so a caller passing
        # explicit ``None`` no longer surprisingly overrides the
        # surrounding scope's timeout to ``None``.
        self._dispatch_timeout = (
            Undefined if dispatch_timeout is None else dispatch_timeout
        )
        self._dispatch_timeout_token = None

    def __enter__(self) -> RuntimeContext:
        # Block-scoped, single-use: re-entering would overwrite the
        # outer token without releasing it, leaking the original
        # ``dispatch_timeout`` binding for the chain's lifetime.
        if self._dispatch_timeout_token is not None:
            raise RuntimeError(
                "RuntimeContext is already active in a `with` block; "
                "instances are block-scoped and single-use as context "
                "managers"
            )
        if self._dispatch_timeout is not Undefined:
            self._dispatch_timeout_token = dispatch_timeout.set(self._dispatch_timeout)
        return self

    def __exit__(self, *_):
        if self._dispatch_timeout_token is not None:
            dispatch_timeout.reset(self._dispatch_timeout_token)
            self._dispatch_timeout_token = None

    @classmethod
    def get_current(cls) -> RuntimeContext:
        """Capture the current stdlib `dispatch_timeout` value."""
        return cls(dispatch_timeout=dispatch_timeout.get())

    @classmethod
    def from_protobuf(cls, context: protocol.RuntimeContext) -> RuntimeContext:
        """Reconstruct from a `protocol.RuntimeContext` message.

        Mirrors `to_protobuf`'s omit-when-None semantic: an
        absent ``dispatch_timeout`` field on the wire decodes to the
        `Undefined` sentinel rather than explicit ``None`` so
        the receiver's existing scope inherits through unchanged.
        Decoding the absent field as explicit ``None`` would force
        the receiver's `dispatch_timeout` to ``None`` on
        `__enter__`, silently overriding whatever timeout the
        receiver's scope had set — a round-trip non-identity for the
        "encoder had no timeout, receiver had a scope timeout" case.
        """
        return cls(
            dispatch_timeout=(
                context.dispatch_timeout
                if context.HasField("dispatch_timeout")
                else Undefined
            )
        )

    def to_protobuf(self) -> protocol.RuntimeContext:
        """Serialize to a `protocol.RuntimeContext` message.

        When the instance was constructed without an explicit
        ``dispatch_timeout`` (i.e., the default sentinel), the live
        `dispatch_timeout` value from the current scope is
        captured at encode time and rides the wire. An explicit
        `None` skips emission, so the receiver inherits its
        own scope's default.
        """
        message = protocol.RuntimeContext()
        timeout = self._dispatch_timeout
        if timeout is Undefined:
            timeout = dispatch_timeout.get()
        if timeout is not None:
            message.dispatch_timeout = timeout
        return message


__all__ = ["RuntimeContext", "dispatch_timeout"]
