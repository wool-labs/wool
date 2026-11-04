"""A failure held for later re-raising.

Provides `FailureReport`: the record a container keeps of the one
exception it owes every caller that asks it for something the failure
made impossible.
"""

from __future__ import annotations

from types import TracebackType
from typing import NoReturn


class FailureReport:
    """One exception, raised to many callers, without accumulating.

    Wraps a failure that outlives the frame that produced it and is owed
    to every caller that later asks for what it made impossible — the
    consumers of a shared subscription, the dispatches of a proxy whose
    membership feed died. Those callers must each receive an exception
    they can match on, so the instance itself is handed to all of them
    rather than rebuilt: reconstructing it assumes a constructor that
    accepts its own ``args``, which an arbitrary exception need not, and
    wrapping it would defeat the ``except`` clause the caller wrote.

    Raising one instance repeatedly costs what the first raise does not.
    Each propagation prepends a traceback node per frame it unwinds
    through, and each node pins that frame's locals, so a held failure
    raised often enough retains a slice of every call that ever received
    it. `contextlib.AsyncExitStack` compounds it from the other side,
    writing a body's exception onto whatever a teardown callback raises.

    This holds the failure's traceback, context and context-suppression
    flag as they stood at the first report and restores them before each
    raise. A caller's traceback is then the origin plus their own call,
    rather than the origin plus every call that came before, and
    identity and type are untouched. ``__cause__`` is not among the
    three: an explicit cause, set by a later ``raise ... from``, is
    permanent.

    What this bounds is growth, not retention. The depth a caller
    receives is the same at the hundredth report as at the first, which
    is the accumulation this exists to prevent. The origin traceback
    itself is held for the report's life, and with it every frame the
    failure was raised through and whatever those frames' locals reach
    — for a container that recorded a failure from its own machinery,
    that machinery. A report is therefore sized by where its failure
    came from, and is worth clearing once nothing is owed it.

    Restoring the context cannot survive a raise the interpreter
    restamps. CPython sets ``__context__`` at raise time, so reporting
    from inside an active ``except`` block replaces the held context
    with whatever is being handled there — report from outside the
    handler wherever the chaining is part of the answer.

    :param failure:
        The exception to hold. Its chaining state is captured as it
        stands, so construct the report where the failure is first
        caught rather than after re-raising it.
    """

    __slots__ = ("failure", "_context", "_suppress_context", "_traceback")

    def __init__(self, failure: BaseException) -> None:
        self.failure = failure
        self._traceback: TracebackType | None = failure.__traceback__
        self._context: BaseException | None = failure.__context__
        self._suppress_context: bool = failure.__suppress_context__

    def raise_failure(self) -> NoReturn:
        """Raise the held failure with the chaining it had when first reported.

        :raises BaseException:
            The held failure, always.
        """
        failure = self.failure
        failure.__traceback__ = self._traceback
        failure.__context__ = self._context
        failure.__suppress_context__ = self._suppress_context
        raise failure
