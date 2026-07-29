from __future__ import annotations

import asyncio.coroutines
import functools
import inspect
import sys
import weakref
from typing import Callable
from typing import ParamSpec
from typing import TypeVar

P = ParamSpec("P")
R = TypeVar("R")


class _Token:
    """Hashable, weakly-referenceable token for instance tracking."""

    pass


class NoReentryDescriptor:
    """Guard a method against a second invocation on the same instance.

    A descriptor that wraps one method of one class. It exists for the
    once-per-instance operations — teardown, shutdown, close — whose second
    execution is a latent bug rather than a no-op, and turns that bug into an
    immediate, attributable failure.

    The guard is per instance, not per class: each instance of the owning class
    may invoke the method once. The first call executes the method and returns
    its result; every later call on the same instance raises `RuntimeError`.
    Guard state lives on the instance under ``__noreentry_token__`` and is
    discarded with it, so a fresh instance is unguarded and an instance that is
    garbage collected leaves nothing behind.

    Because the guard keys on the receiver, the method must be reached with one:
    it may be called on an instance, or unbound off the owning class with the
    instance as the first argument, i.e., the form `contextlib` uses to enter a
    context manager (``type(cm).__enter__(cm)``). The latter reaches the
    descriptor rather than a bound wrapper, so a guarded ``__enter__`` or
    ``__aenter__`` composes with `contextlib.ExitStack` and
    `contextlib.AsyncExitStack`. Synchronous and asynchronous methods are both
    supported. A receiver of any other type, or none at all, raises `TypeError`,
    as does decorating a bare function, which has no receiver to key on.

    .. rubric:: Implementation notes

    Owner tracking is the reason this is a descriptor rather than a plain
    function decorator. A decorator that leans on Python's descriptor protocol
    to do the binding never sees the unbound call form at all — the arguments
    arrive as an ordinary positional tuple — so it cannot tell an unbound call
    on the owning instance from a call whose first argument merely happens to be
    an object, and would silently run the method against a foreign receiver.
    Holding the owner lets that case be rejected.

    ``__set_name__`` supplies the discriminator: the interpreter invokes it only
    for a descriptor assigned in a class body, so ``_owner`` stays ``None`` for a
    bare function. A ``None`` owner therefore means "no receiver exists" and a
    set owner means "a receiver is required", which is the distinction ``__call__``
    dispatches on.

    An owned descriptor called with its own instance first is a bound call in
    disguise: rebinding through ``__get__`` and re-dispatching routes it through
    the same wrapper and the same guard state as an instance call, so the two
    call forms cannot diverge.
    """

    def __init__(self, fn, /):
        functools.update_wrapper(self, fn)
        if inspect.iscoroutinefunction(fn):
            if sys.version_info >= (3, 12):
                inspect.markcoroutinefunction(self)
            else:
                self._is_coroutine = asyncio.coroutines._is_coroutine  # type: ignore[attr-defined]
        self._fn = fn
        self._owner = None
        self._invocations = weakref.WeakSet()

    def __set_name__(self, owner, name):
        """Record the class the descriptor is defined on."""
        self._owner = owner

    def __call__(self, *args, **kwargs):
        """Dispatch an unbound call to the receiver's bound wrapper.

        This is the path `contextlib` takes when it enters a context manager by
        invoking the special method off the type (``type(cm).__enter__(cm)``).
        See the class docstring for the guard semantics and the supported call
        forms.

        :raises TypeError:
            If the decorated object is a bare function, or if the first
            positional argument is not an instance of the owning class.
        """
        if self._owner is None:
            raise TypeError("@noreentry only decorates methods, not bare functions")
        if not args or not isinstance(args[0], self._owner):
            raise TypeError(
                f"'{self._fn.__qualname__}' must be called with an instance of "
                f"'{self._owner.__name__}' as the first argument"
            )
        obj, *rest = args
        return self.__get__(obj, self._owner)(*rest, **kwargs)

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self

        # Cache the wrapper on the instance to avoid recreating it on every access.
        cache_key = f"__noreentry_wrapper_{id(self)}__"
        return obj.__dict__.setdefault(cache_key, self._make_wrapper(obj))

    def _make_wrapper(self, obj):
        """Create the bound wrapper for an instance."""
        guard = self._guard
        fn = self._fn
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args, **kwargs):
                guard(obj)
                return await fn(obj, *args, **kwargs)

            return async_wrapper
        else:

            @functools.wraps(fn)
            def sync_wrapper(*args, **kwargs):
                guard(obj)
                return fn(obj, *args, **kwargs)

            return sync_wrapper

    def _guard(self, obj):
        """Check and record invocation on the specified object."""
        # Get or create a unique token for this object.
        token = obj.__dict__.setdefault("__noreentry_token__", _Token())

        # Check if this descriptor was invoked on this object already.
        if token in self._invocations:
            raise RuntimeError(
                f"'{self._fn.__qualname__}' cannot be invoked more than once"
            )

        # Track invocation.
        self._invocations.add(token)


def noreentry(fn: Callable[P, R]) -> Callable[P, R]:
    """Mark a method as single-use.

    Returns a `NoReentryDescriptor`; see that class for the guard semantics and
    the supported call forms.

    :param fn:
        The instance method to decorate.
    """
    return NoReentryDescriptor(fn)
