from __future__ import annotations

import asyncio.coroutines
import functools
import inspect
import sys
import weakref
from typing import Never


class _Token:
    """Hashable, weakly-referenceable token for instance tracking."""

    pass


class NoReentryBoundMethod:
    """Descriptor implementing single-use guard for bound methods.

    On the first call the decorated method executes normally. Any
    subsequent call raises :class:`RuntimeError`.

    Guard state uses a per-instance token stored on the instance under
    ``__noreentry_token__``. The token is unique, hashable, and tied to the
    instance's lifetime. The descriptor tracks tokens in a WeakSet to auto-clean
    when instances are garbage collected.

    Works with both synchronous and asynchronous methods. Only supports
    bound methods; using @noreentry on bare functions raises TypeError.
    """

    def __init__(self, fn, /):
        functools.update_wrapper(self, fn)
        if inspect.iscoroutinefunction(fn):
            if sys.version_info >= (3, 12):
                inspect.markcoroutinefunction(self)
            else:
                self._is_coroutine = asyncio.coroutines._is_coroutine  # type: ignore[attr-defined]
        self._fn = fn
        self._invocations = weakref.WeakSet()

    def __call__(self, *args, **kwargs) -> Never:
        raise TypeError("@noreentry only decorates methods, not bare functions")

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


def noreentry(fn):
    """Mark a method as single-use.

    On the first call the decorated method executes normally. Any
    subsequent call raises :class:`RuntimeError`.

    Guard state uses a per-instance token stored on the instance under
    ``__noreentry_token__``. The token is unique, hashable, and tied to the
    instance's lifetime. The descriptor tracks tokens in a :class:`WeakSet`
    to auto-clean when instances are garbage collected.

    Works with both synchronous and asynchronous methods. Only supports
    bound methods; using @noreentry on bare functions raises :class:`TypeError`
    on the first invocation.

    :param fn:
        The bound instance or class method to decorate.
    """
    return NoReentryBoundMethod(fn)
