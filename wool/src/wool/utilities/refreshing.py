from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from collections.abc import Generator
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from typing import Generic
from typing import TypeVar

T = TypeVar("T")


@dataclass(slots=True)
class _Entry(Generic[T]):
    """One derived value and the monotonic time it was derived."""

    value: T
    refreshed_at: float


# public
class Refreshing(Generic[T]):
    """A value re-derived by a synchronous factory, at most once per interval.

    The refresh-ahead (stale-while-revalidate) pattern as a standalone
    resource: reads are cheap and frequent, the factory is slow and called
    rarely, concurrent readers never stampede it, and a reader is never
    handed nothing while a usable value exists.

    Age is measured from the last successful refresh, and puts the resource
    in one of four states:

    - **Empty** — nothing derived yet. A reader must wait for the factory.
    - **Fresh** — younger than ``fresh_for``. Readers get the value.
    - **Stale** — older than ``fresh_for``. Readers get the value and one
      of them drives a refresh.
    - **Expired** — older than ``fresh_for`` plus ``stale_for``. The value
      is no longer servable, so a reader must wait for the factory, as in
      Empty. Unreachable when ``stale_for`` is ``None``, which lets a
      value serve indefinitely while refreshes keep failing.

    Because ``factory`` is synchronous, who pays for a Stale refresh
    depends on how the resource is read, and both callers are served:

    - `get` is for a caller with no event loop, which has nothing to hand
      the work to, so it runs the refresh itself and receives the new
      value. Other threads reading concurrently get the previous value.
    - `__await__` is for a caller on an event loop, which offloads the
      refresh to the loop's executor and returns the previous value
      without suspending. No await point is delayed by a slow factory.

    In the Empty and Expired states there is nothing to serve, so both
    forms wait — sharing one factory invocation rather than each running
    their own, and sharing its exception if it raises.

    A refresh that fails while a servable value exists leaves that value in
    place and does not restart the interval, so the next read retries
    immediately. Pass ``on_error`` to observe those failures; the exception
    reaches a reader only when there was nothing to serve instead. A refresh
    that exceeds ``timeout`` is treated the same way: the reader stops
    waiting, the flight is abandoned so it no longer gates the next read,
    and a servable value is served in its place.

    ``factory`` MUST be safe to call from any thread. It is never called
    with the resource's lock held, so it may take as long as it needs
    without blocking readers of a servable value. It MUST NOT read this
    resource, which would join the invocation it is running inside and
    deadlock.

    State is process-local: pickling yields an Empty copy, since the
    monotonic timestamps a refresh interval is measured against are
    meaningless in another process.

    :param factory:
        A zero-argument callable returning the current value.
    :param fresh_for:
        How long a derived value is served without triggering a refresh.
        Zero means every read past the first triggers one; ``None`` means it
        is served forever, so ``factory`` runs exactly once and the resource
        is a memoized value rather than a refreshing one.
    :param stale_for:
        How much longer past ``fresh_for`` a value stays servable while
        refreshes are attempted. ``None`` (default) means indefinitely.
    :param timeout:
        How long a reader waits on an in-flight refresh **it did not start**
        before abandoning it, serving whatever is still servable and raising
        `TimeoutError` when nothing is. ``None`` (default) waits
        indefinitely. Abandoning frees the flight so the next read starts a
        fresh attempt; it does not interrupt ``factory``, which has no
        cancellation point, so an abandoned invocation may still commit and
        simply arrive late.

        This bounds joining, not running: the reader that *drives* a refresh
        calls ``factory`` directly and blocks for its full duration whatever
        ``timeout`` says, because a synchronous call cannot be abandoned
        from outside. On the `__await__` path that reader is the loop's
        executor and no await point is delayed by it; on the `get` path it
        is the calling thread, so a ``factory`` that can hang indefinitely
        needs its own internal deadline as well as this one.
    :param on_refresh:
        Called when a refresh commits, with the new value and the one it
        replaced — ``None`` for the first. Runs on whichever thread drove
        the refresh, outside the lock. Like ``factory``, it MUST NOT raise.
    :param on_error:
        Called when a refresh raises, with the exception and whether a
        previous value was served in its place. Runs on whichever thread
        drove the refresh, outside the lock.

    .. rubric:: Implementation notes

    ``on_refresh`` pairs each value with its predecessor under the lock, at
    the commit itself, which is what makes it sound for a caller comparing
    successive values. Doing that comparison in ``factory`` instead would be
    wrong twice over: ``factory`` runs off the lock, so two invocations can
    overlap once a timeout abandons one and its replacement starts; and it
    runs *before* the commit, so it cannot know what the value it produced
    will actually displace, or whether it will commit at all. Reporting
    happens after the value is published to waiters, so no reader waits on
    the callback — and consequently an exception from it reaches whichever
    caller drove the refresh only after that caller has been handed the
    committed value, which is why it must not raise.
    """

    def __init__(
        self,
        factory: Callable[[], T],
        *,
        fresh_for: timedelta | None,
        stale_for: timedelta | None = None,
        timeout: timedelta | None = None,
        on_refresh: Callable[[T, T | None], None] | None = None,
        on_error: Callable[[BaseException, bool], None] | None = None,
    ) -> None:
        if fresh_for is not None and fresh_for < timedelta():
            raise ValueError("fresh_for must be non-negative")
        if stale_for is not None and stale_for < timedelta():
            raise ValueError("stale_for must be non-negative")
        if timeout is not None and timeout <= timedelta():
            raise ValueError("timeout must be positive")
        self._factory = factory
        self._fresh_for = None if fresh_for is None else fresh_for.total_seconds()
        self._stale_for = None if stale_for is None else stale_for.total_seconds()
        self._timeout = None if timeout is None else timeout.total_seconds()
        self._on_refresh = on_refresh
        self._on_error = on_error
        # The lock only ever protects field reads and writes — never a
        # factory call — so no reader blocks on it for factory duration.
        self._lock = threading.Lock()
        self._entry: _Entry[T] | None = None
        self._flight: Future[T] | None = None

    def __getstate__(self) -> dict:
        """Return the picklable state, dropping process-local fields."""
        state = self.__dict__.copy()
        state.pop("_lock")
        state["_entry"] = None
        state["_flight"] = None
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore pickled state and recreate the process-local lock."""
        self.__dict__.update(state)
        self._lock = threading.Lock()

    def __await__(self) -> Generator[Any, None, T]:
        """Return the current value, offloading any refresh to the executor.

        The event-loop counterpart to `get`. Yields nothing when a servable
        value exists, so awaiting a Fresh resource does not suspend.
        """
        servable, flight, mine = self._claim()
        if flight is not None and mine:
            # Hand the factory to the loop's executor rather than running it
            # here. The flight slot holds the reference, so the work is not
            # collected mid-flight and its outcome is never dropped.
            asyncio.get_running_loop().run_in_executor(None, self._run, flight)
        if servable is not None:
            return servable
        assert flight is not None
        return (yield from self._settled(flight).__await__())

    def peek(self) -> T | None:
        """Return the current value if one is servable, else ``None``.

        Never calls ``factory`` and never triggers a refresh, so it is safe
        from any thread and from a running event loop. ``None`` means a
        factory call is owed: the resource is Empty or Expired, or Stale
        with no refresh yet running.
        """
        with self._lock:
            entry, fresh, expired = self._classify()
            if entry is None or expired:
                return None
            # Stale material is servable as-is only once a refresh is
            # actually running; otherwise one is owed and this reports it.
            return entry.value if fresh or self._flight is not None else None

    def get(self) -> T:
        """Return the current value, running any refresh on this thread.

        The synchronous counterpart to `__await__`, for a caller with no
        event loop to offload to. Blocks for the factory's duration only
        when this caller is the one that owes a refresh.
        """
        servable, flight, mine = self._claim()
        if mine:
            assert flight is not None
            self._run(flight)
            return self._settle(flight)
        if servable is not None:
            return servable
        assert flight is not None
        return self._settle(flight)

    def refresh(self) -> T:
        """Re-derive the value now, regardless of age, and return it.

        Joins a refresh already in flight rather than starting a second.
        """
        with self._lock:
            if (flight := self._flight) is not None:
                mine = False
            else:
                flight = self._flight = Future()
                mine = True
        if mine:
            self._run(flight)
        return self._settle(flight)

    def invalidate(self) -> None:
        """Discard the current value, returning the resource to Empty.

        A refresh already in flight still commits its result, so the next
        read may be served by material derived before this call.
        """
        with self._lock:
            self._entry = None

    def _classify(self) -> tuple[_Entry[T] | None, bool, bool]:
        """Return ``(entry, fresh, expired)``. Caller MUST hold the lock.

        The single home for the age comparison, so every entry point agrees
        on what Fresh, Stale, and Expired mean.
        """
        entry = self._entry
        if entry is None:
            return None, False, True
        if self._fresh_for is None:
            # Unbounded freshness: the value never ages out of Fresh, so no
            # refresh is ever owed and Stale and Expired are unreachable.
            return entry, True, False
        age = time.monotonic() - entry.refreshed_at
        fresh = age < self._fresh_for
        expired = (
            self._stale_for is not None and age >= self._fresh_for + self._stale_for
        )
        return entry, fresh, expired

    def _claim(self) -> tuple[T | None, Future[T] | None, bool]:
        """Decide what this caller is served and what it owes.

        Returns ``(servable, flight, mine)``. A non-``None`` ``servable`` is
        this caller's value. A non-``None`` ``flight`` is a refresh that
        needs driving — by this caller when ``mine``, otherwise by whoever
        claimed it — and is what to wait on when ``servable`` is ``None``.
        """
        with self._lock:
            entry, fresh, expired = self._classify()
            if fresh:
                assert entry is not None
                return entry.value, None, False
            servable = None if (entry is None or expired) else entry.value
            if (flight := self._flight) is not None:
                # A refresh is already running: drop this caller's rather
                # than pile on, and serve the previous value if it is still
                # servable.
                return servable, flight, False
            flight = self._flight = Future()
            return servable, flight, True

    def _run(self, flight: Future[T]) -> None:
        """Invoke ``factory`` once and publish its outcome to *flight*.

        Never raises: a failure is published to *flight* and reported
        through ``on_error``, so a caller that only triggered this refresh
        is unaffected by it. The outcome is published to *flight* rather
        than read back off the resource, so a reader woken after a later
        refresh has committed still sees the outcome it waited for.
        """
        try:
            value = self._factory()
        except BaseException as error:  # noqa: BLE001 — factory is caller code
            with self._lock:
                # A failed refresh neither commits a value nor restarts the
                # interval, so the next read retries immediately. Only clear
                # the slot if it is still ours: an abandoned flight that
                # finishes late must not evict the flight that replaced it.
                if self._flight is flight:
                    self._flight = None
                served_stale = self._entry is not None
            flight.set_exception(error)
            if self._on_error is not None:
                self._on_error(error, served_stale)
            return
        with self._lock:
            # Captured here so the value each refresh replaced is decided by
            # commit order, even when a late abandoned invocation commits
            # alongside the flight that replaced it.
            previous = self._entry
            self._entry = _Entry(value=value, refreshed_at=time.monotonic())
            if self._flight is flight:
                self._flight = None
            # Published inside the lock, before anything that could raise,
            # so a waiter never depends on code running after its commit.
            flight.set_result(value)
        if self._on_refresh is not None:
            self._on_refresh(value, previous.value if previous is not None else None)

    def _fallback(self) -> T | None:
        """Return the previous value if a failed refresh may serve it instead.

        Respects ``stale_for``: material past its staleness bound is not
        servable, so a failure with nothing else to offer propagates rather
        than presenting a value the caller has declared too old. Takes the
        lock itself — no caller needs it held across anything else.
        """
        with self._lock:
            entry, _, expired = self._classify()
            return None if entry is None or expired else entry.value

    def _abandon(self, flight: Future[T]) -> None:
        """Release *flight*'s slot so a lost refresh cannot gate the next read.

        Called when a reader's wait times out. ``factory`` has no
        cancellation point, so the invocation may still be running; this
        only stops it being joined. Should it commit later it publishes as
        normal and simply arrives late, and `_run` will not evict whatever
        flight replaced it.
        """
        with self._lock:
            if self._flight is flight:
                self._flight = None

    def _settle(self, flight: Future[T]) -> T:
        """Return *flight*'s value, or a servable one if it fails or times out."""
        try:
            return flight.result(self._timeout)
        except TimeoutError as error:
            self._abandon(flight)
            failure: BaseException = error
        except BaseException as error:
            failure = error
        if (value := self._fallback()) is not None:
            return value
        raise failure

    async def _settled(self, flight: Future[T]) -> T:
        """Await *flight* without holding a thread, then settle it."""
        try:
            return await asyncio.wait_for(asyncio.wrap_future(flight), self._timeout)
        except TimeoutError as error:
            self._abandon(flight)
            failure: BaseException = error
        except BaseException as error:
            failure = error
        if (value := self._fallback()) is not None:
            return value
        raise failure
