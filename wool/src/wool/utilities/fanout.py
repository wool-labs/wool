from __future__ import annotations

import asyncio
import weakref
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Final
from typing import Generic
from typing import TypeVar

T = TypeVar("T")

_SENTINEL: Final = object()


class Fanout(Generic[T]):
    """Demand-driven async multicast container.

    Wraps a single async iterable source and fans out items to
    multiple independent consumers on demand.  No background task
    runs — the first consumer whose queue is empty acquires a lock,
    pulls one item from the shared source iterator, and distributes
    it to every other consumer's queue.

    A source that fails does so for every consumer. The failure is
    recorded here and the sentinel wakes the consumers that were not
    pulling, which then raise it in place of `StopAsyncIteration`; see
    `FanoutConsumer.__anext__`. Without that the failure would reach
    only whichever consumer happened to be pulling, and the rest would
    end as though the source had simply run out.

    :param source:
        The async generator to multicast.
    """

    def __init__(self, source: AsyncGenerator[T]) -> None:
        self._source = source
        self._lock = asyncio.Lock()
        self._iterator: AsyncIterator[T] | None = None
        self._consumers: weakref.WeakSet[FanoutConsumer[T]] = weakref.WeakSet()
        #: The source's failure, once it has one. Held here rather than
        #: queued, so a consumer that joins after the failure raises it
        #: too, and set once — the first failure is the cause.
        self._failure: BaseException | None = None

    def consumer(self) -> FanoutConsumer[T]:
        """Create a new independent consumer.

        :returns:
            A :class:`FanoutConsumer` backed by this container's
            shared source iterator.
        """
        c: FanoutConsumer[T] = FanoutConsumer(self)
        self._consumers.add(c)
        return c

    async def cleanup(self) -> None:
        """Close the shared iterator and signal remaining consumers.

        After cleanup, any active consumer will receive
        :exc:`StopAsyncIteration` on its next pull — or the source's
        failure, if it had one. A subscription that ended in a failure
        owes its consumers that cause rather than a clean end, and
        cleaning up is not what made it end.
        """
        self._iterator = None
        try:
            await self._source.aclose()
        except Exception:
            pass
        for c in list(self._consumers):
            c._queue.put_nowait(_SENTINEL)


class FanoutConsumer(Generic[T]):
    """Independent consumer backed by a shared :class:`Fanout` source.

    Instances are created via :meth:`Fanout.consumer` and implement
    the async iterator protocol.  Each consumer maintains its own
    queue so that items are delivered independently.

    :param fanout:
        The parent :class:`Fanout` container.
    """

    def __init__(self, fanout: Fanout[T]) -> None:
        self._fanout = fanout
        self._queue: asyncio.Queue[T | object] = asyncio.Queue()

    def enqueue(self, item: T) -> None:
        """Push an item directly into this consumer's queue.

        Useful for injecting replay or synthetic items that bypass
        the shared source iterator.

        :param item:
            The item to enqueue.
        """
        self._queue.put_nowait(item)

    def __aiter__(self) -> FanoutConsumer[T]:
        return self

    async def __anext__(self) -> T:
        """Return this consumer's next item from the shared source.

        :returns:
            The next item.
        :raises StopAsyncIteration:
            When the source is exhausted, or the fanout was cleaned up.
        :raises BaseException:
            The source's own failure, re-raised in every consumer
            rather than only in whichever one was pulling when it
            happened; see `Fanout`.
        """
        fanout = self._fanout
        # Fast path — dequeue if available.
        if not self._queue.empty():
            value = self._queue.get_nowait()
            if value is _SENTINEL:
                return self._end(fanout)
            return value  # type: ignore[return-value]

        async with fanout._lock:
            # Double-check after acquiring lock — another consumer
            # may have filled our queue while we waited.
            if not self._queue.empty():
                value = self._queue.get_nowait()
                if value is _SENTINEL:
                    return self._end(fanout)
                return value  # type: ignore[return-value]

            # A source that already failed stays failed. Checked before
            # the pull rather than left to the iterator, which would
            # report a generator closed by its own exception as an
            # ordinary exhaustion and end this consumer cleanly.
            if fanout._failure is not None:
                raise fanout._failure

            # Lazily initialise the shared source iterator.
            if fanout._iterator is None:
                fanout._iterator = aiter(fanout._source)

            try:
                item = await anext(fanout._iterator)
            except StopAsyncIteration:
                self._wake_others(fanout)
                raise
            except Exception as error:
                # Recorded before the wake, so a consumer that runs the
                # instant it is woken finds the cause already in place.
                # `Exception` and not `BaseException`: cancelling one
                # consumer's pull is that consumer's business, and must
                # not end the subscription for every other one.
                fanout._failure = error
                self._wake_others(fanout)
                raise

            # Fan out to all other registered consumers.
            for c in list(fanout._consumers):
                if c is not self:
                    c._queue.put_nowait(item)
            return item

    def _end(self, fanout: Fanout[T]) -> T:
        """Raise whatever ended this consumer, having dequeued a sentinel.

        :param fanout:
            The parent container, holding the source's failure if it
            had one.
        :raises StopAsyncIteration:
            If the source ended without failing.
        """
        if fanout._failure is not None:
            raise fanout._failure
        raise StopAsyncIteration

    def _wake_others(self, fanout: Fanout[T]) -> None:
        """Wake every other consumer so it observes how the source ended.

        The sentinel is only the wake-up; what it means is whether
        `Fanout._failure` is set by the time the woken consumer looks.

        :param fanout:
            The parent container whose consumers to wake.
        """
        for c in list(fanout._consumers):
            if c is not self:
                c._queue.put_nowait(_SENTINEL)
