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

    :param source:
        The async generator to multicast.
    """

    def __init__(self, source: AsyncGenerator[T]) -> None:
        self._source = source
        self._lock = asyncio.Lock()
        self._iterator: AsyncIterator[T] | None = None
        self._consumers: weakref.WeakSet[FanoutConsumer[T]] = weakref.WeakSet()

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
        :exc:`StopAsyncIteration` on its next pull.
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
        # Fast path — dequeue if available.
        if not self._queue.empty():
            value = self._queue.get_nowait()
            if value is _SENTINEL:
                raise StopAsyncIteration
            return value  # type: ignore[return-value]

        fanout = self._fanout
        async with fanout._lock:
            # Double-check after acquiring lock — another consumer
            # may have filled our queue while we waited.
            if not self._queue.empty():
                value = self._queue.get_nowait()
                if value is _SENTINEL:
                    raise StopAsyncIteration
                return value  # type: ignore[return-value]

            # Lazily initialise the shared source iterator.
            if fanout._iterator is None:
                fanout._iterator = aiter(fanout._source)

            try:
                item = await anext(fanout._iterator)
            except StopAsyncIteration:
                for c in list(fanout._consumers):
                    if c is not self:
                        c._queue.put_nowait(_SENTINEL)
                raise

            # Fan out to all other registered consumers.
            for c in list(fanout._consumers):
                if c is not self:
                    c._queue.put_nowait(item)
            return item
