from __future__ import annotations

import asyncio
import inspect
import weakref
from typing import AsyncGenerator
from typing import AsyncIterable
from typing import AsyncIterator
from typing import Final
from typing import Generic
from typing import Iterable
from typing import NoReturn
from typing import TypeVar

from wool.utilities.failure import FailureReport

T = TypeVar("T")

_SENTINEL: Final = object()


class Fanout(Generic[T]):
    """Demand-driven async multicast container.

    Wraps a single async iterable source and fans out items to multiple
    independent consumers on demand. No pump runs in the background: a
    consumer whose queue is empty starts one pull, and that pull ends as
    soon as it has an item to distribute.

    The pull belongs to the container rather than to the consumer that
    started it. A consumer waiting on it can be cancelled, and the
    cancellation reaches only that consumer: the pull runs on, the item
    still reaches every queue, and the shared source is never thrown
    into. This is what makes one consumer's teardown its own business.
    Cancelling the pull itself is `cleanup`'s alone.

    A source that fails does so for every consumer. The failure is
    recorded here and the sentinel wakes the consumers that were not
    waiting, which then raise it in place of `StopAsyncIteration`; see
    `FanoutConsumer.__anext__`. Without that the failure would reach only
    whichever consumer happened to be pulling, and the rest would end as
    though the source had simply run out.

    :param source:
        The async iterable to multicast. It is iterated once, however
        many consumers there are.
    """

    def __init__(self, source: AsyncIterable[T]) -> None:
        # Checked here so a source that cannot be iterated fails at the
        # construction that supplied it. `aiter` is not called until the
        # first pull, and that pull happens inside a consumer's
        # `__anext__` under this container's lock — so the mistake would
        # otherwise surface from an iteration several frames away, be
        # reported to whichever consumer pulled first rather than to all
        # of them, and leave `_closed` unset so the next consumer repeats
        # it.
        if not isinstance(source, AsyncIterable):
            raise TypeError(
                f"Expected an async iterable to multicast, got: {type(source)}"
            )
        self._source = source
        self._lock = asyncio.Lock()
        self._iterator: AsyncIterator[T] | None = None
        self._pull: asyncio.Task[None] | None = None
        self._consumers: weakref.WeakSet[FanoutConsumer[T]] = weakref.WeakSet()
        #: The source's failure, once it has one. Held here rather than
        #: queued, so a consumer that joins after the failure raises it
        #: too, and set once — the first failure is the cause.
        self._failure: FailureReport | None = None
        #: Whether the source is done, however it ended. Read by a
        #: consumer that has just woken, so what it does next never
        #: depends on the order two wake-ups happened to run in.
        self._closed = False

    def consumer(self, prime: Iterable[T] = ()) -> FanoutConsumer[T]:
        """Create a new independent consumer, optionally primed.

        A consumer created after the source ended is not an error: its
        first pull after any primed items reports however the source
        ended, like any other.

        :param prime:
            Items this consumer yields before anything the shared source
            broadcasts. Delivered in order, ahead of every source item
            and ahead of however the source ends.
        :returns:
            A :class:`FanoutConsumer` backed by this container's shared
            source iterator.
        """
        c: FanoutConsumer[T] = FanoutConsumer(self)
        # Primed before joining, which is what makes the ordering
        # structural rather than a matter of the caller not suspending:
        # a broadcast reaches only consumers already in the set, so
        # until the line below this queue cannot have been written to by
        # anything else, and nothing the source sends — including how it
        # ends — can land ahead of these.
        for item in prime:
            c._queue.put_nowait(item)
        self._consumers.add(c)
        return c

    async def cleanup(self) -> None:
        """Retire the shared iterator and signal every consumer.

        After cleanup a consumer's next pull raises
        `StopAsyncIteration` — or the source's failure, if it had one. A
        subscription that ended in a failure owes its consumers that
        cause rather than a clean end, and cleaning up is not what made
        it end.

        The shared iterator is closed when it is an async generator or
        when it exposes ``aclose`` as a coroutine function. A source
        offering neither is left to its own collection, and a
        synchronous ``aclose`` is never called.

        A failure to close the iterator propagates. The caller is a
        resource pool's finalizer, which knows which pooled resource is
        being retired and logs it against that; this container knows
        only that a close failed. Consumers are signalled either way.

        :raises BaseException:
            Whatever closing the shared iterator raised.

        .. rubric:: Implementation notes

        The close takes two arms because neither predicate covers the
        other while both shapes are closeable. `isinstance` against
        `AsyncGenerator` is itself structural, over five names rather
        than one, so it admits a conforming duck and misses a
        class-based iterator that implements ``aclose`` and not
        ``asend``; a native generator's ``aclose`` is a builtin, which
        `inspect.iscoroutinefunction` does not report. What the pair
        excludes is the case worth excluding: a synchronous ``aclose``
        a third party happened to name that way, which `AsyncIterable`'s
        guarantee of ``__aiter__`` and nothing else leaves possible and
        which awaiting would raise on.
        """
        # Set before anything can suspend, so a consumer cannot start a
        # pull against an iterator this is about to close.
        self._closed = True
        pull, self._pull = self._pull, None
        try:
            if pull is not None and not pull.done():
                pull.cancel()
                # Awaited to settle, not for its outcome: a generator
                # suspended inside an active pull refuses to close.
                await asyncio.wait({pull})
            iterator, self._iterator = self._iterator, None
            # Two arms, for the reason in the implementation notes.
            close = getattr(iterator, "aclose", None)
            if isinstance(iterator, AsyncGenerator):
                await iterator.aclose()
            elif inspect.iscoroutinefunction(close):
                await close()
        finally:
            self._wake()

    def _broadcast(self, item: T | object) -> None:
        """Put an item into every consumer's queue.

        :param item:
            The item to distribute, or `_SENTINEL` to wake.
        """
        for c in list(self._consumers):
            c._queue.put_nowait(item)

    def _wake(self) -> None:
        """Wake every consumer so it observes how the source ended.

        The sentinel is only the wake-up. What it means is whatever
        `_closed` and `_failure` say by the time the woken consumer
        looks, which is why neither is written after it.
        """
        self._broadcast(_SENTINEL)

    def _end(self) -> NoReturn:
        """Raise whatever ended this subscription.

        :raises BaseException:
            The source's failure, if it had one.
        :raises StopAsyncIteration:
            If the source ended without failing.
        """
        if self._failure is not None:
            self._failure.raise_failure()
        raise StopAsyncIteration

    def _fail(self, error: BaseException) -> None:
        """Record the source's failure and wake every consumer.

        :param error:
            What the source raised. The first one is the cause; a later
            one cannot displace it.
        """
        if self._failure is None:
            self._failure = FailureReport(error)
        self._closed = True
        self._wake()

    async def _pull_once(self) -> None:
        """Start or join the one in-flight pull, and wait for it to settle.

        Detachable: a caller cancelled while waiting leaves the pull
        running, so the item it was fetching still reaches every queue.
        Callers hold `_lock`, so at most one pull is ever outstanding.
        """
        if self._iterator is None:
            self._iterator = aiter(self._source)
        pull = self._pull
        if pull is None or pull.done():
            pull = self._pull = asyncio.ensure_future(self._pull_one())
        try:
            # `wait` rather than awaiting the task: it never cancels what
            # it waits on and never raises that task's outcome, so a
            # cancellation here is unambiguously this caller's own.
            await asyncio.wait({pull})
        finally:
            if self._pull is pull and pull.done():
                self._pull = None

    async def _pull_one(self) -> None:
        """Pull one item from the shared source and fan it out.

        Runs as the container's own task, so the only cancellation it
        can see is `cleanup`'s. Never raises the source's failure —
        `_failure` and `_closed` carry it to every consumer instead.
        """
        assert self._iterator is not None
        try:
            item = await anext(self._iterator)
        except StopAsyncIteration:
            self._closed = True
            self._wake()
        except asyncio.CancelledError:
            task = asyncio.current_task()
            if self._closed or (task is not None and task.cancelling() > 0):
                # `cleanup` cancelled this pull. It owns the close and
                # the wake; recording a failure would hand every
                # consumer a cause for a teardown that is not one.
                raise
            # Nobody cancelled this task, so the source raised
            # `CancelledError` itself. The subscription is over for
            # everyone, and a silent end is what `_failure` exists to
            # prevent.
            self._fail(asyncio.CancelledError("Fanout source was cancelled"))
        except BaseException as error:
            self._fail(error)
        else:
            self._broadcast(item)


class FanoutConsumer(Generic[T]):
    """Independent consumer backed by a shared :class:`Fanout` source.

    Instances are created via :meth:`Fanout.consumer` and implement the
    async iterator protocol. Each consumer maintains its own queue so
    that items are delivered independently.

    That queue is unbounded, and distribution does not wait for demand:
    a pull is started by whichever consumer wants an item, but the item
    it produces is put into *every* consumer's queue. A consumer that
    stops being pulled therefore accumulates at the rate its peers pull,
    and leaves the container only when it is garbage-collected out of the
    weak set holding it — which an iteration still referencing it
    prevents. Bounding this is the caller's to decide, because what may
    be discarded depends on what the items mean: see
    `~wool.runtime.discovery.pool._SharedSubscription` for a case where
    one kind of item cannot be dropped at all.

    :param fanout:
        The parent :class:`Fanout` container.
    """

    def __init__(self, fanout: Fanout[T]) -> None:
        self._fanout = fanout
        self._queue: asyncio.Queue[T | object] = asyncio.Queue()

    def __aiter__(self) -> FanoutConsumer[T]:
        return self

    async def __anext__(self) -> T:
        """Return this consumer's next item from the shared source.

        :returns:
            The next item.
        :raises StopAsyncIteration:
            When the source is exhausted, or the fanout was cleaned up.
        :raises BaseException:
            The source's own failure, re-raised in every consumer rather
            than only in whichever one was pulling when it happened; see
            `Fanout`.
        """
        fanout = self._fanout
        # Fast path — dequeue if available.
        if not self._queue.empty():
            return self._take()

        async with fanout._lock:
            while True:
                # Another consumer's pull may have filled this queue
                # while this one waited for the lock.
                if not self._queue.empty():
                    return self._take()
                # A settled source stays settled — checked before the
                # pull, because an iterator closed by its own exception
                # reports ordinary exhaustion.
                if fanout._closed:
                    fanout._end()
                await fanout._pull_once()

    def _take(self) -> T:
        """Dequeue one value, resolving a sentinel to how the source ended.

        :returns:
            The dequeued item.
        :raises BaseException:
            Whatever ended the subscription; see `Fanout._end`.
        """
        value = self._queue.get_nowait()
        if value is _SENTINEL:
            self._fanout._end()
        return value  # type: ignore[return-value]
