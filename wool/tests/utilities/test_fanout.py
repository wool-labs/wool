from __future__ import annotations

import asyncio

import pytest

from wool.utilities.fanout import Fanout
from wool.utilities.fanout import FanoutConsumer


class _Source:
    """Async-iterable source that yields given items on each
    ``__aiter__`` call."""

    def __init__(self, *items):
        self._items = items

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for item in self._items:
            yield item


class TestFanout:
    """Tests for the Fanout class.

    Fully qualified name: wool.utilities.fanout.Fanout
    """

    def test_consumer_with_new_instance(self):
        """Test consumer creates a FanoutConsumer.

        Given:
            A Fanout wrapping a source.
        When:
            consumer is called.
        Then:
            It should return a FanoutConsumer instance.
        """
        # Arrange
        fanout = Fanout(_Source("a"))

        # Act
        c = fanout.consumer()

        # Assert
        assert isinstance(c, FanoutConsumer)

    def test_consumer_with_independent_instances(self):
        """Test consumer returns distinct instances per call.

        Given:
            A Fanout wrapping a source.
        When:
            consumer is called twice.
        Then:
            It should return two distinct FanoutConsumer instances.
        """
        # Arrange
        fanout = Fanout(_Source("a"))

        # Act
        a = fanout.consumer()
        b = fanout.consumer()

        # Assert
        assert a is not b

    @pytest.mark.asyncio
    async def test_cleanup_with_active_consumer(self):
        """Test cleanup terminates active consumers.

        Given:
            A Fanout with one registered consumer.
        When:
            cleanup is called.
        Then:
            It should cause the consumer to raise
            StopAsyncIteration on next pull.
        """
        # Arrange
        fanout = Fanout(_Source("a", "b"))
        consumer = fanout.consumer()

        # Act
        await fanout.cleanup()

        # Assert
        with pytest.raises(StopAsyncIteration):
            await anext(consumer)

    @pytest.mark.asyncio
    async def test_cleanup_with_open_iterator(self):
        """Test cleanup closes the shared source iterator.

        Given:
            A Fanout whose shared iterator has been initialised by
            a pull.
        When:
            cleanup is called.
        Then:
            It should close the shared iterator.
        """
        # Arrange
        closed = False

        async def tracked_source():
            nonlocal closed
            try:
                while True:
                    yield "item"
            finally:
                closed = True

        fanout = Fanout(tracked_source())
        consumer = fanout.consumer()
        await anext(consumer)  # initialise the iterator

        # Act
        await fanout.cleanup()

        # Assert
        assert closed

    @pytest.mark.asyncio
    async def test_cleanup_with_failing_aclose(self):
        """Test cleanup swallows exceptions from the source iterator.

        Given:
            A Fanout whose source raises during aclose.
        When:
            cleanup is called.
        Then:
            It should swallow the exception and still signal
            consumers with a sentinel.
        """

        # Arrange
        class _FailingSource:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                try:
                    while True:
                        yield "item"
                finally:
                    raise RuntimeError("aclose failed")

        fanout = Fanout(_FailingSource())
        consumer = fanout.consumer()
        await anext(consumer)  # initialise the iterator

        # Act
        await fanout.cleanup()

        # Assert — consumer is signalled despite the exception
        with pytest.raises(StopAsyncIteration):
            await anext(consumer)


class TestFanoutConsumer:
    """Tests for the FanoutConsumer class.

    Fully qualified name: wool.utilities.fanout.FanoutConsumer
    """

    @pytest.mark.asyncio
    async def test___aiter___with_self_return(self):
        """Test async iteration protocol returns self.

        Given:
            A FanoutConsumer instance.
        When:
            aiter is called on it.
        Then:
            It should return the same instance.
        """
        # Arrange
        consumer = Fanout(_Source("a")).consumer()

        # Act
        result = aiter(consumer)

        # Assert
        assert result is consumer

    @pytest.mark.asyncio
    async def test___anext___with_single_item(self):
        """Test pulling a single item from the source.

        Given:
            A Fanout wrapping a source that yields one item.
        When:
            anext is called on a consumer.
        Then:
            It should return the item from the source.
        """
        # Arrange
        fanout = Fanout(_Source("hello"))
        consumer = fanout.consumer()

        # Act
        result = await anext(consumer)

        # Assert
        assert result == "hello"

    @pytest.mark.asyncio
    async def test___anext___with_fan_out_to_multiple_consumers(self):
        """Test items are fanned out to all registered consumers.

        Given:
            Two consumers sharing the same Fanout source.
        When:
            One consumer triggers a pull via anext.
        Then:
            Both consumers should receive the item.
        """
        # Arrange
        fanout = Fanout(_Source("event-1", "event-2"))
        consumer_a = fanout.consumer()
        consumer_b = fanout.consumer()

        # Act — consumer_a pulls, fan-out to consumer_b
        result_a = await anext(consumer_a)
        result_b = await anext(consumer_b)

        # Assert
        assert result_a == "event-1"
        assert result_b == "event-1"

    @pytest.mark.asyncio
    async def test___anext___with_exhausted_source(self):
        """Test source exhaustion propagates to all consumers.

        Given:
            Two consumers sharing a source that yields two items.
        When:
            Both consumers iterate to exhaustion via async for.
        Then:
            Both consumers should receive both items and iteration
            should terminate.
        """
        # Arrange
        fanout = Fanout(_Source("a", "b"))
        consumer_a = fanout.consumer()
        consumer_b = fanout.consumer()

        # Act
        collected_a = [item async for item in consumer_a]
        collected_b = [item async for item in consumer_b]

        # Assert
        assert collected_a == ["a", "b"]
        assert collected_b == ["a", "b"]

    @pytest.mark.asyncio
    async def test___anext___after_cleanup(self):
        """Test anext raises StopAsyncIteration after cleanup.

        Given:
            A consumer registered against a Fanout that has been
            cleaned up.
        When:
            anext is called on the consumer.
        Then:
            It should raise StopAsyncIteration.
        """
        # Arrange
        fanout = Fanout(_Source("a"))
        consumer = fanout.consumer()
        await fanout.cleanup()

        # Act & assert
        with pytest.raises(StopAsyncIteration):
            await anext(consumer)

    def test_enqueue_with_direct_item(self):
        """Test enqueue pushes an item into the consumer's queue.

        Given:
            A FanoutConsumer instance.
        When:
            enqueue is called with an item.
        Then:
            It should be retrievable on the next pull.
        """
        # Arrange
        fanout = Fanout(_Source())
        consumer = fanout.consumer()

        # Act
        consumer.enqueue("injected")

        # Assert — item is in the queue (verified via queue size)
        assert not consumer._queue.empty()

    @pytest.mark.asyncio
    async def test_enqueue_with_pull_after_inject(self):
        """Test enqueued items are returned before source items.

        Given:
            A FanoutConsumer with an enqueued item and a source
            that also yields items.
        When:
            anext is called.
        Then:
            It should return the enqueued item first.
        """
        # Arrange
        fanout = Fanout(_Source("from-source"))
        consumer = fanout.consumer()
        consumer.enqueue("injected")

        # Act
        first = await anext(consumer)
        second = await anext(consumer)

        # Assert
        assert first == "injected"
        assert second == "from-source"

    @pytest.mark.asyncio
    async def test___anext___with_late_registered_consumer(self):
        """Test a late-registered consumer does not receive past items.

        Given:
            A Fanout where consumer_a has already pulled an item.
        When:
            A new consumer_b is created and pulls.
        Then:
            It should receive only subsequent items, not past ones.
        """
        # Arrange
        fanout = Fanout(_Source("first", "second"))
        consumer_a = fanout.consumer()
        await anext(consumer_a)  # pull "first"

        # Act
        consumer_b = fanout.consumer()
        result_b = await anext(consumer_b)

        # Assert — consumer_b missed "first", gets "second"
        # consumer_a also triggered fan-out of "second" during its pull
        # but consumer_b wasn't registered yet. consumer_b's pull
        # triggers the next source item.
        assert result_b == "second"

    @pytest.mark.asyncio
    async def test___anext___with_concurrent_queue_fill(self):
        """Test the lock double-check returns a queued item.

        Given:
            Two consumers sharing a Fanout whose source suspends
            during iteration.
        When:
            Both consumers pull concurrently via asyncio.gather.
        Then:
            The consumer that waited for the lock should receive
            the item from its queue via the double-check path.
        """

        # Arrange — source suspends so the event loop can
        # schedule both consumers before the first completes.
        class _SuspendingSource:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                await asyncio.sleep(0)
                yield "item"

        fanout = Fanout(_SuspendingSource())
        consumer_a = fanout.consumer()
        consumer_b = fanout.consumer()

        # Act — concurrent pulls force the double-check path
        results = await asyncio.gather(anext(consumer_a), anext(consumer_b))

        # Assert — both receive the same item
        assert results == ["item", "item"]

    @pytest.mark.asyncio
    async def test___anext___with_sentinel_in_double_check(self):
        """Test the double-check path handles SENTINEL on exhaustion.

        Given:
            Two consumers sharing a Fanout whose source suspends
            then exhausts without yielding any items.
        When:
            Both consumers pull concurrently via asyncio.gather.
        Then:
            Both should receive StopAsyncIteration — one directly
            from the source, the other via the SENTINEL placed in
            its queue during the lock double-check.
        """

        # Arrange — source suspends then exhausts, allowing the
        # event loop to interleave both consumers before either
        # completes.
        class _SuspendExhaustSource:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                await asyncio.sleep(0)
                return
                yield  # noqa: F841 — makes this an async gen

        fanout = Fanout(_SuspendExhaustSource())
        consumer_a = fanout.consumer()
        consumer_b = fanout.consumer()

        # Act — return_exceptions=True so gather doesn't cancel
        # the second task when the first raises.
        results = await asyncio.gather(
            anext(consumer_a),
            anext(consumer_b),
            return_exceptions=True,
        )

        # Assert — both received StopAsyncIteration
        assert all(isinstance(r, StopAsyncIteration) for r in results)

    @pytest.mark.asyncio
    async def test___anext___with_failing_source(self):
        """Test a source's failure reaches every consumer.

        Given:
            Two consumers sharing a source that raises part way
            through, with the first consumer having already pulled the
            item before it
        When:
            Each consumer pulls again
        Then:
            Both should raise that failure, rather than the consumer
            that was not pulling ending as though the source had simply
            run out.
        """

        # Arrange
        class _FailingSource:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield "a"
                raise RuntimeError("source failed")

        fanout = Fanout(_FailingSource())
        consumer_a = fanout.consumer()
        consumer_b = fanout.consumer()
        assert await anext(consumer_a) == "a"
        # Drain the item the pull above fanned out, so the next pull
        # below reaches the source rather than this consumer's queue.
        assert await anext(consumer_b) == "a"

        # Act
        with pytest.raises(RuntimeError, match="source failed") as first:
            await anext(consumer_a)

        # Assert — the consumer that never pulled the source gets the
        # same failure, not a clean end.
        with pytest.raises(RuntimeError, match="source failed") as second:
            await anext(consumer_b)
        assert second.value is first.value

    @pytest.mark.asyncio
    async def test___anext___with_consumer_joining_after_failure(self):
        """Test a consumer that joins a failed fanout raises its failure.

        Given:
            A Fanout whose source has already raised
        When:
            A consumer created after that failure pulls
        Then:
            It should raise the recorded failure, rather than binding a
            fresh iterator over a source that is already finished.
        """

        # Arrange
        class _FailingSource:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                raise RuntimeError("source failed")
                yield  # pragma: no cover — makes this an async generator

        fanout = Fanout(_FailingSource())
        with pytest.raises(RuntimeError, match="source failed"):
            await anext(fanout.consumer())

        # Act & assert
        with pytest.raises(RuntimeError, match="source failed"):
            await anext(fanout.consumer())

    @pytest.mark.asyncio
    async def test___anext___with_cancelled_consumer(self):
        """Test one consumer's cancellation does not end the others.

        Given:
            Two consumers sharing a source that suspends before its
            first item, and a pull by the first that is cancelled while
            it waits
        When:
            The second consumer pulls
        Then:
            It should end cleanly and the fanout should hold no
            failure — a cancellation belongs to the consumer that was
            cancelled and is not the source failing, so it must not be
            recorded as one and re-raised in every other consumer.

            That the shared source does not survive a cancelled pull is
            pre-existing and pinned here rather than changed: cancelling
            mid-pull closes the generator, so the survivor sees the end
            of the source rather than the item it was waiting for.
        """

        # Arrange
        release = asyncio.Event()

        class _SuspendingSource:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                await release.wait()
                yield "a"

        fanout = Fanout(_SuspendingSource())
        consumer_a = fanout.consumer()
        consumer_b = fanout.consumer()
        pulling = asyncio.ensure_future(anext(consumer_a))
        await asyncio.sleep(0)

        # Act
        pulling.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pulling
        release.set()

        # Assert — no failure was recorded, so the survivor ends
        # cleanly rather than re-raising another consumer's
        # cancellation.
        assert fanout._failure is None
        with pytest.raises(StopAsyncIteration):
            await anext(consumer_b)
