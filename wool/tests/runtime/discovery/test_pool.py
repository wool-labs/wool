from __future__ import annotations

import uuid

import cloudpickle
import pytest

from wool.runtime.discovery import __subscriber_pool__
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.pool import SubscriberMeta
from wool.runtime.discovery.pool import _pool_factory
from wool.runtime.discovery.pool import _SharedSubscription
from wool.runtime.discovery.pool import _subscriber_factories
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.worker.metadata import WorkerMetadata


class _StubSubscriber(metaclass=SubscriberMeta):
    """Minimal subscriber stub for testing SubscriberMeta."""

    def __init__(self, key_value: str) -> None:
        self.key_value = key_value

    @classmethod
    def _cache_key(cls, key_value: str):
        return (cls, key_value)

    def __reduce__(self):
        return type(self), (self.key_value,)

    async def _shutdown(self) -> None:
        pass

    def __aiter__(self):
        return self._event_stream()

    async def _event_stream(self):
        yield  # pragma: no cover


def _make_event(event_type="worker-added", *, address="127.0.0.1:50051"):
    return DiscoveryEvent(
        event_type,
        metadata=WorkerMetadata(
            uid=uuid.uuid4(),
            address=address,
            pid=1,
            version="1.0.0",
        ),
    )


def _setup_pool():
    """Ensure the subscriber pool exists for direct-construction tests."""
    pool = __subscriber_pool__.get()
    if pool is None:
        pool = ResourcePool(factory=_pool_factory, ttl=0)
        __subscriber_pool__.set(pool)
    return pool


def _make_shared(source, key="test-key"):
    """Create a _SharedSubscription backed by a raw source via the pool."""
    _setup_pool()
    _subscriber_factories[key] = lambda _: source
    return _SharedSubscription(key=key, reduce_info=(type(source), (), {}))


class TestSubscriberMeta:
    """Tests for SubscriberMeta metaclass.

    Fully qualified name: wool.runtime.discovery.pool.SubscriberMeta
    """

    def test___new___with_shared_subscription_wrapping(self):
        """Test SubscriberMeta returns a _SharedSubscription.

        Given:
            A subscriber class using SubscriberMeta.
        When:
            The class is instantiated.
        Then:
            It should return a _SharedSubscription wrapping the
            cached raw subscriber.
        """
        # Act
        result = _StubSubscriber("wrap-key")

        # Assert
        assert isinstance(result, _SharedSubscription)

    @pytest.mark.asyncio
    async def test___new___with_shared_fan_out(self):
        """Test SubscriberMeta shares events across subscriptions.

        Given:
            A source that yields events and two subscriptions
            created with the same key.
        When:
            Both consumers are initialised and one pulls a new
            event.
        Then:
            The other should receive the same event via fan-out.
        """
        # Arrange
        events = [_make_event(address=f"127.0.0.1:{50051 + i}") for i in range(3)]

        class _EventStub(metaclass=SubscriberMeta):
            def __init__(self, tag):
                self.tag = tag

            @classmethod
            def _cache_key(cls, tag):
                return (cls, tag)

            async def _shutdown(self):
                pass

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                for e in events:
                    yield e

        sub_a = _EventStub("shared")
        sub_b = _EventStub("shared")
        it_a = aiter(sub_a)
        it_b = aiter(sub_b)

        # Initialise both consumers — a pulls events[0] (b not
        # registered yet), b gets a replay on its first pull.
        await anext(it_a)
        await anext(it_b)

        # Act — b pulls events[1] from source, fans out to a
        result_b = await anext(it_b)
        result_a = await anext(it_a)

        # Assert — same object via fan-out
        assert result_a is result_b
        assert result_a is events[1]

    @pytest.mark.asyncio
    async def test___new___with_different_keys(self):
        """Test SubscriberMeta isolates different keys.

        Given:
            Two subscriptions created with different keys, each
            backed by a distinct source.
        When:
            Both are iterated.
        Then:
            Events from one key should not appear in the other.
        """
        # Arrange
        event_a = _make_event(address="10.0.0.1:1")
        event_b = _make_event(address="10.0.0.2:2")

        class _IsoStub(metaclass=SubscriberMeta):
            def __init__(self, tag, event):
                self.tag = tag
                self._event = event

            @classmethod
            def _cache_key(cls, tag, event):
                return (cls, tag)

            async def _shutdown(self):
                pass

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield self._event

        sub_a = _IsoStub("key-a", event_a)
        sub_b = _IsoStub("key-b", event_b)

        # Act
        result_a = await anext(aiter(sub_a))
        result_b = await anext(aiter(sub_b))

        # Assert
        assert result_a is event_a
        assert result_b is event_b

    def test___new___with_lazy_pool_init(self):
        """Test lazy pool initialization when ContextVar is None.

        Given:
            The __subscriber_pool__ ContextVar is explicitly set to
            None.
        When:
            A subscriber is constructed via SubscriberMeta.
        Then:
            The ContextVar should be set to a ResourcePool.
        """
        # Arrange
        __subscriber_pool__.set(None)

        # Act
        _StubSubscriber("lazy-init")

        # Assert
        assert __subscriber_pool__.get() is not None


class TestSharedSubscription:
    """Tests for _SharedSubscription class.

    Fully qualified name: wool.runtime.discovery.pool._SharedSubscription
    """

    @pytest.mark.asyncio
    async def test___aiter___with_demand_driven_pull(self):
        """Test iteration pulls from the source.

        Given:
            A _SharedSubscription backed by a source that yields
            one event.
        When:
            The subscription is iterated.
        Then:
            It should return the event from the source.
        """
        # Arrange
        event = _make_event()

        class _Source:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield event

        shared = _make_shared(_Source())

        # Act
        result = await anext(aiter(shared))

        # Assert
        assert result is event

    @pytest.mark.asyncio
    async def test___aiter___with_fan_out_to_multiple_consumers(self):
        """Test events are fanned out to all iterators.

        Given:
            Two iterators from subscriptions sharing the same key,
            both initialised.
        When:
            One iterator pulls a new event.
        Then:
            The other should receive the same event via fan-out.
        """
        # Arrange
        events = [_make_event(address=f"127.0.0.1:{50051 + i}") for i in range(3)]

        class _Source:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                for e in events:
                    yield e

        _setup_pool()
        key = "fan-out-key"
        source = _Source()
        _subscriber_factories[key] = lambda _: source

        sub_a = _SharedSubscription(key=key, reduce_info=(type(source), (), {}))
        sub_b = _SharedSubscription(key=key, reduce_info=(type(source), (), {}))
        it_a = aiter(sub_a)
        it_b = aiter(sub_b)

        # Initialise both consumers
        await anext(it_a)
        await anext(it_b)

        # Act — b pulls events[1] from source, fans out to a
        result_b = await anext(it_b)
        result_a = await anext(it_a)

        # Assert — same object via fan-out
        assert result_a is result_b
        assert result_a is events[1]

    @pytest.mark.asyncio
    async def test___aiter___with_independent_iterators(self):
        """Test each __aiter__ call returns a distinct iterator.

        Given:
            A _SharedSubscription instance.
        When:
            __aiter__ is called twice.
        Then:
            It should return two distinct async generator instances.
        """
        # Arrange
        shared = _StubSubscriber("iter-key")

        # Act
        a = aiter(shared)
        b = aiter(shared)

        # Assert
        assert a is not b

    @pytest.mark.asyncio
    async def test___aiter___with_resource_release_on_exhaustion(self):
        """Test the pool resource is released when the source exhausts.

        Given:
            A _SharedSubscription backed by a finite source.
        When:
            The subscription is iterated to exhaustion.
        Then:
            The pool resource should be released.
        """

        # Arrange
        class _Source:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield _make_event()

        shared = _make_shared(_Source())

        # Act
        collected = [event async for event in shared]

        # Assert
        assert len(collected) == 1
        pool = __subscriber_pool__.get()
        assert pool.stats.referenced_entries == 0

    def test___reduce___pickle_roundtrip(self):
        """Test _SharedSubscription pickle roundtrip via cloudpickle.

        Given:
            A _SharedSubscription wrapping a stub subscriber with
            __reduce__.
        When:
            The subscription is pickled and unpickled.
        Then:
            It should produce a _SharedSubscription wrapping a
            subscriber with the same key.
        """
        # Arrange
        original = _StubSubscriber("pickle-key")

        # Act
        pickled = cloudpickle.dumps(original)
        restored = cloudpickle.loads(pickled)

        # Assert
        assert isinstance(restored, _SharedSubscription)

    @pytest.mark.asyncio
    async def test___aiter___with_late_joiner_replay(self):
        """Test late-joining consumer receives replayed worker state.

        Given:
            A subscription that has pulled two events, tracking
            two workers.
        When:
            A second subscription with the same key starts
            iterating while the first is still active.
        Then:
            The second should receive replayed worker-added events
            for all tracked workers.
        """
        # Arrange
        events = [_make_event(address=f"127.0.0.1:{50051 + i}") for i in range(2)]

        class _Source:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                for e in events:
                    yield e

        _setup_pool()
        key = "replay-key"
        source = _Source()
        _subscriber_factories[key] = lambda _: source

        sub_a = _SharedSubscription(key=key, reduce_info=(type(source), (), {}))
        sub_b = _SharedSubscription(key=key, reduce_info=(type(source), (), {}))

        # A pulls both events, tracking two workers.
        it_a = aiter(sub_a)
        await anext(it_a)
        await anext(it_a)

        # Act — B joins while A is still active, gets replay.
        collected_b = [event async for event in sub_b]

        # Assert — B receives replayed worker-added events.
        assert len(collected_b) == 2
        assert all(e.type == "worker-added" for e in collected_b)
        expected_uids = {str(e.metadata.uid) for e in events}
        actual_uids = {str(e.metadata.uid) for e in collected_b}
        assert actual_uids == expected_uids

    @pytest.mark.asyncio
    async def test___aiter___with_no_pool(self):
        """Test iteration raises RuntimeError when pool is absent.

        Given:
            A _SharedSubscription constructed directly with no
            subscriber pool initialised.
        When:
            The subscription is iterated.
        Then:
            It should raise RuntimeError.
        """
        # Arrange
        __subscriber_pool__.set(None)
        shared = _SharedSubscription(key="no-pool", reduce_info=(object, (), {}))

        # Act & assert
        with pytest.raises(RuntimeError, match="subscriber pool not initialised"):
            await anext(aiter(shared))

    @pytest.mark.asyncio
    async def test___aiter___with_worker_dropped_tracking(self):
        """Test worker-dropped events clear tracked worker state.

        Given:
            A subscription whose source yields worker-added then
            worker-dropped events for the same worker.
        When:
            A second subscription starts iterating after the first
            has processed both events.
        Then:
            The late joiner should receive no replay because the
            worker was dropped.
        """
        # Arrange
        worker_uid = __import__("uuid").uuid4()
        metadata = WorkerMetadata(
            uid=worker_uid,
            address="127.0.0.1:50051",
            pid=1,
            version="1.0.0",
        )
        add_event = DiscoveryEvent("worker-added", metadata=metadata)
        drop_event = DiscoveryEvent("worker-dropped", metadata=metadata)

        class _Source:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield add_event
                yield drop_event

        _setup_pool()
        key = "drop-key"
        source = _Source()
        _subscriber_factories[key] = lambda _: source

        sub_a = _SharedSubscription(key=key, reduce_info=(type(source), (), {}))
        sub_b = _SharedSubscription(key=key, reduce_info=(type(source), (), {}))

        # A pulls both events — worker added then dropped.
        it_a = aiter(sub_a)
        await anext(it_a)
        await anext(it_a)

        # Act — B joins; worker was dropped so no replay.
        collected_b = [event async for event in sub_b]

        # Assert — B receives nothing (worker was removed).
        assert collected_b == []

    @pytest.mark.asyncio
    async def test___aiter___after_cleanup(self):
        """Test iteration raises StopAsyncIteration after cleanup.

        Given:
            A _SharedSubscription whose Fanout has been cleaned up.
        When:
            The subscription is iterated.
        Then:
            It should raise StopAsyncIteration.
        """

        # Arrange
        class _Source:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                while True:
                    yield _make_event()  # pragma: no cover

        shared = _make_shared(_Source())
        it = aiter(shared)
        await anext(it)  # initialise the fanout

        # Retrieve and clean up the fanout
        pool = __subscriber_pool__.get()
        subscriber = pool._cache["test-key"].obj
        fanout = _SharedSubscription._fanouts.get(subscriber)
        await fanout.cleanup()

        # Act & assert
        with pytest.raises(StopAsyncIteration):
            await anext(it)
