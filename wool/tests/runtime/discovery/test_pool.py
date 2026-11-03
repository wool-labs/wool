from __future__ import annotations

import asyncio
import gc
import itertools
import logging
import subprocess
import sys
import uuid
import weakref

import cloudpickle
import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.runtime.discovery import __subscriber_pool__
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.pool import SubscriberMeta
from wool.runtime.discovery.pool import _SharedSubscription
from wool.runtime.discovery.pool import _SubscriberKey
from wool.runtime.discovery.pool import install_subscriber_pool
from wool.runtime.worker.metadata import WorkerMetadata

from . import _stranded_partition_probe


class _StubSubscriber(
    metaclass=SubscriberMeta,
    key=lambda cls, key_value: (cls, key_value),
):
    """Minimal subscriber stub for testing SubscriberMeta."""

    def __init__(self, key_value: str) -> None:
        self.key_value = key_value

    def __reduce__(self):
        return type(self), (self.key_value,)

    async def _shutdown(self) -> None:
        pass

    def __aiter__(self):
        return self._event_stream()

    async def _event_stream(self):
        while True:
            yield _make_event()


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
    return install_subscriber_pool()


def _make_shared(source, key="test-key"):
    """Create a _SharedSubscription backed by a raw source via the pool."""
    _setup_pool()
    return _SharedSubscription(
        key=_SubscriberKey(key, lambda: source),
        reduce_info=(type(source), (), {}),
    )


def _numbered_subscriber_class():
    """Build a subscriber class whose instances number themselves.

    Every instance the pool's factory builds takes the next number and
    addresses every event it yields with it, so a consumer can tell
    which instance served it from the events alone. The source never
    ends, so an iterator parked on it stays parked until something
    finalizes the subscriber behind it.
    """
    numbers = itertools.count()

    class _Numbered(
        metaclass=SubscriberMeta,
        key=lambda cls, tag: (cls, tag),
    ):
        def __init__(self, tag: str) -> None:
            self.tag = tag
            self.number = next(numbers)

        async def _shutdown(self) -> None:
            pass

        def __aiter__(self):
            return self._gen()

        async def _gen(self):
            while True:
                yield _make_event(address=f"127.0.0.1:{50051 + self.number}")

    return _Numbered


@pytest.mark.asyncio
async def test_install_subscriber_pool_should_return_the_pool_subscribers_cache_in(
    subscriber_pool,
):
    """Test the installed pool is the one a subscriber caches itself in.

    Given:
        A context carrying the subscriber pool the autouse fixture
        installed.
    When:
        A subscription built in that context is iterated as far as its
        first event, so the pool has built the subscriber behind it and
        still holds a reference on it.
    Then:
        It should hold that subscriber in the installed pool, which is
        what makes clearing that object a teardown of what the test
        actually built rather than of an empty pool nothing ever used.
    """

    # Arrange
    shared = _numbered_subscriber_class()("installed-pool-key")
    events = aiter(shared)
    await anext(events)

    # Act
    installed = install_subscriber_pool()

    # Assert
    assert installed is subscriber_pool
    assert subscriber_pool.stats.referenced_entries == 1
    await events.aclose()


@pytest.mark.asyncio
async def test_install_subscriber_pool_should_refuse_a_key_that_cannot_build(
    subscriber_pool,
):
    """Test the installed pool rejects a key that cannot build.

    Given:
        The pool this function installs, whose every acquisition is
        made under a key carrying its own factory.
    When:
        A resource is acquired under a bare key that carries none.
    Then:
        It should refuse the acquisition, naming the key, rather than
        failing later on an attribute the key never had.
    """
    # Arrange
    pool = install_subscriber_pool()
    assert pool is subscriber_pool

    # Act & assert
    with pytest.raises(TypeError, match="carries no factory"):
        async with pool.get("bare-key"):
            pass  # pragma: no cover


class TestSubscriberKey:
    """Tests for _SubscriberKey dataclass.

    Fully qualified name: wool.runtime.discovery.pool._SubscriberKey
    """

    @pytest.mark.asyncio
    async def test___repr___should_name_the_identity_in_a_pool_record(
        self, subscriber_pool, caplog
    ):
        """Test a key renders as its identity where the pool reports one.

        Given:
            A pooled subscriber whose shutdown raises, so retiring it
            makes the pool report the failure against the key it was
            cached under.
        When:
            The subscription is iterated and the pool is then cleared.
        Then:
            It should name the subscriber by its identity alone, since a
            record carrying the factory would identify a subscriber by a
            function address that differs between two keys the pool
            treats as one.
        """

        # Arrange
        class _Failing(
            metaclass=SubscriberMeta,
            key=lambda cls, tag: (cls, tag),
        ):
            def __init__(self, tag: str) -> None:
                self.tag = tag

            async def _shutdown(self) -> None:
                raise RuntimeError("shutdown refused")

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                while True:
                    yield _make_event()

        shared = _Failing("repr-key")
        iterator = aiter(shared)
        await anext(iterator)
        await iterator.aclose()

        # Act
        with caplog.at_level(logging.WARNING):
            await subscriber_pool.clear()

        # Assert
        records = [r.getMessage() for r in caplog.records if "key" in r.getMessage()]
        assert records, caplog.records
        assert any(repr((_Failing, "repr-key")) in message for message in records)
        assert not any("function" in message for message in records)


@pytest.mark.asyncio
async def test_install_subscriber_pool_should_report_the_first_retirement_failure(
    subscriber_pool, caplog
):
    """Test the earlier of two retirement failures is the one reported.

    Given:
        A pooled subscriber whose fanout fails to close and whose
        shutdown then also raises, so retiring it produces two
        failures.
    When:
        The subscription is iterated and the pool is then cleared.
    Then:
        It should report the fanout's failure, since retiring the
        subscriber must not hide why closing its fanout did not work.
    """

    # Arrange
    class _DoublyFailing(
        metaclass=SubscriberMeta,
        key=lambda cls, tag: tag,
    ):
        def __init__(self, tag: str) -> None:
            self.tag = tag

        async def _shutdown(self) -> None:
            raise RuntimeError("shutdown refused")

        def __aiter__(self):
            return self._gen()

        async def _gen(self):
            try:
                while True:
                    yield _make_event()
            finally:
                raise RuntimeError("cleanup refused")

    shared = _DoublyFailing("both-fail-key")
    iterator = aiter(shared)
    await anext(iterator)

    # Act
    #
    # Closing the last iteration retires the entry, because the pool has
    # no TTL, so the finalizer runs here and the pool logs what it
    # raised against the resource it was retiring.
    with caplog.at_level(logging.WARNING):
        await iterator.aclose()

    # Assert
    reported = [
        record.exc_info[1] for record in caplog.records if record.exc_info is not None
    ]
    assert reported, [record.getMessage() for record in caplog.records]
    assert any("cleanup refused" in str(failure) for failure in reported), reported
    assert not any("shutdown refused" in str(failure) for failure in reported), reported


@pytest.mark.subprocess_probe
def test___aiter___should_retain_no_subscriber_when_its_partition_is_discarded():
    """Test a partition stranded by a closed loop retains no subscriber.

    Given:
        A subscription iterated on an event loop that then closes
        without draining, stranding a referenced pool entry, which the
        next operation from a live loop discards without running
        finalizers.
    When:
        Every reference the probe holds is dropped and a collection
        runs.
    Then:
        It should leave nothing referencing the subscriber, since
        per-subscriber state reachable from a class-level mapping
        outlives a discard and is immortal for the process.
    """
    # Arrange
    probe = _stranded_partition_probe.__file__

    # Act
    result = subprocess.run(
        [sys.executable, probe], capture_output=True, text=True, timeout=60
    )

    # Assert
    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout


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
    async def test___new___with_shared_fan_out(self, subscriber_pool):
        """Test SubscriberMeta shares events across subscriptions.

        Given:
            A source that yields events and two subscriptions
            created with the same key, each carrying a factory of its
            own.
        When:
            Both consumers are initialised and one pulls a new
            event.
        Then:
            The other should receive the same event via fan-out, from
            the one entry both keys resolve to — a key compares and
            hashes on its identity alone, so the factory it carries
            cannot split them apart.
        """
        # Arrange
        events = [_make_event(address=f"127.0.0.1:{50051 + i}") for i in range(3)]

        class _EventStub(
            metaclass=SubscriberMeta,
            key=lambda cls, tag: (cls, tag),
        ):
            def __init__(self, tag):
                self.tag = tag

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

        # Assert — same object via fan-out, from one shared entry
        assert result_a is result_b
        assert result_a is events[1]
        assert subscriber_pool.stats.total_entries == 1

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

        class _IsoStub(
            metaclass=SubscriberMeta,
            key=lambda cls, tag, event: (cls, tag),
        ):
            def __init__(self, tag, event):
                self.tag = tag
                self._event = event

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

    @pytest.mark.asyncio
    async def test___new___should_isolate_two_classes_whose_keys_omit_the_class(self):
        """Test two subscriber classes with equal keys stay distinct.

        Given:
            Two subscriber classes whose key callables satisfy the
            documented contract but omit the class, so both return the
            same key for the same argument, each yielding events naming
            which class served them.
        When:
            Both are constructed under that argument and iterated, with
            both iterations held open at once.
        Then:
            It should serve each class from its own subscriber, since a
            key that cannot name the class must not make two classes
            share one backend.
        """

        # Arrange
        def _subscriber_class(label):
            class _Subscriber(
                metaclass=SubscriberMeta,
                key=lambda cls, tag: tag,
            ):
                def __init__(self, tag: str) -> None:
                    self.tag = tag

                async def _shutdown(self) -> None:
                    pass

                def __aiter__(self):
                    return self._gen()

                async def _gen(self):
                    while True:
                        yield _make_event(address=label)

            return _Subscriber

        first = _subscriber_class("from-first")
        second = _subscriber_class("from-second")

        # Act
        #
        # Both iterations are held open across the second's first pull.
        # The pool has ``ttl=0``, so releasing the first would retire
        # its entry and the second would miss a cache already gone —
        # which reads as correct isolation whether or not it is.
        first_events = aiter(first("shared-tag"))
        second_events = aiter(second("shared-tag"))
        from_first = await anext(first_events)
        from_second = await anext(second_events)

        # Assert
        assert from_first.metadata.address == "from-first"
        assert from_second.metadata.address == "from-second"

        # Cleanup
        await first_events.aclose()
        await second_events.aclose()

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

    def test___new___with_key_callable_receiving_cls(self):
        """Test key callable receives the class as its first argument.

        Given:
            A subscriber class defined with a key callable that
            includes cls in the returned tuple.
        When:
            The class is instantiated.
        Then:
            It should produce a _SharedSubscription whose cache key
            was computed using the class itself.
        """
        # Arrange
        captured = {}

        def capture_key(cls, value):
            captured["cls"] = cls
            captured["value"] = value
            return (cls, value)

        class _CaptureSub(
            metaclass=SubscriberMeta,
            key=capture_key,
        ):
            def __init__(self, value):
                self.value = value

            async def _shutdown(self):
                pass

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield  # pragma: no cover

        # Act
        result = _CaptureSub("test-val")

        # Assert
        assert isinstance(result, _SharedSubscription)
        assert captured["cls"] is _CaptureSub
        assert captured["value"] == "test-val"

    def test___new___with_inherited_key(self):
        """Test subclass inherits key callable from parent.

        Given:
            A parent subscriber class defined with a key callable
            and a subclass that does not pass its own key.
        When:
            The subclass is instantiated.
        Then:
            It should inherit the parent's key callable and return
            a _SharedSubscription.
        """

        # Arrange
        class _Parent(
            metaclass=SubscriberMeta,
            key=lambda cls, tag: (cls, tag),
        ):
            def __init__(self, tag):
                self.tag = tag

            async def _shutdown(self):
                pass

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield  # pragma: no cover

        class _Child(_Parent):
            pass

        # Act
        result = _Child("child-tag")

        # Assert
        assert isinstance(result, _SharedSubscription)

    def test___new___should_raise_when_the_key_is_unhashable(self):
        """Test an unhashable key is refused by the construction making it.

        Given:
            A subscriber class whose key callable returns a value that
            cannot be hashed.
        When:
            The class is instantiated.
        Then:
            It should raise `TypeError` here, where the offending key
            callable is named, rather than inside the pool at the first
            iteration of a subscription built with it.
        """

        # Arrange
        class _Unhashable(
            metaclass=SubscriberMeta,
            key=lambda cls, tag: (cls, [tag]),
        ):
            def __init__(self, tag: str) -> None:
                self.tag = tag

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield  # pragma: no cover

        # Act & assert
        with pytest.raises(TypeError, match="unhashable"):
            _Unhashable("unhashable-key")

    @pytest.mark.asyncio
    async def test___new___should_release_the_class_when_never_iterated(
        self, subscriber_pool
    ):
        """Test a construction alone pins nothing once it is dropped.

        Given:
            A subscriber class defined in this test and constructed,
            with no iteration, so the pool has never been asked to build
            anything under its key.
        When:
            Every reference the test holds is dropped and a collection
            runs.
        Then:
            It should leave nothing referencing the class, so a weak
            reference to it clears — a construction is what used to
            register a factory, and registering is what outlived the
            caller.
        """

        # Arrange
        class _Unused(
            metaclass=SubscriberMeta,
            key=lambda cls, tag: (cls, tag),
        ):
            def __init__(self, tag: str) -> None:
                self.tag = tag

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield  # pragma: no cover

        shared = _Unused("never-iterated")
        # Asserted so the reclamation below cannot be read as an entry
        # the pool quietly evicted rather than one it never built.
        assert subscriber_pool.stats.total_entries == 0
        reference = weakref.ref(_Unused)

        # Act
        del shared, _Unused
        gc.collect()

        # Assert
        assert reference() is None

    @pytest.mark.asyncio
    async def test___new___should_build_with_the_surviving_arguments(
        self, subscriber_pool
    ):
        """Test the construction a build is made for supplies its arguments.

        Given:
            A key callable that leaves a constructor argument out, and
            two constructions sharing an identity while differing in
            that argument, the first dropped and collected.
        When:
            The second is iterated, so the pool builds for it.
        Then:
            It should build with the second's argument, since each
            construction carries its own factory rather than deferring
            to whichever one registered first.
        """

        # Arrange
        class _Marked(
            metaclass=SubscriberMeta,
            key=lambda cls, tag, mark: (cls, tag),
        ):
            def __init__(self, tag: str, mark: str) -> None:
                self.tag = tag
                self.mark = mark

            async def _shutdown(self) -> None:
                pass

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                while True:
                    yield _make_event(address=f"127.0.0.1:{self.mark}")

        first = _Marked("shared-tag", "50051")
        second = _Marked("shared-tag", "50052")
        del first
        gc.collect()

        # Act
        iterator = aiter(second)
        event = await anext(iterator)
        await iterator.aclose()

        # Assert
        assert event.metadata.address == "127.0.0.1:50052"

    @pytest.mark.asyncio
    async def test___new___should_release_the_class_when_subscriptions_end(
        self, subscriber_pool
    ):
        """Test constructing a subscriber pins its class no longer than it.

        Given:
            A subscriber class defined in this test over a finite
            source, and a subscription over it iterated to exhaustion,
            so the pool has released and finalized the subscriber it
            built and holds nothing under its key.
        When:
            Every reference the test holds is dropped and a collection
            runs.
        Then:
            It should leave nothing referencing the class, so a weak
            reference to it clears — a construction outliving its
            subscriptions pins one class per lifecycle for the life of
            the process.
        """

        # Arrange
        class _Ephemeral(
            metaclass=SubscriberMeta,
            key=lambda cls, tag: (cls, tag),
        ):
            def __init__(self, tag: str) -> None:
                self.tag = tag

            async def _shutdown(self) -> None:
                pass

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield _make_event()

        shared = _Ephemeral("ephemeral-key")
        assert [event async for event in shared]
        # Asserted so a lingering entry cannot be mistaken below for the
        # retention this test is about.
        assert subscriber_pool.stats.total_entries == 0
        reference = weakref.ref(_Ephemeral)

        # Act
        del shared, _Ephemeral
        gc.collect()

        # Assert
        assert reference() is None


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
        source = _Source()
        key = _SubscriberKey("fan-out-key", lambda: source)

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
        source = _Source()
        key = _SubscriberKey("replay-key", lambda: source)

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
        source = _Source()
        key = _SubscriberKey("drop-key", lambda: source)

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
    async def test___aiter___should_survive_a_peer_iterations_cancellation(self):
        """Test cancelling one iteration leaves its peers subscribed.

        Given:
            Two iterations of one shared subscription over a source that
            suspends before its first event, the first parked on a pull
        When:
            That first iteration is cancelled and the source then yields
        Then:
            It should deliver the event to the second iteration, one
            consumer's teardown not being the end of a feed its peers
            still hold.
        """
        # Arrange
        release = asyncio.Event()

        class _Suspending(
            metaclass=SubscriberMeta,
            key=lambda cls, tag: (cls, tag),
        ):
            def __init__(self, tag: str) -> None:
                self.tag = tag

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                await release.wait()
                while True:
                    yield _make_event()

        shared = _Suspending("cancelled-peer-key")
        first = aiter(shared)
        second = aiter(shared)
        parked = asyncio.ensure_future(anext(first))
        waiting = asyncio.ensure_future(anext(second))
        # Both iterations are on the shared source before either is
        # cancelled, so the survivor is a genuine peer.
        await asyncio.sleep(0.05)
        assert not parked.done()
        assert not waiting.done()

        # Act
        parked.cancel()
        with pytest.raises(asyncio.CancelledError):
            await parked
        release.set()

        # Assert
        event = await asyncio.wait_for(waiting, timeout=2)
        assert event.type == "worker-added"

    @pytest.mark.asyncio
    async def test___aiter___should_raise_stop_async_iteration_when_pool_cleared(
        self,
    ):
        """Test iteration ends once the pool finalizes the shared subscriber.

        Given:
            A subscription whose iterator is parked on an endless
            source, holding the pool's only reference to the subscriber
            behind it.
        When:
            The subscriber pool is cleared on the loop that cached it.
        Then:
            It should raise StopAsyncIteration on the next pull, the
            finalized subscriber's fan-out having no events left to
            hand out.
        """
        # Arrange
        shared = _numbered_subscriber_class()("cleanup-key")
        it = aiter(shared)
        await anext(it)  # initialise the fanout
        pool = __subscriber_pool__.get()

        # Act -- clear force-finalizes the cached subscriber, cleaning
        # up its fan-out, even though the parked iterator still holds a
        # reference to it.
        await pool.clear()

        # Assert
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test___aiter___should_build_a_subscriber_per_loop_when_iterated_on_two(
        self, background_loops
    ):
        """Test one cache key is served by one subscriber per iterating loop.

        Given:
            A subscription for a single key whose subscriber instances
            number themselves, already iterated and still referenced on
            the test loop.
        When:
            The same subscription is iterated on a background loop.
        Then:
            It should serve that loop from a second subscriber instance,
            leaving the test loop's own entry cached, since the pool
            caches per loop and not per key alone.
        """
        # Arrange
        shared = _numbered_subscriber_class()("two-loop-key")
        pool = __subscriber_pool__.get()
        background = background_loops(teardown=lambda: pool.clear())
        it = aiter(shared)
        local = await anext(it)

        async def first_event(pool, shared):
            # ``run_coroutine_threadsafe`` does not carry the caller's
            # context, so the pool is handed over and installed here.
            __subscriber_pool__.set(pool)
            assert __subscriber_pool__.get() is pool
            return await anext(aiter(shared))

        # Act
        remote = background.run(first_event(pool, shared))

        # Assert
        assert remote.metadata.address != local.metadata.address
        assert pool.stats.total_entries == 1

    @pytest.mark.asyncio
    async def test___aiter___should_keep_yielding_when_another_loop_clears_the_pool(
        self, background_loops
    ):
        """Test a clear on one loop leaves another loop's subscriber iterable.

        Given:
            One subscription iterated on both the test loop and a
            background loop, each served by its own subscriber instance.
        When:
            The subscriber pool is cleared on the test loop.
        Then:
            It should leave the background loop's iterator yielding from
            the subscriber it already had, the clear having reached only
            the partition of the loop that ran it.
        """
        # Arrange
        shared = _numbered_subscriber_class()("clear-key")
        pool = __subscriber_pool__.get()
        background = background_loops(teardown=lambda: pool.clear())
        parked: dict = {}

        async def start_iterating(pool, shared, parked):
            # ``run_coroutine_threadsafe`` does not carry the caller's
            # context, so the pool is handed over and installed here.
            __subscriber_pool__.set(pool)
            assert __subscriber_pool__.get() is pool
            parked["iterator"] = aiter(shared)
            return await anext(parked["iterator"])

        async def next_event(pool, parked):
            __subscriber_pool__.set(pool)
            assert __subscriber_pool__.get() is pool
            return await anext(parked["iterator"])

        before = background.run(start_iterating(pool, shared, parked))
        await anext(aiter(shared))

        # Act
        await pool.clear()

        # Assert
        after = background.run(next_event(pool, parked))
        assert after.metadata.address == before.metadata.address
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test___aiter___should_rebuild_when_its_subscriber_retired(
        self, subscriber_pool
    ):
        """Test a subscription outlives the subscriber it built.

        Given:
            A subscription iterated as far as its first event and then
            closed, so the pool released its only reference and, having
            no TTL, finalized and evicted the subscriber behind it.
        When:
            The same subscription is iterated a second time.
        Then:
            It should build a second subscriber and yield from it, since
            a subscription holds no subscriber of its own and has to be
            able to rebuild one for as long as it is reachable.
        """
        # Arrange
        shared = _numbered_subscriber_class()("rebuild-key")
        first = aiter(shared)
        before = await anext(first)
        await first.aclose()
        # Asserted so the second iteration below is known to be a
        # rebuild rather than a reuse of an entry still cached.
        assert subscriber_pool.stats.total_entries == 0

        # Act
        second = aiter(shared)
        after = await anext(second)

        # Assert
        assert before.metadata.address == "127.0.0.1:50051"
        assert after.metadata.address == "127.0.0.1:50052"
        await second.aclose()

    @pytest.mark.asyncio
    async def test___reduce___should_share_an_entry_with_a_local_construction(
        self, subscriber_pool
    ):
        """Test a restored subscription joins the entry a local one builds.

        Given:
            A subscription restored from its reduction alongside a fresh
            local construction under the same identity, each therefore
            carrying a factory object of its own.
        When:
            Both are iterated.
        Then:
            It should serve both from one entry, since reduction
            replays the construction rather than the key, and the key
            the far side rebuilds compares equal to a local one.
        """
        # Arrange
        original = _StubSubscriber("restored-key")
        restored = cloudpickle.loads(cloudpickle.dumps(original))
        local = _StubSubscriber("restored-key")

        # Act
        first = aiter(restored)
        second = aiter(local)
        await anext(first)
        await anext(second)

        # Assert
        assert subscriber_pool.stats.total_entries == 1
        await first.aclose()
        await second.aclose()

    @given(
        key_value=st.text(
            alphabet=st.characters(whitelist_categories=("Ll", "Nd")),
            min_size=1,
            max_size=32,
        )
    )
    @settings(max_examples=25, deadline=None)
    def test___reduce___should_round_trip_across_the_construction_domain(
        self, key_value
    ):
        """Test reduction is a fixed point over the constructor's domain.

        Given:
            Any key value a subscriber accepts.
        When:
            The subscription is reduced, restored, and reduced again.
        Then:
            It should produce identical bytes both times, since the
            payload replays the construction and never reaches the
            key or the factory the key carries.
        """
        # Arrange
        original = _StubSubscriber(key_value)

        # Act
        payload = cloudpickle.dumps(original)
        restored = cloudpickle.loads(payload)

        # Assert
        assert cloudpickle.dumps(restored) == payload
        assert isinstance(restored, _SharedSubscription)
