from __future__ import annotations

import uuid

import cloudpickle
import pytest

from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import WorkerMetadata
from wool.utilities.afilter import afilter


def _make_metadata(*, address: str = "127.0.0.1:50051", **kwargs):
    defaults = dict(
        uid=uuid.uuid4(),
        address=address,
        pid=1234,
        version="1.0.0",
    )
    defaults.update(kwargs)
    return WorkerMetadata(**defaults)


class _Source:
    """Async-iterable source that yields given events on each
    ``__aiter__`` call."""

    def __init__(self, *events: DiscoveryEvent):
        self._events = events

    def __aiter__(self):
        return self._iter()

    async def _iter(self):
        for event in self._events:
            yield event


class Testafilter:
    """Tests for the afilter utility class."""

    @pytest.mark.asyncio
    async def test___aiter___with_single_matching_event(self):
        """Test basic filtered iteration with one matching event.

        Given:
            A source yielding one worker-added event.
        When:
            afilter is iterated with a predicate that accepts the
            worker.
        Then:
            It should yield the worker-added event.
        """
        # Arrange
        worker = _make_metadata()
        source = _Source(DiscoveryEvent("worker-added", metadata=worker))

        # Act
        events = []
        async for e in afilter(lambda _: True, source):
            events.append(e)

        # Assert
        assert len(events) == 1
        assert events[0].type == "worker-added"
        assert events[0].metadata == worker

    @pytest.mark.asyncio
    async def test___aiter___with_add_then_update(self):
        """Test unfiltered passthrough of multiple events.

        Given:
            A source yielding worker-added then worker-updated.
        When:
            afilter is iterated with an accept-all predicate.
        Then:
            It should yield both events in order.
        """
        # Arrange
        worker = _make_metadata()
        source = _Source(
            DiscoveryEvent("worker-added", metadata=worker),
            DiscoveryEvent("worker-updated", metadata=worker),
        )

        # Act
        events = []
        async for e in afilter(lambda _: True, source):
            events.append(e)

        # Assert
        assert len(events) == 2
        assert events[0].type == "worker-added"
        assert events[1].type == "worker-updated"

    @pytest.mark.asyncio
    async def test___aiter___with_rejecting_filter(self):
        """Test filter suppression of non-matching events.

        Given:
            A source yielding a worker-added event.
        When:
            afilter is iterated with a predicate that rejects the
            worker.
        Then:
            It should yield no events.
        """
        # Arrange
        worker = _make_metadata(address="127.0.0.1:9999")
        source = _Source(DiscoveryEvent("worker-added", metadata=worker))

        # Act
        events = []
        async for e in afilter(lambda w: w.address == "never", source):
            events.append(e)

        # Assert
        assert events == []

    @pytest.mark.asyncio
    async def test___aiter___with_filter_entry_transition(self):
        """Test that a worker entering the filter emits worker-added.

        Given:
            A source yielding worker-added (fails filter) then
            worker-updated (passes filter).
        When:
            afilter is iterated with a filter on tags.
        Then:
            It should yield worker-added for the now-matching worker.
        """
        # Arrange
        uid = uuid.uuid4()
        worker_v1 = _make_metadata(uid=uid, tags=frozenset())
        worker_v2 = _make_metadata(uid=uid, tags=frozenset({"gpu"}))
        source = _Source(
            DiscoveryEvent("worker-added", metadata=worker_v1),
            DiscoveryEvent("worker-updated", metadata=worker_v2),
        )

        # Act
        events = []
        async for e in afilter(lambda w: "gpu" in w.tags, source):
            events.append(e)

        # Assert
        assert len(events) == 1
        assert events[0].type == "worker-added"
        assert events[0].metadata == worker_v2

    @pytest.mark.asyncio
    async def test___aiter___with_filter_exit_transition(self):
        """Test that a worker leaving the filter emits worker-dropped.

        Given:
            A source yielding worker-added (passes filter) then
            worker-updated (fails filter).
        When:
            afilter is iterated with a filter the worker initially
            passes.
        Then:
            It should yield worker-added then worker-dropped.
        """
        # Arrange
        uid = uuid.uuid4()
        worker_v1 = _make_metadata(uid=uid, tags=frozenset({"gpu"}))
        worker_v2 = _make_metadata(uid=uid, tags=frozenset())
        source = _Source(
            DiscoveryEvent("worker-added", metadata=worker_v1),
            DiscoveryEvent("worker-updated", metadata=worker_v2),
        )

        # Act
        events = []
        async for e in afilter(lambda w: "gpu" in w.tags, source):
            events.append(e)

        # Assert
        assert len(events) == 2
        assert events[0].type == "worker-added"
        assert events[1].type == "worker-dropped"
        assert events[1].metadata == worker_v1

    @pytest.mark.asyncio
    async def test___aiter___with_tracked_worker_dropped(self):
        """Test worker-dropped for a tracked worker.

        Given:
            A source yielding worker-added then worker-dropped.
        When:
            afilter is iterated with an accept-all predicate.
        Then:
            It should yield worker-added then worker-dropped.
        """
        # Arrange
        worker = _make_metadata()
        source = _Source(
            DiscoveryEvent("worker-added", metadata=worker),
            DiscoveryEvent("worker-dropped", metadata=worker),
        )

        # Act
        events = []
        async for e in afilter(lambda _: True, source):
            events.append(e)

        # Assert
        assert len(events) == 2
        assert events[0].type == "worker-added"
        assert events[1].type == "worker-dropped"

    @pytest.mark.asyncio
    async def test___aiter___with_untracked_worker_dropped(self):
        """Test worker-dropped for an untracked worker is ignored.

        Given:
            A source yielding worker-added (fails filter) then
            worker-dropped for the same worker.
        When:
            afilter is iterated with a rejecting filter.
        Then:
            It should yield no events.
        """
        # Arrange
        worker = _make_metadata()
        source = _Source(
            DiscoveryEvent("worker-added", metadata=worker),
            DiscoveryEvent("worker-dropped", metadata=worker),
        )

        # Act
        events = []
        async for e in afilter(lambda _: False, source):
            events.append(e)

        # Assert
        assert events == []

    @pytest.mark.asyncio
    async def test___aiter___with_independent_iterations(self):
        """Test that each __aiter__ call produces independent state.

        Given:
            An afilter wrapping a source that yields the same events
            on each iteration.
        When:
            The afilter is iterated twice sequentially.
        Then:
            Each iteration should produce the same results with
            independent tracked-worker state.
        """
        # Arrange
        worker = _make_metadata()
        source = _Source(
            DiscoveryEvent("worker-added", metadata=worker),
        )
        filtered = afilter(lambda _: True, source)

        # Act
        first = [e async for e in filtered]
        second = [e async for e in filtered]

        # Assert
        assert len(first) == 1
        assert first[0].type == "worker-added"
        assert len(second) == 1
        assert second[0].type == "worker-added"

    def test___reduce___pickle_roundtrip(self):
        """Test afilter pickle roundtrip via cloudpickle.

        Given:
            An afilter wrapping a picklable subscriber.
        When:
            The filter is pickled and unpickled.
        Then:
            It should produce an afilter instance that can be
            iterated.
        """
        # Arrange
        from wool.runtime.discovery.local import LocalDiscovery

        inner = LocalDiscovery.Subscriber("afilter-pickle-ns")
        filtered = afilter(lambda w: True, inner)

        # Act
        pickled = cloudpickle.dumps(filtered)
        restored = cloudpickle.loads(pickled)

        # Assert
        assert isinstance(restored, afilter)
