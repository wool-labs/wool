import uuid
from types import MappingProxyType
from typing import Any
from unittest.mock import AsyncMock

import pytest
from hypothesis import given
from hypothesis import strategies as st

from wool.runtime.discovery.base import WorkerMetadata
from wool.runtime.worker.base import Worker
from wool.runtime.worker.base import WorkerFactory
from wool.runtime.worker.base import WorkerLike


class ConcreteWorker(Worker):
    """Concrete implementation of Worker for testing."""

    def __init__(self, *tags: str, **extra: Any):
        super().__init__(*tags, **extra)
        self._address = None
        self._host = None
        self._port = None

    @property
    def address(self) -> str | None:
        return self._address

    @property
    def host(self) -> str | None:
        return self._host

    @property
    def port(self) -> int | None:
        return self._port

    async def _start(self, timeout: float | None):
        """Mock start implementation."""
        self._address = "localhost:50051"
        self._host = "localhost"
        self._port = 50051
        self._info = WorkerMetadata(
            uid=self._uid,
            host="localhost",
            port=50051,
            pid=12345,
            version="1.0.0",
            tags=frozenset(self._tags),
            extra=MappingProxyType(self._extra),
        )

    async def _stop(self, timeout: float | None):
        """Mock stop implementation."""
        pass


class TestWorker:
    """Test suite for Worker abstract base class."""

    def test_init_assigns_unique_uid(self):
        """Test Worker initialization assigns a unique UUID.

        Given:
            No pre-existing worker instances
        When:
            A new Worker is instantiated
        Then:
            It should have a unique UUID assigned
        """
        worker = ConcreteWorker()
        assert isinstance(worker.uid, uuid.UUID)

    def test_init_stores_tags_as_set(self, worker_tags):
        """Test Worker initialization stores tags as a set.

        Given:
            Worker capability tags
        When:
            A Worker is instantiated with tags
        Then:
            Tags should be stored as a set
        """
        worker = ConcreteWorker(*worker_tags)
        assert worker.tags == set(worker_tags)

    def test_init_stores_extra_metadata(self, worker_extra):
        """Test Worker initialization stores extra metadata.

        Given:
            Extra metadata key-value pairs
        When:
            A Worker is instantiated with extra metadata
        Then:
            Metadata should be stored and accessible
        """
        worker = ConcreteWorker(**worker_extra)
        assert worker.extra == worker_extra

    def test_info_property_returns_none_before_start(self):
        """Test Worker info property returns None before starting.

        Given:
            A Worker instance that has not been started
        When:
            The info property is accessed
        Then:
            It should return None
        """
        worker = ConcreteWorker()
        assert worker.metadata is None

    @pytest.mark.asyncio
    async def test_start_calls_implementation_start(self, mocker, metadata):
        """Test Worker start method calls _start implementation.

        Given:
            A Worker with mocked _start method
        When:
            start() is called
        Then:
            It should call the _start implementation
        """
        worker = ConcreteWorker()

        async def mock_start_impl(timeout):
            worker._info = metadata

        mock_start = mocker.patch.object(
            worker, "_start", side_effect=mock_start_impl, new_callable=AsyncMock
        )
        await worker.start(timeout=60.0)
        mock_start.assert_called_once_with(timeout=60.0)

    @pytest.mark.asyncio
    async def test_start_enables_worker_operation(self):
        """Test Worker start method enables worker operation.

        Given:
            A Worker instance
        When:
            start() is called
        Then:
            The worker should be operational (info is set)
        """
        worker = ConcreteWorker()
        await worker.start()
        assert worker.metadata is not None

    @pytest.mark.asyncio
    async def test_start_raises_error_if_already_started(self):
        """Test Worker start raises RuntimeError if already started.

        Given:
            A Worker that has already been started
        When:
            start() is called again
        Then:
            It should raise RuntimeError
        """
        worker = ConcreteWorker()
        await worker.start()
        with pytest.raises(RuntimeError, match="already been started"):
            await worker.start()

    @pytest.mark.asyncio
    async def test_start_raises_error_for_non_positive_timeout(self):
        """Test Worker start raises ValueError for non-positive timeout.

        Given:
            A Worker instance
        When:
            start() is called with non-positive timeout
        Then:
            It should raise ValueError
        """
        worker = ConcreteWorker()
        with pytest.raises(ValueError, match="Timeout must be positive"):
            await worker.start(timeout=0)

    @pytest.mark.asyncio
    @given(timeout=st.floats(min_value=0.001, max_value=1000.0))
    async def test_start_accepts_positive_timeouts(self, timeout):
        """Test Worker start accepts positive timeout values.

        Given:
            A Worker instance and a positive timeout value
        When:
            start() is called with the timeout
        Then:
            It should complete without error and worker should be operational
        """
        worker = ConcreteWorker()
        await worker.start(timeout=timeout)
        assert worker.metadata is not None

    @given(count=st.integers(min_value=2, max_value=100))
    def test_multiple_workers_have_unique_uids(self, count):
        """Test multiple Workers get unique UIDs.

        Given:
            Creating multiple worker instances
        When:
            Workers are instantiated
        Then:
            All should have unique UIDs
        """
        workers = [ConcreteWorker() for _ in range(count)]
        uids = [w.uid for w in workers]
        assert len(uids) == len(set(uids))  # All unique

    @given(tags=st.lists(st.text(min_size=1, max_size=20), min_size=1, max_size=10))
    def test_init_deduplicates_tags(self, tags):
        """Test Worker initialization deduplicates tags.

        Given:
            A list of tags potentially containing duplicates
        When:
            A Worker is instantiated with these tags
        Then:
            Tags should be stored as a unique set
        """
        worker = ConcreteWorker(*tags)
        assert len(worker.tags) == len(set(tags))
        assert worker.tags == set(tags)

    @given(
        extra=st.dictionaries(
            keys=st.text(
                min_size=1,
                max_size=50,
                alphabet=st.characters(
                    whitelist_categories=("L", "N"), whitelist_characters="_"
                ),
            ),
            values=st.one_of(
                st.integers(),
                st.floats(allow_nan=False, allow_infinity=False),
                st.text(max_size=100),
                st.booleans(),
            ),
            min_size=0,
            max_size=10,
        )
    )
    def test_init_preserves_extra_metadata(self, extra):
        """Test Worker preserves arbitrary extra metadata.

        Given:
            Random dictionary of extra metadata
        When:
            Worker is instantiated with this metadata
        Then:
            Metadata should be stored exactly as provided
        """
        worker = ConcreteWorker(**extra)
        assert worker.extra == extra

    @pytest.mark.asyncio
    async def test_stop_calls_implementation_stop(self, mocker):
        """Test Worker stop method calls _stop implementation.

        Given:
            A started Worker with mocked _stop method
        When:
            stop() is called
        Then:
            It should call the _stop implementation
        """
        worker = ConcreteWorker()
        await worker.start()
        mock_stop = mocker.patch.object(worker, "_stop", new_callable=AsyncMock)
        await worker.stop(timeout=60.0)
        mock_stop.assert_called_once_with(60.0)

    @pytest.mark.asyncio
    async def test_stop_allows_restart(self):
        """Test Worker stop method allows worker to be restarted.

        Given:
            A started Worker instance
        When:
            stop() is called
        Then:
            The worker should be able to start again
        """
        worker = ConcreteWorker()
        await worker.start()
        await worker.stop()
        # Should be able to start again without error
        await worker.start()
        assert worker.metadata is not None

    @pytest.mark.asyncio
    async def test_stop_raises_error_if_not_started(self):
        """Test Worker stop raises RuntimeError if not started.

        Given:
            A Worker that has not been started
        When:
            stop() is called
        Then:
            It should raise RuntimeError
        """
        worker = ConcreteWorker()
        with pytest.raises(RuntimeError, match="has not been started"):
            await worker.stop()

    @pytest.mark.asyncio
    async def test_stop_enables_restart_even_on_error(self, mocker):
        """Test Worker stop enables restart even if _stop raises error.

        Given:
            A started Worker where _stop raises an exception
        When:
            stop() is called and raises error
        Then:
            The worker should still be able to restart
        """
        worker = ConcreteWorker()
        await worker.start()
        mocker.patch.object(
            worker, "_stop", side_effect=Exception("Stop failed"), new_callable=AsyncMock
        )
        with pytest.raises(Exception, match="Stop failed"):
            await worker.stop()
        # Should still be able to restart despite error
        mocker.patch.object(worker, "_stop", new_callable=AsyncMock)
        await worker.start()
        assert worker.metadata is not None


class TestWorkerLike:
    """Test suite for WorkerLike protocol."""

    def test_isinstance_check_for_compatible_implementation(self):
        """Test isinstance check returns True for compatible implementation.

        Given:
            A ConcreteWorker instance that implements WorkerLike
        When:
            isinstance check is performed
        Then:
            It should return True
        """
        worker = ConcreteWorker()
        assert isinstance(worker, WorkerLike)

    def test_isinstance_check_for_incompatible_implementation(self):
        """Test isinstance check returns False for incompatible implementation.

        Given:
            A class that doesn't implement WorkerLike interface
        When:
            isinstance check is performed
        Then:
            It should return False
        """

        class IncompatibleWorker:
            """Class missing required WorkerLike methods."""

            def __init__(self):
                self.uid = uuid.uuid4()

        incompatible = IncompatibleWorker()
        assert not isinstance(incompatible, WorkerLike)


class TestWorkerFactory:
    """Test suite for WorkerFactory protocol."""

    def test_isinstance_check_for_compatible_factory(self):
        """Test isinstance check returns True for compatible factory.

        Given:
            A callable that implements WorkerFactory protocol
        When:
            isinstance check is performed
        Then:
            It should return True
        """

        def factory(*tags: str, **_) -> WorkerLike:
            return ConcreteWorker(*tags)

        assert isinstance(factory, WorkerFactory)

    def test_isinstance_check_for_incompatible_factory(self):
        """Test isinstance check for incompatible factory.

        Given:
            A callable that doesn't match WorkerFactory signature
        When:
            isinstance check is performed
        Then:
            It should return True (Protocol only checks for __call__)

        Note:
            Python's Protocol runtime checking is structural and only verifies
            the presence of __call__, not the exact signature. This is a
            known limitation of Protocol type checking.
        """

        def incompatible_factory() -> str:
            """Factory with wrong signature."""
            return "not a worker"

        # Protocol only checks for __call__ existence, not signature
        assert isinstance(incompatible_factory, WorkerFactory)

    def test_isinstance_check_for_non_callable(self):
        """Test isinstance check returns False for non-callable.

        Given:
            A non-callable object
        When:
            isinstance check is performed
        Then:
            It should return False
        """
        not_a_factory = "not callable"
        assert not isinstance(not_a_factory, WorkerFactory)
