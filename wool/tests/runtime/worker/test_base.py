import uuid
from types import MappingProxyType
from typing import Any

import grpc
import pytest
from hypothesis import given
from hypothesis import strategies as st

from wool.runtime.worker.base import ChannelOptions
from wool.runtime.worker.base import Worker
from wool.runtime.worker.base import WorkerFactory
from wool.runtime.worker.base import WorkerLike
from wool.runtime.worker.base import WorkerOptions
from wool.runtime.worker.metadata import WorkerMetadata


class TestChannelOptions:
    """Test suite for ChannelOptions dataclass."""

    def test___init___with_default_values(self):
        """Test ChannelOptions default constructor yields 100 MB limits.

        Given:
            No custom parameters
        When:
            ChannelOptions is instantiated
        Then:
            All fields should have their default values
        """
        # Act
        opts = ChannelOptions()

        # Assert
        assert opts.max_receive_message_length == 100 * 1024 * 1024
        assert opts.max_send_message_length == 100 * 1024 * 1024
        assert opts.keepalive_time_ms == 30_000
        assert opts.keepalive_timeout_ms == 30_000
        assert opts.keepalive_permit_without_calls is True
        assert opts.max_pings_without_data == 2
        assert opts.max_concurrent_streams == 100
        assert opts.compression is grpc.Compression.NoCompression

    def test___init___with_custom_transport_options(self):
        """Test ChannelOptions with custom transport options.

        Given:
            Custom max_pings_without_data, max_concurrent_streams,
            and compression values
        When:
            ChannelOptions is instantiated with those values
        Then:
            All three fields should reflect the provided values
        """
        # Act
        opts = ChannelOptions(
            max_pings_without_data=5,
            max_concurrent_streams=50,
            compression=grpc.Compression.Gzip,
        )

        # Assert
        assert opts.max_pings_without_data == 5
        assert opts.max_concurrent_streams == 50
        assert opts.compression is grpc.Compression.Gzip

    def test___init___with_custom_message_sizes(self):
        """Test ChannelOptions with custom message size values.

        Given:
            Custom max_receive_message_length and max_send_message_length
        When:
            ChannelOptions is instantiated with those values
        Then:
            Both attributes should reflect the custom values
        """
        # Act
        opts = ChannelOptions(
            max_receive_message_length=50 * 1024 * 1024,
            max_send_message_length=25 * 1024 * 1024,
        )

        # Assert
        assert opts.max_receive_message_length == 50 * 1024 * 1024
        assert opts.max_send_message_length == 25 * 1024 * 1024

    def test___init___with_partial_override(self):
        """Test ChannelOptions with only one attribute overridden.

        Given:
            Custom max_receive_message_length only
        When:
            ChannelOptions is instantiated with that value
        Then:
            max_receive_message_length should reflect the custom value
            and max_send_message_length should keep the default
        """
        # Act
        opts = ChannelOptions(max_receive_message_length=200 * 1024 * 1024)

        # Assert
        assert opts.max_receive_message_length == 200 * 1024 * 1024
        assert opts.max_send_message_length == 100 * 1024 * 1024

    def test___init___with_keepalive_time_and_timeout(self):
        """Test ChannelOptions with custom keepalive time and timeout.

        Given:
            Custom keepalive_time_ms and keepalive_timeout_ms values
        When:
            ChannelOptions is instantiated with those values
        Then:
            Both fields should reflect the custom values
        """
        # Act
        opts = ChannelOptions(
            keepalive_time_ms=30000,
            keepalive_timeout_ms=10000,
        )

        # Assert
        assert opts.keepalive_time_ms == 30000
        assert opts.keepalive_timeout_ms == 10000

    def test___init___with_keepalive_permit_without_calls_disabled(self):
        """Test ChannelOptions with keepalive_permit_without_calls disabled.

        Given:
            keepalive_permit_without_calls set to False
        When:
            ChannelOptions is instantiated
        Then:
            It should store False for keepalive_permit_without_calls
        """
        # Act
        opts = ChannelOptions(keepalive_permit_without_calls=False)

        # Assert
        assert opts.keepalive_permit_without_calls is False

    def test___init___with_all_keepalive_options(self):
        """Test ChannelOptions with all keepalive fields set.

        Given:
            All three keepalive parameters specified
        When:
            ChannelOptions is instantiated
        Then:
            All fields should reflect the provided values
        """
        # Act
        opts = ChannelOptions(
            keepalive_time_ms=30000,
            keepalive_timeout_ms=10000,
            keepalive_permit_without_calls=True,
        )

        # Assert
        assert opts.keepalive_time_ms == 30000
        assert opts.keepalive_timeout_ms == 10000
        assert opts.keepalive_permit_without_calls is True


class TestWorkerOptions:
    """Test suite for WorkerOptions dataclass."""

    def test___init___with_default_values(self):
        """Test WorkerOptions default constructor yields default ChannelOptions.

        Given:
            No custom parameters
        When:
            WorkerOptions is instantiated
        Then:
            It should contain default ChannelOptions and default
            http2_min_recv_ping_interval_without_data_ms
        """
        # Act
        opts = WorkerOptions()

        # Assert
        assert opts.channel == ChannelOptions()
        assert opts.http2_min_recv_ping_interval_without_data_ms == 30_000
        assert opts.max_ping_strikes == 2
        assert opts.max_connection_idle_ms is None
        assert opts.max_connection_age_ms is None
        assert opts.max_connection_age_grace_ms is None

    def test___init___with_custom_server_only_options(self):
        """Test WorkerOptions with custom server-only fields.

        Given:
            Custom max_ping_strikes and all three lifecycle options
        When:
            WorkerOptions is instantiated with those values
        Then:
            All four server-only fields should reflect the provided values
        """
        # Act
        opts = WorkerOptions(
            max_ping_strikes=5,
            max_connection_idle_ms=60000,
            max_connection_age_ms=120000,
            max_connection_age_grace_ms=5000,
        )

        # Assert
        assert opts.max_ping_strikes == 5
        assert opts.max_connection_idle_ms == 60000
        assert opts.max_connection_age_ms == 120000
        assert opts.max_connection_age_grace_ms == 5000

    def test___init___with_custom_channel_options(self):
        """Test WorkerOptions with custom ChannelOptions.

        Given:
            A ChannelOptions instance with custom message sizes
        When:
            WorkerOptions is instantiated with that ChannelOptions
        Then:
            The channel field should reflect the custom values
        """
        # Act
        channel = ChannelOptions(
            max_receive_message_length=50 * 1024 * 1024,
            max_send_message_length=25 * 1024 * 1024,
        )
        opts = WorkerOptions(channel=channel)

        # Assert
        assert opts.channel.max_receive_message_length == 50 * 1024 * 1024
        assert opts.channel.max_send_message_length == 25 * 1024 * 1024

    def test___init___with_custom_http2_min_ping_interval(self):
        """Test WorkerOptions with custom http2_min_recv_ping_interval_without_data_ms.

        Given:
            A custom http2_min_recv_ping_interval_without_data_ms value
        When:
            WorkerOptions is instantiated
        Then:
            It should store the custom value
        """
        # Act
        opts = WorkerOptions(http2_min_recv_ping_interval_without_data_ms=20000)

        # Assert
        assert opts.http2_min_recv_ping_interval_without_data_ms == 20000

    def test___init___with_keepalive_time_below_min_ping_interval(self):
        """Test WorkerOptions raises ValueError when channel.keepalive_time_ms < http2_min_recv_ping_interval_without_data_ms.

        Given:
            A ChannelOptions with keepalive_time_ms=5000 and http2_min_recv_ping_interval_without_data_ms=10000
        When:
            WorkerOptions is instantiated
        Then:
            It should raise ValueError
        """
        # Act & assert
        with pytest.raises(
            ValueError,
            match="keepalive_time_ms must be >= http2_min_recv_ping_interval_without_data_ms",
        ):
            WorkerOptions(
                channel=ChannelOptions(keepalive_time_ms=5000),
                http2_min_recv_ping_interval_without_data_ms=10000,
            )

    def test___init___with_keepalive_time_equal_to_min_ping_interval(self):
        """Test WorkerOptions accepts channel.keepalive_time_ms equal to http2_min_recv_ping_interval_without_data_ms.

        Given:
            A ChannelOptions with keepalive_time_ms=10000 and http2_min_recv_ping_interval_without_data_ms=10000
        When:
            WorkerOptions is instantiated
        Then:
            It should succeed without error
        """
        # Act
        opts = WorkerOptions(
            channel=ChannelOptions(keepalive_time_ms=10000),
            http2_min_recv_ping_interval_without_data_ms=10000,
        )

        # Assert
        assert opts.channel.keepalive_time_ms == 10000
        assert opts.http2_min_recv_ping_interval_without_data_ms == 10000

    def test___init___with_keepalive_time_above_min_ping_interval(self):
        """Test WorkerOptions accepts channel.keepalive_time_ms above http2_min_recv_ping_interval_without_data_ms.

        Given:
            A ChannelOptions with keepalive_time_ms=20000 and http2_min_recv_ping_interval_without_data_ms=10000
        When:
            WorkerOptions is instantiated
        Then:
            It should succeed without error
        """
        # Act
        opts = WorkerOptions(
            channel=ChannelOptions(keepalive_time_ms=20000),
            http2_min_recv_ping_interval_without_data_ms=10000,
        )

        # Assert
        assert opts.channel.keepalive_time_ms == 20000
        assert opts.http2_min_recv_ping_interval_without_data_ms == 10000

    @given(
        keepalive_time_ms=st.integers(min_value=1, max_value=300000),
        min_ping_interval=st.integers(min_value=1, max_value=300000),
    )
    def test___init___validation_property(self, keepalive_time_ms, min_ping_interval):
        """Test WorkerOptions validation invariant across arbitrary values.

        Given:
            Arbitrary positive keepalive_time_ms and http2_min_recv_ping_interval_without_data_ms
        When:
            WorkerOptions is instantiated
        Then:
            It should raise ValueError iff keepalive_time_ms < http2_min_recv_ping_interval_without_data_ms
        """
        # Act & assert
        if keepalive_time_ms < min_ping_interval:
            with pytest.raises(ValueError):
                WorkerOptions(
                    channel=ChannelOptions(keepalive_time_ms=keepalive_time_ms),
                    http2_min_recv_ping_interval_without_data_ms=min_ping_interval,
                )
        else:
            opts = WorkerOptions(
                channel=ChannelOptions(keepalive_time_ms=keepalive_time_ms),
                http2_min_recv_ping_interval_without_data_ms=min_ping_interval,
            )
            assert opts.channel.keepalive_time_ms == keepalive_time_ms
            assert opts.http2_min_recv_ping_interval_without_data_ms == min_ping_interval


class ConcreteWorker(Worker):
    """Concrete implementation of Worker for testing."""

    def __init__(self, *tags: str, **extra: Any):
        super().__init__(*tags, **extra)
        self._address = None

    @property
    def address(self) -> str | None:
        return self._address

    async def _start(self, timeout: float | None):
        """Mock start implementation."""
        self._address = "localhost:50051"
        self._info = WorkerMetadata(
            uid=self._uid,
            address="localhost:50051",
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

    def test___init___assigns_unique_uid(self):
        """Test Worker initialization assigns a unique UUID.

        Given:
            No pre-existing worker instances
        When:
            A new Worker is instantiated
        Then:
            It should have a unique UUID assigned
        """
        # Act
        worker = ConcreteWorker()

        # Assert
        assert isinstance(worker.uid, uuid.UUID)

    def test___init___stores_tags_as_set(self, worker_tags):
        """Test Worker initialization stores tags as a set.

        Given:
            Worker capability tags
        When:
            A Worker is instantiated with tags
        Then:
            Tags should be stored as a set
        """
        # Act
        worker = ConcreteWorker(*worker_tags)

        # Assert
        assert worker.tags == set(worker_tags)

    def test___init___stores_extra_metadata(self, worker_extra):
        """Test Worker initialization stores extra metadata.

        Given:
            Extra metadata key-value pairs
        When:
            A Worker is instantiated with extra metadata
        Then:
            Metadata should be stored and accessible
        """
        # Act
        worker = ConcreteWorker(**worker_extra)

        # Assert
        assert worker.extra == worker_extra

    def test_metadata_returns_none_before_start(self):
        """Test Worker info property returns None before starting.

        Given:
            A Worker instance that has not been started
        When:
            The info property is accessed
        Then:
            It should return None
        """
        # Act
        worker = ConcreteWorker()

        # Assert
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
        # Arrange
        worker = ConcreteWorker()

        async def mock_start_impl(timeout):
            worker._info = metadata

        mock_start = mocker.patch.object(
            worker,
            "_start",
            side_effect=mock_start_impl,
            new_callable=mocker.AsyncMock,
        )

        # Act
        await worker.start(timeout=60.0)

        # Assert
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
        # Arrange
        worker = ConcreteWorker()

        # Act
        await worker.start()

        # Assert
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
        # Arrange
        worker = ConcreteWorker()
        await worker.start()

        # Act & assert
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
        # Arrange
        worker = ConcreteWorker()

        # Act & assert
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
        # Arrange
        worker = ConcreteWorker()

        # Act
        await worker.start(timeout=timeout)

        # Assert
        assert worker.metadata is not None

    @given(count=st.integers(min_value=2, max_value=100))
    def test___init___multiple_instances_have_unique_uids(self, count):
        """Test multiple Workers get unique UIDs.

        Given:
            Creating multiple worker instances
        When:
            Workers are instantiated
        Then:
            All should have unique UIDs
        """
        # Act
        workers = [ConcreteWorker() for _ in range(count)]
        uids = [w.uid for w in workers]

        # Assert
        assert len(uids) == len(set(uids))  # All unique

    @given(tags=st.lists(st.text(min_size=1, max_size=20), min_size=1, max_size=10))
    def test___init___deduplicates_tags(self, tags):
        """Test Worker initialization deduplicates tags.

        Given:
            A list of tags potentially containing duplicates
        When:
            A Worker is instantiated with these tags
        Then:
            Tags should be stored as a unique set
        """
        # Act
        worker = ConcreteWorker(*tags)

        # Assert
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
    def test___init___preserves_extra_metadata(self, extra):
        """Test Worker preserves arbitrary extra metadata.

        Given:
            Random dictionary of extra metadata
        When:
            Worker is instantiated with this metadata
        Then:
            Metadata should be stored exactly as provided
        """
        # Act
        worker = ConcreteWorker(**extra)

        # Assert
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
        # Arrange
        worker = ConcreteWorker()
        await worker.start()
        mock_stop = mocker.patch.object(
            worker,
            "_stop",
            new_callable=mocker.AsyncMock,
        )

        # Act
        await worker.stop(timeout=60.0)

        # Assert
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
        # Arrange
        worker = ConcreteWorker()
        await worker.start()

        # Act
        await worker.stop()
        await worker.start()

        # Assert
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
        # Arrange
        worker = ConcreteWorker()

        # Act & assert
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
        # Arrange
        worker = ConcreteWorker()
        await worker.start()
        mocker.patch.object(
            worker,
            "_stop",
            side_effect=Exception("Stop failed"),
            new_callable=mocker.AsyncMock,
        )

        # Act & assert
        with pytest.raises(Exception, match="Stop failed"):
            await worker.stop()

        # Arrange (restart)
        mocker.patch.object(
            worker,
            "_stop",
            new_callable=mocker.AsyncMock,
        )

        # Act
        await worker.start()

        # Assert
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
        # Act
        worker = ConcreteWorker()

        # Assert
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

        # Arrange
        class IncompatibleWorker:
            """Class missing required WorkerLike methods."""

            def __init__(self):
                self.uid = uuid.uuid4()

        # Act
        incompatible = IncompatibleWorker()

        # Assert
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

        # Arrange
        def factory(*tags: str, **_) -> WorkerLike:
            return ConcreteWorker(*tags)

        # Act & assert
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

        # Arrange
        def incompatible_factory() -> str:
            """Factory with wrong signature."""
            return "not a worker"

        # Act & assert
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
        # Act
        not_a_factory = "not callable"

        # Assert
        assert not isinstance(not_a_factory, WorkerFactory)
