import uuid
from types import MappingProxyType
from typing import Any

import grpc
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.runtime.discovery.base import WorkerMetadata
from wool.runtime.worker.base import Worker
from wool.runtime.worker.base import WorkerFactory
from wool.runtime.worker.base import WorkerLike
from wool.runtime.worker.base import WorkerOptions
from wool.runtime.worker.base import resolve_channel_credentials


class TestWorkerOptions:
    """Test suite for WorkerOptions dataclass."""

    def test___init___with_default_values(self):
        """Test WorkerOptions default constructor yields 100 MB limits.

        Given:
            No custom parameters
        When:
            WorkerOptions is instantiated
        Then:
            Both max_receive_message_length and max_send_message_length
            should be 100 MB
        """
        # Act
        opts = WorkerOptions()

        # Assert
        assert opts.max_receive_message_length == 100 * 1024 * 1024
        assert opts.max_send_message_length == 100 * 1024 * 1024

    def test___init___with_custom_values(self):
        """Test WorkerOptions with custom values for both attributes.

        Given:
            Custom max_receive_message_length and max_send_message_length
        When:
            WorkerOptions is instantiated with those values
        Then:
            Both attributes should reflect the custom values
        """
        # Act
        opts = WorkerOptions(
            max_receive_message_length=50 * 1024 * 1024,
            max_send_message_length=25 * 1024 * 1024,
        )

        # Assert
        assert opts.max_receive_message_length == 50 * 1024 * 1024
        assert opts.max_send_message_length == 25 * 1024 * 1024

    def test___init___with_partial_override(self):
        """Test WorkerOptions with only one attribute overridden.

        Given:
            Custom max_receive_message_length only
        When:
            WorkerOptions is instantiated with that value
        Then:
            max_receive_message_length should reflect the custom value
            and max_send_message_length should keep the default
        """
        # Act
        opts = WorkerOptions(max_receive_message_length=200 * 1024 * 1024)

        # Assert
        assert opts.max_receive_message_length == 200 * 1024 * 1024
        assert opts.max_send_message_length == 100 * 1024 * 1024


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


# Fixtures for credential resolver tests
@pytest.fixture
def channel_credentials():
    """Create grpc.ChannelCredentials for testing.

    Returns:
        grpc.ChannelCredentials configured for testing
    """
    # Create minimal SSL channel credentials
    return grpc.ssl_channel_credentials()


@pytest.mark.parametrize(
    "input_value,expected_result",
    [
        (None, None),
        pytest.param(
            "channel_credentials",
            "channel_credentials",
            id="direct_credentials",
        ),
    ],
)
def test_resolve_channel_credentials_direct_values(
    input_value,
    expected_result,
    channel_credentials,
):
    """Test resolve_channel_credentials with direct values.

    Given:
        None or ChannelCredentials instance
    When:
        resolve_channel_credentials is called
    Then:
        Returns the input unchanged
    """
    # Arrange
    if input_value == "channel_credentials":
        input_value = channel_credentials
        expected_result = channel_credentials

    # Act
    result = resolve_channel_credentials(input_value)

    # Assert
    assert result is expected_result


@pytest.mark.parametrize(
    "return_value,expected_result",
    [
        (None, None),
        pytest.param(
            "channel_credentials",
            "channel_credentials",
            id="valid_credentials",
        ),
    ],
)
def test_resolve_channel_credentials_callable_valid_returns(
    return_value,
    expected_result,
    channel_credentials,
):
    """Test resolve_channel_credentials with callable returning valid values.

    Given:
        Callable returning None or ChannelCredentials
    When:
        resolve_channel_credentials is called
    Then:
        Returns result from calling the callable
    """
    # Arrange
    if return_value == "channel_credentials":
        return_value = channel_credentials
        expected_result = channel_credentials
    callable_creds = lambda: return_value

    # Act
    result = resolve_channel_credentials(callable_creds)

    # Assert
    assert result is expected_result


@pytest.mark.parametrize(
    "invalid_value,error_pattern",
    [
        ("invalid", "Channel credentials callable"),
        (42, r"grpc\.ChannelCredentials.*got <class 'int'>"),
    ],
)
def test_resolve_channel_credentials_callable_invalid_returns(
    invalid_value,
    error_pattern,
):
    """Test resolve_channel_credentials with callable returning invalid types.

    Given:
        Callable returning non-ChannelCredentials type
    When:
        resolve_channel_credentials is called
    Then:
        Raises TypeError with appropriate message
    """
    # Arrange
    callable_creds = lambda: invalid_value

    # Act & assert
    with pytest.raises(TypeError, match=error_pattern):
        resolve_channel_credentials(callable_creds)


@given(
    input_type=st.sampled_from(["none", "direct", "callable_none", "callable_creds"]),
    call_count=st.integers(min_value=1, max_value=5),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_resolve_channel_credentials_idempotency_and_type_safety(
    input_type,
    call_count,
    channel_credentials,
):
    """Test resolve_channel_credentials idempotency and type safety.

    Given:
        Any valid input (None, credentials, or callable)
    When:
        resolve_channel_credentials is called multiple times
    Then:
        All results are identical (idempotent) and are
        ChannelCredentials or None (type safe).
    """
    # Arrange
    if input_type == "none":
        input_val = None
    elif input_type == "direct":
        input_val = channel_credentials
    elif input_type == "callable_none":
        input_val = lambda: None
    else:  # callable_creds
        input_val = lambda: channel_credentials

    # Act
    results = [resolve_channel_credentials(input_val) for _ in range(call_count)]

    # Assert
    assert all(r == results[0] for r in results), "Results must be identical"
    for result in results:
        assert result is None or isinstance(result, grpc.ChannelCredentials), (
            "Result must be ChannelCredentials or None"
        )
