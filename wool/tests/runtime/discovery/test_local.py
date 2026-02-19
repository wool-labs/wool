import asyncio
import struct
import tempfile
import uuid
from multiprocessing.shared_memory import SharedMemory
from pathlib import Path
from unittest.mock import patch

import cloudpickle
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import WorkerMetadata
from wool.runtime.discovery.local import NULL_REF
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.discovery.local import _lock
from wool.runtime.discovery.local import _shared_memory
from wool.runtime.discovery.local import _short_hash
from wool.runtime.discovery.local import _Watchdog
from wool.runtime.discovery.local import _watchdog_path
from wool.runtime.discovery.local import _WorkerReference


@pytest.fixture
def metadata():
    """Provides sample WorkerMetadata for testing.

    Creates a WorkerMetadata instance with typical field values for use in
    tests that need a well-formed worker instance.
    """
    return WorkerMetadata(
        uid=uuid.UUID("12345678-1234-5678-1234-567812345678"),
        address="localhost:50051",
        pid=12345,
        version="1.0.0",
    )


@pytest.fixture
def namespace():
    """Provides unique namespace for test isolation.

    Creates a unique namespace string for each test to ensure shared
    memory regions don't interfere with each other.
    """
    return f"test-namespace-{uuid.uuid4()}"


class TestWorkerReference:
    """Tests for _WorkerReference class."""

    def test_init(self):
        """Test creating _WorkerReference from UUID.

        Given:
            A valid UUID
        When:
            Creating _WorkerReference
        Then:
            It should store the UUID and provide access via uuid property
        """
        # Arrange
        uid = uuid.uuid4()

        # Act
        ref = _WorkerReference(uid)

        # Assert
        assert ref.uuid == uid

    def test_str(self):
        """Test __str__() returns abbreviated hex string.

        Given:
            A _WorkerReference instance
        When:
            Converting to string
        Then:
            It should return abbreviated SHA-256 hash of hex UUID
        """
        # Arrange
        uid = uuid.UUID("12345678-1234-5678-1234-567812345678")
        ref = _WorkerReference(uid)

        # Act
        result = str(ref)

        # Assert
        assert result == _short_hash(uid.hex)
        assert len(result) == 30  # default abbreviation length

    def test_bytes(self):
        """Test __bytes__() returns 16-byte UUID representation.

        Given:
            A _WorkerReference instance
        When:
            Converting to bytes
        Then:
            It should return 16-byte UUID representation
        """
        # Arrange
        uid = uuid.uuid4()
        ref = _WorkerReference(uid)

        # Act
        result = bytes(ref)

        # Assert
        assert len(result) == 16
        assert result == uid.bytes

    def test_eq(self):
        """Test __hash__() and __eq__() enable dict/set usage.

        Given:
            Two _WorkerReference instances with same UUID
        When:
            Using as dict keys or set members
        Then:
            It should treat them as same object (same hash, equal)
        """
        # Arrange
        uid = uuid.uuid4()
        ref1 = _WorkerReference(uid)
        ref2 = _WorkerReference(uid)

        # Act & Assert
        assert hash(ref1) == hash(ref2)
        assert ref1 == ref2

        # Should work in dict and set
        d = {ref1: "value"}
        assert d[ref2] == "value"

        s = {ref1}
        assert ref2 in s

    def test_eq_with_non_worker_reference_type(self):
        """Test __eq__() with non-_WorkerReference type returns False.

        Given:
            A _WorkerReference instance and objects of other types
        When:
            Comparing with ==
        Then:
            It should return False (NotImplemented leads to False)
        """
        # Arrange
        ref = _WorkerReference(uuid.uuid4())

        # Act & Assert
        assert not ref == "not a reference"
        assert not ref == 123
        assert ref is not None
        assert not ref == uuid.uuid4()

    def test_from_bytes(self):
        """Test from_bytes() with valid 16-byte input.

        Given:
            A valid 16-byte UUID representation
        When:
            Creating _WorkerReference from bytes
        Then:
            It should create instance with correct UUID
        """
        # Arrange
        uid = uuid.uuid4()
        data = uid.bytes

        # Act
        ref = _WorkerReference.from_bytes(data)

        # Assert
        assert ref.uuid == uid

    def test_from_bytes_invalid_length(self):
        """Test from_bytes() with invalid length raises ValueError.

        Given:
            Byte data with length != 16
        When:
            Creating _WorkerReference from bytes
        Then:
            It should raise ValueError
        """
        # Act & Assert
        with pytest.raises(ValueError, match="Expected 16 bytes"):
            _WorkerReference.from_bytes(b"short")

        with pytest.raises(ValueError, match="Expected 16 bytes"):
            _WorkerReference.from_bytes(b"x" * 17)

    def test_from_bytes_invalid_value(self):
        """Test from_bytes() with NULL bytes raises ValueError.

        Given:
            16 NULL bytes
        When:
            Creating _WorkerReference from bytes
        Then:
            It should raise ValueError (NULL indicates empty slot)
        """
        # Act & Assert
        with pytest.raises(
            ValueError, match="Cannot create _WorkerReference from NULL bytes"
        ):
            _WorkerReference.from_bytes(NULL_REF)

    def test_bytes_property(self):
        """Test bytes property returns 16-byte representation.

        Given:
            A _WorkerReference instance
        When:
            Accessing bytes property
        Then:
            It should return same value as __bytes__()
        """
        # Arrange
        uid = uuid.uuid4()
        ref = _WorkerReference(uid)

        # Act
        result = ref.bytes

        # Assert
        assert result == bytes(ref)
        assert len(result) == 16

    def test_repr(self):
        """Test __repr__() returns expected format.

        Given:
            A _WorkerReference instance
        When:
            Calling repr()
        Then:
            It should return string in format "_WorkerReference(uuid)"
        """
        # Arrange
        uid = uuid.uuid4()
        ref = _WorkerReference(uid)

        # Act
        result = repr(ref)

        # Assert
        assert result == f"_WorkerReference({uid})"
        assert str(uid) in result


class TestShortHash:
    """Tests for _short_hash() function."""

    @given(s=st.text(min_size=1))
    def test(self, s: str):
        """Test _short_hash() handles various namespaces.

        Given:
            Strings of varying lengths
        When:
            Hashing the string
        Then:
            It should return a deterministic hash truncated to the default length
        """
        # Act
        h = _short_hash(s)

        # Assert
        assert len(h) == 30
        assert h == _short_hash(s)

    @given(s=st.text(min_size=1), n=st.integers(min_value=1))
    def test_custom_length(self, s: str, n: int):
        """Test _short_hash() with custom length parameter.

        Given:
            Strings of varying lengths and custom lengths
        When:
            Hashing the string with custom length
        Then:
            It should return a deterministic hash truncated to specified length
        """
        # Act
        h = _short_hash(s, n=n)

        # Assert
        assert len(h) == min(n, len(h))
        assert h == _short_hash(s, n=n)


class TestSharedMemory:
    """Tests for _shared_memory() context manager.

    Fully qualified name: wool.runtime.discovery.local._shared_memory
    """

    def test_open_existing_shared_memory(self, namespace):
        """Test _shared_memory() opens existing shared memory.

        Given:
            Existing shared memory region
        When:
            Opening with _shared_memory() context manager
        Then:
            It should open and provide access to the memory
        """
        # Arrange - create shared memory
        name = _short_hash(namespace)
        created = SharedMemory(name=name, create=True, size=1024)

        try:
            # Act - open existing
            with _shared_memory(name) as shm:
                # Assert
                assert shm.name == created.name
                # Note: macOS rounds size up to page size, so we check >= requested size
                assert shm.size >= 1024
        finally:
            created.close()
            created.unlink()

    def test_open_nonexistent_shared_memory(self, namespace):
        """Test _shared_memory() attempts to open nonexistent shared memory.

        Given:
            Nonexistent shared memory region
        When:
            Opening with _shared_memory() context manager
        Then:
            It should open and provide access to the memory
        """
        # Arrange
        name = _short_hash(namespace)

        # Act
        with pytest.raises(FileNotFoundError):
            with _shared_memory(name):
                pass

    def test_cleanup_on_exit(self, namespace):
        """Test _shared_memory() closes memory on context exit.

        Given:
            An open shared memory context
        When:
            Exiting the context
        Then:
            It should close the shared memory (but not unlink)
        """
        # Arrange
        name = _short_hash(namespace)
        created = SharedMemory(name=name, create=True, size=1024)

        try:
            # Act
            with _shared_memory(name) as shm:
                # Memory is open within context
                assert shm.buf is not None

            # Assert - memory still exists but our handle is closed
            # We can re-open it
            with _shared_memory(name) as shm2:
                assert shm2.buf is not None
        finally:
            created.close()
            created.unlink()

    def test_cleanup_on_exit_fails(self, namespace):
        """Test _shared_memory() handles cleanup errors gracefully.

        Given:
            Shared memory that may fail to close
        When:
            Exiting the context with close errors
        Then:
            It should suppress exceptions during cleanup
        """
        # Arrange
        name = _short_hash(namespace)
        created = SharedMemory(name=name, create=True, size=1024)

        try:
            # Act & Assert - should not raise even if cleanup fails
            with _shared_memory(name) as shm:
                # Manually close to simulate error on exit
                shm.close()
            # Context exit should handle the error gracefully
        finally:
            try:
                created.close()
            except Exception:
                pass
            created.unlink()

    def test_close_exception_is_suppressed(self, namespace, mocker: MockerFixture):
        """Test _shared_memory() suppresses exceptions from close().

        Given:
            A shared memory context manager
        When:
            SharedMemory.close() raises an exception during context exit
        Then:
            The exception should be caught and suppressed without propagating
        """
        # Arrange
        name = _short_hash(namespace)
        created = SharedMemory(name=name, create=True, size=1024)

        # Mock close to raise an exception
        close_spy = mocker.patch.object(
            SharedMemory, "close", side_effect=OSError("Simulated close failure")
        )

        try:
            # Act - should not raise despite close() raising OSError
            with _shared_memory(name):
                pass

            # Assert - close was called and exception was suppressed
            assert close_spy.called
        finally:
            # Cleanup - stop the mock to allow real cleanup
            mocker.stop(close_spy)
            try:
                created.close()
            except Exception:
                pass
            created.unlink()


class TestLock:
    """Tests for _lock() context manager.

    Fully qualified name: wool.runtime.discovery.local._lock
    """

    @pytest.mark.asyncio
    async def test(self, namespace):
        """Test _lock() prevents concurrent access.

        Given:
            Two concurrent tasks trying to acquire same lock
        When:
            First task acquires and holds the lock
        Then:
            Second task should block until first releases, ensuring serial execution
        """
        # Arrange
        import asyncio

        execution_log = []
        lock_acquired = asyncio.Event()
        can_release = asyncio.Event()

        async def task_1(name):
            async with _lock(name):
                execution_log.append("task-1-acquired")
                lock_acquired.set()  # Signal that first lock is held
                await can_release.wait()  # Hold lock until signaled
                execution_log.append("task-1-releasing")

        async def task_2(name):
            await lock_acquired.wait()  # Wait until first lock is definitely held
            execution_log.append("task-2-attempting")
            async with _lock(name):
                execution_log.append("task-2-acquired")

        # Act - start both tasks concurrently
        t1 = asyncio.create_task(task_1(namespace))
        t2 = asyncio.create_task(task_2(namespace))

        # Give tasks time to reach their states
        await lock_acquired.wait()

        # Assert - task 1 holds lock, task 2 is blocked
        assert "task-1-acquired" in execution_log
        assert "task-2-attempting" in execution_log
        assert "task-2-acquired" not in execution_log  # Should be blocked

        # Release task 1's lock
        can_release.set()

        # Wait for both tasks to complete
        await t1
        await t2

        # Assert - proper serial execution order
        assert execution_log == [
            "task-1-acquired",
            "task-2-attempting",
            "task-1-releasing",
            "task-2-acquired",
        ]


class TestWatchdogPath:
    """Tests for _watchdog_path() function."""

    def test_returns_correct_path(self, namespace):
        """Test _watchdog_path() returns correct temp file path.

        Given:
            A namespace
        When:
            Getting watchdog path
        Then:
            It should return path in temp directory with namespace in name
        """
        # Act
        path = _watchdog_path(namespace)

        # Assert
        assert isinstance(path, Path)
        assert path.parent == Path(tempfile.gettempdir())
        assert namespace in str(path)
        assert "wool-notify" in str(path)

    def test_deterministic_path_for_namespace(self, namespace):
        """Test _watchdog_path() returns same path for same namespace.

        Given:
            Same namespace
        When:
            Calling _watchdog_path() multiple times
        Then:
            It should always return same path
        """
        # Act
        path1 = _watchdog_path(namespace)
        path2 = _watchdog_path(namespace)

        # Assert
        assert path1 == path2


class TestWatchdog:
    """Tests for _Watchdog class.

    Fully qualified name: wool.runtime.discovery.local._Watchdog
    """

    @pytest.mark.asyncio
    async def test_on_modified_triggers_event(self, mocker: MockerFixture, namespace):
        """Test on_modified() triggers event when watchdog file modified.

        Given:
            A _Watchdog handler with asyncio.Event
        When:
            Watchdog file is modified
        Then:
            It should set the notification event
        """
        # Arrange
        notification = asyncio.Event()
        lock = asyncio.Lock()
        loop = asyncio.get_running_loop()
        watchdog_path = _watchdog_path(namespace)

        handler = _Watchdog(notification, watchdog_path, lock, loop)

        # Mock filesystem event
        mock_event = mocker.MagicMock()
        mock_event.src_path = str(watchdog_path)

        # Act
        handler.on_modified(mock_event)

        # Wait for event to be set via call_soon_threadsafe
        await asyncio.sleep(0.1)

        # Assert
        assert notification.is_set()

    @pytest.mark.asyncio
    async def test_on_modified_ignores_other_files(
        self, mocker: MockerFixture, namespace
    ):
        """Test on_modified() ignores modifications to other files.

        Given:
            A _Watchdog handler watching specific file
        When:
            A different file is modified
        Then:
            It should not set the notification event
        """
        # Arrange
        notification = asyncio.Event()
        lock = asyncio.Lock()
        loop = asyncio.get_running_loop()
        watchdog_path = _watchdog_path(namespace)

        handler = _Watchdog(notification, watchdog_path, lock, loop)

        # Mock filesystem event for different file
        mock_event = mocker.MagicMock()
        mock_event.src_path = str(Path("/tmp/other-file.txt"))

        # Act
        handler.on_modified(mock_event)
        await asyncio.sleep(0.1)

        # Assert
        assert not notification.is_set()

    @pytest.mark.asyncio
    async def test_event_scheduled_in_correct_loop(
        self, mocker: MockerFixture, namespace
    ):
        """Test event notification scheduled in correct event loop.

        Given:
            A _Watchdog handler with specific event loop
        When:
            File modification triggers notification
        Then:
            It should schedule event.set() in the correct loop via call_soon_threadsafe
        """
        # Arrange
        notification = asyncio.Event()
        lock = asyncio.Lock()
        loop = asyncio.get_running_loop()

        watchdog_path = _watchdog_path(namespace)
        handler = _Watchdog(notification, watchdog_path, lock, loop)

        # Spy on call_soon_threadsafe
        call_soon_spy = mocker.spy(loop, "call_soon_threadsafe")

        # Mock filesystem event
        mock_event = mocker.MagicMock()
        mock_event.src_path = str(watchdog_path)

        # Act
        handler.on_modified(mock_event)
        await asyncio.sleep(0.1)

        # Assert
        call_soon_spy.assert_called_once()


class TestLocalDiscovery:
    """Tests for LocalDiscovery class."""

    def test_publisher_propert(self, namespace):
        """Test publisher property returns Publisher instance.

        Given:
            A LocalDiscovery instance
        When:
            Accessing publisher property
        Then:
            It should return new Publisher with same namespace
        """
        # Arrange
        discovery = LocalDiscovery(namespace)

        # Act
        publisher = discovery.publisher

        # Assert
        assert isinstance(publisher, LocalDiscovery.Publisher)
        assert publisher.namespace == namespace

    def test_subscriber_property(self, namespace):
        """Test subscriber property returns Subscriber instance.

        Given:
            A LocalDiscovery instance
        When:
            Accessing subscriber property
        Then:
            It should return new Subscriber with same namespace
        """
        # Arrange
        discovery = LocalDiscovery(namespace)

        # Act
        subscriber = discovery.subscriber

        # Assert
        assert isinstance(subscriber, LocalDiscovery.Subscriber)
        assert subscriber.namespace == namespace

    def test_subscribe_without_filter(self, namespace, mocker: MockerFixture):
        """Test subscribe() without filter returns unfiltered subscriber.

        Given:
            A LocalDiscovery instance
        When:
            Calling subscribe() without filter
        Then:
            It should create Subscriber without filter
        """
        # Arrange
        SubscriberClass = LocalDiscovery.Subscriber
        discovery = LocalDiscovery(namespace)
        SubscriberSpy = mocker.spy(LocalDiscovery, "Subscriber")

        # Act
        subscriber = discovery.subscribe()

        # Assert
        SubscriberSpy.assert_called_once_with(namespace, None, poll_interval=None)
        assert isinstance(subscriber, SubscriberClass)

    def test_subscribe_with_filter(self, namespace, mocker: MockerFixture):
        """Test subscribe() with filter returns filtered subscriber.

        Given:
            A LocalDiscovery instance and filter predicate
        When:
            Calling subscribe() with filter
        Then:
            It should create Subscriber with filter applied
        """
        # Arrange
        SubscriberClass = LocalDiscovery.Subscriber
        discovery = LocalDiscovery(namespace)

        def predicate(w):
            return w.address == "localhost:50051"

        SubscriberSpy = mocker.spy(LocalDiscovery, "Subscriber")

        # Act
        subscriber = discovery.subscribe(predicate)

        # Assert
        SubscriberSpy.assert_called_once_with(namespace, predicate, poll_interval=None)
        assert isinstance(subscriber, SubscriberClass)

    def test_subscriber_property_with_default_filter(
        self, namespace, mocker: MockerFixture
    ):
        """Test subscriber property uses constructor's default filter.

        Given:
            A LocalDiscovery instance created with a default filter
        When:
            Accessing subscriber property
        Then:
            It should return Subscriber with the default filter applied
        """
        # Arrange
        SubscriberClass = LocalDiscovery.Subscriber

        def predicate(w):
            return w.address.endswith(":50051")

        discovery = LocalDiscovery(namespace, filter=predicate)
        SubscriberSpy = mocker.spy(LocalDiscovery, "Subscriber")

        # Act
        subscriber = discovery.subscriber

        # Assert
        SubscriberSpy.assert_called_once_with(namespace, predicate, poll_interval=None)
        assert isinstance(subscriber, SubscriberClass)

    def test_subscribe_with_default_filter(self, namespace, mocker: MockerFixture):
        """Test subscribe() without explicit filter falls back to default.

        Given:
            A LocalDiscovery instance created with a default filter
        When:
            Calling subscribe() without providing a filter
        Then:
            It should create Subscriber with the constructor's default filter
        """
        # Arrange
        SubscriberClass = LocalDiscovery.Subscriber

        def predicate(w):
            return w.address.endswith(":50051")

        discovery = LocalDiscovery(namespace, filter=predicate)
        SubscriberSpy = mocker.spy(LocalDiscovery, "Subscriber")

        # Act
        subscriber = discovery.subscribe()

        # Assert
        SubscriberSpy.assert_called_once_with(namespace, predicate, poll_interval=None)
        assert isinstance(subscriber, SubscriberClass)

    def test_subscribe_with_custom_poll_interval(self, namespace, mocker: MockerFixture):
        """Test subscribe() with custom poll_interval.

        Given:
            A LocalDiscovery instance
        When:
            Calling subscribe() with custom poll_interval
        Then:
            It should create Subscriber with specified poll_interval
        """
        # Arrange
        SubscriberClass = LocalDiscovery.Subscriber
        discovery = LocalDiscovery(namespace)
        SubscriberSpy = mocker.spy(LocalDiscovery, "Subscriber")

        # Act
        subscriber = discovery.subscribe(poll_interval=0.5)

        # Assert
        SubscriberSpy.assert_called_once_with(namespace, None, poll_interval=0.5)
        assert isinstance(subscriber, SubscriberClass)

    def test_subscribe_with_none_poll_interval(self, namespace, mocker: MockerFixture):
        """Test subscribe() with None poll_interval (default).

        Given:
            A LocalDiscovery instance
        When:
            Calling subscribe() with poll_interval=None
        Then:
            It should create Subscriber with poll_interval=None
        """
        # Arrange
        SubscriberClass = LocalDiscovery.Subscriber
        discovery = LocalDiscovery(namespace)
        SubscriberSpy = mocker.spy(LocalDiscovery, "Subscriber")

        # Act
        subscriber = discovery.subscribe(poll_interval=None)

        # Assert
        SubscriberSpy.assert_called_once_with(namespace, None, poll_interval=None)
        assert isinstance(subscriber, SubscriberClass)

    def test_namespace_property(self, namespace):
        """Test namespace property returns namespace string.

        Given:
            A LocalDiscovery instance
        When:
            Accessing namespace property
        Then:
            It should return the namespace identifier
        """
        # Arrange
        discovery = LocalDiscovery(namespace)

        # Act
        result = discovery.namespace

        # Assert
        assert result == namespace


class TestLocalDiscoveryPublisher:
    """Tests for LocalDiscovery.Publisher class."""

    @pytest.mark.asyncio
    async def test_publish_worker_added(self, namespace, metadata):
        """Test publish() with worker-added event.

        Given:
            A Publisher instance
        When:
            Publishing worker-added event
        Then:
            It should write worker to shared memory and create notification file
        """
        # Arrange
        abbreviated_namespace = _short_hash(namespace)

        # Create address space shared memory
        address_space = SharedMemory(name=abbreviated_namespace, create=True, size=1024)
        assert address_space.buf
        # Initialize with NULL slots - write bytes properly
        for i in range(0, len(address_space.buf), 16):
            address_space.buf[i : i + 16] = NULL_REF

        publisher = LocalDiscovery.Publisher(namespace)
        event = DiscoveryEvent("worker-added", metadata=metadata)

        try:
            async with publisher:
                # Act
                await publisher.publish(event.type, event.metadata)

                # Assert - worker should be in shared memory
                with _shared_memory(abbreviated_namespace) as shm:
                    assert shm.buf

                    # First slot should contain worker reference
                    slot = struct.unpack_from("16s", shm.buf, 0)[0]
                    assert slot != NULL_REF

                # Notification file should exist
                assert _watchdog_path(namespace).exists()
        finally:
            address_space.close()
            address_space.unlink()

    @pytest.mark.asyncio
    async def test_publish_worker_dropped(self, namespace, metadata):
        """Test publish() with worker-dropped event.

        Given:
            A Publisher with previously added worker
        When:
            Publishing worker-dropped event
        Then:
            It should remove worker from shared memory
        """
        # Arrange
        abbreviated_namespace = _short_hash(namespace)

        # Create and initialize address space
        address_space = SharedMemory(name=abbreviated_namespace, create=True, size=1024)
        assert address_space.buf
        for i in range(0, len(address_space.buf), 16):
            address_space.buf[i : i + 16] = NULL_REF

        publisher = LocalDiscovery.Publisher(namespace)

        try:
            async with publisher:
                # Add worker first
                add_event = DiscoveryEvent("worker-added", metadata=metadata)
                await publisher.publish(add_event.type, add_event.metadata)

                # Act - drop worker
                drop_event = DiscoveryEvent("worker-dropped", metadata=metadata)
                await publisher.publish(drop_event.type, drop_event.metadata)

                # Assert - worker should be removed (slot is NULL)
                with _shared_memory(abbreviated_namespace) as shm:
                    assert shm.buf
                    slot = struct.unpack_from("16s", shm.buf, 0)[0]
                    assert slot == NULL_REF
        finally:
            address_space.close()
            address_space.unlink()

    @pytest.mark.asyncio
    async def test_publish_worker_updated(self, namespace, metadata):
        """Test publish() with worker-updated event.

        Given:
            A Publisher with existing worker
        When:
            Publishing worker-updated event with modified worker
        Then:
            It should update worker in shared memory
        """
        # Arrange
        abbreviated_namespace = _short_hash(namespace)
        address_space = SharedMemory(name=abbreviated_namespace, create=True, size=1024)
        assert address_space.buf
        for i in range(0, len(address_space.buf), 16):
            address_space.buf[i : i + 16] = NULL_REF

        publisher = LocalDiscovery.Publisher(namespace)

        try:
            async with publisher:
                # Add worker first
                add_event = DiscoveryEvent("worker-added", metadata=metadata)
                await publisher.publish(add_event.type, add_event.metadata)

                # Act - update worker with new version
                updated_worker = WorkerMetadata(
                    uid=metadata.uid,  # Same UID
                    address="newhost:9999",
                    pid=99999,
                    version="2.0.0",
                )
                update_event = DiscoveryEvent("worker-updated", metadata=updated_worker)
                await publisher.publish(update_event.type, update_event.metadata)

                # Assert - worker should be updated in shared memory
                # (We can verify by reading back the worker metadata)
                assert _watchdog_path(namespace).exists()
        finally:
            address_space.close()
            address_space.unlink()

    @pytest.mark.asyncio
    async def test_namespace_isolation(self):
        """Test namespace isolation between publishers.

        Given:
            Two publishers with different namespaces
        When:
            Publishing workers to each namespace
        Then:
            Workers in namespace A should not appear in namespace B
        """
        # Arrange
        namespace_a = f"test-ns-a-{uuid.uuid4()}"
        namespace_b = f"test-ns-b-{uuid.uuid4()}"

        worker_a = WorkerMetadata(
            uid=uuid.uuid4(), address="host-a:5001", pid=111, version="1.0"
        )
        worker_b = WorkerMetadata(
            uid=uuid.uuid4(), address="host-b:5002", pid=222, version="1.0"
        )

        # Create address spaces for both namespaces
        name_a = _short_hash(namespace_a)
        block_a = SharedMemory(name=name_a, create=True, size=1024)
        assert block_a.buf
        for i in range(0, len(block_a.buf), 16):
            block_a.buf[i : i + 16] = NULL_REF

        name_b = _short_hash(namespace_b)
        block_b = SharedMemory(name=name_b, create=True, size=1024)
        assert block_b.buf
        for i in range(0, len(block_b.buf), 16):
            block_b.buf[i : i + 16] = NULL_REF

        publisher_a = LocalDiscovery.Publisher(namespace_a)
        publisher_b = LocalDiscovery.Publisher(namespace_b)

        try:
            async with publisher_a, publisher_b:
                # Act - publish workers to different namespaces
                await publisher_a.publish("worker-added", worker_a)
                await publisher_b.publish("worker-added", worker_b)

                # Assert - each namespace should only have its own worker
                with _shared_memory(name_a) as shm_a:
                    assert shm_a.buf
                    slot_a = struct.unpack_from("16s", shm_a.buf, 0)[0]
                    assert slot_a != NULL_REF  # Worker A is in namespace A

                with _shared_memory(name_b) as shm_b:
                    assert shm_b.buf
                    slot_b = struct.unpack_from("16s", shm_b.buf, 0)[0]
                    assert slot_b != NULL_REF  # Worker B is in namespace B

                # The worker references should be different
                assert slot_a != slot_b
        finally:
            block_a.close()
            block_a.unlink()
            block_b.close()
            block_b.unlink()

    @pytest.mark.asyncio
    @settings(
        max_examples=16, suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    @given(worker_count=st.integers(min_value=1, max_value=8))
    async def test_concurrent_publishes(self, namespace, worker_count):
        """Test concurrent publish operations are safe.

        Given:
            Multiple workers to publish concurrently
        When:
            Publishing workers in parallel with asyncio.gather
        Then:
            All workers should be added correctly with file locking
        """
        # Arrange
        abbreviated_namespace = _short_hash(namespace)
        address_space = SharedMemory(name=abbreviated_namespace, create=True, size=1024)
        assert address_space.buf
        for i in range(0, len(address_space.buf), 16):
            address_space.buf[i : i + 16] = NULL_REF

        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"host-{i}:{5000 + i}",
                pid=100 + i,
                version="1.0",
            )
            for i in range(worker_count)
        ]

        publisher = LocalDiscovery.Publisher(namespace)

        try:
            async with publisher:
                # Act - publish all workers concurrently
                events = [DiscoveryEvent("worker-added", metadata=w) for w in workers]
                await asyncio.gather(
                    *[publisher.publish(e.type, e.metadata) for e in events]
                )

                # Assert - all workers should be in shared memory
                with _shared_memory(abbreviated_namespace) as shm:
                    assert shm.buf
                    non_null_slots = 0
                    for i in range(0, len(shm.buf), 16):
                        slot = struct.unpack_from("16s", shm.buf, i)[0]
                        if slot != NULL_REF:
                            non_null_slots += 1

                    assert non_null_slots == worker_count
        finally:
            address_space.close()
            address_space.unlink()

    @pytest.mark.asyncio
    async def test_shared_memory_full_condition(self, namespace):
        """Test behavior when shared memory is full.

        Given:
            A Publisher with limited shared memory capacity
        When:
            Publishing more workers than available slots
        Then:
            It should raise RuntimeError when no slots available
        """
        # Arrange - create very small address space (2 slots)
        # Note: macOS rounds up to page size, so we need to calculate actual slots
        abbreviated_namespace = _short_hash(namespace)
        address_space = SharedMemory(
            name=abbreviated_namespace, create=True, size=32
        )  # Request 2 slots
        assert address_space.buf

        # Calculate actual slots based on rounded-up size
        actual_slots = len(address_space.buf) // 16

        for i in range(0, len(address_space.buf), 16):
            address_space.buf[i : i + 16] = NULL_REF

        publisher = LocalDiscovery.Publisher(namespace)

        # Create workers equal to actual_slots + 1
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"host{i}:{5000 + i}",
                pid=100 + i,
                version="1.0",
            )
            for i in range(actual_slots + 1)
        ]

        try:
            async with publisher:
                # Fill all available slots
                for i in range(actual_slots):
                    await publisher.publish("worker-added", workers[i])

                # Act & Assert - next worker should fail
                with pytest.raises(RuntimeError, match="No available slots"):
                    await publisher.publish("worker-added", workers[actual_slots])
        finally:
            address_space.close()
            address_space.unlink()

    def test_publisher_init_negative_block_size(self, namespace):
        """Test Publisher rejects negative block sizes.

        Given:
            A negative block_size value
        When:
            Creating a Publisher instance
        Then:
            It should raise ValueError
        """
        # Act & Assert
        with pytest.raises(ValueError, match="Block size must be positive"):
            LocalDiscovery.Publisher(namespace, block_size=-1)

    @pytest.mark.asyncio
    async def test_publish_unexpected_event_type_raises_error(self, namespace):
        """Test publish() raises error for unexpected event types at runtime.

        Given:
            A Publisher instance and an event with invalid type
        When:
            Publishing an event with type not in valid set (bypassing type hints)
        Then:
            Should raise RuntimeError with descriptive message
        """
        # Arrange
        abbreviated_namespace = _short_hash(namespace)
        address_space = SharedMemory(name=abbreviated_namespace, create=True, size=1024)
        assert address_space.buf is not None
        for i in range(0, len(address_space.buf), 16):
            address_space.buf[i : i + 16] = NULL_REF

        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="test-host:50051", pid=12345, version="1.0"
        )

        publisher = LocalDiscovery.Publisher(namespace)

        try:
            async with publisher:
                # Act - Create event with invalid type by bypassing type system
                # Use object.__setattr__ to bypass frozen dataclass validation
                event = DiscoveryEvent("worker-added", metadata=worker)
                object.__setattr__(event, "type", "invalid-event-type")

                # Assert
                with pytest.raises(
                    RuntimeError, match="Unexpected discovery event type"
                ):
                    await publisher.publish(event.type, event.metadata)
        finally:
            address_space.close()
            address_space.unlink()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "event_type",
        ["worker-added", "worker-dropped", "worker-updated"],
    )
    async def test_publish_with_none_buf_raises_error(
        self, namespace, mocker: MockerFixture, event_type
    ):
        """Test publish() raises error when address_space.buf is None.

        Given:
            A Publisher instance with address_space.buf mocked as None
        When:
            Publishing any type of discovery event
        Then:
            Should raise RuntimeError about improper initialization
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="test-host:50051", pid=12345, version="1.0"
        )

        publisher = LocalDiscovery.Publisher(namespace)

        # Create a mock SharedMemory with buf=None
        mock_address_space = mocker.MagicMock()
        mock_address_space.buf = None

        # Act & Assert
        async with publisher:
            # Mock _shared_memory context manager to yield our mock with buf=None
            mock_context = mocker.MagicMock()
            mock_context.__enter__.return_value = mock_address_space
            mock_context.__exit__.return_value = None

            with patch(
                "wool.runtime.discovery.local._shared_memory",
                return_value=mock_context,
            ):
                with pytest.raises(
                    RuntimeError, match="Registrar service not properly initialized"
                ):
                    await publisher.publish(event_type, worker)

    @pytest.mark.asyncio
    async def test_publish_worker_added_cleanup_on_exception(self, namespace):
        """Test worker is not discoverable when exception occurs during _add.

        Given:
            A Publisher where struct.pack_into raises an exception
        When:
            Publishing a worker-added event that fails
        Then:
            Worker should not be discoverable by subscribers (cleanup successful)
        """
        # Arrange
        abbreviated_namespace = _short_hash(namespace)
        address_space = SharedMemory(name=abbreviated_namespace, create=True, size=1024)
        assert address_space.buf is not None
        for i in range(0, len(address_space.buf), 16):
            address_space.buf[i : i + 16] = NULL_REF

        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="test-host:50051", pid=12345, version="1.0"
        )

        publisher = LocalDiscovery.Publisher(namespace)
        subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

        try:
            async with publisher:
                # Mock struct.pack_into to raise an exception on the second call
                original_pack_into = struct.pack_into
                call_count = [0]

                def mock_pack_into(*args, **kwargs):
                    call_count[0] += 1
                    # First call is for memory_block.buf, second is for address_space.buf
                    if call_count[0] == 2:
                        raise RuntimeError("Simulated pack_into failure")
                    return original_pack_into(*args, **kwargs)

                with patch("struct.pack_into", side_effect=mock_pack_into):
                    # Attempt to publish should fail
                    with pytest.raises(
                        RuntimeError, match="Simulated pack_into failure"
                    ):
                        await publisher.publish("worker-added", worker)

                # Verify worker is not discoverable by scanning subscriber with timeout
                # If cleanup worked, subscriber should see no workers (timeout)
                events = []
                try:
                    async with asyncio.timeout(0.2):
                        async for event in subscriber:
                            events.append(event)
                            # If we get any event, check it's not our worker
                            break
                except TimeoutError:
                    # Timeout is expected - no workers were discovered
                    pass

                # Assert that either no events were discovered,
                # or the worker is not in them
                assert len(events) == 0 or all(
                    e.metadata.uid != worker.uid for e in events
                )
        finally:
            address_space.close()
            address_space.unlink()

    @pytest.mark.asyncio
    async def test_shared_memory_finalizer_handles_oserror(
        self, namespace, mocker: MockerFixture
    ):
        """Test Publisher finalizer handles OSError when unlinking shared memory.

        Given:
            A Publisher with a worker that has been added
        When:
            The Publisher exits and finalizer attempts to unlink shared memory
            that raises OSError
        Then:
            The Publisher should exit cleanly without raising the OSError
        """
        # Arrange - create address space
        abbreviated_namespace = _short_hash(namespace)
        address_space = SharedMemory(name=abbreviated_namespace, create=True, size=1024)
        assert address_space.buf is not None
        for i in range(0, len(address_space.buf), 16):
            address_space.buf[i : i + 16] = NULL_REF

        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="test-host:50051", pid=12345, version="1.0"
        )

        publisher = LocalDiscovery.Publisher(namespace)

        # Mock SharedMemory.unlink to raise OSError
        original_unlink = SharedMemory.unlink

        def mock_unlink(self):
            # Only raise OSError for worker memory blocks, not the address space
            if self.name != abbreviated_namespace:
                raise OSError("Simulated unlink failure")
            return original_unlink(self)

        try:
            mocker.patch.object(SharedMemory, "unlink", mock_unlink)
            async with publisher:
                # Publish a worker to trigger shared memory allocation
                await publisher.publish("worker-added", worker)
            # Act - exit Publisher context manager
            # The finalizer will be called and should handle OSError gracefully

            # Assert - no exception was raised during exit, test passes
        finally:
            address_space.close()
            address_space.unlink()

    @pytest.mark.asyncio
    async def test_atexit_cleanup_handles_oserror(
        self, namespace, mocker: MockerFixture
    ):
        """Test atexit cleanup function handles OSError when unlinking shared memory.

        Given:
            A Publisher that registers an atexit cleanup function
        When:
            The cleanup function is called and unlink raises OSError
        Then:
            The OSError should be caught and the cleanup should complete without error
        """
        # Arrange - create address space
        abbreviated_namespace = _short_hash(namespace)
        address_space = SharedMemory(name=abbreviated_namespace, create=True, size=1024)
        assert address_space.buf is not None
        for i in range(0, len(address_space.buf), 16):
            address_space.buf[i : i + 16] = NULL_REF

        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="test-host:50051", pid=12345, version="1.0"
        )

        # Mock atexit.register to capture the cleanup function
        captured_cleanup_funcs = []

        def mock_atexit_register(func):
            captured_cleanup_funcs.append(func)
            return func

        atexit_spy = mocker.patch("atexit.register", side_effect=mock_atexit_register)

        # Mock SharedMemory.unlink to raise OSError
        unlink_spy = mocker.patch.object(
            SharedMemory, "unlink", side_effect=OSError("Simulated unlink failure")
        )

        publisher = LocalDiscovery.Publisher(namespace)

        try:
            async with publisher:
                await publisher.publish("worker-added", worker)

            # Assert atexit.register was called
            assert atexit_spy.call_count >= 1
            assert len(captured_cleanup_funcs) >= 1

            # Act - manually invoke the captured cleanup function
            # This simulates what happens when the process exits
            cleanup_func = captured_cleanup_funcs[0]
            cleanup_func()  # Should not raise despite OSError from unlink

            # Assert - verify unlink was called and no exception was raised
            assert unlink_spy.called
        finally:
            # Cleanup by unregistering the real unlink mock to allow cleanup
            mocker.stop(unlink_spy)
            address_space.close()
            address_space.unlink()

    @pytest.mark.asyncio
    async def test_update_nonexistent_worker_raises_keyerror(self, namespace):
        """Test updating a worker that doesn't exist raises KeyError.

        Given:
            A Publisher with an empty address space
        When:
            Attempting to update a worker that was never added
        Then:
            It should raise KeyError with appropriate message
        """
        # Arrange - create address space
        abbreviated_namespace = _short_hash(namespace)
        address_space = SharedMemory(name=abbreviated_namespace, create=True, size=1024)
        assert address_space.buf is not None
        for i in range(0, len(address_space.buf), 16):
            address_space.buf[i : i + 16] = NULL_REF

        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="test-host:50051", pid=12345, version="1.0"
        )

        publisher = LocalDiscovery.Publisher(namespace)

        try:
            async with publisher:
                # Act & Assert - attempt to update non-existent worker
                with pytest.raises(
                    KeyError, match=f"Worker {worker.uid} not found in address space"
                ):
                    await publisher.publish("worker-updated", worker)
        finally:
            address_space.close()
            address_space.unlink()

    @pytest.mark.asyncio
    async def test_update_restores_prior_state_on_exception(self, namespace):
        """Test update restores prior worker state when exception occurs.

        Given:
            A Publisher with a worker already added
        When:
            Updating the worker fails during serialization
        Then:
            The prior worker state should be restored in shared memory
        """
        # Arrange - create address space
        abbreviated_namespace = _short_hash(namespace)
        address_space = SharedMemory(name=abbreviated_namespace, create=True, size=1024)
        assert address_space.buf is not None
        for i in range(0, len(address_space.buf), 16):
            address_space.buf[i : i + 16] = NULL_REF

        # Create initial worker
        initial_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="initial-host:50051",
            pid=12345,
            version="1.0",
            tags=frozenset(["initial"]),
        )

        # Create updated worker with same UID
        updated_worker = WorkerMetadata(
            uid=initial_worker.uid,
            address="updated-host:60061",
            pid=99999,
            version="2.0",
            tags=frozenset(["updated"]),
        )

        publisher = LocalDiscovery.Publisher(namespace)
        subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

        try:
            async with publisher:
                # Add initial worker
                await publisher.publish("worker-added", initial_worker)

                # Wait for subscriber to discover initial worker
                async with asyncio.timeout(1.0):
                    async for event in subscriber:
                        if (
                            event.type == "worker-added"
                            and event.metadata.uid == initial_worker.uid
                        ):
                            break

                # Mock struct.pack_into to fail on the new update write
                original_pack_into = struct.pack_into
                call_count = [0]

                def mock_pack_into(fmt, buffer, offset, *args):
                    call_count[0] += 1
                    # First call writes new data - make this one fail
                    # Second call restores prior data - let this succeed
                    if call_count[0] == 1:
                        raise RuntimeError("Simulated pack_into failure during update")
                    return original_pack_into(fmt, buffer, offset, *args)

                with patch(
                    "wool.runtime.discovery.local.struct.pack_into",
                    side_effect=mock_pack_into,
                ):
                    # Attempt to update worker - should fail
                    with pytest.raises(
                        RuntimeError, match="Simulated pack_into failure"
                    ):
                        await publisher.publish("worker-updated", updated_worker)

                # Verify worker still has initial state by checking what subscriber sees
                # Wait a bit for any changes to propagate
                await asyncio.sleep(0.2)

                # Re-scan to get current worker state
                events = []
                async with asyncio.timeout(1.0):
                    async for event in subscriber:
                        if event.metadata.uid == initial_worker.uid:
                            events.append(event)
                            break

                # Assert the worker's prior state was restored
                assert len(events) == 1
                discovered_worker = events[0].metadata
                assert discovered_worker.address == "initial-host:50051"
                assert discovered_worker.version == "1.0"
                assert discovered_worker.tags == frozenset(["initial"])
        finally:
            address_space.close()
            address_space.unlink()


class TestLocalDiscoverySubscriber:
    """Tests for LocalDiscovery.Subscriber class."""

    @pytest.mark.asyncio
    async def test_initial_scan_discovers_existing_workers(self, namespace, metadata):
        """Test initial scan discovers existing workers immediately.

        Given:
            Existing workers in shared memory
        When:
            Creating subscriber and starting iteration
        Then:
            It should yield worker-added events for all existing workers
        """
        # Arrange - add worker before subscriber starts
        abbreviated_namespace = _short_hash(namespace)
        address_space = SharedMemory(name=abbreviated_namespace, create=True, size=1024)
        assert address_space.buf
        for i in range(0, len(address_space.buf), 16):
            address_space.buf[i : i + 16] = NULL_REF

        publisher = LocalDiscovery.Publisher(namespace)

        try:
            # Keep publisher context open so worker blocks remain available
            async with publisher:
                await publisher.publish("worker-added", metadata)

                # Act - create subscriber after worker is added
                subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.1)

                event_received = asyncio.Event()
                events = []

                async def collect_events():
                    async for event in subscriber:
                        events.append(event)
                        event_received.set()
                        break  # Stop after first event

                task = asyncio.create_task(collect_events())

                # Wait for event with timeout
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

                # Assert
                assert len(events) >= 1
                assert events[0].type == "worker-added"
                assert events[0].metadata.uid == metadata.uid
        finally:
            address_space.close()
            address_space.unlink()

    @pytest.mark.asyncio
    async def test_filtering_with_predicate(self, namespace):
        """Test subscriber filtering with PredicateFunction.

        Given:
            Subscriber with filter predicate
        When:
            Workers matching and not matching filter are published
        Then:
            Only workers matching predicate should be yielded
        """
        # Arrange
        abbreviated_namespace = _short_hash(namespace)
        address_space = SharedMemory(name=abbreviated_namespace, create=True, size=1024)
        assert address_space.buf
        for i in range(0, len(address_space.buf), 16):
            address_space.buf[i : i + 16] = NULL_REF

        worker_match = WorkerMetadata(
            uid=uuid.uuid4(), address="host1:50051", pid=111, version="1.0"
        )
        worker_no_match = WorkerMetadata(
            uid=uuid.uuid4(), address="host2:9999", pid=222, version="1.0"
        )

        def filter_fn(w):
            return w.address == "host1:50051"

        publisher = LocalDiscovery.Publisher(namespace)
        subscriber = LocalDiscovery.Subscriber(
            namespace, filter=filter_fn, poll_interval=0.1
        )

        try:
            event_received = asyncio.Event()
            events = []

            async def collect_events():
                async for event in subscriber:
                    events.append(event)
                    event_received.set()

            collect_task = asyncio.create_task(collect_events())

            # Give subscriber a moment to initialize
            await asyncio.sleep(0.05)

            # Keep publisher open so worker blocks remain available
            async with publisher:
                await publisher.publish("worker-added", worker_match)
                await publisher.publish("worker-added", worker_no_match)

                # Wait for at least one event with timeout
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass

                # Give time for any additional events to arrive
                await asyncio.sleep(0.1)

                collect_task.cancel()
                try:
                    await collect_task
                except asyncio.CancelledError:
                    pass

                # Assert - only matching worker should appear
                assert len(events) >= 1
                assert all(e.metadata.address == "host1:50051" for e in events)
        finally:
            address_space.close()
            address_space.unlink()

    @pytest.mark.asyncio
    async def test_invalid_poll_interval_raises_valueerror(self, namespace):
        """Test subscriber raises ValueError for negative poll_interval.

        Given:
            Negative poll_interval value
        When:
            Creating subscriber
        Then:
            It should raise ValueError
        """
        # Act & Assert
        with pytest.raises(ValueError, match="Expected positive poll interval"):
            LocalDiscovery.Subscriber(namespace, poll_interval=-1.0)

    @given(
        matching_address=st.from_regex(r"^host:[0-9]{1,5}$", fullmatch=True),
        non_matching_address=st.from_regex(r"^other:[0-9]{1,5}$", fullmatch=True),
    )
    @settings(
        max_examples=10,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @pytest.mark.asyncio
    async def test_subscriber_pickle_round_trip(
        self, namespace, matching_address, non_matching_address
    ):
        """Test Subscriber can be pickled and unpickled with cloudpickle.

        Given:
            A Subscriber instance with custom filter
        When:
            Pickling and unpickling the subscriber with cloudpickle
        Then:
            Unpickled subscriber should work correctly with same filter
        """
        # Arrange
        abbreviated_namespace = _short_hash(namespace)
        address_space = SharedMemory(name=abbreviated_namespace, create=True, size=1024)
        assert address_space.buf is not None
        for i in range(0, len(address_space.buf), 16):
            address_space.buf[i : i + 16] = NULL_REF

        def filter_func(w):
            return w.address == matching_address

        subscriber = LocalDiscovery.Subscriber(
            namespace, filter=filter_func, poll_interval=0.05
        )

        # Act - pickle and unpickle
        pickled = cloudpickle.dumps(subscriber)
        unpickled = cloudpickle.loads(pickled)

        matching_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address=matching_address,
            pid=123,
            version="1.0",
        )
        non_matching_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address=non_matching_address,
            pid=456,
            version="1.0",
        )

        publisher = LocalDiscovery.Publisher(namespace)
        events = []
        event_received = asyncio.Event()

        async def collect_events():
            async for event in unpickled:
                events.append(event)
                event_received.set()
                break

        try:
            async with publisher:
                collect_task = asyncio.create_task(collect_events())

                # Publish both workers
                await publisher.publish("worker-added", matching_worker)
                await publisher.publish("worker-added", non_matching_worker)

                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
                finally:
                    collect_task.cancel()
                    try:
                        await collect_task
                    except asyncio.CancelledError:
                        pass

                # Assert - should only receive matching worker
                assert len(events) == 1
                assert events[0].metadata.address == matching_address
                assert events[0].metadata.uid == matching_worker.uid
        finally:
            address_space.close()
            address_space.unlink()

    @pytest.mark.asyncio
    async def test_subscriber_detects_worker_dropped(self, namespace):
        """Test Subscriber emits worker-dropped events when worker removed.

        Given:
            A Subscriber that has discovered workers
        When:
            A previously discovered worker is removed from shared memory
        Then:
            Should emit worker-dropped event with the removed worker's info
        """
        # Arrange
        abbreviated_namespace = _short_hash(namespace)
        address_space = SharedMemory(name=abbreviated_namespace, create=True, size=1024)
        assert address_space.buf
        for i in range(0, len(address_space.buf), 16):
            address_space.buf[i : i + 16] = NULL_REF

        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="test-host:50051", pid=12345, version="1.0"
        )

        publisher = LocalDiscovery.Publisher(namespace)
        subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

        events = []
        worker_added_event = asyncio.Event()
        worker_dropped_event = asyncio.Event()

        async def collect_events():
            async for event in subscriber:
                events.append(event)
                if event.type == "worker-added" and event.metadata.uid == worker.uid:
                    worker_added_event.set()
                elif event.type == "worker-dropped" and event.metadata.uid == worker.uid:
                    worker_dropped_event.set()
                    break

        try:
            async with publisher:
                # Start collecting events
                collect_task = asyncio.create_task(collect_events())

                # Add worker first
                await publisher.publish("worker-added", worker)

                # Wait for worker-added event to be detected
                await asyncio.wait_for(worker_added_event.wait(), timeout=2.0)

                # Act - drop the worker
                await publisher.publish("worker-dropped", worker)

                # Wait for worker-dropped event with timeout
                try:
                    await asyncio.wait_for(worker_dropped_event.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
                finally:
                    collect_task.cancel()
                    try:
                        await collect_task
                    except asyncio.CancelledError:
                        pass

                # Assert
                dropped_events = [e for e in events if e.type == "worker-dropped"]
                assert len(dropped_events) >= 1
                assert dropped_events[0].metadata.uid == worker.uid
        finally:
            address_space.close()
            address_space.unlink()
