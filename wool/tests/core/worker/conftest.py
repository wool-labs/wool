import asyncio
import uuid
from types import MappingProxyType
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

import wool.core.worker.pool as wp
from wool.core.discovery.base import DiscoveryEvent
from wool.core.discovery.base import WorkerInfo


@pytest.fixture
def worker_info():
    """Provides sample WorkerInfo for testing.

    Creates a WorkerInfo instance with typical field values for use in
    tests that need a well-formed worker instance.
    """
    return WorkerInfo(
        uid=uuid.UUID("12345678-1234-5678-1234-567812345678"),
        host="localhost",
        port=50051,
        pid=12345,
        version="1.0.0",
        tags=frozenset(["test", "worker"]),
        extra=MappingProxyType({"key": "value"}),
    )


@pytest.fixture
def worker_tags():
    """Provides sample worker tags for testing."""
    return ("gpu", "ml-capable", "production")


@pytest.fixture
def worker_extra():
    """Provides sample worker extra metadata for testing."""
    return {"region": "us-west-2", "instance_type": "t3.large"}


# ============================================================================
# Mock Fixtures for WorkerPool and WorkerProxy Testing
# ============================================================================


class MockWorker:
    """Mock worker implementing WorkerLike protocol for testing.

    This test double simulates a worker instance with configurable behavior
    for testing WorkerPool orchestration without actual subprocess overhead.
    """

    def __init__(
        self, *tags: str, should_fail: bool = False, start_delay: float = 0.0, **kwargs
    ):
        """Create mock worker.

        Args:
            *tags: Capability tags for this worker
            should_fail: If True, start() and dispatch() raise RuntimeError
            start_delay: Simulated startup delay in seconds
            **kwargs: Additional configuration (reserved)
        """
        self._uid = uuid.uuid4()
        self._tags = set(tags)
        self._started = False
        self._info: WorkerInfo | None = None
        self.dispatch_count = 0
        self.should_fail = should_fail
        self.start_delay = start_delay

    @property
    def uid(self):
        """Worker unique identifier."""
        return self._uid

    @property
    def tags(self):
        """Worker capability tags."""
        return self._tags

    @property
    def info(self) -> WorkerInfo | None:
        """Worker information (available after start)."""
        return self._info

    @property
    def extra(self):
        """Extra metadata."""
        return {}

    @property
    def address(self) -> str | None:
        """Network address (available after start)."""
        if self._info:
            return f"{self._info.host}:{self._info.port}"
        return None

    async def start(self) -> None:
        """Start the mock worker.

        Raises:
            RuntimeError: If should_fail=True in constructor
        """
        if self.should_fail:
            raise RuntimeError("Mock worker startup failed")
        if self.start_delay > 0:
            await asyncio.sleep(self.start_delay)

        # Create WorkerInfo after successful start
        self._info = WorkerInfo(
            uid=self._uid,
            host="localhost",
            port=50051,
            pid=12345,
            version="1.0.0",
            tags=frozenset(self._tags),
            extra=MappingProxyType({}),
        )
        self._started = True

    async def stop(self) -> None:
        """Stop the mock worker (always succeeds)."""
        self._started = False
        self._info = None

    async def dispatch(self, task: Any) -> Any:
        """Simulate task dispatch.

        Args:
            task: Task to dispatch

        Returns:
            Task result (echoes task for testing)

        Raises:
            RuntimeError: If not started or should_fail=True
        """
        if not self._started or self.should_fail:
            raise RuntimeError("Mock worker not available")
        self.dispatch_count += 1
        return task


@pytest.fixture
def mock_worker_factory(mocker: MockerFixture):
    """Factory that creates fresh MockWorker instances for each test.

    Returns:
        Callable that creates MockWorker instances with specified tags
    """

    def factory(*tags: str, **kwargs):
        return MockWorker(*tags, **kwargs)

    return factory


@pytest.fixture
async def worker_pool(mock_worker_factory):
    """Pre-configured WorkerPool with mock worker factory.

    Yields:
        Started WorkerPool instance with 3 mock workers
    """
    from wool.core.worker.pool import WorkerPool

    pool = WorkerPool(worker=mock_worker_factory, size=3)
    try:
        await pool.start()
        yield pool
    finally:
        await pool.stop()


class MockDiscoveryService:
    """Mock discovery service for WorkerProxy testing.

    Simulates worker discovery events with controllable event injection.
    """

    def __init__(self, **kwargs):
        """Create mock discovery service."""
        self.workers: list[WorkerInfo] = []
        self.started = False
        self._event_queue: asyncio.Queue = asyncio.Queue()

    async def start(self) -> None:
        """Start the mock discovery service."""
        self.started = True

    async def stop(self) -> None:
        """Stop the mock discovery service."""
        self.started = False

    def inject_worker_added(self, worker_info: WorkerInfo) -> None:
        """Simulate discovery of a new worker.

        Args:
            worker_info: Worker connection information
        """
        if worker_info not in self.workers:
            self.workers.append(worker_info)
        if self.started:
            event = DiscoveryEvent(type="worker-added", worker_info=worker_info)
            self._event_queue.put_nowait(event)

    def inject_worker_removed(self, worker_info: WorkerInfo) -> None:
        """Simulate departure of a worker.

        Args:
            worker_info: Worker connection information
        """
        if worker_info in self.workers:
            self.workers.remove(worker_info)
        if self.started:
            event = DiscoveryEvent(type="worker-dropped", worker_info=worker_info)
            self._event_queue.put_nowait(event)

    async def __aiter__(self):
        """Async iterator yielding discovery events."""
        while self.started:
            try:
                event = await asyncio.wait_for(self._event_queue.get(), timeout=0.1)
                yield event
            except asyncio.TimeoutError:
                continue


@pytest.fixture
def mock_discovery_service():
    """Mock discovery service for WorkerProxy tests.

    Returns:
        MockDiscoveryService instance (not started)
    """
    return MockDiscoveryService()


# ============================================================================
# Additional Mock Fixtures for test_worker_pool.py Integration Tests
# ============================================================================


@pytest.fixture
def mock_shared_memory(mocker: MockerFixture):
    """Mock SharedMemory for isolation from multiprocessing resources."""
    mock_memory = mocker.MagicMock()
    mock_memory.buf = bytearray(1024)
    mock_memory.close = mocker.MagicMock()
    mock_memory.unlink = mocker.MagicMock()
    mocker.patch("multiprocessing.shared_memory.SharedMemory", return_value=mock_memory)
    return mock_memory


@pytest.fixture
def mock_worker_proxy(mocker: MockerFixture):
    """Mock WorkerProxy for isolation from proxy behavior."""
    mock_proxy = mocker.MagicMock()
    mock_proxy.__aenter__ = mocker.AsyncMock(return_value=mock_proxy)
    mock_proxy.__aexit__ = mocker.AsyncMock()
    mocker.patch.object(wp, "WorkerProxy", return_value=mock_proxy)
    return mock_proxy


@pytest.fixture
def mock_local_worker(mocker: MockerFixture):
    """Mock LocalWorker for isolation from worker process management."""
    worker_count = [0]  # Counter for unique UIDs
    workers = []  # Store all created workers

    def create_worker(*args, **kwargs):
        mock_worker = mocker.MagicMock()
        mock_worker.start = mocker.AsyncMock()
        mock_worker.stop = mocker.AsyncMock()
        mock_worker.info = mocker.MagicMock()
        worker_count[0] += 1
        mock_worker.info.uid = f"test-worker-{worker_count[0]}"
        mock_worker.info.port = 50050 + worker_count[0]
        workers.append(mock_worker)
        return mock_worker

    mocker.patch.object(wp, "LocalWorker", side_effect=create_worker)

    # Return the first worker by default for backwards compatibility
    # Tests can call create_worker() to get additional workers
    first_worker = create_worker()
    first_worker.all_workers = workers
    return first_worker


@pytest.fixture
def mock_discovery_service_for_pool(mocker: MockerFixture):
    """Mock discovery service for WorkerPool durable mode tests.

    Returns a mock that implements DiscoveryLike protocol.
    """

    # Create a proper mock class that implements DiscoveryPublisherLike protocol
    class MockPublisher:
        def __init__(self):
            self.publish = mocker.AsyncMock()

    # Create a proper mock class that implements DiscoveryLike protocol
    class MockDiscovery:
        def __init__(self):
            self.publisher = MockPublisher()
            self.subscriber = mocker.MagicMock()

        def subscribe(self, filter=None):
            return self.subscriber

    mock_discovery = MockDiscovery()
    mocker.patch.object(wp, "LocalDiscovery", return_value=mock_discovery)
    return mock_discovery


class MockGrpcStub:
    """Mock gRPC stub for WorkerProxy testing.

    Simulates gRPC task dispatch with configurable response behavior.
    """

    def __init__(
        self,
        worker_info: WorkerInfo,
        response_mode: str = "success",
        response_value: Any = None,
    ):
        """Create mock gRPC stub.

        Args:
            worker_info: Worker connection information
            response_mode: Response behavior ('success', 'failure', 'timeout')
            response_value: Value to return on success
        """
        self.worker_info = worker_info
        self.dispatch_calls: list = []
        self.response_mode = response_mode
        self.response_value = response_value

    async def Dispatch(self, request, timeout=None):
        """Simulate task dispatch via gRPC.

        Args:
            request: Task request
            timeout: Optional timeout

        Returns:
            Task response based on response_mode

        Raises:
            Exception: If response_mode='failure'
            asyncio.TimeoutError: If response_mode='timeout'
        """
        self.dispatch_calls.append(request)

        if self.response_mode == "failure":
            raise Exception("gRPC dispatch failed")
        elif self.response_mode == "timeout":
            raise asyncio.TimeoutError("gRPC dispatch timeout")

        # Success mode
        response = MagicMock()
        response.result = self.response_value
        return response

    def configure_response(self, mode: str, value: Any = None) -> None:
        """Configure stub response behavior.

        Args:
            mode: Response mode ('success', 'failure', 'timeout')
            value: Value to return on success
        """
        if mode not in ("success", "failure", "timeout"):
            raise ValueError(f"Invalid mode: {mode}")
        self.response_mode = mode
        self.response_value = value


@pytest.fixture
def mock_grpc_stub_factory():
    """Factory creating mock gRPC stubs for WorkerProxy tests.

    Returns:
        Callable that creates MockGrpcStub instances
    """

    def factory(worker_info: WorkerInfo, **kwargs):
        return MockGrpcStub(worker_info, **kwargs)

    return factory


@pytest.fixture
async def worker_proxy(mock_discovery_service, mock_grpc_stub_factory, worker_info):
    """Pre-configured WorkerProxy with mock discovery and gRPC stubs.

    Yields:
        WorkerProxy instance with 2 pre-configured mock workers
    """
    from wool.core.worker.proxy import WorkerProxy

    # Inject 2 mock workers into discovery
    worker1 = WorkerInfo(
        uid=uuid.uuid4(),
        host="192.168.1.100",
        port=50051,
        pid=1001,
        version="1.0.0",
        tags=frozenset(["test"]),
        extra=MappingProxyType({}),
    )
    worker2 = WorkerInfo(
        uid=uuid.uuid4(),
        host="192.168.1.101",
        port=50051,
        pid=1002,
        version="1.0.0",
        tags=frozenset(["test"]),
        extra=MappingProxyType({}),
    )

    mock_discovery_service.inject_worker_added(worker1)
    mock_discovery_service.inject_worker_added(worker2)

    # Create proxy (note: actual WorkerProxy may need different initialization)
    # This is a placeholder - adjust based on actual WorkerProxy constructor
    proxy = MagicMock()  # TODO: Replace with actual WorkerProxy when implementing tests
    proxy.workers = [worker1, worker2]

    yield proxy
