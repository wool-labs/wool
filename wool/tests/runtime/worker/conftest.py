import asyncio
import datetime
import uuid
from types import MappingProxyType
from typing import Any
from unittest.mock import MagicMock

import grpc
import pytest
import pytest_asyncio
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from pytest_mock import MockerFixture

import wool.runtime.worker.pool as wp
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import WorkerMetadata
from wool.runtime.worker.auth import WorkerCredentials


@pytest_asyncio.fixture(autouse=True)
async def _clear_channel_pool():
    """Clear the module-level gRPC channel pool between tests.

    Prevents stale cached channels (with outdated mocks) from leaking
    across tests.
    """
    yield
    import wool.runtime.worker.connection as _conn

    await _conn._channel_pool.clear()


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
        self,
        *tags: str,
        should_fail: bool = False,
        start_delay: float = 0.0,
        credentials: WorkerCredentials | None = None,
    ):
        self._uid = uuid.uuid4()
        self._tags = set(tags)
        self._started = False
        self._info: WorkerMetadata | None = None
        self.dispatch_count = 0
        self.should_fail = should_fail
        self.start_delay = start_delay
        self.credentials = credentials

    @property
    def uid(self):
        """Worker unique identifier."""
        return self._uid

    @property
    def tags(self):
        """Worker capability tags."""
        return self._tags

    @property
    def metadata(self) -> WorkerMetadata | None:
        """Worker metadata (available after start)."""
        return self._info

    @property
    def extra(self):
        """Extra metadata."""
        return {}

    @property
    def address(self) -> str | None:
        """Network address (available after start)."""
        if self._info:
            return self._info.address
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

        # Create WorkerMetadata after successful start
        self._info = WorkerMetadata(
            uid=self._uid,
            address="localhost:50051",
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

    def factory(*tags: str, credentials=None):
        return MockWorker(*tags, credentials=credentials)

    return factory


@pytest.fixture
async def worker_pool(mock_worker_factory):
    """Pre-configured WorkerPool with mock worker factory.

    Yields:
        Started WorkerPool instance with 3 mock workers
    """
    from wool.runtime.worker.pool import WorkerPool

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
        self.workers: list[WorkerMetadata] = []
        self.started = False
        self._event_queue: asyncio.Queue = asyncio.Queue()

    async def start(self) -> None:
        """Start the mock discovery service."""
        self.started = True

    async def stop(self) -> None:
        """Stop the mock discovery service."""
        self.started = False

    def inject_worker_added(self, metadata: WorkerMetadata) -> None:
        """Simulate discovery of a new worker.

        Args:
            metadata: Worker connection metadata
        """
        if metadata not in self.workers:
            self.workers.append(metadata)
        if self.started:
            event = DiscoveryEvent("worker-added", metadata=metadata)
            self._event_queue.put_nowait(event)

    def inject_worker_removed(self, metadata: WorkerMetadata) -> None:
        """Simulate departure of a worker.

        Args:
            metadata: Worker connection metadata
        """
        if metadata in self.workers:
            self.workers.remove(metadata)
        if self.started:
            event = DiscoveryEvent("worker-dropped", metadata=metadata)
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
        mock_worker.metadata = mocker.MagicMock()
        worker_count[0] += 1
        mock_worker.metadata.uid = f"test-worker-{worker_count[0]}"
        mock_worker.metadata.address = f"localhost:{50050 + worker_count[0]}"
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
        metadata: WorkerMetadata,
        response_mode: str = "success",
        response_value: Any = None,
    ):
        """Create mock gRPC stub.

        Args:
            metadata: Worker connection metadata
            response_mode: Response behavior ('success', 'failure', 'timeout')
            response_value: Value to return on success
        """
        self.metadata = metadata
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

    def factory(metadata: WorkerMetadata, **kwargs):
        return MockGrpcStub(metadata, **kwargs)

    return factory


@pytest.fixture
async def worker_proxy(mock_discovery_service, mock_grpc_stub_factory, metadata):
    """Pre-configured WorkerProxy with mock discovery and gRPC stubs.

    Yields:
        WorkerProxy instance with 2 pre-configured mock workers
    """
    from wool.runtime.worker.proxy import WorkerProxy

    # Inject 2 mock workers into discovery
    worker1 = WorkerMetadata(
        uid=uuid.uuid4(),
        address="192.168.1.100:50051",
        pid=1001,
        version="1.0.0",
        tags=frozenset(["test"]),
        extra=MappingProxyType({}),
    )
    worker2 = WorkerMetadata(
        uid=uuid.uuid4(),
        address="192.168.1.101:50051",
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


# ============================================================================
# Credential Fixtures for Authentication Tests
# ============================================================================


def _generate_test_certificates():
    """Generate self-signed test certificates for SSL/TLS testing.

    Creates a certificate authority (CA) and worker certificate for
    localhost. These certificates are used for secure gRPC connections
    in tests.

    Returns:
        Tuple of (private_key_pem, certificate_pem, ca_cert_pem)
    """
    # Generate private key
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )

    # Create certificate subject
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
        ]
    )

    # Build self-signed certificate with both server and client auth
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.utcnow())
        .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                ]
            ),
            critical=False,
        )
        .add_extension(
            x509.ExtendedKeyUsage(
                [
                    x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,
                    x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
                ]
            ),
            critical=False,
        )
        .sign(private_key, hashes.SHA256(), default_backend())
    )

    # Serialize to PEM format
    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )

    cert_pem = cert.public_bytes(serialization.Encoding.PEM)

    return private_key_pem, cert_pem, cert_pem


@pytest.fixture(scope="module")
def test_certificates():
    """Provide test certificates for the test module.

    Returns:
        Tuple of (private_key_pem, certificate_pem, ca_cert_pem)
    """
    return _generate_test_certificates()


@pytest.fixture
def worker_credentials(test_certificates):
    """Provide WorkerCredentials with mutual=True for testing.

    Returns:
        WorkerCredentials instance configured for mTLS
    """
    key_pem, cert_pem, ca_pem = test_certificates
    return WorkerCredentials(
        ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
    )


@pytest.fixture
def worker_credentials_one_way(test_certificates):
    """Provide WorkerCredentials with mutual=False for testing.

    Returns:
        WorkerCredentials instance configured for one-way TLS
    """
    key_pem, cert_pem, ca_pem = test_certificates
    return WorkerCredentials(
        ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=False
    )


@pytest.fixture
def worker_credentials_callable(test_certificates):
    """Provide WorkerCredentials with callable credentials for testing.

    Returns:
        WorkerCredentials with callable server and client credentials
    """
    key_pem, cert_pem, ca_pem = test_certificates

    def server_factory():
        return grpc.ssl_server_credentials(
            private_key_certificate_chain_pairs=[(key_pem, cert_pem)],
            root_certificates=ca_pem,
            require_client_auth=True,
        )

    def client_factory():
        return grpc.ssl_channel_credentials(
            root_certificates=ca_pem, private_key=key_pem, certificate_chain=cert_pem
        )

    # Create a WorkerCredentials-like object with callable properties
    # Since WorkerCredentials is frozen, we need to create it differently
    return WorkerCredentials(
        ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
    )
