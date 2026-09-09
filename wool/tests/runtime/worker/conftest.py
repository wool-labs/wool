import asyncio
import datetime
import multiprocessing.shared_memory
import threading
import time
import uuid
from contextlib import asynccontextmanager
from types import MappingProxyType
from typing import Any
from typing import Callable
from typing import Coroutine
from unittest.mock import MagicMock

import cloudpickle
import grpc.aio
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
import wool.runtime.worker.proxy as proxy_module
from tests.helpers import scoped_context
from wool import protocol
from wool.runtime.context.factory import _loops_with_factory
from wool.runtime.context.factory import install_task_factory
from wool.runtime.discovery import __subscriber_pool__
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import WorkerProxyLike
from wool.runtime.worker import connection as connection_module
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.connection import clear_channel_pool
from wool.runtime.worker.metadata import WorkerMetadata
from wool.runtime.worker.proxy import WorkerProxy


class PicklableMock(MagicMock):
    """A :class:`MagicMock` subclass that survives ``cloudpickle``.

    Intended for mock proxies that must be serialized in
    ``Task.to_protobuf()`` calls during integration tests.
    """

    def __reduce__(self):
        return (MagicMock, (self._spec_class,))


@pytest.fixture(autouse=True)
def _isolate_wool_context():
    """Install a fresh, unarmed Wool context for the duration of the test.

    Each test runs under its own unarmed context so var values set
    in one test do not leak into subsequent tests via the chain
    context. The process-wide var_registry is not reset; tests
    SHOULD use unique key namespaces (e.g. via uuid suffix) to avoid
    cross-test collisions on shared keys.
    """
    with scoped_context():
        yield


@pytest.fixture(autouse=True)
def _clear_proxy_context():
    """Reset proxy context vars between tests.

    Prevents ContextVar leakage from tests that set
    ``wool.__proxy__`` or ``wool.__proxy_pool__`` (directly
    or via worker_dispatch on the gRPC server handler
    context).
    """
    import wool

    proxy_token = wool.__proxy__.set(None)
    pool_token = wool.__proxy_pool__.set(None)
    yield
    wool.__proxy__.reset(proxy_token)
    wool.__proxy_pool__.reset(pool_token)


@pytest.fixture(autouse=True)
def _clear_worker_context():
    """Reset worker identity state between tests.

    Prevents leakage from tests that set
    ``wool.__worker_metadata__`` or ``wool.__worker_service__``
    for self-dispatch testing.
    """
    import wool

    wool.__worker_metadata__ = None
    wool.__worker_uds_address__ = None
    svc_token = wool.__worker_service__.set(None)
    yield
    wool.__worker_metadata__ = None
    wool.__worker_uds_address__ = None
    wool.__worker_service__.reset(svc_token)


@pytest.fixture(autouse=True)
def _reap_worker_loops():
    """Stop worker event-loops that a dispatch left warm.

    A test that dispatches through a `WorkerService` without stopping
    it leaves the service's warm worker loop running on its daemon
    thread (see `_WORKER_LOOP_TTL` for why the loop pool keeps a loop
    warm across dispatches). Reap any such loop after the test so it
    neither leaks a thread across tests nor survives to interpreter
    exit, where wool's task-factory finalizer logs a spurious
    displacement warning against the still-running loop.

    Runs as an autouse teardown after the test's explicitly-requested
    fixtures, so a loop those fixtures already closed (e.g.,
    `worker_loop`) reads back closed and is skipped — only genuinely
    leaked worker loops are stopped. A multi-generation residual-task
    drain — matching the production finalizer
    `WorkerService._destroy_worker_loop`, so a cancelled task's
    follow-up cleanup is drained rather than stranded — then a stop is
    scheduled onto the loop, and the worker thread closes it once
    ``run_forever`` returns.
    """
    yield
    leaked = [
        loop
        for loop in list(_loops_with_factory)
        if loop.is_running() and not loop.is_closed()
    ]
    for loop in leaked:

        async def _shutdown():
            # Drain successive generations of pending tasks, mirroring
            # the production finalizer's ``_shutdown``: a cancelled
            # task's ``finally`` can schedule a second generation, so
            # cancel -> await -> repeat until none remain (or a bounded
            # budget elapses), then stop. A single pass would strand
            # that second generation, surfacing the intermittent
            # "Task was destroyed but it is pending!" warning.
            current = asyncio.current_task()
            deadline = asyncio.get_running_loop().time() + 5.0
            try:
                while True:
                    pending = [t for t in asyncio.all_tasks() if t is not current]
                    if not pending:
                        break
                    for task in pending:
                        task.cancel()
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        break
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*pending, return_exceptions=True),
                            timeout=remaining,
                        )
                    except TimeoutError:
                        break
            finally:
                asyncio.get_running_loop().stop()

        try:
            loop.call_soon_threadsafe(lambda loop=loop: loop.create_task(_shutdown()))
        except RuntimeError:
            continue
        deadline = time.monotonic() + 5.0
        while loop.is_running() and time.monotonic() < deadline:
            time.sleep(0.005)


@pytest.fixture
def worker_loop():
    """Spin up a real worker loop on a daemon thread.

    The wool task factory is installed so :func:`routine_scope` can run
    on a separate loop. Used by DispatchSession unit tests and any
    other test that needs to cross-loop coordinate.
    """
    loop = asyncio.new_event_loop()
    install_task_factory(loop)
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        yield loop
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        if not loop.is_closed():
            loop.close()


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


def _make_worker_metadata(*tags: str) -> WorkerMetadata:
    """Build a valid WorkerMetadata with a fresh UUID.

    Advertises the ambient protocol version — what a real in-process
    worker would — so the metadata passes the proxy's
    admission gate.
    """
    return WorkerMetadata(
        uid=uuid.uuid4(),
        address="localhost:50051",
        pid=12345,
        version=protocol.__version__,
        tags=frozenset(tags),
        extra=MappingProxyType({}),
    )


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
    def started(self):
        """Whether ``start`` has run without a matching ``stop``."""
        return self._started

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

        # Create WorkerMetadata after successful start; advertise the
        # ambient protocol version so the mock worker passes the
        # proxy's admission gate.
        self._info = WorkerMetadata(
            uid=self._uid,
            address="localhost:50051",
            pid=12345,
            version=protocol.__version__,
            tags=frozenset(self._tags),
            extra=MappingProxyType({}),
        )
        self._started = True

    async def stop(
        self, *, grace: float | None = None, timeout: float | None = None
    ) -> None:
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

    def factory(*tags: str, credentials=None, options=None):
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


@pytest.fixture
def mock_shared_memory(mocker: MockerFixture):
    """Mock SharedMemory for isolation from multiprocessing resources."""
    mock_memory = mocker.MagicMock()
    mock_memory.buf = bytearray(1024)
    mock_memory.close = mocker.MagicMock()
    mock_memory.unlink = mocker.MagicMock()
    mocker.patch.object(
        multiprocessing.shared_memory, "SharedMemory", return_value=mock_memory
    )
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
    real_cls = wp.LocalWorker  # Capture before patching
    workers = []  # Store all created workers

    def create_worker(*args, **kwargs):
        mock_worker = mocker.MagicMock(spec=real_cls)
        mock_worker.start = mocker.AsyncMock()
        mock_worker.stop = mocker.AsyncMock()
        mock_worker.metadata = _make_worker_metadata(*args)
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
        bind_host = "127.0.0.1"

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
def sample_task():
    """Build a `Task` whose callable returns ``"test_result"``.

    The task carries a picklable mock proxy, so it survives the
    serialization a dispatch performs.
    """

    async def sample_task():
        return "test_result"

    mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

    return Task(
        id=uuid.uuid4(),
        callable=sample_task,
        args=(),
        kwargs={},
        proxy=mock_proxy,
    )


@pytest.fixture
def async_stream():
    """Return a factory that turns an iterable into an async generator.

    The generator yields each item as a mock gRPC response, calling any
    callable item and awaiting any coroutine item in place of yielding
    it, so a stream can interleave side effects with responses.
    """

    async def create_async_stream(iterable):
        """Convert an iterable into an async generator.

        :param iterable:
            Any iterable (list, tuple, generator, etc.).
        """
        for item in iterable:
            if isinstance(item, Callable):
                item()
            elif isinstance(item, Coroutine):
                await item
            else:
                yield item

    return create_async_stream


@pytest.fixture
def mock_grpc_call(mocker: MockerFixture):
    """Return a factory for mock bidi-streaming gRPC calls.

    Each call wraps a caller-supplied stream iterator and exposes async
    ``write`` and ``done_writing`` plus a configurable ``cancel``.
    """

    def create_call(stream_iterator, cancel_raises=False):
        """Create a mock gRPC call object for bidi-streaming.

        :param stream_iterator:
            The async iterator the call iterates over.
        :param cancel_raises:
            If ``True``, ``cancel()`` raises `RuntimeError`.
        """
        mock_call = mocker.MagicMock()
        mock_call.__aiter__ = lambda _: stream_iterator
        mock_call.write = mocker.AsyncMock()
        mock_call.done_writing = mocker.AsyncMock()

        if cancel_raises:
            mock_call.cancel = mocker.MagicMock(
                side_effect=RuntimeError("cancel failed")
            )
        else:
            mock_call.cancel = mocker.MagicMock()

        return mock_call

    return create_call


@pytest.fixture
def dispatching_stub(mocker: MockerFixture, async_stream, mock_grpc_call):
    """Patch `protocol.WorkerStub` with a stub whose dispatch always succeeds.

    Every call builds a fresh ack-then-result stream, so a test may
    dispatch any number of times without exhausting a shared generator.
    Returns the stub, for tests that assert on the dispatch calls.
    """

    def fresh_call(*args, **kwargs):
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("ok"))),
        )
        return mock_grpc_call(async_stream(responses))

    stub = mocker.MagicMock()
    stub.dispatch = mocker.MagicMock(side_effect=fresh_call)
    mocker.patch.object(protocol, "WorkerStub", return_value=stub)
    return stub


@pytest.fixture
def insecure_channel(mocker: MockerFixture):
    """Patch `grpc.aio.insecure_channel`, returning the patch.

    The patch records how each insecure channel was requested, and its
    ``return_value`` is the `AsyncMock` channel every insecure dispatch
    receives — see `pooled_channel` for the channel alone.

    :returns:
        The patch mock standing in for `grpc.aio.insecure_channel`.
    """
    return mocker.patch.object(
        grpc.aio, "insecure_channel", return_value=mocker.AsyncMock()
    )


@pytest.fixture
def pooled_channel(insecure_channel):
    """Patch `grpc.aio.insecure_channel` with a mock channel.

    Every insecure dispatch made while this fixture is active caches the
    returned channel in the module-level channel pool, so a test asserts
    on the pool's lifecycle through the channel's ``close``.

    :returns:
        The `AsyncMock` channel the patched factory hands out.
    """
    return insecure_channel.return_value


@pytest.fixture
def channel_per_call(mocker: MockerFixture):
    """Patch `grpc.aio.insecure_channel` to build a fresh channel per call.

    `pooled_channel` hands every dispatch one shared mock, which cannot
    tell one loop's channel from another's. This patch records each
    ``(target, channel)`` pair as it is built, under a lock because two
    loops on two threads may call the factory at once.

    :returns:
        The list of ``(target, channel)`` pairs built so far.
    """
    built: list[tuple[str, Any]] = []
    guard = threading.Lock()

    def build(target, *args, **kwargs):
        channel = mocker.AsyncMock()
        with guard:
            built.append((target, channel))
        return channel

    mocker.patch.object(grpc.aio, "insecure_channel", side_effect=build)
    return built


@pytest.fixture
def channel_pool_loop(background_loops):
    """Spawn a background loop that clears its channel-pool partition on close.

    A loop that stops with channels cached strands them for the next
    pool operation on any loop to report, which would land a warning in
    an unrelated test's log capture. This handle runs `clear_channel_pool`
    on the loop before stopping it.

    :returns:
        A `background_loops` handle.
    """
    return background_loops(teardown=clear_channel_pool)


@pytest.fixture
def mock_subscriber_pool(mocker: MockerFixture):
    """Install a mock discovery subscriber pool for the test's duration.

    :returns:
        A `ResourcePool`-shaped mock whose ``clear`` is an `AsyncMock`.
    """
    pool = mocker.MagicMock(spec=ResourcePool)
    pool.clear = mocker.AsyncMock()
    token = __subscriber_pool__.set(pool)
    try:
        yield pool
    finally:
        __subscriber_pool__.reset(token)


@pytest.fixture
def wedged_channel_pool():
    """Return a context manager that holds the calling loop's channel-pool lock.

    The one place the worker suite reaches past `ResourcePool`'s public
    API, and deliberately so: a test that needs a pool operation to
    suspend mid-flight has no public lever for it. The channel factory
    is synchronous, so no factory call can be made to block, and every
    public entry point acquires and releases the lock within a single
    await, leaving the partition's own lock as the only way to park a
    release where a test can observe it.

    The context manager yields a ``release`` callable so a test can free
    the lock at a chosen point in the block; the block's exit releases it
    if the test did not, so a failed assertion cannot leave the pool
    wedged for the rest of the session.

    :returns:
        An async context manager factory.
    """

    @asynccontextmanager
    async def wedge():
        lock = connection_module._channel_pool._partition()._lock
        await lock.acquire()
        released = False

        def release() -> None:
            nonlocal released
            if not released:
                released = True
                lock.release()

        try:
            yield release
        finally:
            release()

    return wedge


@pytest.fixture
def closable_connection(mocker: MockerFixture):
    """Patch `WorkerConnection` in the proxy module with a closable mock.

    :returns:
        A factory ``make(*, close_error=None)`` returning the mock the
        patched constructor hands out, whose ``close`` is an `AsyncMock`
        raising ``close_error`` when one is given.
    """

    def make(*, close_error: BaseException | None = None) -> MagicMock:
        connection = mocker.MagicMock(spec=WorkerConnection)
        connection.close = mocker.AsyncMock(side_effect=close_error)
        mocker.patch.object(proxy_module, "WorkerConnection", return_value=connection)
        return connection

    return make


@pytest_asyncio.fixture
async def proxy_factory():
    """Build `WorkerProxy` instances and stop any still started at teardown.

    For tests that start a proxy incidentally, so a leftover hold is
    released on the loop that took it rather than reported (see the
    root ``tests/conftest.py`` fixture ``_clear_channel_pool``); a test
    whose stop, exit, or failed start is the behavior under test
    constructs directly.
    """
    proxies: list[WorkerProxy] = []

    def factory(**kwargs) -> WorkerProxy:
        proxy = WorkerProxy(**kwargs)
        proxies.append(proxy)
        return proxy

    yield factory

    for proxy in proxies:
        if proxy.started:
            await proxy.stop()


@pytest.fixture
async def worker_proxy(mock_discovery_service, mock_grpc_stub_factory, metadata):
    """Pre-configured WorkerProxy with mock discovery and gRPC stubs.

    Yields:
        WorkerProxy instance with 2 pre-configured mock workers
    """

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

    proxy = MagicMock()
    proxy.workers = [worker1, worker2]

    yield proxy


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
