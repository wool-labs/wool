import asyncio
import atexit
import os
import pickle
import re
import shutil
import tempfile
import uuid
from collections import Counter
from contextlib import AsyncExitStack
from contextlib import ExitStack
from contextlib import asynccontextmanager
from pathlib import Path
from types import MappingProxyType
from types import SimpleNamespace

import portalocker
import pytest
import pytest_asyncio
from hypothesis import HealthCheck
from hypothesis import example
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from tests.helpers import namespace_directory
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.exceptions import DiscoveryBlockExhausted
from wool.runtime.discovery.exceptions import DiscoveryCapacityExhausted
from wool.runtime.discovery.exceptions import DiscoveryNamespaceInUse
from wool.runtime.discovery.exceptions import DiscoveryNamespaceNotFound
from wool.runtime.discovery.exceptions import DiscoveryWorkerNotFound
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.worker.metadata import WorkerMetadata
from wool.utilities.afilter import afilter


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
def oversized_metadata():
    """Provides WorkerMetadata whose serialization overflows small blocks.

    The extra payload serializes to roughly 20 KB — larger than the
    default block size and any page-rounded small block — so publishes
    against modest block sizes deterministically overflow.
    """
    return WorkerMetadata(
        uid=uuid.uuid4(),
        address="localhost:50051",
        pid=123,
        version="1.0",
        extra=MappingProxyType({"data": "x" * 20000}),
    )


@pytest.fixture
def namespace():
    """Provides unique namespace for test isolation.

    Creates a unique namespace string for each test to ensure
    namespaces don't interfere with each other.
    """
    return f"test-namespace-{uuid.uuid4()}"


@pytest.fixture
def teardown_log():
    """An ordered log of owner-teardown events.

    `atexit_recorder` appends ``"unregister"`` and `unlink_schedule`
    appends ``"unlink"`` as each call passes through, so a test holding
    both fixtures can assert their relative order — asserting that each
    happened cannot distinguish disarm-then-unlink from unlink-then-disarm.
    """
    return []


@pytest.fixture
def atexit_recorder(mocker, teardown_log):
    """Wraps atexit registration in recording pass-throughs.

    Returns a (registered, unregistered) tuple of lists capturing every
    callable that flows through atexit.register and atexit.unregister
    while the real registry stays consistent, and logs each
    unregistration to `teardown_log`.

    Tests built on this fixture deliberately pin the atexit mechanism
    rather than an observable outcome: the disarm-before-unlink ordering
    is not observable in-process any other way, and an armed handler
    only misbehaves at interpreter shutdown. The behavioral contract is
    covered cross-process in tests/integration/. A refactor away from
    atexit (to weakref.finalize, say) is expected to rewrite these
    assertions along with it.
    """
    registered = []
    unregistered = []
    real_register = atexit.register
    real_unregister = atexit.unregister

    def register(func, *args, **kwargs):
        registered.append(func)
        return real_register(func, *args, **kwargs)

    def unregister(func):
        unregistered.append(func)
        teardown_log.append("unregister")
        return real_unregister(func)

    mocker.patch.object(atexit, "register", register)
    mocker.patch.object(atexit, "unregister", unregister)
    return registered, unregistered


@pytest_asyncio.fixture
async def borrowed_publisher(namespace, metadata, atexit_recorder):
    """Yield an owner and a borrowing Publisher with independent releases.

    Yields a `SimpleNamespace` carrying the live ``owner`` and
    ``publisher`` alongside ``release_owner`` and ``release_publisher``.
    The owner is entered into an `ExitStack` of its own and the borrowing
    publisher into an `AsyncExitStack` of its own, which lets a test
    close one while the other is still bound; a nested ``with`` releases
    the publisher first. One worker is published through the borrower
    before the yield, so the registry a test inspects is never empty.

    Requests `atexit_recorder` so the recorder is in place before either
    participant arms a fallback: a test that reads the recorded lists
    would otherwise depend on the order pytest happens to build two
    independent fixtures in.

    Both releases are idempotent and teardown runs them again in order,
    so a test may drive either, both, or neither.
    """
    owner_stack = ExitStack()
    borrower_stack = AsyncExitStack()
    try:
        owner = owner_stack.enter_context(LocalDiscovery(namespace))
        publisher = await borrower_stack.enter_async_context(
            LocalDiscovery.Publisher(namespace)
        )
        await publisher.publish("worker-added", metadata)
        yield SimpleNamespace(
            owner=owner,
            publisher=publisher,
            release_owner=owner_stack.close,
            release_publisher=borrower_stack.aclose,
        )
    finally:
        await borrower_stack.aclose()
        owner_stack.close()


@pytest.fixture
def unlink_schedule(mocker, namespace, teardown_log):
    """Patches file removal with a schedule-driven wrapper and returns
    the schedule list. Each removal is logged to `teardown_log`.

    Only removals inside this test's namespace are wrapped, so unrelated
    removals anywhere in the interpreter pass straight through. A test
    deriving per-example namespaces from this one is covered too, since
    the scope is every directory whose name carries the namespace.
    Removals the owner resolves against its claim descriptor name no
    directory at all, so that descriptor is matched against the
    directories it could be holding.

    Each wrapped call performs the real removal — so no file leaks — then
    consumes one schedule entry and raises it when the entry is an
    exception, simulating an external remover or a hostile filesystem. An
    empty or exhausted schedule means the removal passes through
    untouched. A one-shot failure is therefore a one-entry schedule, and
    a generated failure pattern is a longer one. Patching once per test
    (rather than per failure or per Hypothesis example) avoids stacking
    wrappers.
    """
    schedule: list[Exception | None] = []
    real_unlink = os.unlink
    root = namespace_directory(namespace).parent
    prefix = f"wool-{namespace}"

    def scoped(path, dir_fd):
        if dir_fd is None:
            return Path(path).parent.name.startswith(prefix)
        try:
            holder = os.fstat(dir_fd)
        except OSError:
            return False
        for candidate in root.glob(f"{prefix}*"):
            try:
                if os.path.samestat(holder, os.stat(candidate)):
                    return True
            except OSError:
                continue
        return False

    def unlink(path, *, dir_fd=None, **kwargs):
        if not scoped(path, dir_fd):
            return real_unlink(path, dir_fd=dir_fd, **kwargs)
        teardown_log.append("unlink")
        real_unlink(path, dir_fd=dir_fd, **kwargs)
        if schedule and (error := schedule.pop(0)) is not None:
            raise error

    mocker.patch.object(os, "unlink", unlink)
    return schedule


@pytest.fixture
def held_lock(mocker):
    """Patch portalocker.lock to report the lock is permanently held.

    Every acquisition attempt raises LockException, so a publish resolves
    only through its lock_timeout deadline, never by acquiring.
    """

    def _held(fh, flags):
        raise portalocker.LockException("Lock held")

    mocker.patch.object(portalocker, "lock", side_effect=_held)


@pytest.fixture
def contending_lock(mocker):
    """Return a factory patching portalocker.lock to fail its first n calls.

    The returned callable installs a side effect that raises LockException
    on the first n acquisition attempts and then delegates to the real
    lock, returning the mock so a test can assert on its call count.
    """
    real_lock = portalocker.lock

    def _make(n):
        calls = {"count": 0}

        def _contend(fh, flags):
            calls["count"] += 1
            if calls["count"] <= n:
                raise portalocker.LockException("Lock held")
            real_lock(fh, flags)

        return mocker.patch.object(portalocker, "lock", side_effect=_contend)

    return _make


@asynccontextmanager
async def _collecting(subscriber, collector):
    """Run collector over subscriber as a task, cancelling it on exit.

    Yields the collect task so a test can await its side effects; the
    task is cancelled and awaited on exit regardless of outcome.
    """
    task = asyncio.create_task(collector(subscriber))
    try:
        yield task
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


#: Same-namespace lifecycle forests: each node is one claim on the
#: namespace; siblings model teardown+respawn generations reusing it and
#: nesting models a claim made against a live owner, which that owner
#: rejects. Referenced by ``@given`` at class-definition time, so it must
#: precede the test classes.
_LIFECYCLE_FORESTS = st.recursive(
    st.just([]),
    lambda children: st.lists(children, max_size=3),
    max_leaves=6,
)


def _worker():
    """Return WorkerMetadata for a distinct worker."""
    return WorkerMetadata(
        uid=uuid.uuid4(), address="localhost:50051", pid=12345, version="1.0.0"
    )


def _rejected_claim_is_raised(namespace):
    """Return whether a claim against the live owner of ``namespace`` is rejected."""
    try:
        with LocalDiscovery(namespace):
            return False
    except DiscoveryNamespaceInUse:
        return True


def _rejected_claim(namespace):
    """Return the `DiscoveryNamespaceInUse` a claim against a live owner raises."""
    with LocalDiscovery(namespace):
        try:
            with LocalDiscovery(namespace):
                pass
        except DiscoveryNamespaceInUse as error:
            return error
    raise AssertionError(f"second claim on {namespace!r} was not rejected")


class TestLocalDiscovery:
    """Tests for LocalDiscovery class.

    Fully qualified name: wool.runtime.discovery.local.LocalDiscovery
    """

    def test___init___without_namespace(self):
        """Test LocalDiscovery default namespace generation.

        Given:
            No arguments
        When:
            LocalDiscovery is instantiated
        Then:
            It should auto-generate a namespace starting with
            "workerpool-".
        """
        # Act
        discovery = LocalDiscovery()

        # Assert
        assert discovery.namespace.startswith("workerpool-")

    def test___init___with_custom_namespace(self):
        """Test LocalDiscovery custom namespace.

        Given:
            A custom namespace string
        When:
            LocalDiscovery is instantiated
        Then:
            It should return the provided namespace.
        """
        # Act
        discovery = LocalDiscovery("my-namespace")

        # Assert
        assert discovery.namespace == "my-namespace"

    @pytest.mark.parametrize("capacity", [0, -1])
    def test___init___should_raise_when_capacity_below_one(self, capacity):
        """Test LocalDiscovery rejects a capacity below one.

        Given:
            A capacity of zero or a negative capacity
        When:
            LocalDiscovery is instantiated
        Then:
            It should raise ValueError, since a registry with fewer than one
            slot can never admit a worker.
        """
        # Act & assert
        with pytest.raises(ValueError, match="Expected capacity of at least 1"):
            LocalDiscovery("ns", capacity=capacity)

    @pytest.mark.parametrize(
        "bad",
        [
            "a/b",
            "../escape",
            "probeX/../victim",
            ".",
            "..",
            "",
            "nul\x00byte",
            "x" * 256,
        ],
    )
    def test___init___should_raise_when_namespace_leaves_its_root(self, bad):
        """Test LocalDiscovery rejects a namespace that is not one path component.

        Given:
            A namespace carrying a path separator, a relative-path
            element, a NUL, nothing at all, or more characters than a
            directory name can hold
        When:
            LocalDiscovery is instantiated
        Then:
            It should raise ValueError, since the namespace is
            interpolated into a directory name and any of these would
            claim, write and unlink outside the module's own root.
        """
        # Act & assert
        with pytest.raises(ValueError, match="namespace"):
            LocalDiscovery(bad)

    def test___init___should_generate_a_namespace_only_when_none_is_given(self):
        """Test only an omitted namespace is replaced by a generated one.

        Given:
            No namespace, and separately an empty namespace
        When:
            LocalDiscovery is instantiated with each
        Then:
            It should generate a unique name for the omitted one and
            reject the empty one, rather than silently renaming a
            namespace the caller did supply.
        """
        # Act
        generated = LocalDiscovery()

        # Assert
        assert generated.namespace.startswith("workerpool-")
        with pytest.raises(ValueError, match="non-empty namespace"):
            LocalDiscovery("")

    @pytest.mark.parametrize("block_size", [0, -1, 1, 4])
    def test___init___should_raise_when_block_size_within_the_prefix(self, block_size):
        """Test LocalDiscovery rejects a block size the prefix alone fills.

        Given:
            A block_size no larger than the 4-byte length prefix every
            block spends before its payload
        When:
            LocalDiscovery is instantiated
        Then:
            It should raise ValueError, since such a block leaves no room
            for metadata at all and would make every publish raise
            permanently.
        """
        # Act & assert
        with pytest.raises(
            ValueError, match="Expected block size greater than the 4-byte"
        ):
            LocalDiscovery("ns", block_size=block_size)

    @given(block_size=st.integers())
    @settings(max_examples=50)
    @example(block_size=5)
    @example(block_size=4)
    @example(block_size=1)
    @example(block_size=0)
    @example(block_size=-1)
    def test___init___should_validate_block_size_across_domain(self, block_size):
        """Test LocalDiscovery validates block_size across the integer domain.

        Given:
            Any integer block_size.
        When:
            LocalDiscovery is instantiated with it.
        Then:
            It should raise ValueError naming the offending value exactly
            when the value does not exceed the length prefix, and
            construct successfully for every larger value.
        """
        # Act & assert
        if block_size <= 4:
            with pytest.raises(
                ValueError,
                match=(
                    f"Expected block size greater than the 4-byte length prefix, "
                    f"got {block_size}"
                ),
            ):
                LocalDiscovery("ns", block_size=block_size)
        else:
            discovery = LocalDiscovery("ns", block_size=block_size)
            assert discovery.namespace == "ns"

    def test___init___should_raise_when_lock_timeout_negative(self):
        """Test LocalDiscovery rejects a negative lock timeout.

        Given:
            A negative lock_timeout
        When:
            LocalDiscovery is instantiated
        Then:
            It should raise ValueError at construction.
        """
        # Act & assert
        with pytest.raises(ValueError, match="Lock timeout must be non-negative"):
            LocalDiscovery("ns", lock_timeout=-1)

    def test___hash___with_same_namespace(self):
        """Test hash equality for same namespace.

        Given:
            Two LocalDiscovery instances with the same namespace.
        When:
            Their hashes are compared.
        Then:
            It should produce equal hashes.
        """
        # Arrange
        a = LocalDiscovery("shared-ns")
        b = LocalDiscovery("shared-ns")

        # Act & assert
        assert hash(a) == hash(b)

    def test___hash___with_different_namespace(self):
        """Test hash inequality for different namespaces.

        Given:
            Two LocalDiscovery instances with different namespaces.
        When:
            Their hashes are compared.
        Then:
            It should produce different hashes.
        """
        # Arrange
        a = LocalDiscovery("ns-a")
        b = LocalDiscovery("ns-b")

        # Act & assert
        assert hash(a) != hash(b)

    def test___eq___with_same_namespace(self):
        """Test equality for same namespace.

        Given:
            Two LocalDiscovery instances with the same namespace.
        When:
            They are compared with ==.
        Then:
            It should return True.
        """
        # Arrange
        a = LocalDiscovery("shared-ns")
        b = LocalDiscovery("shared-ns")

        # Act & assert
        assert a == b

    def test___eq___with_different_namespace(self):
        """Test inequality for different namespaces.

        Given:
            Two LocalDiscovery instances with different namespaces.
        When:
            They are compared with ==.
        Then:
            It should return False.
        """
        # Arrange
        a = LocalDiscovery("ns-a")
        b = LocalDiscovery("ns-b")

        # Act & assert
        assert a != b

    def test___eq___with_non_local_discovery(self):
        """Test equality with a non-LocalDiscovery object.

        Given:
            A LocalDiscovery instance and a non-LocalDiscovery object.
        When:
            They are compared with ==.
        Then:
            It should not be equal.
        """
        # Act & assert
        assert LocalDiscovery("ns") != "not-a-discovery"

    def test_publisher_with_default_instance(self, namespace):
        """Test publisher property returns Publisher with matching namespace.

        Given:
            A LocalDiscovery instance
        When:
            publisher property is accessed
        Then:
            It should return a Publisher with matching namespace.
        """
        # Arrange
        discovery = LocalDiscovery(namespace)

        # Act
        publisher = discovery.publisher

        # Assert
        assert isinstance(publisher, LocalDiscovery.Publisher)
        assert publisher.namespace == namespace

    @pytest.mark.asyncio
    async def test_publisher_should_propagate_lock_timeout(
        self, namespace, metadata, held_lock
    ):
        """Test the publisher property plumbs lock_timeout to publish.

        Given:
            A LocalDiscovery constructed with a zero-second lock_timeout
            and a file lock permanently held by another process
        When:
            A publisher obtained from the publisher property publishes
        Then:
            It should raise TimeoutError, proving lock_timeout reaches the
            nested Publisher's lock acquisition.
        """
        # Act & assert
        with LocalDiscovery(namespace, lock_timeout=0) as discovery:
            async with discovery.publisher as publisher:
                with pytest.raises(TimeoutError):
                    await publisher.publish("worker-added", metadata)

    @pytest.mark.asyncio
    async def test_publisher_should_propagate_block_size(
        self, namespace, oversized_metadata
    ):
        """Test the publisher property plumbs block_size to publish.

        Given:
            A LocalDiscovery constructed with a block_size larger than
            the Publisher default and worker metadata whose serialization
            exceeds any default-sized block
        When:
            A publisher obtained from the publisher property publishes it
        Then:
            It should publish successfully, proving the constructor's
            block_size governs the nested Publisher — a default-sized
            block could not hold the metadata.
        """
        # Act & assert — success is only possible if block_size propagated
        with LocalDiscovery(namespace, block_size=65536) as discovery:
            async with discovery.publisher as publisher:
                await publisher.publish("worker-added", oversized_metadata)

    def test_subscriber_with_default_instance(self, namespace):
        """Test subscriber property returns Subscriber instance.

        Given:
            A LocalDiscovery instance
        When:
            subscriber property is accessed
        Then:
            It should return a Subscriber.
        """
        # Arrange
        discovery = LocalDiscovery(namespace)

        # Act
        subscriber = discovery.subscriber

        # Assert
        assert isinstance(subscriber, DiscoverySubscriberLike)

    @pytest.mark.asyncio
    async def test_subscribe_with_default_filter(self, namespace):
        """Test subscribe() propagates the constructor's default filter.

        Given:
            A LocalDiscovery with a default filter
        When:
            subscribe() is called without a filter
        Then:
            It should use the default filter for the event stream.
        """

        # Arrange
        def predicate(w):
            return w.address == "localhost:50051"

        worker_match = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
        )
        worker_no_match = WorkerMetadata(
            uid=uuid.uuid4(),
            address="otherhost:9999",
            pid=456,
            version="1.0",
        )

        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()

        with LocalDiscovery(namespace, filter=predicate) as discovery:
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = discovery.subscribe(poll_interval=0.05)

            async with publisher:
                task = asyncio.create_task(collect(subscriber))
                await asyncio.sleep(0.05)
                await publisher.publish("worker-added", worker_match)
                await publisher.publish("worker-added", worker_no_match)

                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
                await asyncio.sleep(0.1)

                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Assert
        assert len(events) >= 1
        assert all(e.metadata.address == "localhost:50051" for e in events)

    @pytest.mark.asyncio
    async def test_subscribe_with_explicit_filter(self, namespace):
        """Test subscribe(filter=predicate) overrides the default filter.

        Given:
            A LocalDiscovery instance
        When:
            subscribe(filter=predicate) is called
        Then:
            It should use the provided filter for the event stream.
        """

        # Arrange
        def predicate(w):
            return w.address == "localhost:50051"

        worker_match = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
        )
        worker_no_match = WorkerMetadata(
            uid=uuid.uuid4(),
            address="otherhost:9999",
            pid=456,
            version="1.0",
        )

        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()

        with LocalDiscovery(namespace) as discovery:
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = discovery.subscribe(filter=predicate, poll_interval=0.05)

            async with publisher:
                task = asyncio.create_task(collect(subscriber))
                await asyncio.sleep(0.05)
                await publisher.publish("worker-added", worker_match)
                await publisher.publish("worker-added", worker_no_match)

                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
                await asyncio.sleep(0.1)

                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Assert
        assert len(events) >= 1
        assert all(e.metadata.address == "localhost:50051" for e in events)

    @pytest.mark.asyncio
    async def test_subscribe_with_custom_poll_interval(self, namespace):
        """Test subscribe(poll_interval=...) uses the specified interval.

        Given:
            A LocalDiscovery instance
        When:
            subscribe(poll_interval=1.0) is called
        Then:
            It should discover workers via polling within the interval.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
        )

        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        with LocalDiscovery(namespace) as discovery:
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = discovery.subscribe(poll_interval=0.1)

            async with publisher:
                await publisher.publish("worker-added", worker)

                task = asyncio.create_task(collect(subscriber))
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

    def test___enter___and___exit___lifecycle(self, namespace):
        """Test LocalDiscovery context manager lifecycle.

        Given:
            A LocalDiscovery instance
        When:
            Used as a context manager via with statement
        Then:
            It should yield the same instance on entry and complete
            the exit cleanly.
        """
        # Arrange
        discovery = LocalDiscovery(namespace)

        # Act & assert
        with discovery as ctx:
            assert ctx is discovery

    def test___enter___should_raise_when_namespace_already_owned(self, namespace):
        """Test a second claim on a live namespace is rejected.

        Given:
            A LocalDiscovery that owns a namespace
        When:
            A second LocalDiscovery enters the same namespace via with
        Then:
            It should raise DiscoveryNamespaceInUse naming the
            namespace.
        """
        # Arrange
        with LocalDiscovery(namespace):
            # Act & assert
            with pytest.raises(DiscoveryNamespaceInUse) as excinfo:
                with LocalDiscovery(namespace):
                    pass

            assert excinfo.value.namespace == namespace

    @pytest.mark.asyncio
    async def test___enter___should_preserve_registry_when_second_claim_rejected(
        self, namespace, metadata
    ):
        """Test a rejected claim leaves the incumbent's registry intact.

        Given:
            An owner's namespace already containing a published worker
        When:
            A second LocalDiscovery is entered on the same namespace
            and rejected, and the owner's subscriber then iterates
        Then:
            It should yield the worker-added event published before the
            rejected claim, proving the rejection neither reinitialized
            nor reclaimed the incumbent's registry.
        """
        # Arrange
        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        with LocalDiscovery(namespace) as owner:
            publisher = LocalDiscovery.Publisher(namespace)
            async with publisher:
                await publisher.publish("worker-added", metadata)

                # Act
                with pytest.raises(DiscoveryNamespaceInUse):
                    with LocalDiscovery(namespace):
                        pass

                subscriber = owner.subscribe(poll_interval=0.05)
                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker not discovered after the rejected claim")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        assert len(events) == 1
        assert events[0].type == "worker-added"
        assert events[0].metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test___enter___should_recreate_registry_when_namespace_reused(
        self, namespace, metadata
    ):
        """Test a namespace remains fully usable after rapid teardowns.

        Given:
            A namespace already cycled through several rapid owner
            enter/exit lifecycles, leaving no registry behind
        When:
            A fresh LocalDiscovery enters the namespace and a worker
            is published and subscribed to
        Then:
            It should yield the worker-added event, proving each
            teardown freed the namespace for a functional respawn.
        """
        # Arrange
        for _ in range(3):
            with LocalDiscovery(namespace):
                pass

        # Arrange — the last teardown freed the name: with no owner
        # holding it, a publisher's bind raises DiscoveryNamespaceNotFound.
        # Without this probe a stale surviving registry would satisfy the
        # roundtrip below just as well as a recreated one.
        probe = LocalDiscovery.Publisher(namespace)
        with pytest.raises(DiscoveryNamespaceNotFound):
            async with probe:
                pass

        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        # Act
        with LocalDiscovery(namespace) as discovery:
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = discovery.subscribe(poll_interval=0.05)
            async with publisher:
                await publisher.publish("worker-added", metadata)

                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker not discovered after namespace reuse")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        assert len(events) == 1
        assert events[0].type == "worker-added"
        assert events[0].metadata.uid == metadata.uid

    def test___enter___should_not_register_atexit_fallback_when_claim_rejected(
        self, namespace, atexit_recorder
    ):
        """Test a rejected claim performs no atexit traffic.

        Given:
            An owner already holding a namespace, with atexit
            registration wrapped in recording pass-throughs
        When:
            A second LocalDiscovery is entered on the same namespace
            and rejected
        Then:
            It should neither register nor unregister any atexit
            fallback — a rejected claim owns nothing to reclaim.
        """
        # Arrange
        registered, unregistered = atexit_recorder

        with LocalDiscovery(namespace):
            registrations = len(registered)

            # Act
            with pytest.raises(DiscoveryNamespaceInUse):
                with LocalDiscovery(namespace):
                    pass

            # Assert
            assert len(registered) == registrations
            assert unregistered == []

    def test___enter___should_raise_when_the_same_instance_is_reentered(self, namespace):
        """Test a second entry on one instance is rejected.

        Given:
            A LocalDiscovery instance already entered via with
        When:
            The same instance is entered a second time
        Then:
            It should raise RuntimeError.
        """
        # Arrange
        discovery = LocalDiscovery(namespace)

        # Act & assert
        with discovery:
            with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
                with discovery:
                    pass

    def test___enter___should_admit_distinct_instances_sharing_a_namespace(
        self, namespace
    ):
        """Test the single-use guard is per instance, not per namespace.

        Given:
            A LocalDiscovery instance that has been entered and exited,
            and a second instance equal to it by namespace
        When:
            The second instance is entered
        Then:
            It should enter successfully, the guard binding to the
            instance rather than to the namespace it compares equal on.
        """
        # Arrange
        first = LocalDiscovery(namespace)
        second = LocalDiscovery(namespace)
        assert first == second and hash(first) == hash(second)
        with first:
            pass

        # Act & assert
        with second as entered:
            assert entered is second

    def test___enter___should_name_the_namespace_when_the_claim_is_rejected(
        self, namespace
    ):
        """Test a rejected claim reports the namespace it was rejected on.

        Given:
            Two distinct namespaces, each claimable by a fresh owner
        When:
            A claim against a live owner is rejected on each namespace
        Then:
            It should name the namespace claimed in both the field and
            the message, and carry no instruction to remove anything —
            a namespace is in use only while its owner lives, so there
            is nothing for an operator to reclaim by hand.
        """
        # Arrange
        other = f"{namespace}-other"

        # Act
        rejected = _rejected_claim(namespace)
        separate = _rejected_claim(other)

        # Assert
        assert rejected.namespace == namespace
        assert separate.namespace == other
        assert namespace in str(rejected)
        assert "remove" not in str(rejected)

    @pytest.mark.asyncio
    async def test___enter___should_claim_the_namespace_when_its_owner_is_gone(
        self, namespace, metadata
    ):
        """Test a claim replaces the residue an owner that is gone left.

        Given:
            A namespace whose registry, notification file and worker
            block are left in place with no live process holding the
            namespace, as a killed owner leaves them
        When:
            A fresh owner claims the namespace at a capacity of one and
            publishes a worker of its own
        Then:
            It should claim it and serve a registry of its own capacity,
            a second publish exhausting it — the stranded registration
            is replaced rather than adopted, so it occupies no slot.
        """
        # Arrange — a namespace snapshotted while live and restored once
        # its owner has exited, which is the residue a kill leaves.
        directory = namespace_directory(namespace)
        snapshot = directory.with_name(f"{directory.name}-snapshot")
        with LocalDiscovery(namespace):
            async with LocalDiscovery.Publisher(namespace) as publisher:
                await publisher.publish("worker-added", metadata)
                shutil.copytree(directory, snapshot)
        shutil.copytree(snapshot, directory)
        shutil.rmtree(snapshot)

        # Act & assert
        try:
            with LocalDiscovery(namespace, capacity=1) as successor:
                assert successor.namespace == namespace
                async with LocalDiscovery.Publisher(namespace) as publisher:
                    await publisher.publish("worker-added", _worker())
                    with pytest.raises(DiscoveryCapacityExhausted):
                        await publisher.publish("worker-added", _worker())
        finally:
            # The restored residue holds a block no live publisher owns,
            # which outlives the successor and keeps its directory.
            shutil.rmtree(directory, ignore_errors=True)

    @pytest.mark.asyncio
    async def test___exit___should_remove_thenamespace_directory(
        self, namespace, metadata
    ):
        """Test a fully torn down namespace leaves nothing behind.

        Given:
            An owner whose borrowing publisher published a worker, so
            the namespace holds a registry, a notification file and the
            worker's metadata block
        When:
            The publisher and then the owner exit
        Then:
            It should leave no namespace directory, so a host that
            creates many short-lived namespaces accumulates no residue.
        """
        # Arrange
        directory = namespace_directory(namespace)

        # Act
        with LocalDiscovery(namespace):
            async with LocalDiscovery.Publisher(namespace) as publisher:
                await publisher.publish("worker-added", metadata)
            assert directory.exists()

        # Assert
        assert not directory.exists()

    @pytest.mark.asyncio
    async def test___exit___should_leave_the_directory_to_an_orphaned_publisher(
        self, namespace, borrowed_publisher
    ):
        """Test the last orphaned publisher removes the namespace directory.

        Given:
            An owner and a borrowing publisher holding a worker's block
        When:
            The owner exits while the borrower still holds that block,
            and the orphaned borrower then exits
        Then:
            It should leave the directory in place while the block lives
            in it, and remove it with that block — the borrower, not the
            owner, reclaims what it created last.
        """
        # Arrange
        directory = namespace_directory(borrowed_publisher.owner.namespace)

        # Act
        borrowed_publisher.release_owner()

        # Assert — the block outlives the owner, so its home does too
        assert directory.exists()

        # Act
        await borrowed_publisher.release_publisher()

        # Assert
        assert not directory.exists()

    @pytest.mark.asyncio
    async def test_publish_should_not_recreate_thenamespace_directory(
        self, namespace, metadata
    ):
        """Test a publish against a reclaimed namespace creates nothing.

        Given:
            A publisher bound to a namespace whose owner has since
            exited and removed it
        When:
            The publisher publishes a worker
        Then:
            It should raise DiscoveryNamespaceNotFound and leave no
            namespace directory — a borrower recreating one would
            resurrect the residue the owner's exit just reclaimed.
        """
        # Arrange
        directory = namespace_directory(namespace)
        async with AsyncExitStack() as stack:
            with LocalDiscovery(namespace):
                publisher = await stack.enter_async_context(
                    LocalDiscovery.Publisher(namespace)
                )

            # Act & assert
            with pytest.raises(DiscoveryNamespaceNotFound):
                await publisher.publish("worker-added", metadata)
            assert not directory.exists()

    def test___enter___should_retry_when_the_directory_is_replaced_mid_claim(
        self, namespace, mocker
    ):
        """Test a claim on a replaced directory is retried, not accepted.

        Given:
            A namespace whose directory is replaced between being opened
            and being locked, as a departing owner's reclaim does, so
            the claim is held on a directory the namespace no longer
            resolves to
        When:
            A fresh owner claims the namespace
        Then:
            It should discard that claim and retry until it holds the
            directory the namespace names — a claim on an unlinked
            directory would let a second owner claim its replacement.
        """
        # Arrange — report the first claim as holding a stale directory
        real_samestat = os.path.samestat
        checks = []

        def samestat(claimed, named):
            checks.append(None)
            if len(checks) == 1:
                return False
            return real_samestat(claimed, named)

        mocker.patch.object(os.path, "samestat", samestat)

        # Act
        with LocalDiscovery(namespace) as discovery:
            # Assert
            assert discovery.namespace == namespace
            assert len(checks) > 1
            assert _rejected_claim_is_raised(namespace)

    @pytest.mark.parametrize("vanishing", ["directory", "registry.tmp"])
    def test___enter___should_retry_when_the_directory_vanishes_mid_claim(
        self, namespace, mocker, vanishing
    ):
        """Test a directory removed mid-claim is recreated and reclaimed.

        Given:
            A namespace whose directory is removed just as the claim
            opens it, or just as the claim writes the registry into it,
            as a departing owner's reclaim does
        When:
            A fresh owner claims the namespace
        Then:
            It should recreate the directory and claim the namespace in
            both cases, rather than failing a claim on a transient race.
        """
        # Arrange
        directory = namespace_directory(namespace)
        target = directory if vanishing == "directory" else directory / vanishing
        real_open = os.open
        vanished = []

        def flaky_open(path, flags, *args, **kwargs):
            if Path(path) == target and not vanished:
                vanished.append(None)
                raise FileNotFoundError(2, "No such file or directory", str(path))
            return real_open(path, flags, *args, **kwargs)

        mocker.patch.object(os, "open", flaky_open)

        # Act
        with LocalDiscovery(namespace) as discovery:
            # Assert
            assert discovery.namespace == namespace
            assert vanished

    def test___enter___should_release_the_claim_when_the_registry_cannot_be_written(
        self, namespace, mocker
    ):
        """Test a claim that cannot create its registry is released.

        Given:
            A namespace whose registry cannot be written, as an
            exhausted filesystem leaves it
        When:
            An owner claims the namespace
        Then:
            It should propagate the failure, leave no namespace
            directory, and hold no claim — a claim retained after a
            failed entry would lock the namespace out for the life of
            the process.
        """
        # Arrange
        directory = namespace_directory(namespace)
        mocker.patch.object(os, "pwrite", side_effect=OSError(28, "No space left"))

        # Act & assert
        with pytest.raises(OSError, match="No space left"):
            with LocalDiscovery(namespace):
                pass

        # Assert — nothing of the namespace survives the failed claim.
        # The fault is lifted first: it would break the successor's own
        # staging rather than the claim under test.
        assert not directory.exists()
        mocker.stopall()
        with LocalDiscovery(namespace) as successor:
            assert successor.namespace == namespace

    def test___exit___should_warn_when_the_directory_cannot_be_removed(
        self, namespace, mocker
    ):
        """Test an unexpected directory-removal failure surfaces as a warning.

        Given:
            An owner whose namespace directory cannot be removed, a
            failure with no benign explanation
        When:
            The owner exits via with
        Then:
            It should emit a ResourceWarning naming the directory it
            could not remove, so an operator can find the leak, rather
            than raising out of a teardown.
        """
        # Arrange
        directory = namespace_directory(namespace)
        mocker.patch.object(os, "rmdir", side_effect=OSError(13, "Permission denied"))

        # Act & assert
        try:
            with pytest.warns(ResourceWarning, match=re.escape(str(directory))):
                with LocalDiscovery(namespace):
                    pass
        finally:
            # The failure this arranges is what left the directory.
            mocker.stopall()
            shutil.rmtree(directory, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_publish_should_complete_when_the_notification_file_vanished(
        self, namespace, metadata
    ):
        """Test a publish whose notification file is gone still registers.

        Given:
            An owner, a borrowing publisher bound to its registry, and a
            notification file removed out from under them — the window
            in which an owner reclaims a namespace mid-publish
        When:
            The publisher publishes a worker
        Then:
            It should register the worker and leave the notification
            file absent, a publisher that recreated it resurrecting
            residue its owner had reclaimed.
        """
        # Arrange
        directory = namespace_directory(namespace)
        with LocalDiscovery(namespace) as discovery:
            async with LocalDiscovery.Publisher(namespace) as publisher:
                notification = directory / "notify"
                assert notification.exists()
                notification.unlink()

                # Act
                await publisher.publish("worker-added", metadata)

                # Assert
                assert not notification.exists()
                events = []
                async for event in discovery.subscribe(poll_interval=0.05):
                    events.append(event)
                    break
                assert [event.metadata.uid for event in events] == [metadata.uid]

    def test___enter___should_raise_when_a_rejected_instance_is_reentered(
        self, namespace
    ):
        """Test a rejected claim still spends the instance's single use.

        Given:
            A LocalDiscovery whose claim was rejected while an incumbent
            held the namespace, and an incumbent that has since exited
        When:
            That same instance is entered again on the now-free
            namespace, and then a fresh instance is entered
        Then:
            It should raise RuntimeError for the rejected instance, since
            any entry attempt spends it, and a fresh instance should
            claim the namespace.
        """
        # Arrange
        rejected = LocalDiscovery(namespace)
        with LocalDiscovery(namespace):
            with pytest.raises(DiscoveryNamespaceInUse):
                with rejected:
                    pass

        # Act & assert — the namespace is free, but the instance is not
        with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
            with rejected:
                pass

        # Act & assert — a fresh instance claims it
        with LocalDiscovery(namespace) as claimed:
            assert claimed.namespace == namespace

    def test___enter___should_raise_when_a_rejected_instance_retries_under_owner(
        self, namespace
    ):
        """Test a rejected instance is refused while the incumbent still holds.

        Given:
            A LocalDiscovery whose claim was rejected, and the incumbent
            that rejected it still holding the namespace
        When:
            That same instance is entered again, before the incumbent
            has exited
        Then:
            It should raise RuntimeError: the rejected entry already
            spent the instance.
        """
        # Arrange
        with LocalDiscovery(namespace):
            rejected = LocalDiscovery(namespace)
            with pytest.raises(DiscoveryNamespaceInUse):
                with rejected:
                    pass

            # Act & assert — the incumbent is still live, and the guard
            # is still what refuses
            with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
                with rejected:
                    pass

    def test___enter___should_raise_when_an_exited_instance_is_reentered(
        self, namespace
    ):
        """Test a spent instance is refused even once its namespace is free.

        Given:
            A LocalDiscovery that has been entered and exited, leaving
            its namespace claimable
        When:
            The same instance is entered a second time
        Then:
            It should raise RuntimeError: the guard binds to the
            instance.
        """
        # Arrange
        discovery = LocalDiscovery(namespace)
        with discovery:
            pass

        # Act & assert
        with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
            with discovery:
                pass

    @given(ops=st.lists(st.sampled_from(["claim", "release", "borrow"]), max_size=12))
    @settings(
        max_examples=50,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @pytest.mark.asyncio
    async def test___enter___should_admit_one_owner_across_operation_sequences(
        self, namespace, ops
    ):
        """Test the single-owner invariant over arbitrary operation orders.

        Given:
            Any sequence of claim, release and borrow operations against
            one namespace
        When:
            The sequence is executed against a model tracking whether an
            owner is currently live
        Then:
            A claim should succeed exactly when no owner is live and
            raise DiscoveryNamespaceInUse otherwise, and a Publisher
            borrower should bind exactly when an owner is live and raise
            DiscoveryNamespaceNotFound otherwise.
        """
        # Arrange — a per-example namespace, so a registry stranded by
        # one example cannot decide the next. Borrowing goes through
        # `Publisher`, which binds on every entry; an iteration of a
        # pooled `Subscriber` binds only when it starts the shared
        # subscription (see `LocalDiscovery.Subscriber`).
        example_ns = f"{namespace}-{uuid.uuid4().hex[:8]}"
        generation = ExitStack()
        owned = False

        # Act & assert
        try:
            for op in ops:
                if op == "claim" and owned:
                    with pytest.raises(DiscoveryNamespaceInUse):
                        with LocalDiscovery(example_ns):
                            pass
                elif op == "claim":
                    generation.enter_context(LocalDiscovery(example_ns))
                    owned = True
                elif op == "release":
                    generation.close()
                    generation = ExitStack()
                    owned = False
                elif owned:
                    async with LocalDiscovery.Publisher(example_ns):
                        pass
                else:
                    with pytest.raises(DiscoveryNamespaceNotFound):
                        async with LocalDiscovery.Publisher(example_ns):
                            pass
        finally:
            generation.close()

    def test___exit___should_disarm_atexit_fallback_when_reentry_is_rejected(
        self, namespace, atexit_recorder
    ):
        """Test a rejected re-entry leaves no fallback armed.

        Given:
            A LocalDiscovery instance whose second entry was rejected,
            with atexit registration wrapped in recording pass-throughs
        When:
            The instance exits the with block it did enter
        Then:
            It should unregister every atexit fallback it registered.
        """
        # Arrange
        registered, unregistered = atexit_recorder
        discovery = LocalDiscovery(namespace)

        # Act
        with discovery:
            with pytest.raises(RuntimeError):
                with discovery:
                    pass

        # Assert
        assert len(registered) == 1
        assert registered == unregistered

    @pytest.mark.asyncio
    async def test___exit___should_remove_registry_when_owner_exits(self, namespace):
        """Test owner exit removes the registry file.

        Given:
            An owner LocalDiscovery that entered a namespace
        When:
            The owner exits via with and a Publisher then publishes
            to that namespace
        Then:
            It should raise DiscoveryNamespaceNotFound from the
            publisher's bind — the registry no longer exists.
        """
        # Arrange
        with LocalDiscovery(namespace):
            pass
        publisher = LocalDiscovery.Publisher(namespace)

        # Act & assert
        with pytest.raises(DiscoveryNamespaceNotFound):
            async with publisher:
                pass

    def test___exit___should_disarm_atexit_fallback_when_exit_is_clean(
        self, namespace, atexit_recorder
    ):
        """Test a clean owner exit pairs the atexit fallback exactly.

        Given:
            An owner LocalDiscovery with atexit registration wrapped
            in recording pass-throughs and no fault injected
        When:
            The owner enters and exits via with
        Then:
            It should register exactly one shutdown fallback and
            unregister that same callable.
        """
        # Arrange
        registered, unregistered = atexit_recorder

        # Act
        with LocalDiscovery(namespace):
            pass

        # Assert
        assert registered == unregistered
        assert len(registered) == 1

    @pytest.mark.asyncio
    async def test___exit___should_remove_registry_when_body_raises(self, namespace):
        """Test exceptional exit still tears the registry down.

        Given:
            An owner LocalDiscovery whose with body raises ValueError
        When:
            The exception unwinds the with statement
        Then:
            It should propagate the ValueError unsuppressed while
            still removing the registry, so a subsequent borrower's
            bind raises DiscoveryNamespaceNotFound.
        """
        # Arrange
        publisher = LocalDiscovery.Publisher(namespace)

        # Act
        with pytest.raises(ValueError, match="boom"):
            with LocalDiscovery(namespace):
                raise ValueError("boom")

        # Assert — teardown still removed the registry
        with pytest.raises(DiscoveryNamespaceNotFound):
            async with publisher:
                pass

    @pytest.mark.asyncio
    async def test___exit___should_free_the_namespace_when_a_borrower_outlives_it(
        self, namespace, borrowed_publisher
    ):
        """Test a live borrower does not hold the namespace open.

        Given:
            An owner holding a namespace and a Publisher that borrowed
            its registry and published through it
        When:
            The owner exits while that borrower is still bound
        Then:
            It should reclaim the registry regardless — a fresh owner
            claims the namespace immediately — and the orphaned
            borrower should still exit without raising.
        """
        # Act — the borrower stays bound (see `borrowed_publisher`)
        borrowed_publisher.release_owner()

        # Assert — a live borrower blocks neither the reclaim nor
        # the next claim
        with LocalDiscovery(namespace) as respawned:
            assert respawned.namespace == namespace

        # Act & assert — the orphaned borrower unwinds without raising
        await borrowed_publisher.release_publisher()

    @pytest.mark.asyncio
    async def test___exit___should_unwind_cleanly_when_overlapping_lifecycles_interleave(
        self, namespace, metadata, borrowed_publisher
    ):
        """Test overlapping same-namespace generations unwind cleanly.

        Given:
            Owner A holding a namespace, and a Publisher B that
            borrowed A's registry and published a worker
        When:
            A exits, orphaning B, B publishes again, C enters the freed
            namespace, a second worker is published in C's epoch, and
            C and then B exit
        Then:
            It should reject B's publish with DiscoveryNamespaceNotFound,
            make the second worker discoverable through C's fresh
            registry, and raise nothing else.
        """
        # Arrange
        successor_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50052",
            pid=12346,
            version="1.0.0",
        )
        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        # Act — A-exit/B-rejected/C-enter/publish/C-exit/B-exit, with B
        # bound throughout (see `borrowed_publisher`)
        borrowed_publisher.release_owner()

        with pytest.raises(DiscoveryNamespaceNotFound):
            await borrowed_publisher.publisher.publish("worker-added", metadata)

        with LocalDiscovery(namespace) as c:
            publisher = LocalDiscovery.Publisher(namespace)
            async with publisher:
                await publisher.publish("worker-added", successor_worker)

                subscriber = c.subscribe(poll_interval=0.05)
                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker not discovered in C's epoch")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        await borrowed_publisher.release_publisher()

        # Assert
        assert len(events) == 1
        assert events[0].type == "worker-added"
        assert events[0].metadata.uid == successor_worker.uid

    def test___exit___should_not_raise_when_registry_already_removed(
        self, namespace, unlink_schedule
    ):
        """Test owner exit tolerates an externally removed registry.

        Given:
            An owner LocalDiscovery whose registry file is removed
            out from under it, as by an operator clearing residue
        When:
            The owner exits via with
        Then:
            It should exit cleanly without raising FileNotFoundError.
        """
        # Arrange
        unlink_schedule.append(FileNotFoundError(2, "No such file or directory"))

        # Act & assert — exits cleanly despite the vanished registry
        with LocalDiscovery(namespace):
            pass

    def test___exit___should_disarm_atexit_fallback_when_unlink_raises_permission_error(
        self, namespace, atexit_recorder, unlink_schedule, teardown_log
    ):
        """Test the fallback is disarmed before the unlink is attempted.

        Given:
            An owner LocalDiscovery whose registry removal raises
            PermissionError, with atexit unregistration and the removal
            recorded into one ordered log
        When:
            The owner exits via with
        Then:
            It should record the unregistration strictly before the
            unlink — a failing unlink cannot strand an armed fallback.
        """
        # Arrange
        registered, unregistered = atexit_recorder
        unlink_schedule.append(PermissionError(13, "Permission denied"))

        # Act
        with pytest.warns(ResourceWarning):
            with LocalDiscovery(namespace):
                pass

        # Assert — one log for both events, so their relative order is
        # observable (see `teardown_log`)
        assert registered == unregistered
        assert len(registered) == 1
        assert teardown_log[0] == "unregister"
        assert set(teardown_log[1:]) == {"unlink"}

    def test___exit___should_warn_when_unlink_fails_unexpectedly(
        self, namespace, unlink_schedule
    ):
        """Test an unexpected unlink failure surfaces as a warning.

        Given:
            An owner LocalDiscovery whose registry removal raises
            PermissionError, a failure with no benign explanation
        When:
            The owner exits via with
        Then:
            It should emit a ResourceWarning naming the file it could
            not remove, so an operator can find the leak.
        """
        # Arrange — the path is the warning's actionable payload, so
        # match on it rather than on the prefix alone.
        directory = namespace_directory(namespace)
        unlink_schedule.append(PermissionError(13, "Permission denied"))

        # Act & assert
        with pytest.warns(ResourceWarning, match=re.escape(str(directory))):
            with LocalDiscovery(namespace):
                pass

    def test___exit___should_propagate_body_error_when_unlink_fails_unexpectedly(
        self, namespace, unlink_schedule
    ):
        """Test a failing teardown does not mask the caller's exception.

        Given:
            An owner LocalDiscovery whose registry removal raises
            PermissionError
        When:
            The body raises ValueError and the owner exits via with
        Then:
            It should surface the body's ValueError rather than
            replacing it with the teardown failure.
        """
        # Arrange
        unlink_schedule.append(PermissionError(13, "Permission denied"))

        # Act & assert
        with pytest.warns(ResourceWarning):
            with pytest.raises(ValueError, match="boom"):
                with LocalDiscovery(namespace):
                    raise ValueError("boom")

    @given(forest=_LIFECYCLE_FORESTS)
    @settings(
        max_examples=25,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test___exit___should_unwind_arbitrary_lifecycle_interleavings(
        self, namespace, forest
    ):
        """Test arbitrary same-namespace lifecycles unwind cleanly.

        Given:
            An arbitrary forest of same-namespace claims, where
            siblings model teardown+respawn generations and nesting
            models a claim against a live owner
        When:
            Every claim is entered via nested with statements, a claim
            made against a live owner being rejected, and a final
            fresh owner enters the namespace
        Then:
            It should reject exactly the nested claims with
            DiscoveryNamespaceInUse, raise nothing else, and leave the
            namespace re-creatable.
        """
        # Arrange — per-example namespace so a leaked registry in one
        # example cannot reject the next example's first claim
        example_ns = f"{namespace}-{uuid.uuid4().hex[:8]}"

        # Act
        _enter_lifecycle_forest(example_ns, forest)

        # Assert — the namespace remains re-creatable
        with LocalDiscovery(example_ns):
            pass

    @given(forest=_LIFECYCLE_FORESTS, mask=st.lists(st.booleans(), max_size=10))
    @settings(
        max_examples=25,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test___exit___should_unwind_interleavings_when_files_vanish(
        self, namespace, atexit_recorder, unlink_schedule, forest, mask
    ):
        """Test vanishing files never break lifecycle unwinding.

        Given:
            An arbitrary forest of same-namespace claims and an
            arbitrary subset of removals that observe the file
            already removed by an external unlinker, with atexit
            registration wrapped in recording pass-throughs
        When:
            Every claim is entered via nested with statements, a claim
            made against a live owner being rejected
        Then:
            It should reject exactly the nested claims with
            DiscoveryNamespaceInUse and raise nothing else for any
            forest and mask combination, pair every registered
            fallback with exactly one unregistration, and leave the
            namespace re-creatable.
        """
        # Arrange
        registered, unregistered = atexit_recorder
        registered.clear()
        unregistered.clear()
        unlink_schedule.clear()
        unlink_schedule.extend(
            FileNotFoundError(2, "No such file or directory") if vanished else None
            for vanished in mask
        )
        example_ns = f"{namespace}-{uuid.uuid4().hex[:8]}"

        # Act
        _enter_lifecycle_forest(example_ns, forest)

        # Assert
        assert registered == unregistered
        with LocalDiscovery(example_ns):
            pass

    @pytest.mark.asyncio
    async def test_subscribe_should_keep_streaming_when_owner_exits(
        self, metadata, borrowed_publisher
    ):
        """Test an iteration already under way outlives the owner.

        Given:
            An owner's namespace, a Publisher still bound that
            published a worker, and a subscriber already iterating it
        When:
            The owner exits, reclaiming the registry out from under
            the running iteration
        Then:
            It should keep yielding the worker's registration.
        """
        # Arrange — the borrower keeps the worker's block alive past the
        # owner's release (see `borrowed_publisher`).
        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()

        subscriber = borrowed_publisher.owner.subscribe(poll_interval=0.05)
        task = asyncio.create_task(collect(subscriber))
        try:
            await asyncio.wait_for(event_received.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            pytest.fail("Worker not discovered within timeout")

        # Act — the owner exits out from under the live iteration
        observed = len(events)
        event_received.clear()
        borrowed_publisher.release_owner()

        try:
            # Assert — the stream kept yielding after the reclaim. The
            # subscriber re-reports the unchanged worker as
            # worker-updated on each scan, so a further event arrives
            # only if scanning continued.
            await asyncio.wait_for(event_received.wait(), timeout=2.0)
            assert len(events) > observed

            # Assert — and kept reading the same registration: a
            # registry it could no longer read would surface the
            # worker as dropped.
            assert not task.done()
            assert events[0].type == "worker-added"
            assert events[0].metadata.uid == metadata.uid
            assert {event.type for event in events[1:]} <= {"worker-updated"}
            assert all(event.metadata.uid == metadata.uid for event in events)
        except asyncio.TimeoutError:
            pytest.fail("Orphaned iteration stopped yielding after the reclaim")
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test___enter___should_restamp_capacity_when_recreated_by_new_owner(
        self, namespace
    ):
        """Test a new owner generation re-stamps the registry's capacity.

        Given:
            A namespace previously owned at capacity 1 whose owner has
            entered and exited, removing the registry
        When:
            A new owner enters the same namespace at capacity 3 and its
            publisher registers three workers, then a fourth
        Then:
            It should admit all three and reject the fourth — the fresh
            owner re-stamps capacity 3, so the prior generation's cap of 1
            does not persist.
        """
        # Arrange — a prior owner generation stamps capacity 1, then exits.
        with LocalDiscovery(namespace, capacity=1) as first:
            async with first.publisher as publisher:
                await publisher.publish(
                    "worker-added",
                    WorkerMetadata(
                        uid=uuid.uuid4(),
                        address="localhost:50051",
                        pid=1,
                        version="1.0",
                    ),
                )

        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"localhost:{50051 + i}",
                pid=100 + i,
                version="1.0",
            )
            for i in range(3)
        ]

        # Act & assert — a new owner generation stamps capacity 3.
        with LocalDiscovery(namespace, capacity=3) as second:
            async with second.publisher as publisher:
                for worker in workers:
                    await publisher.publish("worker-added", worker)

                with pytest.raises(DiscoveryCapacityExhausted):
                    await publisher.publish(
                        "worker-added",
                        WorkerMetadata(
                            uid=uuid.uuid4(),
                            address="localhost:60000",
                            pid=999,
                            version="1.0",
                        ),
                    )


class TestLocalDiscoveryPublisher:
    """Tests for LocalDiscovery.Publisher class.

    Fully qualified name:
    wool.runtime.discovery.local.LocalDiscovery.Publisher
    """

    @pytest.mark.parametrize("bad", ["a/b", "../escape", ".", "..", "", "x" * 256])
    def test___init___should_raise_when_namespace_leaves_its_root(self, bad):
        """Test Publisher rejects a namespace that is not one path component.

        Given:
            A namespace carrying a path separator, a relative-path
            element, nothing at all, or more characters than a directory
            name can hold
        When:
            Publisher is instantiated
        Then:
            It should raise ValueError at construction, so a borrower
            cannot reach outside the module's root any more than an
            owner can.
        """
        # Act & assert
        with pytest.raises(ValueError, match="namespace"):
            LocalDiscovery.Publisher(bad)

    @pytest.mark.parametrize("block_size", [0, -1, 1, 4])
    def test___init___should_raise_when_block_size_within_the_prefix(
        self, namespace, block_size
    ):
        """Test Publisher rejects a block size the prefix alone fills.

        Given:
            A block_size no larger than the 4-byte length prefix every
            block spends before its payload
        When:
            Publisher is instantiated
        Then:
            It should raise ValueError at construction.
        """
        # Act & assert
        with pytest.raises(
            ValueError, match="Expected block size greater than the 4-byte"
        ):
            LocalDiscovery.Publisher(namespace, block_size=block_size)

    @given(block_size=st.integers())
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @example(block_size=5)
    @example(block_size=4)
    @example(block_size=1)
    @example(block_size=0)
    @example(block_size=-1)
    def test___init___should_validate_block_size_across_domain(
        self, namespace, block_size
    ):
        """Test Publisher validates block_size across the integer domain.

        Given:
            Any integer block_size.
        When:
            A Publisher is instantiated with it.
        Then:
            It should raise ValueError naming the offending value exactly
            when the value does not exceed the length prefix, and
            construct successfully for every larger value.
        """
        # Act & assert
        if block_size <= 4:
            with pytest.raises(
                ValueError,
                match=(
                    f"Expected block size greater than the 4-byte length prefix, "
                    f"got {block_size}"
                ),
            ):
                LocalDiscovery.Publisher(namespace, block_size=block_size)
        else:
            publisher = LocalDiscovery.Publisher(namespace, block_size=block_size)
            assert publisher.namespace == namespace

    def test___init___should_raise_when_lock_timeout_negative(self, namespace):
        """Test Publisher rejects a negative lock timeout.

        Given:
            A negative lock_timeout
        When:
            Publisher is instantiated
        Then:
            It should raise ValueError.
        """
        # Act & assert
        with pytest.raises(ValueError, match="Lock timeout must be non-negative"):
            LocalDiscovery.Publisher(namespace, lock_timeout=-1)

    def test___init___should_raise_when_capacity_is_declared(self, namespace):
        """Test a borrower is offered no capacity of its own.

        Given:
            A namespace whose capacity is the owner's to stamp
        When:
            A Publisher is constructed with a capacity argument
        Then:
            It should raise TypeError naming the rejected argument —
            capacity is read from the registry the owner stamped, and a
            borrower has no way to declare or override it.
        """
        # Act & assert
        with pytest.raises(TypeError, match="capacity"):
            LocalDiscovery.Publisher(namespace, capacity=128)  # type: ignore[call-arg]

    @given(
        lock_timeout=st.one_of(
            st.none(),
            st.floats(min_value=0, allow_nan=False, allow_infinity=False),
            st.floats(
                max_value=0,
                exclude_max=True,
                allow_nan=False,
                allow_infinity=False,
            ),
        )
    )
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test___init___should_validate_lock_timeout_across_domain(
        self, namespace, lock_timeout
    ):
        """Test Publisher validates lock_timeout across the float domain.

        Given:
            Any None or finite float lock_timeout.
        When:
            A Publisher is instantiated with it.
        Then:
            It should raise ValueError exactly when the value is a negative
            float, and construct successfully for None and every
            non-negative float.
        """
        # Act & assert
        if lock_timeout is not None and lock_timeout < 0:
            with pytest.raises(ValueError, match="Lock timeout must be non-negative"):
                LocalDiscovery.Publisher(namespace, lock_timeout=lock_timeout)
        else:
            publisher = LocalDiscovery.Publisher(namespace, lock_timeout=lock_timeout)
            assert publisher.namespace == namespace

    def test_bind_host_with_default_value(self, namespace):
        """Test bind_host prescribes the loopback address.

        Given:
            A LocalDiscovery Publisher
        When:
            The bind_host attribute is accessed
        Then:
            It should be "127.0.0.1" since registry announcements
            are only discoverable same-host.
        """
        # Act
        publisher = LocalDiscovery.Publisher(namespace)

        # Assert
        assert publisher.bind_host == "127.0.0.1"

    def test_namespace_with_provided_value(self, namespace):
        """Test Publisher.namespace property returns provided value.

        Given:
            A namespace string
        When:
            Publisher is instantiated
        Then:
            It should return the provided namespace.
        """
        # Act
        publisher = LocalDiscovery.Publisher(namespace)

        # Assert
        assert publisher.namespace == namespace

    @pytest.mark.asyncio
    async def test___aenter___and___aexit___lifecycle(self, namespace):
        """Test Publisher async context manager lifecycle.

        Given:
            A Publisher instance on a namespace an owner holds
        When:
            Used as an async context manager via async with
        Then:
            It should be available inside the block and cleaned up
            after.
        """
        # Arrange
        publisher = LocalDiscovery.Publisher(namespace)

        # Act & assert
        with LocalDiscovery(namespace):
            async with publisher as ctx:
                assert ctx is publisher

    @pytest.mark.asyncio
    async def test___aenter___should_raise_when_namespace_has_no_owner(self, namespace):
        """Test a publisher never creates the registry it borrows.

        Given:
            A Publisher on a namespace no LocalDiscovery has entered
        When:
            The publisher is entered via async with
        Then:
            It should raise DiscoveryNamespaceNotFound naming the
            namespace, having created no registry of its own.
        """
        # Arrange
        publisher = LocalDiscovery.Publisher(namespace)

        # Act & assert
        with pytest.raises(DiscoveryNamespaceNotFound) as excinfo:
            async with publisher:
                pass

        assert excinfo.value.namespace == namespace

        # Assert — the rejected bind left no registry behind, so the
        # namespace is still free for an owner to claim.
        with LocalDiscovery(namespace) as owner:
            assert owner.namespace == namespace

    @pytest.mark.parametrize("event", ["worker-added", "worker-dropped"])
    @pytest.mark.asyncio
    async def test_publish_should_raise_when_the_owner_has_exited(
        self, namespace, metadata, event, borrowed_publisher
    ):
        """Test publishing through an orphaned publisher is rejected.

        Given:
            A Publisher bound while its owner was live, with a worker
            already published, whose owner has since exited and
            reclaimed the registry
        When:
            The still-bound publisher publishes again
        Then:
            It should raise DiscoveryNamespaceNotFound naming the
            namespace, chained from the underlying FileNotFoundError,
            for either event kind.
        """
        # Arrange — orphan the still-bound publisher (see
        # `borrowed_publisher`)
        borrowed_publisher.release_owner()

        # Act & assert
        with pytest.raises(DiscoveryNamespaceNotFound) as excinfo:
            await borrowed_publisher.publisher.publish(event, metadata)

        assert excinfo.value.namespace == namespace
        assert isinstance(excinfo.value.__cause__, FileNotFoundError)

    @pytest.mark.asyncio
    async def test_publish_should_reach_successor_registry_when_orphaned(
        self, namespace, metadata, borrowed_publisher
    ):
        """Test an orphaned publisher publishes to a successor's registry.

        Given:
            A Publisher bound while its owner was live, whose owner has
            since exited, and a successor LocalDiscovery that has
            entered the same namespace
        When:
            The orphaned publisher publishes worker-added
        Then:
            It should raise nothing, and a subscriber on the successor
            should observe the worker.
        """
        # Arrange — orphan the still-bound publisher (see
        # `borrowed_publisher`)
        borrowed_publisher.release_owner()
        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        with LocalDiscovery(namespace) as successor:
            # Act
            await borrowed_publisher.publisher.publish("worker-added", metadata)

            # Assert
            subscriber = successor.subscribe(poll_interval=0.05)
            task = asyncio.create_task(collect(subscriber))
            try:
                await asyncio.wait_for(event_received.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pytest.fail("Worker not discovered in the successor's registry")
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            assert len(events) == 1
            assert events[0].type == "worker-added"
            assert events[0].metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test___aexit___should_reclaim_its_blocks_when_the_owner_has_exited(
        self, atexit_recorder, borrowed_publisher
    ):
        """Test an orphaned publisher still reclaims its own blocks.

        Given:
            A Publisher that published a worker while its owner was
            live, with atexit registration wrapped in recording
            pass-throughs, whose owner has since reclaimed the registry
        When:
            The orphaned publisher exits
        Then:
            It should exit without raising and unregister every block
            fallback it registered — a borrower owns its worker blocks
            even once the registry it borrowed is gone.
        """
        # Arrange — orphan the still-bound publisher (see
        # `borrowed_publisher`)
        registered, unregistered = atexit_recorder
        borrowed_publisher.release_owner()

        # Act
        await borrowed_publisher.release_publisher()

        # Assert — every fallback the publisher armed was disarmed
        assert registered == unregistered
        assert len(registered) >= 1

    @pytest.mark.asyncio
    async def test___aexit___should_leave_the_registry_to_its_owner(
        self, namespace, metadata
    ):
        """Test a publisher's exit leaves the registry to its owner.

        Given:
            An owner holding a namespace and a Publisher borrowing its
            registry
        When:
            The publisher exits while the owner is still live
        Then:
            It should leave the registry intact, so a fresh borrower
            binds and publishes to it afterwards.
        """
        # Arrange
        with LocalDiscovery(namespace):
            # Act
            async with LocalDiscovery.Publisher(namespace):
                pass

            # Assert — a fresh borrower still binds and publishes
            async with LocalDiscovery.Publisher(namespace) as publisher:
                await publisher.publish("worker-added", metadata)

    @pytest.mark.asyncio
    async def test_publish_worker_added(self, namespace, metadata):
        """Test publish("worker-added") makes worker discoverable.

        Given:
            A LocalDiscovery context and an initialized Publisher
        When:
            publish("worker-added", metadata) is called
        Then:
            It should store the worker so subscribers can discover it.
        """
        # Arrange
        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

            # Act
            async with publisher:
                await publisher.publish("worker-added", metadata)

                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker not discovered within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        assert len(events) == 1
        assert events[0].type == "worker-added"
        assert events[0].metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test_publish_worker_dropped(self, namespace, metadata):
        """Test publish("worker-dropped") removes worker from discovery.

        Given:
            A published worker
        When:
            publish("worker-dropped", metadata) is called
        Then:
            It should remove the worker from the registry.
        """
        # Arrange
        events = []
        worker_added = asyncio.Event()
        worker_dropped = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                if event.type == "worker-added" and event.metadata.uid == metadata.uid:
                    worker_added.set()
                elif (
                    event.type == "worker-dropped" and event.metadata.uid == metadata.uid
                ):
                    worker_dropped.set()
                    break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

            async with publisher:
                task = asyncio.create_task(collect(subscriber))

                await publisher.publish("worker-added", metadata)
                await asyncio.wait_for(worker_added.wait(), timeout=2.0)

                # Act
                await publisher.publish("worker-dropped", metadata)

                try:
                    await asyncio.wait_for(worker_dropped.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker drop not detected within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        dropped = [e for e in events if e.type == "worker-dropped"]
        assert len(dropped) >= 1
        assert dropped[0].metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test_publish_should_complete_silently_when_dropping_unknown_worker(
        self, namespace, metadata
    ):
        """Test dropping a never-added worker is a public no-op.

        Given:
            An initialized Publisher on an empty namespace
        When:
            publish("worker-dropped", metadata) is called for a
            worker that was never added
        Then:
            It should complete without raising and emit no
            subscriber-visible event.
        """
        # Arrange
        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            async with publisher:
                # Act
                await publisher.publish("worker-dropped", metadata)

                # Assert — no event lands within the observation window
                subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)
                task = asyncio.create_task(collect(subscriber))
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(event_received.wait(), timeout=0.25)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        assert events == []

    @pytest.mark.asyncio
    async def test_publish_should_disarm_block_atexit_fallback_when_worker_dropped(
        self, namespace, metadata, atexit_recorder
    ):
        """Test drop disarms the per-block atexit fallback.

        Given:
            A Publisher in an owner discovery context, with atexit
            registration wrapped in recording pass-throughs
        When:
            A worker is published and then dropped
        Then:
            It should register exactly one per-block fallback and
            unregister that same callable at the drop.
        """
        # Arrange
        registered, unregistered = atexit_recorder

        with LocalDiscovery(namespace):
            baseline = len(registered)
            publisher = LocalDiscovery.Publisher(namespace)
            async with publisher:
                # Act
                await publisher.publish("worker-added", metadata)
                await publisher.publish("worker-dropped", metadata)

                # Assert
                block_registered = registered[baseline:]
                assert block_registered == unregistered
                assert len(block_registered) == 1

    @pytest.mark.asyncio
    async def test_publish_should_pair_atexit_fallbacks_when_worker_cycles_repeatedly(
        self, namespace, metadata, atexit_recorder
    ):
        """Test repeated add/drop cycles never accumulate fallbacks.

        Given:
            A Publisher in an owner discovery context, with atexit
            registration wrapped in recording pass-throughs
        When:
            The same worker is added, dropped, added, and dropped
        Then:
            It should record two registrations paired one-to-one in
            order with two unregistrations, the second add succeeding
            because the drop removed the block's file.
        """
        # Arrange
        registered, unregistered = atexit_recorder

        with LocalDiscovery(namespace):
            baseline = len(registered)
            publisher = LocalDiscovery.Publisher(namespace)
            async with publisher:
                # Act
                await publisher.publish("worker-added", metadata)
                await publisher.publish("worker-dropped", metadata)
                await publisher.publish("worker-added", metadata)
                await publisher.publish("worker-dropped", metadata)

                # Assert
                block_registered = registered[baseline:]
                assert block_registered == unregistered
                assert len(block_registered) == 2

    @pytest.mark.asyncio
    async def test___aexit___should_finalize_every_block_when_workers_are_published(
        self, namespace, atexit_recorder, mocker
    ):
        """Test the publisher closes its own blocks before exiting the pool.

        Given:
            A Publisher in an owner discovery context with two workers
            published and never dropped, with atexit registration
            wrapped in recording pass-throughs and the block's
            pool's exit wrapped to record what had been unregistered by
            the time it began
        When:
            The publisher's context exits
        Then:
            It should have unregistered both blocks' fallbacks already
            when the pool exit starts, the ledger owning the close
            rather than leaving it to the pool that would otherwise
            finalize the same entries anyway.
        """
        # Arrange
        registered, unregistered = atexit_recorder
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"localhost:5005{i}",
                pid=100 + i,
                version="1.0",
            )
            for i in range(2)
        ]

        with LocalDiscovery(namespace):
            baseline = len(registered)
            publisher = LocalDiscovery.Publisher(namespace)
            stack = AsyncExitStack()
            await stack.enter_async_context(publisher)
            for worker in workers:
                await publisher.publish("worker-added", worker)
            unregistered_before = list(unregistered)
            at_pool_exit = []
            pool_exit = ResourcePool.__aexit__

            async def record_then_exit(self, *args):
                at_pool_exit.append(list(unregistered))
                return await pool_exit(self, *args)

            mocker.patch.object(ResourcePool, "__aexit__", record_then_exit)

            # Act
            await stack.aclose()

            # Assert
            block_registered = registered[baseline:]
            assert len(block_registered) == 2
            assert unregistered_before == []
            assert sorted(map(id, block_registered)) == sorted(map(id, unregistered))
            # The ordering oracle: every unregistration is already in
            # hand when the pool exit begins, so removing the ledger's
            # own loop fails here rather than passing on the pool's
            # finalization of the same entries.
            assert len(at_pool_exit) == 1
            assert sorted(map(id, at_pool_exit[0])) == sorted(map(id, unregistered))

    @pytest.mark.asyncio
    async def test___aexit___should_release_every_other_block_when_one_close_raises(
        self, namespace, atexit_recorder, mocker
    ):
        """Test one block's failure does not abandon the blocks after it.

        Given:
            A Publisher in an owner discovery context with two workers
            published, the close of the first block the exit reaches
            patched to raise, and atexit registration wrapped in
            recording pass-throughs
        When:
            The publisher's context exits
        Then:
            It should raise that first failure, having still closed the
            other block before the pool exit began, so a bad handle
            costs its own block and no more — and not merely left the
            rest for the pool to finalize.
        """
        # Arrange
        registered, unregistered = atexit_recorder
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"localhost:5006{i}",
                pid=200 + i,
                version="1.0",
            )
            for i in range(2)
        ]
        real_aclose = AsyncExitStack.aclose
        failed = []
        at_pool_exit = []
        pool_exit = ResourcePool.__aexit__

        async def fail_first(self):
            if not failed:
                failed.append(self)
                await real_aclose(self)
                raise RuntimeError("block close failed")
            return await real_aclose(self)

        async def record_then_exit(self, *args):
            at_pool_exit.append(list(unregistered))
            return await pool_exit(self, *args)

        with LocalDiscovery(namespace):
            baseline = len(registered)
            publisher = LocalDiscovery.Publisher(namespace)
            stack = AsyncExitStack()
            await stack.enter_async_context(publisher)
            for worker in workers:
                await publisher.publish("worker-added", worker)
            mocker.patch.object(AsyncExitStack, "aclose", fail_first)
            mocker.patch.object(ResourcePool, "__aexit__", record_then_exit)

            # Act & assert
            with pytest.raises(RuntimeError, match="block close failed"):
                await publisher.__aexit__(None, None, None)

            block_registered = registered[baseline:]
            assert len(block_registered) == 2
            assert sorted(map(id, block_registered)) == sorted(map(id, unregistered))
            # Both blocks are already released when the pool exit
            # starts: abandoning the loop at the first failure would
            # leave the second for the pool to finalize instead.
            assert len(at_pool_exit) == 1
            assert sorted(map(id, at_pool_exit[0])) == sorted(map(id, unregistered))

    @pytest.mark.asyncio
    async def test_publish_should_ignore_a_drop_for_a_worker_never_added(
        self, namespace, metadata, atexit_recorder
    ):
        """Test dropping an unknown worker is a no-op.

        Given:
            A Publisher in an owner discovery context that never
            published the worker
        When:
            worker-dropped is published for it
        Then:
            It should return without raising and touch no atexit
            registration.
        """
        # Arrange
        registered, unregistered = atexit_recorder
        with LocalDiscovery(namespace):
            baseline = (len(registered), len(unregistered))
            publisher = LocalDiscovery.Publisher(namespace)
            async with publisher:
                # Act
                await publisher.publish("worker-dropped", metadata)

                # Assert
                assert (len(registered), len(unregistered)) == baseline

    @pytest.mark.asyncio
    async def test_publish_should_release_the_old_block_when_a_re_add_reclaims_a_slot(
        self, namespace, metadata, atexit_recorder
    ):
        """Test reclaiming a slot releases the block the publisher still held.

        Given:
            A published worker whose per-worker block a dead peer
            unlinked out from under this publisher, so re-publishing it
            reclaims the stale slot and registers a fresh block, with
            atexit registration wrapped in recording pass-throughs
        When:
            The worker is published again and then dropped
        Then:
            It should release the handle naming the vanished block as
            the re-add displaces it, rather than at the end of the
            publisher's life, and pair the fresh block's own fallback
            registration with its unregistration at the drop.
        """
        # Arrange
        registered, unregistered = atexit_recorder
        directory = namespace_directory(namespace)
        with LocalDiscovery(namespace):
            baseline = len(registered)
            released = len(unregistered)
            publisher = LocalDiscovery.Publisher(namespace)
            stack = AsyncExitStack()
            await stack.enter_async_context(publisher)
            before = set(directory.iterdir())
            await publisher.publish("worker-added", metadata)
            # A dead peer's teardown removes blocks without nulling the
            # slots that name them -- the case the reclaim branch exists
            # for. The block is the file the publish created rather than
            # a path rebuilt here, so the test removes what it made.
            [block] = set(directory.iterdir()) - before
            block.unlink()

            # Act
            await publisher.publish("worker-added", metadata)
            at_readd = unregistered[released:]
            await publisher.publish("worker-dropped", metadata)
            at_drop = unregistered[released:]
            await stack.aclose()

            # Assert — one fallback per block, each disarmed in its turn:
            # the vanished block's as the re-add replaces it, the fresh
            # block's at the drop, leaving the publisher's exit nothing.
            block_registered = registered[baseline:]
            assert len(block_registered) == 2
            assert at_readd == block_registered[:1]
            assert at_drop == block_registered
            assert unregistered[released:] == at_drop

    @pytest.mark.asyncio
    async def test_publish_should_release_its_block_when_a_peer_nulled_the_slot(
        self, namespace, metadata, atexit_recorder
    ):
        """Test a drop releases this publisher's block with no slot to match.

        Given:
            A published worker whose slot a peer publisher on the same
            namespace has already nulled by dropping it, with atexit
            registration wrapped in recording pass-throughs
        When:
            The publisher that owns the block drops the worker too
        Then:
            It should pair the block's fallback registration with its
            unregistration, the slot scan finding nothing being no
            reason to keep holding the block.
        """
        # Arrange
        registered, unregistered = atexit_recorder
        with LocalDiscovery(namespace):
            baseline = len(registered)
            owner = LocalDiscovery.Publisher(namespace)
            peer = LocalDiscovery.Publisher(namespace)
            stack = AsyncExitStack()
            await stack.enter_async_context(owner)
            await stack.enter_async_context(peer)
            await owner.publish("worker-added", metadata)
            await peer.publish("worker-dropped", metadata)

            # Act
            await owner.publish("worker-dropped", metadata)
            at_drop = list(unregistered)
            await stack.aclose()

            # Assert
            block_registered = registered[baseline:]
            assert len(block_registered) == 1
            assert block_registered == at_drop
            assert unregistered == at_drop

    @pytest.mark.asyncio
    async def test_publish_should_complete_drop_when_block_already_unlinked(
        self, namespace, metadata, atexit_recorder, unlink_schedule
    ):
        """Test drop tolerates an externally unlinked worker block.

        Given:
            A published worker whose per-worker block is removed
            out from under the publisher, with atexit registration
            wrapped in recording pass-throughs
        When:
            publish("worker-dropped", metadata) is called
        Then:
            It should complete without raising and still pair the
            block's fallback registration with its unregistration.
        """
        # Arrange
        registered, unregistered = atexit_recorder

        with LocalDiscovery(namespace):
            baseline = len(registered)
            publisher = LocalDiscovery.Publisher(namespace)
            async with publisher:
                await publisher.publish("worker-added", metadata)
                unlink_schedule.append(FileNotFoundError(2, "No such file or directory"))

                # Act
                await publisher.publish("worker-dropped", metadata)

                # Assert
                block_registered = registered[baseline:]
                assert block_registered == unregistered
                assert len(block_registered) == 1

    @pytest.mark.asyncio
    async def test_publish_should_disarm_atexit_fallback_when_unlink_fails(
        self, namespace, metadata, atexit_recorder, unlink_schedule
    ):
        """Test the block fallback is disarmed before the unlink runs.

        Given:
            A published worker whose next block unlink raises
            RuntimeError, an error the block finalizer does not
            suppress, with atexit registration wrapped in recording
            pass-throughs
        When:
            publish("worker-dropped", metadata) is called
        Then:
            It should complete without raising, the pool swallowing
            the finalizer error, and the block's fallback should
            already be unregistered — proving the disarm precedes the
            unlink.
        """
        # Arrange
        registered, unregistered = atexit_recorder

        with LocalDiscovery(namespace):
            baseline = len(registered)
            publisher = LocalDiscovery.Publisher(namespace)
            async with publisher:
                await publisher.publish("worker-added", metadata)
                unlink_schedule.append(RuntimeError("unlink failed"))

                # Act
                await publisher.publish("worker-dropped", metadata)

                # Assert
                block_registered = registered[baseline:]
                assert block_registered == unregistered
                assert len(block_registered) == 1

    @pytest.mark.asyncio
    async def test_publish_worker_updated(self, namespace, metadata):
        """Test publish("worker-updated") updates worker metadata.

        Given:
            A published worker
        When:
            publish("worker-updated", updated_metadata) is called
        Then:
            It should update the worker's metadata block.
        """
        # Arrange
        updated_worker = WorkerMetadata(
            uid=metadata.uid,
            address="newhost:9999",
            pid=99999,
            version="2.0.0",
        )
        events = []
        worker_added = asyncio.Event()
        worker_updated = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                if event.type == "worker-added" and event.metadata.uid == metadata.uid:
                    worker_added.set()
                elif (
                    event.type == "worker-updated"
                    and event.metadata.uid == metadata.uid
                    and event.metadata.version == "2.0.0"
                ):
                    worker_updated.set()
                    break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

            async with publisher:
                task = asyncio.create_task(collect(subscriber))

                await publisher.publish("worker-added", metadata)
                await asyncio.wait_for(worker_added.wait(), timeout=2.0)

                # Act
                await publisher.publish("worker-updated", updated_worker)

                try:
                    await asyncio.wait_for(worker_updated.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker update not detected within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        updated = [
            e
            for e in events
            if e.type == "worker-updated" and e.metadata.version == "2.0.0"
        ]
        assert len(updated) >= 1
        assert updated[0].metadata.version == "2.0.0"

    @pytest.mark.asyncio
    async def test_publish_with_invalid_event_type(self, namespace, metadata):
        """Test publish() raises error for invalid event types.

        Given:
            An initialized Publisher
        When:
            publish("invalid-type", metadata) is called
        Then:
            It should raise RuntimeError.
        """
        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)

            # Act & assert
            async with publisher:
                with pytest.raises(
                    RuntimeError,
                    match="Unexpected discovery event type",
                ):
                    await publisher.publish(
                        "invalid-type",
                        metadata,  # type: ignore
                    )

    @given(
        address=st.from_regex(r"^[a-zA-Z0-9._-]+:[0-9]+$", fullmatch=True),
        pid=st.integers(min_value=1, max_value=2147483647),
        version=st.text(
            min_size=1,
            max_size=20,
            alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-_",
        ),
    )
    @settings(
        max_examples=10,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @pytest.mark.asyncio
    async def test_publish_roundtrip_with_arbitrary_metadata(
        self, namespace, address, pid, version
    ):
        """Test publish-discover roundtrip with arbitrary metadata.

        Given:
            Arbitrary valid WorkerMetadata field values
        When:
            Worker is published then discovered via a subscriber
        Then:
            All metadata fields should match the published values.
        """
        # Arrange — use a per-example namespace so the subscriber
        # singleton does not carry stale state across Hypothesis
        # examples.
        example_ns = f"{namespace}-{uuid.uuid4().hex[:8]}"
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address=address,
            pid=pid,
            version=version,
        )

        events = []
        discovered = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                discovered.set()
                break

        with LocalDiscovery(example_ns):
            publisher = LocalDiscovery.Publisher(example_ns)
            subscriber = LocalDiscovery.Subscriber(example_ns, poll_interval=0.05)

            # Act
            async with publisher:
                await publisher.publish("worker-added", worker)

                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(discovered.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker not discovered within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        assert len(events) == 1
        assert events[0].metadata.uid == worker.uid
        assert events[0].metadata.address == worker.address
        assert events[0].metadata.pid == worker.pid
        assert events[0].metadata.version == worker.version
        assert events[0].metadata.tags == worker.tags
        assert events[0].metadata.extra == worker.extra

    @pytest.mark.asyncio
    async def test_publish_worker_updated_non_existent_worker(self, namespace, metadata):
        """Test update non-existent worker raises a typed error.

        Given:
            An initialized Publisher with no published workers
        When:
            publish("worker-updated", metadata) is called for a
            worker that was never added
        Then:
            It should raise DiscoveryWorkerNotFound.
        """
        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)

            # Act & assert
            async with publisher:
                with pytest.raises(DiscoveryWorkerNotFound, match=str(metadata.uid)):
                    await publisher.publish("worker-updated", metadata)

    @pytest.mark.asyncio
    async def test_publish_should_raise_when_address_space_full(self, namespace):
        """Test publish to full address space raises DiscoveryCapacityExhausted.

        Given:
            A LocalDiscovery whose declared capacity has been filled
            with published workers
        When:
            One worker beyond capacity is published
        Then:
            It should raise DiscoveryCapacityExhausted.
        """
        # Arrange
        capacity = 8
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"localhost:{50051 + i}",
                pid=123 + i,
                version="1.0",
            )
            for i in range(capacity + 1)
        ]

        with LocalDiscovery(namespace, capacity=capacity) as discovery:
            publisher = discovery.publisher

            async with publisher:
                for worker in workers[:capacity]:
                    await publisher.publish("worker-added", worker)

                # Act & assert
                with pytest.raises(DiscoveryCapacityExhausted):
                    await publisher.publish("worker-added", workers[capacity])

    @pytest.mark.asyncio
    async def test_publish_update_overflow_preserves_prior_state(self, namespace):
        """Test update with oversized metadata preserves prior state.

        Given:
            A published worker with small metadata in a Publisher
            with a small block size
        When:
            The worker is updated with metadata too large for the
            block
        Then:
            It should raise DiscoveryBlockExhausted and preserve the original
            metadata.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
        )
        oversized_worker = WorkerMetadata(
            uid=worker.uid,
            address="localhost:50051",
            pid=123,
            version="1.0",
            extra=MappingProxyType({"data": "x" * 20000}),
        )

        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace, block_size=100)

            async with publisher:
                await publisher.publish("worker-added", worker)

                # Act
                with pytest.raises(DiscoveryBlockExhausted):
                    await publisher.publish("worker-updated", oversized_worker)

                # Assert — original metadata preserved via subscriber
                subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)
                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker not discoverable after rollback")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        assert len(events) == 1
        assert events[0].metadata.uid == worker.uid
        assert events[0].metadata.version == "1.0"

    @pytest.mark.asyncio
    async def test_publish_should_release_block_when_add_overflows(
        self, namespace, atexit_recorder
    ):
        """Test a failed add releases the block it acquired.

        Given:
            A Publisher with a small block size and a worker whose
            metadata exceeds the block, with atexit registration
            wrapped in recording pass-throughs
        When:
            The oversized worker is published and the add fails
        Then:
            It should release the block it acquired, pairing the
            block's fallback registration with its unregistration.
        """
        # Arrange
        registered, unregistered = atexit_recorder
        oversized = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
            extra=MappingProxyType({"data": "x" * 20000}),
        )

        with LocalDiscovery(namespace):
            baseline = len(registered)
            publisher = LocalDiscovery.Publisher(namespace, block_size=100)
            async with publisher:
                # Act
                with pytest.raises(DiscoveryBlockExhausted):
                    await publisher.publish("worker-added", oversized)

                # Assert
                block_registered = registered[baseline:]
                assert len(block_registered) == 1
                assert block_registered == unregistered

    @pytest.mark.asyncio
    async def test_publish_should_leave_worker_undiscoverable_when_add_overflows(
        self, namespace
    ):
        """Test a failed add leaves no trace of the worker behind.

        Given:
            A Publisher with a small block size, a worker whose
            metadata exceeds the block, and a worker that fits
        When:
            The oversized worker is published and then the fitting
            worker is published
        Then:
            It should raise DiscoveryBlockExhausted for the oversized worker,
            discover only the fitting worker, and tear both contexts
            down cleanly.
        """
        # Arrange
        oversized = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
            extra=MappingProxyType({"data": "x" * 20000}),
        )
        fitting = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50052",
            pid=124,
            version="1.0",
        )

        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace, block_size=100)
            async with publisher:
                # Act
                with pytest.raises(DiscoveryBlockExhausted):
                    await publisher.publish("worker-added", oversized)
                await publisher.publish("worker-added", fitting)

                # Assert — drain a bounded window rather than stopping at
                # the first event, so a leaked oversized worker surfaces
                # whatever order the two would arrive in
                subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)
                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Fitting worker not discovered within timeout")
                await asyncio.sleep(0.25)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        assert events[0].type == "worker-added"
        assert {e.metadata.uid for e in events} == {fitting.uid}

    @pytest.mark.asyncio
    async def test_publish_with_concurrent_lock_contention(
        self, namespace, metadata, contending_lock
    ):
        """Test publish retries on concurrent lock contention.

        Given:
            A file lock on the publisher's namespace that is
            temporarily held by another process
        When:
            publish("worker-added") is called
        Then:
            It should retry lock acquisition and succeed once
            the lock is released.
        """
        # Arrange
        lock = contending_lock(1)

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)

            # Act
            async with publisher:
                await publisher.publish("worker-added", metadata)

        # Assert
        assert lock.call_count == 2

    @pytest.mark.asyncio
    async def test_publish_should_poll_at_one_millisecond_when_lock_contended(
        self, namespace, metadata, mocker, contending_lock
    ):
        """Test publish polls the file lock at the 1ms interval.

        Given:
            A file lock temporarily held by another process, with
            asyncio.sleep wrapped in a recording pass-through
        When:
            publish("worker-added") retries lock acquisition
        Then:
            It should sleep 1ms between attempts rather than busy-spinning
            on a zero-second yield.
        """
        # Arrange
        contending_lock(1)

        recorded: list[float | None] = []
        real_sleep = asyncio.sleep

        async def recording_sleep(delay, *args, **kwargs):
            recorded.append(delay)
            return await real_sleep(delay, *args, **kwargs)

        mocker.patch.object(asyncio, "sleep", recording_sleep)

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)

            # Act
            async with publisher:
                await publisher.publish("worker-added", metadata)

        # Assert — the retry polls at exactly 1ms and never busy-spins on a
        # zero-second yield. Pinning the poll interval (an otherwise
        # unobservable implementation constant) mirrors atexit_recorder's
        # deliberate mechanism-pinning; a refactor of the retry is expected
        # to rewrite this assertion.
        assert 0.001 in recorded
        assert 0 not in recorded

    @pytest.mark.asyncio
    async def test_publish_should_raise_timeout_error_when_lock_acquisition_times_out(
        self, namespace, metadata, held_lock
    ):
        """Test publish surfaces a held lock as a bounded TimeoutError.

        Given:
            A file lock permanently held by another process and a
            publisher with a zero-second lock timeout
        When:
            publish("worker-added") attempts lock acquisition
        Then:
            It should raise TimeoutError rather than waiting forever.
        """
        # Act & assert
        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace, lock_timeout=0)

            async with publisher:
                with pytest.raises(TimeoutError):
                    await publisher.publish("worker-added", metadata)

    @pytest.mark.asyncio
    async def test_publish_should_bound_a_held_lock_with_the_default_timeout(
        self, namespace, metadata, mocker, held_lock
    ):
        """Test the default lock timeout bounds a permanently held lock.

        Given:
            A permanently held lock, a publisher constructed with the
            default lock_timeout, and a clock advanced far past that
            default on the first poll
        When:
            publish("worker-added", metadata) attempts lock acquisition
        Then:
            It should raise TimeoutError, proving the default timeout is
            finite rather than an unbounded wait.
        """
        # Arrange
        loop = asyncio.get_running_loop()
        fake_now = loop.time()

        async def jumping_sleep(delay, *args, **kwargs):
            nonlocal fake_now
            # Jump far past any finite timeout on the first poll so the
            # default deadline expires without a real 30s wait.
            fake_now += 1_000_000.0

        mocker.patch.object(loop, "time", side_effect=lambda: fake_now)
        mocker.patch.object(asyncio, "sleep", jumping_sleep)

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)

            # Act & assert
            async with publisher:
                with pytest.raises(TimeoutError, match="discovery lock"):
                    await publisher.publish("worker-added", metadata)

    @pytest.mark.asyncio
    async def test_publish_should_retry_without_bound_when_lock_timeout_none(
        self, namespace, metadata, mocker, contending_lock
    ):
        """Test a None lock timeout imposes no deadline as the clock advances.

        Given:
            A lock held across several attempts before release, a publisher
            whose lock_timeout is None, and a clock that jumps far past any
            finite timeout on every poll
        When:
            publish("worker-added") retries lock acquisition
        Then:
            It should keep polling until acquisition succeeds without ever
            raising, where any finite timeout would already have expired.
        """
        # Arrange
        lock = contending_lock(5)

        loop = asyncio.get_running_loop()
        fake_now = loop.time()

        async def jumping_sleep(delay, *args, **kwargs):
            nonlocal fake_now
            # Each poll jumps far past any finite timeout; a None deadline
            # never consults the clock, so acquisition still succeeds.
            fake_now += 1_000_000.0

        mocker.patch.object(loop, "time", side_effect=lambda: fake_now)
        mocker.patch.object(asyncio, "sleep", jumping_sleep)

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace, lock_timeout=None)

            # Act
            async with publisher:
                await publisher.publish("worker-added", metadata)

        # Assert — reaching the sixth attempt proves no deadline fired
        # despite the clock advancing past any finite timeout; a finite
        # lock_timeout would have raised on the second poll.
        assert lock.call_count == 6

    @pytest.mark.asyncio
    async def test_publish_should_acquire_when_lock_timeout_zero_and_uncontended(
        self, namespace, metadata
    ):
        """Test a zero lock timeout still acquires an uncontended lock.

        Given:
            An owner LocalDiscovery and a Publisher with lock_timeout=0 on
            an uncontended lock
        When:
            publish("worker-added", metadata) is called
        Then:
            It should acquire on the first attempt and publish the worker
            so a subscriber discovers it, proving the timeout gates
            contended acquisition only and never an uncontended acquire or
            the held section.
        """
        # Arrange
        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace, lock_timeout=0)
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

            # Act
            async with publisher:
                await publisher.publish("worker-added", metadata)

                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker not discovered within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        assert len(events) == 1
        assert events[0].type == "worker-added"
        assert events[0].metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test_publish_should_name_timeout_and_namespace_in_timeout_error(
        self, namespace, metadata, held_lock
    ):
        """Test an elapsed finite timeout raises a message naming the timeout.

        Given:
            A permanently held lock and a Publisher with a small finite
            lock_timeout
        When:
            publish("worker-added", metadata) exhausts the timeout
        Then:
            It should raise TimeoutError whose message names both the
            configured timeout and the namespace.
        """

        # Arrange
        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace, lock_timeout=0.02)

            # Act
            async with publisher:
                with pytest.raises(TimeoutError) as excinfo:
                    await publisher.publish("worker-added", metadata)

        # Assert
        message = str(excinfo.value)
        assert "0.02" in message
        assert namespace in message
        assert "discovery lock" in message

    @pytest.mark.asyncio
    async def test___aexit___should_disarm_atexit_fallbacks_when_workers_still_published(
        self, namespace, atexit_recorder
    ):
        """Test publisher exit finalizes blocks of residual workers.

        Given:
            A Publisher with two published workers that were never
            dropped, with atexit registration wrapped in recording
            pass-throughs
        When:
            The publisher's async with block exits
        Then:
            It should exit cleanly and unregister every per-block
            fallback it registered.
        """
        # Arrange
        registered, unregistered = atexit_recorder
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"localhost:5005{i}",
                pid=123 + i,
                version="1.0",
            )
            for i in range(2)
        ]

        with LocalDiscovery(namespace):
            baseline = len(registered)
            publisher = LocalDiscovery.Publisher(namespace)

            # Act
            async with publisher:
                for worker in workers:
                    await publisher.publish("worker-added", worker)

            # Assert
            block_registered = registered[baseline:]
            assert len(block_registered) == 2
            assert Counter(block_registered) == Counter(unregistered)

    @pytest.mark.asyncio
    async def test___aexit___should_exit_cleanly_when_worker_blocks_already_removed(
        self, namespace, metadata, atexit_recorder, unlink_schedule
    ):
        """Test publisher exit tolerates vanished worker blocks.

        Given:
            A Publisher with a published worker whose per-block
            block is removed out from under it, with atexit
            registration wrapped in recording pass-throughs
        When:
            The publisher's async with block exits
        Then:
            It should exit without raising and pair the block's
            fallback registration with its unregistration.
        """
        # Arrange
        registered, unregistered = atexit_recorder

        with LocalDiscovery(namespace):
            baseline = len(registered)
            publisher = LocalDiscovery.Publisher(namespace)

            # Act
            async with publisher:
                await publisher.publish("worker-added", metadata)
                unlink_schedule.append(FileNotFoundError(2, "No such file or directory"))

            # Assert
            block_registered = registered[baseline:]
            assert block_registered == unregistered
            assert len(block_registered) == 1

    @given(
        ops=st.lists(
            st.tuples(
                st.sampled_from(["add", "drop"]),
                st.integers(min_value=0, max_value=4),
            ),
            max_size=8,
        )
    )
    @settings(
        max_examples=15,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @pytest.mark.asyncio
    async def test_publish_should_pair_atexit_fallbacks_across_add_drop_sequences(
        self, namespace, atexit_recorder, ops
    ):
        """Test arbitrary add/drop sequences never leave armed fallbacks.

        Given:
            An arbitrary sequence of add and drop operations over a
            small worker roster, where adds may re-add currently-live
            workers and drops may target workers that were never
            added, with atexit registration wrapped in recording
            pass-throughs
        When:
            The sequence is published and the publisher then exits
            with any residual workers still registered
        Then:
            It should unwind cleanly and pair every per-block
            fallback registration with exactly one unregistration —
            a re-add registers no second fallback.
        """
        # Arrange — per-example namespace and recorder state so
        # Hypothesis examples stay independent
        registered, unregistered = atexit_recorder
        registered.clear()
        unregistered.clear()
        example_ns = f"{namespace}-{uuid.uuid4().hex[:8]}"
        roster = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"localhost:5005{i}",
                pid=123 + i,
                version="1.0",
            )
            for i in range(5)
        ]
        added = set()

        # Act
        with LocalDiscovery(example_ns):
            baseline = len(registered)
            publisher = LocalDiscovery.Publisher(example_ns)
            async with publisher:
                for op, index in ops:
                    if op == "add":
                        # A re-add of a live worker is legal (#321) and
                        # must register no second fallback.
                        await publisher.publish("worker-added", roster[index])
                        added.add(index)
                    else:
                        await publisher.publish("worker-dropped", roster[index])
                        added.discard(index)

            # Assert
            block_registered = registered[baseline:]
            assert Counter(block_registered) == Counter(unregistered)

    @given(capacity=st.integers(min_value=1, max_value=8))
    @settings(
        max_examples=10,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @pytest.mark.asyncio
    async def test_publish_should_bound_registrations_by_capacity(
        self, namespace, capacity
    ):
        """Test capacity caps the number of registerable workers.

        Given:
            A LocalDiscovery declaring an arbitrary small capacity C
            and its publisher
        When:
            C distinct workers are published, then one more
        Then:
            It should accept all C and raise DiscoveryCapacityExhausted on the
            (C+1)th.
        """
        # Arrange — a per-example namespace so registry state does
        # not carry across Hypothesis examples.
        example_ns = f"{namespace}-{uuid.uuid4().hex[:8]}"
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"localhost:{50051 + i}",
                pid=123 + i,
                version="1.0",
            )
            for i in range(capacity + 1)
        ]

        # Act & assert
        with LocalDiscovery(example_ns, capacity=capacity) as discovery:
            async with discovery.publisher as publisher:
                for worker in workers[:capacity]:
                    await publisher.publish("worker-added", worker)

                with pytest.raises(DiscoveryCapacityExhausted):
                    await publisher.publish("worker-added", workers[capacity])

    @pytest.mark.asyncio
    async def test_publish_should_reject_second_worker_when_capacity_is_one(
        self, namespace
    ):
        """Test a capacity-one registry admits exactly one worker.

        Given:
            A LocalDiscovery(capacity=1) — a single 16-byte slot in a
            page-rounded mapping — and its publisher
        When:
            A first worker is published, then a second
        Then:
            It should register the first and raise DiscoveryCapacityExhausted
            on the second; page rounding grants no extra slot.
        """
        # Arrange
        first = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=123, version="1.0"
        )
        second = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50052", pid=124, version="1.0"
        )

        # Act & assert
        with LocalDiscovery(namespace, capacity=1) as discovery:
            async with discovery.publisher as publisher:
                await publisher.publish("worker-added", first)

                with pytest.raises(DiscoveryCapacityExhausted):
                    await publisher.publish("worker-added", second)

    @pytest.mark.asyncio
    async def test_publish_should_reject_worker_beyond_default_capacity(self, namespace):
        """Test the default capacity admits exactly 128 workers.

        Given:
            A LocalDiscovery constructed with no capacity argument and
            its default publisher
        When:
            128 distinct workers are published, then a 129th
        Then:
            It should accept all 128 and raise DiscoveryCapacityExhausted on
            the 129th, pinning the documented default of 128.
        """
        # Arrange
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"localhost:{50051 + i}",
                pid=123 + i,
                version="1.0",
            )
            for i in range(129)
        ]

        # Act & assert
        with LocalDiscovery(namespace) as discovery:
            async with discovery.publisher as publisher:
                for worker in workers[:128]:
                    await publisher.publish("worker-added", worker)

                with pytest.raises(DiscoveryCapacityExhausted):
                    await publisher.publish("worker-added", workers[128])

    @pytest.mark.asyncio
    async def test_publish_should_free_slot_within_capacity_when_worker_dropped(
        self, namespace
    ):
        """Test dropping a worker frees its capped slot for reuse.

        Given:
            A LocalDiscovery(capacity=1) whose only slot is filled and a
            second add already rejected
        When:
            The first worker is dropped and a second worker is published
        Then:
            It should admit the second worker and make it discoverable —
            the bounded drop freed the single in-cap slot.
        """
        # Arrange
        first = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=123, version="1.0"
        )
        second = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50052", pid=124, version="1.0"
        )

        events = []
        discovered = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                if event.type == "worker-added" and event.metadata.uid == second.uid:
                    events.append(event)
                    discovered.set()
                    break

        with LocalDiscovery(namespace, capacity=1) as discovery:
            async with discovery.publisher as publisher:
                await publisher.publish("worker-added", first)
                with pytest.raises(DiscoveryCapacityExhausted):
                    await publisher.publish("worker-added", second)

                # Act
                await publisher.publish("worker-dropped", first)
                await publisher.publish("worker-added", second)

                # Assert
                subscriber = discovery.subscribe(poll_interval=0.05)
                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(discovered.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Second worker not discoverable after freed slot reuse")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        assert len(events) == 1
        assert events[0].metadata.uid == second.uid

    @pytest.mark.asyncio
    async def test_publish_should_leave_worker_undiscoverable_when_readded_then_dropped(
        self, namespace, metadata
    ):
        """Test one drop fully unregisters a worker added twice.

        Given:
            A worker published "worker-added" twice through one publisher
        When:
            A single "worker-dropped" is published, followed by a
            distinct sentinel worker, and a subscriber observes the
            namespace
        Then:
            It should discover only the sentinel — its arrival proves a
            full scan completed with the re-added worker absent.
        """
        # Arrange
        sentinel = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50052", pid=124, version="1.0"
        )

        events = []
        sentinel_seen = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                if event.metadata.uid == sentinel.uid:
                    sentinel_seen.set()
                    break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            async with publisher:
                await publisher.publish("worker-added", metadata)
                await publisher.publish("worker-added", metadata)

                # Act
                await publisher.publish("worker-dropped", metadata)
                await publisher.publish("worker-added", sentinel)

                # Assert — the sentinel's arrival proves a completed scan
                subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)
                async with _collecting(subscriber, collect):
                    try:
                        await asyncio.wait_for(sentinel_seen.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        pytest.fail("Sentinel worker not discovered")

        assert {event.metadata.uid for event in events} == {sentinel.uid}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("readd_via_second_publisher", [False, True])
    async def test_publish_should_not_consume_slot_when_worker_readded_at_capacity(
        self, namespace, readd_via_second_publisher
    ):
        """Test re-adding the sole registered worker is not exhaustion.

        Given:
            A LocalDiscovery(capacity=1) whose only slot holds a worker,
            and a re-announcement issued through the registering
            publisher or a second publisher on the same namespace
            (parameterized)
        When:
            The same worker is published "worker-added" again, then a
            distinct worker is published
        Then:
            It should accept the re-add without raising and raise
            DiscoveryCapacityExhausted only for the distinct worker — the
            re-add consumed no slot through either publisher.
        """
        # Arrange
        first = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=123, version="1.0"
        )
        second = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50052", pid=124, version="1.0"
        )

        # Act & assert
        with LocalDiscovery(namespace, capacity=1):
            publisher_a = LocalDiscovery.Publisher(namespace)
            publisher_b = LocalDiscovery.Publisher(namespace)
            async with publisher_a, publisher_b:
                readding = publisher_b if readd_via_second_publisher else publisher_a
                await publisher_a.publish("worker-added", first)
                await readding.publish("worker-added", first)

                with pytest.raises(DiscoveryCapacityExhausted):
                    await readding.publish("worker-added", second)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("readd_via_second_publisher", [False, True])
    async def test_publish_should_refresh_metadata_when_worker_readded(
        self, namespace, readd_via_second_publisher
    ):
        """Test a re-add carries its newer metadata to subscribers.

        Given:
            A registered worker and a re-announcement of the same UID
            with a bumped version, issued through the registering
            publisher or a second publisher on the same namespace
            (parameterized)
        When:
            The re-announcement is published as "worker-added"
        Then:
            It should leave the worker discoverable at the bumped
            version.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=123, version="1.0"
        )
        readded = WorkerMetadata(
            uid=worker.uid, address="localhost:50051", pid=123, version="2.0"
        )

        discovered = {}
        target_seen = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                discovered[event.metadata.uid] = event.metadata
                if event.metadata.version == "2.0":
                    target_seen.set()
                    break

        with LocalDiscovery(namespace) as discovery:
            publisher_a = LocalDiscovery.Publisher(namespace)
            publisher_b = LocalDiscovery.Publisher(namespace)
            async with publisher_a, publisher_b:
                readding = publisher_b if readd_via_second_publisher else publisher_a
                await publisher_a.publish("worker-added", worker)

                # Act
                await readding.publish("worker-added", readded)

                # Assert
                subscriber = discovery.subscribe(poll_interval=0.05)
                async with _collecting(subscriber, collect):
                    try:
                        await asyncio.wait_for(target_seen.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        pytest.fail("Re-added worker's metadata not discovered")

        assert discovered[worker.uid].version == "2.0"

    @pytest.mark.asyncio
    async def test_publish_should_disarm_atexit_fallback_when_readded_worker_dropped(
        self, namespace, metadata, atexit_recorder
    ):
        """Test one drop finalizes the block of a worker added twice.

        Given:
            A worker published "worker-added" twice through one
            publisher, with atexit registration wrapped in recording
            pass-throughs
        When:
            A single "worker-dropped" is published
        Then:
            It should register exactly one per-block fallback and
            unregister that same callable — the re-add held no extra
            pool reference to keep the block alive past the drop.
        """
        # Arrange
        registered, unregistered = atexit_recorder

        with LocalDiscovery(namespace):
            baseline = len(registered)
            publisher = LocalDiscovery.Publisher(namespace)
            async with publisher:
                await publisher.publish("worker-added", metadata)
                await publisher.publish("worker-added", metadata)

                # Act
                await publisher.publish("worker-dropped", metadata)

                # Assert
                block_registered = registered[baseline:]
                assert block_registered == unregistered
                assert len(block_registered) == 1

    @pytest.mark.asyncio
    async def test_publish_should_update_worker_when_previously_readded(self, namespace):
        """Test a refreshed registration still applies updates.

        Given:
            A worker registered once and re-announced via "worker-added"
            with a bumped version
        When:
            A "worker-updated" with a further-bumped version is published
        Then:
            It should apply the update to the single registration — a
            subscriber discovers the final version.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=123, version="1.0"
        )
        readded = WorkerMetadata(
            uid=worker.uid, address="localhost:50051", pid=123, version="2.0"
        )
        updated = WorkerMetadata(
            uid=worker.uid, address="localhost:50051", pid=123, version="3.0"
        )

        discovered = {}
        final_seen = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                if event.metadata.uid != worker.uid:
                    continue
                discovered[event.metadata.uid] = event.metadata
                if event.metadata.version == "3.0":
                    final_seen.set()
                    break

        with LocalDiscovery(namespace) as discovery:
            async with discovery.publisher as publisher:
                await publisher.publish("worker-added", worker)
                await publisher.publish("worker-added", readded)

                # Act
                await publisher.publish("worker-updated", updated)

                # Assert
                subscriber = discovery.subscribe(poll_interval=0.05)
                async with _collecting(subscriber, collect):
                    try:
                        await asyncio.wait_for(final_seen.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        pytest.fail("Updated metadata not discovered after re-add")

        assert discovered[worker.uid].version == "3.0"

    @pytest.mark.asyncio
    async def test_publish_should_register_worker_fresh_when_readded_after_drop(
        self, namespace
    ):
        """Test add-after-drop is a fresh registration, not a refresh.

        Given:
            A LocalDiscovery(capacity=1) whose worker was added then
            dropped
        When:
            The same worker is published "worker-added" again, then a
            distinct worker, then the first is dropped and the
            distinct worker is published once more
        Then:
            It should re-register the first worker in the freed slot —
            the distinct worker raises DiscoveryCapacityExhausted while
            the first is live and is admitted after the final drop.
        """
        # Arrange
        first = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=123, version="1.0"
        )
        second = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50052", pid=124, version="1.0"
        )

        # Act & assert
        with LocalDiscovery(namespace, capacity=1) as discovery:
            async with discovery.publisher as publisher:
                await publisher.publish("worker-added", first)
                await publisher.publish("worker-dropped", first)

                await publisher.publish("worker-added", first)
                with pytest.raises(DiscoveryCapacityExhausted):
                    await publisher.publish("worker-added", second)

                await publisher.publish("worker-dropped", first)
                await publisher.publish("worker-added", second)

    @pytest.mark.asyncio
    async def test_publish_should_register_worker_fresh_when_its_own_block_vanished(
        self, namespace, metadata
    ):
        """Test a re-add recreates a block the re-adding publisher lost.

        Given:
            A worker registered through a publisher whose metadata block
            was then unlinked out from under it, as a dead peer's
            teardown does, with that publisher still bound
        When:
            That same publisher publishes "worker-added" for the worker
            again
        Then:
            It should leave the worker discoverable at the re-announced
            metadata, the re-add having registered it fresh rather than
            leaving a registration naming a block no reader can open.
        """
        # Arrange — the re-add comes from the publisher that registered
        # the worker, so its own ledger still holds a handle naming the
        # block that vanished. A re-add from a second publisher, which
        # the test below covers, brings no such handle and cannot reach
        # this path.
        directory = namespace_directory(namespace)
        with LocalDiscovery(namespace) as discovery:
            async with LocalDiscovery.Publisher(namespace) as publisher:
                before = set(directory.iterdir())
                await publisher.publish("worker-added", metadata)
                [block] = set(directory.iterdir()) - before
                block.unlink()

                # Act
                await publisher.publish("worker-added", metadata)

                # Assert
                discovered = set()
                async for event in discovery.subscribe(poll_interval=0.05):
                    discovered.add((event.type, event.metadata.uid))
                    break
                assert discovered == {("worker-added", metadata.uid)}

    @pytest.mark.asyncio
    async def test_publish_should_recreate_the_block_when_it_publishes_to_a_successor(
        self, namespace, metadata
    ):
        """Test a reclaimed block is recreated when publishing to a successor.

        Given:
            A publisher that registered a worker under one owner, whose
            block was then unlinked, and whose owner has exited so a
            successor owner now holds a freshly zeroed registry
        When:
            That publisher publishes "worker-added" for the worker again
        Then:
            It should recreate the block rather than write through the
            handle it still holds on the unlinked file, so the slot it
            claims names a file a subscriber can open.
        """
        # Arrange — the successor's registry has no matching slot, so the
        # recovery path that repairs the same-registry case never fires
        # and the pool would otherwise hand back its cached handle on the
        # unlinked inode. Nothing later repairs it, so the dangling slot
        # would last the worker's lifetime.
        directory = namespace_directory(namespace)
        owner = LocalDiscovery(namespace)
        owner.__enter__()
        async with LocalDiscovery.Publisher(namespace) as publisher:
            before = set(directory.iterdir())
            await publisher.publish("worker-added", metadata)
            [block] = set(directory.iterdir()) - before
            block.unlink()
            owner.__exit__(None, None, None)

            with LocalDiscovery(namespace) as successor:
                # Act
                await publisher.publish("worker-added", metadata)

                # Assert
                discovered = None
                async with asyncio.timeout(10):
                    async for event in successor.subscribe(poll_interval=0.05):
                        discovered = event
                        break
                assert discovered is not None
                assert discovered.metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test_publish_should_register_both_when_concurrent_on_one_publisher(
        self, namespace, metadata
    ):
        """Test two concurrent publishes on one publisher both register.

        Given:
            A publisher whose registered worker's block was unlinked out
            from under it, so a re-add reaches the recovery path that
            suspends inside the registry's critical section
        When:
            That re-add and a second worker's registration run
            concurrently on the same publisher
        Then:
            It should leave both workers discoverable, since a publish
            holds the registry lock for the whole of its own critical
            section rather than releasing a peer's.
        """
        # Arrange — the re-add must be gather's first argument: it is the
        # only one of the two that reaches a suspension point inside the
        # held section, so the interleaving this pins does not occur with
        # the order reversed. Capacity stays at its default, or the
        # second publish is refused before it can race.
        other = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50052", pid=124, version="1.0"
        )
        directory = namespace_directory(namespace)

        with LocalDiscovery(namespace) as discovery:
            async with LocalDiscovery.Publisher(namespace) as publisher:
                before = set(directory.iterdir())
                await publisher.publish("worker-added", metadata)
                [block] = set(directory.iterdir()) - before
                block.unlink()

                # Act
                await asyncio.gather(
                    publisher.publish("worker-added", metadata),
                    publisher.publish("worker-added", other),
                )

                # Assert — bounded, because a lost worker leaves the
                # survivor's stream running rather than ending it.
                discovered = set()
                async with asyncio.timeout(10):
                    async for event in discovery.subscribe(poll_interval=0.05):
                        discovered.add(event.metadata.uid)
                        if discovered == {metadata.uid, other.uid}:
                            break
                assert discovered == {metadata.uid, other.uid}

    @pytest.mark.asyncio
    async def test_publish_should_register_worker_fresh_when_block_vanished(
        self, namespace
    ):
        """Test a re-add reclaims a slot whose block has vanished.

        Given:
            A worker registered through a publisher that exited its
            context without dropping the worker, leaving its slot
            populated but its metadata block unlinked
        When:
            A second publisher publishes "worker-added" for the same UID
            with a bumped version
        Then:
            It should reclaim the stale registration and register the
            worker fresh, leaving it discoverable at the bumped version.
        """
        # Arrange — the exiting publisher unlinks its blocks but leaves
        # the address-space slot populated.
        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=123, version="1.0"
        )
        readded = WorkerMetadata(
            uid=worker.uid, address="localhost:50051", pid=123, version="2.0"
        )

        discovered = {}
        target_seen = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                discovered[event.metadata.uid] = event.metadata
                if event.metadata.version == "2.0":
                    target_seen.set()
                    break

        with LocalDiscovery(namespace) as discovery:
            publisher_a = LocalDiscovery.Publisher(namespace)
            async with publisher_a:
                await publisher_a.publish("worker-added", worker)

            publisher_b = LocalDiscovery.Publisher(namespace)
            async with publisher_b:
                # Act
                await publisher_b.publish("worker-added", readded)

                # Assert
                subscriber = discovery.subscribe(poll_interval=0.05)
                async with _collecting(subscriber, collect):
                    try:
                        await asyncio.wait_for(target_seen.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        pytest.fail("Worker not re-registered over vanished block")

        assert discovered[worker.uid].version == "2.0"

    @pytest.mark.asyncio
    async def test_publish_should_unregister_worker_when_dropped_by_second_publisher(
        self, namespace
    ):
        """Test the refreshing publisher's drop unregisters the worker.

        Given:
            A worker registered through publisher A and refreshed with
            a bumped version by publisher B on the same namespace
        When:
            B publishes a single "worker-dropped" for that worker
        Then:
            It should emit worker-dropped to a subscriber and leave the
            worker unregistered, with both publishers exiting cleanly.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=123, version="1.0"
        )
        readded = WorkerMetadata(
            uid=worker.uid, address="localhost:50051", pid=123, version="2.0"
        )

        refreshed = asyncio.Event()
        dropped = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                if event.metadata.uid != worker.uid:
                    continue
                if event.type == "worker-dropped":
                    dropped.set()
                    break
                if event.metadata.version == "2.0":
                    refreshed.set()

        with LocalDiscovery(namespace) as discovery:
            publisher_a = LocalDiscovery.Publisher(namespace)
            publisher_b = LocalDiscovery.Publisher(namespace)
            async with publisher_a, publisher_b:
                await publisher_a.publish("worker-added", worker)
                await publisher_b.publish("worker-added", readded)

                subscriber = discovery.subscribe(poll_interval=0.05)
                async with _collecting(subscriber, collect):
                    try:
                        await asyncio.wait_for(refreshed.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        pytest.fail("Refreshed worker not discoverable")

                    # Act
                    await publisher_b.publish("worker-dropped", worker)

                    # Assert
                    try:
                        await asyncio.wait_for(dropped.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        pytest.fail("Drop by refreshing publisher not observed")

    @pytest.mark.asyncio
    async def test_publish_should_update_worker_when_registered_by_second_publisher(
        self, namespace
    ):
        """Test an update reaches a worker another publisher registered.

        Given:
            A worker registered through publisher A and a second
            publisher B on the same namespace
        When:
            B publishes "worker-updated" for that UID with a bumped
            version
        Then:
            It should apply the update — a subscriber discovers the
            bumped version.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=123, version="1.0"
        )
        updated = WorkerMetadata(
            uid=worker.uid, address="localhost:50051", pid=123, version="2.0"
        )

        discovered = {}
        target_seen = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                discovered[event.metadata.uid] = event.metadata
                if event.metadata.version == "2.0":
                    target_seen.set()
                    break

        with LocalDiscovery(namespace) as discovery:
            publisher_a = LocalDiscovery.Publisher(namespace)
            publisher_b = LocalDiscovery.Publisher(namespace)
            async with publisher_a, publisher_b:
                await publisher_a.publish("worker-added", worker)

                # Act
                await publisher_b.publish("worker-updated", updated)

                # Assert
                subscriber = discovery.subscribe(poll_interval=0.05)
                async with _collecting(subscriber, collect):
                    try:
                        await asyncio.wait_for(target_seen.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        pytest.fail("Second publisher's update not discovered")

        assert discovered[worker.uid].version == "2.0"

    @pytest.mark.asyncio
    async def test_publish_should_disarm_atexit_fallback_when_updated_worker_dropped(
        self, namespace, metadata, atexit_recorder
    ):
        """Test an update leaves the drop able to finalize the block.

        Given:
            A registered worker whose metadata has since been published
            as "worker-updated", with atexit registration wrapped in
            recording pass-throughs
        When:
            A single "worker-dropped" is published
        Then:
            It should register exactly one per-block fallback and
            unregister that same callable — the update held no extra
            pool reference to keep the block alive past the drop.
        """
        # Arrange
        registered, unregistered = atexit_recorder
        updated = WorkerMetadata(
            uid=metadata.uid, address="localhost:50051", pid=12345, version="2.0"
        )

        with LocalDiscovery(namespace):
            baseline = len(registered)
            publisher = LocalDiscovery.Publisher(namespace)
            async with publisher:
                await publisher.publish("worker-added", metadata)
                await publisher.publish("worker-updated", updated)

                # Act
                await publisher.publish("worker-dropped", metadata)

                # Assert
                block_registered = registered[baseline:]
                assert block_registered == unregistered
                assert len(block_registered) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("readd_via_second_publisher", [False, True])
    async def test_publish_should_preserve_prior_state_when_readd_overflows(
        self, namespace, readd_via_second_publisher
    ):
        """Test an oversized re-add preserves the prior registration.

        Given:
            A worker registered through a small-block publisher and an
            oversized re-announcement of the same UID, issued through the
            registering publisher or a second publisher with the default
            block size (parameterized)
        When:
            The oversized re-announcement is published "worker-added"
        Then:
            It should raise DiscoveryBlockExhausted and leave the worker
            discoverable with its original metadata intact — the
            rollback holds whichever publisher issues the refresh.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=123, version="1.0"
        )
        oversized = WorkerMetadata(
            uid=worker.uid,
            address="localhost:50051",
            pid=123,
            version="2.0",
            extra=MappingProxyType({"data": "x" * 20000}),
        )

        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        with LocalDiscovery(namespace):
            publisher_a = LocalDiscovery.Publisher(namespace, block_size=100)
            publisher_b = LocalDiscovery.Publisher(namespace)
            async with publisher_a, publisher_b:
                readding = publisher_b if readd_via_second_publisher else publisher_a
                await publisher_a.publish("worker-added", worker)

                # Act
                with pytest.raises(DiscoveryBlockExhausted):
                    await readding.publish("worker-added", oversized)

                # Assert — original metadata preserved via subscriber
                subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)
                async with _collecting(subscriber, collect):
                    try:
                        await asyncio.wait_for(event_received.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        pytest.fail("Worker not discoverable after failed re-add")

        assert len(events) == 1
        assert events[0].metadata.uid == worker.uid
        assert events[0].metadata.version == "1.0"

    @pytest.mark.asyncio
    async def test_publish_should_free_slot_when_worker_dropped_after_failed_readd(
        self, namespace
    ):
        """Test a failed re-add leaves one drop able to free the slot.

        Given:
            A LocalDiscovery(capacity=1) whose worker's oversized
            re-announcement has failed with DiscoveryBlockExhausted
        When:
            A single "worker-dropped" is published and a distinct
            fitting worker is then published
        Then:
            It should admit the distinct worker — the failed refresh
            consumed no extra slot or reference, so one drop fully
            unregisters.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=123, version="1.0"
        )
        oversized = WorkerMetadata(
            uid=worker.uid,
            address="localhost:50051",
            pid=123,
            version="2.0",
            extra=MappingProxyType({"data": "x" * 20000}),
        )
        distinct = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50052", pid=124, version="1.0"
        )

        # Act & assert
        with LocalDiscovery(namespace, capacity=1):
            publisher = LocalDiscovery.Publisher(namespace, block_size=100)
            async with publisher:
                await publisher.publish("worker-added", worker)
                with pytest.raises(DiscoveryBlockExhausted):
                    await publisher.publish("worker-added", oversized)

                await publisher.publish("worker-dropped", worker)
                await publisher.publish("worker-added", distinct)

    @pytest.mark.asyncio
    async def test_publish_should_disarm_atexit_fallback_when_readd_overflows(
        self, namespace, atexit_recorder
    ):
        """Test a failed refresh leaves the atexit lifecycle paired.

        Given:
            A registered worker whose oversized re-announcement has
            failed with DiscoveryBlockExhausted, with atexit registration wrapped
            in recording pass-throughs
        When:
            A single "worker-dropped" is published
        Then:
            It should have registered exactly one per-block fallback —
            from the original add, none from the failed refresh — and
            unregistered that same callable at the drop.
        """
        # Arrange
        registered, unregistered = atexit_recorder
        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=123, version="1.0"
        )
        oversized = WorkerMetadata(
            uid=worker.uid,
            address="localhost:50051",
            pid=123,
            version="2.0",
            extra=MappingProxyType({"data": "x" * 20000}),
        )

        with LocalDiscovery(namespace):
            baseline = len(registered)
            publisher = LocalDiscovery.Publisher(namespace, block_size=100)
            async with publisher:
                await publisher.publish("worker-added", worker)
                with pytest.raises(DiscoveryBlockExhausted):
                    await publisher.publish("worker-added", oversized)

                # Act
                await publisher.publish("worker-dropped", worker)

                # Assert
                block_registered = registered[baseline:]
                assert block_registered == unregistered
                assert len(block_registered) == 1

    @pytest.mark.asyncio
    async def test_publish_should_update_worker_within_capacity(self, namespace):
        """Test updating a worker in the last in-cap slot succeeds.

        Given:
            A LocalDiscovery(capacity=2) holding a filler in slot 0 and
            the target worker in the last in-cap slot
        When:
            The target is republished as "worker-updated" with a bumped
            version
        Then:
            It should locate the target within the bounded scan (no
            DiscoveryWorkerNotFound) and make the new version discoverable.
        """
        # Arrange
        filler = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=123, version="1.0"
        )
        target = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50052", pid=124, version="1.0"
        )
        updated = WorkerMetadata(
            uid=target.uid, address="localhost:50052", pid=124, version="2.0"
        )

        discovered = {}
        target_seen = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                discovered[event.metadata.uid] = event.metadata
                if event.metadata.uid == target.uid:
                    target_seen.set()
                    break

        with LocalDiscovery(namespace, capacity=2) as discovery:
            async with discovery.publisher as publisher:
                await publisher.publish("worker-added", filler)
                await publisher.publish("worker-added", target)

                # Act
                await publisher.publish("worker-updated", updated)

                # Assert
                subscriber = discovery.subscribe(poll_interval=0.05)
                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(target_seen.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Updated worker not discoverable")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        assert discovered[target.uid].version == "2.0"

    @pytest.mark.asyncio
    async def test_publish_should_enforce_the_owner_capacity_for_a_borrower(
        self, namespace
    ):
        """Test a borrowing publisher enforces the owner's stamped capacity.

        Given:
            An owner LocalDiscovery holding the registry at capacity 1
            and a Publisher borrowing that registry
        When:
            The borrower publishes a first worker, then a second
        Then:
            It should admit the first and raise DiscoveryCapacityExhausted
            on the second — the owner's stamped cap of 1 governs, and a
            borrower takes no capacity of its own with which to override
            it.
        """
        # Arrange
        worker_a = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=1, version="1.0"
        )
        worker_b = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50052", pid=2, version="1.0"
        )

        # Act & assert
        with LocalDiscovery(namespace, capacity=1):
            async with LocalDiscovery.Publisher(namespace) as publisher:
                await publisher.publish("worker-added", worker_a)

                with pytest.raises(DiscoveryCapacityExhausted):
                    await publisher.publish("worker-added", worker_b)

    @given(owner_cap=st.integers(min_value=1, max_value=8))
    @settings(
        max_examples=15,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @pytest.mark.asyncio
    async def test_publish_should_bound_borrower_registrations_by_owner_capacity(
        self, namespace, owner_cap
    ):
        """Test the owner's stamped capacity bounds a borrowing publisher.

        Given:
            An owner declaring an arbitrary capacity and a Publisher
            borrowing the registry it stamped
        When:
            The borrower publishes owner-capacity workers, then one more
        Then:
            It should admit exactly the owner's capacity and raise "No
            available slots" on the next — the registry header is the
            single source of truth for every borrower of it.
        """
        # Arrange — a per-example namespace so registry state does not
        # carry across Hypothesis examples.
        example_ns = f"{namespace}-{uuid.uuid4().hex[:8]}"
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"localhost:{50051 + i}",
                pid=100 + i,
                version="1.0",
            )
            for i in range(owner_cap + 1)
        ]

        # Act & assert
        with LocalDiscovery(example_ns, capacity=owner_cap):
            async with LocalDiscovery.Publisher(example_ns) as publisher:
                for worker in workers[:owner_cap]:
                    await publisher.publish("worker-added", worker)

                with pytest.raises(DiscoveryCapacityExhausted):
                    await publisher.publish("worker-added", workers[owner_cap])

    @given(
        capacity=st.integers(min_value=1, max_value=4),
        ops=st.lists(
            st.tuples(
                st.sampled_from(["add", "drop"]),
                st.integers(min_value=0, max_value=5),
            ),
            max_size=12,
        ),
    )
    @settings(
        max_examples=15,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @pytest.mark.asyncio
    async def test_publish_should_conserve_slots_when_workers_readded_across_capacities(
        self, namespace, capacity, ops
    ):
        """Test re-adds conserve slots for any capacity and op sequence.

        Given:
            A LocalDiscovery declaring an arbitrary small capacity C, a
            six-worker roster, and an arbitrary sequence of add and
            drop operations in which adds may re-add currently-live
            workers
        When:
            The sequence is published and fresh distinct workers are
            then published until the registry is exhausted
        Then:
            It should admit exactly C minus the live count of fresh
            workers before raising DiscoveryCapacityExhausted — re-adds
            consumed no slots and every drop reclaimed exactly one.
        """
        # Arrange — a per-example namespace so registry state does
        # not carry across Hypothesis examples.
        example_ns = f"{namespace}-{uuid.uuid4().hex[:8]}"
        roster = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"localhost:{50051 + i}",
                pid=100 + i,
                version="1.0",
            )
            for i in range(6)
        ]
        live: set[int] = set()

        # Act & assert
        with LocalDiscovery(example_ns, capacity=capacity) as discovery:
            async with discovery.publisher as publisher:
                for action, index in ops:
                    if action == "add":
                        if index in live or len(live) < capacity:
                            await publisher.publish("worker-added", roster[index])
                            live.add(index)
                        else:
                            with pytest.raises(DiscoveryCapacityExhausted):
                                await publisher.publish("worker-added", roster[index])
                    elif index in live:
                        await publisher.publish("worker-dropped", roster[index])
                        live.discard(index)

                # Act & assert — flush the remaining capacity with fresh workers
                for i in range(capacity - len(live)):
                    await publisher.publish(
                        "worker-added",
                        WorkerMetadata(
                            uid=uuid.uuid4(),
                            address=f"localhost:{60000 + i}",
                            pid=900 + i,
                            version="1.0",
                        ),
                    )
                with pytest.raises(DiscoveryCapacityExhausted):
                    await publisher.publish(
                        "worker-added",
                        WorkerMetadata(
                            uid=uuid.uuid4(),
                            address="localhost:60099",
                            pid=999,
                            version="1.0",
                        ),
                    )

    @given(
        variants=st.lists(
            st.tuples(st.booleans(), st.integers(min_value=0, max_value=999)),
            max_size=6,
        )
    )
    @settings(
        max_examples=10,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @pytest.mark.asyncio
    async def test_publish_should_converge_on_last_fitting_metadata_when_readded(
        self, namespace, variants
    ):
        """Test arbitrary refresh chains converge on the last fitting write.

        Given:
            A registered worker in a small-block publisher and an
            arbitrary chain of re-announcements of its UID, each either
            fitting the block or exceeding it
        When:
            Each re-announcement is published as "worker-added" in
            order, the oversized ones raising DiscoveryBlockExhausted
        Then:
            It should leave the worker discoverable exactly once with
            the metadata of the last fitting write — failed refreshes
            never corrupt or regress the registration.
        """
        # Arrange — a per-example namespace so registry state does
        # not carry across Hypothesis examples.
        example_ns = f"{namespace}-{uuid.uuid4().hex[:8]}"
        uid = uuid.uuid4()
        worker = WorkerMetadata(
            uid=uid, address="localhost:50051", pid=123, version="1.0"
        )
        expected_version = "1.0"

        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        with LocalDiscovery(example_ns):
            publisher = LocalDiscovery.Publisher(example_ns, block_size=100)
            async with publisher:
                await publisher.publish("worker-added", worker)

                # Act
                for fits, n in variants:
                    if fits:
                        variant = WorkerMetadata(
                            uid=uid,
                            address="localhost:50051",
                            pid=123,
                            version=f"2.{n}",
                        )
                        await publisher.publish("worker-added", variant)
                        expected_version = f"2.{n}"
                    else:
                        variant = WorkerMetadata(
                            uid=uid,
                            address="localhost:50051",
                            pid=123,
                            version=f"2.{n}",
                            extra=MappingProxyType({"data": "x" * 20000}),
                        )
                        with pytest.raises(DiscoveryBlockExhausted):
                            await publisher.publish("worker-added", variant)

                # Assert
                subscriber = LocalDiscovery.Subscriber(example_ns, poll_interval=0.05)
                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker not discoverable after refresh chain")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        assert len(events) == 1
        assert events[0].metadata.uid == uid
        assert events[0].metadata.version == expected_version

    @given(
        ops=st.lists(
            st.tuples(
                st.sampled_from(["add", "drop"]),
                st.integers(min_value=0, max_value=4),
                st.booleans(),
            ),
            min_size=1,
            max_size=20,
        )
    )
    @settings(
        max_examples=15,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @pytest.mark.asyncio
    async def test_publish_should_bound_registrations_when_two_publishers_interleave(
        self, namespace, ops
    ):
        """Test two publishers share one re-add contract on a namespace.

        Given:
            A capacity-3 discovery, a five-worker roster, and an
            arbitrary add/drop sequence in which each add draws which
            of two publishers issues it, re-adds of live workers going
            through either publisher and drops through the worker's
            registering publisher
        When:
            The operations are published serially
        Then:
            It should accept an add exactly when the worker is live or
            fewer than three are live and raise
            DiscoveryCapacityExhausted when three others are live —
            publisher identity never affects whether an add refreshes
            or registers.
        """
        # Arrange — a per-example namespace so registry state does
        # not carry across Hypothesis examples.
        capacity = 3
        example_ns = f"{namespace}-{uuid.uuid4().hex[:8]}"
        roster = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"localhost:{50051 + i}",
                pid=100 + i,
                version="1.0",
            )
            for i in range(5)
        ]
        # Drops route through each worker's registering publisher: a
        # foreign drop leaves the registrar's pool entry live — a
        # pre-existing reclamation gap tracked as a follow-up to #321 —
        # which would skew the model's slot accounting.
        registrar: dict[int, LocalDiscovery.Publisher] = {}

        # Act & assert
        with LocalDiscovery(example_ns, capacity=capacity):
            publisher_a = LocalDiscovery.Publisher(example_ns)
            publisher_b = LocalDiscovery.Publisher(example_ns)
            async with publisher_a, publisher_b:
                for action, index, use_b in ops:
                    chosen = publisher_b if use_b else publisher_a
                    if action == "add":
                        if index in registrar:
                            await chosen.publish("worker-added", roster[index])
                        elif len(registrar) < capacity:
                            await chosen.publish("worker-added", roster[index])
                            registrar[index] = chosen
                        else:
                            with pytest.raises(DiscoveryCapacityExhausted):
                                await chosen.publish("worker-added", roster[index])
                    elif index in registrar:
                        publisher = registrar.pop(index)
                        await publisher.publish("worker-dropped", roster[index])


class TestWorkerReference:
    """Tests for the internal _WorkerReference value object."""

    def test_is_hashable_by_its_uuid(self):
        """Test a _WorkerReference hashes by its UUID.

        Given:
            A _WorkerReference wrapping a UUID.
        When:
            It is hashed.
        Then:
            Its hash should equal the UUID's hash — references are
            usable as dict keys / set members keyed by worker identity.
        """
        # Arrange
        from wool.runtime.discovery.local import _WorkerReference

        uid = uuid.uuid4()

        # Act & assert
        assert hash(_WorkerReference(uid)) == hash(uid)


class TestLocalDiscoverySubscriber:
    """Tests for LocalDiscovery.Subscriber class.

    Fully qualified name:
    wool.runtime.discovery.local.LocalDiscovery.Subscriber
    """

    def test___init___should_raise_when_capacity_is_declared(self, namespace):
        """Test a borrower is offered no capacity of its own.

        Given:
            A namespace whose capacity is the owner's to stamp
        When:
            A Subscriber is constructed with a capacity argument
        Then:
            It should raise TypeError naming the rejected argument —
            capacity is read from the registry the owner stamped, and a
            borrower has no way to declare or override it.
        """
        # Act & assert
        with pytest.raises(TypeError, match="capacity"):
            LocalDiscovery.Subscriber(namespace, capacity=128)  # type: ignore[call-arg]

    def test___new___should_return_a_subscriber_protocol_not_the_class(self, namespace):
        """Test constructing a Subscriber yields a DiscoverySubscriberLike.

        Given:
            A namespace and a poll interval.
        When:
            A LocalDiscovery.Subscriber is constructed.
        Then:
            It should return an object satisfying DiscoverySubscriberLike
            that is not a LocalDiscovery.Subscriber and exposes no
            namespace attribute.
        """
        # Act
        subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

        # Assert
        assert isinstance(subscriber, DiscoverySubscriberLike)
        assert not isinstance(subscriber, LocalDiscovery.Subscriber)
        assert not hasattr(subscriber, "namespace")

    @pytest.mark.asyncio
    async def test___aiter___with_negative_poll_interval(self, namespace):
        """Test iteration rejects negative poll_interval.

        Given:
            A subscriber constructed with a negative ``poll_interval``
            (the metaclass defers ``__init__`` until the resource pool
            factory fires on first iteration)
        When:
            The caller starts iterating the subscriber
        Then:
            It should raise :class:`ValueError` naming the bad value.
        """
        # Arrange
        with LocalDiscovery(namespace):
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=-0.1)

            # Act & assert
            with pytest.raises(ValueError, match=r"positive poll interval.*-0\.1"):
                await anext(aiter(subscriber))

    @pytest.mark.asyncio
    async def test___aiter___should_reject_a_namespace_that_leaves_its_root(self):
        """Test iteration rejects a namespace that is not one path component.

        Given:
            A subscriber constructed on a namespace containing a path
            separator (the metaclass defers ``__init__`` until the
            resource pool factory fires on first iteration)
        When:
            The caller starts iterating the subscriber
        Then:
            It should raise ValueError from the iteration rather than
            from the constructor, since that is where construction
            happens, and never open a path outside the module's root.
        """
        # Arrange
        subscriber = LocalDiscovery.Subscriber("probeX/../victim")

        # Act & assert
        with pytest.raises(ValueError, match="namespace"):
            await anext(aiter(subscriber))

    @pytest.mark.asyncio
    async def test___aiter___should_raise_when_namespace_has_no_owner(self, namespace):
        """Test a subscriber never creates the registry it borrows.

        Given:
            A Subscriber on a namespace no LocalDiscovery has entered
        When:
            The subscriber is iterated
        Then:
            It should raise DiscoveryNamespaceNotFound naming the
            namespace, having created neither a registry nor the
            notification directory a bound subscriber watches.
        """
        # Arrange
        subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

        # Act & assert
        with pytest.raises(DiscoveryNamespaceNotFound) as excinfo:
            async for _ in subscriber:
                pytest.fail("Subscriber yielded an event without an owner")

        assert excinfo.value.namespace == namespace

        # Assert — the rejected bind created nothing: no notification
        # directory for a namespace that never had an owner, and no
        # registry, so the namespace is still free for an owner to claim.
        assert not (Path(tempfile.gettempdir()).resolve() / f"wool-{namespace}").exists()
        with LocalDiscovery(namespace) as owner:
            assert owner.namespace == namespace

    @pytest.mark.asyncio
    async def test___aiter___should_raise_when_the_owner_has_exited(self, namespace):
        """Test a borrower binding after the reclaim is rejected.

        Given:
            A namespace whose owner has entered and exited, with no
            iteration on it outstanding
        When:
            A Subscriber on that namespace is iterated
        Then:
            It should raise DiscoveryNamespaceNotFound naming the
            namespace.
        """
        # Arrange — no iteration is left live, so this iteration binds
        # (see `LocalDiscovery.Subscriber`).
        with LocalDiscovery(namespace):
            pass

        # Act & assert
        with pytest.raises(DiscoveryNamespaceNotFound) as excinfo:
            async for _ in LocalDiscovery.Subscriber(namespace, poll_interval=0.05):
                pytest.fail("Subscriber yielded an event after the reclaim")

        assert excinfo.value.namespace == namespace

    @pytest.mark.asyncio
    async def test___aiter___should_bind_when_a_new_owner_claims_the_namespace(
        self, namespace, metadata
    ):
        """Test a rejected borrower recovers under a new owner.

        Given:
            A Subscriber whose iteration was rejected because no owner
            held its namespace
        When:
            A new owner claims that namespace, publishes a worker, and
            the same subscriber is iterated again
        Then:
            It should discover the worker.
        """
        # Arrange
        subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)
        with pytest.raises(DiscoveryNamespaceNotFound):
            async for _ in subscriber:
                pytest.fail("Subscriber yielded an event with no owner")

        # Act & assert — the same handle, under a new owner
        with LocalDiscovery(namespace):
            async with LocalDiscovery.Publisher(namespace) as publisher:
                await publisher.publish("worker-added", metadata)

                discovered = None
                async for event in subscriber:
                    discovered = event
                    break

        assert discovered is not None
        assert discovered.type == "worker-added"
        assert discovered.metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test___aiter___should_raise_file_not_found_when_a_block_vanished(
        self, namespace
    ):
        """Test a vanished worker block is not reported as a lost registry.

        Given:
            A live owner whose registry holds a slot for a worker whose
            metadata block was unlinked by the publisher that created it
        When:
            A Subscriber on that namespace scans
        Then:
            It should raise FileNotFoundError — the registry is intact
            and only one worker's block is gone, so reporting the
            namespace as missing would misdiagnose it.
        """
        # Arrange — the exiting publisher unlinks its blocks but leaves
        # the address-space slot populated.
        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=123, version="1.0"
        )

        with LocalDiscovery(namespace):
            async with LocalDiscovery.Publisher(namespace) as publisher:
                await publisher.publish("worker-added", worker)

            # Act & assert
            with pytest.raises(FileNotFoundError):
                async for _ in LocalDiscovery.Subscriber(namespace, poll_interval=0.05):
                    pytest.fail("Subscriber yielded an event for a vanished block")

    @pytest.mark.asyncio
    async def test___aiter___discovers_added_worker(self, namespace, metadata):
        """Test async for yields worker-added event.

        Given:
            A published worker
        When:
            Subscriber is iterated via async for
        Then:
            It should yield a worker-added event with matching
            metadata.
        """
        # Arrange
        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

            async with publisher:
                await publisher.publish("worker-added", metadata)

                # Act
                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker not discovered within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        assert len(events) == 1
        assert events[0].type == "worker-added"
        assert events[0].metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test___aiter___detects_dropped_worker(self, namespace, metadata):
        """Test async for yields worker-dropped event.

        Given:
            A published then dropped worker
        When:
            Subscriber is iterated via async for
        Then:
            It should yield a worker-dropped event.
        """
        # Arrange
        events = []
        worker_added = asyncio.Event()
        worker_dropped = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                if event.type == "worker-added" and event.metadata.uid == metadata.uid:
                    worker_added.set()
                elif (
                    event.type == "worker-dropped" and event.metadata.uid == metadata.uid
                ):
                    worker_dropped.set()
                    break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

            async with publisher:
                task = asyncio.create_task(collect(subscriber))

                await publisher.publish("worker-added", metadata)
                await asyncio.wait_for(worker_added.wait(), timeout=2.0)

                # Act
                await publisher.publish("worker-dropped", metadata)

                try:
                    await asyncio.wait_for(worker_dropped.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker drop not detected within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        dropped = [e for e in events if e.type == "worker-dropped"]
        assert len(dropped) >= 1
        assert dropped[0].metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test___aiter___detects_updated_worker(self, namespace, metadata):
        """Test async for yields worker-updated event.

        Given:
            A published then updated worker
        When:
            Subscriber is iterated via async for
        Then:
            It should yield a worker-updated event with new metadata.
        """
        # Arrange
        updated_worker = WorkerMetadata(
            uid=metadata.uid,
            address="newhost:9999",
            pid=99999,
            version="2.0.0",
        )
        events = []
        worker_added = asyncio.Event()
        worker_updated = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                if event.type == "worker-added" and event.metadata.uid == metadata.uid:
                    worker_added.set()
                elif (
                    event.type == "worker-updated"
                    and event.metadata.uid == metadata.uid
                    and event.metadata.version == "2.0.0"
                ):
                    worker_updated.set()
                    break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

            async with publisher:
                task = asyncio.create_task(collect(subscriber))

                await publisher.publish("worker-added", metadata)
                await asyncio.wait_for(worker_added.wait(), timeout=2.0)

                # Act
                await publisher.publish("worker-updated", updated_worker)

                try:
                    await asyncio.wait_for(worker_updated.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker update not detected within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        updated = [
            e
            for e in events
            if e.type == "worker-updated" and e.metadata.version == "2.0.0"
        ]
        assert len(updated) >= 1
        assert updated[0].metadata.version == "2.0.0"

    @pytest.mark.asyncio
    async def test___aiter___with_filter_predicate(self, namespace):
        """Test async for with filter predicate.

        Given:
            A subscriber with a filter predicate
        When:
            Workers matching and not matching the filter are published
        Then:
            It should yield only matching workers in the event stream.
        """
        # Arrange
        worker_match = WorkerMetadata(
            uid=uuid.uuid4(),
            address="host1:50051",
            pid=111,
            version="1.0",
        )
        worker_no_match = WorkerMetadata(
            uid=uuid.uuid4(),
            address="host2:9999",
            pid=222,
            version="1.0",
        )

        def filter_fn(w):
            return w.address == "host1:50051"

        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = afilter(
                filter_fn,
                LocalDiscovery.Subscriber(namespace, poll_interval=0.05),
            )

            async with publisher:
                task = asyncio.create_task(collect(subscriber))
                await asyncio.sleep(0.05)

                await publisher.publish("worker-added", worker_match)
                await publisher.publish("worker-added", worker_no_match)

                try:
                    await asyncio.wait_for(event_received.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
                await asyncio.sleep(0.1)

                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Assert
        assert len(events) >= 1
        assert all(e.metadata.address == "host1:50051" for e in events)

    @pytest.mark.asyncio
    async def test___aiter___with_poll_interval(self, namespace):
        """Test subscriber discovers workers within poll window.

        Given:
            A subscriber with poll_interval set
        When:
            A worker is published
        Then:
            It should yield the event within the poll window.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
        )

        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.1)

            async with publisher:
                await publisher.publish("worker-added", worker)

                # Act
                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker not discovered within poll window")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        assert len(events) >= 1
        assert events[0].type == "worker-added"

    @pytest.mark.asyncio
    async def test___aiter___with_concurrent_namespaces(self):
        """Test concurrent subscribers on different namespaces do not collide.

        Given:
            Two LocalDiscovery instances with different namespaces
        When:
            Both subscribers async-iterate simultaneously
        Then:
            It should deliver events to both without RuntimeError or
            BlockingIOError.
        """
        # Arrange
        ns_a = f"test-concurrent-a-{uuid.uuid4()}"
        ns_b = f"test-concurrent-b-{uuid.uuid4()}"
        worker_a = WorkerMetadata(
            uid=uuid.uuid4(),
            address="host-a:50051",
            pid=111,
            version="1.0",
        )
        worker_b = WorkerMetadata(
            uid=uuid.uuid4(),
            address="host-b:50052",
            pid=222,
            version="1.0",
        )

        events_a: list = []
        events_b: list = []
        received_a = asyncio.Event()
        received_b = asyncio.Event()
        started_a = asyncio.Event()
        started_b = asyncio.Event()

        async def collect(subscriber, events, received, started):
            started.set()
            async for event in subscriber:
                events.append(event)
                received.set()
                break

        with LocalDiscovery(ns_a) as discovery_a, LocalDiscovery(ns_b) as discovery_b:
            publisher_a = discovery_a.publisher
            publisher_b = discovery_b.publisher
            subscriber_a = discovery_a.subscribe(poll_interval=0.05)
            subscriber_b = discovery_b.subscribe(poll_interval=0.05)

            async with publisher_a, publisher_b:
                task_a = asyncio.create_task(
                    collect(subscriber_a, events_a, received_a, started_a)
                )
                task_b = asyncio.create_task(
                    collect(subscriber_b, events_b, received_b, started_b)
                )
                await asyncio.gather(started_a.wait(), started_b.wait())

                # Act
                await publisher_a.publish("worker-added", worker_a)
                await publisher_b.publish("worker-added", worker_b)

                try:
                    await asyncio.wait_for(
                        asyncio.gather(received_a.wait(), received_b.wait()),
                        timeout=2.0,
                    )
                except asyncio.TimeoutError:
                    pytest.fail("Concurrent subscribers did not both receive events")
                finally:
                    for t in (task_a, task_b):
                        t.cancel()
                        try:
                            await t
                        except asyncio.CancelledError:
                            pass

        # Assert
        assert len(events_a) == 1
        assert events_a[0].metadata.uid == worker_a.uid
        assert len(events_b) == 1
        assert events_b[0].metadata.uid == worker_b.uid

    @pytest.mark.asyncio
    async def test___aiter___with_multiple_subscribers_same_namespace(
        self, namespace, metadata
    ):
        """Test two subscribers on the same namespace receive events independently.

        Given:
            Two Subscribers on the same namespace
        When:
            A worker is published
        Then:
            It should deliver the worker-added event to both subscribers
            independently.
        """
        # Arrange
        events_1: list = []
        events_2: list = []
        received_1 = asyncio.Event()
        received_2 = asyncio.Event()
        started_1 = asyncio.Event()
        started_2 = asyncio.Event()

        async def collect(subscriber, events, received, started):
            started.set()
            async for event in subscriber:
                events.append(event)
                received.set()
                break

        with LocalDiscovery(namespace) as discovery:
            publisher = discovery.publisher
            subscriber_1 = discovery.subscribe(poll_interval=0.05)
            subscriber_2 = discovery.subscribe(poll_interval=0.05)

            async with publisher:
                task_1 = asyncio.create_task(
                    collect(subscriber_1, events_1, received_1, started_1)
                )
                task_2 = asyncio.create_task(
                    collect(subscriber_2, events_2, received_2, started_2)
                )
                await asyncio.gather(started_1.wait(), started_2.wait())

                # Act
                await publisher.publish("worker-added", metadata)

                try:
                    await asyncio.wait_for(
                        asyncio.gather(received_1.wait(), received_2.wait()),
                        timeout=2.0,
                    )
                except asyncio.TimeoutError:
                    pytest.fail("Both subscribers did not receive the event")
                finally:
                    for t in (task_1, task_2):
                        t.cancel()
                        try:
                            await t
                        except asyncio.CancelledError:
                            pass

        # Assert
        assert len(events_1) == 1
        assert events_1[0].type == "worker-added"
        assert events_1[0].metadata.uid == metadata.uid
        assert len(events_2) == 1
        assert events_2[0].type == "worker-added"
        assert events_2[0].metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test___aiter___should_discover_all_workers_up_to_capacity(self, namespace):
        """Test a subscriber discovers every worker up to capacity.

        Given:
            A LocalDiscovery(capacity=4) with four distinct workers
            published through its publisher
        When:
            The discovery's subscriber iterates
        Then:
            It should discover exactly the four published worker uids.
        """
        # Arrange
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"localhost:{50051 + i}",
                pid=123 + i,
                version="1.0",
            )
            for i in range(4)
        ]
        expected = {worker.uid for worker in workers}
        seen = set()
        all_seen = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                if event.type == "worker-added":
                    seen.add(event.metadata.uid)
                    if expected <= seen:
                        all_seen.set()
                        break

        with LocalDiscovery(namespace, capacity=4) as discovery:
            async with discovery.publisher as publisher:
                for worker in workers:
                    await publisher.publish("worker-added", worker)

                # Act
                subscriber = discovery.subscribe(poll_interval=0.05)
                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(all_seen.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Not all workers discovered within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        assert seen == expected

    @given(capacity=st.integers(min_value=1, max_value=8))
    @settings(
        max_examples=10,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @pytest.mark.asyncio
    async def test___aiter___should_discover_workers_up_to_capacity(
        self, namespace, capacity
    ):
        """Test a subscriber discovers every worker the registry admits.

        Given:
            A LocalDiscovery declaring an arbitrary small capacity C with
            C workers published through its publisher
        When:
            The discovery's subscriber — told no capacity of its own —
            iterates
        Then:
            It should discover exactly the C published worker uids, its
            scan bounded by the capacity the owner stamped into the
            registry.
        """
        # Arrange — a per-example namespace so registry state does
        # not carry across Hypothesis examples.
        example_ns = f"{namespace}-{uuid.uuid4().hex[:8]}"
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"localhost:{50051 + i}",
                pid=123 + i,
                version="1.0",
            )
            for i in range(capacity)
        ]
        expected = {worker.uid for worker in workers}
        seen = set()
        all_seen = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                if event.type == "worker-added":
                    seen.add(event.metadata.uid)
                    if expected <= seen:
                        all_seen.set()
                        break

        # Act
        with LocalDiscovery(example_ns, capacity=capacity) as discovery:
            async with discovery.publisher as publisher:
                for worker in workers:
                    await publisher.publish("worker-added", worker)

                subscriber = discovery.subscribe(poll_interval=0.05)
                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(all_seen.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pytest.fail("Not all workers discovered within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        assert seen == expected

    @pytest.mark.asyncio
    async def test___aiter___should_bound_a_borrowing_subscriber_by_owner_capacity(
        self, namespace
    ):
        """Test a borrowing subscriber reads the owner's stamped capacity.

        Given:
            An owner LocalDiscovery at capacity 4 with four workers
            published, and a Subscriber borrowing that registry
        When:
            The borrowing subscriber iterates
        Then:
            It should discover all four workers — the scan is bounded by
            the capacity the owner stamped, which every borrower of the
            registry reads rather than declaring its own.
        """
        # Arrange
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"localhost:{50051 + i}",
                pid=100 + i,
                version="1.0",
            )
            for i in range(4)
        ]
        expected = {worker.uid for worker in workers}
        seen = set()
        all_seen = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                if event.type == "worker-added":
                    seen.add(event.metadata.uid)
                    if expected <= seen:
                        all_seen.set()
                        break

        # Act
        with LocalDiscovery(namespace, capacity=4) as owner:
            async with owner.publisher as publisher:
                for worker in workers:
                    await publisher.publish("worker-added", worker)

                subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)
                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(all_seen.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pytest.fail("Borrowing subscriber did not discover all workers")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        assert seen == expected

    @pytest.mark.asyncio
    async def test___aiter___when_live_worker_readded_with_changed_metadata(
        self, namespace
    ):
        """Test a live subscriber observes a re-add as an update.

        Given:
            A subscriber already iterating that has observed
            "worker-added" for a worker at version 1.0
        When:
            The same UID is published "worker-added" again with
            version 2.0
        Then:
            It should yield a worker-updated event carrying version 2.0,
            with no worker-dropped and no second worker-added observed
            before it.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=123, version="1.0"
        )
        readded = WorkerMetadata(
            uid=worker.uid, address="localhost:50051", pid=123, version="2.0"
        )

        events = []
        added_seen = asyncio.Event()
        refreshed_seen = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                if event.metadata.uid != worker.uid:
                    continue
                events.append(event)
                if event.type == "worker-added":
                    added_seen.set()
                if event.metadata.version == "2.0":
                    refreshed_seen.set()
                    break

        with LocalDiscovery(namespace) as discovery:
            async with discovery.publisher as publisher:
                subscriber = discovery.subscribe(poll_interval=0.05)
                async with _collecting(subscriber, collect):
                    await publisher.publish("worker-added", worker)
                    await asyncio.wait_for(added_seen.wait(), timeout=2.0)

                    # Act
                    await publisher.publish("worker-added", readded)

                    # Assert
                    try:
                        await asyncio.wait_for(refreshed_seen.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        pytest.fail("Refresh not observed by live subscriber")

        assert events[-1].type == "worker-updated"
        assert events[-1].metadata.version == "2.0"
        added = [e for e in events if e.type == "worker-added"]
        assert len(added) == 1
        assert not any(e.type == "worker-dropped" for e in events)

    @pytest.mark.asyncio
    async def test___aiter___when_readded_worker_dropped_once_while_iterating(
        self, namespace
    ):
        """Test a live subscriber sees one drop fully retire a re-add.

        Given:
            A subscriber live from before a worker's first add that has
            observed the worker's add and its re-add at a bumped
            version
        When:
            A single "worker-dropped" is published
        Then:
            It should yield a worker-dropped event for that UID — the
            re-add left no residual registration keeping the worker
            discoverable past the drop.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=123, version="1.0"
        )
        readded = WorkerMetadata(
            uid=worker.uid, address="localhost:50051", pid=123, version="2.0"
        )

        events = []
        added_seen = asyncio.Event()
        refreshed_seen = asyncio.Event()
        dropped_seen = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                if event.metadata.uid != worker.uid:
                    continue
                events.append(event)
                if event.type == "worker-added":
                    added_seen.set()
                if event.metadata.version == "2.0":
                    refreshed_seen.set()
                if event.type == "worker-dropped":
                    dropped_seen.set()
                    break

        with LocalDiscovery(namespace) as discovery:
            async with discovery.publisher as publisher:
                subscriber = discovery.subscribe(poll_interval=0.05)
                async with _collecting(subscriber, collect):
                    await publisher.publish("worker-added", worker)
                    await asyncio.wait_for(added_seen.wait(), timeout=2.0)
                    await publisher.publish("worker-added", readded)
                    await asyncio.wait_for(refreshed_seen.wait(), timeout=2.0)

                    # Act
                    await publisher.publish("worker-dropped", worker)

                    # Assert
                    try:
                        await asyncio.wait_for(dropped_seen.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        pytest.fail("Single drop not observed by live subscriber")

        assert events[0].type == "worker-added"
        assert events[-1].type == "worker-dropped"

    def test___new___should_return_distinct_object_when_key_matches(self, namespace):
        """Test constructing a Subscriber returns a fresh object each call.

        Given:
            A namespace and a fixed poll interval
        When:
            `LocalDiscovery.Subscriber` is constructed twice with the same
            namespace and poll interval
        Then:
            It should return two distinct objects of the same type.

        Note:
            The metaclass returns a fresh wrapper over a shared source per
            call; that sharing is not observable through object identity, so
            this test pins only the per-call distinctness.
        """
        # Act
        first = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)
        second = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

        # Assert
        assert first is not second
        assert type(first) is type(second)

    @pytest.mark.asyncio
    async def test___reduce___should_rebind_subscriber_to_its_namespace(
        self, namespace, metadata
    ):
        """Test a pickled subscriber reconstructs bound to its namespace.

        Given:
            A subscriber for a namespace, pickled before any iteration
        When:
            It is unpickled and the reconstructed subscriber iterates while
            a worker is published to that namespace
        Then:
            It should discover the worker — the pickle round-trip rebinds
            the subscriber to the same namespace.
        """
        # Arrange
        subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)
        restored = pickle.loads(pickle.dumps(subscriber))

        events = []
        received = asyncio.Event()

        async def collect(sub):
            async for event in sub:
                events.append(event)
                received.set()
                break

        # Act
        with LocalDiscovery(namespace) as discovery:
            async with discovery.publisher as publisher:
                task = asyncio.create_task(collect(restored))
                await publisher.publish("worker-added", metadata)
                try:
                    await asyncio.wait_for(received.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pytest.fail("Reconstructed subscriber did not discover the worker")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        assert len(events) == 1
        assert events[0].metadata.uid == metadata.uid

    def test___reduce___should_carry_the_poll_interval(self, namespace):
        """Test a reduced subscriber keeps the interval that keys its subscription.

        Given:
            A Subscriber constructed on a namespace at a non-default
            poll interval.
        When:
            It is pickled and unpickled.
        Then:
            It should reduce to the same configuration, poll interval
            included, since the interval is part of the subscription key.
        """
        # Arrange
        subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=2.5)

        # Act
        restored = pickle.loads(pickle.dumps(subscriber))

        # Assert — the restored copy and a fresh construction at the
        # same interval reduce to the same bytes, and one at a different
        # interval does not
        assert pickle.dumps(restored) == pickle.dumps(subscriber)
        assert pickle.dumps(subscriber) == pickle.dumps(
            LocalDiscovery.Subscriber(namespace, poll_interval=2.5)
        )
        assert pickle.dumps(subscriber) != pickle.dumps(
            LocalDiscovery.Subscriber(namespace, poll_interval=0.05)
        )

    @pytest.mark.asyncio
    async def test___aiter___should_raise_only_on_the_iteration_that_binds(
        self, namespace
    ):
        """Test only the binding iteration of an unowned namespace raises.

        Given:
            A subscriber for a namespace no owner holds, iterated by
            several consumers started together.
        When:
            All of the iterations are driven to completion.
        Then:
            It should raise DiscoveryNamespaceNotFound on the iteration
            that performs the bind and end the others without events.
        """

        # Arrange
        async def drain(subscriber):
            try:
                async for _ in subscriber:
                    pass
            except DiscoveryNamespaceNotFound:
                return "raised"
            return "ended"

        subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

        # Act
        outcomes = await asyncio.gather(
            drain(subscriber),
            drain(subscriber),
            drain(LocalDiscovery.Subscriber(namespace, poll_interval=0.05)),
        )

        # Assert
        assert outcomes.count("raised") == 1
        assert outcomes.count("ended") == 2


def _enter_lifecycle_forest(namespace, forest, owned=False):
    """Enter one claim on the namespace per node, nesting children.

    A node entered while the namespace is free becomes its owner and
    holds it for the duration of its children; a node entered while a
    generation is live is a claim that generation's owner must reject,
    and its children run on under the incumbent.

    Each outcome is asserted rather than suppressed, so forest *depth*
    discriminates: every nested node is a live rejection, which is what
    ties these forests to the single-owner model rather than to teardown
    hygiene alone.
    """
    for children in forest:
        with ExitStack() as generation:
            if owned:
                with pytest.raises(DiscoveryNamespaceInUse):
                    generation.enter_context(LocalDiscovery(namespace))
            else:
                generation.enter_context(LocalDiscovery(namespace))
            _enter_lifecycle_forest(namespace, children, owned=True)
