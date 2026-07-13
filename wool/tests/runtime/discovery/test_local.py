import asyncio
import atexit
import struct
import uuid
from collections import Counter
from contextlib import ExitStack
from multiprocessing.shared_memory import SharedMemory
from types import MappingProxyType

import portalocker
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.local import LocalDiscovery
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
def namespace():
    """Provides unique namespace for test isolation.

    Creates a unique namespace string for each test to ensure shared
    memory regions don't interfere with each other.
    """
    return f"test-namespace-{uuid.uuid4()}"


@pytest.fixture
def atexit_recorder(mocker):
    """Wraps atexit registration in recording pass-throughs.

    Returns a (registered, unregistered) tuple of lists capturing every
    callable that flows through atexit.register and atexit.unregister
    while the real registry stays consistent.

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
        return real_unregister(func)

    mocker.patch.object(atexit, "register", register)
    mocker.patch.object(atexit, "unregister", unregister)
    return registered, unregistered


@pytest.fixture
def unlink_schedule(mocker):
    """Patches SharedMemory.unlink with a schedule-driven wrapper and
    returns the schedule list.

    Each unlink call performs the real unlink — so no segment leaks —
    then consumes one schedule entry and raises it when the entry is an
    exception, simulating an external unlinker or a hostile filesystem.
    An empty or exhausted schedule means the unlink passes through
    untouched. A one-shot failure is therefore a one-entry schedule, and
    a generated failure pattern is a longer one. Patching once per test
    (rather than per failure or per Hypothesis example) avoids stacking
    wrappers.
    """
    schedule: list[Exception | None] = []
    real_unlink = SharedMemory.unlink

    def unlink(shm):
        real_unlink(shm)
        if schedule and (error := schedule.pop(0)) is not None:
            raise error

    mocker.patch.object(SharedMemory, "unlink", unlink)
    return schedule


#: Same-namespace lifecycle forests: each node is one LocalDiscovery
#: context; nesting models concurrently overlapping instances and
#: siblings model teardown+respawn generations reusing the namespace.
#: Referenced by ``@given`` at class-definition time, so it must precede
#: the test classes.
_LIFECYCLE_FORESTS = st.recursive(
    st.just([]),
    lambda children: st.lists(children, max_size=3),
    max_leaves=6,
)


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

    def test___enter___with_existing_namespace(self, namespace):
        """Test LocalDiscovery joins an existing namespace without error.

        Given:
            A LocalDiscovery that owns a namespace
        When:
            A second LocalDiscovery enters the same namespace via with
        Then:
            It should succeed without raising FileExistsError.
        """
        # Arrange
        with LocalDiscovery(namespace):
            # Act & assert
            with LocalDiscovery(namespace) as joiner:
                assert joiner is not None

    @pytest.mark.asyncio
    async def test___enter___should_preserve_workers_when_joining_existing_namespace(
        self, namespace, metadata
    ):
        """Test joining an existing namespace preserves its contents.

        Given:
            An owner's namespace already containing a published worker
        When:
            A second LocalDiscovery enters the same namespace and a
            subscriber obtained from the joiner iterates
        Then:
            It should yield the pre-join worker-added event, proving
            attaching did not reinitialize the segment.
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
                await publisher.publish("worker-added", metadata)

                # Act
                with LocalDiscovery(namespace) as joiner:
                    subscriber = joiner.subscribe(poll_interval=0.05)
                    task = asyncio.create_task(collect(subscriber))
                    try:
                        await asyncio.wait_for(event_received.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        pytest.fail("Pre-join worker not discovered within timeout")
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
    async def test___enter___should_recreate_segment_when_namespace_reused(
        self, namespace, metadata
    ):
        """Test a namespace remains fully usable after rapid teardowns.

        Given:
            A namespace already cycled through several rapid owner
            enter/exit lifecycles, leaving no segment behind
        When:
            A fresh LocalDiscovery enters the namespace and a worker
            is published and subscribed to
        Then:
            It should yield the worker-added event, proving each
            teardown freed the segment name for a functional respawn.
        """
        # Arrange
        for _ in range(3):
            with LocalDiscovery(namespace):
                pass

        # Arrange — the last teardown really freed the name: with no
        # owner holding it, a publish finds no segment to attach to.
        # Without this probe a stale surviving segment would satisfy the
        # roundtrip below just as well as a recreated one.
        probe = LocalDiscovery.Publisher(namespace)
        async with probe:
            with pytest.raises(FileNotFoundError):
                await probe.publish("worker-added", metadata)

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

    def test___enter___should_not_register_atexit_fallback_when_namespace_already_owned(
        self, namespace, atexit_recorder
    ):
        """Test the non-owner branch performs no atexit traffic.

        Given:
            An owner already holding a namespace, with atexit
            registration wrapped in recording pass-throughs
        When:
            A second LocalDiscovery enters and exits the same
            namespace via with
        Then:
            It should neither register nor unregister any atexit
            fallback.
        """
        # Arrange
        registered, unregistered = atexit_recorder

        with LocalDiscovery(namespace):
            registrations = len(registered)

            # Act
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
    async def test___exit___as_non_owner(self, namespace):
        """Test non-owner exit leaves shared memory accessible to owner.

        Given:
            An owner and a non-owner sharing a namespace
        When:
            The non-owner exits via with
        Then:
            It should close without unlinking, leaving the shared
            memory accessible to the owner.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
        )

        with LocalDiscovery(namespace):
            with LocalDiscovery(namespace):
                pass  # non-owner enters and exits

            # Act & assert — publishing succeeds, proving shared
            # memory was not unlinked by the non-owner
            publisher = LocalDiscovery.Publisher(namespace)
            async with publisher:
                await publisher.publish("worker-added", worker)

    @pytest.mark.asyncio
    async def test___exit___should_unlink_segment_when_owner_exits(
        self, namespace, metadata
    ):
        """Test owner exit removes the shared-memory segment.

        Given:
            An owner LocalDiscovery that entered a namespace
        When:
            The owner exits via with and a Publisher then publishes
            to that namespace
        Then:
            It should raise FileNotFoundError from the publish — the
            segment no longer exists.
        """
        # Arrange
        with LocalDiscovery(namespace):
            pass
        publisher = LocalDiscovery.Publisher(namespace)

        # Act & assert
        async with publisher:
            with pytest.raises(FileNotFoundError):
                await publisher.publish("worker-added", metadata)

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
    async def test___exit___should_unlink_segment_when_body_raises(
        self, namespace, metadata
    ):
        """Test exceptional exit still tears the segment down.

        Given:
            An owner LocalDiscovery whose with body raises ValueError
        When:
            The exception unwinds the with statement
        Then:
            It should propagate the ValueError unsuppressed while
            still unlinking the segment, so a subsequent publish
            raises FileNotFoundError.
        """
        # Arrange
        publisher = LocalDiscovery.Publisher(namespace)

        # Act
        with pytest.raises(ValueError, match="boom"):
            with LocalDiscovery(namespace):
                raise ValueError("boom")

        # Assert — teardown still removed the segment
        async with publisher:
            with pytest.raises(FileNotFoundError):
                await publisher.publish("worker-added", metadata)

    def test___exit___should_unwind_cleanly_when_owner_exits_before_non_owner(
        self, namespace
    ):
        """Test a non-owner outliving the owner still exits cleanly.

        Given:
            An owner and an attached non-owner sharing a namespace
        When:
            The owner exits first, unlinking the segment name, and
            the non-owner exits afterwards
        Then:
            It should raise nothing from either exit and leave the
            namespace re-creatable.
        """
        # Arrange — the ExitStack holds the non-owner open past the
        # owner's exit and closes it afterwards
        attached = ExitStack()

        # Act
        with attached:
            with LocalDiscovery(namespace):
                attached.enter_context(LocalDiscovery(namespace))

        # Assert
        with LocalDiscovery(namespace) as respawned:
            assert respawned.namespace == namespace

    @pytest.mark.asyncio
    async def test___exit___should_unwind_cleanly_when_overlapping_lifecycles_interleave(
        self, namespace, metadata
    ):
        """Test overlapping same-namespace generations unwind cleanly.

        Given:
            Owner A and attached non-owner B sharing a namespace
        When:
            A exits, C enters the freed namespace, B exits inside
            C's epoch, and a worker is published in C's epoch
        Then:
            It should raise nothing at any step and the worker should
            be discoverable through C's fresh segment.
        """
        # Arrange
        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        attached = ExitStack()

        # Act — A-enter/B-enter/A-exit/C-enter/B-exit/publish/C-exit
        with attached:
            with LocalDiscovery(namespace):
                attached.enter_context(LocalDiscovery(namespace))
            with LocalDiscovery(namespace) as c:
                attached.close()
                publisher = LocalDiscovery.Publisher(namespace)
                async with publisher:
                    await publisher.publish("worker-added", metadata)

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

        # Assert
        assert len(events) == 1
        assert events[0].type == "worker-added"
        assert events[0].metadata.uid == metadata.uid

    def test___exit___should_not_raise_when_segment_already_unlinked(
        self, namespace, unlink_schedule
    ):
        """Test owner exit tolerates an externally unlinked segment.

        Given:
            An owner LocalDiscovery whose shared-memory segment is
            unlinked out from under it, as by another process's
            resource tracker
        When:
            The owner exits via with
        Then:
            It should exit cleanly without raising FileNotFoundError.
        """
        # Arrange
        unlink_schedule.append(FileNotFoundError(2, "No such file or directory"))

        # Act & assert — exits cleanly despite the vanished segment
        with LocalDiscovery(namespace):
            pass

    def test___exit___should_disarm_atexit_fallback_when_unlink_raises(
        self, namespace, atexit_recorder, unlink_schedule
    ):
        """Test owner exit disarms the atexit fallback when unlink raises.

        Given:
            An owner LocalDiscovery whose shared-memory segment is
            unlinked out from under it, as by another process's
            resource tracker
        When:
            The owner exits via with
        Then:
            It should unregister the atexit-registered fallback so it
            cannot fire a second unlink at interpreter shutdown.
        """
        # Arrange
        registered, unregistered = atexit_recorder
        unlink_schedule.append(FileNotFoundError(2, "No such file or directory"))

        # Act
        with LocalDiscovery(namespace):
            pass

        # Assert
        assert registered == unregistered
        assert len(registered) == 1

    def test___exit___should_disarm_atexit_fallback_when_unlink_raises_permission_error(
        self, namespace, atexit_recorder, unlink_schedule
    ):
        """Test the fallback is disarmed before the unlink can fail.

        Given:
            An owner LocalDiscovery whose segment unlink raises
            PermissionError, with atexit registration wrapped in
            recording pass-throughs
        When:
            The owner exits via with
        Then:
            It should have unregistered the fallback, proving the
            disarm precedes the unlink.
        """
        # Arrange
        registered, unregistered = atexit_recorder
        unlink_schedule.append(PermissionError(13, "Permission denied"))

        # Act
        with pytest.warns(ResourceWarning):
            with LocalDiscovery(namespace):
                pass

        # Assert
        assert registered == unregistered
        assert len(registered) == 1

    def test___exit___should_warn_when_unlink_fails_unexpectedly(
        self, namespace, unlink_schedule
    ):
        """Test an unexpected unlink failure surfaces as a warning.

        Given:
            An owner LocalDiscovery whose segment unlink raises
            PermissionError, a failure with no benign explanation
        When:
            The owner exits via with
        Then:
            It should emit a ResourceWarning naming the segment it
            could not reclaim.
        """
        # Arrange
        unlink_schedule.append(PermissionError(13, "Permission denied"))

        # Act & assert
        with pytest.warns(ResourceWarning, match="failed to unlink shared memory"):
            with LocalDiscovery(namespace):
                pass

    def test___exit___should_propagate_body_error_when_unlink_fails_unexpectedly(
        self, namespace, unlink_schedule
    ):
        """Test a failing teardown does not mask the caller's exception.

        Given:
            An owner LocalDiscovery whose segment unlink raises
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
            An arbitrary forest of same-namespace LocalDiscovery
            contexts, where nesting models overlapping instances and
            siblings model teardown+respawn generations
        When:
            Every context is entered and exited via nested with
            statements and a final fresh context enters the namespace
        Then:
            It should raise nothing for any interleaving and leave
            the namespace re-creatable.
        """
        # Arrange — per-example namespace so a leaked segment in one
        # example cannot demote the next example's first context
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
    def test___exit___should_unwind_interleavings_when_segments_vanish(
        self, namespace, atexit_recorder, unlink_schedule, forest, mask
    ):
        """Test vanishing segments never break lifecycle unwinding.

        Given:
            An arbitrary forest of same-namespace LocalDiscovery
            contexts and an arbitrary subset of unlink calls that
            observe the segment already removed by an external
            unlinker, with atexit registration wrapped in recording
            pass-throughs
        When:
            Every context is entered and exited via nested with
            statements
        Then:
            It should raise nothing for any forest and mask
            combination, pair every registered fallback with exactly
            one unregistration, and leave the namespace re-creatable.
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
    async def test_subscribe_with_non_owner_discovery(self, namespace):
        """Test non-owner can discover workers published by the owner.

        Given:
            An owner and a non-owner sharing a namespace
        When:
            A worker is published by the owner and discovered through
            the non-owner's subscriber
        Then:
            It should yield the worker-added event with matching
            metadata.
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
            with LocalDiscovery(namespace) as joiner:
                publisher = LocalDiscovery.Publisher(namespace)
                subscriber = joiner.subscribe(poll_interval=0.05)

                # Act
                async with publisher:
                    await publisher.publish("worker-added", worker)

                    task = asyncio.create_task(collect(subscriber))
                    try:
                        await asyncio.wait_for(event_received.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        pytest.fail("Worker not discovered via non-owner within timeout")
                    finally:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass

        # Assert
        assert len(events) == 1
        assert events[0].type == "worker-added"
        assert events[0].metadata.uid == worker.uid


class TestLocalDiscoveryPublisher:
    """Tests for LocalDiscovery.Publisher class.

    Fully qualified name:
    wool.runtime.discovery.local.LocalDiscovery.Publisher
    """

    def test_bind_host_with_default_value(self, namespace):
        """Test bind_host prescribes the loopback address.

        Given:
            A LocalDiscovery Publisher
        When:
            The bind_host attribute is accessed
        Then:
            It should be "127.0.0.1" since shared-memory announcements
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

    def test___init___with_negative_block_size(self, namespace):
        """Test Publisher rejects negative block sizes.

        Given:
            A negative block_size
        When:
            Publisher is instantiated
        Then:
            It should raise ValueError.
        """
        # Act & assert
        with pytest.raises(ValueError, match="Block size must be positive"):
            LocalDiscovery.Publisher(namespace, block_size=-1)

    @pytest.mark.asyncio
    async def test___aenter___and___aexit___lifecycle(self, namespace):
        """Test Publisher async context manager lifecycle.

        Given:
            A Publisher instance
        When:
            Used as an async context manager via async with
        Then:
            It should be available inside the block and cleaned up
            after.
        """
        # Arrange
        publisher = LocalDiscovery.Publisher(namespace)

        # Act & assert
        async with publisher as ctx:
            assert ctx is publisher

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
            It should remove the worker from shared memory.
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
            because the drop unlinked the block's segment name.
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
    async def test_publish_should_complete_drop_when_block_already_unlinked(
        self, namespace, metadata, atexit_recorder, unlink_schedule
    ):
        """Test drop tolerates an externally unlinked worker block.

        Given:
            A published worker whose per-block segment is unlinked
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
            It should update the worker metadata in shared memory.
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
    async def test_publish_worker_updated_non_existent_raises_key_error(
        self, namespace, metadata
    ):
        """Test update non-existent worker raises KeyError.

        Given:
            An initialized Publisher with no published workers
        When:
            publish("worker-updated", metadata) is called for a
            worker that was never added
        Then:
            It should raise KeyError.
        """
        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)

            # Act & assert
            async with publisher:
                with pytest.raises(KeyError, match=str(metadata.uid)):
                    await publisher.publish("worker-updated", metadata)

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "capacity is not enforced until #299 — page rounding inflates "
            "the segment to a full page"
        ),
    )
    @pytest.mark.asyncio
    async def test_publish_to_full_address_space_raises_runtime_error(self, namespace):
        """Test publish to full address space raises RuntimeError.

        Given:
            A LocalDiscovery whose declared capacity has been filled
            with published workers
        When:
            One worker beyond capacity is published
        Then:
            It should raise RuntimeError with "No available slots".
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

        with LocalDiscovery(namespace, capacity=capacity):
            publisher = LocalDiscovery.Publisher(namespace)

            async with publisher:
                for worker in workers[:capacity]:
                    await publisher.publish("worker-added", worker)

                # Act & assert
                with pytest.raises(RuntimeError, match="No available slots"):
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
            It should raise struct.error and preserve the original
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
                with pytest.raises(struct.error):
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
                with pytest.raises(struct.error):
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
            It should raise struct.error for the oversized worker,
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
                with pytest.raises(struct.error):
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
        self, namespace, metadata, mocker
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
        original_lock = portalocker.lock
        attempt = 0

        def contending_lock(fh, flags):
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise portalocker.LockException("Lock held")
            original_lock(fh, flags)

        mocker.patch(
            "wool.runtime.discovery.local.portalocker.lock",
            side_effect=contending_lock,
        )

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)

            # Act
            async with publisher:
                await publisher.publish("worker-added", metadata)

        # Assert
        assert attempt == 2

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
    async def test___aexit___should_exit_cleanly_when_worker_segments_already_unlinked(
        self, namespace, metadata, atexit_recorder, unlink_schedule
    ):
        """Test publisher exit tolerates vanished worker blocks.

        Given:
            A Publisher with a published worker whose per-block
            segment is unlinked out from under it, with atexit
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
            small worker roster, where drops may target workers that
            were never added, with atexit registration wrapped in
            recording pass-throughs
        When:
            The sequence is published and the publisher then exits
            with any residual workers still registered
        Then:
            It should unwind cleanly and pair every per-block
            fallback registration with exactly one unregistration.
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
                        if index in added:
                            continue
                        await publisher.publish("worker-added", roster[index])
                        added.add(index)
                    else:
                        await publisher.publish("worker-dropped", roster[index])
                        added.discard(index)

            # Assert
            block_registered = registered[baseline:]
            assert Counter(block_registered) == Counter(unregistered)


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


def _enter_lifecycle_forest(namespace, forest):
    """Enter one same-namespace context per node, nesting children."""
    for children in forest:
        with LocalDiscovery(namespace):
            _enter_lifecycle_forest(namespace, children)
