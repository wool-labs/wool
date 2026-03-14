from types import MappingProxyType
from uuid import uuid4

from hypothesis import given
from hypothesis import strategies as st

from wool.runtime.discovery.base import WorkerMetadata
from wool.runtime.loadbalancer.base import LoadBalancerContext
from wool.runtime.loadbalancer.base import LoadBalancerContextLike
from wool.runtime.loadbalancer.base import LoadBalancerLike
from wool.runtime.worker.connection import WorkerConnection


@st.composite
def loadbalancer_context(draw):
    """Generate a LoadBalancerContext seeded with 1-8 workers.

    Creates a LoadBalancerContext populated with randomly generated workers.
    Each worker has unique identifying information and a connection.

    :param draw:
        Hypothesis draw function

    :returns:
        LoadBalancerContext instance with 1-8 workers
    """
    worker_count = draw(st.integers(min_value=1, max_value=8))
    ctx = LoadBalancerContext()

    for i in range(worker_count):
        metadata = WorkerMetadata(
            uid=uuid4(),
            address=f"localhost:{50051 + i}",
            pid=1000 + i,
            version="1.0.0",
        )
        ctx.add_worker(metadata, WorkerConnection(f"localhost:{50051 + i}"))

    return ctx


class TestLoadBalancerContextLike:
    def test_isinstance_with_conforming_class(self):
        """Test a conforming class satisfies the protocol.

        Given:
            A class implementing all LoadBalancerContextLike methods
        When:
            Checked against the LoadBalancerContextLike protocol
        Then:
            It should satisfy the protocol
        """

        # Arrange
        class Conforming:
            @property
            def workers(self):
                return MappingProxyType({})

            def add_worker(self, metadata, connection):
                pass

            def update_worker(self, metadata, connection, *, upsert=False):
                pass

            def remove_worker(self, metadata):
                pass

        # Act & assert
        assert isinstance(Conforming(), LoadBalancerContextLike)

    def test_isinstance_with_non_conforming_class(self):
        """Test a non-conforming class does not satisfy the protocol.

        Given:
            A class missing one or more LoadBalancerContextLike
            methods
        When:
            Checked against the LoadBalancerContextLike protocol
        Then:
            It should not satisfy the protocol
        """

        # Arrange
        class NonConforming:
            @property
            def workers(self):
                return MappingProxyType({})

            def add_worker(self, metadata, connection):
                pass

        # Act & assert
        assert not isinstance(NonConforming(), LoadBalancerContextLike)


class TestLoadBalancerLike:
    def test_isinstance_with_conforming_class(self):
        """Test a conforming class satisfies the protocol.

        Given:
            A class implementing the dispatch method
        When:
            Checked against the LoadBalancerLike protocol
        Then:
            It should satisfy the protocol
        """

        # Arrange
        class Conforming:
            async def dispatch(self, task, *, context, timeout=None):
                pass

        # Act & assert
        assert isinstance(Conforming(), LoadBalancerLike)

    def test_isinstance_with_non_conforming_class(self):
        """Test a non-conforming class does not satisfy the protocol.

        Given:
            A class missing the dispatch method
        When:
            Checked against the LoadBalancerLike protocol
        Then:
            It should not satisfy the protocol
        """

        # Arrange
        class NonConforming:
            pass

        # Act & assert
        assert not isinstance(NonConforming(), LoadBalancerLike)


class TestLoadBalancerContext:
    def test_isinstance_with_concrete_context(self):
        """Test LoadBalancerContext satisfies the protocol.

        Given:
            A concrete LoadBalancerContext instance
        When:
            Checked against the LoadBalancerContextLike protocol
        Then:
            It should satisfy the protocol
        """
        # Act & assert
        assert isinstance(LoadBalancerContext(), LoadBalancerContextLike)

    @given(loadbalancer_context())
    def test_workers_property_immutability(self, context: LoadBalancerContext):
        """Test that the ``workers`` property is immutable.

        Given:
            A load balancer context with one or more workers
            registered
        When:
            The workers property is accessed
        Then:
            The returned value is an immutable mapping proxy
        """
        # Act & assert
        assert isinstance(context.workers, MappingProxyType)

    @given(loadbalancer_context())
    def test_add_worker_with_new_worker(self, context: LoadBalancerContext):
        """Test that a worker can be added to the context.

        Given:
            A load balancer context with one or more workers
            registered
        When:
            A worker is added to the context
        Then:
            The new worker is present and the existing workers in
            the context are unchanged
        """
        # Arrange
        original_workers = dict(context.workers)
        worker = WorkerMetadata(
            uid=uuid4(),
            address="localhost:60000",
            pid=9999,
            version="1.0.0",
        )
        connection = WorkerConnection("localhost:60000")

        # Act
        context.add_worker(worker, connection)

        # Assert
        assert worker in context.workers
        assert context.workers[worker] == connection
        for metadata in original_workers:
            assert metadata in context.workers
            assert context.workers[metadata] == original_workers[metadata]

    @given(loadbalancer_context())
    def test_update_worker_with_existing_worker(self, context: LoadBalancerContext):
        """Test that a worker already in the context can be updated.

        Given:
            A load balancer context with one or more workers
            registered
        When:
            One of the workers in the context is updated
        Then:
            The updated worker reflects the updates accurately and
            the other existing workers in the context are unchanged
        """
        # Arrange
        original_workers = dict(context.workers)
        worker_to_update = next(iter(context.workers.keys()))
        connection = WorkerConnection("localhost:60000")

        # Act
        context.update_worker(worker_to_update, connection)

        # Assert
        assert context.workers[worker_to_update] == connection
        for metadata in original_workers:
            if metadata != worker_to_update:
                assert context.workers[metadata] == original_workers[metadata]

    @given(loadbalancer_context())
    def test_update_worker_with_upsert_existing(self, context: LoadBalancerContext):
        """Test that a worker already in the context can be updated
        with the ``upsert`` flag.

        Given:
            A load balancer context with one or more workers
            registered
        When:
            One of the workers in the context is updated with the
            ``upsert`` flag
        Then:
            The updated worker reflects the updates accurately and
            the other existing workers in the context are unchanged
        """
        # Arrange
        original_workers = dict(context.workers)
        worker_to_update = next(iter(context.workers.keys()))
        connection = WorkerConnection("localhost:60000")

        # Act
        context.update_worker(worker_to_update, connection, upsert=True)

        # Assert
        assert context.workers[worker_to_update] == connection
        for metadata in original_workers:
            if metadata != worker_to_update:
                assert context.workers[metadata] == original_workers[metadata]

    @given(loadbalancer_context())
    def test_update_worker_with_upsert_new(self, context: LoadBalancerContext):
        """Test that updating a non-existent worker with the
        ``upsert`` flag adds the worker to the context.

        Given:
            A load balancer context with one or more workers
            registered
        When:
            A non-existent worker is updated with the ``upsert``
            flag
        Then:
            The upserted worker is present and the existing workers
            in the context are unchanged
        """
        # Arrange
        original_workers = dict(context.workers)
        new_worker = WorkerMetadata(
            uid=uuid4(),
            address="localhost:60000",
            pid=9999,
            version="1.0.0",
        )
        connection = WorkerConnection("localhost:60000")

        # Act
        context.update_worker(new_worker, connection, upsert=True)

        # Assert
        assert new_worker in context.workers
        assert context.workers[new_worker] == connection
        for metadata in original_workers:
            assert metadata in context.workers
            assert context.workers[metadata] == original_workers[metadata]

    def test_update_worker_noop_without_upsert(self):
        """Test that updating an absent worker without upsert is a
        no-op.

        Given:
            A load balancer context with no workers registered
        When:
            A non-existent worker is updated without the ``upsert``
            flag
        Then:
            The context remains unchanged
        """
        # Arrange
        ctx = LoadBalancerContext()
        absent_worker = WorkerMetadata(
            uid=uuid4(),
            address="localhost:60000",
            pid=9999,
            version="1.0.0",
        )
        connection = WorkerConnection("localhost:60000")

        # Act
        ctx.update_worker(absent_worker, connection)

        # Assert
        assert len(ctx.workers) == 0

    @given(loadbalancer_context())
    def test_remove_worker_with_existing_worker(self, context: LoadBalancerContext):
        """Test that a worker can be removed from the context.

        Given:
            A load balancer context with one or more workers
            registered
        When:
            A worker is removed from the context
        Then:
            The removed worker is no longer present and the existing
            workers in the context are unchanged
        """
        # Arrange
        original_workers = dict(context.workers)
        worker_to_remove = next(iter(context.workers.keys()))

        # Act
        context.remove_worker(worker_to_remove)

        # Assert
        assert worker_to_remove not in context.workers
        for metadata in original_workers:
            if metadata != worker_to_remove:
                assert metadata in context.workers
                assert context.workers[metadata] == original_workers[metadata]

    def test_remove_worker_noop_when_absent(self):
        """Test that removing an absent worker is a no-op.

        Given:
            A load balancer context with no workers registered
        When:
            A non-existent worker is removed from the context
        Then:
            The context remains unchanged
        """
        # Arrange
        ctx = LoadBalancerContext()
        absent_worker = WorkerMetadata(
            uid=uuid4(),
            address="localhost:60000",
            pid=9999,
            version="1.0.0",
        )

        # Act
        ctx.remove_worker(absent_worker)

        # Assert
        assert len(ctx.workers) == 0
