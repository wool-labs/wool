from types import MappingProxyType
from uuid import uuid4

from hypothesis import given
from hypothesis import strategies as st

from wool.runtime.loadbalancer.base import DelegatingLoadBalancerLike
from wool.runtime.loadbalancer.base import DispatchingLoadBalancerLike
from wool.runtime.loadbalancer.base import LoadBalancerContext
from wool.runtime.loadbalancer.base import LoadBalancerContextLike
from wool.runtime.loadbalancer.base import LoadBalancerContextView
from wool.runtime.loadbalancer.base import LoadBalancerLike
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.metadata import WorkerMetadata


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


class TestLoadBalancerContextView:
    def test_isinstance_with_conforming_class(self):
        """Test a conforming class satisfies the protocol.

        Given:
            A class exposing only a read-only ``workers`` property
        When:
            Checked against the LoadBalancerContextView protocol
        Then:
            It should satisfy the protocol
        """

        # Arrange
        class Conforming:
            @property
            def workers(self):
                return MappingProxyType({})

        # Act & assert
        assert isinstance(Conforming(), LoadBalancerContextView)

    def test_isinstance_with_non_conforming_class(self):
        """Test a non-conforming class does not satisfy the protocol.

        Given:
            A class missing the workers property
        When:
            Checked against the LoadBalancerContextView protocol
        Then:
            It should not satisfy the protocol
        """

        # Arrange
        class NonConforming:
            pass

        # Act & assert
        assert not isinstance(NonConforming(), LoadBalancerContextView)

    def test_isinstance_with_concrete_context(self):
        """Test LoadBalancerContext satisfies LoadBalancerContextView.

        Given:
            A concrete LoadBalancerContext instance
        When:
            Checked against the LoadBalancerContextView protocol
        Then:
            It should satisfy the protocol — the mutable context is
            also a valid read-only view.
        """
        # Act & assert
        assert isinstance(LoadBalancerContext(), LoadBalancerContextView)


class TestDispatchingLoadBalancerLike:
    def test_isinstance_with_conforming_class(self):
        """Test a conforming class satisfies the protocol.

        Given:
            A class implementing the dispatch method
        When:
            Checked against the DispatchingLoadBalancerLike protocol
        Then:
            It should satisfy the protocol
        """

        # Arrange
        class Conforming:
            async def dispatch(self, task, *, context, timeout=None):
                pass

        # Act & assert
        assert isinstance(Conforming(), DispatchingLoadBalancerLike)

    def test_isinstance_with_non_conforming_class(self):
        """Test a non-conforming class does not satisfy the protocol.

        Given:
            A class missing the dispatch method
        When:
            Checked against the DispatchingLoadBalancerLike protocol
        Then:
            It should not satisfy the protocol
        """

        # Arrange
        class NonConforming:
            pass

        # Act & assert
        assert not isinstance(NonConforming(), DispatchingLoadBalancerLike)


class TestDelegatingLoadBalancerLike:
    def test_isinstance_with_conforming_class(self):
        """Test a conforming class satisfies the protocol.

        Given:
            A class implementing the delegate method as an async
            generator
        When:
            Checked against the DelegatingLoadBalancerLike protocol
        Then:
            It should satisfy the protocol
        """

        # Arrange
        class Conforming:
            async def delegate(self, *, context):
                if False:
                    yield  # pragma: no cover

        # Act & assert
        assert isinstance(Conforming(), DelegatingLoadBalancerLike)

    def test_isinstance_with_non_conforming_class(self):
        """Test a non-conforming class does not satisfy the protocol.

        Given:
            A class missing the delegate method
        When:
            Checked against the DelegatingLoadBalancerLike protocol
        Then:
            It should not satisfy the protocol
        """

        # Arrange
        class NonConforming:
            pass

        # Act & assert
        assert not isinstance(NonConforming(), DelegatingLoadBalancerLike)


class TestLoadBalancerLike:
    def test_isinstance_with_delegating_implementation(self):
        """Test a delegating load balancer satisfies the union alias.

        Given:
            A class implementing DelegatingLoadBalancerLike
        When:
            Checked against the LoadBalancerLike union
        Then:
            It should satisfy the union
        """

        # Arrange
        class Delegating:
            async def delegate(self, *, context):
                if False:
                    yield  # pragma: no cover

        # Act & assert
        assert isinstance(Delegating(), LoadBalancerLike)

    def test_isinstance_with_dispatching_implementation(self):
        """Test a dispatching load balancer satisfies the union alias.

        Given:
            A class implementing DispatchingLoadBalancerLike
        When:
            Checked against the LoadBalancerLike union
        Then:
            It should satisfy the union — preserves backwards
            compatibility during the deprecation window.
        """

        # Arrange
        class Dispatching:
            async def dispatch(self, task, *, context, timeout=None):
                pass

        # Act & assert
        assert isinstance(Dispatching(), LoadBalancerLike)

    def test_isinstance_with_non_conforming_class(self):
        """Test a non-conforming class does not satisfy the union.

        Given:
            A class implementing neither protocol
        When:
            Checked against the LoadBalancerLike union
        Then:
            It should not satisfy either member of the union
        """

        # Arrange
        class NonConforming:
            pass

        # Act & assert
        assert not isinstance(NonConforming(), LoadBalancerLike)

    def test_isinstance_with_dual_implementation(self):
        """Test a class implementing both protocols satisfies both.

        Given:
            A class implementing both delegate and dispatch
        When:
            Checked against both protocols individually
        Then:
            It should satisfy both — this edge case supports
            balancers that want to implement the new protocol while
            keeping the old one available for backwards compatibility.
        """

        # Arrange
        class Dual:
            async def delegate(self, *, context):
                if False:
                    yield  # pragma: no cover

            async def dispatch(self, task, *, context, timeout=None):
                pass

        dual = Dual()

        # Act & assert
        assert isinstance(dual, DelegatingLoadBalancerLike)
        assert isinstance(dual, DispatchingLoadBalancerLike)
        assert isinstance(dual, LoadBalancerLike)


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
