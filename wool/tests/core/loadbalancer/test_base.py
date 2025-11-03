from types import MappingProxyType
from uuid import uuid4

from hypothesis import given
from hypothesis import strategies as st

from wool._connection import Connection
from wool._resource_pool import ResourcePool
from wool.core.discovery.base import WorkerInfo
from wool.core.loadbalancer.base import LoadBalancerContext

connection_pool = ResourcePool(Connection)


@st.composite
def loadbalancer_context(draw):
    """Generate a LoadBalancerContext seeded with 1-8 workers.

    Creates a LoadBalancerContext populated with randomly generated workers.
    Each worker has unique identifying information and a mock connection
    resource factory.

    :param draw:
        Hypothesis draw function

    :returns:
        LoadBalancerContext instance with 1-8 workers
    """
    worker_count = draw(st.integers(min_value=1, max_value=8))
    ctx = LoadBalancerContext()

    for i in range(worker_count):
        worker_info = WorkerInfo(
            uid=uuid4(),
            host="localhost",
            port=50051 + i,
            pid=1000 + i,
            version="1.0.0",
        )
        ctx.add_worker(worker_info, lambda: connection_pool.get(worker_info))

    return ctx


class TestLoadBalancerContext:
    @given(loadbalancer_context())
    def test_workers_property_immutability(self, context: LoadBalancerContext):
        """Test that the ``workers`` property is immutable.

        Given:
            A load balancer context with one or more workers registered
        When:
            A worker is added, updated, or deleted via the ``workers`` property
        Then:
            The context's workers are unchanged
        """
        assert isinstance(context.workers, MappingProxyType)

    @given(loadbalancer_context())
    def test_add_worker(self, context: LoadBalancerContext):
        """Test that a worker can be added to the context.

        Given:
            A load balancer context with one or more workers registered
        When:
            A worker is added to the context
        Then:
            The new worker is present and the existing workers in the context
            are unchanged
        """
        original_workers = dict(context.workers)

        # Add a new worker
        worker = WorkerInfo(
            uid=uuid4(),
            host="localhost",
            port=60000,
            pid=9999,
            version="1.0.0",
        )

        def factory():
            return connection_pool.get(worker_info)

        context.add_worker(worker, factory)

        # Verify the new worker is present
        assert worker in context.workers
        assert context.workers[worker] == factory

        # Verify existing workers are unchanged
        for worker_info in original_workers:
            assert worker_info in context.workers
            assert context.workers[worker_info] == original_workers[worker_info]

    @given(loadbalancer_context())
    def test_update_worker(self, context: LoadBalancerContext):
        """Test that a worker already in the context can be updated.

        Given:
            A load balancer context with one or more workers registered
        When:
            One of the workers in the context is updated
        Then:
            The updated worker reflects the updates accurately and the other
            existing workers in the context are unchanged
        """
        original_workers = dict(context.workers)
        worker_to_update = next(iter(context.workers.keys()))

        # Create a different factory function
        def factory():
            return connection_pool.get(worker_info)

        # Update the worker with a new factory
        context.update_worker(worker_to_update, factory)

        # Verify the worker was updated
        assert context.workers[worker_to_update] == factory

        # Verify other workers are unchanged
        for worker_info in original_workers:
            if worker_info != worker_to_update:
                assert context.workers[worker_info] == original_workers[worker_info]

    @given(loadbalancer_context())
    def test_upsert_existing_worker(self, context: LoadBalancerContext):
        """Test that a worker already in the context can be updated with the
        ``upsert`` flag.

        Given:
            A load balancer context with one or more workers registered
        When:
            One of the workers in the context is updated with the
            ``upsert`` flag
        Then:
            The updated worker reflects the updates accurately and the other
            existing workers in the context are unchanged
        """
        original_workers = dict(context.workers)
        worker_to_update = next(iter(context.workers.keys()))

        # Create a different factory function
        def factory():
            return connection_pool.get(worker_info)

        # Update the worker with a new factory using upsert=True
        context.update_worker(worker_to_update, factory, upsert=True)

        # Verify the worker was updated
        assert context.workers[worker_to_update] == factory

        # Verify other workers are unchanged
        for worker_info in original_workers:
            if worker_info != worker_to_update:
                assert context.workers[worker_info] == original_workers[worker_info]

    @given(loadbalancer_context())
    def test_upsert_new_worker(self, context: LoadBalancerContext):
        """Test that updating a non-existent worker with the ``upsert`` flag
        adds the worker to the context.

        Given:
            A load balancer context with one or more workers registered
        When:
            A non-existent worker is updated with the ``upsert`` flag
        Then:
            The upserted worker is present and the existing workers in the
            context are unchanged
        """
        original_workers = dict(context.workers)

        # Upsert a new worker
        new_worker = WorkerInfo(
            uid=uuid4(),
            host="localhost",
            port=60000,
            pid=9999,
            version="1.0.0",
        )

        def factory():
            return connection_pool.get(worker_info)

        context.update_worker(new_worker, factory, upsert=True)

        # Verify the new worker is present
        assert new_worker in context.workers
        assert context.workers[new_worker] == factory

        # Verify existing workers are unchanged
        for worker_info in original_workers:
            assert worker_info in context.workers
            assert context.workers[worker_info] == original_workers[worker_info]

    @given(loadbalancer_context())
    def test_remove_worker(self, context: LoadBalancerContext):
        """Test that a worker can be removed from the context.

        Given:
            A load balancer context with one or more workers registered
        When:
            A worker is removed from the context
        Then:
            The removed worker is no longer present and the existing workers
            in the context are unchanged
        """
        original_workers = dict(context.workers)
        worker_to_remove = next(iter(context.workers.keys()))

        # Remove the worker
        context.remove_worker(worker_to_remove)

        # Verify the worker was removed
        assert worker_to_remove not in context.workers

        # Verify other workers are unchanged
        for worker_info in original_workers:
            if worker_info != worker_to_remove:
                assert worker_info in context.workers
                assert context.workers[worker_info] == original_workers[worker_info]
