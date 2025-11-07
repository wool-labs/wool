from __future__ import annotations

import asyncio
import os
import sys
import uuid
from contextlib import asynccontextmanager
from typing import AsyncContextManager
from typing import Awaitable
from typing import ContextManager
from typing import Coroutine
from typing import Final
from typing import overload

from wool._worker_proxy import LoadBalancerLike
from wool._worker_proxy import RoundRobinLoadBalancer
from wool._worker_proxy import WorkerProxy
from wool.core.discovery.base import DiscoveryLike
from wool.core.discovery.base import DiscoveryPublisherLike
from wool.core.discovery.local import LocalDiscovery
from wool.core.typing import Factory
from wool.core.worker.base import WorkerFactory
from wool.core.worker.base import WorkerLike
from wool.core.worker.local import LocalWorker


# public
class WorkerPool:
    """Manages a pool of distributed worker processes for task execution.

    The WorkerPool is the core orchestrator in the wool framework, providing
    both ephemeral and durable pool configurations. It handles worker lifecycle,
    service discovery, and load balancing through configurable components.

    **Ephemeral Pools** spawn local worker processes with automatic cleanup,
    ideal for development and single-machine deployments.

    **Durable Pools** connect to existing distributed workers via discovery
    services, supporting production deployments across multiple machines.

    Example usage:

    **Basic ephemeral pool (default configuration):**

    .. code-block:: python

        import wool


        @wool.work
        async def fibonacci(n: int) -> int:
            if n <= 1:
                return n
            return await fibonacci(n - 1) + await fibonacci(n - 2)


        async def main():
            async with wool.WorkerPool() as pool:
                result = await fibonacci(10)
                print(f"Result: {result}")

    **Ephemeral pool with custom configuration:**

    .. code-block:: python

        from wool import WorkerPool, LocalWorker
        from functools import partial

        # Custom worker factory with specific configuration
        worker_factory = partial(LocalWorker, host="0.0.0.0", port=50051)

        async with WorkerPool(
            "gpu-capable",
            "ml-model",  # Worker tags
            size=4,  # Number of workers
            worker=worker_factory,  # Custom factory
        ) as pool:
            result = await process_data()

    **Durable pool with LAN discovery:**

    .. code-block:: python

        from wool import WorkerPool

        # Connect to existing workers on the network
        discovery = LanDiscovery(filter=lambda w: "production" in w.tags)

        async with WorkerPool(discovery=discovery) as pool:
            results = await gather_metrics()

    **Durable pool with custom load balancer:**

    .. code-block:: python

        from wool import WorkerPool
        from wool._worker_proxy import RoundRobinLoadBalancer


        class WeightedLoadBalancer(RoundRobinLoadBalancer):
            # Custom load balancing logic
            pass


        async with WorkerPool(
            discovery=discovery_service, loadbalancer=WeightedLoadBalancer
        ) as pool:
            result = await distributed_computation()

    :param tags:
        Capability tags to associate with spawned workers (ephemeral pools only).
    :param size:
        Number of worker processes to spawn (ephemeral pools, 0 = CPU count).
    :param worker:
        Factory function for creating worker instances (ephemeral pools).
    :param loadbalancer:
        Load balancer for task distribution. Can be provided as:

        - **Instance**: Direct loadbalancer object
        - **Factory function**: Function returning a loadbalancer instance
        - **Context manager factory**: Function returning a context manager
            that yields a loadbalancer instance

        Examples::

            # Direct instance
            loadbalancer = RoundRobinLoadBalancer()

            # Instance factory
            loadbalancer = lambda: CustomLoadBalancer(...)


            # Context manager factory
            @contextmanager
            def loadbalancer():
                async with CustomLoadBalancer() as lb:
                    ...
                    yield lb
                    ...


            loadbalancer = loadbalancer

    :param discovery:
        Discovery service for finding existing workers (durable pools only).
        Can be provided as:

        - **Instance**: Direct discovery service object
        - **Factory function**: Function returning a discovery service instance
        - **Context manager factory**: Function returning a context manager that
            yields a discovery service

        Examples::

            # Direct instance
            discovery=LanDiscovery(filter=lambda w: "prod" in w.tags)

            # Instance factory
            discovery=lambda: LocalDiscovery("pool-123")

            # Context manager factory
            @asynccontextmanager
            async def discovery():
                service = await DatabaseDiscovery.create(connection_string)
                try:
                    ...
                    yield service
                finally:
                    ...
                    await service.close()
            discovery=discovery
    :raises ValueError:
        If invalid configuration is provided or CPU count cannot be determined.
    """

    _workers: Final[dict[WorkerLike, Coroutine]]

    @overload
    def __init__(
        self,
        *tags: str,
        size: int = 0,
        worker: WorkerFactory = LocalWorker,
        discovery: DiscoveryLike | Factory[DiscoveryLike] | None = None,
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
    ):
        """
        Create an ephemeral pool of workers, spawning the specified quantity of workers
        using the specified worker factory.
        """
        ...

    @overload
    def __init__(
        self,
        *,
        discovery: DiscoveryLike | Factory[DiscoveryLike],
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
    ):
        """
        Connect to an existing pool of workers discovered by the specified discovery
        protocol.
        """
        ...

    def __init__(
        self,
        *tags: str,
        size: int | None = None,
        worker: WorkerFactory | None = None,
        discovery: DiscoveryLike | Factory[DiscoveryLike] | None = None,
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
    ):
        self._workers = {}

        match (size, discovery):
            case (size, discovery) if size is not None and discovery is not None:
                if size == 0:
                    cpu_count = os.cpu_count()
                    if cpu_count is None:
                        raise ValueError("Unable to determine CPU count")
                    size = cpu_count
                elif size < 0:
                    raise ValueError("Size must be non-negative")

                @asynccontextmanager
                async def create_proxy():
                    discovery_svc, discovery_ctx = await self._enter_context(discovery)
                    if not isinstance(discovery_svc, DiscoveryLike):
                        raise TypeError(
                            f"Expected DiscoveryLike, got: {type(discovery_svc)}"
                        )

                    try:
                        async with self._worker_context(
                            *tags,
                            size=size,
                            factory=worker,
                            publisher=discovery_svc.publisher,
                        ):
                            async with WorkerProxy(
                                discovery=discovery_svc.subscribe(_predicate(tags)),
                                loadbalancer=loadbalancer,
                            ):
                                yield
                    finally:
                        await self._exit_context(discovery_ctx)

            case (size, None) if size is not None:
                if size == 0:
                    cpu_count = os.cpu_count()
                    if cpu_count is None:
                        raise ValueError("Unable to determine CPU count")
                    size = cpu_count
                elif size < 0:
                    raise ValueError("Size must be non-negative")

                namespace = f"pool-{uuid.uuid4().hex}"

                @asynccontextmanager
                async def create_proxy():
                    discovery = LocalDiscovery(namespace)
                    async with self._worker_context(
                        *tags,
                        size=size,
                        factory=worker,
                        publisher=discovery.publisher,
                    ):
                        async with WorkerProxy(
                            discovery=discovery.subscribe(_predicate(tags)),
                            loadbalancer=loadbalancer,
                        ):
                            yield

            case (None, discovery) if discovery is not None:

                @asynccontextmanager
                async def create_proxy():
                    discovery_svc, discovery_ctx = await self._enter_context(discovery)
                    if not isinstance(discovery_svc, DiscoveryLike):
                        raise ValueError
                    try:
                        async with WorkerProxy(
                            discovery=discovery_svc.subscriber,
                            loadbalancer=loadbalancer,
                        ):
                            yield
                    finally:
                        await self._exit_context(discovery_ctx)

            case (None, None):
                cpu_count = os.cpu_count()
                if cpu_count is None:
                    raise ValueError("Unable to determine CPU count")
                size = cpu_count

                namespace = f"pool-{uuid.uuid4().hex}"

                @asynccontextmanager
                async def create_proxy():
                    discovery = LocalDiscovery(namespace)
                    async with self._worker_context(
                        *tags,
                        size=size,
                        factory=worker,
                        publisher=discovery.publisher,
                    ):
                        async with WorkerProxy(
                            discovery=discovery.subscriber,
                            loadbalancer=loadbalancer,
                        ):
                            yield

            case _:
                raise RuntimeError

        self._proxy_factory = create_proxy

    async def __aenter__(self) -> WorkerPool:
        """Starts the worker pool and its services, returning a session.

        This method starts the worker registrar, creates a connection,
        launches all worker processes, and registers them.

        :returns:
            The :class:`WorkerPool` instance itself for method chaining.
        """
        self._proxy_context = self._proxy_factory()
        await self._proxy_context.__aenter__()
        return self

    async def __aexit__(self, *args):
        """Stops all workers and tears down the pool and its services."""
        await self._proxy_context.__aexit__(*args)

    @asynccontextmanager
    async def _worker_context(
        self,
        *tags: str,
        size: int,
        factory: WorkerFactory | None,
        publisher: DiscoveryPublisherLike,
    ):
        if factory is None:
            factory = self._default_worker_factory()
        publisher_svc, publisher_ctx = await self._enter_context(publisher)
        if not isinstance(publisher_svc, DiscoveryPublisherLike):
            raise ValueError

        tasks = []
        for _ in range(size):
            worker = factory(*tags)

            async def start(worker):
                await worker.start()
                await publisher.publish("worker-added", worker.info)

            async def stop(worker):
                await publisher.publish("worker-dropped", worker.info)
                await worker.stop()

            task = asyncio.create_task(start(worker))
            tasks.append(task)
            self._workers[worker] = stop(worker)

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
            yield [w.info for w in self._workers if w.info]
        finally:
            tasks = [asyncio.create_task(stop) for stop in self._workers.values()]
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._exit_context(publisher_ctx)

    def _default_worker_factory(self):
        def factory(*tags, **_):
            return LocalWorker(*tags)

        return factory

    async def _enter_context(self, factory):
        ctx = None
        if isinstance(factory, ContextManager):
            ctx = factory
            obj = ctx.__enter__()
        elif isinstance(factory, AsyncContextManager):
            ctx = factory
            obj = await ctx.__aenter__()
        elif callable(factory):
            return await self._enter_context(factory())
        elif isinstance(factory, Awaitable):
            obj = await factory
        else:
            obj = factory
        return obj, ctx

    async def _exit_context(self, ctx: AsyncContextManager | ContextManager | None):
        if isinstance(ctx, AsyncContextManager):
            await ctx.__aexit__(*sys.exc_info())
        elif isinstance(ctx, ContextManager):
            ctx.__exit__(*sys.exc_info())


def _predicate(tags):
    return lambda w: bool(w.tags & set(tags)) if tags else True
