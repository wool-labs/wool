from __future__ import annotations

import asyncio
import hashlib
import os
import uuid
from functools import partial
from multiprocessing.shared_memory import SharedMemory
from typing import AsyncIterator
from typing import Final
from typing import overload

from wool._worker import LocalWorker
from wool._worker import Worker
from wool._worker import WorkerFactory
from wool._worker_discovery import DiscoveryEvent
from wool._worker_discovery import Factory
from wool._worker_discovery import LocalDiscoveryService
from wool._worker_discovery import LocalRegistryService
from wool._worker_discovery import ReducibleAsyncIteratorLike
from wool._worker_discovery import RegistryServiceLike
from wool._worker_proxy import LoadBalancerFactory
from wool._worker_proxy import LoadBalancerLike
from wool._worker_proxy import RoundRobinLoadBalancer
from wool._worker_proxy import WorkerProxy


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
        from wool._worker_discovery import LocalRegistryService
        from functools import partial

        # Custom worker factory with specific tags
        worker_factory = partial(
            LocalWorker, registry_service=LocalRegistryService("my-pool")
        )

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
        from wool._worker_discovery import LanDiscoveryService

        # Connect to existing workers on the network
        discovery = LanDiscoveryService(filter=lambda w: "production" in w.tags)

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
            discovery=LanDiscoveryService(filter=lambda w: "prod" in w.tags)

            # Instance factory
            discovery=lambda: LocalDiscoveryService("pool-123")

            # Context manager factory
            @asynccontextmanager
            async def discovery():
                service = await DatabaseDiscoveryService.create(connection_string)
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

    _workers: Final[list[Worker]]
    _shared_memory = None

    @overload
    def __init__(
        self,
        *tags: str,
        size: int = 0,
        worker: WorkerFactory[RegistryServiceLike] = LocalWorker[LocalRegistryService],
        loadbalancer: LoadBalancerLike | LoadBalancerFactory = RoundRobinLoadBalancer,
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
        discovery: (
            ReducibleAsyncIteratorLike[DiscoveryEvent]
            | Factory[AsyncIterator[DiscoveryEvent]]
        ),
        loadbalancer: LoadBalancerLike | LoadBalancerFactory = RoundRobinLoadBalancer,
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
        loadbalancer: LoadBalancerLike | LoadBalancerFactory = RoundRobinLoadBalancer,
        discovery: (
            ReducibleAsyncIteratorLike[DiscoveryEvent]
            | Factory[AsyncIterator[DiscoveryEvent]]
            | None
        ) = None,
    ):
        self._workers = []

        match (size, discovery):
            case (None, None):
                cpu_count = os.cpu_count()
                if cpu_count is None:
                    raise ValueError("Unable to determine CPU count")
                size = cpu_count

                uri = f"pool-{uuid.uuid4().hex}"

                async def create_proxy():
                    self._shared_memory = SharedMemory(
                        name=hashlib.sha256(uri.encode()).hexdigest()[:12],
                        create=True,
                        size=1024,
                    )
                    for i in range(1024):
                        self._shared_memory.buf[i] = 0
                    await self._spawn_workers(uri, *tags, size=size, factory=worker)
                    return WorkerProxy(
                        discovery=LocalDiscoveryService(uri),
                        loadbalancer=loadbalancer,
                    )

            case (size, None) if size is not None:
                if size == 0:
                    cpu_count = os.cpu_count()
                    if cpu_count is None:
                        raise ValueError("Unable to determine CPU count")
                    size = cpu_count
                elif size < 0:
                    raise ValueError("Size must be non-negative")

                uri = f"pool-{uuid.uuid4().hex}"

                async def create_proxy():
                    self._shared_memory = SharedMemory(
                        name=hashlib.sha256(uri.encode()).hexdigest()[:12],
                        create=True,
                        size=1024,
                    )
                    for i in range(1024):
                        self._shared_memory.buf[i] = 0
                    await self._spawn_workers(uri, *tags, size=size, factory=worker)
                    return WorkerProxy(
                        discovery=LocalDiscoveryService(uri),
                        loadbalancer=loadbalancer,
                    )

            case (None, discovery) if discovery is not None:

                async def create_proxy():
                    return WorkerProxy(
                        discovery=discovery,
                        loadbalancer=loadbalancer,
                    )

            case _:
                raise RuntimeError

        self._proxy_factory = create_proxy

    async def __aenter__(self) -> WorkerPool:
        """Starts the worker pool and its services, returning a session.

        This method starts the worker registry, creates a client session,
        launches all worker processes, and registers them.

        :returns:
            The :py:class:`WorkerPool` instance itself for method chaining.
        """
        self._proxy = await self._proxy_factory()
        await self._proxy.__aenter__()
        return self

    async def __aexit__(self, *args):
        """Stops all workers and tears down the pool and its services."""
        try:
            await self._stop_workers()
            await self._proxy.__aexit__(*args)
        finally:
            if self._shared_memory is not None:
                self._shared_memory.unlink()

    async def _spawn_workers(
        self, uri, *tags: str, size: int, factory: WorkerFactory | None
    ):
        if factory is None:
            factory = partial(LocalWorker, registry_service=LocalRegistryService(uri))

        tasks = []
        for _ in range(size):
            worker = factory(*tags)
            task = asyncio.create_task(worker.start())
            tasks.append(task)
            self._workers.append(worker)

        await asyncio.gather(*tasks, return_exceptions=True)

        return [w.info for w in self._workers if w.info]

    async def _stop_workers(self):
        """Sends a stop command to all workers and unregisters them."""
        tasks = [asyncio.create_task(worker.stop()) for worker in self._workers]
        await asyncio.gather(*tasks, return_exceptions=True)
