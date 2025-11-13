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

from wool.runtime.discovery.base import DiscoveryLike
from wool.runtime.discovery.base import DiscoveryPublisherLike
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.typing import Factory
from wool.runtime.worker.base import WorkerFactory
from wool.runtime.worker.base import WorkerLike
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.proxy import LoadBalancerLike
from wool.runtime.worker.proxy import RoundRobinLoadBalancer
from wool.runtime.worker.proxy import WorkerProxy


# public
class WorkerPool:
    """Orchestrates distributed workers for task execution.

    The core of wool's distributed runtime. Manages worker lifecycle,
    discovery, and load balancing across two modes:

    - **Ephemeral pools** spawn local workers automatically managed within the
    pool's lifecycle. Perfect for development and single-machine deployments.

    - **Durable pools** connect to existing remote workers through discovery
    services. Workers run independently, serving multiple clients across
    distributed deployments.

    **Basic ephemeral pool:**

    .. code-block:: python

        import wool


        @wool.work
        async def fibonacci(n: int) -> int:
            if n <= 1:
                return n
            a = await fibonacci(n - 1)
            b = await fibonacci(n - 2)
            return a + b


        async with wool.WorkerPool():
            result = await fibonacci(10)

    **Ephemeral with tags:**

    .. code-block:: python

        async with WorkerPool("gpu-capable", size=4):
            result = await gpu_task()

    **Custom worker factory:**

    .. code-block:: python

        from functools import partial

        worker_factory = partial(LocalWorker, host="0.0.0.0")

        async with WorkerPool(size=8, worker=worker_factory):
            result = await task()

    **Durable pool:**

    .. code-block:: python

        from wool.runtime.discovery.lan import LanDiscovery

        async with WorkerPool(discovery=LanDiscovery()):
            result = await task()

    **Filtered discovery:**

    .. code-block:: python

        discovery = LanDiscovery().subscribe(filter=lambda w: "production" in w.tags)
        async with WorkerPool(discovery=discovery):
            result = await task()

    **Hybrid pool:**

    .. code-block:: python

        # Spawn local workers AND discover remote workers
        async with WorkerPool(size=4, discovery=LanDiscovery()):
            result = await task()

    **Custom load balancer:**

    .. code-block:: python

        from wool.runtime.loadbalancer.roundrobin import RoundRobinLoadBalancer


        class PriorityBalancer(RoundRobinLoadBalancer):
            async def dispatch(self, task, context, timeout=None):
                # Custom routing logic
                ...


        async with WorkerPool(loadbalancer=PriorityBalancer()):
            result = await task()

    **Custom discovery:**

    .. code-block:: python

        from contextlib import asynccontextmanager


        @asynccontextmanager
        async def custom_discovery():
            svc = await DatabaseDiscovery.connect()
            try:
                yield svc.subscribe()
            finally:
                await svc.close()


        async with WorkerPool(discovery=custom_discovery):
            result = await task()

    :param tags:
        Capability tags for spawned workers.
    :param size:
        Number of workers to spawn (0 = CPU count).
    :param worker:
        Worker factory callable. Defaults to :class:`LocalWorker`.
    :param loadbalancer:
        Load balancer instance, factory, or context manager.
    :param discovery:
        Discovery service instance, factory, or context manager.
    :raises ValueError:
        If configuration is invalid or CPU count unavailable.
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
                await publisher.publish("worker-added", worker.metadata)

            async def stop(worker):
                await publisher.publish("worker-dropped", worker.metadata)
                await worker.stop()

            task = asyncio.create_task(start(worker))
            tasks.append(task)
            self._workers[worker] = stop(worker)

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
            yield [w.metadata for w in self._workers if w.metadata]
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
