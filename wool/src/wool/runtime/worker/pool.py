from __future__ import annotations

import asyncio
import os
import sys
import uuid
import warnings
from contextlib import asynccontextmanager
from typing import AsyncContextManager
from typing import Awaitable
from typing import ContextManager
from typing import Coroutine
from typing import Final
from typing import overload

from typing_extensions import deprecated

from wool.runtime.discovery.base import DiscoveryLike
from wool.runtime.discovery.base import DiscoveryPublisherLike
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.typing import Factory
from wool.runtime.worker.auth import WorkerCredentials
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

    - **Ephemeral pools** spawn local workers managed within the pool's
    lifecycle. Perfect for development and single-machine deployments.

    - **Durable pools** connect to existing remote workers through discovery
    services. Workers run independently, serving multiple clients across
    distributed deployments.

    **Basic ephemeral pool:**

    .. code-block:: python

        @wool.routine
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

        async with WorkerPool("gpu-capable", spawn=4):
            result = await gpu_task()

    **Custom worker factory:**

    .. code-block:: python

        from functools import partial

        worker_factory = partial(LocalWorker, host="0.0.0.0")

        async with WorkerPool(spawn=8, worker=worker_factory):
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
        async with WorkerPool(spawn=4, discovery=LanDiscovery()):
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
    :param spawn:
        Number of workers to spawn (0 = CPU count).
    :param size:
        .. deprecated::
            Use ``spawn`` instead. Will be removed in the next major release.
    :param lease:
        Maximum number of additionally discovered workers to admit to the pool.
        The total pool capacity is ``spawn + lease`` when both are set, or just
        ``lease`` for external pools. Defaults to ``None`` (unbounded).
    :param worker:
        Worker factory callable. Defaults to :class:`LocalWorker`.
    :param loadbalancer:
        Load balancer instance, factory, or context manager.
    :param discovery:
        Discovery service instance, factory, or context manager.
    :param credentials:
        Optional channel credentials for TLS/mTLS connections to workers.
    :raises ValueError:
        If configuration is invalid or CPU count unavailable.

    .. caution::

       Pre-called context manager instances passed as ``loadbalancer``
       or ``discovery`` are not picklable and will cause nested routine
       dispatch to fail.  Pass a callable returning the context manager
       instead.  See :data:`Factory`.
    """

    _workers: Final[dict[WorkerLike, Coroutine]]

    @overload
    def __init__(
        self,
        *tags: str,
        spawn: int = 0,
        lease: int | None = None,
        worker: WorkerFactory = LocalWorker,
        discovery: None = None,
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | None = None,
        lazy: bool = True,
    ):
        """
        Create an ephemeral pool of workers, spawning the specified
        quantity of workers using the specified worker factory.
        """
        ...

    @overload
    def __init__(
        self,
        *,
        lease: int | None = None,
        discovery: DiscoveryLike | Factory[DiscoveryLike],
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | None = None,
        lazy: bool = True,
    ):
        """
        Connect to an existing pool of workers discovered by the
        specified discovery protocol.
        """
        ...

    @overload
    def __init__(
        self,
        *tags: str,
        spawn: int = 0,
        lease: int | None = None,
        worker: WorkerFactory = LocalWorker,
        discovery: DiscoveryLike | Factory[DiscoveryLike],
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | None = None,
        lazy: bool = True,
    ):
        """
        Create a hybrid pool that spawns local workers and discovers
        remote workers through the specified discovery protocol.
        """
        ...

    @overload
    @deprecated("Use 'spawn' instead of 'size'.")
    def __init__(
        self,
        *tags: str,
        size: int,
        lease: int | None = None,
        worker: WorkerFactory = LocalWorker,
        discovery: None = None,
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | None = None,
        lazy: bool = True,
    ): ...

    @overload
    @deprecated("Use 'spawn' instead of 'size'.")
    def __init__(
        self,
        *tags: str,
        size: int,
        lease: int | None = None,
        worker: WorkerFactory = LocalWorker,
        discovery: DiscoveryLike | Factory[DiscoveryLike],
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | None = None,
        lazy: bool = True,
    ): ...

    def __init__(
        self,
        *tags: str,
        spawn: int | None = None,
        size: int | None = None,
        lease: int | None = None,
        worker: WorkerFactory | None = None,
        discovery: DiscoveryLike | Factory[DiscoveryLike] | None = None,
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | None = None,
        lazy: bool = True,
    ):
        self._workers = {}
        self._credentials = credentials
        self._lazy = lazy

        if size is not None and spawn is not None:
            raise TypeError(
                "Cannot specify both 'spawn' and 'size'. "
                "Use 'spawn' instead — 'size' is deprecated."
            )
        if size is not None:
            warnings.warn(
                "The 'size' parameter is deprecated. Use 'spawn' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            spawn = size

        if lease is not None and lease < 0:
            raise ValueError("Lease must be non-negative")

        match (spawn, discovery):
            case (spawn, discovery) if spawn is not None and discovery is not None:
                spawn = _resolve_spawn(spawn)
                max_workers = spawn + lease if lease is not None else None

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
                            spawn=spawn,
                            factory=worker,
                            publisher=discovery_svc.publisher,
                        ):
                            async with WorkerProxy(
                                discovery=discovery_svc.subscribe(_predicate(tags)),
                                loadbalancer=loadbalancer,
                                credentials=self._credentials,
                                lease=max_workers,
                                lazy=self._lazy,
                            ):
                                yield
                    finally:
                        await self._exit_context(discovery_ctx)

            case (spawn, None) if spawn is not None:
                spawn = _resolve_spawn(spawn)
                max_workers = spawn + lease if lease is not None else None

                namespace = f"pool-{uuid.uuid4().hex}"

                @asynccontextmanager
                async def create_proxy():
                    with LocalDiscovery(namespace) as discovery:
                        async with self._worker_context(
                            *tags,
                            spawn=spawn,
                            factory=worker,
                            publisher=discovery.publisher,
                        ):
                            async with WorkerProxy(
                                discovery=discovery.subscribe(_predicate(tags)),
                                loadbalancer=loadbalancer,
                                credentials=self._credentials,
                                lease=max_workers,
                                lazy=self._lazy,
                            ):
                                yield

            case (None, discovery) if discovery is not None:
                if lease is not None and lease == 0:
                    raise ValueError("Lease must be positive for discovery-only pools")

                @asynccontextmanager
                async def create_proxy():
                    discovery_svc, discovery_ctx = await self._enter_context(discovery)
                    if not isinstance(discovery_svc, DiscoveryLike):
                        raise ValueError
                    try:
                        async with WorkerProxy(
                            discovery=discovery_svc.subscriber,
                            loadbalancer=loadbalancer,
                            credentials=self._credentials,
                            lease=lease,
                            lazy=self._lazy,
                        ):
                            yield
                    finally:
                        await self._exit_context(discovery_ctx)

            case (None, None):
                spawn = _resolve_spawn(0)
                max_workers = spawn + lease if lease is not None else None

                namespace = f"pool-{uuid.uuid4().hex}"

                @asynccontextmanager
                async def create_proxy():
                    with LocalDiscovery(namespace) as discovery:
                        async with self._worker_context(
                            *tags,
                            spawn=spawn,
                            factory=worker,
                            publisher=discovery.publisher,
                        ):
                            async with WorkerProxy(
                                discovery=discovery.subscriber,
                                loadbalancer=loadbalancer,
                                credentials=self._credentials,
                                lease=max_workers,
                                lazy=self._lazy,
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
        spawn: int,
        factory: WorkerFactory | None,
        publisher: DiscoveryPublisherLike,
    ):
        if factory is None:
            factory = self._default_worker_factory()
        publisher_svc, publisher_ctx = await self._enter_context(publisher)
        if not isinstance(publisher_svc, DiscoveryPublisherLike):
            raise ValueError

        tasks = []
        for _ in range(spawn):
            worker = factory(*tags, credentials=self._credentials)

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
            results = await asyncio.gather(*tasks, return_exceptions=True)
            if errors := [r for r in results if isinstance(r, Exception)]:
                raise ExceptionGroup("worker spawn failures", errors)
            yield [w.metadata for w in self._workers if w.metadata]
        finally:
            tasks = [asyncio.create_task(stop) for stop in self._workers.values()]
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._exit_context(publisher_ctx)

    def _default_worker_factory(self):
        def factory(*tags, credentials=None):
            return LocalWorker(*tags, credentials=credentials)

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


def _resolve_spawn(spawn: int) -> int:
    if spawn == 0:
        cpu_count = os.cpu_count()
        if cpu_count is None:
            raise ValueError("Unable to determine CPU count")
        spawn = cpu_count
    elif spawn < 0:
        raise ValueError("Spawn must be non-negative")
    return spawn


def _predicate(tags):
    return lambda w: bool(w.tags & set(tags)) if tags else True
