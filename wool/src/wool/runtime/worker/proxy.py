from __future__ import annotations

import asyncio
import uuid
from functools import partial
from typing import TYPE_CHECKING
from typing import AsyncContextManager
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import ContextManager
from typing import Generic
from typing import Sequence
from typing import TypeAlias
from typing import TypeVar
from typing import overload

import wool
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.base import WorkerMetadata
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.base import LoadBalancerContext
from wool.runtime.loadbalancer.base import LoadBalancerLike
from wool.runtime.loadbalancer.roundrobin import RoundRobinLoadBalancer
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.typing import Factory
from wool.runtime.worker.base import ChannelCredentialsType
from wool.runtime.worker.connection import WorkerConnection

if TYPE_CHECKING:
    from wool.runtime.routine.task import Task

T = TypeVar("T")


class ReducibleAsyncIterator(Generic[T]):
    """An async iterator that can be pickled via __reduce__.

    Converts a sequence into an async iterator while maintaining
    picklability for distributed task execution contexts.

    :param items:
        Sequence of items to convert to async iterator.
    """

    def __init__(self, items: Sequence[T]):
        self._items = items
        self._index = 0

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        if self._index >= len(self._items):
            raise StopAsyncIteration
        item = self._items[self._index]
        self._index += 1
        return item

    def __reduce__(self) -> tuple:
        """Return constructor args for unpickling."""
        return (self.__class__, (self._items,))


async def connection_factory(
    target: str, credentials: ChannelCredentialsType = None
) -> WorkerConnection:
    """Factory function for creating worker connections.

    Creates a connection to the specified worker target.
    The target is passed as the key from ResourcePool.

    :param target:
        The network target (host:port) to create a channel for.
    :param credentials:
        Optional channel credentials for TLS/mTLS.
    :returns:
        A new connection to the target.
    """
    return WorkerConnection(target, credentials=credentials)


async def connection_finalizer(connection: WorkerConnection) -> None:
    """Finalizer function for gRPC channels.

    Closes the gRPC connection when it's being cleaned up from the resource pool.

    :param connection:
        The gRPC connection to close.
    """
    try:
        await connection.close()
    except Exception:
        pass


WorkerUri: TypeAlias = str


class WorkerProxy:
    """Client-side proxy for dispatching tasks to distributed workers.

    Manages worker discovery, connection pooling, and load-balanced task
    routing. The bridge between :func:`@wool.routine <wool.routine>` decorated
    functions and the worker pool.

    Connects to workers through discovery services, pool URIs, or static
    worker lists. Handles connection lifecycle and fault tolerance
    automatically.

    **Connect via pool URI:**

    .. code-block:: python

        async with WorkerProxy("pool-abc123") as proxy:
            result = await task()

    **Connect via discovery:**

    .. code-block:: python

        from wool.runtime.discovery.lan import LanDiscovery

        discovery = LanDiscovery().subscribe()
        async with WorkerProxy(discovery=discovery) as proxy:
            result = await task()

    **Connect to static workers:**

    .. code-block:: python

        workers = [
            WorkerMetadata(host="10.0.0.1", port=50051, ...),
            WorkerMetadata(host="10.0.0.2", port=50051, ...),
        ]
        async with WorkerProxy(workers=workers) as proxy:
            result = await task()

    **Custom load balancer:**

    .. code-block:: python

        from wool.runtime.loadbalancer.roundrobin import RoundRobinLoadBalancer


        class CustomBalancer(RoundRobinLoadBalancer):
            async def dispatch(self, task, context, timeout=None):
                # Custom routing strategy
                ...


        async with WorkerProxy(
            discovery=discovery,
            loadbalancer=CustomBalancer(),
        ) as proxy:
            result = await task()

    :param pool_uri:
        Pool identifier for discovery-based connection.
    :param tags:
        Additional tags for filtering discovered workers.
    :param discovery:
        Discovery service or event stream.
    :param workers:
        Static worker list for direct connection.
    :param loadbalancer:
        Load balancer instance, factory, or context manager.
    :param credentials:
        Optional channel credentials for TLS/mTLS connections to workers.
    """

    _discovery: DiscoverySubscriberLike | Factory[DiscoverySubscriberLike]
    _discovery_manager: (
        AsyncContextManager[DiscoverySubscriberLike]
        | ContextManager[DiscoverySubscriberLike]
    )

    _loadbalancer = LoadBalancerLike | Factory[LoadBalancerLike]
    _loadbalancer_manager: (
        AsyncContextManager[LoadBalancerLike] | ContextManager[LoadBalancerLike]
    )
    _credentials: ChannelCredentialsType

    @overload
    def __init__(
        self,
        *,
        discovery: DiscoverySubscriberLike | Factory[DiscoverySubscriberLike],
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: ChannelCredentialsType | None = None,
    ): ...

    @overload
    def __init__(
        self,
        *,
        workers: Sequence[WorkerMetadata],
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: ChannelCredentialsType | None = None,
    ): ...

    @overload
    def __init__(
        self,
        pool_uri: str,
        *tags: str,
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: ChannelCredentialsType | None = None,
    ): ...

    def __init__(
        self,
        pool_uri: str | None = None,
        *tags: str,
        discovery: (
            DiscoverySubscriberLike | Factory[DiscoverySubscriberLike] | None
        ) = None,
        workers: Sequence[WorkerMetadata] | None = None,
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: ChannelCredentialsType | None = None,
    ):
        if not (pool_uri or discovery or workers):
            raise ValueError(
                "Must specify either a workerpool URI, discovery event stream, or a "
                "sequence of workers"
            )

        self._id: uuid.UUID = uuid.uuid4()
        self._started = False
        self._loadbalancer = loadbalancer
        self._credentials = credentials

        # Create security filter based on resolved credentials
        security_filter = self._create_security_filter(self._credentials)

        match (pool_uri, discovery, workers):
            case (pool_uri, None, None) if pool_uri is not None:
                # Combine tag filter with security filter
                def combined_filter(w):
                    return bool({pool_uri, *tags} & w.tags) and security_filter(w)

                self._discovery = LocalDiscovery(pool_uri).subscribe(
                    filter=combined_filter
                )
            case (None, discovery, None) if discovery is not None:
                self._discovery = discovery
            case (None, None, workers) if workers is not None:
                # Filter workers by security compatibility
                compatible_workers = [w for w in workers if security_filter(w)]
                self._discovery = ReducibleAsyncIterator(
                    [
                        DiscoveryEvent("worker-added", metadata=w)
                        for w in compatible_workers
                    ]
                )
            case _:
                raise ValueError(
                    "Must specify exactly one of: "
                    "pool_uri, discovery_event_stream, or workers"
                )
        self._sentinel_task: asyncio.Task[None] | None = None
        self._loadbalancer_context: LoadBalancerContext | None = None

    async def __aenter__(self):
        """Starts the proxy and sets it as the active context."""
        await self.start()
        return self

    async def __aexit__(self, *args):
        """Stops the proxy and resets the active context."""
        await self.stop(*args)

    def __hash__(self) -> int:
        return hash(str(self.id))

    def __eq__(self, value: object) -> bool:
        return isinstance(value, WorkerProxy) and hash(self) == hash(value)

    def __reduce__(self) -> tuple:
        """Return constructor args for unpickling with proxy ID preserved.

        Creates a new WorkerProxy instance with the same discovery stream and
        load balancer type, then sets the preserved proxy ID on the new object.
        Workers will be re-discovered on the new instance.

        :returns:
            Tuple of (callable, args, state) for unpickling.
        """

        def _restore_proxy(discovery, loadbalancer, proxy_id):
            proxy = WorkerProxy(discovery=discovery, loadbalancer=loadbalancer)
            proxy._id = proxy_id
            return proxy

        return (
            _restore_proxy,
            (self._discovery, self._loadbalancer, self._id),
        )

    @property
    def id(self) -> uuid.UUID:
        return self._id

    @property
    def started(self) -> bool:
        return self._started

    @property
    def workers(self) -> list[WorkerMetadata]:
        """A list of the currently discovered worker gRPC stubs."""
        if self._loadbalancer_context:
            return list(self._loadbalancer_context.workers.keys())
        else:
            return []

    async def start(self) -> None:
        """Starts the proxy by initiating the worker discovery process.

        :raises RuntimeError:
            If the proxy has already been started.
        """
        if self._started:
            raise RuntimeError("Proxy already started")

        (
            self._loadbalancer_service,
            self._loadbalancer_context_manager,
        ) = await self._enter_context(self._loadbalancer)
        if not isinstance(self._loadbalancer_service, LoadBalancerLike):
            raise ValueError

        (
            self._discovery_stream,
            self._discovery_context_manager,
        ) = await self._enter_context(self._discovery)
        if not isinstance(self._discovery_stream, DiscoverySubscriberLike):
            raise ValueError

        self._proxy_token = wool.__proxy__.set(self)
        # Create connection factory with credentials bound
        factory = partial(connection_factory, credentials=self._credentials)
        self._connection_pool = ResourcePool(
            factory=factory, finalizer=connection_finalizer, ttl=60
        )
        self._loadbalancer_context = LoadBalancerContext()
        self._sentinel_task = asyncio.create_task(self._worker_sentinel())
        self._started = True

    async def stop(self, *args) -> None:
        """Stops the proxy, terminating discovery and clearing connections.

        :raises RuntimeError:
            If the proxy was not started first.
        """
        if not self._started:
            raise RuntimeError("Proxy not started - call start() first")

        await self._exit_context(self._discovery_context_manager, *args)
        await self._exit_context(self._loadbalancer_context_manager, *args)

        wool.__proxy__.reset(self._proxy_token)
        if self._sentinel_task:
            self._sentinel_task.cancel()
            try:
                await self._sentinel_task
            except asyncio.CancelledError:
                pass
            self._sentinel_task = None
        await self._connection_pool.clear()
        self._loadbalancer_context = None
        self._started = False

    async def dispatch(
        self, task: Task, *, timeout: float | None = None
    ) -> AsyncGenerator:
        """Dispatches a task to an available worker in the pool.

        This method selects a worker using a round-robin strategy. If no
        workers are available within the timeout period, it raises an
        exception.

        :param task:
            The :class:`Task` object to be dispatched.
        :param timeout:
            Timeout in seconds for getting a worker.
        :returns:
            A protobuf result object from the worker.
        :raises RuntimeError:
            If the proxy is not started.
        :raises asyncio.TimeoutError:
            If no worker is available within the timeout period.
        """
        if not self._started:
            raise RuntimeError("Proxy not started - call start() first")

        await asyncio.wait_for(self._await_workers(), 60)

        assert isinstance(self._loadbalancer_service, LoadBalancerLike)
        assert self._loadbalancer_context
        return await self._loadbalancer_service.dispatch(
            task, context=self._loadbalancer_context, timeout=timeout
        )

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

    async def _exit_context(
        self, ctx: AsyncContextManager | ContextManager | None, *args
    ):
        if isinstance(ctx, AsyncContextManager):
            await ctx.__aexit__(*args)
        elif isinstance(ctx, ContextManager):
            ctx.__exit__(*args)

    def _create_security_filter(
        self, credentials: ChannelCredentialsType
    ) -> Callable[[WorkerMetadata], bool]:
        """Create discovery filter based on proxy credentials.

        Workers and proxies must have compatible security settings:
        - Proxy with credentials only discovers workers with secure=True
        - Proxy without credentials only discovers workers with secure=False

        :param credentials:
            Channel credentials for this proxy.
        :returns:
            Predicate function for filtering workers by security compatibility.
        """
        if credentials is not None:
            # Proxy has credentials: only accept secure workers
            return lambda metadata: metadata.secure
        else:
            # Proxy has no credentials: only accept insecure workers
            return lambda metadata: not metadata.secure

    async def _await_workers(self):
        while not self._loadbalancer_context or not self._loadbalancer_context.workers:
            await asyncio.sleep(0)

    async def _worker_sentinel(self):
        assert self._loadbalancer_context
        async for event in self._discovery_stream:
            match event.type:
                case "worker-added":
                    # Use default argument to capture metadata by value, not reference
                    self._loadbalancer_context.add_worker(
                        event.metadata,
                        lambda m=event.metadata: self._connection_pool.get(
                            f"{m.host}:{m.port}",
                        ),
                    )
                case "worker-updated":
                    # Use default argument to capture metadata by value, not reference
                    self._loadbalancer_context.update_worker(
                        event.metadata,
                        lambda m=event.metadata: self._connection_pool.get(
                            f"{m.host}:{m.port}",
                        ),
                    )
                case "worker-dropped":
                    self._loadbalancer_context.remove_worker(event.metadata)
            event.emit(context={"proxy_id": str(self.id)})
