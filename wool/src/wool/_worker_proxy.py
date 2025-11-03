from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING
from typing import AsyncContextManager
from typing import AsyncIterator
from typing import Awaitable
from typing import ContextManager
from typing import Generic
from typing import Sequence
from typing import TypeAlias
from typing import TypeVar
from typing import overload

import wool
from wool._connection import Connection
from wool._resource_pool import ResourcePool
from wool.core.discovery.base import DiscoveryEvent
from wool.core.discovery.base import DiscoverySubscriberLike
from wool.core.discovery.base import WorkerInfo
from wool.core.discovery.local import LocalDiscovery
from wool.core.loadbalancer.base import LoadBalancerContext
from wool.core.loadbalancer.base import LoadBalancerLike
from wool.core.loadbalancer.roundrobin import RoundRobinLoadBalancer
from wool.core.typing import Factory

if TYPE_CHECKING:
    from wool._work import WoolTask

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


async def connection_factory(target: str) -> Connection:
    """Factory function for creating worker connections.

    Creates a connection to the specified worker target.
    The target is passed as the key from ResourcePool.

    :param target:
        The network target (host:port) to create a channel for.
    :returns:
        A new connection to the target.
    """
    return Connection(target)


async def connection_finalizer(connection: Connection) -> None:
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


# public
class WorkerProxy:
    """Client-side interface for task dispatch to distributed workers.

    The WorkerProxy manages worker discovery, load balancing, and task routing
    within the wool framework. It serves as the bridge between task decorators
    and the underlying worker pool, handling connection management and fault
    tolerance transparently.

    Supports multiple configuration modes:
    - Pool URI-based discovery for connecting to specific worker pools
    - Custom discovery services for advanced deployment scenarios
    - Static worker lists for testing and development
    - Configurable load balancing strategies

    :param pool_uri:
        Unique identifier for connecting to a specific worker pool.
    :param tags:
        Additional capability tags for filtering discovered workers.
    :param discovery:
        Custom discovery service or event stream for finding workers.
    :param workers:
        Static list of workers for direct connection (testing/development).
    :param loadbalancer:
        Load balancer implementation or factory for task distribution.
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

    @overload
    def __init__(
        self,
        *,
        discovery: DiscoverySubscriberLike | Factory[DiscoverySubscriberLike],
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
    ): ...

    @overload
    def __init__(
        self,
        *,
        workers: Sequence[WorkerInfo],
        loadbalancer: LoadBalancerLike
        | Factory[LoadBalancerLike] = RoundRobinLoadBalancer,
    ): ...

    @overload
    def __init__(
        self,
        pool_uri: str,
        *tags: str,
        loadbalancer: LoadBalancerLike
        | Factory[LoadBalancerLike] = RoundRobinLoadBalancer,
    ): ...

    def __init__(
        self,
        pool_uri: str | None = None,
        *tags: str,
        discovery: (
            DiscoverySubscriberLike | Factory[DiscoverySubscriberLike] | None
        ) = None,
        workers: Sequence[WorkerInfo] | None = None,
        loadbalancer: LoadBalancerLike
        | Factory[LoadBalancerLike] = RoundRobinLoadBalancer,
    ):
        if not (pool_uri or discovery or workers):
            raise ValueError(
                "Must specify either a workerpool URI, discovery event stream, or a "
                "sequence of workers"
            )

        self._id: uuid.UUID = uuid.uuid4()
        self._started = False
        self._loadbalancer = loadbalancer

        match (pool_uri, discovery, workers):
            case (pool_uri, None, None) if pool_uri is not None:
                self._discovery = LocalDiscovery(pool_uri).subscribe(
                    filter=lambda w: bool({pool_uri, *tags} & w.tags)
                )
            case (None, discovery, None) if discovery is not None:
                self._discovery = discovery
            case (None, None, workers) if workers is not None:
                self._discovery = ReducibleAsyncIterator(
                    [DiscoveryEvent(type="worker-added", worker_info=w) for w in workers]
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
    def workers(self) -> list[WorkerInfo]:
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
        self._connection_pool = ResourcePool(
            factory=connection_factory, finalizer=connection_finalizer, ttl=60
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

    async def dispatch(self, task: WoolTask, *, timeout: float | None = None):
        """Dispatches a task to an available worker in the pool.

        This method selects a worker using a round-robin strategy. If no
        workers are available within the timeout period, it raises an
        exception.

        :param task:
            The :class:`WoolTask` object to be dispatched.
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

    async def _await_workers(self):
        while not self._loadbalancer_context or not self._loadbalancer_context.workers:
            await asyncio.sleep(0)

    async def _worker_sentinel(self):
        assert self._loadbalancer_context
        async for event in self._discovery_stream:
            match event.type:
                case "worker-added":
                    self._loadbalancer_context.add_worker(
                        event.worker_info,
                        lambda: self._connection_pool.get(
                            f"{event.worker_info.host}:{event.worker_info.port}",
                        ),
                    )
                case "worker-updated":
                    self._loadbalancer_context.update_worker(
                        event.worker_info,
                        lambda: self._connection_pool.get(
                            f"{event.worker_info.host}:{event.worker_info.port}",
                        ),
                    )
                case "worker-dropped":
                    self._loadbalancer_context.remove_worker(event.worker_info)
