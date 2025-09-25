from __future__ import annotations

import asyncio
import itertools
import uuid
from typing import TYPE_CHECKING
from typing import AsyncContextManager
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import ContextManager
from typing import Final
from typing import Generic
from typing import Protocol
from typing import Sequence
from typing import TypeAlias
from typing import TypeVar
from typing import overload
from typing import runtime_checkable

import grpc
import grpc.aio

import wool
from wool import _protobuf as pb
from wool._resource_pool import Resource
from wool._resource_pool import ResourcePool
from wool._worker import WorkerClient
from wool._worker_discovery import DiscoveryEvent
from wool._worker_discovery import Factory
from wool._worker_discovery import LocalDiscoveryService
from wool._worker_discovery import ReducibleAsyncIteratorLike
from wool._worker_discovery import WorkerInfo

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


async def client_factory(address: str) -> WorkerClient:
    """Factory function for creating gRPC channels.

    Creates an insecure gRPC channel for the given address.
    The address is passed as the key from ResourcePool.

    :param address:
        The network address (host:port) to create a channel for.
    :returns:
        A new gRPC channel for the address.
    """
    return WorkerClient(address)


async def client_finalizer(client: WorkerClient) -> None:
    """Finalizer function for gRPC channels.

    Closes the gRPC client when it's being cleaned up from the resource pool.

    :param client:
        The gRPC client to close.
    """
    try:
        await client.stop()
    except Exception:
        pass


WorkerUri: TypeAlias = str


class NoWorkersAvailable(Exception):
    """Raised when no workers are available for task dispatch.

    This exception indicates that either no workers exist in the worker pool
    or all available workers have been tried and failed with transient errors.
    """


@runtime_checkable
class LoadBalancerLike(Protocol):
    """Protocol for load balancer v2 that directly dispatches tasks.

    This simplified protocol does not manage discovery services and instead
    operates on a dynamic list of (worker_uri, WorkerInfo) tuples sorted by
    worker_uri. It only defines a dispatch method that accepts a WoolTask and
    returns a task result.

    Expected constructor signature (see LoadBalancerV2Factory):
        __init__(self, workers: list[tuple[str, WorkerInfo]])
    """

    def dispatch(self, task: WoolTask) -> AsyncIterator: ...

    def worker_added_callback(
        self, client: Resource[WorkerClient], info: WorkerInfo
    ): ...

    def worker_updated_callback(
        self, client: Resource[WorkerClient], info: WorkerInfo
    ): ...

    def worker_removed_callback(self, info: WorkerInfo): ...


LoadBalancerFactory: TypeAlias = Factory[LoadBalancerLike]


DispatchCall: TypeAlias = grpc.aio.UnaryStreamCall[pb.task.Task, pb.worker.Response]


class RoundRobinLoadBalancer:
    """Round-robin load balancer for distributing tasks across workers.

    Distributes tasks evenly across available workers using a simple round-robin
    algorithm. Automatically handles worker failures by trying the next worker
    when transient errors occur. Workers are dynamically managed through
    callback methods for addition, updates, and removal.
    """

    TRANSIENT_ERRORS: Final = {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    }

    _current_index: int
    _workers: dict[WorkerInfo, Callable[[], Resource[WorkerClient]]]

    def __init__(self):
        """Initialize the round-robin load balancer.

        Sets up internal state for tracking workers and round-robin index.
        Workers are managed dynamically through callback methods.
        """
        self._current_index = 0
        self._workers = {}

    async def dispatch(self, task: WoolTask) -> AsyncIterator:
        """Dispatch a task to the next available worker using round-robin.

        Tries all workers in one round-robin cycle. If a worker fails with a
        transient error, continues to the next worker. Returns a streaming
        result that automatically manages channel cleanup.

        :param task:
            The WoolTask to dispatch.
        :returns:
            A streaming dispatch result that yields worker responses.
        :raises NoWorkersAvailable:
            If no workers are available or all workers fail with transient errors.
        """
        # Track the first worker URI we try to detect when we've looped back
        checkpoint = None

        while self._workers:
            self._current_index = self._current_index + 1
            if self._current_index >= len(self._workers):
                # Reset index if it's out of bounds
                self._current_index = 0

            worker_info, worker_resource = next(
                itertools.islice(
                    self._workers.items(), self._current_index, self._current_index + 1
                )
            )

            # Check if we've looped back to the first worker we tried
            if checkpoint is None:
                checkpoint = worker_info.uid
            elif worker_info.uid == checkpoint:
                # We've tried all workers and looped back around
                break

            async with worker_resource() as worker:
                async for result in worker.dispatch(task):
                    yield result
                return
        else:
            raise NoWorkersAvailable("No workers available for dispatch")

        # If we get here, all workers failed with transient errors
        raise NoWorkersAvailable(
            f"All {len(self._workers)} workers failed with transient errors"
        )

    def worker_added_callback(self, client: Resource[WorkerClient], info: WorkerInfo):
        self._workers[info] = client

    def worker_updated_callback(self, client: Resource[WorkerClient], info: WorkerInfo):
        self._workers[info] = client

    def worker_removed_callback(self, info: WorkerInfo):
        if info in self._workers:
            del self._workers[info]


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

    _discovery: (
        ReducibleAsyncIteratorLike[DiscoveryEvent]
        | Factory[AsyncIterator[DiscoveryEvent]]
    )
    _discovery_manager: (
        AsyncContextManager[AsyncIterator[DiscoveryEvent]]
        | ContextManager[AsyncIterator[DiscoveryEvent]]
    )
    _loadbalancer = LoadBalancerLike | LoadBalancerFactory
    _loadbalancer_manager: (
        AsyncContextManager[LoadBalancerLike] | ContextManager[LoadBalancerLike]
    )

    @overload
    def __init__(
        self,
        *,
        discovery: (
            ReducibleAsyncIteratorLike[DiscoveryEvent]
            | Factory[AsyncIterator[DiscoveryEvent]]
        ),
        loadbalancer: LoadBalancerLike | LoadBalancerFactory = RoundRobinLoadBalancer,
    ): ...

    @overload
    def __init__(
        self,
        *,
        workers: Sequence[WorkerInfo],
        loadbalancer: LoadBalancerLike | LoadBalancerFactory = RoundRobinLoadBalancer,
    ): ...

    @overload
    def __init__(
        self,
        pool_uri: str,
        *tags: str,
        loadbalancer: LoadBalancerLike | LoadBalancerFactory = RoundRobinLoadBalancer,
    ): ...

    def __init__(
        self,
        pool_uri: str | None = None,
        *tags: str,
        discovery: (
            ReducibleAsyncIteratorLike[DiscoveryEvent]
            | Factory[AsyncIterator[DiscoveryEvent]]
            | None
        ) = None,
        workers: Sequence[WorkerInfo] | None = None,
        loadbalancer: LoadBalancerLike | LoadBalancerFactory = RoundRobinLoadBalancer,
    ):
        if not (pool_uri or discovery or workers):
            raise ValueError(
                "Must specify either a workerpool URI, discovery event stream, or a "
                "sequence of workers"
            )

        self._id: Final = uuid.uuid4()
        self._started = False
        self._workers: dict[WorkerInfo, Resource[WorkerClient]] = {}
        self._loadbalancer = loadbalancer

        match (pool_uri, discovery, workers):
            case (pool_uri, None, None) if pool_uri is not None:
                self._discovery = LocalDiscoveryService(
                    pool_uri, filter=lambda w: bool({pool_uri, *tags} & w.tags)
                )
            case (None, discovery, None) if discovery is not None:
                self._discovery = discovery
            case (None, None, workers) if workers is not None:
                self._discovery = ReducibleAsyncIterator(
                    [DiscoveryEvent(type="worker_added", worker_info=w) for w in workers]
                )
            case _:
                raise ValueError(
                    "Must specify exactly one of: "
                    "pool_uri, discovery_event_stream, or workers"
                )
        self._sentinel_task: asyncio.Task[None] | None = None

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
    def workers(self) -> dict[WorkerInfo, Resource[WorkerClient]]:
        """A list of the currently discovered worker gRPC stubs."""
        return self._workers

    async def start(self) -> None:
        """Starts the proxy by initiating the worker discovery process.

        :raises RuntimeError:
            If the proxy has already been started.
        """
        if self._started:
            raise RuntimeError("Proxy already started")

        (self._loadbalancer_service, self._loadbalancer_ctx) = await self._enter_context(
            self._loadbalancer
        )
        if not isinstance(self._loadbalancer_service, LoadBalancerLike):
            raise ValueError

        self._discovery_service, self._discovery_ctx = await self._enter_context(
            self._discovery
        )
        if not isinstance(self._discovery_service, AsyncIterator):
            raise ValueError

        self._proxy_token = wool.__proxy__.set(self)
        self._client_pool = ResourcePool(
            factory=client_factory, finalizer=client_finalizer, ttl=60
        )
        self._sentinel_task = asyncio.create_task(self._worker_sentinel())
        self._started = True

    async def stop(self, *args) -> None:
        """Stops the proxy, terminating discovery and clearing connections.

        :raises RuntimeError:
            If the proxy was not started first.
        """
        if not self._started:
            raise RuntimeError("Proxy not started - call start() first")

        await self._exit_context(self._discovery_ctx, *args)
        await self._exit_context(self._loadbalancer_ctx, *args)

        wool.__proxy__.reset(self._proxy_token)
        if self._sentinel_task:
            self._sentinel_task.cancel()
            try:
                await self._sentinel_task
            except asyncio.CancelledError:
                pass
            self._sentinel_task = None
        await self._client_pool.clear()

        self._workers.clear()
        self._started = False

    async def dispatch(self, task: WoolTask):
        """Dispatches a task to an available worker in the pool.

        This method selects a worker using a round-robin strategy. If no
        workers are available within the timeout period, it raises an
        exception.

        :param task:
            The :py:class:`WoolTask` object to be dispatched.
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
        async for result in self._loadbalancer_service.dispatch(task):
            yield result

    async def _enter_context(self, factory):
        ctx = None
        if callable(factory):
            obj = factory()
            if isinstance(obj, ContextManager):
                ctx = obj
                obj = obj.__enter__()
            elif isinstance(obj, AsyncContextManager):
                ctx = obj
                obj = await obj.__aenter__()
            elif isinstance(obj, Awaitable):
                obj = await obj
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
        while not self._loadbalancer_service._workers:
            await asyncio.sleep(0)

    async def _worker_sentinel(self):
        assert isinstance(self._discovery_service, AsyncIterator)
        assert isinstance(self._loadbalancer_service, LoadBalancerLike)
        async for event in self._discovery_service:
            match event.type:
                case "worker_added":
                    self._loadbalancer_service.worker_added_callback(
                        lambda: self._client_pool.get(
                            f"{event.worker_info.host}:{event.worker_info.port}",
                        ),
                        event.worker_info,
                    )
                case "worker_updated":
                    self._loadbalancer_service.worker_updated_callback(
                        lambda: self._client_pool.get(
                            f"{event.worker_info.host}:{event.worker_info.port}",
                        ),
                        event.worker_info,
                    )
                case "worker_removed":
                    if event.worker_info.uid in self._workers:
                        self._loadbalancer_service.worker_removed_callback(
                            event.worker_info
                        )
