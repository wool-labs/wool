from __future__ import annotations

import asyncio
import uuid
import warnings
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

from packaging.version import InvalidVersion
from packaging.version import Version

import wool
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.base import DelegatingLoadBalancerLike
from wool.runtime.loadbalancer.base import DispatchingLoadBalancerLike
from wool.runtime.loadbalancer.base import LoadBalancerContext
from wool.runtime.loadbalancer.base import LoadBalancerLike
from wool.runtime.loadbalancer.base import NoWorkersAvailable
from wool.runtime.loadbalancer.roundrobin import RoundRobinLoadBalancer
from wool.runtime.typing import Factory
from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType
from wool.runtime.worker.auth import CredentialContext
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.connection import TransientRpcError
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.metadata import WorkerMetadata
from wool.utilities.noreentry import noreentry

if TYPE_CHECKING:
    from contextvars import Token

    from wool.runtime.routine.task import Task

T = TypeVar("T")


def parse_version(version: str) -> Version | None:
    """Parse a PEP 440 version string.

    :param version:
        A version string (e.g. ``"1.2.3"``).
    :returns:
        A :class:`~packaging.version.Version` instance, or
        ``None`` if unparseable.
    """
    try:
        return Version(version)
    except InvalidVersion:
        return None


def is_version_compatible(client: Version, server: Version) -> bool:
    """Check whether a client version is compatible with a server.

    A server accepts requests from any client whose protocol
    version is less than or equal to its own within the same major
    version.  There is no forward-compatibility guarantee.

    :param client:
        The client's protocol version.
    :param server:
        The server's protocol version.
    :returns:
        ``True`` if compatible, ``False`` otherwise.
    """
    return client.major == server.major and client <= server


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
            WorkerMetadata(address="10.0.0.1:50051", ...),
            WorkerMetadata(address="10.0.0.2:50051", ...),
        ]
        async with WorkerProxy(workers=workers) as proxy:
            result = await task()

    **Custom load balancer:**

    .. code-block:: python

        class StickyBalancer:
            async def delegate(self, *, context):
                # Yield a preferred worker first, then fall back.
                for metadata, connection in context.workers.items():
                    try:
                        sent = yield metadata, connection
                    except Exception:
                        continue
                    if sent is not None:
                        return


        async with WorkerProxy(
            discovery=discovery,
            loadbalancer=StickyBalancer(),
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
    :param lease:
        Maximum number of workers this proxy will admit from discovery.
        Defaults to ``None`` (unbounded).

    .. caution::

       Pre-called context manager instances passed as ``loadbalancer``
       or ``discovery`` are not picklable and will cause nested routine
       dispatch to fail.  Pass a callable returning the context manager
       instead.  See :data:`Factory`.
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
    _credentials: WorkerCredentials | None

    @overload
    def __init__(
        self,
        *,
        discovery: DiscoverySubscriberLike | Factory[DiscoverySubscriberLike],
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | None | UndefinedType = Undefined,
        lease: int | None = None,
        lazy: bool = True,
    ): ...

    @overload
    def __init__(
        self,
        *,
        workers: Sequence[WorkerMetadata],
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | None | UndefinedType = Undefined,
        lease: int | None = None,
        lazy: bool = True,
    ): ...

    @overload
    def __init__(
        self,
        pool_uri: str,
        *tags: str,
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | None | UndefinedType = Undefined,
        lease: int | None = None,
        lazy: bool = True,
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
        credentials: WorkerCredentials | None | UndefinedType = Undefined,
        lease: int | None = None,
        lazy: bool = True,
    ):
        if not (pool_uri or discovery or workers):
            raise ValueError(
                "Must specify either a workerpool URI, discovery event stream, or a "
                "sequence of workers"
            )

        if lease is not None and lease < 1:
            raise ValueError("Lease must be a positive, non-zero integer")

        self._id: uuid.UUID = uuid.uuid4()
        self._started = False
        self._lazy = lazy
        self._start_lock = asyncio.Lock() if lazy else None
        self._loadbalancer = loadbalancer
        self._lease = lease
        self._proxy_token: Token[WorkerProxy | None] | None = None

        if isinstance(loadbalancer, (ContextManager, AsyncContextManager)):
            warnings.warn(
                "Passing a context manager instance as 'loadbalancer' is "
                "not picklable and will fail during nested routine "
                "dispatch. Wrap it in a callable instead "
                "(e.g., loadbalancer=my_cm instead of "
                "loadbalancer=my_cm()).",
                UserWarning,
                stacklevel=2,
            )
        if isinstance(discovery, (ContextManager, AsyncContextManager)):
            warnings.warn(
                "Passing a context manager instance as 'discovery' is "
                "not picklable and will fail during nested routine "
                "dispatch. Wrap it in a callable instead "
                "(e.g., discovery=my_cm instead of discovery=my_cm()).",
                UserWarning,
                stacklevel=2,
            )

        if credentials is Undefined:
            self._credentials = CredentialContext.current()
        else:
            self._credentials = credentials

        # Create security filter based on resolved credentials
        security_filter = self._create_security_filter(self._credentials)
        version_filter = self._create_version_filter()

        def compatible(w):
            return security_filter(w) and version_filter(w)

        match (pool_uri, discovery, workers):
            case (pool_uri, None, None) if pool_uri is not None:
                # Combine tag and compatibility filters
                def combined_filter(w):
                    return bool({pool_uri, *tags} & w.tags) and compatible(w)

                self._discovery = LocalDiscovery(pool_uri).subscribe(
                    filter=combined_filter
                )
            case (None, discovery, None) if discovery is not None:
                self._discovery = discovery
            case (None, None, workers) if workers is not None:
                compatible_workers = [w for w in workers if compatible(w)]
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
        """Enters the proxy context and sets it as the active proxy."""
        await self.enter()
        return self

    async def __aexit__(self, *args):
        """Exits the proxy context and resets the active proxy."""
        await self.exit(*args)

    def __hash__(self) -> int:
        return hash(str(self.id))

    def __eq__(self, value: object) -> bool:
        return isinstance(value, WorkerProxy) and hash(self) == hash(value)

    def __reduce__(self) -> tuple:
        """Return constructor args for unpickling with proxy ID preserved.

        Creates a new WorkerProxy instance with the same discovery stream
        and load balancer type, then sets the preserved proxy ID on the
        new object.  Credentials are NOT serialized — the restored proxy
        resolves them from the active credential context.

        :returns:
            Tuple of (callable, args) for unpickling.
        :raises TypeError:
            If ``loadbalancer`` or ``discovery`` is a context manager
            instance, which cannot be pickled.
        """
        for name, value in (
            ("loadbalancer", self._loadbalancer),
            ("discovery", self._discovery),
        ):
            if isinstance(value, (ContextManager, AsyncContextManager)):
                raise TypeError(
                    f"Cannot pickle WorkerProxy: the '{name}' parameter "
                    f"is a context manager instance ({type(value).__name__}), "
                    f"which is not picklable. Wrap it in a callable "
                    f"instead (e.g., {name}=my_cm instead of "
                    f"{name}=my_cm())."
                )

        def _restore_proxy(discovery, loadbalancer, proxy_id, lease, lazy):
            proxy = WorkerProxy(
                discovery=discovery,
                loadbalancer=loadbalancer,
                lease=lease,
                lazy=lazy,
            )
            proxy._id = proxy_id
            return proxy

        return (
            _restore_proxy,
            (
                self._discovery,
                self._loadbalancer,
                self._id,
                self._lease,
                self._lazy,
            ),
        )

    @property
    def id(self) -> uuid.UUID:
        return self._id

    @property
    def started(self) -> bool:
        return self._started

    @property
    def lazy(self) -> bool:
        return self._lazy

    @property
    def workers(self) -> list[WorkerMetadata]:
        """A list of the currently discovered worker gRPC stubs."""
        if self._loadbalancer_context:
            return list(self._loadbalancer_context.workers.keys())
        else:
            return []

    @noreentry
    async def enter(self) -> None:
        """Enter the proxy context.

        Sets this proxy as the active context variable.  When
        ``lazy=True``, defers resource acquisition until
        :meth:`dispatch` is first called.  When ``lazy=False``,
        calls :meth:`start` eagerly.

        :raises RuntimeError:
            If the proxy has already been entered.  ``WorkerProxy``
            contexts are single-use — create a new instance instead
            of re-entering.
        :raises RuntimeError:
            If the proxy has already been started and ``lazy`` is
            ``False``.
        """
        self._proxy_token = wool.__proxy__.set(self)
        if self._lazy:
            return
        await self.start()

    async def start(self) -> None:
        """Start the proxy by initiating discovery and load balancing.

        Subscribes to worker discovery, initializes the load-balancer
        context, and launches the worker sentinel task.

        :raises RuntimeError:
            If the proxy has already been started.
        """
        if self._started:
            raise RuntimeError("Proxy already started")

        (
            self._loadbalancer_service,
            self._loadbalancer_context_manager,
        ) = await self._enter_context(self._loadbalancer)
        if not isinstance(
            self._loadbalancer_service,
            (DelegatingLoadBalancerLike, DispatchingLoadBalancerLike),
        ):
            raise ValueError
        if not isinstance(
            self._loadbalancer_service, DelegatingLoadBalancerLike
        ) and isinstance(
            self._loadbalancer_service, DispatchingLoadBalancerLike
        ):
            warnings.warn(
                "DispatchingLoadBalancerLike is deprecated; implement "
                "DelegatingLoadBalancerLike instead. Support for the "
                "dispatch-based protocol will be removed in the next "
                "major release. See issue #155.",
                DeprecationWarning,
                stacklevel=2,
            )

        (
            self._discovery_stream,
            self._discovery_context_manager,
        ) = await self._enter_context(self._discovery)
        if not isinstance(self._discovery_stream, DiscoverySubscriberLike):
            raise ValueError

        self._loadbalancer_context = LoadBalancerContext()
        self._sentinel_task = asyncio.create_task(self._worker_sentinel())
        self._started = True

    async def exit(self, *args) -> None:
        """Exit the proxy context.

        Resets the context variable.  If the proxy was started,
        delegates to :meth:`stop` to release resources.  Calling
        ``exit()`` on an un-started lazy proxy is a safe no-op.

        :raises RuntimeError:
            If the proxy was not started first and ``lazy`` is
            ``False``.
        """
        if self._proxy_token is not None:
            wool.__proxy__.reset(self._proxy_token)
            self._proxy_token = None
        if not self._started:
            if not self._lazy:
                raise RuntimeError("Proxy not started - call start() first")
            return
        await self.stop(*args)

    async def stop(self, *args) -> None:
        """Stop the proxy, terminating discovery and clearing connections.

        :raises RuntimeError:
            If the proxy was not started first.
        """
        if not self._started:
            raise RuntimeError("Proxy not started - call start() first")

        await self._exit_context(self._discovery_context_manager, *args)
        await self._exit_context(self._loadbalancer_context_manager, *args)

        if self._sentinel_task:
            self._sentinel_task.cancel()
            try:
                await self._sentinel_task
            except asyncio.CancelledError:
                pass
            self._sentinel_task = None
        self._loadbalancer_context = None
        self._started = False

    async def dispatch(
        self, task: Task, *, timeout: float | None = None
    ) -> AsyncGenerator:
        """Dispatches a task to an available worker in the pool.

        Selects a worker through the configured load balancer and
        returns its result stream. For :py:class:`DelegatingLoadBalancerLike`
        balancers, the proxy owns the dispatch-retry-evict loop: it
        calls :py:meth:`WorkerConnection.dispatch` on each candidate,
        evicts workers on non-transient errors, and reports outcomes
        back to the balancer via ``asend``/``athrow``.  For the
        deprecated :py:class:`DispatchingLoadBalancerLike`, dispatch is
        delegated to the balancer's legacy ``dispatch`` method.

        When ``lazy=True``, the proxy is started automatically on first
        dispatch.

        :param task:
            The :class:`Task` object to be dispatched.
        :param timeout:
            Timeout in seconds for the per-worker dispatch attempt.
        :returns:
            An async generator streaming task results from the worker.
        :raises RuntimeError:
            If the proxy is not started and ``lazy`` is ``False``.
        :raises NoWorkersAvailable:
            If every candidate the balancer yields fails to dispatch.
        :raises asyncio.TimeoutError:
            If no worker becomes available within the wait window.
        """
        if not self._started:
            if not self._lazy:
                raise RuntimeError("Proxy not started - call start() first")
            assert self._start_lock is not None
            async with self._start_lock:
                if not self._started:
                    await self.start()

        await asyncio.wait_for(self._await_workers(), 60)

        assert self._loadbalancer_context is not None
        lb = self._loadbalancer_service
        if isinstance(lb, DelegatingLoadBalancerLike):
            return await self._delegate_dispatch(lb, task, timeout)
        assert isinstance(lb, DispatchingLoadBalancerLike)
        return await lb.dispatch(
            task, context=self._loadbalancer_context, timeout=timeout
        )

    async def _delegate_dispatch(
        self,
        lb: DelegatingLoadBalancerLike,
        task: Task,
        timeout: float | None,
    ) -> AsyncGenerator:
        """Drive a delegating load balancer through one dispatch.

        Pulls candidates from ``lb.delegate(context=...)``, calls
        :py:meth:`WorkerConnection.dispatch` on each, and reports the
        outcome back via ``asend`` on success or ``athrow`` on failure.
        Non-transient errors trigger eviction from the context before
        the balancer is notified, so it observes the capacity change
        when choosing the next candidate.

        Cancellation (``CancelledError``) and other ``BaseException``
        subclasses propagate through without eviction or retry —
        they are caller intent, not worker failures.

        :param lb:
            The delegating load balancer to drive.
        :param task:
            The task to dispatch.
        :param timeout:
            Per-attempt timeout forwarded to
            :py:meth:`WorkerConnection.dispatch`.
        :returns:
            The result stream from the first successful dispatch.
        :raises NoWorkersAvailable:
            If the balancer's generator ends without producing a
            successful candidate.
        :raises RuntimeError:
            If the balancer's generator yields another candidate
            after receiving a success signal via ``asend``.
        """
        assert self._loadbalancer_context is not None
        ctx = self._loadbalancer_context
        gen = lb.delegate(context=ctx)
        try:
            try:
                metadata, connection = await gen.__anext__()
            except StopAsyncIteration:
                raise NoWorkersAvailable(
                    "No healthy workers available for dispatch"
                )

            while True:
                try:
                    stream = await connection.dispatch(task, timeout=timeout)
                except TransientRpcError as exc:
                    try:
                        metadata, connection = await gen.athrow(exc)
                    except StopAsyncIteration:
                        raise NoWorkersAvailable(
                            "No healthy workers available for dispatch"
                        ) from exc
                    continue
                except Exception as exc:
                    # Non-transient worker error: evict, then ask the
                    # balancer for the next candidate. CancelledError,
                    # KeyboardInterrupt, and SystemExit inherit from
                    # BaseException directly and therefore do NOT land
                    # here — they propagate up to the outer finally,
                    # which cleans up the generator without eviction
                    # or retry.
                    ctx.remove_worker(metadata)
                    try:
                        metadata, connection = await gen.athrow(exc)
                    except StopAsyncIteration:
                        raise NoWorkersAvailable(
                            "No healthy workers available for dispatch"
                        ) from exc
                    continue

                # Success path: connection.dispatch() returned a live
                # stream. The proxy owns it until handed off via
                # `return`. A cancel or a contract-violation RuntimeError
                # between here and the return would orphan the gRPC
                # call, so we guard with try/finally and transfer
                # ownership explicitly by nulling out `stream` on
                # handoff.
                try:
                    try:
                        trailing = await gen.asend(metadata)
                    except StopAsyncIteration:
                        result, stream = stream, None
                        return result
                    raise RuntimeError(
                        f"DelegatingLoadBalancerLike.delegate yielded "
                        f"{trailing!r} after receiving a success signal "
                        f"via asend(); the generator MUST terminate "
                        f"after acknowledging success."
                    )
                finally:
                    if stream is not None:
                        # Did not successfully hand off the stream —
                        # close it so the underlying gRPC call is
                        # released. Covers both cancellation during
                        # asend and the contract-violation RuntimeError
                        # path.
                        await stream.aclose()
        finally:
            # Release any generator-scope resources the balancer holds.
            # Runs on every exit path: success, NoWorkersAvailable,
            # cancellation, and contract violations.
            await gen.aclose()

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
        self, credentials: WorkerCredentials | None
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

    @staticmethod
    def _create_version_filter() -> Callable[[WorkerMetadata], bool]:
        """Create discovery filter based on major version compatibility.

        A worker is accepted when its version is greater than or
        equal to the local proxy version within the same major
        version.  Workers with unparseable versions are rejected.

        :returns:
            Predicate function for filtering workers by version
            compatibility.
        """
        from wool import protocol

        local_version = parse_version(protocol.__version__)

        def version_filter(metadata: WorkerMetadata) -> bool:
            worker_version = parse_version(metadata.version)
            if local_version is None or worker_version is None:
                return False
            return is_version_compatible(local_version, worker_version)

        return version_filter

    async def _await_workers(self):
        while not self._loadbalancer_context or not self._loadbalancer_context.workers:
            await asyncio.sleep(0)

    async def _worker_sentinel(self):
        assert self._loadbalancer_context
        client_credentials = (
            self._credentials.client_credentials()
            if self._credentials is not None
            else None
        )
        async for event in self._discovery_stream:
            match event.type:
                case "worker-added" | "worker-updated":
                    if (
                        event.type == "worker-added"
                        and self._lease is not None
                        and len(self._loadbalancer_context.workers) >= self._lease
                    ):
                        continue
                    connection = WorkerConnection(
                        event.metadata.address,
                        credentials=client_credentials,
                        options=event.metadata.options,
                    )
                    if event.type == "worker-added":
                        self._loadbalancer_context.add_worker(event.metadata, connection)
                    elif event.metadata in self._loadbalancer_context.workers:
                        self._loadbalancer_context.update_worker(
                            event.metadata, connection
                        )
                case "worker-dropped":
                    self._loadbalancer_context.remove_worker(event.metadata)
