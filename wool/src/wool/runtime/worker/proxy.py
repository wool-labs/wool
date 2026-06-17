from __future__ import annotations

import asyncio
import uuid
import warnings
from contextlib import AsyncExitStack
from types import MappingProxyType
from typing import TYPE_CHECKING
from typing import Any
from typing import AsyncContextManager
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import ContextManager
from typing import Final
from typing import Generic
from typing import Mapping
from typing import NoReturn
from typing import Sequence
from typing import SupportsIndex
from typing import TypeAlias
from typing import TypeVar
from typing import overload

from packaging.version import InvalidVersion
from packaging.version import Version

import wool
from wool.exception import WoolWarning
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.base import HandshakeRejection
from wool.runtime.loadbalancer.base import LoadBalancerContext
from wool.runtime.loadbalancer.base import LoadBalancerLike
from wool.runtime.loadbalancer.roundrobin import RoundRobinLoadBalancer
from wool.runtime.typing import Factory
from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType
from wool.runtime.worker.auth import CredentialContext
from wool.runtime.worker.auth import CredentialProviderLike
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.auth import _coerce_provider
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


DEFAULT_QUORUM: Final[int] = 1
"""Default minimum worker count required before dispatch."""

DEFAULT_QUORUM_TIMEOUT: Final[float] = 60.0
"""Default seconds to wait for ``quorum`` workers before raising :class:`asyncio.TimeoutError`."""

DEFAULT_LAZY: Final[bool] = True
"""Default lazy-start behavior: defer discovery setup and the quorum wait to first dispatch."""


# public
class IneffectiveQuorumTimeoutWarning(WoolWarning):
    """Emitted when ``quorum_timeout`` is supplied alongside ``quorum=None`` or ``0``.

    The timeout value is recorded on the proxy but never consulted —
    no quorum wait runs when the gate is disabled.  Filter this category
    to ``"error"`` (via :func:`warnings.filterwarnings`) to enforce the
    previous strict behaviour and turn the warning back into a
    :class:`ValueError`.
    """


def _restore_proxy(
    discovery: DiscoverySubscriberLike,
    loadbalancer: Factory[LoadBalancerLike] | LoadBalancerLike,
    proxy_id: uuid.UUID,
    lease: int | None,
    lazy: bool,
    quorum: int | None = DEFAULT_QUORUM,
    quorum_timeout: float | None = DEFAULT_QUORUM_TIMEOUT,
) -> WorkerProxy:
    """Reconstruct a :class:`WorkerProxy` from its reduce tuple.

    Module-level so the reduce tuple references a stable callable rather
    than a freshly created closure on every reduction.  New parameters
    are appended with defaults so that older clients whose reduce
    tuples predate them still unpickle on newer peers within the same
    major protocol version.

    :param discovery:
        Discovery subscriber or factory carried through the reduce tuple.
    :param loadbalancer:
        Load balancer instance or factory carried through the reduce tuple.
    :param proxy_id:
        Original proxy id, restored after construction so the receiving
        side observes the same identity.
    :param lease:
        Lease size, ``None`` for no cap.
    :param lazy:
        Whether to defer worker resolution until first dispatch.
    :param quorum:
        Minimum number of workers required before the proxy is ready.
        Defaults to ``1`` rather than the constructor's ``None`` default,
        so reduce tuples emitted by clients that predate this parameter
        preserve their pre-quorum "wait for any worker" semantics.
    :param quorum_timeout:
        Seconds to wait for ``quorum`` workers before raising
        :class:`asyncio.TimeoutError`.  Recorded but never consulted
        when ``quorum`` is ``None`` or ``0``.  Defaults to ``60`` for
        back-compat with reduce tuples emitted by clients that
        predate this parameter.
    :returns:
        A reconstructed :class:`WorkerProxy` with the original id.
    """
    if quorum:
        proxy = WorkerProxy(
            discovery=discovery,
            loadbalancer=loadbalancer,
            lease=lease,
            quorum=quorum,
            quorum_timeout=quorum_timeout,
            lazy=lazy,
        )
    else:
        proxy = WorkerProxy(
            discovery=discovery,
            loadbalancer=loadbalancer,
            lease=lease,
            quorum=None,
            lazy=lazy,
        )
    proxy._id = proxy_id
    return proxy


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

    **Quorum gate:**

    .. code-block:: python

        # Block first dispatch until at least 3 workers are admitted,
        # giving up after 30 seconds if the quorum is not reached.
        async with WorkerProxy(
            discovery=discovery,
            quorum=3,
            quorum_timeout=30,
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
        Optional credentials for TLS/mTLS connections to workers — either a
        :class:`~wool.WorkerCredentials` or a
        :class:`~wool.CredentialProviderLike` (e.g. from
        :meth:`WorkerCredentials.provider_from_files` for identity-based
        verification or credential rotation). A bare ``WorkerCredentials``
        is wrapped in a static provider. Defaults to resolving from the
        ambient credential context.
    :param lease:
        Maximum number of workers this proxy will admit from discovery.
        Defaults to ``None`` (unbounded).
    :param quorum:
        Minimum number of workers that must be discovered before the proxy
        considers itself ready. Defaults to ``1`` — block until at least
        one worker is admitted, preserving the pre-quorum implicit-wait
        behaviour. Pass a larger integer to require more workers, or
        ``None``/``0`` to disable the gate entirely (``dispatch`` may
        then raise immediately if no workers have been discovered yet).
        When ``lazy=True`` (default), the quorum wait is deferred to the
        first dispatch; with ``lazy=False`` it blocks at context entry.
    :param quorum_timeout:
        Seconds to wait for ``quorum`` workers to be discovered before
        raising :class:`asyncio.TimeoutError`. Only meaningful when
        ``quorum`` is a positive integer; supplying it with
        ``quorum=None`` or ``quorum=0`` records the value but never
        consults it, accompanied by an
        :class:`IneffectiveQuorumTimeoutWarning`. Defaults to ``60``;
        pass ``None`` to wait indefinitely.

    :raises ValueError:
        If ``quorum`` is negative, ``lease`` is non-positive, ``quorum``
        exceeds ``lease``, ``quorum`` exceeds the compatible worker
        count when constructed with a static ``workers`` list, or
        ``quorum_timeout`` is non-positive or supplied without a
        positive ``quorum``.
    :raises asyncio.TimeoutError:
        Raised at context entry (``lazy=False``) or first
        :meth:`dispatch` (``lazy=True``) if ``quorum`` workers are not
        admitted within ``quorum_timeout`` seconds.

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
    _provider: CredentialProviderLike | None

    @overload
    def __init__(
        self,
        *,
        discovery: DiscoverySubscriberLike | Factory[DiscoverySubscriberLike],
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: (
            WorkerCredentials | CredentialProviderLike | None | UndefinedType
        ) = Undefined,
        lease: int | None = None,
        quorum: int | None = DEFAULT_QUORUM,
        quorum_timeout: float | None = DEFAULT_QUORUM_TIMEOUT,
        lazy: bool = DEFAULT_LAZY,
    ): ...

    @overload
    def __init__(
        self,
        *,
        workers: Sequence[WorkerMetadata],
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: (
            WorkerCredentials | CredentialProviderLike | None | UndefinedType
        ) = Undefined,
        lease: int | None = None,
        quorum: int | None = DEFAULT_QUORUM,
        quorum_timeout: float | None = DEFAULT_QUORUM_TIMEOUT,
        lazy: bool = DEFAULT_LAZY,
    ): ...

    @overload
    def __init__(
        self,
        pool_uri: str,
        *tags: str,
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: (
            WorkerCredentials | CredentialProviderLike | None | UndefinedType
        ) = Undefined,
        lease: int | None = None,
        quorum: int | None = DEFAULT_QUORUM,
        quorum_timeout: float | None = DEFAULT_QUORUM_TIMEOUT,
        lazy: bool = DEFAULT_LAZY,
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
        credentials: (
            WorkerCredentials | CredentialProviderLike | None | UndefinedType
        ) = Undefined,
        lease: int | None = None,
        quorum: int | None = DEFAULT_QUORUM,
        quorum_timeout: float | None | UndefinedType = Undefined,
        lazy: bool = DEFAULT_LAZY,
    ):
        if not (pool_uri or discovery or workers):
            raise ValueError(
                "Must specify either a workerpool URI, discovery event stream, or a "
                "sequence of workers"
            )

        if quorum is not None and quorum < 0:
            raise ValueError("Quorum must be a non-negative integer")

        if lease is not None and lease < 1:
            raise ValueError("Lease must be a positive, non-zero integer")

        if quorum is not None and lease is not None and quorum > lease:
            raise ValueError(
                f"Quorum ({quorum}) cannot exceed lease ({lease}) — "
                "the quorum would never be satisfied"
            )

        if quorum_timeout is Undefined:
            quorum_timeout = DEFAULT_QUORUM_TIMEOUT
        elif not quorum:
            warnings.warn(
                "'quorum_timeout' has no effect when 'quorum' is None or 0; "
                "the value is recorded but never consulted",
                IneffectiveQuorumTimeoutWarning,
                stacklevel=2,
            )

        if quorum_timeout is not None and quorum_timeout <= 0:
            raise ValueError("Quorum timeout must be positive")

        self._id: uuid.UUID = uuid.uuid4()
        self._started = False
        self._lazy = lazy
        self._start_lock = asyncio.Lock() if lazy else None
        self._loadbalancer = loadbalancer
        self._lease = lease
        self._quorum = quorum
        self._quorum_timeout = quorum_timeout
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
            resolved = CredentialContext.current()
        else:
            resolved = credentials
        # Normalize either a bare WorkerCredentials or a provider (from the
        # argument or the ambient credential context) into a provider the
        # sentinel resolves per connection.
        self._provider = _coerce_provider(resolved)

        # Create security filter based on resolved credentials
        security_filter = self._create_security_filter(self._provider)
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
                if quorum is not None and quorum > len(compatible_workers):
                    raise ValueError(
                        f"Quorum ({quorum}) cannot exceed compatible worker "
                        f"count ({len(compatible_workers)} of {len(workers)} "
                        "after security/version filtering) — "
                        "the quorum would never be satisfied"
                    )
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

    def __wool_reduce__(self) -> tuple[Callable[..., WorkerProxy], tuple[Any, ...]]:
        """Return constructor args for unpickling via Wool's pickler.

        The reduce tuple carries ``discovery``, ``loadbalancer``, the
        proxy id, ``lease``, ``lazy``, ``quorum``, and
        ``quorum_timeout``.  Credentials are NOT serialized — the
        restored proxy resolves them from the active credential
        context.

        WorkerProxy is guarded against vanilla pickling (see
        :meth:`__reduce_ex__`); this method is invoked only by Wool's own
        pickler.

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

        return (
            _restore_proxy,
            (
                self._discovery,
                self._loadbalancer,
                self._id,
                self._lease,
                self._lazy,
                self._quorum,
                self._quorum_timeout,
            ),
        )

    def __reduce_ex__(self, _protocol: SupportsIndex) -> NoReturn:
        """Reject vanilla pickling.

        WorkerProxy carries credentials that are resolved from the active
        credential context at construction and intentionally not
        transported across process boundaries.  Allowing vanilla
        :func:`pickle.dumps` or :func:`cloudpickle.dumps` would silently
        produce payloads that deserialize into nonsense outside the
        dispatch path.  Wool's own pickler consults ``reducer_override``
        (and therefore ``__wool_reduce__``) before ``__reduce_ex__``, so
        this guard is invisible to Wool's own serialization.

        :func:`copy.copy` and :func:`copy.deepcopy` also route through
        ``__reduce_ex__`` and are rejected for the same reason — a
        runtime-bound proxy has no meaningful copy semantics.

        :raises TypeError:
            Always.
        """
        raise TypeError(
            "WorkerProxy cannot be pickled via vanilla pickle/cloudpickle; "
            "it is serialized automatically when dispatched through Wool's "
            "runtime."
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
    def quorum(self) -> int | None:
        """Configured minimum worker count, or ``None``/``0`` for no gate."""
        return self._quorum

    @property
    def quorum_timeout(self) -> float | None:
        """Seconds to wait for ``quorum`` workers; ``None`` waits indefinitely."""
        return self._quorum_timeout

    @property
    def workers(self) -> list[WorkerMetadata]:
        """A list of the currently discovered worker gRPC stubs."""
        if self._loadbalancer_context:
            return list(self._loadbalancer_context.workers.keys())
        else:
            return []

    @property
    def rejections(self) -> Mapping[uuid.UUID, HandshakeRejection]:
        """Per-worker secure-handshake rejection ledger.

        Surfaces how many discovered workers refused, or were refused by,
        the client's credentials, and why — so a fleet-wide mTLS
        misconfiguration is distinguishable from an empty pool. Empty when
        the proxy is not started.

        :returns:
            Immutable mapping of worker uid to its
            :class:`HandshakeRejection` record.
        """
        if self._loadbalancer_context:
            return self._loadbalancer_context.rejections
        return MappingProxyType({})

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
        try:
            await self.start()
        except BaseException:
            wool.__proxy__.reset(self._proxy_token)
            self._proxy_token = None
            raise

    async def start(self) -> None:
        """Start the proxy by initiating discovery and load balancing.

        Subscribes to worker discovery, initializes the load-balancer
        context, and launches the worker sentinel task.  Acquired
        resources are unwound in reverse order if any setup step
        (including the quorum wait) raises.

        :raises RuntimeError:
            If the proxy has already been started.
        :raises asyncio.TimeoutError:
            If the quorum wait does not complete within
            ``quorum_timeout``.
        """
        if self._started:
            raise RuntimeError("Proxy already started")

        async with AsyncExitStack() as stack:
            (
                self._loadbalancer_service,
                self._loadbalancer_context_manager,
            ) = await self._enter_context(self._loadbalancer)
            if not isinstance(self._loadbalancer_service, LoadBalancerLike):
                raise ValueError
            stack.push_async_callback(
                self._exit_context,
                self._loadbalancer_context_manager,
                None,
                None,
                None,
            )

            (
                self._discovery_stream,
                self._discovery_context_manager,
            ) = await self._enter_context(self._discovery)
            if not isinstance(self._discovery_stream, DiscoverySubscriberLike):
                raise ValueError
            stack.push_async_callback(
                self._exit_context,
                self._discovery_context_manager,
                None,
                None,
                None,
            )

            self._loadbalancer_context = LoadBalancerContext()
            self._workers_changed = asyncio.Event()
            self._sentinel_task = asyncio.create_task(self._worker_sentinel())
            stack.push_async_callback(self._teardown_sentinel)

            if self._quorum:
                await asyncio.wait_for(self._await_workers(), self._quorum_timeout)

            stack.pop_all()

        self._started = True

    async def _teardown_sentinel(self) -> None:
        """Cancel the sentinel task and null all partial-init attributes.

        Idempotent rollback callback used by :meth:`start`'s
        :class:`AsyncExitStack` and reused by :meth:`stop`.  Cancels
        ``_sentinel_task`` (if any), awaits its termination swallowing
        ``CancelledError``, and resets every attribute populated by
        :meth:`start` to ``None`` so a failed start leaves no stale
        references.
        """
        if self._sentinel_task:
            self._sentinel_task.cancel()
            try:
                await self._sentinel_task
            except asyncio.CancelledError:
                pass
            self._sentinel_task = None
        self._loadbalancer_context = None
        self._loadbalancer_service = None
        self._loadbalancer_context_manager = None
        self._discovery_stream = None
        self._discovery_context_manager = None
        self._workers_changed = None

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

        Teardown runs in reverse-acquisition order: sentinel first
        (so it stops reading from the discovery stream), then
        discovery, then load balancer.  All three are guaranteed to
        run via :class:`AsyncExitStack` even if an earlier teardown
        raises.

        :raises RuntimeError:
            If the proxy was not started first.
        """
        if not self._started:
            raise RuntimeError("Proxy not started - call start() first")

        async with AsyncExitStack() as stack:
            stack.push_async_callback(
                self._exit_context,
                self._loadbalancer_context_manager,
                *args,
            )
            stack.push_async_callback(
                self._exit_context,
                self._discovery_context_manager,
                *args,
            )
            stack.push_async_callback(self._teardown_sentinel)
        self._started = False

    async def dispatch(
        self, task: Task, *, timeout: float | None = None
    ) -> AsyncGenerator:
        """Dispatch a task to an available worker.

        Selects a worker via the configured load balancer.  When
        ``lazy=True``, the proxy is started on first dispatch — which
        runs the quorum wait.  Subsequent dispatches do not re-check
        quorum; the load balancer surfaces "no workers available"
        directly if the worker set later drains.

        A failed first dispatch (e.g., quorum timeout) leaves the
        proxy un-started: the next dispatch re-runs ``start()`` and
        retries, re-paying the quorum wait.  This lets a pool recover
        once a worker is admitted, without constructing a new proxy.

        :param task:
            The :class:`Task` to dispatch.
        :param timeout:
            Timeout in seconds for getting a worker.
        :returns:
            An async generator yielding result objects from the
            worker.
        :raises RuntimeError:
            If the proxy is not started and ``lazy`` is ``False``.
        :raises asyncio.TimeoutError:
            If no worker is available within ``timeout``, or if the
            quorum wait fires during a lazy first dispatch.
        """
        if not self._started:
            if not self._lazy:
                raise RuntimeError("Proxy not started - call start() first")
            assert self._start_lock is not None
            async with self._start_lock:
                if not self._started:
                    await self.start()

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
        self, provider: CredentialProviderLike | None
    ) -> Callable[[WorkerMetadata], bool]:
        """Create discovery filter based on proxy credentials.

        Workers and proxies must have compatible security settings:
        - Proxy with credentials only discovers workers with secure=True
        - Proxy without credentials only discovers workers with secure=False

        :param provider:
            Credential provider for this proxy, or ``None``.
        :returns:
            Predicate function for filtering workers by security compatibility.
        """
        if provider is not None:
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
        """Block until at least ``quorum`` workers are admitted.

        Waits on the workers-changed event set by
        :meth:`_worker_sentinel` after each add or drop, re-checking
        the worker count after each wakeup.  The caller is expected
        to gate this call on ``self._quorum`` being a positive
        integer; calling with ``quorum`` of ``None`` or ``0`` will
        loop forever.
        """
        assert self._quorum is not None and self._quorum > 0
        assert self._workers_changed is not None
        while True:
            if (
                self._loadbalancer_context
                and len(self._loadbalancer_context.workers) >= self._quorum
            ):
                return
            await self._workers_changed.wait()
            self._workers_changed.clear()

    async def _worker_sentinel(self):
        assert self._loadbalancer_context is not None
        assert self._discovery_stream is not None
        assert self._workers_changed is not None
        provider = self._provider
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
                        provider=provider,
                        options=event.metadata.options,
                    )
                    if event.type == "worker-added":
                        self._loadbalancer_context.add_worker(event.metadata, connection)
                        self._workers_changed.set()
                    elif event.metadata in self._loadbalancer_context.workers:
                        self._loadbalancer_context.update_worker(
                            event.metadata, connection
                        )
                case "worker-dropped":
                    self._loadbalancer_context.remove_worker(event.metadata)
                    self._workers_changed.set()
