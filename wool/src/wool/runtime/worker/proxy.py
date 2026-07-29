from __future__ import annotations

import asyncio
import logging
import uuid
import warnings
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING
from typing import Any
from typing import AsyncContextManager
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import ClassVar
from typing import ContextManager
from typing import Final
from typing import Generic
from typing import NoReturn
from typing import Sequence
from typing import SupportsIndex
from typing import TypeAlias
from typing import TypeVar
from typing import cast
from typing import overload

from packaging.version import InvalidVersion
from packaging.version import Version

import wool
from wool.exceptions import WoolWarning
from wool.runtime.context.var import ContextVar
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.base import DISPATCHING_LOADBALANCER_DEPRECATION_MESSAGE
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
from wool.runtime.worker.connection import RpcError
from wool.runtime.worker.connection import TransientRpcError
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.exceptions import UnparsableVersionWarning
from wool.runtime.worker.metadata import WorkerMetadata
from wool.utilities.noreentry import noreentry

if TYPE_CHECKING:
    from contextvars import Token

    from wool.runtime.routine.task import Task

_logger = logging.getLogger(__name__)

T = TypeVar("T")


def parse_version(version: str) -> Version | None:
    """Parse a PEP 440 version string.

    :param version:
        A version string (e.g. ``"1.2.3"``).
    :returns:
        A :class:`~packaging.version.Version` instance, or
        ``None`` if unparsable.
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
    routing. The bridge between `wool.routine`-decorated functions and
    the worker pool.

    Connects to workers through discovery services, pool URIs, or static
    worker lists. Handles connection lifecycle and fault tolerance
    automatically.

    Every worker on every construction path — discovery stream, pool
    URI, or static list — passes a security/version admission gate
    before joining the pool: the worker's ``secure`` flag must match
    the proxy's credentials, and its version must share the local
    protocol version's major and be at least the local version.
    Workers surfaced by a user-supplied ``discovery`` subscriber are
    subject to the same gate — they are not trusted verbatim. The gate
    is a non-overridable invariant rather than tunable policy. It fails
    closed — a worker whose advertised version does not parse is
    rejected, and if the local protocol version is unresolvable no
    worker is admitted at all (observable as the quorum
    `asyncio.TimeoutError`). The gate is re-derived from the credential
    context and protocol version of whichever process holds the proxy,
    so a static-worker proxy re-gated after serialization may admit
    fewer workers than at construction, and its construction-time
    quorum check (see ``:raises ValueError:``) does not carry across a
    pickle.

    .. rubric:: Implementation notes

    The gate is non-overridable because the version floor is a
    wire-protocol guarantee the worker interceptor also enforces
    server-side, and an uncredentialed proxy cannot complete a TLS
    handshake with a ``secure`` worker.

    .. versionchanged:: 0.12.0
       Workers from every discovery path, including a user-supplied
       ``discovery`` subscriber, are now screened by the admission gate;
       previously such workers were admitted unconditionally.

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
            async def delegate(self, task, *, context):
                # Yield a preferred worker first, then fall back.
                for uid in context.workers:
                    try:
                        sent = yield uid
                    except Exception:
                        continue
                    if sent is not None:
                        return


        async with WorkerProxy(
            discovery=discovery,
            loadbalancer=StickyBalancer(),
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
        Discovery service or event stream. Workers it surfaces are
        admitted only after passing the admission gate described above.
    :param workers:
        Static worker list for direct connection.
    :param loadbalancer:
        Load balancer instance, factory, or context manager.
    :param credentials:
        Optional channel credentials for TLS/mTLS connections to workers.
    :param lease:
        Maximum number of workers this proxy will admit from discovery.
        Defaults to ``None`` (unbounded).  The cap counts distinct
        worker uids, so a worker re-announcing itself refreshes its
        entry without consuming a slot; only a worker the proxy has
        not admitted can be turned away.
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
        raising `asyncio.TimeoutError`. Only meaningful when
        ``quorum`` is a positive integer; supplying it with
        ``quorum=None`` or ``quorum=0`` records the value but never
        consults it, accompanied by an
        `IneffectiveQuorumTimeoutWarning`. Defaults to ``60``;
        pass ``None`` to wait indefinitely.

    :raises ValueError:
        If ``quorum`` is negative, ``lease`` is non-positive, ``quorum``
        exceeds ``lease``, ``quorum`` exceeds the compatible worker
        count when constructed with a static ``workers`` list, or
        ``quorum_timeout`` is non-positive or supplied without a
        positive ``quorum``.
    :raises asyncio.TimeoutError:
        Raised at context entry (``lazy=False``) or first
        `dispatch` (``lazy=True``) if ``quorum`` workers are not
        admitted within ``quorum_timeout`` seconds.

    .. caution::

       Pre-called context manager instances passed as ``loadbalancer``
       or ``discovery`` are not picklable and will cause nested routine
       dispatch to fail.  Pass a callable returning the context manager
       instead.  See `Factory`.
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
    _security_filter: Callable[[WorkerMetadata], bool]
    _version_filter: Callable[[WorkerMetadata], bool]

    # ``wool.__proxy__`` is a plain ``contextvars.ContextVar``, invisible to
    # the chain-contention guard; this ``wool.ContextVar`` is the guarded
    # marker whose arming brings proxy entry under the guard. The value is an
    # inert arming sentinel — only ``set`` is consulted (it arms the chain),
    # so the ``bool`` payload and ``default`` are never read. ``__aenter__``
    # owns the contention contract.
    _armed: ClassVar[ContextVar[bool]] = ContextVar(
        "_armed", namespace="wool", default=False
    )

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
        credentials: WorkerCredentials | None | UndefinedType = Undefined,
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
        credentials: WorkerCredentials | None | UndefinedType = Undefined,
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
        credentials: WorkerCredentials | None | UndefinedType = Undefined,
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
        self._dispatching_deprecation_warned = False
        self._delegating = False
        self._lazy = lazy
        self._start_lock = asyncio.Lock() if lazy else None
        self._loadbalancer = loadbalancer
        self._lease = lease
        self._quorum = quorum
        self._quorum_timeout = quorum_timeout
        self._proxy_token: Token[WorkerProxy | None] | None = None
        self._exit_stack: AsyncExitStack | None = None

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

        # Warn here, where the caller's construction frame is reachable, when a
        # balancer that implements *only* the deprecated dispatch protocol is
        # passed as an instance. A dual-protocol instance takes the delegate
        # path and is not warned; a factory only resolves to an instance in
        # start(), which warns as a fallback.
        if isinstance(loadbalancer, DispatchingLoadBalancerLike) and not isinstance(
            loadbalancer, LoadBalancerLike
        ):
            warnings.warn(
                DISPATCHING_LOADBALANCER_DEPRECATION_MESSAGE,
                DeprecationWarning,
                stacklevel=2,
            )
            self._dispatching_deprecation_warned = True

        if credentials is Undefined:
            self._credentials = CredentialContext.current()
        else:
            self._credentials = credentials

        # Build both halves of the admission gate from the resolved
        # credentials and the local protocol version. Deliberately not
        # serialized: __wool_reduce__ restores the proxy through __init__,
        # so a restored proxy rebuilds the gate from its own credential
        # context and protocol version.
        self._security_filter = self._create_security_filter(self._credentials)
        self._version_filter = self._create_version_filter()

        match (pool_uri, discovery, workers):
            case (pool_uri, None, None) if pool_uri is not None:
                # Tag semantics only — compatibility is enforced at
                # sentinel admission.
                match_tags = frozenset({pool_uri, *tags})

                def tag_filter(w):
                    return bool(match_tags & w.tags)

                self._discovery = LocalDiscovery(pool_uri).subscribe(filter=tag_filter)
            case (None, discovery, None) if discovery is not None:
                self._discovery = discovery
            case (None, None, workers) if workers is not None:
                compatible_workers = [
                    w for w in workers if self._incompatibility_reason(w) is None
                ]
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
        """Enter the proxy context and set it as the active proxy.

        Arm `_armed` *before* binding the proxy, so a second task
        entering a proxy in a `contextvars.Context` it does not own
        raises `wool.ChainContention` at this point rather than silently
        clobbering `wool.__proxy__`. The arming and its reset are
        registered on an `~contextlib.AsyncExitStack` (the same idiom
        `start` uses) so the marker is unwound if `enter` raises and
        reset on `__aexit__` — never leaked.
        """
        async with AsyncExitStack() as stack:
            token = self._armed.set(True)
            stack.callback(self._armed.reset, token)
            await self.enter()
            self._exit_stack = stack.pop_all()
        return self

    async def __aexit__(self, *args):
        """Exit the proxy context and reset the active proxy."""
        try:
            await self.exit(*args)
        finally:
            if self._exit_stack is not None:
                await self._exit_stack.aclose()
                self._exit_stack = None

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
        """Return the current metadata record of every discovered worker.

        A worker's record is replaced in place when discovery
        republishes it, so correlate entries across calls by
        `WorkerMetadata.uid` rather than by holding a record.
        """
        if self._loadbalancer_context:
            return [
                metadata for metadata, _ in self._loadbalancer_context.workers.values()
            ]
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
            if not isinstance(
                self._loadbalancer_service,
                (LoadBalancerLike, DispatchingLoadBalancerLike),
            ):
                raise ValueError
            # Classify the balancer once, here, so dispatch() need not
            # re-run a @runtime_checkable isinstance on the hot path.
            self._delegating = isinstance(self._loadbalancer_service, LoadBalancerLike)
            if (
                not self._delegating
                and isinstance(self._loadbalancer_service, DispatchingLoadBalancerLike)
                and not self._dispatching_deprecation_warned
            ):
                warnings.warn(
                    DISPATCHING_LOADBALANCER_DEPRECATION_MESSAGE,
                    DeprecationWarning,
                    stacklevel=2,
                )
                self._dispatching_deprecation_warned = True
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

        Selects a worker through the configured load balancer and
        returns its result stream. For `LoadBalancerLike`
        balancers, the proxy owns the dispatch-retry-evict loop: it
        calls `WorkerConnection.dispatch` on each candidate,
        evicts workers on non-transient errors, and reports outcomes
        back to the balancer via ``asend``/``athrow``.  For the
        deprecated `DispatchingLoadBalancerLike`, dispatch is
        delegated to the balancer's legacy ``dispatch`` method.

        When ``lazy=True``, the proxy is started on first dispatch —
        which runs the quorum wait.  Subsequent dispatches do not
        re-check quorum; the load balancer surfaces "no workers
        available" directly if the worker set later drains.

        A failed first dispatch (e.g., quorum timeout) leaves the
        proxy un-started: the next dispatch re-runs ``start()`` and
        retries, re-paying the quorum wait.  This lets a pool recover
        once a worker is admitted, without constructing a new proxy.

        :param task:
            The `Task` to dispatch.
        :param timeout:
            Timeout in seconds for the per-worker dispatch attempt.
        :returns:
            An async generator streaming task results from the worker.
        :raises RuntimeError:
            If the proxy is not started and ``lazy`` is ``False``.
        :raises NoWorkersAvailable:
            If every candidate the balancer yields fails to dispatch.
        :raises asyncio.TimeoutError:
            If the quorum wait fires during a lazy first dispatch. An
            exceeded per-attempt ``timeout`` does not surface here; the
            worker connection reports it as an `RpcError` that the
            dispatch loop handles.
        """
        if not self._started:
            if not self._lazy:
                raise RuntimeError("Proxy not started - call start() first")
            assert self._start_lock is not None
            async with self._start_lock:
                if not self._started:
                    await self.start()

        assert self._loadbalancer_context is not None
        # Balancer kind was resolved in start(); branch on the cached flag
        # rather than a per-dispatch structural isinstance.
        if self._delegating:
            delegating = cast(LoadBalancerLike, self._loadbalancer_service)
            return await self._delegate_dispatch(delegating, task, timeout)
        dispatching = cast(DispatchingLoadBalancerLike, self._loadbalancer_service)
        return await dispatching.dispatch(
            task, context=self._loadbalancer_context, timeout=timeout
        )

    async def _delegate_dispatch(
        self,
        loadbalancer: LoadBalancerLike,
        task: Task,
        timeout: float | None,
    ) -> AsyncGenerator:
        """Drive a delegating load balancer through one dispatch.

        Implements the driver side of the `LoadBalancerLike` handshake.

        Only `RpcError` is treated as a worker-health signal:
        a non-transient `RpcError` triggers eviction from the
        context before the balancer is notified, so it observes the
        capacity change when choosing the next candidate.

        Exceptions that are not worker-health signals — e.g., a
        strict-mode `wool.ChainSerializationError` or a
        `ValueError` — propagate to the caller unwrapped,
        without eviction or retry. Cancellation (``CancelledError``)
        and other ``BaseException`` subclasses propagate the same way —
        they are caller intent, not worker failures.

        :param loadbalancer:
            The load balancer to drive.
        :param task:
            The task to dispatch.
        :param timeout:
            Per-attempt timeout forwarded to
            `WorkerConnection.dispatch`.
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
        generator = loadbalancer.delegate(task, context=ctx)
        try:
            try:
                uid = await anext(generator)
            except StopAsyncIteration:
                raise NoWorkersAvailable()

            while True:
                current = ctx.workers.get(uid)
                if current is None:
                    # Departed worker: not a dispatch failure — see
                    # LoadBalancerLike.
                    try:
                        uid = await anext(generator)
                    except StopAsyncIteration:
                        raise NoWorkersAvailable()
                    continue

                metadata, connection = current
                try:
                    stream = await connection.dispatch(task, timeout=timeout)
                except RpcError as exc:
                    # Catch only RpcError: any other exception falls to the
                    # outer finally, which aclose()s the generator without
                    # eviction.
                    if not isinstance(exc, TransientRpcError):
                        ctx.remove_worker(metadata)
                    try:
                        uid = await generator.athrow(exc)
                    except StopAsyncIteration:
                        raise NoWorkersAvailable() from exc
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
                        trailing = await generator.asend(uid)
                    except StopAsyncIteration:
                        result, stream = stream, None
                        return result
                    raise RuntimeError(
                        f"LoadBalancerLike.delegate yielded "
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
            await generator.aclose()

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
        """Create the security half of the sentinel admission gate.

        Workers and proxies must have compatible security settings:
        - Proxy with credentials only admits workers with secure=True
        - Proxy without credentials only admits workers with secure=False

        :param credentials:
            Channel credentials for this proxy.
        :returns:
            Predicate function for filtering workers by security
            compatibility, enforced at `_worker_sentinel`
            admission.
        """
        if credentials is not None:
            # Proxy has credentials: only accept secure workers
            return lambda metadata: metadata.secure
        else:
            # Proxy has no credentials: only accept insecure workers
            return lambda metadata: not metadata.secure

    @staticmethod
    def _create_version_filter() -> Callable[[WorkerMetadata], bool]:
        """Create the version half of the sentinel admission gate.

        A worker is accepted when its version is greater than or
        equal to the local proxy version within the same major
        version.  Workers with unparsable versions are rejected.

        :returns:
            Predicate function for filtering workers by version
            compatibility, enforced at `_worker_sentinel`
            admission.
        """
        from wool import protocol

        local_version = parse_version(protocol.__version__)
        if local_version is None:
            warnings.warn(
                f"Local protocol version {protocol.__version__!r} is not "
                "a parsable version; the admission gate rejects every "
                "worker until the proxy's own version metadata is available.",
                UnparsableVersionWarning,
                stacklevel=3,
            )

        def version_filter(metadata: WorkerMetadata) -> bool:
            worker_version = parse_version(metadata.version)
            if local_version is None or worker_version is None:
                return False
            return is_version_compatible(local_version, worker_version)

        return version_filter

    def _incompatibility_reason(self, metadata: WorkerMetadata) -> str | None:
        """Return which gate half rejects ``metadata``, or None if it passes.

        Security is reported first when both halves fail. Enforced by
        `_worker_sentinel` on every discovery path.
        """
        if not self._security_filter(metadata):
            return "security"
        if not self._version_filter(metadata):
            return "version"
        return None

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
        """Reconcile discovery events against the load-balancer context.

        The single admission gate for every discovery source.
        ``worker-added`` and ``worker-updated`` are handled
        identically — each reconciles the worker's membership to its
        current eligibility, regardless of how the proxy was
        constructed (a subscription filter screens on tags or any
        user-chosen metadata field, never on security/version
        compatibility). An event whose metadata fails the combined
        security/version predicate evicts the worker if it was admitted
        and is otherwise ignored; an eligible worker not yet held is
        admitted subject to the lease; an eligible worker already held
        is refreshed in place. Because either event type can admit or
        evict, a worker that crosses the compatibility boundary in
        either direction is tracked correctly on every path.
        ``worker-dropped`` events remove unconditionally: removing an
        unknown worker is a no-op.
        """
        assert self._loadbalancer_context is not None
        assert self._discovery_stream is not None
        assert self._workers_changed is not None
        client_credentials = (
            self._credentials.client_credentials()
            if self._credentials is not None
            else None
        )

        def connect(metadata: WorkerMetadata) -> WorkerConnection:
            return WorkerConnection(
                metadata.address,
                credentials=client_credentials,
                options=metadata.options,
            )

        async for event in self._discovery_stream:
            match event.type:
                case "worker-added" | "worker-updated":
                    uid = event.metadata.uid
                    # Bind once: the ``workers`` property builds a fresh
                    # MappingProxyType per access, and this branch runs per
                    # still-registered worker every rescan.
                    workers = self._loadbalancer_context.workers
                    present = uid in workers
                    reason = self._incompatibility_reason(event.metadata)
                    if reason is not None:
                        # Incompatible now — evict an admitted worker
                        # whose posture flipped; ignore one never held.
                        # Admission and eviction are one authority here,
                        # independent of the event type.
                        if present:
                            _logger.debug(
                                "Admission gate evicted worker %s: %s incompatible",
                                uid,
                                reason,
                            )
                            self._loadbalancer_context.remove_worker(event.metadata)
                            self._workers_changed.set()
                        else:
                            # Fires per rescan for standing chaff, so
                            # debug keeps it out of the default log.
                            _logger.debug(
                                "Admission gate rejected worker %s: %s incompatible",
                                uid,
                                reason,
                            )
                        continue
                    if present:
                        # Refresh in place; membership is unchanged, so
                        # the quorum wait need not re-evaluate. Displaced
                        # connections are not closed — see
                        # LoadBalancerContextLike.
                        self._loadbalancer_context.update_worker(
                            event.metadata, connect(event.metadata)
                        )
                    elif self._lease is not None and len(workers) >= self._lease:
                        # Admission is per uid — see the ``lease``
                        # parameter on this class.
                        continue
                    else:
                        _logger.debug("Admission gate admitted worker %s", uid)
                        # Displaced connections are not closed — see
                        # LoadBalancerContextLike.
                        self._loadbalancer_context.add_worker(
                            event.metadata, connect(event.metadata)
                        )
                        self._workers_changed.set()
                case "worker-dropped":
                    self._loadbalancer_context.remove_worker(event.metadata)
                    self._workers_changed.set()
