from __future__ import annotations

import asyncio
import logging
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
from typing import cast
from typing import overload

from typing_extensions import deprecated

from wool.exceptions import WoolWarning
from wool.runtime.context.factory import install_task_factory
from wool.runtime.discovery.base import DiscoveryLike
from wool.runtime.discovery.base import DiscoveryPublisherLike
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.typing import Factory
from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.base import BoundWorkerFactory
from wool.runtime.worker.base import WorkerFactory
from wool.runtime.worker.base import WorkerLike
from wool.runtime.worker.base import declares_host
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.proxy import DEFAULT_LAZY
from wool.runtime.worker.proxy import DEFAULT_QUORUM
from wool.runtime.worker.proxy import DEFAULT_QUORUM_TIMEOUT
from wool.runtime.worker.proxy import IneffectiveQuorumTimeoutWarning
from wool.runtime.worker.proxy import LoadBalancerLike
from wool.runtime.worker.proxy import RoundRobinLoadBalancer
from wool.runtime.worker.proxy import WorkerProxy
from wool.utilities.noreentry import noreentry

logger = logging.getLogger(__name__)


# public
class IneffectiveLeaseWarning(WoolWarning):
    """Emitted when ``lease`` is supplied to a `WorkerPool` that has
    no ``discovery`` service configured.

    The pool's worker count is bounded by ``spawn`` alone in those
    modes — ``lease`` is recorded but never consulted, so the supplied
    value has no effect at runtime.  Users who want strict behaviour
    can elevate the category to an error via
    `warnings.filterwarnings`.
    """


# public
class WorkerPool:
    """Orchestrates distributed workers for task execution.

    The core of wool's distributed runtime. Manages worker lifecycle,
    discovery, and load balancing across three modes:

    - **Ephemeral pools** spawn local workers managed within the
      pool's lifecycle. Perfect for development and single-machine
      deployments.

    - **Durable pools** connect to existing remote workers through
      discovery services. Workers run independently, serving multiple
      clients across distributed deployments.

    - **Hybrid pools** spawn local workers and additionally admit
      remote workers found through discovery.

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

    **Quorum gate:**

    .. code-block:: python

        # Spawn 4 workers, block on context entry until all 4 are
        # admitted, and time out after 30s if not.
        async with WorkerPool(spawn=4, quorum=4, quorum_timeout=30, lazy=False):
            result = await task()

    :param tags:
        Capability tags for spawned workers.
    :param spawn:
        Number of workers to spawn (0 = CPU count).
    :param size:
        .. deprecated::
            Use ``spawn`` instead. Will be removed in the next major release.
    :param lease:
        Maximum number of additionally discovered workers to admit to the
        pool. The total pool capacity is ``spawn + lease`` when both are
        set, or just ``lease`` for external pools. Defaults to ``None``
        (unbounded). Only meaningful when a ``discovery`` service is
        configured; supplying ``lease`` without ``discovery`` records
        the value but never consults it, accompanied by an
        `IneffectiveLeaseWarning`.
    :param worker:
        Worker factory callable. A `WorkerFactory` declares a ``host``
        keyword and receives the discovery publisher's prescribed bind
        host. A `BoundWorkerFactory` declares no ``host`` and owns its
        binding. Classification is by explicit signature declaration;
        see `WorkerFactory` for the rules. Defaults to `LocalWorker`,
        which declares ``host`` and therefore binds the publisher's
        prescribed host; a partial of it that leaves ``host`` unset
        (e.g. ``partial(LocalWorker, ...)``) stays bind-host-aware,
        while ``partial(LocalWorker, host="...")`` pins the bind and
        classifies bound.
    :param discovery:
        Discovery service to attach — a `~wool.DiscoveryLike` instance
        or any `Factory` form resolving to one. The resolved object is
        validated against the protocol at context entry.
    :param loadbalancer:
        Load balancer instance, factory, or context manager.
    :param credentials:
        Optional channel credentials for TLS/mTLS connections to workers.
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
        `~wool.runtime.worker.proxy.IneffectiveQuorumTimeoutWarning`.
        Defaults to ``60``; pass ``None`` to wait indefinitely.  With
        ``lazy=False`` the timeout fires at ``__aenter__``; the pool,
        never having entered, is unusable per its single-use
        semantics, so construct a new pool to retry.  With
        ``lazy=True`` (default) the timeout fires on the first
        `WorkerProxy.dispatch`, which leaves the proxy un-started so a
        later dispatch retries and recovers once a worker is admitted.
    :param shutdown_timeout:
        Maximum number of seconds to wait for spawned workers to stop
        during pool teardown. The bound applies as a single deadline to
        the full teardown sequence: each worker's ``stop()`` receives
        ``shutdown_timeout`` as its per-worker bound, the gather waits
        for whatever time remains, and publisher cleanup gets whatever
        is left. When the deadline elapses, abandoned worker UIDs are
        logged via ``logging.warning`` and teardown proceeds. ``None``
        disables the bound and restores the legacy unbounded wait.
        Must be positive when provided. Defaults to ``60.0``.
    :param lazy:
        When ``True`` (default), defer discovery setup and the quorum
        wait to the first `WorkerProxy.dispatch`.  When ``False``,
        eagerly enter the underlying proxy at ``__aenter__`` time and
        run the quorum wait there.
    :raises ValueError:
        If configuration is invalid, CPU count unavailable, or
        ``shutdown_timeout`` is not positive.
    :raises asyncio.TimeoutError:
        If the quorum wait does not complete within ``quorum_timeout``
        — raised by the underlying `WorkerProxy` at context entry
        (``lazy=False``) or first dispatch (``lazy=True``).

    .. caution::

       Pre-called context manager instances passed as ``loadbalancer``
       or ``discovery`` are not picklable and will cause nested routine
       dispatch to fail.  Pass a callable returning the context manager
       instead.  See `Factory`.
    """

    _workers: Final[dict[WorkerLike, Coroutine]]

    @overload
    def __init__(
        self,
        *tags: str,
        spawn: int = 0,
        worker: WorkerFactory | BoundWorkerFactory = LocalWorker,
        discovery: None = None,
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | None = None,
        quorum: int | None = DEFAULT_QUORUM,
        quorum_timeout: float | None = DEFAULT_QUORUM_TIMEOUT,
        shutdown_timeout: float | None = 60.0,
        lazy: bool = DEFAULT_LAZY,
    ):
        """Create an ephemeral pool of workers.

        Spawns the specified quantity of workers using the specified
        worker factory.
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
        quorum: int | None = DEFAULT_QUORUM,
        quorum_timeout: float | None = DEFAULT_QUORUM_TIMEOUT,
        shutdown_timeout: float | None = 60.0,
        lazy: bool = DEFAULT_LAZY,
    ):
        """Connect to an external pool of workers discovered by the
        specified discovery protocol.
        """
        ...

    @overload
    def __init__(
        self,
        *tags: str,
        spawn: int = 0,
        lease: int | None = None,
        worker: WorkerFactory | BoundWorkerFactory = LocalWorker,
        discovery: DiscoveryLike | Factory[DiscoveryLike],
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | None = None,
        quorum: int | None = DEFAULT_QUORUM,
        quorum_timeout: float | None = DEFAULT_QUORUM_TIMEOUT,
        shutdown_timeout: float | None = 60.0,
        lazy: bool = DEFAULT_LAZY,
    ):
        """Create a hybrid pool that spawns local workers and
        discovers remote workers through the specified discovery
        protocol.
        """
        ...

    @overload
    @deprecated("Use 'spawn' instead of 'size'.")
    def __init__(
        self,
        *tags: str,
        size: int,
        worker: WorkerFactory | BoundWorkerFactory = LocalWorker,
        discovery: None = None,
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | None = None,
        quorum: int | None = DEFAULT_QUORUM,
        quorum_timeout: float | None = DEFAULT_QUORUM_TIMEOUT,
        shutdown_timeout: float | None = 60.0,
        lazy: bool = DEFAULT_LAZY,
    ): ...

    @overload
    @deprecated("Use 'spawn' instead of 'size'.")
    def __init__(
        self,
        *tags: str,
        size: int,
        lease: int | None = None,
        worker: WorkerFactory | BoundWorkerFactory = LocalWorker,
        discovery: DiscoveryLike | Factory[DiscoveryLike],
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | None = None,
        quorum: int | None = DEFAULT_QUORUM,
        quorum_timeout: float | None = DEFAULT_QUORUM_TIMEOUT,
        shutdown_timeout: float | None = 60.0,
        lazy: bool = DEFAULT_LAZY,
    ): ...

    def __init__(
        self,
        *tags: str,
        spawn: int | None = None,
        size: int | None = None,
        lease: int | None = None,
        worker: WorkerFactory | BoundWorkerFactory | None = None,
        discovery: DiscoveryLike | Factory[DiscoveryLike] | None = None,
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | None = None,
        quorum: int | None = DEFAULT_QUORUM,
        quorum_timeout: float | None | UndefinedType = Undefined,
        shutdown_timeout: float | None = 60.0,
        lazy: bool = DEFAULT_LAZY,
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

        # Warn here rather than in WorkerProxy: _make_proxy elides
        # quorum_timeout for falsy quorums (see its docstring), so the
        # proxy never sees that the pool's user supplied one. Other
        # quorum validations are delegated to WorkerProxy.
        if quorum_timeout is Undefined:
            quorum_timeout = DEFAULT_QUORUM_TIMEOUT
        elif not quorum:
            warnings.warn(
                "'quorum_timeout' has no effect when 'quorum' is None or 0; "
                "the value is recorded but never consulted",
                IneffectiveQuorumTimeoutWarning,
                stacklevel=2,
            )

        if shutdown_timeout is not None and shutdown_timeout <= 0:
            raise ValueError("Shutdown timeout must be positive")
        self._shutdown_timeout = shutdown_timeout

        match (spawn, discovery):
            case (spawn, discovery) if spawn is not None and discovery is not None:
                spawn = _resolve_spawn(spawn)
                max_workers = spawn + lease if lease is not None else None
                self._validate_quorum(quorum, max_workers)

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
                            async with self._make_proxy(
                                discovery=discovery_svc.subscribe(_predicate(tags)),
                                loadbalancer=loadbalancer,
                                lease=max_workers,
                                quorum=quorum,
                                quorum_timeout=quorum_timeout,
                                lazy=self._lazy,
                            ):
                                yield
                    finally:
                        await self._exit_context(discovery_ctx)

            case (spawn, None) if spawn is not None:
                if lease is not None:
                    warnings.warn(
                        "'lease' has no effect when no 'discovery' service is "
                        "configured; the value is recorded but never consulted",
                        IneffectiveLeaseWarning,
                        stacklevel=2,
                    )
                spawn = _resolve_spawn(spawn)
                max_workers = None
                self._validate_quorum(quorum, max_workers)

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
                            async with self._make_proxy(
                                discovery=discovery.subscribe(_predicate(tags)),
                                loadbalancer=loadbalancer,
                                lease=max_workers,
                                quorum=quorum,
                                quorum_timeout=quorum_timeout,
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
                        raise TypeError(
                            f"Expected DiscoveryLike, got: {type(discovery_svc)}"
                        )
                    try:
                        async with self._make_proxy(
                            discovery=discovery_svc.subscriber,
                            loadbalancer=loadbalancer,
                            lease=lease,
                            quorum=quorum,
                            quorum_timeout=quorum_timeout,
                            lazy=self._lazy,
                        ):
                            yield
                    finally:
                        await self._exit_context(discovery_ctx)

            case (None, None):
                if lease is not None:
                    warnings.warn(
                        "'lease' has no effect when no 'discovery' service is "
                        "configured; the value is recorded but never consulted",
                        IneffectiveLeaseWarning,
                        stacklevel=2,
                    )
                spawn = _resolve_spawn(0)
                max_workers = None
                self._validate_quorum(quorum, max_workers)

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
                            async with self._make_proxy(
                                discovery=discovery.subscriber,
                                lease=max_workers,
                                loadbalancer=loadbalancer,
                                quorum=quorum,
                                quorum_timeout=quorum_timeout,
                                lazy=self._lazy,
                            ):
                                yield

            case _:
                raise RuntimeError

        self._proxy_factory = create_proxy

    @staticmethod
    def _validate_quorum(quorum: int | None, max_workers: int | None) -> None:
        """Reject a quorum that exceeds the pool's bounded capacity.

        No-op when either side is ``None`` (an unset quorum or an
        unbounded ``max_workers``).
        """
        if max_workers is not None and quorum is not None and quorum > max_workers:
            raise ValueError(
                f"Quorum ({quorum}) cannot exceed pool capacity "
                f"({max_workers}) — the quorum would never be satisfied"
            )

    @noreentry
    async def __aenter__(self) -> WorkerPool:
        """Start the worker pool and its services.

        Installs wool's task factory on the running loop and enters
        the pool context for the configured mode — bringing up
        discovery, spawning local workers where applicable, and
        preparing the dispatch proxy.

        :returns:
            This `WorkerPool` instance.
        :raises RuntimeError:
            If the pool has already been entered.  `WorkerPool`
            contexts are single-use — create a new instance instead
            of re-entering.
        """
        install_task_factory()
        self._proxy_context = self._proxy_factory()
        await self._proxy_context.__aenter__()
        return self

    async def __aexit__(self, *args):
        """Stop all workers and tear down the pool and its services."""
        await self._proxy_context.__aexit__(*args)

    @asynccontextmanager
    async def _worker_context(
        self,
        *tags: str,
        spawn: int,
        factory: WorkerFactory | BoundWorkerFactory | None,
        publisher: DiscoveryPublisherLike,
    ):
        """Spawn, publish, and reap the pool's local workers.

        Workers start concurrently and announce themselves through
        the entered publisher; any spawn failure aborts entry with an
        `ExceptionGroup`. Factories that declare ``host``, including
        the default `LocalWorker`, receive the publisher's prescribed
        bind host (see `~wool.DiscoveryPublisherLike.bind_host`);
        bound factories own their binding. Teardown applies the
        pool's ``shutdown_timeout`` as a single deadline across worker
        stops and publisher cleanup, logging any workers it abandons,
        and runs even when publisher validation or worker construction
        fails, so an entered publisher context is never leaked.

        :yields:
            Metadata for the spawned workers.
        """
        publisher_svc, publisher_ctx = await self._enter_context(publisher)
        try:
            if not isinstance(publisher_svc, DiscoveryPublisherLike):
                raise TypeError(
                    f"Expected DiscoveryPublisherLike, got: {type(publisher_svc)}"
                )
            if factory is None:
                # The pool owns the default; no signature inspection required.
                factory = LocalWorker
                unbound = True
            else:
                unbound = declares_host(factory)

            tasks = []
            for _ in range(spawn):
                if unbound:
                    worker = cast(WorkerFactory, factory)(
                        *tags,
                        credentials=self._credentials,
                        host=publisher_svc.bind_host,
                    )
                else:
                    worker = cast(BoundWorkerFactory, factory)(
                        *tags, credentials=self._credentials
                    )

                async def start(worker):
                    await worker.start()
                    await publisher_svc.publish("worker-added", worker.metadata)

                async def stop(worker):
                    await publisher_svc.publish("worker-dropped", worker.metadata)
                    await worker.stop(timeout=self._shutdown_timeout)

                task = asyncio.create_task(start(worker))
                tasks.append(task)
                self._workers[worker] = stop(worker)

            results = await asyncio.gather(*tasks, return_exceptions=True)
            if errors := [r for r in results if isinstance(r, Exception)]:
                raise ExceptionGroup("worker spawn failures", errors)
            yield [w.metadata for w in self._workers if w.metadata]
        finally:
            loop = asyncio.get_running_loop()
            deadline = (
                None
                if self._shutdown_timeout is None
                else loop.time() + self._shutdown_timeout
            )

            def remaining() -> float | None:
                if deadline is None:
                    return None
                return max(0.0, deadline - loop.time())

            if self._workers:
                worker_by_task = {
                    asyncio.create_task(coro): worker
                    for worker, coro in self._workers.items()
                }
                done, pending = await asyncio.wait(worker_by_task, timeout=remaining())
                for task in pending:
                    task.cancel()
                await asyncio.gather(*done, *pending, return_exceptions=True)
                abandoned_tasks = set(pending)
                for task in done:
                    if not task.cancelled() and isinstance(
                        task.exception(), TimeoutError
                    ):
                        abandoned_tasks.add(task)
                if abandoned_tasks:
                    abandoned_uids = sorted(
                        str(worker_by_task[task].uid) for task in abandoned_tasks
                    )
                    logger.warning(
                        "WorkerPool shutdown abandoned %d worker(s) "
                        "that did not stop within %ss: %s",
                        len(abandoned_uids),
                        self._shutdown_timeout,
                        abandoned_uids,
                        extra={"abandoned_worker_uids": abandoned_uids},
                    )

            try:
                await asyncio.wait_for(
                    self._exit_context(publisher_ctx), timeout=remaining()
                )
            except TimeoutError:
                logger.warning(
                    "WorkerPool publisher cleanup did not complete within %ss",
                    self._shutdown_timeout,
                )

    def _make_proxy(
        self,
        *,
        discovery: DiscoverySubscriberLike,
        loadbalancer: LoadBalancerLike | Factory[LoadBalancerLike],
        lease: int | None,
        quorum: int | None,
        quorum_timeout: float | None,
        lazy: bool,
    ) -> WorkerProxy:
        """Construct a `WorkerProxy` for this pool's discovery.

        Selects `WorkerProxy`'s typed overload via narrowing: when
        ``quorum`` is truthy, forwards both ``quorum`` and
        ``quorum_timeout``; otherwise normalizes ``quorum=0`` and
        ``quorum=None`` to literal ``None`` (which the pool's user
        contract documents as equivalent) and drops ``quorum_timeout``.
        """
        if quorum:
            return WorkerProxy(
                discovery=discovery,
                loadbalancer=loadbalancer,
                credentials=self._credentials,
                lease=lease,
                quorum=quorum,
                quorum_timeout=quorum_timeout,
                lazy=lazy,
            )
        return WorkerProxy(
            discovery=discovery,
            loadbalancer=loadbalancer,
            credentials=self._credentials,
            lease=lease,
            quorum=None,
            lazy=lazy,
        )

    async def _enter_context(self, factory):
        """Normalize a configured dependency into a live object.

        Accepts a bare instance, a callable factory, an awaitable, or
        a sync/async context manager (entering the latter) and
        returns ``(object, owning_context)``, where the context is
        ``None`` unless this call entered one and owes it an exit via
        `_exit_context`.
        """
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
        """Exit a sync or async context manager, if any.

        Forwards the active exception info; ``None`` is a no-op.
        """
        if isinstance(ctx, AsyncContextManager):
            await ctx.__aexit__(*sys.exc_info())
        elif isinstance(ctx, ContextManager):
            ctx.__exit__(*sys.exc_info())


def _resolve_spawn(spawn: int) -> int:
    """Resolve ``spawn=0`` to the CPU count and reject negative values."""
    if spawn == 0:
        cpu_count = os.cpu_count()
        if cpu_count is None:
            raise ValueError("Unable to determine CPU count")
        spawn = cpu_count
    elif spawn < 0:
        raise ValueError("Spawn must be non-negative")
    return spawn


def _predicate(tags):
    """Build a tag-intersection worker filter; matches all when no tags."""
    return lambda w: bool(w.tags & set(tags)) if tags else True
