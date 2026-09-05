"""Worker pools: local spawning, external discovery, or both.

Provides `WorkerPool`, the lifecycle owner that starts workers, publishes
them, and holds the dispatch proxy its callers route through.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import uuid
import warnings
from contextlib import AsyncExitStack
from contextlib import asynccontextmanager
from typing import Any
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
from wool.runtime.typing import resolved
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.auth import WorkerCredentialsProvider
from wool.runtime.worker.auth import normalize_peer
from wool.runtime.worker.base import WorkerFactoryLike
from wool.runtime.worker.base import WorkerLike
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.proxy import DEFAULT_LAZY
from wool.runtime.worker.proxy import DEFAULT_QUORUM
from wool.runtime.worker.proxy import DEFAULT_QUORUM_TIMEOUT
from wool.runtime.worker.proxy import IneffectiveQuorumTimeoutWarning
from wool.runtime.worker.proxy import LoadBalancerLike
from wool.runtime.worker.proxy import RoundRobinLoadBalancer
from wool.runtime.worker.proxy import WorkerProxy
from wool.utilities.noreentry import noreentry
from wool.utilities.signature import accepts_kwarg
from wool.utilities.signature import presupplies_kwarg
from wool.utilities.signature import requires_kwarg
from wool.utilities.signature import unbindable_call

logger = logging.getLogger(__name__)


# public
class IneffectiveIdentityWarning(WoolWarning):
    """Emitted when a `WorkerPool`'s ``identity`` is inert.

    A pool that both spawns workers and dispatches through them hands one
    credential provider to both roles. When the provider configures
    ``peers`` but what the pool's workers will advertise is not something
    that policy accepts (i.e., a name it refuses, or no name at all),
    every worker the pool spawns is refused by the pool's own admission
    gate, which surfaces far from the cause as a quorum timeout. A pool
    with no ``discovery`` service raises instead — see `WorkerPool`'s
    ``ValueError``. Checked only when the pool owns the default factory:
    with a custom one it cannot know what its workers will advertise.

    Separately, a factory that cannot accept an ``identity`` keyword owns
    the name its workers advertise, so the pool has no way to pass its own
    down and that value goes unused.

    Finally, a pool that spawns no workers has nobody to advertise a name
    on its behalf, so an ``identity`` given to a discovery-only pool is
    inert.

    Warned rather than raised because a hybrid pool may legitimately
    contribute capacity it does not itself dial, because a factory
    owning its identity is a valid configuration, and because an inert
    parameter is not an invalid one. Users who want strict behaviour can
    elevate the category to an error via `warnings.filterwarnings`.

    Never emitted for a factory that *can* receive the value — see
    `WorkerFactoryLike` for why the pool asserts nothing beyond that.
    """


# public
class IneffectiveLeaseWarning(WoolWarning):
    """Emitted when ``lease`` is supplied without ``discovery``.

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
    discovery, and load balancing. Which mode a call resolves to follows
    from whether ``spawn`` and ``discovery`` are given: a pool may spawn
    workers it owns, admit workers others are running, or both. The
    worker package README tabulates the modes and what each does.

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
        set, or just ``lease`` for a durable pool with no spawned
        workers. Defaults to ``None`` (unbounded). Only meaningful when
        a ``discovery`` service is configured; supplying ``lease``
        without ``discovery`` records the value but never consults it,
        accompanied by an `IneffectiveLeaseWarning`. Admission semantics
        are `WorkerProxy`'s — see its ``lease`` parameter.
    :param worker:
        Worker factory callable, in any of the four shapes
        `WorkerFactoryLike` admits. Every keyword the pool forwards —
        the bind host, the ``identity``, and the ``credentials`` — is
        offered on one rule: passed when the factory can receive it and
        has not already pre-supplied it. See `WorkerFactoryLike`.
        Defaults to `LocalWorker`, which takes all three.
    :param discovery:
        Discovery service to attach — a `~wool.DiscoveryLike` instance
        or any `Factory` form resolving to one. The resolved object is
        validated against the protocol at context entry. Workers it
        surfaces are additionally subject to the underlying
        `WorkerProxy`'s admission gate.

        .. caution::

           A pre-called context-manager instance passed as
           ``discovery`` is not picklable and breaks nested routine
           dispatch. Pass a callable returning it instead (see
           `Factory`).
    :param loadbalancer:
        Load balancer instance, factory, or context manager.

        .. caution::

           A pre-called context-manager instance passed as
           ``loadbalancer`` is not picklable and breaks nested routine
           dispatch. Pass a callable returning it instead (see
           `Factory`).
    :param credentials:
        Optional credentials for TLS/mTLS — either a `WorkerCredentials` or a
        `WorkerCredentialsProvider` (from `WorkerCredentials.as_provider`, or
        built with a factory callable for credential rotation) — see
        `WorkerCredentialsProvider` for what a provider adds. Applied to
        both spawned workers and the dispatch proxy.
    :param identity:
        Logical workload identity for the workers this pool spawns,
        advertised through discovery so peers know which name to verify
        them against. Every worker in a pool shares it: an identity
        names the workload, not the instance, so replicas of one
        workload are meant to be indistinguishable. Distinguishing
        *between* pools is what this expresses.

        Three states, distinguished because an explicit ``None`` is a
        value and an unset parameter is not:

        - ``Undefined`` (the default) — the keyword is withheld
          entirely and the factory owns the name its workers advertise.
          Nothing is inferred from ``peers``: a policy names which
          workers this pool's client will accept, never what its own
          workers claim to be.
        - ``str`` or ``None`` — passed to a factory that can receive it.
          ``None`` says this pool has no identity to give.
        - ``str`` or ``None``, factory cannot accept it — withheld, and
          construction reports an `IneffectiveIdentityWarning`.

        .. caution::

           Passing the value is all the pool can guarantee — see
           `WorkerFactoryLike`. Where a factory accepts an ``identity``
           and advertises something else, the ``peers`` policy refuses
           the workers and nothing fails at construction: the proxy
           admits none of them and it surfaces as a **startup timeout**.
           Suspect this first when a pool with a custom factory hangs
           waiting for quorum.
    :param quorum:
        Minimum number of workers admitted before the pool is ready.
        Forwarded to this pool's `WorkerProxy`, which documents the
        gate, its default, and how ``lazy`` decides when it is waited
        on.
    :param quorum_timeout:
        Seconds to wait for ``quorum`` workers before raising
        `asyncio.TimeoutError`. Forwarded to this pool's `WorkerProxy`,
        which documents the bound and when it is inert. A timeout at
        context entry (``lazy=False``) leaves the pool, never having
        entered, unusable per its single-use semantics, so construct a
        new pool to retry.
    :param shutdown_timeout:
        Maximum number of seconds to wait for spawned workers to stop
        during pool teardown, applied as a single deadline to the full
        teardown sequence. When it elapses the pool stops waiting and
        logs the workers it gave up on; those workers are reaped
        regardless, so what is lost is the graceful drain, not the
        process. A stop that fails for any other reason is logged and
        teardown continues, so one worker's failure never strands
        another's cleanup. Reaping does not depend on discovery health:
        a ``worker-dropped`` announcement that fails or hangs is logged
        and the worker stopped regardless — discovery cannot strand a
        worker, but a hanging announcement can consume the deadline and
        cost that worker its graceful drain. A finite value overrides a
        worker's own shutdown grace period; ``None`` disables the bound
        and requests an indefinite drain from each worker, in keeping
        with that value's unbounded-wait contract — a hanging
        announcement (or task) then blocks teardown indefinitely. Must
        be positive when provided. Defaults to ``60.0``.

        .. caution::

           Size ``shutdown_timeout`` for the slowest worker the pool
           spawns. Because a finite value overrides the worker's own
           grace period, a worker configured to shut down more slowly
           than the pool waits — e.g., ``partial(LocalWorker,
           shutdown_grace_period=120)`` under the default
           ``shutdown_timeout=60.0`` — never receives that grace; it is
           silently ignored unless the pool waits unbounded
           (``shutdown_timeout=None``).
    :param lazy:
        Forwarded to this pool's `WorkerProxy`, which documents both
        states and where the quorum timeout surfaces in each.
    :raises ValueError:
        If configuration is invalid, CPU count unavailable,
        ``shutdown_timeout`` is not positive, ``identity`` names a
        workload without ``credentials`` to back it, the worker factory
        requires an ``identity`` this pool has none to give, or this
        pool spawns workers its own ``peers`` policy would refuse and
        has no ``discovery`` service to supply others (see
        `IneffectiveIdentityWarning`, which covers the hybrid case).
    :raises asyncio.TimeoutError:
        If the quorum wait does not complete within ``quorum_timeout``
        — raised by the underlying `WorkerProxy` at context entry
        (``lazy=False``) or first dispatch (``lazy=True``).

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

        class PriorityBalancer:
            async def delegate(self, task, *, context):
                # Yield worker uids in priority order.
                for uid in context.workers:
                    try:
                        sent = yield uid
                    except Exception:
                        continue
                    if sent is not None:
                        return


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

    .. rubric:: Implementation notes

    ``shutdown_timeout`` is apportioned across teardown in order:
    each worker's drop announcement and ``stop()`` share whatever
    remains of the deadline, the gather waits for whatever is left,
    and publisher cleanup gets the remainder. Workers the pool stops
    waiting on are logged via ``logging.warning``; a `LocalWorker`
    is still reaped off-loop (see `LocalWorker._stop`), which can
    hold ``__aexit__`` past the deadline by up to the reap
    escalation. A ``worker-dropped`` announcement that raises is
    logged via ``logging.error``, and one that hangs is cancelled at
    the deadline and logged there; either way the stop runs on the
    deadline's remainder. `_worker_context` owns the rationale for
    that cancellation handling.
    """

    _workers: Final[dict[WorkerLike, Coroutine]]

    @overload
    def __init__(
        self,
        *tags: str,
        spawn: int = 0,
        worker: WorkerFactoryLike = LocalWorker,
        discovery: None = None,
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | WorkerCredentialsProvider | None = None,
        identity: str | None | UndefinedType = Undefined,
        quorum: int | None = DEFAULT_QUORUM,
        quorum_timeout: float | None = DEFAULT_QUORUM_TIMEOUT,
        shutdown_timeout: float | None = 60.0,
        lazy: bool = DEFAULT_LAZY,
    ):
        """Create an ephemeral pool of workers.

        Spawns the specified quantity of workers using the specified
        worker factory. An ``identity`` reaches only a factory that
        declares one — see the parameter's documentation.
        """
        ...

    # Overload order is important: a call supplying only 'discovery'
    # matches this overload and the hybrid one, since every other hybrid
    # parameter defaults, and resolution takes the first match.
    @overload
    def __init__(
        self,
        *,
        lease: int | None = None,
        discovery: DiscoveryLike | Factory[DiscoveryLike],
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | WorkerCredentialsProvider | None = None,
        quorum: int | None = DEFAULT_QUORUM,
        quorum_timeout: float | None = DEFAULT_QUORUM_TIMEOUT,
        shutdown_timeout: float | None = 60.0,
        lazy: bool = DEFAULT_LAZY,
    ):
        """Connect to workers a discovery protocol supplies."""
        ...

    @overload
    def __init__(
        self,
        *tags: str,
        spawn: int = 0,
        lease: int | None = None,
        worker: WorkerFactoryLike = LocalWorker,
        discovery: DiscoveryLike | Factory[DiscoveryLike],
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | WorkerCredentialsProvider | None = None,
        identity: str | None | UndefinedType = Undefined,
        quorum: int | None = DEFAULT_QUORUM,
        quorum_timeout: float | None = DEFAULT_QUORUM_TIMEOUT,
        shutdown_timeout: float | None = 60.0,
        lazy: bool = DEFAULT_LAZY,
    ):
        """Spawn local workers and discover remote ones too."""
        ...

    @overload
    @deprecated("Use 'spawn' instead of 'size'.")
    def __init__(
        self,
        *tags: str,
        size: int,
        worker: WorkerFactoryLike = LocalWorker,
        discovery: None = None,
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | WorkerCredentialsProvider | None = None,
        identity: str | None | UndefinedType = Undefined,
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
        worker: WorkerFactoryLike = LocalWorker,
        discovery: DiscoveryLike | Factory[DiscoveryLike],
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | WorkerCredentialsProvider | None = None,
        identity: str | None | UndefinedType = Undefined,
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
        worker: WorkerFactoryLike | None = None,
        discovery: DiscoveryLike | Factory[DiscoveryLike] | None = None,
        loadbalancer: (
            LoadBalancerLike | Factory[LoadBalancerLike]
        ) = RoundRobinLoadBalancer,
        credentials: WorkerCredentials | WorkerCredentialsProvider | None = None,
        identity: str | None | UndefinedType = Undefined,
        quorum: int | None = DEFAULT_QUORUM,
        quorum_timeout: float | None | UndefinedType = Undefined,
        shutdown_timeout: float | None = 60.0,
        lazy: bool = DEFAULT_LAZY,
    ):
        self._workers = {}
        self._provider = WorkerCredentialsProvider.coerce(credentials)
        # Three-state — see the `identity` parameter. `normalize_peer`
        # accepts only `str | None`, so `Undefined` bypasses it.
        self._identity = (
            identity
            if identity is Undefined
            else normalize_peer(identity, parameter="identity")
        )
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

        # The identity guards below concern workers this pool starts, so
        # they key on whether it spawns at all rather than on whether
        # 'spawn' was passed; keyed on the argument, they would refuse a
        # discovery-only pool, which starts nothing and is purely a client.
        spawns = spawn is not None or discovery is None

        if not spawns and self._identity is not Undefined:
            # Inert, not invalid — warned like 'lease' — see
            # `IneffectiveIdentityWarning`.
            warnings.warn(
                "'identity' has no effect on a pool that spawns no workers; "
                "it names what this pool's own workers advertise, and a "
                "discovery-only pool starts none",
                IneffectiveIdentityWarning,
                stacklevel=2,
            )

        if spawns and isinstance(self._identity, str) and self._provider is None:
            # `WorkerProcess` enforces the same precondition; hoisting it
            # fails construction here rather than a subprocess at
            # __aenter__. Only a name needs backing — `None` claims nothing.
            raise ValueError(
                "identity requires credentials: an identity is proven by a "
                "name in the worker's certificate, so a worker serving "
                "plaintext has nothing to back the one it claims."
            )

        # One rule governs every forwarded keyword — see `WorkerFactoryLike`.
        # Each check binds the call this pool will make and so needs the
        # rest of it; binding ignores values, so a placeholder stands in for
        # the bind host, which the publisher supplies only at entry.
        def forwards(keyword: str, *args: Any, **kwargs: Any) -> bool:
            assert worker is not None
            return accepts_kwarg(worker, keyword, *args, **kwargs) and not (
                presupplies_kwarg(worker, keyword)
            )

        rest: dict[str, Any] = {}
        if worker is not None:
            self._forwards_credentials = forwards("credentials")
            if self._forwards_credentials:
                rest["credentials"] = self._provider
            self._forwards_host = forwards("host", *tags, **rest)
            if self._forwards_host:
                rest["host"] = ""
            identity_reaches_workers = forwards("identity", *tags, **rest)
            identity_is_mandatory = requires_kwarg(worker, "identity", *tags, **rest)
        else:
            # The pool owns the default; no signature inspection required.
            self._forwards_credentials = True
            self._forwards_host = True
            rest["credentials"] = self._provider
            identity_reaches_workers = True
            identity_is_mandatory = False

        # Settled once: the answers are properties of the factory and of
        # this configuration, neither of which changes before spawn, and
        # one home keeps the diagnostics below consistent with the call
        # they describe. `Undefined` withholds the keyword — see the
        # `identity` parameter.
        self._forwards_identity = (
            identity_reaches_workers and self._identity is not Undefined
        )

        if spawns and self._identity is Undefined and identity_is_mandatory:
            # The factory cannot be called without a value this pool does
            # not have. Provable now; left alone, it surfaces as a TypeError
            # inside __aenter__, far from the mistake.
            raise ValueError(
                "the worker factory requires an 'identity' keyword but this "
                "pool has none configured; pass 'identity' to the pool or "
                "give the factory a default."
            )

        if spawns and worker is not None:
            # The per-keyword checks say nothing about the rest of the call,
            # so a factory demanding something the pool never passes reaches
            # this point. Bind the whole call once and name the argument at
            # fault, rather than let a TypeError surface inside __aenter__
            # against a signature the caller can no longer see.
            if self._identity is not Undefined and identity_reaches_workers:
                rest["identity"] = self._identity
            if reason := unbindable_call(worker, *tags, **rest):
                raise ValueError(
                    f"the worker factory cannot be called by this pool: {reason}"
                )

        if spawns and self._provider is not None and worker is None:
            # The pool is its own client — see `IneffectiveIdentityWarning`
            # for the failure this predicts. Predictable only for the
            # default factory: a custom one may accept the identity and
            # advertise something else, and no signature distinguishes the
            # two — see `WorkerFactoryLike`.
            advertised = None if self._identity is Undefined else self._identity
            configured = self._provider.peers is not None
            if configured and not self._provider.accepts_peer(advertised):
                detail = (
                    f"advertises {advertised!r}"
                    if advertised is not None
                    else "advertises no identity"
                )
                message = (
                    f"this pool {detail}, which its own 'peers' policy does "
                    "not accept, so its proxy will refuse every worker it "
                    "spawns. Set 'identity' to a name the policy accepts."
                )
                if discovery is None:
                    # Ephemeral: the spawned workers are all the proxy will
                    # ever see, so the pool is unsatisfiable rather than
                    # merely wasteful.
                    raise ValueError(message)
                warnings.warn(message, IneffectiveIdentityWarning, stacklevel=2)

        if spawns and self._identity is not Undefined and not identity_reaches_workers:
            # Report only what is provable: which of the two ways the
            # factory owns the name, since the remedies differ. See
            # `WorkerFactoryLike`.
            if worker is not None and presupplies_kwarg(worker, "identity"):
                detail = (
                    "the worker factory already binds its own 'identity', "
                    "which this pool does not override. Drop one of the two"
                )
            else:
                detail = (
                    "the worker factory cannot accept an 'identity' keyword, "
                    "so this pool has no way to pass it down. Give the "
                    "factory an 'identity' keyword to receive it"
                )
            warnings.warn(
                f"'identity' is set but {detail}; whatever identity its "
                "workers advertise is the factory's, not this value.",
                IneffectiveIdentityWarning,
                stacklevel=2,
            )

        if lease is not None and lease < 0:
            raise ValueError("Lease must be non-negative")

        # Warned here rather than in WorkerProxy: `_make_proxy` drops
        # quorum_timeout for a falsy quorum, so the proxy never sees that
        # one was supplied. Every other quorum validation is WorkerProxy's.
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
                    async with resolved(discovery) as discovery_svc:
                        if not isinstance(discovery_svc, DiscoveryLike):
                            raise TypeError(
                                f"Expected DiscoveryLike, got: {type(discovery_svc)}"
                            )
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
                    async with resolved(discovery) as discovery_svc:
                        if not isinstance(discovery_svc, DiscoveryLike):
                            raise TypeError(
                                f"Expected DiscoveryLike, got: {type(discovery_svc)}"
                            )
                        async with self._make_proxy(
                            discovery=discovery_svc.subscriber,
                            loadbalancer=loadbalancer,
                            lease=lease,
                            quorum=quorum,
                            quorum_timeout=quorum_timeout,
                            lazy=self._lazy,
                        ):
                            yield

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
        factory: WorkerFactoryLike | None,
        publisher: DiscoveryPublisherLike,
    ):
        """Spawn, publish, and reap the pool's local workers.

        Workers start concurrently and announce themselves through
        the entered publisher; any spawn failure aborts entry with an
        `ExceptionGroup`. Which keywords each factory receives is
        settled at construction — see `WorkerFactoryLike`. Only the
        bind host is resolved here, at publisher entry (see
        `~wool.DiscoveryPublisherLike.bind_host`). Teardown applies the pool's
        ``shutdown_timeout`` as a single deadline across worker stops
        and publisher cleanup — see that parameter for the contract
        this implements — and runs even when publisher validation or
        worker construction fails, so an entered publisher context is
        always exited rather than dropped on the floor. Cleanup is
        itself bounded by what remains of the deadline, so a teardown
        that exhausts the deadline can time cleanup out before the
        publisher's ``__aexit__`` runs.

        :yields:
            Metadata for the spawned workers.

        .. rubric:: Implementation notes

        The drop announcement gets its own cancellation arm because
        `asyncio.CancelledError` is a `BaseException`: an ``except
        Exception`` guard cannot see the cancellation the shutdown
        deadline delivers to an announcement that hangs rather than
        raises, so without that arm the discovery failure driving the
        teardown goes unreported and the reap warning is left blaming
        the worker for a fault that was discovery's.

        The stop sits in that ``try``'s ``finally`` rather than after
        it, so it outlives the same cancellation, and it takes
        ``remaining()`` rather than the full ``shutdown_timeout``. The
        deadline has already elapsed by the time a hanging announcement
        reaches the stop, and handing it a fresh budget there would let
        an already-cancelled task outlive the deadline that the gather
        below exists to enforce — trading a leaked worker for an
        unbounded teardown.
        """
        publisher_stack = AsyncExitStack()
        publisher_svc = await publisher_stack.enter_async_context(resolved(publisher))
        try:
            if not isinstance(publisher_svc, DiscoveryPublisherLike):
                raise TypeError(
                    f"Expected DiscoveryPublisherLike, got: {type(publisher_svc)}"
                )
            if factory is None:
                factory = LocalWorker

            # Settled in `__init__`; only the bind host is resolved here.
            kwargs: dict[str, Any] = {}
            if self._forwards_credentials:
                kwargs["credentials"] = self._provider
            if self._forwards_host:
                kwargs["host"] = publisher_svc.bind_host
            if self._forwards_identity:
                kwargs["identity"] = cast(str | None, self._identity)

            tasks = []
            for _ in range(spawn):
                worker = cast(WorkerFactoryLike, factory)(*tags, **kwargs)

                async def start(worker):
                    await worker.start()
                    await publisher_svc.publish("worker-added", worker.metadata)

                async def stop(worker):
                    if (metadata := worker.metadata) is None:
                        # A worker whose start failed was never announced
                        # and cannot be stopped (`Worker.stop` rejects it),
                        # so teardown has nothing to undo.
                        return
                    try:
                        # Announce the drop before stopping so subscribers
                        # stop routing to a worker that is about to go away.
                        await publisher_svc.publish("worker-dropped", metadata)
                    except Exception:
                        # An announcement failure never abandons the stop —
                        # see the docstring's implementation notes.
                        logger.error(
                            "WorkerPool shutdown could not announce worker %s as "
                            "dropped; the stop is attempted regardless and "
                            "reported separately if it fails, but subscribers "
                            "may keep routing to it until they observe the drop "
                            "by other means",
                            worker.uid,
                            exc_info=True,
                            extra={"undropped_worker_uid": str(worker.uid)},
                        )
                    except asyncio.CancelledError:
                        # `except Exception` cannot see a cancelled
                        # announcement — see the docstring's implementation
                        # notes.
                        logger.error(
                            "WorkerPool shutdown could not announce worker %s as "
                            "dropped within the shutdown deadline; the stop is "
                            "attempted regardless, but subscribers may keep "
                            "routing to it until they observe the drop by other "
                            "means",
                            worker.uid,
                            extra={"undropped_worker_uid": str(worker.uid)},
                        )
                        raise
                    finally:
                        # Outlives the cancellation above on the deadline's
                        # remainder — see the implementation notes. An
                        # unbounded teardown asks for an indefinite drain,
                        # which `Worker.stop` spells as a negative grace;
                        # ``grace=None`` would mean no grace at all.
                        grace = remaining()
                        await worker.stop(grace=-1.0 if grace is None else grace)

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
                reaped_tasks = set(pending)
                for task in worker_by_task:
                    if task.cancelled():
                        continue
                    if (error := task.exception()) is None:
                        continue
                    if isinstance(error, TimeoutError):
                        reaped_tasks.add(task)
                    else:
                        logger.error(
                            "WorkerPool shutdown could not stop worker %s cleanly",
                            worker_by_task[task].uid,
                            exc_info=error,
                        )
                if reaped_tasks:
                    reaped_uids = sorted(
                        str(worker_by_task[task].uid) for task in reaped_tasks
                    )
                    logger.warning(
                        "WorkerPool shutdown stopped waiting for %d worker(s) "
                        "that did not stop gracefully within %ss and reaped "
                        "them; in-flight work may have been lost: %s",
                        len(reaped_uids),
                        self._shutdown_timeout,
                        reaped_uids,
                        extra={"reaped_worker_uids": reaped_uids},
                    )

            try:
                await asyncio.wait_for(
                    publisher_stack.__aexit__(*sys.exc_info()), timeout=remaining()
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
                credentials=self._provider,
                lease=lease,
                quorum=quorum,
                quorum_timeout=quorum_timeout,
                lazy=lazy,
            )
        return WorkerProxy(
            discovery=discovery,
            loadbalancer=loadbalancer,
            credentials=self._provider,
            lease=lease,
            quorum=None,
            lazy=lazy,
        )


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
