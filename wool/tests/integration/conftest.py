"""Integration test infrastructure.

Provides the scenario model, dimension enums, pairwise covering array,
fixtures, and builder functions for composable integration tests.
"""

from __future__ import annotations

import asyncio
import datetime
import ipaddress
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import fields
from enum import Enum
from enum import auto
from functools import partial

import grpc
import pytest
import pytest_asyncio
from allpairspy import AllPairs
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from hypothesis import strategies as st

from wool.runtime.context.runtime import dispatch_timeout
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.roundrobin import RoundRobinLoadBalancer
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.base import ChannelOptions
from wool.runtime.worker.base import WorkerOptions
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.pool import WorkerPool

from . import routines
from .routines import ContextVarPattern


class RoutineShape(Enum):
    COROUTINE = auto()
    ASYNC_GEN_ANEXT = auto()
    ASYNC_GEN_ANEXT_SINGLE = auto()
    ASYNC_GEN_ASEND = auto()
    ASYNC_GEN_ATHROW = auto()
    ASYNC_GEN_ACLOSE = auto()
    NESTED_COROUTINE = auto()
    NESTED_ASYNC_GEN = auto()
    NESTED_ASYNC_GEN_READBACK = auto()


class PoolMode(Enum):
    DEFAULT = auto()
    EPHEMERAL = auto()
    DURABLE = auto()
    DURABLE_JOINED = auto()
    DURABLE_SHARED = auto()
    HYBRID = auto()
    NESTED_DEFAULT_IN_EPHEMERAL = auto()
    NESTED_EPHEMERAL_IN_EPHEMERAL = auto()


class DiscoveryFactory(Enum):
    NONE = auto()
    LOCAL_DIRECT = auto()
    LOCAL_CALLABLE = auto()
    LOCAL_SYNC_CM = auto()
    LOCAL_ASYNC_CM = auto()
    LAN_DIRECT = auto()
    LAN_CALLABLE = auto()
    LAN_ASYNC_CM = auto()


class LbFactory(Enum):
    CLASS_REF = auto()
    INSTANCE = auto()
    CALLABLE = auto()
    ASYNC_CM = auto()


class CredentialType(Enum):
    INSECURE = auto()
    MTLS = auto()
    ONE_WAY = auto()


class WorkerOptionsKind(Enum):
    DEFAULT = auto()
    RESTRICTIVE = auto()
    KEEPALIVE = auto()


class TimeoutKind(Enum):
    NONE = auto()
    VIA_DISPATCH_TIMEOUT_VAR = auto()


class RoutineBinding(Enum):
    MODULE_FUNCTION = auto()
    INSTANCE_METHOD = auto()
    CLASSMETHOD = auto()
    STATICMETHOD = auto()


class BackpressureMode(Enum):
    NONE = auto()
    SYNC = auto()
    ASYNC = auto()


class LazyMode(Enum):
    LAZY = auto()
    EAGER = auto()


class QuorumMode(Enum):
    DEFAULT = auto()
    ABOVE_DEFAULT = auto()


class StrictWarnings(Enum):
    """Documents whether warnings are promoted to errors during dispatch.

    ``OFF`` leaves the ambient warning filter untouched. ``ALL_DECODABLE``
    promotes :class:`wool.SerializationWarning` to an error for the
    duration of the dispatch and ships caller vars whose values can be
    cleanly decoded on the worker — under strict mode the dispatch is
    expected to complete without raising, proving the happy-path
    propagation pipeline is silent on the warning channel.
    """

    OFF = auto()
    ALL_DECODABLE = auto()


# Optional Scenario dimension — defaulted to ``None`` and excluded
# from :attr:`Scenario.is_complete` so the existing pairwise covering
# array (which leaves it unset) remains valid. It is populated only
# on explicitly-enumerated scenarios (e.g. ``test_unified_driver``)
# to document observable annotations.
_OPTIONAL_DIMENSIONS: frozenset[str] = frozenset({"strict_warnings"})


def _sync_accept_hook(ctx):
    """Sync backpressure hook that accepts all tasks."""
    return False


async def _async_accept_hook(ctx):
    """Async backpressure hook that accepts all tasks."""
    return False


@dataclass(frozen=True)
class Scenario:
    """Composable scenario describing one integration test configuration.

    Each field corresponds to one dimension. Partial scenarios (some fields
    ``None``) can be merged with ``|`` to build complete configurations.
    """

    shape: RoutineShape | None = None
    pool_mode: PoolMode | None = None
    discovery: DiscoveryFactory | None = None
    lb: LbFactory | None = None
    credential: CredentialType | None = None
    options: WorkerOptionsKind | None = None
    timeout: TimeoutKind | None = None
    binding: RoutineBinding | None = None
    lazy: LazyMode | None = None
    backpressure: BackpressureMode | None = None
    ctx_var_1: ContextVarPattern | None = None
    ctx_var_2: ContextVarPattern | None = None
    ctx_var_3: ContextVarPattern | None = None
    quorum: QuorumMode | None = None
    # NOTE: ``strict_warnings`` is listed in ``_OPTIONAL_DIMENSIONS``
    # and serves as a documentation annotation on test IDs — it does
    # not drive ``build_pool_from_scenario`` behavior today. Tests
    # that vary along this axis set it explicitly so the pytest ID
    # reflects the warning regime under exercise.
    strict_warnings: StrictWarnings | None = None

    def __or__(self, other: Scenario) -> Scenario:
        """Merge two partial scenarios. Right side wins on ``None`` fields.

        Raises ValueError if both sides set the same field to different
        non-None values.
        """
        kwargs = {}
        for f in fields(self):
            left = getattr(self, f.name)
            right = getattr(other, f.name)
            if left is not None and right is not None and left != right:
                raise ValueError(
                    f"Conflicting values for {f.name}: {left!r} vs {right!r}"
                )
            kwargs[f.name] = right if right is not None else left
        return Scenario(**kwargs)

    @property
    def is_complete(self) -> bool:
        """True when all required (non-optional) dimensions are set.

        The optional documentation field listed in
        ``_OPTIONAL_DIMENSIONS`` (``strict_warnings``) is excluded from
        the completeness check — it describes an observable annotation
        on a scenario rather than required configuration. Pairwise rows
        generate with it at ``None``.
        """
        return all(
            getattr(self, f.name) is not None
            for f in fields(self)
            if f.name not in _OPTIONAL_DIMENSIONS
        )

    def __str__(self) -> str:
        parts = []
        for f in fields(self):
            val = getattr(self, f.name)
            if val is not None:
                parts.append(val.name)
            elif f.name in _OPTIONAL_DIMENSIONS:
                # Optional dimensions are omitted from the ID when
                # unset so existing pairwise IDs remain stable.
                continue
            else:
                parts.append("_")
        return "-".join(parts)


def default_scenario(
    *,
    shape: RoutineShape = RoutineShape.COROUTINE,
    pool_mode: PoolMode = PoolMode.DEFAULT,
    binding: RoutineBinding = RoutineBinding.MODULE_FUNCTION,
    backpressure: BackpressureMode = BackpressureMode.NONE,
    lazy: LazyMode = LazyMode.LAZY,
    ctx_var_1: ContextVarPattern = ContextVarPattern.NONE,
    ctx_var_2: ContextVarPattern = ContextVarPattern.NONE,
    ctx_var_3: ContextVarPattern = ContextVarPattern.NONE,
    quorum: QuorumMode = QuorumMode.DEFAULT,
    timeout: TimeoutKind = TimeoutKind.NONE,
    strict_warnings: StrictWarnings | None = None,
) -> Scenario:
    """Build a fully-populated :class:`Scenario` with sensible defaults.

    Used by happy-path integration tests that want to vary only one or two
    dimensions while leaving the rest at their canonical values. The
    optional documentation field ``strict_warnings`` defaults to
    ``None`` so it remains absent from the pytest ID unless explicitly set.
    """
    return Scenario(
        shape=shape,
        pool_mode=pool_mode,
        discovery=DiscoveryFactory.NONE,
        lb=LbFactory.CLASS_REF,
        credential=CredentialType.INSECURE,
        options=WorkerOptionsKind.DEFAULT,
        timeout=timeout,
        binding=binding,
        lazy=lazy,
        backpressure=backpressure,
        ctx_var_1=ctx_var_1,
        ctx_var_2=ctx_var_2,
        ctx_var_3=ctx_var_3,
        quorum=quorum,
        strict_warnings=strict_warnings,
    )


class _DirectDiscovery:
    """Wraps an already-entered discovery service as a plain object.

    Does NOT implement ``__enter__``/``__exit__``/``__aenter__``/
    ``__aexit__``, forcing ``WorkerPool._enter_context`` to take the
    passthrough path. Used for the ``*_DIRECT`` factory form arrangements.
    """

    def __init__(self, discovery):
        self._discovery = discovery

    @property
    def publisher(self):
        return self._discovery.publisher

    @property
    def subscriber(self):
        return self._discovery.subscriber

    def subscribe(self, filter=None):
        return self._discovery.subscribe(filter)


# The return annotation is load-bearing for tooling, not just docs: pyright's
# inference gives up on this large unannotated async generator and models it
# as a plain coroutine, breaking ``async with`` analysis at every call site.
@asynccontextmanager
async def build_pool_from_scenario(
    scenario, credentials_map, *, backpressure=None
) -> AsyncIterator[WorkerPool]:
    """Build and enter a WorkerPool from a complete Scenario.

    Resolves each dimension to its concrete runtime value and yields the
    entered pool context.

    :param backpressure:
        Optional admission-control hook that overrides the hook the
        ``BackpressureMode`` dimension would otherwise resolve. Tests
        that need a bespoke (e.g. context-var-aware) hook pass it here
        rather than building a :class:`WorkerPool` by hand.
    """
    missing = [
        f.name
        for f in fields(scenario)
        if f.name not in _OPTIONAL_DIMENSIONS and getattr(scenario, f.name) is None
    ]
    if missing:
        raise ValueError(f"Scenario incomplete; missing required dimensions: {missing}")

    creds = credentials_map[scenario.credential]

    if scenario.options is WorkerOptionsKind.RESTRICTIVE:
        options = WorkerOptions(
            channel=ChannelOptions(
                max_receive_message_length=64 * 1024,
                max_send_message_length=64 * 1024,
            ),
        )
    elif scenario.options is WorkerOptionsKind.KEEPALIVE:
        options = WorkerOptions(
            channel=ChannelOptions(
                keepalive_time_ms=10000,
                keepalive_timeout_ms=5000,
                keepalive_permit_without_calls=True,
            ),
            http2_min_recv_ping_interval_without_data_ms=5000,
        )
    else:
        options = WorkerOptions()

    lb: object
    match scenario.lb:
        case LbFactory.CLASS_REF:
            lb = RoundRobinLoadBalancer
        case LbFactory.INSTANCE:
            lb = RoundRobinLoadBalancer()
        case LbFactory.CALLABLE:
            lb = lambda: RoundRobinLoadBalancer()  # noqa: E731
        case LbFactory.ASYNC_CM:

            @asynccontextmanager
            async def _lb_cm():
                yield RoundRobinLoadBalancer()

            lb = _lb_cm()

    discovery_obj = None
    _local_cm = None

    if (
        scenario.discovery is not DiscoveryFactory.NONE
        and scenario.pool_mode is not PoolMode.DURABLE_JOINED
    ):
        namespace = f"integration-{uuid.uuid4().hex[:12]}"

        match scenario.discovery:
            case DiscoveryFactory.LOCAL_SYNC_CM:
                discovery_obj = LocalDiscovery(namespace)
            case DiscoveryFactory.LOCAL_CALLABLE:
                discovery_obj = lambda: LocalDiscovery(namespace)  # noqa: E731
            case DiscoveryFactory.LOCAL_DIRECT:
                _local_cm = LocalDiscovery(namespace)
                _local_cm.__enter__()
                discovery_obj = _DirectDiscovery(_local_cm)
            case DiscoveryFactory.LOCAL_ASYNC_CM:

                @asynccontextmanager
                async def _local_async_cm():
                    with LocalDiscovery(namespace) as d:
                        yield d

                discovery_obj = _local_async_cm()

            case DiscoveryFactory.LAN_DIRECT:
                from wool.runtime.discovery.lan import LanDiscovery

                lan_ns = f"integration-lan-{uuid.uuid4().hex[:12]}"
                discovery_obj = LanDiscovery(lan_ns)
            case DiscoveryFactory.LAN_CALLABLE:
                from wool.runtime.discovery.lan import LanDiscovery

                lan_ns = f"integration-lan-{uuid.uuid4().hex[:12]}"
                discovery_obj = lambda: LanDiscovery(lan_ns)  # noqa: E731
            case DiscoveryFactory.LAN_ASYNC_CM:
                from wool.runtime.discovery.lan import LanDiscovery

                lan_ns = f"integration-lan-{uuid.uuid4().hex[:12]}"

                @asynccontextmanager
                async def _lan_async_cm():
                    discovery = LanDiscovery(lan_ns)
                    yield discovery

                discovery_obj = _lan_async_cm()

    dispatch_timeout_token = None
    if scenario.timeout is TimeoutKind.VIA_DISPATCH_TIMEOUT_VAR:
        dispatch_timeout_token = dispatch_timeout.set(30.0)

    lazy = scenario.lazy is LazyMode.LAZY

    if backpressure is not None:
        bp_hook = backpressure
    else:
        match scenario.backpressure:
            case BackpressureMode.SYNC:
                bp_hook = _sync_accept_hook
            case BackpressureMode.ASYNC:
                bp_hook = _async_accept_hook
            case _:
                bp_hook = None

    match scenario.quorum:
        case QuorumMode.ABOVE_DEFAULT:
            quorum = 2
        case _:
            quorum = 1

    try:
        try:
            if scenario.pool_mode is PoolMode.DURABLE:
                async with _durable_pool_context(
                    lb, creds, options, lazy, quorum, backpressure=bp_hook
                ) as pool:
                    yield pool
            elif scenario.pool_mode is PoolMode.DURABLE_SHARED:
                async with _durable_shared_pool_context(
                    lb, creds, options, lazy, quorum, backpressure=bp_hook
                ) as pool:
                    yield pool
            elif scenario.pool_mode is PoolMode.DURABLE_JOINED:
                async with _durable_joined_pool_context(
                    scenario.discovery,
                    lb,
                    creds,
                    options,
                    lazy,
                    quorum,
                    backpressure=bp_hook,
                ) as pool:
                    yield pool
            else:
                pool_kwargs = {
                    "loadbalancer": lb,
                    "credentials": creds,
                    # Explicitly loopback-pinned factory by design: the
                    # pre-supplied host classifies it bound, carrying the
                    # options/backpressure dimensions while keeping LAN
                    # scenarios deterministic (the test certs' SAN covers
                    # loopback only). The publisher-prescribed bind host is
                    # covered by test_lan_publish.py instead.
                    "worker": partial(
                        LocalWorker,
                        host="127.0.0.1",
                        options=options,
                        backpressure=bp_hook,
                    ),
                    "lazy": lazy,
                    "quorum": quorum,
                }
                match scenario.pool_mode:
                    case PoolMode.DEFAULT:
                        pool_kwargs["size"] = 1
                    case PoolMode.EPHEMERAL:
                        pool_kwargs["size"] = 2
                    case PoolMode.HYBRID:
                        pool_kwargs["size"] = 1
                        pool_kwargs["discovery"] = discovery_obj
                    case PoolMode.NESTED_DEFAULT_IN_EPHEMERAL:
                        pool_kwargs["size"] = 1
                    case PoolMode.NESTED_EPHEMERAL_IN_EPHEMERAL:
                        pool_kwargs["size"] = 1

                pool = WorkerPool(**pool_kwargs)
                async with pool:
                    if scenario.pool_mode in (
                        PoolMode.NESTED_DEFAULT_IN_EPHEMERAL,
                        PoolMode.NESTED_EPHEMERAL_IN_EPHEMERAL,
                    ):
                        # Nested pool modes verify that entering a second
                        # WorkerPool context doesn't break the outer pool.
                        # Dispatch still goes through the outer pool.
                        nested_size = (
                            2
                            if scenario.pool_mode
                            is PoolMode.NESTED_EPHEMERAL_IN_EPHEMERAL
                            else 1
                        )
                        nested_pool = WorkerPool(
                            size=nested_size,
                            credentials=creds,
                            worker=partial(LocalWorker, options=options),
                        )
                        async with nested_pool:
                            yield pool
                    else:
                        yield pool
        finally:
            if dispatch_timeout_token is not None:
                dispatch_timeout.reset(dispatch_timeout_token)
    finally:
        if _local_cm is not None:
            _local_cm.__exit__(None, None, None)


@asynccontextmanager
async def _durable_pool_context(lb, creds, options, lazy, quorum, *, backpressure=None):
    """Manually start a worker, register it, then create a DURABLE pool.

    DURABLE pools don't spawn workers — they only discover external
    ones. This helper starts a LocalWorker, registers it via a
    LocalDiscovery publisher, and creates a DURABLE WorkerPool that
    discovers it. Discovery is always LocalDiscovery (managed
    internally); the D3 dimension is constrained to NONE for DURABLE
    mode in the pairwise filter.
    """
    namespace = f"durable-{uuid.uuid4().hex[:12]}"
    with LocalDiscovery(namespace) as discovery:
        worker = LocalWorker(
            credentials=creds, options=options, backpressure=backpressure
        )
        await worker.start()
        try:
            publisher = discovery.publisher
            async with publisher:
                await publisher.publish("worker-added", worker.metadata)
                try:
                    pool = WorkerPool(
                        discovery=_DirectDiscovery(discovery),
                        loadbalancer=lb,
                        credentials=creds,
                        lazy=lazy,
                        quorum=quorum,
                    )
                    async with pool:
                        yield pool
                finally:
                    await publisher.publish("worker-dropped", worker.metadata)
        finally:
            await worker.stop()


@asynccontextmanager
async def _durable_shared_pool_context(
    lb, creds, options, lazy, quorum, *, backpressure=None
):
    """Create two pools sharing the same LocalDiscovery subscriber.

    Exercises ``SubscriberMeta`` singleton caching and
    ``_SharedSubscription`` fan-out: both pools call
    ``Subscriber(namespace)`` through the metaclass, the second
    hits the cache and gets a separate ``_SharedSubscription``
    backed by the same raw subscriber and source iterator.
    """
    namespace = f"shared-{uuid.uuid4().hex[:12]}"
    with LocalDiscovery(namespace) as discovery:
        worker = LocalWorker(
            credentials=creds, options=options, backpressure=backpressure
        )
        await worker.start()
        try:
            publisher = discovery.publisher
            async with publisher:
                await publisher.publish("worker-added", worker.metadata)
                try:
                    shared = _DirectDiscovery(discovery)
                    pool_a = WorkerPool(
                        discovery=shared,
                        loadbalancer=lb,
                        credentials=creds,
                        lazy=lazy,
                        quorum=quorum,
                    )
                    pool_b = WorkerPool(
                        discovery=shared,
                        loadbalancer=lb,
                        credentials=creds,
                        lazy=lazy,
                        quorum=quorum,
                    )
                    async with pool_a:
                        async with pool_b:
                            yield pool_a
                finally:
                    await publisher.publish("worker-dropped", worker.metadata)
        finally:
            await worker.stop()


_LOCAL_FACTORIES = (
    DiscoveryFactory.LOCAL_DIRECT,
    DiscoveryFactory.LOCAL_CALLABLE,
    DiscoveryFactory.LOCAL_SYNC_CM,
    DiscoveryFactory.LOCAL_ASYNC_CM,
)


def _resolve_joiner(namespace, factory):
    """Resolve a DiscoveryFactory into a joiner discovery object.

    Uses the given namespace (same as the owner), triggering the non-owner
    fallback path in ``LocalDiscovery.__enter__``.

    Returns ``(discovery_obj, entered_cm_or_None)``. The caller must
    exit the CM (if non-None) when done.
    """
    match factory:
        case DiscoveryFactory.LOCAL_DIRECT:
            cm = LocalDiscovery(namespace)
            cm.__enter__()
            return _DirectDiscovery(cm), cm
        case DiscoveryFactory.LOCAL_CALLABLE:
            return (lambda: LocalDiscovery(namespace)), None  # noqa: E731
        case DiscoveryFactory.LOCAL_SYNC_CM:
            return LocalDiscovery(namespace), None
        case DiscoveryFactory.LOCAL_ASYNC_CM:

            @asynccontextmanager
            async def _acm():
                with LocalDiscovery(namespace) as d:
                    yield d

            return _acm(), None
        case _:
            raise ValueError(f"Unsupported factory for joiner: {factory}")


@asynccontextmanager
async def _durable_joined_pool_context(
    discovery_factory, lb, creds, options, lazy, quorum, *, backpressure=None
):
    """Create a DURABLE pool that joins an externally owned namespace.

    Sets up an owner ``LocalDiscovery`` that creates workers and publishes
    them, then resolves a joiner discovery from the D3 factory form. The
    joiner reuses the owner's namespace, exercising the non-owner fallback
    path in ``LocalDiscovery.__enter__``.
    """
    namespace = f"joined-{uuid.uuid4().hex[:12]}"

    worker = LocalWorker(credentials=creds, options=options, backpressure=backpressure)
    await worker.start()
    try:
        owner = LocalDiscovery(namespace)
        owner.__enter__()
        try:
            publisher = owner.publisher
            async with publisher:
                await publisher.publish("worker-added", worker.metadata)
                joiner, _joiner_cm = _resolve_joiner(namespace, discovery_factory)
                try:
                    pool = WorkerPool(
                        discovery=joiner,
                        loadbalancer=lb,
                        credentials=creds,
                        lazy=lazy,
                        quorum=quorum,
                    )
                    async with pool:
                        yield pool
                finally:
                    if _joiner_cm is not None:
                        _joiner_cm.__exit__(None, None, None)
                    await publisher.publish("worker-dropped", worker.metadata)
        finally:
            owner.__exit__(None, None, None)
    finally:
        await worker.stop()


_VAR_NAMES = ("tenant_id", "region", "trace_id")
_CALLER_VARS = {
    "tenant_id": routines.TENANT_ID,
    "region": routines.REGION,
    "trace_id": routines.TRACE_ID,
}


def _build_patterns_dict(scenario):
    """Build a patterns dict from the scenario's ctx_var fields.

    Maps var names to ContextVarPattern members for non-NONE
    patterns. Returns an empty dict when all three patterns are
    NONE.
    """
    result: dict[str, ContextVarPattern] = {}
    for idx, var_name in enumerate(_VAR_NAMES):
        pattern = getattr(scenario, f"ctx_var_{idx + 1}")
        if pattern is not None and pattern is not ContextVarPattern.NONE:
            result[var_name] = pattern
    return result


def _setup_caller_vars(patterns):
    """Set caller-side initial values for patterns that need them.

    Returns a dict of {var_name: token} for cleanup, a dict of
    {var_name: initial_value} for later assertion, and the
    _RESET_TOKENS reset token (or None) minted when a
    TOKEN_REMOTE_RESET_THEN_FRESH_SET pattern deposits caller-minted
    tokens for the worker to consume.
    """
    tokens = {}
    initial_values = {}
    ferry = {}
    for var_name, pattern in patterns.items():
        var = _CALLER_VARS[var_name]
        if pattern is not ContextVarPattern.NONE:
            initial = f"caller-initial-{var_name}"
            tokens[var_name] = var.set(initial)
            initial_values[var_name] = initial
        if pattern is ContextVarPattern.TOKEN_REMOTE_RESET_THEN_FRESH_SET:
            # Mint the token the worker will consume; it rides to the
            # worker nested inside _RESET_TOKENS' propagated value.
            ferry[var_name] = var.set(f"remote-reset-{var_name}")
    ferry_token = routines._RESET_TOKENS.set(ferry) if ferry else None
    return tokens, initial_values, ferry_token


def _assert_caller_vars(patterns, initial_values, *, shape=None):
    """Assert caller-side var state after dispatch completes.

    Nested patterns (DOWNSTREAM_OVERWRITE, DOWNSTREAM_RESET,
    UPSTREAM_RESET) assert the outer worker's final state, which is
    what the caller observes via back-propagation. The inner worker's
    mutations do not reach the outer worker's copied context because
    the nested dispatch crosses event loop boundaries; only the outer
    worker's own writes are captured in its context.

    For NESTED_ASYNC_GEN shapes, the async generator's final context
    is sent with the last yield, not after exhaustion.
    Post-teardown mutations (UPSTREAM_RESET) are not visible to the
    caller because there is no subsequent yield to carry them. For
    DOWNSTREAM_OVERWRITE and DOWNSTREAM_RESET the outer worker's
    ``_pre_nested_setup`` runs before the first inner yield, so those
    values ARE captured in per-yield contexts.
    """
    is_nested_gen = shape is RoutineShape.NESTED_ASYNC_GEN
    for var_name, pattern in patterns.items():
        var = _CALLER_VARS[var_name]
        match pattern:
            case ContextVarPattern.ROUND_TRIP:
                assert var.get() == f"worker-mutated-{var_name}", (
                    f"ROUND_TRIP: expected caller {var_name} to reflect "
                    f"worker mutation, got {var.get()!r}"
                )
            case ContextVarPattern.LOCAL_RESET:
                assert var.get() == initial_values[var_name], (
                    f"LOCAL_RESET: expected caller {var_name} to be "
                    f"unchanged at {initial_values[var_name]!r}, "
                    f"got {var.get()!r}"
                )
            case ContextVarPattern.DOWNSTREAM_OVERWRITE:
                # Under stdlib-mirror semantics, wool routines run in
                # the caller's stdlib Context — outer sets, inner
                # overwrites in the same Chain, and back-prop
                # carries the final state (inner's overwrite) to the
                # caller. Matches `await coro()` semantics.
                assert var.get() == f"inner-overwrite-{var_name}", (
                    f"DOWNSTREAM_OVERWRITE: expected inner-overwrite value, "
                    f"got {var.get()!r}"
                )
            case ContextVarPattern.DOWNSTREAM_RESET:
                # Outer sets and passes token; inner resets using the
                # token, restoring the pre-outer value. Caller sees
                # its own initial value.
                assert var.get() == initial_values[var_name], (
                    f"DOWNSTREAM_RESET: expected caller's initial value "
                    f"{initial_values[var_name]!r}, got {var.get()!r}"
                )
            case ContextVarPattern.UPSTREAM_RESET:
                if is_nested_gen:
                    # Async gen: inner sets "inner-set-" before
                    # yielding; that value is captured in a
                    # per-yield context and back-propagated to the
                    # caller. _post_nested_teardown runs after the
                    # gen exhausts — no subsequent yield carries
                    # its mutation — so the caller's final visible
                    # state is inner's set.
                    assert var.get() == f"inner-set-{var_name}", (
                        f"UPSTREAM_RESET (async gen): expected "
                        f"inner-set value, got {var.get()!r}"
                    )
                else:
                    # Coroutine: inner sets, then _post_nested_teardown
                    # overwrites with outer-reset before the response
                    # context ships.
                    assert var.get() == f"outer-reset-{var_name}", (
                        f"UPSTREAM_RESET: expected outer-reset value, got {var.get()!r}"
                    )
            case ContextVarPattern.PER_YIELD:
                # After iteration, caller should see the last
                # step value back-propagated.
                pass  # validated inline during iteration
            case ContextVarPattern.MID_STREAM_FORWARD:
                # Forward propagation is asserted worker-side per
                # frame — a mismatch raises a dispatch exception.
                pass  # validated worker-side during iteration
            case ContextVarPattern.TOKEN_REMOTE_RESET_THEN_FRESH_SET:
                # The worker consumed the ferried token (restoring the
                # caller's initial value) then re-set the var; the
                # fresh set back-propagates home.
                assert var.get() == f"fresh-{var_name}", (
                    f"TOKEN_REMOTE_RESET_THEN_FRESH_SET: expected the "
                    f"worker's fresh value, got {var.get()!r}"
                )
                # The regression this pattern pins: the worker's fresh
                # set must not have erased the consumed id from the
                # spent ledger, so the caller's stale copy still
                # raises. Read the ferry dict back from the var — the
                # post-merge copy carries the same token id either way.
                stale = routines._RESET_TOKENS.get()[var_name]
                with pytest.raises(RuntimeError, match="has already been used once"):
                    var.reset(stale)


def _cleanup_caller_vars(tokens):
    """Reset caller-side vars using saved tokens."""
    for var_name, token in tokens.items():
        _CALLER_VARS[var_name].reset(token)


async def invoke_routine(scenario):
    """Invoke the appropriate routine for the given scenario and return results."""
    binding = scenario.binding
    shape = scenario.shape

    patterns = _build_patterns_dict(scenario)

    # Set up TEST_PATTERNS so the worker decorator picks them up.
    patterns_token = None
    caller_tokens = {}
    initial_values = {}
    ferry_token = None
    if patterns:
        patterns_token = routines.TEST_PATTERNS.set(patterns)
        caller_tokens, initial_values, ferry_token = _setup_caller_vars(patterns)

    try:
        obj = (
            routines.Routines()
            if binding is not RoutineBinding.MODULE_FUNCTION
            else None
        )

        routine = _select_routine(shape, binding)

        match shape:
            case RoutineShape.COROUTINE:
                if binding is RoutineBinding.INSTANCE_METHOD:
                    result = await routine(obj, 1, 2)
                else:
                    result = await routine(1, 2)
                assert result == 3
                if patterns:
                    _assert_caller_vars(patterns, initial_values, shape=shape)
                return result

            case RoutineShape.ASYNC_GEN_ANEXT:
                collected = []
                if binding is RoutineBinding.INSTANCE_METHOD:
                    gen = routine(obj, 3)
                else:
                    gen = routine(3)
                forward = {
                    k: v
                    for k, v in patterns.items()
                    if v is ContextVarPattern.MID_STREAM_FORWARD
                }
                # MID_STREAM_FORWARD drives the generator one step at a
                # time, mutating the var to a per-step value before
                # each ``__anext__`` so the worker frame observes the
                # forward-propagated value.
                if forward:
                    step = 0
                    while True:
                        for var_name in forward:
                            _CALLER_VARS[var_name].set(f"step-{step}")
                        try:
                            item = await gen.__anext__()
                        except StopAsyncIteration:
                            break
                        collected.append(item)
                        step += 1
                    assert collected == [0, 1, 2]
                    if patterns:
                        _assert_caller_vars(patterns, initial_values, shape=shape)
                    return collected
                step = 0
                async for item in gen:
                    collected.append(item)
                    if patterns:
                        per_yield = {
                            k: v
                            for k, v in patterns.items()
                            if v is ContextVarPattern.PER_YIELD
                        }
                        for var_name in per_yield:
                            var = _CALLER_VARS[var_name]
                            assert var.get() == f"step-{step}", (
                                f"PER_YIELD step {step}: expected "
                                f"'step-{step}', got {var.get()!r}"
                            )
                    step += 1
                assert collected == [0, 1, 2]
                if patterns:
                    _assert_caller_vars(patterns, initial_values, shape=shape)
                return collected

            case RoutineShape.ASYNC_GEN_ANEXT_SINGLE:
                collected = []
                gen = routine()
                async for item in gen:
                    collected.append(item)
                assert collected == [0]
                if patterns:
                    _assert_caller_vars(patterns, initial_values, shape=shape)
                return collected

            case RoutineShape.ASYNC_GEN_ASEND:
                if binding is RoutineBinding.INSTANCE_METHOD:
                    gen = routine(obj, 2)
                else:
                    gen = routine(2)
                first = await gen.__anext__()
                assert first == "ready"
                echoed = await gen.asend(42)
                assert echoed == 42
                await gen.aclose()
                if patterns:
                    _assert_caller_vars(patterns, initial_values, shape=shape)
                return echoed

            case RoutineShape.ASYNC_GEN_ATHROW:
                if binding is RoutineBinding.INSTANCE_METHOD:
                    gen = routine(obj, 10)
                else:
                    gen = routine(10)
                first = await gen.__anext__()
                assert first == 10
                reset = await gen.athrow(ValueError)
                assert reset == 0
                await gen.aclose()
                if patterns:
                    _assert_caller_vars(patterns, initial_values, shape=shape)
                return reset

            case RoutineShape.ASYNC_GEN_ACLOSE:
                if binding is RoutineBinding.INSTANCE_METHOD:
                    gen = routine(obj)
                else:
                    gen = routine()
                first = await gen.__anext__()
                assert first == "alive"
                await gen.aclose()
                if patterns:
                    _assert_caller_vars(patterns, initial_values, shape=shape)
                return first

            case RoutineShape.NESTED_COROUTINE:
                result = await routines.nested_add(1, 2)
                assert result == 3
                if patterns:
                    _assert_caller_vars(patterns, initial_values, shape=shape)
                return result

            case RoutineShape.NESTED_ASYNC_GEN:
                collected = []
                step = 0
                async for item in routines.nested_gen(3):
                    collected.append(item)
                    if patterns:
                        per_yield = {
                            k: v
                            for k, v in patterns.items()
                            if v is ContextVarPattern.PER_YIELD
                        }
                        for var_name in per_yield:
                            var = _CALLER_VARS[var_name]
                            assert var.get() == f"step-{step}", (
                                f"PER_YIELD step {step}: expected "
                                f"'step-{step}', got {var.get()!r}"
                            )
                    step += 1
                assert collected == [0, 1, 2]
                if patterns:
                    _assert_caller_vars(patterns, initial_values, shape=shape)
                return collected

            case RoutineShape.NESTED_ASYNC_GEN_READBACK:
                # Streaming routine that, on each iteration, mutates
                # ``TENANT_ID`` to a per-step value and nested-dispatches
                # ``get_tenant_id`` to read it back. Locks in the second
                # consequence of the #176 fix: ``_current_task`` and
                # ``wool.Chain`` remain active across the generator's
                # lifespan, so nested dispatches from inside a streaming
                # routine carry the streaming task as caller.
                collected = [
                    value async for value in routines.streaming_nested_get_tenant_id(3)
                ]
                assert collected == ["step-0", "step-1", "step-2"]
                return collected
    finally:
        if caller_tokens:
            _cleanup_caller_vars(caller_tokens)
        if ferry_token is not None:
            routines._RESET_TOKENS.reset(ferry_token)
        if patterns_token is not None:
            routines.TEST_PATTERNS.reset(patterns_token)


def _select_routine(shape, binding):
    """Return the routine callable for the given shape and binding."""
    match (shape, binding):
        case (RoutineShape.COROUTINE, RoutineBinding.MODULE_FUNCTION):
            return routines.add
        case (RoutineShape.COROUTINE, RoutineBinding.INSTANCE_METHOD):
            return routines.Routines.instance_add
        case (RoutineShape.COROUTINE, RoutineBinding.CLASSMETHOD):
            return routines.Routines.class_add
        case (RoutineShape.COROUTINE, RoutineBinding.STATICMETHOD):
            return routines.Routines.static_add

        case (RoutineShape.ASYNC_GEN_ANEXT, RoutineBinding.MODULE_FUNCTION):
            return routines.gen_range
        case (RoutineShape.ASYNC_GEN_ANEXT, RoutineBinding.INSTANCE_METHOD):
            return routines.Routines.instance_gen
        case (RoutineShape.ASYNC_GEN_ANEXT, RoutineBinding.CLASSMETHOD):
            return routines.Routines.class_gen
        case (RoutineShape.ASYNC_GEN_ANEXT, RoutineBinding.STATICMETHOD):
            return routines.Routines.static_gen

        case (RoutineShape.ASYNC_GEN_ANEXT_SINGLE, RoutineBinding.MODULE_FUNCTION):
            return routines.gen_range_one_yield

        case (RoutineShape.ASYNC_GEN_ASEND, RoutineBinding.MODULE_FUNCTION):
            return routines.echo_send
        case (RoutineShape.ASYNC_GEN_ASEND, RoutineBinding.INSTANCE_METHOD):
            return routines.Routines.instance_echo_send
        case (RoutineShape.ASYNC_GEN_ASEND, _):
            return routines.echo_send

        case (RoutineShape.ASYNC_GEN_ATHROW, RoutineBinding.MODULE_FUNCTION):
            return routines.resilient_counter
        case (RoutineShape.ASYNC_GEN_ATHROW, RoutineBinding.INSTANCE_METHOD):
            return routines.Routines.instance_resilient_counter
        case (RoutineShape.ASYNC_GEN_ATHROW, _):
            return routines.resilient_counter

        case (RoutineShape.ASYNC_GEN_ACLOSE, RoutineBinding.MODULE_FUNCTION):
            return routines.closeable_gen
        case (RoutineShape.ASYNC_GEN_ACLOSE, RoutineBinding.INSTANCE_METHOD):
            return routines.Routines.instance_closeable_gen
        case (RoutineShape.ASYNC_GEN_ACLOSE, _):
            return routines.closeable_gen

        case (RoutineShape.NESTED_COROUTINE, _):
            return routines.nested_add
        case (RoutineShape.NESTED_ASYNC_GEN, _):
            return routines.nested_gen
        case (RoutineShape.NESTED_ASYNC_GEN_READBACK, _):
            return routines.streaming_nested_get_tenant_id

        case _:
            raise ValueError(f"Unsupported shape/binding: {shape}, {binding}")


_ASEND_ATHROW_ACLOSE = (
    RoutineShape.ASYNC_GEN_ASEND,
    RoutineShape.ASYNC_GEN_ATHROW,
    RoutineShape.ASYNC_GEN_ACLOSE,
)
_NESTED_SHAPES = (
    RoutineShape.NESTED_COROUTINE,
    RoutineShape.NESTED_ASYNC_GEN,
    RoutineShape.NESTED_ASYNC_GEN_READBACK,
)
_ASYNC_GEN_SHAPES = (
    RoutineShape.ASYNC_GEN_ANEXT,
    RoutineShape.ASYNC_GEN_ANEXT_SINGLE,
    RoutineShape.ASYNC_GEN_ASEND,
    RoutineShape.ASYNC_GEN_ATHROW,
    RoutineShape.ASYNC_GEN_ACLOSE,
    RoutineShape.NESTED_ASYNC_GEN,
    RoutineShape.NESTED_ASYNC_GEN_READBACK,
)
_NESTED_ONLY_PATTERNS = (
    ContextVarPattern.DOWNSTREAM_OVERWRITE,
    ContextVarPattern.DOWNSTREAM_RESET,
    ContextVarPattern.UPSTREAM_RESET,
)
# MID_STREAM_FORWARD requires the caller to mutate the var before each
# ``__anext__`` and the worker to assert the forward-propagated value
# per frame. Only the plain ``async for`` shape drives the generator
# one step at a time through ``invoke_routine``; the asend/athrow/
# aclose scripts and the single-yield shape do not, so the pattern is
# constrained to ASYNC_GEN_ANEXT.
_MID_STREAM_FORWARD_SHAPES = (RoutineShape.ASYNC_GEN_ANEXT,)


def _is_grpc_internal(exc: BaseException) -> bool:
    return isinstance(exc, grpc.RpcError) and exc.code() == grpc.StatusCode.INTERNAL


_GRPC_INTERNAL_RETRIES = 3
_GRPC_INTERNAL_BACKOFF = 0.5


def _pairwise_filter(row):
    """Filter invalid dimension combinations.

    - D3 must be NONE when D2 is DEFAULT, EPHEMERAL, DURABLE, or NESTED_*
      (DURABLE manages its own LocalDiscovery internally)
    - D3 must NOT be NONE when D2 is HYBRID or DURABLE_JOINED
    - D3 must be a LOCAL_* variant when D2 is DURABLE_JOINED
      (DURABLE_JOINED exercises LocalDiscovery's owner/non-owner join
      semantics, which LanDiscovery has no analogue for)
    - D4 must not be ASYNC_CM (pre-called async CM instances are not
      picklable inside WorkerProxy.__wool_reduce__; documented limitation,
      see #61)
    - D8 must be MODULE_FUNCTION or INSTANCE_METHOD when D1 is ASEND,
      ATHROW, or ACLOSE (no classmethod/staticmethod routines defined
      for these shapes)
    - D8 must be MODULE_FUNCTION when D1 is NESTED_* (nested dispatch
      always uses module-level routines)
    - D11/D12/D13 (ctx_var_1/2/3): DOWNSTREAM_OVERWRITE,
      DOWNSTREAM_RESET, UPSTREAM_RESET only valid with NESTED_* shapes;
      PER_YIELD and MID_STREAM_FORWARD only valid with ASYNC_GEN_*
      shapes; TOKEN_REMOTE_RESET_THEN_FRESH_SET only valid with
      non-nested shapes (on nested shapes non-nested patterns execute
      on BOTH outer and inner workers, and its ferried token is
      single-use)
    - D14 (quorum) ABOVE_DEFAULT (quorum=2) requires PoolMode.EPHEMERAL —
      every other pool mode in the builder produces only one worker, so
      quorum=2 would block forever.
    """
    if len(row) > 2:
        pool_mode = row[1]
        discovery = row[2]
        needs_discovery = pool_mode in (
            PoolMode.HYBRID,
            PoolMode.DURABLE_JOINED,
        )
        forbids_discovery = pool_mode in (
            PoolMode.DEFAULT,
            PoolMode.EPHEMERAL,
            PoolMode.DURABLE,
            PoolMode.DURABLE_SHARED,
            PoolMode.NESTED_DEFAULT_IN_EPHEMERAL,
            PoolMode.NESTED_EPHEMERAL_IN_EPHEMERAL,
        )
        if needs_discovery and discovery is DiscoveryFactory.NONE:
            return False
        if forbids_discovery and discovery is not DiscoveryFactory.NONE:
            return False
        if pool_mode is PoolMode.DURABLE_JOINED and discovery not in _LOCAL_FACTORIES:
            return False
    if len(row) > 3:
        lb = row[3]
        if lb is LbFactory.ASYNC_CM:
            return False
    if len(row) > 7:
        shape = row[0]
        binding = row[7]
        if shape in _ASEND_ATHROW_ACLOSE and binding in (
            RoutineBinding.CLASSMETHOD,
            RoutineBinding.STATICMETHOD,
        ):
            return False
        if shape in _NESTED_SHAPES and binding is not RoutineBinding.MODULE_FUNCTION:
            return False
        # ASYNC_GEN_ANEXT_SINGLE is only defined as a module-level
        # routine (no class/instance/static variants); pin the
        # binding so pairwise generation does not invent
        # combinations that ``_select_routine`` cannot resolve.
        if (
            shape is RoutineShape.ASYNC_GEN_ANEXT_SINGLE
            and binding is not RoutineBinding.MODULE_FUNCTION
        ):
            return False
    # Chain var pattern constraints (indices 10, 11, 12)
    shape = row[0]
    for idx in (10, 11, 12):
        if len(row) > idx:
            pattern = row[idx]
            if pattern in _NESTED_ONLY_PATTERNS and shape not in _NESTED_SHAPES:
                return False
            if pattern is ContextVarPattern.PER_YIELD and shape not in _ASYNC_GEN_SHAPES:
                return False
            if (
                pattern is ContextVarPattern.MID_STREAM_FORWARD
                and shape not in _MID_STREAM_FORWARD_SHAPES
            ):
                return False
            # TOKEN_REMOTE_RESET_THEN_FRESH_SET's ferried token is
            # single-use; on nested shapes non-nested patterns execute
            # on both the outer and inner workers, so the second reset
            # would raise mid-dispatch.
            if (
                pattern is ContextVarPattern.TOKEN_REMOTE_RESET_THEN_FRESH_SET
                and shape in _NESTED_SHAPES
            ):
                return False
            # NESTED_ASYNC_GEN_READBACK's routine self-mutates
            # ``TENANT_ID`` per iteration; combining with framework-
            # driven patterns over the same caller vars would conflict.
            if (
                shape is RoutineShape.NESTED_ASYNC_GEN_READBACK
                and pattern is not ContextVarPattern.NONE
            ):
                return False
    if len(row) > 13:
        quorum = row[13]
        pool_mode = row[1]
        if quorum is QuorumMode.ABOVE_DEFAULT and pool_mode is not PoolMode.EPHEMERAL:
            return False
    return True


PAIRWISE_SCENARIOS = [
    Scenario(
        shape=row[0],
        pool_mode=row[1],
        discovery=row[2],
        lb=row[3],
        credential=row[4],
        options=row[5],
        timeout=row[6],
        binding=row[7],
        lazy=row[8],
        backpressure=row[9],
        ctx_var_1=row[10],
        ctx_var_2=row[11],
        ctx_var_3=row[12],
        quorum=row[13],
    )
    for row in AllPairs(
        [
            list(RoutineShape),
            list(PoolMode),
            list(DiscoveryFactory),
            list(LbFactory),
            list(CredentialType),
            list(WorkerOptionsKind),
            list(TimeoutKind),
            list(RoutineBinding),
            list(LazyMode),
            list(BackpressureMode),
            list(ContextVarPattern),
            list(ContextVarPattern),
            list(ContextVarPattern),
            list(QuorumMode),
        ],
        filter_func=_pairwise_filter,
    )
]

# allpairspy's greedy placement never selects LAN_ASYNC_CM under the
# current filter constraints (LAN factories pair only with HYBRID), so
# the generated array leaves the member entirely unexercised. Pin one
# canonical scenario to guarantee deterministic coverage; the guard
# test in test_integration.py fails loudly if another DiscoveryFactory
# member ever drops out of the array.
PAIRWISE_SCENARIOS.append(
    Scenario(
        shape=RoutineShape.COROUTINE,
        pool_mode=PoolMode.HYBRID,
        discovery=DiscoveryFactory.LAN_ASYNC_CM,
        lb=LbFactory.CLASS_REF,
        credential=CredentialType.INSECURE,
        options=WorkerOptionsKind.DEFAULT,
        timeout=TimeoutKind.NONE,
        binding=RoutineBinding.MODULE_FUNCTION,
        lazy=LazyMode.LAZY,
        backpressure=BackpressureMode.NONE,
        ctx_var_1=ContextVarPattern.NONE,
        ctx_var_2=ContextVarPattern.NONE,
        ctx_var_3=ContextVarPattern.NONE,
        quorum=QuorumMode.DEFAULT,
    )
)


@st.composite
def scenarios_strategy(draw):
    """Hypothesis composite strategy that draws valid Scenarios."""
    shape = draw(st.sampled_from(RoutineShape))
    pool_mode = draw(st.sampled_from(PoolMode))

    needs_discovery = pool_mode in (
        PoolMode.HYBRID,
        PoolMode.DURABLE_JOINED,
    )
    forbids_discovery = pool_mode in (
        PoolMode.DEFAULT,
        PoolMode.EPHEMERAL,
        PoolMode.DURABLE,
        PoolMode.DURABLE_SHARED,
        PoolMode.NESTED_DEFAULT_IN_EPHEMERAL,
        PoolMode.NESTED_EPHEMERAL_IN_EPHEMERAL,
    )

    if pool_mode is PoolMode.DURABLE_JOINED:
        discovery = draw(st.sampled_from(list(_LOCAL_FACTORIES)))
    elif needs_discovery:
        discovery = draw(
            st.sampled_from(
                [d for d in DiscoveryFactory if d is not DiscoveryFactory.NONE]
            )
        )
    elif forbids_discovery:
        discovery = DiscoveryFactory.NONE
    else:
        discovery = draw(st.sampled_from(DiscoveryFactory))

    # ASYNC_CM lb excluded: pre-called CM instances are not picklable
    # inside WorkerProxy.__wool_reduce__ (documented limitation, see #61)
    lb = draw(st.sampled_from([f for f in LbFactory if f is not LbFactory.ASYNC_CM]))
    credential = draw(st.sampled_from(CredentialType))
    options = draw(st.sampled_from(WorkerOptionsKind))
    timeout = draw(st.sampled_from(TimeoutKind))

    if shape in _NESTED_SHAPES:
        binding = RoutineBinding.MODULE_FUNCTION
    elif shape is RoutineShape.ASYNC_GEN_ANEXT_SINGLE:
        # Only a module-level routine exists for the single-yield
        # shape; mirrors the constraint encoded in ``_pairwise_filter``.
        binding = RoutineBinding.MODULE_FUNCTION
    elif shape in _ASEND_ATHROW_ACLOSE:
        binding = draw(
            st.sampled_from(
                [
                    RoutineBinding.MODULE_FUNCTION,
                    RoutineBinding.INSTANCE_METHOD,
                ]
            )
        )
    else:
        binding = draw(st.sampled_from(RoutineBinding))

    lazy = draw(st.sampled_from(LazyMode))
    backpressure = draw(st.sampled_from(BackpressureMode))

    def _draw_ctx_var_pattern(draw):
        # NESTED_ASYNC_GEN_READBACK's routine self-mutates TENANT_ID
        # per iteration; combining with framework-driven patterns over
        # the same caller vars would conflict. Mirrors the exclusion
        # in ``_pairwise_filter``.
        if shape is RoutineShape.NESTED_ASYNC_GEN_READBACK:
            return ContextVarPattern.NONE
        valid = list(ContextVarPattern)
        if shape not in _NESTED_SHAPES:
            valid = [p for p in valid if p not in _NESTED_ONLY_PATTERNS]
        if shape not in _ASYNC_GEN_SHAPES:
            valid = [p for p in valid if p is not ContextVarPattern.PER_YIELD]
        if shape not in _MID_STREAM_FORWARD_SHAPES:
            valid = [p for p in valid if p is not ContextVarPattern.MID_STREAM_FORWARD]
        if shape in _NESTED_SHAPES:
            # Mirrors ``_pairwise_filter``: the ferried token is
            # single-use, but nested shapes execute non-nested
            # patterns on both the outer and inner workers.
            valid = [
                p
                for p in valid
                if p is not ContextVarPattern.TOKEN_REMOTE_RESET_THEN_FRESH_SET
            ]
        return draw(st.sampled_from(valid))

    ctx_var_1 = _draw_ctx_var_pattern(draw)
    ctx_var_2 = _draw_ctx_var_pattern(draw)
    ctx_var_3 = _draw_ctx_var_pattern(draw)

    if pool_mode is PoolMode.EPHEMERAL:
        quorum = draw(st.sampled_from(QuorumMode))
    else:
        quorum = draw(
            st.sampled_from([m for m in QuorumMode if m is not QuorumMode.ABOVE_DEFAULT])
        )

    return Scenario(
        shape=shape,
        pool_mode=pool_mode,
        discovery=discovery,
        lb=lb,
        credential=credential,
        options=options,
        timeout=timeout,
        binding=binding,
        lazy=lazy,
        backpressure=backpressure,
        ctx_var_1=ctx_var_1,
        ctx_var_2=ctx_var_2,
        ctx_var_3=ctx_var_3,
        quorum=quorum,
    )


def _generate_test_certificates():
    """Generate self-signed test certificates for SSL/TLS testing."""
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )

    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
        ]
    )

    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=365))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .add_extension(
            x509.ExtendedKeyUsage(
                [
                    x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,
                    x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
                ]
            ),
            critical=False,
        )
        .sign(private_key, hashes.SHA256(), default_backend())
    )

    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )

    cert_pem = cert.public_bytes(serialization.Encoding.PEM)

    return private_key_pem, cert_pem, cert_pem


@pytest.fixture(scope="session")
def test_certificates():
    """Provide test certificates for the session."""
    return _generate_test_certificates()


@pytest.fixture(scope="session")
def credentials_map(test_certificates):
    """Map CredentialType enum values to WorkerCredentials or None."""
    key_pem, cert_pem, ca_pem = test_certificates
    return {
        CredentialType.INSECURE: None,
        CredentialType.MTLS: WorkerCredentials(
            ca_cert=ca_pem,
            worker_key=key_pem,
            worker_cert=cert_pem,
            mutual=True,
        ),
        CredentialType.ONE_WAY: WorkerCredentials(
            ca_cert=ca_pem,
            worker_key=key_pem,
            worker_cert=cert_pem,
            mutual=False,
        ),
    }


@pytest_asyncio.fixture(autouse=True)
async def _clear_channel_pool():
    """Clear the module-level gRPC channel pool after each test."""
    yield
    import wool.runtime.worker.connection as _conn

    await _conn.clear_channel_pool()


# Integration tests rely on pytest-asyncio's Task-per-test scoping
# for ContextVar isolation: each async test runs inside an
# asyncio.Task whose ``contextvars.Context`` is a copy, so
# wool.ContextVar mutations stay scoped to that copy and don't leak
# to the next test. Sync integration helpers run in the pytest main
# Chain — if they ever mutate routine-level vars, add an explicit
# per-test teardown at that site rather than reviving a global
# autouse cleanup. (The previous sync ``_clear_proxy_context``
# autouse fixture mutated the pytest main Chain, which async test
# tasks never observe; it was load-bearing in appearance only.)


@pytest_asyncio.fixture
async def worker_update_pool(request) -> AsyncIterator[tuple]:
    """Yield a live pool over one announced worker plus a live spare.

    Starts two ``LocalWorker`` processes against a per-test
    ``LocalDiscovery`` namespace, announces only the first, and enters a
    ``WorkerPool`` bound to that discovery through the ``_DIRECT``
    factory form. Yields ``(pool, publisher, worker_a, worker_b)``: the
    publisher so a test can drive worker-added/updated/dropped events,
    and both workers so a test can name their uids, addresses, and pids.

    The pool's ``lease`` is taken from an indirect parameter and
    defaults to ``None`` (uncapped); parametrize the fixture indirectly
    to cap admission.

    The pool is entered inside the caller's task so the ambient proxy
    ``ContextVar`` is visible to the dispatches the test makes. On exit
    the announced uid is dropped and both worker processes are stopped,
    whether or not the test already stopped one of them.
    """
    lease = getattr(request, "param", None)
    namespace = f"worker-update-{uuid.uuid4().hex[:12]}"
    worker_a = LocalWorker()
    worker_b = LocalWorker()
    await worker_a.start()
    await worker_b.start()
    try:
        with LocalDiscovery(namespace) as discovery:
            publisher = discovery.publisher
            async with publisher:
                await publisher.publish("worker-added", worker_a.metadata)
                pool = WorkerPool(
                    discovery=_DirectDiscovery(discovery),
                    loadbalancer=RoundRobinLoadBalancer,
                    lease=lease,
                )
                try:
                    async with pool:
                        yield pool, publisher, worker_a, worker_b
                finally:
                    with suppress(KeyError):
                        await publisher.publish("worker-dropped", worker_a.metadata)
    finally:
        # ``LocalWorker.stop`` raises on an already-stopped worker, so
        # the teardown absorbs that here rather than making every test
        # carry a "did I stop it?" flag.
        for worker in (worker_b, worker_a):
            with suppress(RuntimeError):
                await worker.stop()


@pytest.fixture
def retry_grpc_internal():
    """Retry a test body on transient internal gRPC errors.

    Returns an async callable. Usage::

        await retry_grpc_internal(body)

    where ``body`` is a no-argument async callable containing the
    test logic. Internal gRPC errors (``StatusCode.INTERNAL``) are
    retried with exponential backoff to tolerate the grpcio
    PollerCompletionQueue thundering-herd race. All other exceptions
    propagate immediately. If retries are exhausted the last
    exception propagates as a real test failure.
    """

    async def run(body):
        for attempt in range(_GRPC_INTERNAL_RETRIES + 1):
            try:
                return await body()
            except Exception as exc:
                # Narrow to ``Exception`` — gRPC ``RpcError`` is an
                # ``Exception`` subclass, and signals
                # (``KeyboardInterrupt``, ``SystemExit``,
                # ``CancelledError``) must propagate untouched. The
                # prior ``except BaseException`` could swallow a
                # mid-retry ``CancelledError`` arriving during
                # backoff sleep.
                if not _is_grpc_internal(exc):
                    raise
                if attempt < _GRPC_INTERNAL_RETRIES:
                    await asyncio.sleep(_GRPC_INTERNAL_BACKOFF * (2**attempt))
                    continue
                raise

    return run
