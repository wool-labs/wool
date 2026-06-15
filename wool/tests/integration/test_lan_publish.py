"""End-to-end tests for LAN-published pool reachability (#237).

These are targeted standalone tests rather than pairwise scenarios:
the scenario builder always supplies an explicit worker factory, so
the publisher-prescribed ``bind_host`` default and the publisher's
wildcard auto-resolution are only reachable through direct
``WorkerPool`` construction. The full cross-host mechanism is proved
same-host: the worker binds ``0.0.0.0``, the publisher advertises a
routable (non-loopback) address, and dispatch connects to that
advertised address.
"""

import asyncio
import socket
import uuid
from functools import partial

import pytest

from wool.runtime.discovery.lan import LanDiscovery
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.pool import WorkerPool

from . import routines

_TIMEOUT = 30

_callable_advertise_host_calls: list[int] = []


def _loopback_advertise_host() -> str:
    """Module-level advertise_host callable (picklable) counting calls."""
    _callable_advertise_host_calls.append(1)
    return "127.0.0.1"


def _unbound_worker(*tags, credentials=None, host):
    """Module-level unbound worker factory forwarding the bind host."""
    return LocalWorker(*tags, host=host, credentials=credentials)


def _routable_ipv4() -> str | None:
    """Resolve the default-route IPv4, or None when unavailable.

    Mirrors the production auto-resolution algorithm (a connected UDP
    socket selects the default-route interface without sending
    packets) so guard and assertion run the same probe.
    """
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(("8.8.8.8", 80))
        ip = probe.getsockname()[0]
    except OSError:
        return None
    finally:
        probe.close()
    if ip.startswith("127."):
        return None
    return ip


@pytest.fixture
def routable_ipv4():
    """Default-route IPv4 of this host; skips when there is none."""
    ip = _routable_ipv4()
    if ip is None:
        pytest.skip("no routable IPv4 default route — auto-resolve unavailable")
    return ip


async def _advertised_addresses(discovery: LanDiscovery, count: int) -> list[str]:
    """Collect the first ``count`` distinct worker-added addresses."""
    addresses: dict[str, str] = {}
    async for event in discovery.subscriber:
        if event.type == "worker-added":
            addresses[str(event.metadata.uid)] = event.metadata.address
            if len(addresses) >= count:
                return list(addresses.values())
    raise AssertionError("subscriber stream ended unexpectedly")


def _host_of(address: str) -> str:
    return address.rsplit(":", 1)[0]


@pytest.mark.integration
class TestLanPublish:
    @pytest.mark.asyncio
    async def test_lan_pool_default_config_dispatches_via_routable_address(
        self, routable_ipv4, retry_grpc_internal
    ):
        """Test the issue's acceptance path with zero configuration.

        Given:
            A publisher pool WorkerPool(spawn=1, discovery=
            LanDiscovery(ns)) with no worker factory, and a separate
            leaser pool WorkerPool(lease=1, discovery=
            LanDiscovery(ns))
        When:
            The leaser dispatches a routine and an observer collects
            the advertised worker address
        Then:
            It should return the routine result and advertise the
            host's routable default-route address — never loopback or
            a wildcard.
        """

        async def body():
            # Arrange
            namespace = f"lan-publish-{uuid.uuid4().hex[:12]}"
            observer = LanDiscovery(namespace)

            # Act
            async with asyncio.timeout(_TIMEOUT):
                async with WorkerPool(spawn=1, discovery=LanDiscovery(namespace)):
                    (address,) = await _advertised_addresses(observer, 1)
                    async with WorkerPool(lease=1, discovery=LanDiscovery(namespace)):
                        result = await routines.add(1, 2)

            # Assert
            assert result == 3
            host = _host_of(address)
            assert host == routable_ipv4
            assert host not in ("127.0.0.1", "0.0.0.0", "::", "")

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_lan_pool_explicit_advertise_host_round_trips_verbatim(
        self, retry_grpc_internal
    ):
        """Test a string advertise_host round-trips through zeroconf.

        Given:
            A pool with LanDiscovery(ns, advertise_host="127.0.0.1")
            and no worker factory, so the worker binds the wildcard
        When:
            A routine is dispatched and an observer collects the
            advertised worker address
        Then:
            It should return the routine result and advertise exactly
            the explicit advertise_host.
        """

        async def body():
            # Arrange
            namespace = f"lan-publish-{uuid.uuid4().hex[:12]}"
            observer = LanDiscovery(namespace)
            discovery = LanDiscovery(namespace, advertise_host="127.0.0.1")

            # Act
            async with asyncio.timeout(_TIMEOUT):
                async with WorkerPool(spawn=1, discovery=discovery):
                    (address,) = await _advertised_addresses(observer, 1)
                    result = await routines.add(1, 2)

            # Assert
            assert result == 3
            assert _host_of(address) == "127.0.0.1"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_lan_pool_callable_advertise_host_resolves_once(
        self, retry_grpc_internal
    ):
        """Test a callable advertise_host is invoked once for the pool.

        Given:
            A two-worker pool with LanDiscovery(ns, advertise_host=
            <module-level callable returning "127.0.0.1">)
        When:
            The pool publishes both workers and a routine is
            dispatched
        Then:
            It should return the routine result, advertise the
            callable's address for both workers, and invoke the
            callable exactly once.
        """

        async def body():
            # Arrange
            _callable_advertise_host_calls.clear()
            namespace = f"lan-publish-{uuid.uuid4().hex[:12]}"
            observer = LanDiscovery(namespace)
            discovery = LanDiscovery(namespace, advertise_host=_loopback_advertise_host)

            # Act
            async with asyncio.timeout(_TIMEOUT):
                async with WorkerPool(spawn=2, discovery=discovery):
                    addresses = await _advertised_addresses(observer, 2)
                    result = await routines.add(1, 2)

            # Assert
            assert result == 3
            assert len(_callable_advertise_host_calls) == 1
            assert all(_host_of(a) == "127.0.0.1" for a in addresses)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_lan_pool_wildcard_factory_advertises_routable_address(
        self, routable_ipv4, retry_grpc_internal
    ):
        """Test the previously-broken explicit wildcard-bind config.

        Given:
            A pool with worker=partial(LocalWorker, host="0.0.0.0")
            and LanDiscovery with no advertise_host — the configuration
            that used to advertise the wildcard verbatim
        When:
            A routine is dispatched and an observer collects the
            advertised worker address
        Then:
            It should return the routine result and advertise the
            host's routable default-route address instead of the
            wildcard.
        """

        async def body():
            # Arrange
            namespace = f"lan-publish-{uuid.uuid4().hex[:12]}"
            observer = LanDiscovery(namespace)

            # Act
            async with asyncio.timeout(_TIMEOUT):
                async with WorkerPool(
                    spawn=1,
                    discovery=LanDiscovery(namespace),
                    worker=partial(LocalWorker, host="0.0.0.0"),
                ):
                    (address,) = await _advertised_addresses(observer, 1)
                    result = await routines.add(1, 2)

            # Assert
            assert result == 3
            assert _host_of(address) == routable_ipv4

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_lan_pool_local_worker_factory_advertises_routable_address(
        self, routable_ipv4, retry_grpc_internal
    ):
        """Test passing LocalWorker explicitly equals the default.

        Given:
            A pool with worker=LocalWorker passed explicitly and
            LanDiscovery with no advertise_host
        When:
            A routine is dispatched and an observer collects the
            advertised worker address
        Then:
            It should return the routine result and advertise the
            host's routable default-route address — LocalWorker
            declares host and classifies bind-host-aware, exactly
            like omitting the worker argument.
        """

        async def body():
            # Arrange
            namespace = f"lan-publish-{uuid.uuid4().hex[:12]}"
            observer = LanDiscovery(namespace)

            # Act
            async with asyncio.timeout(_TIMEOUT):
                async with WorkerPool(
                    spawn=1,
                    discovery=LanDiscovery(namespace),
                    worker=LocalWorker,
                ):
                    (address,) = await _advertised_addresses(observer, 1)
                    result = await routines.add(1, 2)

            # Assert
            assert result == 3
            assert _host_of(address) == routable_ipv4

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_lan_pool_unbound_factory_advertises_routable_address(
        self, routable_ipv4, retry_grpc_internal
    ):
        """Test an unbound custom factory keeps cross-host reachability.

        Given:
            A pool with worker set to a module-level factory declaring
            host and LanDiscovery with no advertise_host
        When:
            A routine is dispatched and an observer collects the
            advertised worker address
        Then:
            It should return the routine result and advertise the
            host's routable default-route address — the factory
            received and served the publisher's prescribed bind host.
        """

        async def body():
            # Arrange
            namespace = f"lan-publish-{uuid.uuid4().hex[:12]}"
            observer = LanDiscovery(namespace)

            # Act
            async with asyncio.timeout(_TIMEOUT):
                async with WorkerPool(
                    spawn=1,
                    discovery=LanDiscovery(namespace),
                    worker=_unbound_worker,
                ):
                    (address,) = await _advertised_addresses(observer, 1)
                    result = await routines.add(1, 2)

            # Assert
            assert result == 3
            assert _host_of(address) == routable_ipv4

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_lan_pool_explicit_factory_advertises_bind_host_verbatim(
        self, retry_grpc_internal
    ):
        """Test an explicit concrete factory is never overridden.

        Given:
            A pool with worker=partial(LocalWorker, host="127.0.0.1")
            and LanDiscovery with no advertise_host
        When:
            A routine is dispatched and an observer collects the
            advertised worker address
        Then:
            It should return the routine result and advertise the
            explicit bind host verbatim — the pool neither rebinds the
            worker nor rewrites its concrete advertised address.
        """

        async def body():
            # Arrange
            namespace = f"lan-publish-{uuid.uuid4().hex[:12]}"
            observer = LanDiscovery(namespace)

            # Act
            async with asyncio.timeout(_TIMEOUT):
                async with WorkerPool(
                    spawn=1,
                    discovery=LanDiscovery(namespace),
                    worker=partial(LocalWorker, host="127.0.0.1"),
                ):
                    (address,) = await _advertised_addresses(observer, 1)
                    result = await routines.add(1, 2)

            # Assert
            assert result == 3
            assert _host_of(address) == "127.0.0.1"

        await retry_grpc_internal(body)
