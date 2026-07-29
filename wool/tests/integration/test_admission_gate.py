"""Admission-gate integration tests (issue #293).

These are targeted standalone tests rather than pairwise scenarios: the
security/version admission gate is observable only through deliberate
pool/worker mismatches, and a mismatched-posture dimension member would
break the pairwise array's single dispatch-success oracle. The oracle
here is the exact admitted-worker set read from the active proxy, which
the scenario builder does not surface.

Fabricated incompatible metadata ("chaff") is published through the real
LocalDiscovery pipeline — the untrusted-discovery-source threat model
the fix addresses — alongside real workers. Each test asserts the
admitted set is exactly the compatible workers, both before and after
dispatching, so late chaff admission and spurious eviction are caught in
one check.
"""

import asyncio
import uuid
from dataclasses import replace

import pytest

import wool
from wool import protocol
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.metadata import WorkerMetadata
from wool.runtime.worker.pool import WorkerPool
from wool.runtime.worker.proxy import WorkerProxy
from wool.runtime.worker.proxy import is_version_compatible
from wool.runtime.worker.proxy import parse_version

from . import routines
from .conftest import CredentialType
from .conftest import _DirectDiscovery
from .conftest import poll_dispatch_until_pid
from .conftest import poll_dispatch_until_unavailable
from .conftest import poll_until_count

_DISPATCH_TIMEOUT = 30
"""Seconds to bound a posture-transition test's dispatch loop."""


def _make_chaff():
    """Fabricate incompatible worker metadata for a discovery namespace.

    Versions derive from the running protocol version so a future
    version-scheme change cannot silently neuter the chaff; every
    record is guard-asserted incompatible via the public predicates.
    The dead address yields an instant connection refusal, not a hang,
    if a regression ever admits and dials one.
    """
    local = parse_version(protocol.__version__)
    assert local is not None
    older_same_major = f"{local.major}.0.0.dev0"
    next_major = f"{local.major + 1}.0.0"
    chaff = [
        WorkerMetadata(
            uid=uuid.uuid4(),
            address="127.0.0.1:1",
            pid=1,
            version=older_same_major,
        ),
        WorkerMetadata(
            uid=uuid.uuid4(),
            address="127.0.0.1:1",
            pid=1,
            version=next_major,
        ),
        WorkerMetadata(
            uid=uuid.uuid4(),
            address="127.0.0.1:1",
            pid=1,
            version="not-a-version",
        ),
        WorkerMetadata(
            uid=uuid.uuid4(),
            address="127.0.0.1:1",
            pid=1,
            version=protocol.__version__,
            secure=True,
        ),
    ]
    older = parse_version(older_same_major)
    assert older is not None and not is_version_compatible(local, older)
    bumped = parse_version(next_major)
    assert bumped is not None and not is_version_compatible(local, bumped)
    assert parse_version("not-a-version") is None
    assert chaff[3].secure
    return chaff


def _admitted_uids():
    """Snapshot the active proxy's admitted worker uids."""
    proxy = wool.__proxy__.get()
    assert proxy is not None
    return {worker.uid for worker in proxy.workers}


@pytest.mark.integration
class TestDiscoveryAdmissionGate:
    @pytest.mark.asyncio
    async def test___aenter___should_admit_only_compatible_worker_when_chaffed(
        self, retry_grpc_internal
    ):
        """Test the gate screens a custom discovery source end to end.

        Given:
            A durable pool over a LocalDiscovery namespace carrying one
            real insecure worker and four fabricated incompatible
            records — older same-major, next-major, unparsable, and a
            secure twin at the real version.
        When:
            The pool is entered eagerly and a routine is dispatched
            more times than there are chaff records.
        Then:
            It should admit exactly the real worker before and after
            the dispatches, and every dispatch should succeed with no
            eviction noise.
        """

        async def body():
            # Arrange
            namespace = f"admission-{uuid.uuid4().hex[:12]}"
            chaff = _make_chaff()
            real_worker = LocalWorker()
            await real_worker.start()
            try:
                with LocalDiscovery(namespace) as discovery:
                    publisher = discovery.publisher
                    async with publisher:
                        for record in chaff:
                            await publisher.publish("worker-added", record)
                        await publisher.publish("worker-added", real_worker.metadata)
                        pool = WorkerPool(
                            discovery=_DirectDiscovery(discovery),
                            quorum=1,
                            lazy=False,
                        )

                        # Act & assert
                        async with pool:
                            assert real_worker.metadata is not None
                            expected = {real_worker.metadata.uid}
                            await poll_until_count(_admitted_uids, len(expected))
                            assert _admitted_uids() == expected

                            results = [await routines.add(1, 2) for _ in range(5)]
                            assert results == [3] * 5
                            assert _admitted_uids() == expected

                        await publisher.publish("worker-dropped", real_worker.metadata)
            finally:
                await real_worker.stop()

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestSecurityPostureAdmission:
    @pytest.mark.asyncio
    async def test___aenter___should_admit_only_insecure_worker(
        self, retry_grpc_internal, credentials_map
    ):
        """Test an insecure pool never admits a real secure worker.

        Given:
            Two real workers in one namespace — one insecure and one
            advertising mTLS — and a durable pool without credentials.
        When:
            The pool is entered and a routine is dispatched several
            times.
        Then:
            It should admit only the insecure worker and every
            dispatch should succeed — the secure worker is screened at
            admission instead of failing the TLS handshake at dispatch.
        """

        async def body():
            # Arrange
            namespace = f"admission-{uuid.uuid4().hex[:12]}"
            insecure_worker = LocalWorker()
            secure_worker = LocalWorker(credentials=credentials_map[CredentialType.MTLS])
            await insecure_worker.start()
            await secure_worker.start()
            try:
                with LocalDiscovery(namespace) as discovery:
                    publisher = discovery.publisher
                    async with publisher:
                        await publisher.publish("worker-added", insecure_worker.metadata)
                        await publisher.publish("worker-added", secure_worker.metadata)
                        pool = WorkerPool(
                            discovery=_DirectDiscovery(discovery),
                            quorum=1,
                            lazy=False,
                        )

                        # Act & assert
                        async with pool:
                            assert insecure_worker.metadata is not None
                            expected = {insecure_worker.metadata.uid}
                            await poll_until_count(_admitted_uids, len(expected))
                            assert _admitted_uids() == expected

                            for _ in range(3):
                                assert await routines.add(1, 2) == 3
                            assert _admitted_uids() == expected
            finally:
                await secure_worker.stop()
                await insecure_worker.stop()

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test___aenter___should_admit_only_secure_worker(
        self, retry_grpc_internal, credentials_map
    ):
        """Test a credentialed pool never admits a real insecure worker.

        Given:
            The mirror topology — the same two real workers and a
            durable pool holding mTLS credentials.
        When:
            The pool is entered and a routine is dispatched several
            times.
        Then:
            It should admit only the secure worker and every dispatch
            should succeed over a real mTLS channel.
        """

        async def body():
            # Arrange
            namespace = f"admission-{uuid.uuid4().hex[:12]}"
            credentials = credentials_map[CredentialType.MTLS]
            insecure_worker = LocalWorker()
            secure_worker = LocalWorker(credentials=credentials)
            await insecure_worker.start()
            await secure_worker.start()
            try:
                with LocalDiscovery(namespace) as discovery:
                    publisher = discovery.publisher
                    async with publisher:
                        await publisher.publish("worker-added", insecure_worker.metadata)
                        await publisher.publish("worker-added", secure_worker.metadata)
                        pool = WorkerPool(
                            discovery=_DirectDiscovery(discovery),
                            credentials=credentials,
                            quorum=1,
                            lazy=False,
                        )

                        # Act & assert
                        async with pool:
                            assert secure_worker.metadata is not None
                            expected = {secure_worker.metadata.uid}
                            await poll_until_count(_admitted_uids, len(expected))
                            assert _admitted_uids() == expected

                            for _ in range(3):
                                assert await routines.add(1, 2) == 3
                            assert _admitted_uids() == expected
            finally:
                await secure_worker.stop()
                await insecure_worker.stop()

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestHybridAdmissionGate:
    @pytest.mark.asyncio
    async def test___aenter___should_admit_only_spawned_worker_when_chaffed(
        self, retry_grpc_internal
    ):
        """Test the gate covers the spawn-plus-discovery pool path.

        Given:
            A hybrid pool spawning one worker into a LocalDiscovery
            namespace pre-seeded with the fabricated incompatible
            records.
        When:
            The pool is entered eagerly and a routine is dispatched
            several times.
        Then:
            It should admit exactly the spawned worker — never any
            chaff — and every dispatch should succeed.
        """

        async def body():
            # Arrange
            namespace = f"admission-{uuid.uuid4().hex[:12]}"
            chaff = _make_chaff()
            chaff_uids = {record.uid for record in chaff}
            with LocalDiscovery(namespace) as discovery:
                publisher = discovery.publisher
                async with publisher:
                    for record in chaff:
                        await publisher.publish("worker-added", record)
                    pool = WorkerPool(
                        spawn=1,
                        discovery=_DirectDiscovery(discovery),
                        quorum=1,
                        lazy=False,
                    )

                    # Act & assert
                    async with pool:
                        admitted = await poll_until_count(_admitted_uids, 1)
                        assert not admitted & chaff_uids

                        for _ in range(3):
                            assert await routines.add(1, 2) == 3
                        assert _admitted_uids() == admitted

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestStaticWorkersAdmission:
    @pytest.mark.asyncio
    async def test___aenter___should_admit_only_real_worker_when_chaffed(
        self, retry_grpc_internal
    ):
        """Test the static-workers path end to end with real dispatch.

        Given:
            A WorkerProxy built from a static list containing one real
            started worker's metadata plus the fabricated incompatible
            records, with quorum=1.
        When:
            The proxy is entered eagerly and a routine is dispatched.
        Then:
            It should enter successfully — the quorum counts only the
            compatible worker — admit exactly the real worker, and
            return the dispatch result.
        """

        async def body():
            # Arrange
            chaff = _make_chaff()
            real_worker = LocalWorker()
            await real_worker.start()
            try:
                assert real_worker.metadata is not None
                proxy = WorkerProxy(
                    workers=[real_worker.metadata, *chaff],
                    quorum=1,
                    lazy=False,
                )

                # Act & assert
                async with proxy:
                    await poll_until_count(lambda: {w.uid for w in proxy.workers}, 1)
                    expected = {real_worker.metadata.uid}
                    assert {w.uid for w in proxy.workers} == expected

                    assert await routines.add(1, 2) == 3
                    assert {w.uid for w in proxy.workers} == expected
            finally:
                await real_worker.stop()

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestPostureTransitionAdmission:
    @pytest.mark.asyncio
    async def test___aenter___should_evict_worker_when_update_flips_incompatible(
        self, worker_update_pool, retry_grpc_internal
    ):
        """Test an admitted worker flipped incompatible is evicted end to end.

        Given:
            A pool over a real LocalDiscovery holding one admitted
            insecure worker
        When:
            The publisher publishes worker-updated for that worker's uid
            carrying a secure=True record the credential-less pool cannot
            admit
        Then:
            It should evict the worker within a bounded deadline —
            dispatch raises NoWorkersAvailable rather than routing to a
            worker the gate now rejects
        """
        _, publisher, worker_a, _worker_b = worker_update_pool

        async def body():
            async with asyncio.timeout(_DISPATCH_TIMEOUT):
                # Arrange
                assert await routines.get_pid() == worker_a.metadata.pid

                # Act
                await publisher.publish(
                    "worker-updated", replace(worker_a.metadata, secure=True)
                )

                # Assert
                await poll_dispatch_until_unavailable(
                    "the posture-flipped worker was never evicted"
                )

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test___aenter___should_readmit_worker_when_update_flips_back_compatible(
        self, worker_update_pool, retry_grpc_internal
    ):
        """Test an update re-admits a worker the gate had evicted.

        Given:
            A pool over a real LocalDiscovery whose one admitted worker
            was evicted by a worker-updated flipping it secure=True, so
            the pool is empty
        When:
            The publisher publishes worker-updated for that uid carrying
            the worker's real, compatible metadata
        Then:
            It should re-admit the worker into the pool — dispatch
            reaches it within a bounded deadline, proving an update
            admits a worker not currently held
        """
        _, publisher, worker_a, _worker_b = worker_update_pool

        async def body():
            async with asyncio.timeout(_DISPATCH_TIMEOUT):
                # Arrange — evict the worker via an incompatible flip.
                assert await routines.get_pid() == worker_a.metadata.pid
                await publisher.publish(
                    "worker-updated", replace(worker_a.metadata, secure=True)
                )
                await poll_dispatch_until_unavailable(
                    "the posture-flipped worker was never evicted"
                )

                # Act — its real metadata is compatible again.
                await publisher.publish("worker-updated", worker_a.metadata)

                # Assert
                await poll_dispatch_until_pid(
                    worker_a.metadata.pid,
                    "the re-compatible worker was never admitted on update",
                )

        await retry_grpc_internal(body)
