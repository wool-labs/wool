"""End-to-end integration tests for identity-based mutual TLS.

These tests spawn real workers over ephemeral loopback addresses and
exercise the security outcomes wool offers dynamic-address deployments:
verifying a worker against a stable logical identity (a SAN that is *not*
the dialed address), surfacing a credential misconfiguration as a
distinct, diagnosable signal, and adopting rotated material on both
planes without a restart.
"""

import functools
import logging
from types import SimpleNamespace

import pytest
import pytest_asyncio
from cryptography import x509

import wool
from tests.helpers import LOOPBACK_SANS
from tests.helpers import generate_ca_and_leaf
from tests.helpers import generate_certificate_files
from wool import NoWorkersAvailable
from wool import WorkerCredentials
from wool import WorkerCredentialsProvider
from wool import WorkerProxy

from .routines import add
from .routines import force_adoption
from .routines import read_adopted_worker_ca

_WORKER_IDENTITY = "wool-worker.svc"


@pytest.fixture
def identity_cert_files(tmp_path):
    """Write CA, key, and an identity-only worker cert to PEM files.

    The worker certificate's only SAN is the logical identity, never an
    address, so it can only be validated by verifying against the identity.
    Returns a tuple of (ca_path, key_path, cert_path) as strings.
    """
    files = generate_certificate_files(
        tmp_path, [x509.DNSName(_WORKER_IDENTITY)], common_name=_WORKER_IDENTITY
    )
    return files.ca_path, files.key_path, files.cert_path


@pytest_asyncio.fixture
async def started_worker():
    """Start workers for a test and stop them at teardown.

    Returns an async callable that starts the given `wool.LocalWorker`,
    asserts the started worker announced its metadata, and registers it
    to be stopped at teardown, so test bodies carry no start/stop
    scaffolding.
    """
    workers = []

    async def start(worker):
        await worker.start()
        workers.append(worker)
        assert worker.metadata is not None
        return worker

    yield start
    for worker in workers:
        await worker.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_should_succeed_when_identity_configured(identity_cert_files):
    """Test enforced mTLS works over address-based discovery via identity.

    Given:
        A worker provisioned with a single static certificate whose only
        SAN is a stable logical identity, spawned at a dynamically
        assigned loopback address, and a client configured with that
        identity.
    When:
        A routine is dispatched against the pool.
    Then:
        The client should complete the mutual TLS handshake by verifying
        the worker against the identity rather than the dialed address, and
        the dispatch should succeed.
    """
    # Arrange
    ca_path, key_path, cert_path = identity_cert_files
    provider = WorkerCredentials.from_files(ca_path, key_path, cert_path).as_provider(
        identity=_WORKER_IDENTITY
    )

    # Act
    async with wool.WorkerPool(spawn=1, credentials=provider):
        result = await add(2, 3)

    # Assert
    assert result == 5


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_should_drain_to_no_workers_when_ca_untrusted(
    started_worker, caplog
):
    """Test a credential mismatch is rejected with a diagnosable signal.

    Given:
        A running worker whose certificate is signed by one certificate
        authority, and a client whose credentials trust a different
        certificate authority.
    When:
        A routine is dispatched at the worker through that client.
    Then:
        The handshake should fail, the dispatch should drain to
        NoWorkersAvailable (the worker is skipped, not evicted), and the
        proxy should log a warning identifying the handshake failure so
        the misconfiguration is diagnosable.
    """
    # Arrange — server and client trust different CAs (loopback SANs so the
    # worker's own stop RPC, which dials the address, still validates).
    server = generate_ca_and_leaf(LOOPBACK_SANS)
    client = generate_ca_and_leaf(LOOPBACK_SANS)
    worker = await started_worker(
        wool.LocalWorker(
            credentials=WorkerCredentials(
                ca_cert=server.ca_pem,
                worker_key=server.key_pem,
                worker_cert=server.cert_pem,
            )
        )
    )
    client_credentials = WorkerCredentials(
        ca_cert=client.ca_pem, worker_key=client.key_pem, worker_cert=client.cert_pem
    )

    # Act & assert
    async with WorkerProxy(workers=[worker.metadata], credentials=client_credentials):
        with caplog.at_level(logging.WARNING), pytest.raises(NoWorkersAvailable):
            await add(2, 3)

    # Assert
    assert "handshake" in caplog.text.lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_should_drain_to_no_workers_when_identity_mismatched(
    tmp_path, started_worker, caplog
):
    """Test a certificate that does not match the configured identity is rejected.

    Given:
        A running worker whose certificate carries one logical identity,
        and a client that trusts the same CA but expects a different
        identity.
    When:
        A routine is dispatched at the worker through that client.
    Then:
        The dispatch should drain to NoWorkersAvailable and the proxy
        should log a warning identifying the handshake failure —
        verifying against the configured identity strengthens, not
        relaxes, the guarantee.
    """
    # Arrange — one CA; the worker cert's SANs include loopback (so its own
    # stop RPC validates) and a logical identity the client will not expect.
    files = generate_certificate_files(
        tmp_path, [*LOOPBACK_SANS, x509.DNSName(_WORKER_IDENTITY)]
    )
    worker = await started_worker(
        wool.LocalWorker(
            credentials=WorkerCredentials(
                ca_cert=files.ca_pem,
                worker_key=files.key_pem,
                worker_cert=files.cert_pem,
            )
        )
    )
    client_credentials = WorkerCredentials.from_files(
        files.ca_path, files.key_path, files.cert_path
    ).as_provider(identity="does-not-match.example")

    # Act & assert
    async with WorkerProxy(workers=[worker.metadata], credentials=client_credentials):
        with caplog.at_level(logging.WARNING), pytest.raises(NoWorkersAvailable):
            await add(2, 3)

    # Assert
    assert "handshake" in caplog.text.lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_should_not_classify_when_worker_rejects_client_cert(
    started_worker, caplog
):
    """Test a worker rejecting the client's certificate goes unclassified.

    Given:
        A running worker, and a client that trusts the worker's certificate
        authority but presents a certificate signed by a different one, so
        the client's own verification succeeds and only the worker's
        verification of the client fails.
    When:
        A routine is dispatched at the worker through that client.
    Then:
        The dispatch should drain to NoWorkersAvailable, and the failure
        should not be classified as a handshake failure.

        This pins a known limitation rather than desired behavior. Under
        TLS 1.3 the client sends its certificate in its final flight and
        completes the handshake locally, so the worker's rejection arrives
        afterwards as a plain transport error carrying no TLS evidence —
        see `wool.HandshakeError`. A failure here therefore means the
        limitation has lifted, not that the classifier regressed.
    """
    # Arrange — the worker's own CA, plus a second CA the worker does not
    # trust. The client trusts the worker's CA (so verifying the worker
    # succeeds) but presents a certificate from the second one.
    server = generate_ca_and_leaf(LOOPBACK_SANS)
    rogue = generate_ca_and_leaf(LOOPBACK_SANS)
    worker = await started_worker(
        wool.LocalWorker(
            credentials=WorkerCredentials(
                ca_cert=server.ca_pem,
                worker_key=server.key_pem,
                worker_cert=server.cert_pem,
            )
        )
    )
    client_credentials = WorkerCredentials(
        ca_cert=server.ca_pem, worker_key=rogue.key_pem, worker_cert=rogue.cert_pem
    )

    # Act
    async with WorkerProxy(
        workers=[worker.metadata], credentials=client_credentials
    ) as proxy:
        with caplog.at_level(logging.WARNING), pytest.raises(NoWorkersAvailable):
            await add(2, 3)
        retained = proxy.workers

    # Assert — the worker was tried and skipped without eviction, so the
    # dispatch loop did classify the failure; it just did not classify it
    # as a handshake failure, which would have logged a warning.
    assert [each.uid for each in retained] == [worker.metadata.uid]
    assert "handshake" not in caplog.text.lower()


@pytest_asyncio.fixture
async def rotated_deployment(tmp_path, started_worker):
    """Run a worker and client through a credential rotation, and yield both.

    Starts a worker and client sharing one reloadable file-backed
    provider, completes a dispatch on the original authority, rotates the
    CA, key, and certificate on disk to a brand-new authority, and drives
    both planes to adopt it before yielding. Returns a namespace of the
    provider, the worker, and the original and rotated material.

    Adoption is forced rather than awaited because resolution is debounced
    and stale-while-revalidate, so an unforced adoption lands at an
    unbounded later read. The worker adopts first, over the still-pooled
    original channel, so the barrier dispatch itself needs no fresh
    handshake; the client's provider adopts second.
    """
    sans = [*LOOPBACK_SANS, x509.DNSName(_WORKER_IDENTITY)]
    original = generate_certificate_files(tmp_path, sans)
    provider = WorkerCredentialsProvider(
        functools.partial(
            WorkerCredentials.from_files,
            original.ca_path,
            original.key_path,
            original.cert_path,
        ),
        identity=_WORKER_IDENTITY,
        reloadable=True,
    )
    worker = await started_worker(wool.LocalWorker(credentials=provider))
    async with WorkerProxy(workers=[worker.metadata], credentials=provider):
        assert await add(2, 3) == 5
        rotated = generate_certificate_files(tmp_path, sans)
        assert await read_adopted_worker_ca() == rotated.ca_pem
        assert force_adoption(provider).ca_cert == rotated.ca_pem
    yield SimpleNamespace(
        provider=provider, worker=worker, original=original, rotated=rotated
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_should_succeed_when_credentials_rotated(rotated_deployment):
    """Test a live worker and client dispatch on rotated credentials.

    Given:
        A worker and client that adopted a brand-new certificate
        authority without either being restarted.
    When:
        A routine is dispatched.
    Then:
        It should succeed over a fresh handshake on the rotated
        material — the rotated material compares unequal to the
        original, so the client derives a new channel-pool key rather
        than riding the channel it opened on the original.
    """
    # Arrange
    deployment = rotated_deployment

    # Act
    async with WorkerProxy(
        workers=[deployment.worker.metadata], credentials=deployment.provider
    ):
        result = await add(2, 3)

    # Assert
    assert result == 5


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_should_drain_to_no_workers_when_client_trusts_original_ca(
    rotated_deployment,
):
    """Test a worker that rotated no longer answers to the original authority.

    Given:
        A worker that adopted a brand-new certificate authority, and a
        client still trusting only the original one.
    When:
        A routine is dispatched through that client.
    Then:
        It should drain to NoWorkersAvailable, proving the worker
        actually presents the rotated certificate rather than both
        sides having silently continued on the original material.
    """
    # Arrange
    original = rotated_deployment.original
    stale_client = WorkerCredentials(
        ca_cert=original.ca_pem,
        worker_key=original.key_pem,
        worker_cert=original.cert_pem,
    )

    # Act & assert
    async with WorkerProxy(
        workers=[rotated_deployment.worker.metadata], credentials=stale_client
    ):
        with pytest.raises(NoWorkersAvailable):
            await add(2, 3)
