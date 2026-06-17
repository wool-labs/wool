"""End-to-end integration tests for identity-based mutual TLS.

These tests spawn real workers over ephemeral loopback addresses and
exercise the two security outcomes introduced for dynamic-address
deployments: verifying a worker against a stable logical identity (a SAN
that is *not* the dialed address), and surfacing a credential
misconfiguration as a distinct, diagnosable signal.
"""

import datetime
import ipaddress
import os

import pytest
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

import wool
from wool import AllWorkersUnauthenticated
from wool import HandshakeError
from wool import StaticCredentialProvider
from wool import WorkerCredentials
from wool import WorkerProxy

from .routines import add

_WORKER_IDENTITY = "wool-worker.svc"
_LOOPBACK_SANS = (
    x509.DNSName("localhost"),
    x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
)


def _rsa_key():
    return rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )


def _pem_key(key) -> bytes:
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _generate_ca_and_leaf(sans, *, common_name="wool-worker"):
    """Generate a fresh CA and a leaf certificate carrying *sans*.

    The leaf is signed by the CA and granted both server and client
    extended key usages so it works on both sides of a mutual-TLS
    connection.

    Returns a tuple of (ca_cert_pem, leaf_key_pem, leaf_cert_pem).
    """
    now = datetime.datetime.now(datetime.UTC)
    ca_key = _rsa_key()
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "wool-test-ca")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=365))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256(), default_backend())
    )

    leaf_key = _rsa_key()
    leaf_cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        .issuer_name(ca_name)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=365))
        .add_extension(x509.SubjectAlternativeName(list(sans)), critical=False)
        .add_extension(
            x509.ExtendedKeyUsage(
                [
                    x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,
                    x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
                ]
            ),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256(), default_backend())
    )

    return (
        ca_cert.public_bytes(serialization.Encoding.PEM),
        _pem_key(leaf_key),
        leaf_cert.public_bytes(serialization.Encoding.PEM),
    )


@pytest.fixture
def identity_cert_files(tmp_path):
    """Write CA, key, and an identity-only worker cert to PEM files.

    The worker certificate's only SAN is the logical identity, never an
    address, so it can only be validated by verifying against the identity.
    Returns a tuple of (ca_path, key_path, cert_path) as strings.
    """
    ca_pem, key_pem, cert_pem = _generate_ca_and_leaf(
        [x509.DNSName(_WORKER_IDENTITY)], common_name=_WORKER_IDENTITY
    )
    ca_path = tmp_path / "ca.pem"
    key_path = tmp_path / "key.pem"
    cert_path = tmp_path / "cert.pem"
    ca_path.write_bytes(ca_pem)
    key_path.write_bytes(key_pem)
    cert_path.write_bytes(cert_pem)
    return str(ca_path), str(key_path), str(cert_path)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_identity_based_mtls_dispatch_succeeds(identity_cert_files):
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
    provider = WorkerCredentials.provider_from_files(
        ca_path, key_path, cert_path, identity=_WORKER_IDENTITY
    )

    # Act
    async with wool.WorkerPool(spawn=1, credentials=provider):
        result = await add(2, 3)

    # Assert
    assert result == 5


@pytest.mark.integration
@pytest.mark.asyncio
async def test_untrusted_ca_rejects_with_diagnosable_signal():
    """Test a credential mismatch surfaces as a distinct typed signal.

    Given:
        A running worker whose certificate is signed by one certificate
        authority, and a client whose credentials trust a different
        certificate authority.
    When:
        A routine is dispatched at the worker through that client.
    Then:
        The handshake should fail and surface as AllWorkersUnauthenticated
        — a distinct subclass of NoWorkersAvailable carrying per-worker
        handshake reasons — rather than collapsing into a bare empty-pool
        outcome, so a fleet-wide misconfiguration is diagnosable.
    """
    # Arrange — server and client trust different CAs (loopback SANs so the
    # worker's own stop RPC, which dials the address, still validates).
    server_ca, server_key, server_cert = _generate_ca_and_leaf(_LOOPBACK_SANS)
    client_ca, client_key, client_cert = _generate_ca_and_leaf(_LOOPBACK_SANS)
    worker = wool.LocalWorker(
        credentials=WorkerCredentials(
            ca_cert=server_ca, worker_key=server_key, worker_cert=server_cert
        )
    )
    client_credentials = WorkerCredentials(
        ca_cert=client_ca, worker_key=client_key, worker_cert=client_cert
    )

    # Act & assert
    await worker.start()
    try:
        assert worker.metadata is not None
        async with WorkerProxy(
            workers=[worker.metadata], credentials=client_credentials
        ):
            with pytest.raises(AllWorkersUnauthenticated) as exc_info:
                await add(2, 3)

        reasons = {rej.reason for rej in exc_info.value.rejections.values()}
        assert HandshakeError.Reason.CERT_VERIFY in reasons
    finally:
        await worker.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_identity_mismatch_rejects_with_identity_mismatch_reason():
    """Test a certificate that does not match the configured identity is rejected.

    Given:
        A running worker whose certificate carries one logical identity,
        and a client that trusts the same CA but expects a different
        identity.
    When:
        A routine is dispatched at the worker through that client.
    Then:
        The handshake should be rejected with the IDENTITY_MISMATCH reason
        — verifying against the configured identity strengthens, not
        relaxes, the guarantee.
    """
    # Arrange — one CA; the worker cert's SANs include loopback (so its own
    # stop RPC validates) and a logical identity the client will not expect.
    ca_pem, key_pem, cert_pem = _generate_ca_and_leaf(
        [*_LOOPBACK_SANS, x509.DNSName(_WORKER_IDENTITY)]
    )
    worker = wool.LocalWorker(
        credentials=WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
    )
    client_credentials = StaticCredentialProvider(
        WorkerCredentials(ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem),
        identity="does-not-match.example",
    )

    # Act & assert
    await worker.start()
    try:
        assert worker.metadata is not None
        async with WorkerProxy(
            workers=[worker.metadata], credentials=client_credentials
        ):
            with pytest.raises(AllWorkersUnauthenticated) as exc_info:
                await add(2, 3)

        reasons = {rej.reason for rej in exc_info.value.rejections.values()}
        assert HandshakeError.Reason.IDENTITY_MISMATCH in reasons
    finally:
        await worker.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_credential_rotation_without_restart(tmp_path):
    """Test a long-running pool adopts rotated credentials without a restart.

    Given:
        An ephemeral pool on a reloading file provider, with one successful
        dispatch, after which the CA, key, and certificate are rotated to a
        brand-new certificate authority on disk.
    When:
        A second routine is dispatched without restarting the worker or the
        pool.
    Then:
        It should succeed over a real handshake on the rotated material —
        the worker server adopts it per connection and the client adopts it
        per dispatch.
    """
    # Arrange — start on CA #1.
    sans = [*_LOOPBACK_SANS, x509.DNSName(_WORKER_IDENTITY)]
    ca_pem, key_pem, cert_pem = _generate_ca_and_leaf(sans)
    ca_path = tmp_path / "ca.pem"
    key_path = tmp_path / "key.pem"
    cert_path = tmp_path / "cert.pem"
    ca_path.write_bytes(ca_pem)
    key_path.write_bytes(key_pem)
    cert_path.write_bytes(cert_pem)
    provider = WorkerCredentials.provider_from_files(
        str(ca_path),
        str(key_path),
        str(cert_path),
        identity=_WORKER_IDENTITY,
        reload=True,
    )

    # Act & assert
    async with wool.WorkerPool(spawn=1, credentials=provider):
        assert await add(2, 3) == 5

        # Rotate to a fresh, independent CA on disk (new key and cert too).
        new_ca, new_key, new_cert = _generate_ca_and_leaf(sans)
        rotated_mtime = os.stat(ca_path).st_mtime_ns + 1_000_000_000
        ca_path.write_bytes(new_ca)
        key_path.write_bytes(new_key)
        cert_path.write_bytes(new_cert)
        for path in (ca_path, key_path, cert_path):
            os.utime(path, ns=(rotated_mtime, rotated_mtime))

        assert await add(2, 3) == 5
