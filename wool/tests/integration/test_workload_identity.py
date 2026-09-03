"""Limitation pins for SPIFFE workload-identity mutual TLS.

SPIFFE URI-SAN peer verification is blocked on upstream gRPC: from
grpcio 1.78 through 1.83, the client-side name check behind
``ssl_channel_credentials`` + ``grpc.ssl_target_name_override`` is
``HostNameCertificateVerifier``, which consults DNS SANs, IP SANs, and
the CN only when no DNS SAN is present — URI SANs are never consulted,
and a URI-form target name is mangled by host:port splitting besides. A
SPIFFE X509-SVID identity (a URI SAN) therefore cannot be verified by a
Python gRPC client at the handshake. gRFC A87 (mTLS SPIFFE support)
covers trust-bundle maps only, leaves SAN matching unchanged, and has
no Python implementation.

These tests pin that limitation the way `test_identity_mtls` pins the
TLS 1.3 client-rejection blind spot: a failure here means the
limitation has lifted, not that wool regressed. Described by capability
rather than by issue number, which a re-scoping would silently
falsify.
"""

import logging
from dataclasses import replace

import pytest
from cryptography import x509

import wool
from tests.helpers import LOOPBACK_SANS
from tests.helpers import generate_certificate_files
from wool import NoWorkersAvailable
from wool import WorkerCredentials
from wool import WorkerProxy

from .routines import add

_TRUST_DOMAIN = "wool.test"
_WORKER_IDENTITY = f"spiffe://{_TRUST_DOMAIN}/wool/worker"
_OTHER_IDENTITY = f"spiffe://{_TRUST_DOMAIN}/wool/other"


@pytest.fixture
def spiffe_cert_files(tmp_path):
    """Return a builder for a CA, key, and worker cert over one identity.

    The builder takes the SPIFFE workload identity to carry as the
    certificate's URI SAN, or ``None`` for a certificate carrying no URI
    SAN at all. The loopback addresses are always among the SANs so the
    worker's own stop RPC, which dials the address, still validates.
    Returns the CertificateFiles record.
    """

    def build(identity=None):
        sans = [*LOOPBACK_SANS]
        if identity is not None:
            sans.append(x509.UniformResourceIdentifier(identity))
        return generate_certificate_files(tmp_path, sans)

    return build


def _worker(files):
    """Build a worker serving the material in ``files``."""
    return wool.LocalWorker(
        credentials=WorkerCredentials(
            ca_cert=files.ca_pem,
            worker_key=files.key_pem,
            worker_cert=files.cert_pem,
        )
    )


def _advertising(metadata, identity):
    """Return ``metadata`` as it would read had the worker claimed ``identity``.

    The claim is injected into the advertisement rather than declared on
    the worker, because a worker declaring a SPIFFE identity would pin
    that URI on its own stop RPC — the very check these tests exist to
    show gRPC cannot perform — and take its teardown down with it. The
    client side is unchanged either way: a record carrying an identity
    is exactly what a declaring worker would put on the wire, and the
    proxy admits and pins from the record.
    """
    assert metadata is not None
    return replace(metadata, identity=identity)


def _client(files, peers):
    """Build a client provider from ``files`` accepting ``peers``."""
    return WorkerCredentials.from_files(
        files.ca_path, files.key_path, files.cert_path
    ).as_provider(peers=peers)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.xfail(
    reason=(
        "gRPC Python cannot verify URI SANs client-side, for two "
        "independent reasons: HostNameCertificateVerifier consults DNS "
        "and IP SANs and the CN only, never URI SANs; and a URI-form "
        "target name is mangled by host:port splitting before matching. "
        "A SPIFFE identity pinned via ssl_target_name_override therefore "
        "never matches, which blocks URI-SAN peer verification upstream. "
        "An unexpected pass means at least one of the two has been fixed; "
        "check which before declaring the limitation lifted."
    ),
    raises=NoWorkersAvailable,
    strict=True,
)
async def test_dispatch_should_succeed_when_identity_is_spiffe_uri(
    spiffe_cert_files, started_worker
):
    """Test mTLS verifies a worker by its SPIFFE URI SAN.

    Given:
        A worker whose certificate carries a SPIFFE URI workload
        identity, presented to the client through a record advertising
        that identity, and a client whose peers policy accepts it.
    When:
        A routine is dispatched at the worker through that client.
    Then:
        The client should admit the worker on its advertised identity,
        pin that identity for the connection, complete the mutual TLS
        handshake by matching it against the certificate's URI SAN, and
        the dispatch should succeed.

        Today it cannot: gRPC's client-side name check never consults
        URI SANs, so this is pinned as a strict xfail — an unexpected
        pass is the signal that upstream support arrived.
    """
    # Arrange
    files = spiffe_cert_files(_WORKER_IDENTITY)
    worker = await started_worker(_worker(files))
    advertised = _advertising(worker.metadata, _WORKER_IDENTITY)
    credentials = _client(files, _WORKER_IDENTITY)

    # Act
    async with WorkerProxy(workers=[advertised], credentials=credentials):
        result = await add(2, 3)

    # Assert
    assert result == 5


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_should_drain_when_the_cert_carries_another_spiffe_id(
    spiffe_cert_files, started_worker, caplog
):
    """Test a SPIFFE-identity pin rejects a trust-bundle-signed worker.

    Given:
        A running worker whose certificate chains to the shared trust
        bundle but carries a different SPIFFE workload identity than
        the record presented to the client claims, and a client whose
        peers policy accepts that claim.
    When:
        A routine is dispatched at the worker through that client.
    Then:
        The gate should admit the worker on its advertised identity,
        the handshake should then fail, the dispatch should drain to
        NoWorkersAvailable, and the proxy should log a diagnosable
        handshake warning.

        What this pins today is narrower than the arrangement suggests:
        gRPC matches no URI SAN at all, so any SPIFFE pin is rejected
        whether or not it matches. Trust in the bundle alone is never
        sufficient.
    """
    # Arrange — one CA; the worker cert carries a workload identity
    # other than the one its advertisement claims.
    files = spiffe_cert_files(_OTHER_IDENTITY)
    worker = await started_worker(_worker(files))
    advertised = _advertising(worker.metadata, _WORKER_IDENTITY)
    credentials = _client(files, _WORKER_IDENTITY)

    # Act & assert
    async with WorkerProxy(workers=[advertised], credentials=credentials):
        with caplog.at_level(logging.WARNING), pytest.raises(NoWorkersAvailable):
            await add(2, 3)

    # Assert
    assert "handshake" in caplog.text.lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_should_drain_to_no_workers_when_cert_carries_no_uri_san(
    spiffe_cert_files, started_worker, caplog
):
    """Test a SPIFFE pin is not silently discarded in favor of the address.

    Given:
        A running worker whose certificate carries the loopback
        addresses and no URI SAN at all, presented through a record
        claiming a SPIFFE identity its client accepts.
    When:
        A routine is dispatched at the worker through that client.
    Then:
        The dispatch should drain to NoWorkersAvailable, proving the
        pin is applied and unmatched rather than dropped.

        This is the control for the strict xfail above, whose worker
        cert also carries loopback SANs: without it, a future gRPC that
        discards an unparsable ssl_target_name_override and falls back
        to address verification would make that pin XPASS, and the
        result would be misread as upstream SPIFFE support.
    """
    # Arrange
    files = spiffe_cert_files()
    worker = await started_worker(_worker(files))
    advertised = _advertising(worker.metadata, _WORKER_IDENTITY)
    credentials = _client(files, _WORKER_IDENTITY)

    # Act & assert
    async with WorkerProxy(workers=[advertised], credentials=credentials):
        with caplog.at_level(logging.WARNING), pytest.raises(NoWorkersAvailable):
            await add(2, 3)

    # Assert
    assert "handshake" in caplog.text.lower()
