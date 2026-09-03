"""End-to-end integration tests for verification by worker identity.

These tests spawn real workers over ephemeral loopback addresses and
exercise identity as an advertised property: a worker claims a logical
name through discovery, and a client accepting a set of names verifies
the one the worker advertised at the mutual TLS handshake.

Verification here is outbound only — who a client will dial. A worker
still serves any caller holding a certificate from its authority.

The certificates chain to one authority and differ only in the names
they carry, so nothing under test can be satisfied by trusting the
authority alone.
"""

import logging
from dataclasses import replace

import pytest
from cryptography import x509

import wool
from tests.helpers import generate_authority
from tests.helpers import generate_certificate_files
from wool import NoWorkersAvailable
from wool import WorkerCredentials
from wool import WorkerProxy

from .routines import add

_ALPHA = "alpha.wool.test"
_BETA = "beta.wool.test"
_UNTRUSTED = "rogue.wool.test"


@pytest.fixture
def authority():
    """Return a certificate authority shared by every peer in a test."""
    return generate_authority()


@pytest.fixture
def issue(tmp_path, authority):
    """Return a builder issuing peer certificates under the shared authority.

    The builder takes the logical identity to carry as a DNS subject
    alternative name and writes a distinct PEM set per call. The
    certificate carries that name and nothing else: the workers bind
    loopback, so a leaf that also carried the loopback addresses would
    verify against the dialed address whenever the identity pin was
    absent, and a test asserting that dispatch succeeds could not tell
    identity verification from address fallback. A worker's own stop RPC
    still validates, since it pins the identity the worker declares
    rather than the address it listens on.
    """
    count = 0

    def build(identity):
        nonlocal count
        count += 1
        directory = tmp_path / f"peer-{count}"
        directory.mkdir()
        return generate_certificate_files(
            directory,
            [x509.DNSName(identity)],
            common_name=identity,
            authority=authority,
        )

    return build


async def _worker(started_worker, files, identity):
    """Start a worker holding ``files`` and claiming ``identity``."""
    return await started_worker(
        wool.LocalWorker(
            identity=identity,
            credentials=WorkerCredentials(
                ca_cert=files.ca_pem,
                worker_key=files.key_pem,
                worker_cert=files.cert_pem,
            ),
        )
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_should_succeed_when_identity_among_accepted(
    issue, started_worker
):
    """Test a client accepting several identities dials a worker claiming one.

    Given:
        A worker claiming one identity, and a client that accepts that
        identity alongside another.
    When:
        A routine is dispatched at the worker through that client.
    Then:
        The client should verify the worker against the identity it
        advertised — one of several accepted — rather than the dialed
        address, and the dispatch should succeed.
    """
    # Arrange
    files = issue(_ALPHA)
    worker = await _worker(started_worker, files, _ALPHA)
    credentials = WorkerCredentials.from_files(
        files.ca_path, files.key_path, files.cert_path
    ).as_provider(peers={_ALPHA, _BETA})

    # Act
    async with WorkerProxy(workers=[worker.metadata], credentials=credentials):
        result = await add(2, 3)

    # Assert
    assert result == 5


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_should_succeed_when_identity_matches_predicate(
    issue, started_worker
):
    """Test acceptance expressed as a predicate admits a matching worker.

    Given:
        A worker claiming an identity, and a client accepting any
        identity under the same domain by predicate rather than by
        enumeration.
    When:
        A routine is dispatched at the worker through that client.
    Then:
        The dispatch should succeed, the predicate having admitted the
        advertised identity that the handshake then verified.
    """
    # Arrange
    files = issue(_ALPHA)
    worker = await _worker(started_worker, files, _ALPHA)
    credentials = WorkerCredentials.from_files(
        files.ca_path, files.key_path, files.cert_path
    ).as_provider(peers=lambda name: name.endswith(".wool.test"))

    # Act
    async with WorkerProxy(workers=[worker.metadata], credentials=credentials):
        result = await add(2, 3)

    # Assert
    assert result == 5


@pytest.mark.integration
@pytest.mark.asyncio
async def test___init___should_raise_when_identity_not_accepted(issue, started_worker):
    """Test a trust-bundle-signed worker outside the accepted set is refused.

    Given:
        A worker whose certificate chains to the shared authority but
        claims an identity the client does not accept.
    When:
        A WorkerProxy is constructed over that worker alone.
    Then:
        It should raise ValueError naming identity filtering, since the
        worker is gated out before any connection is attempted and the
        quorum can never be satisfied — a shared certificate authority
        is never sufficient on its own.
    """
    # Arrange
    files = issue(_UNTRUSTED)
    worker = await _worker(started_worker, files, _UNTRUSTED)
    credentials = WorkerCredentials.from_files(
        files.ca_path, files.key_path, files.cert_path
    ).as_provider(peers={_ALPHA, _BETA})

    # Act & assert
    with pytest.raises(ValueError, match="identity filtering"):
        WorkerProxy(workers=[worker.metadata], credentials=credentials)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_should_drain_to_no_workers_when_identity_is_forged(
    issue, started_worker, caplog
):
    """Test claiming an accepted identity does not confer it.

    Given:
        A worker holding a certificate for an unaccepted identity,
        whose advertisement has been rewritten to claim an accepted one
        — the forgery available to anything that can write to a
        discovery plane the deployment has not authenticated.
    When:
        A routine is dispatched at the worker through that client.
    Then:
        The forged advertisement should admit the worker but the
        handshake should reject it, draining to NoWorkersAvailable with
        a diagnosable warning.

        This is the property the whole design rests on: an
        advertisement only selects which name is verified, and can
        never widen what a client accepts. Security therefore does not
        depend on the discovery plane being trustworthy — only
        availability does.
    """
    # Arrange — the worker is honest about itself; the advertisement the
    # client receives is what has been tampered with.
    files = issue(_UNTRUSTED)
    worker = await _worker(started_worker, files, _UNTRUSTED)
    assert worker.metadata is not None
    forged = replace(worker.metadata, identity=_ALPHA)
    credentials = WorkerCredentials.from_files(
        files.ca_path, files.key_path, files.cert_path
    ).as_provider(peers={_ALPHA, _BETA})

    # Act & assert
    async with WorkerProxy(workers=[forged], credentials=credentials):
        with caplog.at_level(logging.WARNING), pytest.raises(NoWorkersAvailable):
            await add(2, 3)

    # Assert — admitted by the gate, refused by the handshake.
    assert "handshake" in caplog.text.lower()
