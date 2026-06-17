import datetime
import logging
import os
import threading
from dataclasses import FrozenInstanceError

import cloudpickle
import grpc
import pytest
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from hypothesis import HealthCheck
from hypothesis import assume
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.runtime.worker.auth import CredentialsProviderLike
from wool.runtime.worker.auth import CredentialsSnapshot
from wool.runtime.worker.auth import FileCredentialsProvider
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.auth import _StaticCredentialsProvider


def _generate_test_certificates():
    """Generate self-signed test certificates for SSL/TLS testing.

    Creates a certificate authority (CA) and worker certificate for
    localhost. These certificates are used for secure gRPC connections
    in tests.

    Returns:
        Tuple of (private_key_pem, certificate_pem, ca_cert_pem)
    """
    # Generate private key
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )

    # Create certificate subject
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
        ]
    )

    # Build self-signed certificate with both server and client auth
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.utcnow())
        .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
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

    # Serialize to PEM format
    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )

    cert_pem = cert.public_bytes(serialization.Encoding.PEM)

    return private_key_pem, cert_pem, cert_pem


@pytest.fixture(scope="module")
def test_certificates():
    """Provide test certificates for the test module.

    Returns:
        Tuple of (private_key_pem, certificate_pem, ca_cert_pem)
    """
    return _generate_test_certificates()


@pytest.fixture
def temp_cert_files(test_certificates, tmp_path):
    """Create temporary PEM certificate files.

    Args:
        test_certificates: Tuple of (key_pem, cert_pem, ca_pem)
        tmp_path: pytest tmp_path fixture

    Returns:
        Tuple of (ca_path, key_path, cert_path)
    """
    key_pem, cert_pem, ca_pem = test_certificates

    ca_path = tmp_path / "ca.pem"
    key_path = tmp_path / "key.pem"
    cert_path = tmp_path / "cert.pem"

    ca_path.write_bytes(ca_pem)
    key_path.write_bytes(key_pem)
    cert_path.write_bytes(cert_pem)

    return str(ca_path), str(key_path), str(cert_path)


class TestWorkerCredentials:
    """Test suite for WorkerCredentials credential management."""

    def test___init___with_mtls(self, test_certificates):
        """Test basic instantiation with mTLS.

        Given:
            CA cert, worker key, and worker cert as bytes.
        When:
            WorkerCredentials is instantiated with mutual=True.
        Then:
            Instance is created with all fields set correctly.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates

        # Act
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
        )

        # Assert
        assert creds.ca_cert == ca_pem
        assert creds.worker_key == key_pem
        assert creds.worker_cert == cert_pem
        assert creds.mutual is True

    def test___init___with_one_way_tls(self, test_certificates):
        """Test instantiation with one-way TLS.

        Given:
            CA cert, worker key, and worker cert as bytes.
        When:
            WorkerCredentials is instantiated with mutual=False.
        Then:
            Instance is created with mutual field set to False.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates

        # Act
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=False
        )

        # Assert
        assert creds.mutual is False

    def test___init___frozen_dataclass_immutability(self, test_certificates):
        """Test immutability via frozen dataclass.

        Given:
            WorkerCredentials instance is created.
        When:
            Attempting to modify fields.
        Then:
            FrozenInstanceError or AttributeError is raised.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
        )

        # Act & assert
        # Dataclasses raise FrozenInstanceError or AttributeError
        with pytest.raises((FrozenInstanceError, AttributeError)):
            creds.mutual = False

    def test_from_files_with_mtls(self, temp_cert_files):
        """Test from_files classmethod with mTLS.

        Given:
            Valid PEM file paths for CA, key, and cert.
        When:
            from_files() is called with mutual=True.
        Then:
            WorkerCredentials instance is created with loaded bytes.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files

        # Act
        creds = WorkerCredentials.from_files(
            ca_path=ca_path, key_path=key_path, cert_path=cert_path, mutual=True
        )

        # Assert
        assert isinstance(creds, WorkerCredentials)
        assert len(creds.ca_cert) > 0
        assert len(creds.worker_key) > 0
        assert len(creds.worker_cert) > 0
        assert creds.mutual is True

    def test_from_files_with_one_way_tls(self, temp_cert_files):
        """Test from_files classmethod with one-way TLS.

        Given:
            Valid PEM file paths for CA, key, and cert.
        When:
            from_files() is called with mutual=False.
        Then:
            WorkerCredentials instance is created with mutual=False.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files

        # Act
        creds = WorkerCredentials.from_files(
            ca_path=ca_path, key_path=key_path, cert_path=cert_path, mutual=False
        )

        # Assert
        assert creds.mutual is False

    def test_from_files_default_mutual_parameter(self, temp_cert_files):
        """Test default mutual=True parameter.

        Given:
            PEM files with valid TLS certificates.
        When:
            from_files() is called with default mutual parameter.
        Then:
            Instance is created with mutual=True (default).
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files

        # Act
        creds = WorkerCredentials.from_files(
            ca_path=ca_path, key_path=key_path, cert_path=cert_path
        )

        # Assert
        assert creds.mutual is True

    def test_from_files_missing_ca_cert(self, temp_cert_files):
        """Test missing CA file error handling.

        Given:
            Non-existent CA certificate file path.
        When:
            from_files() is called.
        Then:
            FileNotFoundError is raised.
        """
        # Arrange
        _, key_path, cert_path = temp_cert_files

        # Act & assert
        with pytest.raises(FileNotFoundError):
            WorkerCredentials.from_files(
                ca_path="/nonexistent/ca.pem", key_path=key_path, cert_path=cert_path
            )

    def test_from_files_missing_key(self, temp_cert_files):
        """Test missing key file error handling.

        Given:
            Non-existent worker key file path.
        When:
            from_files() is called.
        Then:
            FileNotFoundError is raised.
        """
        # Arrange
        ca_path, _, cert_path = temp_cert_files

        # Act & assert
        with pytest.raises(FileNotFoundError):
            WorkerCredentials.from_files(
                ca_path=ca_path, key_path="/nonexistent/key.pem", cert_path=cert_path
            )

    def test_from_files_missing_cert(self, temp_cert_files):
        """Test missing cert file error handling.

        Given:
            Non-existent worker cert file path.
        When:
            from_files() is called.
        Then:
            FileNotFoundError is raised.
        """
        # Arrange
        ca_path, key_path, _ = temp_cert_files

        # Act & assert
        with pytest.raises(FileNotFoundError):
            WorkerCredentials.from_files(
                ca_path=ca_path, key_path=key_path, cert_path="/nonexistent/cert.pem"
            )

    def test_from_files_permission_error(self, temp_cert_files, tmp_path):
        """Test permission error handling.

        Given:
            File path with insufficient read permissions.
        When:
            from_files() is called.
        Then:
            OSError is raised.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        restricted_file = tmp_path / "restricted.pem"
        restricted_file.write_bytes(b"dummy")
        restricted_file.chmod(0o000)

        # Act & assert
        try:
            with pytest.raises((OSError, PermissionError)):
                WorkerCredentials.from_files(
                    ca_path=str(restricted_file), key_path=key_path, cert_path=cert_path
                )
        finally:
            # Restore permissions for cleanup
            restricted_file.chmod(0o644)

    def test_provider_from_files_should_return_static_provider_when_reload_false(
        self, temp_cert_files
    ):
        """Test provider_from_files returns a static provider by default.

        Given:
            Valid PEM file paths.
        When:
            provider_from_files() is called with reload=False.
        Then:
            It should return a _StaticCredentialsProvider that satisfies the
            CredentialsProviderLike protocol.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files

        # Act
        provider = WorkerCredentials.provider_from_files(
            ca_path, key_path, cert_path, reload=False
        )

        # Assert
        assert isinstance(provider, _StaticCredentialsProvider)
        assert isinstance(provider, CredentialsProviderLike)

    def test_provider_from_files_should_return_file_provider_when_reload_true(
        self, temp_cert_files
    ):
        """Test provider_from_files returns a reloading provider on demand.

        Given:
            Valid PEM file paths.
        When:
            provider_from_files() is called with reload=True.
        Then:
            It should return a FileCredentialsProvider that satisfies the
            CredentialsProviderLike protocol.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files

        # Act
        provider = WorkerCredentials.provider_from_files(
            ca_path, key_path, cert_path, reload=True
        )

        # Assert
        assert isinstance(provider, FileCredentialsProvider)
        assert isinstance(provider, CredentialsProviderLike)

    def test_provider_from_files_should_carry_identity(self, temp_cert_files):
        """Test provider_from_files threads the expected identity through.

        Given:
            Valid PEM file paths and an expected identity.
        When:
            provider_from_files() is called with that identity.
        Then:
            The resolved snapshot should carry the identity.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files

        # Act
        provider = WorkerCredentials.provider_from_files(
            ca_path, key_path, cert_path, identity="wool-worker"
        )

        # Assert
        assert provider.resolve().identity == "wool-worker"

    @pytest.mark.parametrize("reload", [False, True])
    def test_provider_from_files_should_normalize_blank_identity(
        self, temp_cert_files, reload
    ):
        """Test a blank identity is normalized away by provider_from_files.

        Given:
            Valid PEM file paths and a whitespace-only identity, for both
            the static and reloading providers.
        When:
            provider_from_files() is called and the snapshot resolved.
        Then:
            The resolved identity should be None (address-based path).
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files

        # Act
        provider = WorkerCredentials.provider_from_files(
            ca_path, key_path, cert_path, identity="  ", reload=reload
        )

        # Assert
        assert provider.resolve().identity is None

    def test_provider_from_files_should_raise_when_static_and_file_missing(
        self, temp_cert_files
    ):
        """Test provider_from_files reads eagerly for a static provider.

        Given:
            A non-existent CA path with reload=False.
        When:
            provider_from_files() is called.
        Then:
            It should raise FileNotFoundError eagerly.
        """
        # Arrange
        _, key_path, cert_path = temp_cert_files

        # Act & assert
        with pytest.raises(FileNotFoundError):
            WorkerCredentials.provider_from_files(
                "/nonexistent/ca.pem", key_path, cert_path, reload=False
            )

    def test_provider_from_files_should_defer_reads_when_reload_true(self):
        """Test provider_from_files defers file reads for a reloading provider.

        Given:
            Non-existent PEM paths with reload=True.
        When:
            provider_from_files() is called.
        Then:
            It should not raise at construction; reads are deferred until
            the provider is first resolved.
        """
        # Act
        provider = WorkerCredentials.provider_from_files(
            "/nonexistent/ca.pem",
            "/nonexistent/key.pem",
            "/nonexistent/cert.pem",
            reload=True,
        )

        # Assert
        assert isinstance(provider, FileCredentialsProvider)

    def test_server_credentials_with_mtls(self, test_certificates):
        """Test server credentials property for mTLS.

        Given:
            WorkerCredentials with mutual=True.
        When:
            server_credentials() method is called.
        Then:
            Returns grpc.ServerCredentials configured for mTLS.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
        )

        # Act
        server_creds = creds.server_credentials()

        # Assert
        assert isinstance(server_creds, grpc.ServerCredentials)

    def test_server_credentials_with_one_way_tls(self, test_certificates):
        """Test server credentials property for one-way TLS.

        Given:
            WorkerCredentials with mutual=False.
        When:
            server_credentials() method is called.
        Then:
            Returns grpc.ServerCredentials configured for one-way TLS.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=False
        )

        # Act
        server_creds = creds.server_credentials()

        # Assert
        assert isinstance(server_creds, grpc.ServerCredentials)

    def test_client_credentials_with_mtls(self, test_certificates):
        """Test client credentials property for mTLS.

        Given:
            WorkerCredentials with mutual=True.
        When:
            client_credentials() method is called.
        Then:
            Returns grpc.ChannelCredentials with worker cert and key.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
        )

        # Act
        client_creds = creds.client_credentials()

        # Assert
        assert isinstance(client_creds, grpc.ChannelCredentials)

    def test_client_credentials_with_one_way_tls(self, test_certificates):
        """Test client credentials property for one-way TLS.

        Given:
            WorkerCredentials with mutual=False.
        When:
            client_credentials() method is called.
        Then:
            Returns grpc.ChannelCredentials without worker cert (anonymous).
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=False
        )

        # Act
        client_creds = creds.client_credentials()

        # Assert
        assert isinstance(client_creds, grpc.ChannelCredentials)

    def test_server_credentials_and_client_credentials_bidirectional(
        self, test_certificates
    ):
        """Test bidirectional credential generation.

        Given:
            Same certificate files used for server and client.
        When:
            Both server_credentials() and client_credentials() methods
            are called.
        Then:
            Both return valid credentials using the same underlying
            certificates.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
        )

        # Act
        server_creds = creds.server_credentials()
        client_creds = creds.client_credentials()

        # Assert
        assert isinstance(server_creds, grpc.ServerCredentials)
        assert isinstance(client_creds, grpc.ChannelCredentials)

    def test_server_credentials_idempotent_access(self, test_certificates):
        """Test server credentials method idempotency.

        Given:
            WorkerCredentials with valid certificates.
        When:
            server_credentials() method is called multiple times.
        Then:
            Returns consistent grpc.ServerCredentials on each access.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
        )

        # Act
        server_creds_1 = creds.server_credentials()
        server_creds_2 = creds.server_credentials()

        # Assert
        # Should return credentials each time (may not be same object)
        assert isinstance(server_creds_1, grpc.ServerCredentials)
        assert isinstance(server_creds_2, grpc.ServerCredentials)

    def test_client_credentials_idempotent_access(self, test_certificates):
        """Test client credentials method idempotency.

        Given:
            WorkerCredentials with valid certificates.
        When:
            client_credentials() method is called multiple times.
        Then:
            Returns consistent grpc.ChannelCredentials on each access.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
        )

        # Act
        client_creds_1 = creds.client_credentials()
        client_creds_2 = creds.client_credentials()

        # Assert
        # Should return credentials each time (may not be same object)
        assert isinstance(client_creds_1, grpc.ChannelCredentials)
        assert isinstance(client_creds_2, grpc.ChannelCredentials)

    @given(mutual=st.booleans())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_server_credentials_and_client_credentials_type_consistency(
        self, mutual, test_certificates
    ):
        """Test credential method idempotency across mutual flag values.

        Given:
            WorkerCredentials with valid certificates and any mutual
            flag value.
        When:
            server_credentials() and client_credentials() are called
            multiple times.
        Then:
            Both methods consistently return the same credential
            types.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=mutual
        )

        # Act
        server1 = creds.server_credentials()
        server2 = creds.server_credentials()
        client1 = creds.client_credentials()
        client2 = creds.client_credentials()

        # Assert
        assert isinstance(server1, grpc.ServerCredentials)
        assert isinstance(server2, grpc.ServerCredentials)
        assert isinstance(client1, grpc.ChannelCredentials)
        assert isinstance(client2, grpc.ChannelCredentials)
        assert type(server1) is type(server2)
        assert type(client1) is type(client2)

    @given(mutual=st.booleans())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_pickle_roundtrip(self, mutual, test_certificates):
        """Test WorkerCredentials survives pickle roundtrip.

        Given:
            WorkerCredentials with valid certificates and any mutual
            flag value.
        When:
            The instance is pickled and unpickled.
        Then:
            It should produce an equal instance that still builds
            valid gRPC credentials.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=mutual
        )

        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(creds))

        # Assert
        assert restored == creds
        assert isinstance(restored.server_credentials(), grpc.ServerCredentials)
        assert isinstance(restored.client_credentials(), grpc.ChannelCredentials)

    def test___enter___not_supported(self, test_certificates):
        """Test WorkerCredentials does not support context manager protocol.

        Given:
            A WorkerCredentials instance.
        When:
            Used in a with statement.
        Then:
            It should raise TypeError.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )

        # Act & assert
        with pytest.raises(TypeError):
            with creds:
                pass

    def test_current_not_supported(self):
        """Test WorkerCredentials does not expose current() classmethod.

        Given:
            The WorkerCredentials class.
        When:
            WorkerCredentials.current() is called.
        Then:
            It should raise AttributeError.
        """
        # Act & assert
        with pytest.raises(AttributeError):
            WorkerCredentials.current()


class TestCredentialsSnapshot:
    """Test suite for CredentialsSnapshot fingerprinting."""

    def test_of_should_compute_fingerprint(self, test_certificates):
        """Test snapshot construction computes a fingerprint.

        Given:
            Credential material and an expected identity.
        When:
            CredentialsSnapshot.of() is called.
        Then:
            It should return a snapshot carrying the material, identity, and
            a non-empty fingerprint.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )

        # Act
        snapshot = CredentialsSnapshot.of(creds, "wool-worker")

        # Assert
        assert snapshot.credentials == creds
        assert snapshot.identity == "wool-worker"
        assert snapshot.fingerprint

    @given(
        ca=st.binary(),
        key=st.binary(),
        cert=st.binary(),
        identity=st.one_of(st.none(), st.text()),
    )
    def test_of_should_compute_identical_fingerprint_when_material_identical(
        self, ca, key, cert, identity
    ):
        """Test fingerprint stability across identical material.

        Given:
            Any two credential instances built from the same bytes, mutual
            flag, and identity.
        When:
            Each is snapshotted.
        Then:
            Both snapshots should share the same fingerprint.
        """
        # Arrange
        creds_a = WorkerCredentials(ca_cert=ca, worker_key=key, worker_cert=cert)
        creds_b = WorkerCredentials(ca_cert=ca, worker_key=key, worker_cert=cert)

        # Act
        fingerprint_a = CredentialsSnapshot.of(creds_a, identity).fingerprint
        fingerprint_b = CredentialsSnapshot.of(creds_b, identity).fingerprint

        # Assert
        assert fingerprint_a == fingerprint_b

    @given(ca=st.binary(), other_ca=st.binary(), key=st.binary(), cert=st.binary())
    def test_of_should_change_fingerprint_when_material_differs(
        self, ca, other_ca, key, cert
    ):
        """Test fingerprint sensitivity to material changes.

        Given:
            Two credential instances differing only in their CA bytes.
        When:
            Each is snapshotted.
        Then:
            Their fingerprints should differ.
        """
        # Arrange
        assume(ca != other_ca)
        creds = WorkerCredentials(ca_cert=ca, worker_key=key, worker_cert=cert)
        rotated = WorkerCredentials(ca_cert=other_ca, worker_key=key, worker_cert=cert)

        # Act
        fingerprint = CredentialsSnapshot.of(creds).fingerprint
        rotated_fingerprint = CredentialsSnapshot.of(rotated).fingerprint

        # Assert
        assert fingerprint != rotated_fingerprint

    def test_of_should_change_fingerprint_when_identity_differs(self, test_certificates):
        """Test fingerprint sensitivity to identity changes.

        Given:
            One credential instance and two distinct identities.
        When:
            Each (material, identity) pair is snapshotted.
        Then:
            Their fingerprints should differ.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )

        # Act
        fingerprint_a = CredentialsSnapshot.of(creds, "worker-a").fingerprint
        fingerprint_b = CredentialsSnapshot.of(creds, "worker-b").fingerprint

        # Assert
        assert fingerprint_a != fingerprint_b

    def test_of_should_change_fingerprint_when_mutual_differs(self, test_certificates):
        """Test fingerprint sensitivity to the mutual-TLS flag.

        Given:
            The same bytes under mutual=True and mutual=False.
        When:
            Each is snapshotted.
        Then:
            Their fingerprints should differ.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        mtls = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
        )
        one_way = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=False
        )

        # Act
        mtls_fingerprint = CredentialsSnapshot.of(mtls).fingerprint
        one_way_fingerprint = CredentialsSnapshot.of(one_way).fingerprint

        # Assert
        assert mtls_fingerprint != one_way_fingerprint

    def test_pickle_roundtrip(self, test_certificates):
        """Test CredentialsSnapshot survives a pickle roundtrip.

        Given:
            A snapshot of valid credential material.
        When:
            It is pickled and unpickled.
        Then:
            It should produce an equal snapshot.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        snapshot = CredentialsSnapshot.of(creds, "wool-worker")

        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(snapshot))

        # Assert
        assert restored == snapshot


class TestStaticCredentialsProvider:
    """Test suite for _StaticCredentialsProvider."""

    def test_resolve_should_return_constant_snapshot(self, test_certificates):
        """Test a static provider resolves to a constant snapshot.

        Given:
            A static provider over fixed credential material.
        When:
            resolve() is called more than once.
        Then:
            It should return the same snapshot instance each time.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = _StaticCredentialsProvider(creds, identity="wool-worker")

        # Act
        first = provider.resolve()
        second = provider.resolve()

        # Assert
        assert first is second
        assert first.credentials == creds
        assert first.identity == "wool-worker"

    def test_resolve_should_default_identity_to_none(self, test_certificates):
        """Test a static provider defaults to address-based verification.

        Given:
            A static provider constructed without an identity.
        When:
            resolve() is called.
        Then:
            The snapshot identity should be None.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = _StaticCredentialsProvider(creds)

        # Act
        snapshot = provider.resolve()

        # Assert
        assert snapshot.identity is None

    def test_reloadable_should_be_false(self, test_certificates):
        """Test a static provider reports itself non-reloadable.

        Given:
            A static provider.
        When:
            Its reloadable flag is read.
        Then:
            It should be False, so a worker built from it serves fixed
            credentials.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )

        # Act & assert
        assert _StaticCredentialsProvider(creds).reloadable is False

    def test_resolve_should_normalize_blank_identity_to_none(self, test_certificates):
        """Test a blank identity collapses to address-based verification.

        Given:
            A static provider constructed with a whitespace-only identity.
        When:
            resolve() is called.
        Then:
            The snapshot identity (and the provider's identity) should be
            None, taking the address-based path rather than emitting an
            empty target-name override.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = _StaticCredentialsProvider(creds, identity="   ")

        # Act & assert
        assert provider.identity is None
        assert provider.resolve().identity is None

    @given(mutual=st.booleans())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_pickle_roundtrip(self, mutual, test_certificates):
        """Test _StaticCredentialsProvider survives a pickle roundtrip.

        Given:
            A static provider over valid material and any mutual flag.
        When:
            It is pickled and unpickled.
        Then:
            It should produce an equal provider resolving to the same
            fingerprint.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=mutual
        )
        provider = _StaticCredentialsProvider(creds, identity="wool-worker")

        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(provider))

        # Assert
        assert restored == provider
        assert restored.resolve().fingerprint == provider.resolve().fingerprint


class TestFileCredentialsProvider:
    """Test suite for FileCredentialsProvider reloading behavior."""

    def test_resolve_should_read_material_from_files(self, temp_cert_files):
        """Test a file provider resolves material from disk.

        Given:
            A file provider over valid PEM paths and an identity.
        When:
            resolve() is called.
        Then:
            It should return a snapshot whose material matches the files and
            whose identity matches the configured identity.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        provider = FileCredentialsProvider(
            ca_path, key_path, cert_path, identity="wool-worker"
        )
        with open(ca_path, "rb") as f:
            expected_ca = f.read()

        # Act
        snapshot = provider.resolve()

        # Assert
        assert snapshot.credentials.ca_cert == expected_ca
        assert snapshot.identity == "wool-worker"

    def test_identity_should_expose_configured_identity(self, temp_cert_files):
        """Test the identity property reflects construction.

        Given:
            A file provider constructed with an identity.
        When:
            The identity property is read.
        Then:
            It should equal the configured identity.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files

        # Act
        provider = FileCredentialsProvider(
            ca_path, key_path, cert_path, identity="wool-worker"
        )

        # Assert
        assert provider.identity == "wool-worker"

    def test_reloadable_should_be_true(self, temp_cert_files):
        """Test a file provider reports itself reloadable.

        Given:
            A file provider.
        When:
            Its reloadable flag is read.
        Then:
            It should be True, so a worker built from it serves rotating
            credentials via a per-handshake fetcher.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files

        # Act & assert
        assert FileCredentialsProvider(ca_path, key_path, cert_path).reloadable is True

    def test_identity_should_normalize_blank_to_none(self, temp_cert_files):
        """Test a blank identity collapses to address-based verification.

        Given:
            A file provider constructed with an empty identity.
        When:
            The identity is read and the snapshot resolved.
        Then:
            Both should be None, taking the address-based path.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files

        # Act
        provider = FileCredentialsProvider(ca_path, key_path, cert_path, identity="")

        # Assert
        assert provider.identity is None
        assert provider.resolve().identity is None

    def test_resolve_should_reuse_snapshot_when_files_unchanged(self, temp_cert_files):
        """Test a file provider caches the snapshot for unchanged files.

        Given:
            A file provider whose files are not modified between calls.
        When:
            resolve() is called twice.
        Then:
            It should return the same snapshot instance.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        provider = FileCredentialsProvider(ca_path, key_path, cert_path)

        # Act
        first = provider.resolve()
        second = provider.resolve()

        # Assert
        assert first is second

    def test_resolve_should_reread_material_when_files_change(self, temp_cert_files):
        """Test a file provider adopts rotated material.

        Given:
            A file provider that has resolved once, then whose CA file is
            rewritten with different bytes.
        When:
            resolve() is called again.
        Then:
            It should return a new snapshot with a different fingerprint.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        provider = FileCredentialsProvider(ca_path, key_path, cert_path)
        before = provider.resolve()

        # Act
        rotated_mtime = os.stat(ca_path).st_mtime_ns + 1_000_000_000
        with open(ca_path, "wb") as f:
            f.write(b"-----ROTATED CA-----\n" + before.credentials.ca_cert)
        os.utime(ca_path, ns=(rotated_mtime, rotated_mtime))
        after = provider.resolve()

        # Assert
        assert after is not before
        assert after.fingerprint != before.fingerprint

    def test_resolve_should_return_last_good_when_reread_fails(
        self, temp_cert_files, caplog
    ):
        """Test a file provider survives a failed re-read mid-rotation.

        Given:
            A file provider that has resolved once, then whose CA file is
            replaced with unreadable content (a partial-write surrogate).
        When:
            resolve() is called again.
        Then:
            It should return the last good snapshot rather than raising, and
            log a warning so the stuck rotation is diagnosable.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        provider = FileCredentialsProvider(ca_path, key_path, cert_path)
        before = provider.resolve()

        # Act
        rotated_mtime = os.stat(ca_path).st_mtime_ns + 1_000_000_000
        with open(ca_path, "wb") as f:
            f.write(b"partial")
        os.utime(ca_path, ns=(rotated_mtime, rotated_mtime))
        os.chmod(ca_path, 0o000)
        try:
            with caplog.at_level(logging.WARNING):
                after = provider.resolve()
        finally:
            os.chmod(ca_path, 0o644)

        # Assert
        assert after is before
        assert "reload failed" in caplog.text.lower()

    def test_resolve_should_keep_last_good_when_material_invalid(self, temp_cert_files):
        """Test a readable-but-malformed rotation never overwrites good material.

        Given:
            A file provider that has resolved once, then whose certificate
            file is replaced with a readable but malformed PEM.
        When:
            resolve() is called again.
        Then:
            It should keep the prior good snapshot rather than caching the
            broken material (which would only fail later at the handshake).
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        provider = FileCredentialsProvider(ca_path, key_path, cert_path)
        before = provider.resolve()

        # Act
        rotated_mtime = os.stat(cert_path).st_mtime_ns + 1_000_000_000
        with open(cert_path, "wb") as f:
            f.write(
                b"-----BEGIN CERTIFICATE-----\nnot base64!!\n-----END CERTIFICATE-----\n"
            )
        os.utime(cert_path, ns=(rotated_mtime, rotated_mtime))
        after = provider.resolve()

        # Assert
        assert after is before

    def test_resolve_should_detect_same_size_rewrite_with_reset_mtime(
        self, temp_cert_files
    ):
        """Test an in-place rewrite is detected even when mtime is reset.

        Given:
            A file provider over a CA file, and a same-size in-place rewrite
            whose mtime is forced back to the prior value (the (mtime,size)
            spoof an attacker or a non-atomic writer could produce).
        When:
            resolve() is called again.
        Then:
            It should still detect the change and return a new snapshot,
            because the change signature also tracks ctime/inode.
        """
        # Arrange — two same-size payloads differing only in trailing bytes
        # outside the PEM block (ignored by the parser, so both validate).
        ca_path, key_path, cert_path = temp_cert_files
        with open(ca_path, "rb") as f:
            ca_pem = f.read()
        with open(ca_path, "wb") as f:
            f.write(ca_pem + b"\n" * 16)
        provider = FileCredentialsProvider(ca_path, key_path, cert_path)
        before = provider.resolve()
        original_mtime = os.stat(ca_path).st_mtime_ns

        # Act
        with open(ca_path, "wb") as f:
            f.write(ca_pem + b"\n" * 15 + b"#")
        os.utime(ca_path, ns=(original_mtime, original_mtime))
        after = provider.resolve()

        # Assert
        assert after.fingerprint != before.fingerprint

    def test_resolve_should_be_thread_safe_under_rotation(self, temp_cert_files):
        """Test concurrent resolves during rotation stay consistent.

        Given:
            A file provider resolved concurrently from several threads while
            its CA file is rewritten repeatedly.
        When:
            Each thread resolves many times.
        Then:
            No resolve should raise, and every returned snapshot should be
            internally consistent (its material recomputes its fingerprint).
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        with open(ca_path, "rb") as f:
            ca_pem = f.read()
        provider = FileCredentialsProvider(ca_path, key_path, cert_path)
        provider.resolve()  # establish a last-good snapshot before rotating
        errors: list[Exception] = []
        snapshots: list = []

        def resolver():
            try:
                for _ in range(25):
                    snapshots.append(provider.resolve())
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        def rotator():
            # Rotate atomically (write-temp + os.replace), as a real rotator
            # would, so a reader never observes a torn file.
            for i in range(25):
                tmp = f"{ca_path}.{i}.tmp"
                with open(tmp, "wb") as f:
                    f.write(ca_pem + b"\n" * (i % 8))
                os.replace(tmp, ca_path)

        # Act
        threads = [threading.Thread(target=resolver) for _ in range(6)]
        threads.append(threading.Thread(target=rotator))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Assert
        assert not errors
        for snapshot in snapshots:
            recomputed = CredentialsSnapshot.of(snapshot.credentials, snapshot.identity)
            assert snapshot.fingerprint == recomputed.fingerprint

    def test_resolve_should_raise_when_first_read_fails(self):
        """Test a file provider propagates an initial read failure.

        Given:
            A file provider over non-existent PEM paths with no prior
            snapshot.
        When:
            resolve() is called.
        Then:
            It should raise FileNotFoundError.
        """
        # Arrange
        provider = FileCredentialsProvider(
            "/nonexistent/ca.pem",
            "/nonexistent/key.pem",
            "/nonexistent/cert.pem",
        )

        # Act & assert
        with pytest.raises(FileNotFoundError):
            provider.resolve()

    def test_pickle_roundtrip_should_reread_files(self, temp_cert_files):
        """Test a pickled file provider re-reads the live files.

        Given:
            A file provider that has resolved once and is then pickled and
            unpickled.
        When:
            The restored provider is resolved.
        Then:
            It should resolve to the same fingerprint by re-reading the
            unchanged files.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        provider = FileCredentialsProvider(
            ca_path, key_path, cert_path, identity="wool-worker"
        )
        original = provider.resolve()

        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(provider))

        # Assert
        assert restored.resolve().fingerprint == original.fingerprint
