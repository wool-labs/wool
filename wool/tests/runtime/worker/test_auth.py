import datetime
from dataclasses import FrozenInstanceError

import grpc
import pytest
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.runtime.worker.auth import WorkerCredentials

# Module-level certificate storage for test reuse
_test_certs = None


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
    global _test_certs
    if _test_certs is None:
        _test_certs = _generate_test_certificates()
    return _test_certs


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

    # === WTC-001 through WTC-006: Basic instantiation tests ===

    def test_instantiation_with_mtls(self, test_certificates):
        """WTC-001: Basic instantiation with mTLS.

        GIVEN CA cert, worker key, and worker cert as bytes
        WHEN WorkerCredentials is instantiated with mutual=True
        THEN Instance is created with all fields set correctly
        """
        key_pem, cert_pem, ca_pem = test_certificates

        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
        )

        assert creds.ca_cert == ca_pem
        assert creds.worker_key == key_pem
        assert creds.worker_cert == cert_pem
        assert creds.mutual is True

    def test_instantiation_with_one_way_tls(self, test_certificates):
        """WTC-002: Instantiation with one-way TLS.

        GIVEN CA cert, worker key, and worker cert as bytes
        WHEN WorkerCredentials is instantiated with mutual=False
        THEN Instance is created with mutual field set to False
        """
        key_pem, cert_pem, ca_pem = test_certificates

        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=False
        )

        assert creds.mutual is False

    def test_immutability_via_frozen_dataclass(self, test_certificates):
        """WTC-003: Immutability via frozen dataclass.

        GIVEN WorkerCredentials instance is created
        WHEN Attempting to modify fields
        THEN FrozenInstanceError or AttributeError is raised
        """
        key_pem, cert_pem, ca_pem = test_certificates

        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
        )

        # Dataclasses raise FrozenInstanceError or AttributeError
        with pytest.raises((FrozenInstanceError, AttributeError)):
            creds.mutual = False

    def test_from_files_with_mtls(self, temp_cert_files):
        """WTC-004: from_files classmethod with mTLS.

        GIVEN Valid PEM file paths for CA, key, and cert
        WHEN from_files() is called with mutual=True
        THEN WorkerCredentials instance is created with loaded bytes
        """
        ca_path, key_path, cert_path = temp_cert_files

        creds = WorkerCredentials.from_files(
            ca_path=ca_path, key_path=key_path, cert_path=cert_path, mutual=True
        )

        assert isinstance(creds, WorkerCredentials)
        assert len(creds.ca_cert) > 0
        assert len(creds.worker_key) > 0
        assert len(creds.worker_cert) > 0
        assert creds.mutual is True

    def test_from_files_with_one_way_tls(self, temp_cert_files):
        """WTC-005: from_files classmethod with one-way TLS.

        GIVEN Valid PEM file paths for CA, key, and cert
        WHEN from_files() is called with mutual=False
        THEN WorkerCredentials instance is created with mutual=False
        """
        ca_path, key_path, cert_path = temp_cert_files

        creds = WorkerCredentials.from_files(
            ca_path=ca_path, key_path=key_path, cert_path=cert_path, mutual=False
        )

        assert creds.mutual is False

    def test_from_files_default_mutual_parameter(self, temp_cert_files):
        """WTC-006: Default mutual=True parameter.

        GIVEN PEM files with valid TLS certificates
        WHEN from_files() is called with default mutual parameter
        THEN Instance is created with mutual=True (default)
        """
        ca_path, key_path, cert_path = temp_cert_files

        creds = WorkerCredentials.from_files(
            ca_path=ca_path, key_path=key_path, cert_path=cert_path
        )

        assert creds.mutual is True

    # === WTC-007 through WTC-010: Error handling ===

    def test_from_files_missing_ca_cert(self, temp_cert_files):
        """WTC-007: Missing CA file error handling.

        GIVEN Non-existent CA certificate file path
        WHEN from_files() is called
        THEN FileNotFoundError is raised
        """
        _, key_path, cert_path = temp_cert_files

        with pytest.raises(FileNotFoundError):
            WorkerCredentials.from_files(
                ca_path="/nonexistent/ca.pem", key_path=key_path, cert_path=cert_path
            )

    def test_from_files_missing_key(self, temp_cert_files):
        """WTC-008: Missing key file error handling.

        GIVEN Non-existent worker key file path
        WHEN from_files() is called
        THEN FileNotFoundError is raised
        """
        ca_path, _, cert_path = temp_cert_files

        with pytest.raises(FileNotFoundError):
            WorkerCredentials.from_files(
                ca_path=ca_path, key_path="/nonexistent/key.pem", cert_path=cert_path
            )

    def test_from_files_missing_cert(self, temp_cert_files):
        """WTC-009: Missing cert file error handling.

        GIVEN Non-existent worker cert file path
        WHEN from_files() is called
        THEN FileNotFoundError is raised
        """
        ca_path, key_path, _ = temp_cert_files

        with pytest.raises(FileNotFoundError):
            WorkerCredentials.from_files(
                ca_path=ca_path, key_path=key_path, cert_path="/nonexistent/cert.pem"
            )

    def test_from_files_permission_error(self, temp_cert_files, tmp_path):
        """WTC-010: Permission error handling.

        GIVEN File path with insufficient read permissions
        WHEN from_files() is called
        THEN OSError is raised
        """
        ca_path, key_path, cert_path = temp_cert_files

        # Create a file with no read permissions
        restricted_file = tmp_path / "restricted.pem"
        restricted_file.write_bytes(b"dummy")
        restricted_file.chmod(0o000)

        try:
            with pytest.raises((OSError, PermissionError)):
                WorkerCredentials.from_files(
                    ca_path=str(restricted_file), key_path=key_path, cert_path=cert_path
                )
        finally:
            # Restore permissions for cleanup
            restricted_file.chmod(0o644)

    # === WTC-011 through WTC-017: Credential property tests ===

    def test_server_credentials_property_mtls(self, test_certificates):
        """WTC-011: Server credentials property for mTLS.

        GIVEN WorkerCredentials with mutual=True
        WHEN server_credentials property is accessed
        THEN Returns grpc.ServerCredentials configured for mTLS
        """
        key_pem, cert_pem, ca_pem = test_certificates

        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
        )

        server_creds = creds.server_credentials

        assert isinstance(server_creds, grpc.ServerCredentials)

    def test_server_credentials_property_one_way_tls(self, test_certificates):
        """WTC-012: Server credentials property for one-way TLS.

        GIVEN WorkerCredentials with mutual=False
        WHEN server_credentials property is accessed
        THEN Returns grpc.ServerCredentials configured for one-way TLS
        """
        key_pem, cert_pem, ca_pem = test_certificates

        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=False
        )

        server_creds = creds.server_credentials

        assert isinstance(server_creds, grpc.ServerCredentials)

    def test_client_credentials_property_mtls(self, test_certificates):
        """WTC-013: Client credentials property for mTLS.

        GIVEN WorkerCredentials with mutual=True
        WHEN client_credentials property is accessed
        THEN Returns grpc.ChannelCredentials with worker cert and key
        """
        key_pem, cert_pem, ca_pem = test_certificates

        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
        )

        client_creds = creds.client_credentials

        assert isinstance(client_creds, grpc.ChannelCredentials)

    def test_client_credentials_property_one_way_tls(self, test_certificates):
        """WTC-014: Client credentials property for one-way TLS.

        GIVEN WorkerCredentials with mutual=False
        WHEN client_credentials property is accessed
        THEN Returns grpc.ChannelCredentials without worker cert (anonymous)
        """
        key_pem, cert_pem, ca_pem = test_certificates

        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=False
        )

        client_creds = creds.client_credentials

        assert isinstance(client_creds, grpc.ChannelCredentials)

    def test_bidirectional_credential_generation(self, test_certificates):
        """WTC-015: Bidirectional credential generation.

        GIVEN Same certificate files used for server and client
        WHEN Both server_credentials and client_credentials properties are accessed
        THEN Both return valid credentials using the same underlying certificates
        """
        key_pem, cert_pem, ca_pem = test_certificates

        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
        )

        server_creds = creds.server_credentials
        client_creds = creds.client_credentials

        assert isinstance(server_creds, grpc.ServerCredentials)
        assert isinstance(client_creds, grpc.ChannelCredentials)

    def test_server_credentials_property_idempotency(self, test_certificates):
        """WTC-016: Server credentials property idempotency.

        GIVEN WorkerCredentials with valid certificates
        WHEN server_credentials property is accessed multiple times
        THEN Returns consistent grpc.ServerCredentials on each access
        """
        key_pem, cert_pem, ca_pem = test_certificates

        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
        )

        server_creds_1 = creds.server_credentials
        server_creds_2 = creds.server_credentials

        # Should return credentials each time (may not be same object)
        assert isinstance(server_creds_1, grpc.ServerCredentials)
        assert isinstance(server_creds_2, grpc.ServerCredentials)

    def test_client_credentials_property_idempotency(self, test_certificates):
        """WTC-017: Client credentials property idempotency.

        GIVEN WorkerCredentials with valid certificates
        WHEN client_credentials property is accessed multiple times
        THEN Returns consistent grpc.ChannelCredentials on each access
        """
        key_pem, cert_pem, ca_pem = test_certificates

        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
        )

        client_creds_1 = creds.client_credentials
        client_creds_2 = creds.client_credentials

        # Should return credentials each time (may not be same object)
        assert isinstance(client_creds_1, grpc.ChannelCredentials)
        assert isinstance(client_creds_2, grpc.ChannelCredentials)

    # === WTC-018: Property-based test for credential property idempotency ===

    @given(mutual=st.booleans())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_credential_properties_idempotency(self, mutual, test_certificates):
        """WTC-018: Property-based test for credential property idempotency.

        GIVEN WorkerCredentials with valid certificates and any mutual flag value
        WHEN server_credentials and client_credentials are accessed multiple times
        THEN Both properties consistently return the same credential types
        """
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=mutual
        )

        # Property: Multiple accesses return consistent types
        server1 = creds.server_credentials
        server2 = creds.server_credentials
        client1 = creds.client_credentials
        client2 = creds.client_credentials

        # Type consistency check
        assert isinstance(server1, grpc.ServerCredentials)
        assert isinstance(server2, grpc.ServerCredentials)
        assert isinstance(client1, grpc.ChannelCredentials)
        assert isinstance(client2, grpc.ChannelCredentials)

        # Verify they're the same type (both should be consistent)
        assert type(server1) == type(server2)
        assert type(client1) == type(client2)
