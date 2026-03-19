import datetime
import pickle
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
        assert type(server1) == type(server2)
        assert type(client1) == type(client2)

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
        restored = pickle.loads(pickle.dumps(creds))

        # Assert
        assert restored == creds
        assert isinstance(restored.server_credentials(), grpc.ServerCredentials)
        assert isinstance(restored.client_credentials(), grpc.ChannelCredentials)
