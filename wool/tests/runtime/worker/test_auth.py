import datetime
import functools
import pickle
from dataclasses import FrozenInstanceError
from dataclasses import replace
from pathlib import Path

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
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.runtime.worker.auth import CredentialsContext
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.auth import WorkerCredentialsProvider


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

    def test_as_provider_should_return_non_reloadable_provider(self, temp_cert_files):
        """Test as_provider wraps fixed credentials in a static provider.

        Given:
            Credentials loaded from PEM files.
        When:
            as_provider() is called.
        Then:
            It should return a non-reloadable WorkerCredentialsProvider whose
            snapshot carries the same material.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        credentials = WorkerCredentials.from_files(ca_path, key_path, cert_path)

        # Act
        provider = credentials.as_provider()

        # Assert
        assert isinstance(provider, WorkerCredentialsProvider)
        assert provider.reloadable is False
        assert provider.resolve() == credentials

    def test_as_provider_should_carry_identity(self, temp_cert_files):
        """Test as_provider threads the expected identity through.

        Given:
            Credentials loaded from PEM files and an expected identity.
        When:
            as_provider() is called with that identity.
        Then:
            The resolved snapshot should carry the identity.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        credentials = WorkerCredentials.from_files(ca_path, key_path, cert_path)

        # Act
        provider = credentials.as_provider(identity="wool-worker")

        # Assert
        assert provider.resolve().identity == "wool-worker"

    def test_as_provider_should_normalize_blank_identity(self, temp_cert_files):
        """Test a blank identity is normalized away by as_provider.

        Given:
            Credentials and a whitespace-only identity.
        When:
            as_provider() is called and the snapshot resolved.
        Then:
            The resolved identity should be None (address-based path).
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        credentials = WorkerCredentials.from_files(ca_path, key_path, cert_path)

        # Act
        provider = credentials.as_provider(identity="  ")

        # Assert
        assert provider.resolve().identity is None

    def test_from_files_should_raise_when_file_missing(self, temp_cert_files):
        """Test from_files reads eagerly and propagates a missing file.

        Given:
            A non-existent CA path.
        When:
            from_files() is called.
        Then:
            It should raise FileNotFoundError.
        """
        # Arrange
        _, key_path, cert_path = temp_cert_files

        # Act & assert
        with pytest.raises(FileNotFoundError):
            WorkerCredentials.from_files("/nonexistent/ca.pem", key_path, cert_path)

    def test_from_files_should_accept_path_objects(self, temp_cert_files):
        """Test from_files accepts os.PathLike paths, not just strings.

        Given:
            The PEM file paths as pathlib.Path objects.
        When:
            from_files() is called with them.
        Then:
            It should load the same material as the string paths would.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files

        # Act
        credentials = WorkerCredentials.from_files(
            Path(ca_path), Path(key_path), Path(cert_path)
        )

        # Assert
        with open(ca_path, "rb") as f:
            assert credentials.ca_cert == f.read()

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

    @given(mutual=st.booleans(), identity=st.one_of(st.none(), st.text()))
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_pickle_roundtrip(self, mutual, identity, test_certificates):
        """Test WorkerCredentials survives pickle roundtrip.

        Given:
            WorkerCredentials with valid certificates, any mutual flag
            value, and any identity.
        When:
            The instance is pickled and unpickled.
        Then:
            It should produce an equal instance — preserving the
            identity — that still builds valid gRPC credentials.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem,
            worker_key=key_pem,
            worker_cert=cert_pem,
            mutual=mutual,
            identity=identity,
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


class TestWorkerCredentialsProvider:
    """Test suite for WorkerCredentialsProvider."""

    def test_resolve_should_return_constant_credentials_when_not_reloadable(
        self, test_certificates
    ):
        """Test a non-reloadable provider resolves to constant credentials.

        Given:
            A non-reloadable provider over fixed credential material and an
            identity.
        When:
            resolve() is called more than once.
        Then:
            It should return the same credentials instance each time, with the
            provider's identity stamped onto the material.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(lambda: creds, identity="wool-worker")

        # Act
        first = provider.resolve()
        second = provider.resolve()

        # Assert
        assert first is second
        assert first == replace(creds, identity="wool-worker")

    def test_resolve_should_default_identity_to_none(self, test_certificates):
        """Test a provider defaults to address-based verification.

        Given:
            A provider constructed without an identity.
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
        provider = WorkerCredentialsProvider(lambda: creds)

        # Act
        snapshot = provider.resolve()

        # Assert
        assert snapshot.identity is None

    def test_identity_should_expose_configured_identity(self, test_certificates):
        """Test the identity property reflects construction.

        Given:
            A provider constructed with an identity.
        When:
            The identity property is read.
        Then:
            It should equal the configured identity.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )

        # Act
        provider = WorkerCredentialsProvider(lambda: creds, identity="wool-worker")

        # Assert
        assert provider.identity == "wool-worker"

    def test_reloadable_should_reflect_the_flag(self, test_certificates):
        """Test reloadable mirrors the constructor argument.

        Given:
            A non-reloadable and a reloadable provider over fixed material.
        When:
            Their reloadable flags are read.
        Then:
            They should be False and True respectively.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )

        # Act & assert
        assert WorkerCredentialsProvider(lambda: creds).reloadable is False
        assert (
            WorkerCredentialsProvider(lambda: creds, reloadable=True).reloadable is True
        )

    def test_resolve_should_normalize_blank_identity_to_none(self, test_certificates):
        """Test a blank identity collapses to address-based verification.

        Given:
            A provider constructed with a whitespace-only identity.
        When:
            The identity property is read and the snapshot resolved.
        Then:
            Both should be None, taking the address-based path rather than
            emitting an empty target-name override.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(lambda: creds, identity="   ")

        # Act & assert
        assert provider.identity is None
        assert provider.resolve().identity is None

    def test_non_reloadable_should_call_factory_once_at_construction(
        self, test_certificates
    ):
        """Test a non-reloadable provider resolves eagerly and caches.

        Given:
            A non-reloadable provider over a counting factory.
        When:
            The provider is constructed and then resolved several times.
        Then:
            factory should be called exactly once, at construction.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        calls = []

        def factory():
            calls.append(1)
            return creds

        # Act
        provider = WorkerCredentialsProvider(factory)
        assert len(calls) == 1
        provider.resolve()
        provider.resolve()

        # Assert
        assert len(calls) == 1

    def test_reloadable_should_call_factory_each_resolve(self, test_certificates):
        """Test a reloadable provider consults factory on every resolution.

        Given:
            A reloadable provider over a counting factory.
        When:
            The provider is constructed and then resolved several times.
        Then:
            factory should be deferred at construction and called once per
            resolve.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        calls = []

        def factory():
            calls.append(1)
            return creds

        # Act
        provider = WorkerCredentialsProvider(factory, reloadable=True)
        assert len(calls) == 0
        provider.resolve()
        provider.resolve()
        provider.resolve()

        # Assert
        assert len(calls) == 3

    def test_reloadable_resolve_should_reflect_rotated_material(self, test_certificates):
        """Test a reloadable provider adopts material returned by factory.

        Given:
            A reloadable provider whose factory returns rotated material on the
            second call.
        When:
            resolve() is called before and after the rotation.
        Then:
            The resolved credentials should differ, since each reflects
            the current material.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        original = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        rotated = WorkerCredentials(
            ca_cert=b"-----ROTATED-----\n" + ca_pem,
            worker_key=key_pem,
            worker_cert=cert_pem,
        )
        materials = iter([original, rotated])

        # Act
        provider = WorkerCredentialsProvider(lambda: next(materials), reloadable=True)
        before = provider.resolve()
        after = provider.resolve()

        # Assert
        assert before != after

    def test_non_reloadable_pickle_should_drop_callback_and_ship_snapshot(
        self, test_certificates
    ):
        """Test a non-reloadable provider pickles without its callback.

        Given:
            A non-reloadable provider built over a lambda, which the standard
            library pickler cannot serialize directly.
        When:
            It is pickled with the standard library pickler and unpickled.
        Then:
            It should round-trip — the eager snapshot rides along and the
            callback is dropped — and resolve to equal credentials.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(lambda: creds, identity="wool-worker")

        # Act
        restored = pickle.loads(pickle.dumps(provider))

        # Assert
        assert restored.resolve() == provider.resolve()
        assert restored.identity == "wool-worker"

    def test_reloadable_pickle_should_keep_factory_and_reread(self, temp_cert_files):
        """Test a reloadable provider re-resolves through a pickle roundtrip.

        Given:
            A reloadable, file-backed provider (whose factory is picklable)
            that has resolved once.
        When:
            It is pickled with the standard library pickler and unpickled.
        Then:
            The restored provider should keep its factory and resolve to
            the same credentials by re-reading the unchanged files.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        provider = WorkerCredentialsProvider(
            functools.partial(
                WorkerCredentials.from_files, ca_path, key_path, cert_path
            ),
            identity="wool-worker",
            reloadable=True,
        )
        original = provider.resolve()

        # Act
        restored = pickle.loads(pickle.dumps(provider))

        # Assert
        assert restored.resolve() == original


class TestCredentialsContext:
    """Test suite for CredentialsContext."""

    def test_exit_should_raise_without_matching_enter(self, test_certificates):
        """Test __exit__ guards against use without a matching __enter__.

        Given:
            A CredentialsContext that was never entered.
        When:
            __exit__ is called.
        Then:
            It should raise RuntimeError rather than resetting a token it
            never set.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        credentials = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        context = CredentialsContext(credentials)

        # Act & assert
        with pytest.raises(RuntimeError, match="without matching __enter__"):
            context.__exit__()
