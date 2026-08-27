import asyncio
import datetime
import functools
import io
import logging
import pickle
import threading
import time
import warnings
from dataclasses import FrozenInstanceError
from datetime import timedelta
from multiprocessing.reduction import ForkingPickler
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
from pytest_mock import MockerFixture

import wool.utilities.throttle as wt
from tests.helpers import write_certificate_files
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.auth import WorkerCredentialsProvider
from wool.runtime.worker.auth import credentials_scope
from wool.runtime.worker.auth import current_credentials
from wool.runtime.worker.auth import normalize_peer
from wool.runtime.worker.exceptions import IneffectivePeersWarning
from wool.utilities.refreshing import Refreshing


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
    files = write_certificate_files(tmp_path, ca_pem, key_pem, cert_pem)
    return files.ca_path, files.key_path, files.cert_path


def _expected_policy(peers):
    """Return the policy shape ``peers`` should compile to.

    Mirrors the provider's own normalization so a property test can
    assert which shape an example actually drew. Text and lists of text
    both shrink toward values that collapse to no names, which leaves the
    provider unconfigured — without this an example that never reached a
    configured policy is indistinguishable from one that did.
    """
    if peers is None:
        return None
    if callable(peers):
        return peers
    names = [peers] if isinstance(peers, str) else list(peers)
    surviving = {name.strip() for name in names if name.strip()}
    return frozenset(surviving) if surviving else None


def _accepts_prod(name: str) -> bool:
    """Module-level peers predicate, so a provider carrying it stays picklable."""
    return name.startswith("prod-")


def _accepts_nothing(name: str) -> bool:
    """Module-level peers predicate that admits no name."""
    return False


class TestWorkerCredentials:
    """Test suite for WorkerCredentials credential management."""

    def test___init___should_set_all_fields_when_mtls(self, test_certificates):
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

    def test___init___should_set_mutual_false_when_one_way_tls(self, test_certificates):
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

    def test___init___should_raise_when_field_mutated(self, test_certificates):
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

    def test_from_files_should_load_bytes_when_mtls(self, temp_cert_files):
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

    def test_from_files_should_set_mutual_false_when_one_way_tls(self, temp_cert_files):
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

    def test_from_files_should_default_mutual_to_true(self, temp_cert_files):
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

    def test_from_files_should_raise_when_ca_cert_missing(self, temp_cert_files):
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

    def test_from_files_should_raise_when_key_missing(self, temp_cert_files):
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

    def test_from_files_should_raise_when_cert_missing(self, temp_cert_files):
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

    def test_from_files_should_raise_when_permission_denied(
        self, temp_cert_files, tmp_path
    ):
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
            material is the same object.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        credentials = WorkerCredentials.from_files(ca_path, key_path, cert_path)

        # Act
        provider = credentials.as_provider()

        # Assert
        assert isinstance(provider, WorkerCredentialsProvider)
        assert provider.reloadable is False
        assert provider.credentials.get() == credentials

    def test_as_provider_should_expose_peers(self, temp_cert_files):
        """Test as_provider forwards the peer name to the provider itself.

        Given:
            Credentials loaded from PEM files and an expected peer name.
        When:
            as_provider() is called with that peer name.
        Then:
            The provider's ``peers`` should be the configured name,
            normalized into a frozenset.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        credentials = WorkerCredentials.from_files(ca_path, key_path, cert_path)

        # Act
        provider = credentials.as_provider(peers="wool-worker")

        # Assert
        assert provider.peers == frozenset({"wool-worker"})

    def test_as_provider_should_expose_no_peers_when_blank(self, temp_cert_files):
        """Test a blank peer name leaves the provider with no policy.

        Given:
            Credentials and a whitespace-only peer name.
        When:
            as_provider() is called with that peer name.
        Then:
            The provider's ``peers`` should be None.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        credentials = WorkerCredentials.from_files(ca_path, key_path, cert_path)

        # Act
        with pytest.warns(IneffectivePeersWarning):
            provider = credentials.as_provider(peers="  ")

        # Assert
        assert provider.peers is None

    def test_server_credentials_should_return_server_credentials_when_mtls(
        self, test_certificates
    ):
        """Test server credentials property for mTLS.

        Given:
            WorkerCredentials with mutual=True.
        When:
            server_credentials() method is called.
        Then:
            It should return grpc.ServerCredentials configured for mTLS.
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

    def test_server_credentials_should_return_server_credentials_when_one_way_tls(
        self, test_certificates
    ):
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

    def test_server_certificate_configuration_should_return_configuration_when_mtls(
        self, test_certificates
    ):
        """Test server certificate configuration assembly for mTLS.

        Given:
            WorkerCredentials with mutual=True.
        When:
            server_certificate_configuration() is called.
        Then:
            It should return a grpc.ServerCertificateConfiguration carrying
            the material for a mutually authenticated handshake.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=True
        )

        # Act
        configuration = creds.server_certificate_configuration()

        # Assert
        assert isinstance(configuration, grpc.ServerCertificateConfiguration)

    def test_server_certificate_configuration_should_return_configuration_when_one_way(
        self, test_certificates
    ):
        """Test server certificate configuration assembly for one-way TLS.

        Given:
            WorkerCredentials with mutual=False.
        When:
            server_certificate_configuration() is called.
        Then:
            It should return a grpc.ServerCertificateConfiguration for a
            one-way handshake with no client-verification CA.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=False
        )

        # Act
        configuration = creds.server_certificate_configuration()

        # Assert
        assert isinstance(configuration, grpc.ServerCertificateConfiguration)

    @pytest.mark.parametrize(
        "mutual,expects_ca",
        [(True, True), (False, False)],
        ids=["mutual", "one-way"],
    )
    def test_server_credentials_should_assemble_the_same_material_as_the_configuration(
        self, mocker: MockerFixture, test_certificates, mutual, expects_ca
    ):
        """Test both server surfaces agree on the material they assemble.

        Given:
            WorkerCredentials configured for mutual or one-way TLS.
        When:
            The static and the rotation-capable server surfaces are both
            built.
        Then:
            It should hand gRPC the same key/certificate pairs and the
            same root certificates through either surface, offering the
            CA for client verification only under mutual TLS — the rule
            the two must never disagree on.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem,
            worker_key=key_pem,
            worker_cert=cert_pem,
            mutual=mutual,
        )
        build_credentials = mocker.patch.object(grpc, "ssl_server_credentials")
        build_configuration = mocker.patch.object(
            grpc, "ssl_server_certificate_configuration"
        )

        # Act
        creds.server_credentials()
        creds.server_certificate_configuration()

        # Assert
        credentials_kwargs = build_credentials.call_args.kwargs
        configuration_args, configuration_kwargs = build_configuration.call_args
        assert (
            credentials_kwargs["private_key_certificate_chain_pairs"]
            == configuration_args[0]
            == [(key_pem, cert_pem)]
        )
        assert (
            credentials_kwargs["root_certificates"]
            == configuration_kwargs["root_certificates"]
            == (ca_pem if expects_ca else None)
        )

    def test_client_credentials_should_return_channel_credentials_when_mtls(
        self, test_certificates
    ):
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

    def test_client_credentials_should_return_channel_credentials_when_one_way_tls(
        self, test_certificates
    ):
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

    @given(mutual=st.booleans())
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_server_credentials_and_client_credentials_should_return_consistent_types(
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
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_pickle_roundtrip_should_produce_equal_instance(
        self, mutual, test_certificates
    ):
        """Test WorkerCredentials survives pickle roundtrip.

        Given:
            WorkerCredentials with valid certificates and any mutual flag
            value.
        When:
            The instance is pickled and unpickled.
        Then:
            It should produce an equal instance that still builds valid
            gRPC credentials — equality is what keys the channel pool, so
            a roundtrip that changed it would fragment the pool across a
            worker-subprocess boundary.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem,
            worker_key=key_pem,
            worker_cert=cert_pem,
            mutual=mutual,
        )

        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(creds))

        # Assert
        assert restored == creds
        assert isinstance(restored.server_credentials(), grpc.ServerCredentials)
        assert isinstance(restored.client_credentials(), grpc.ChannelCredentials)


class TestWorkerCredentialsProvider:
    """Test suite for WorkerCredentialsProvider."""

    def test___init___should_call_factory_once_when_not_reloadable(
        self, test_certificates
    ):
        """Test a non-reloadable provider resolves eagerly at construction.

        Given:
            A counting factory over fixed credential material.
        When:
            A non-reloadable provider is constructed over it.
        Then:
            It should call the factory exactly once.
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
        WorkerCredentialsProvider(factory)

        # Assert
        assert len(calls) == 1

    def test___init___should_not_call_factory_when_reloadable(self, test_certificates):
        """Test a reloadable provider defers its factory at construction.

        Given:
            A counting factory over fixed credential material.
        When:
            A reloadable provider is constructed over it.
        Then:
            It should not call the factory.
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
        WorkerCredentialsProvider(
            factory, reloadable=True, fresh_for=timedelta(seconds=60)
        )

        # Assert
        assert len(calls) == 0

    def test___init___should_accept_several_peers(self, test_certificates):
        """Test a set of accepted peer names is compiled into the policy.

        Given:
            Several peer names.
        When:
            The provider is constructed with those peer names.
        Then:
            It should expose all of them, and the material it yields
            should carry no peer — which name a connection verifies
            against is chosen per worker from what that worker
            advertised, never here.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )

        # Act
        provider = WorkerCredentialsProvider(lambda: creds, peers=["a.svc", "b.svc"])

        # Assert
        assert provider.peers == frozenset({"a.svc", "b.svc"})

    def test___init___should_accept_a_peer_predicate(self, test_certificates):
        """Test a predicate over a candidate name is kept as the policy.

        Given:
            A predicate accepting names under one prefix.
        When:
            The provider is constructed with those peer names.
        Then:
            It should be the predicate itself, and the material should
            carry no peer, since a policy of any shape applies nothing
            to the material it yields.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )

        def accepts(name):
            return name.startswith("prod-")

        # Act
        provider = WorkerCredentialsProvider(lambda: creds, peers=accepts)

        # Assert
        assert provider.peers is accepts

    @pytest.mark.parametrize("peers", ["   ", [], ["  ", ""]])
    def test___init___should_treat_blank_peers_as_unconfigured(
        self, peers, test_certificates
    ):
        """Test blank and empty peer policies collapse to no policy.

        Given:
            A blank name, an empty iterable, or an iterable of only
            blank names.
        When:
            The provider is constructed with those peer names.
        Then:
            It should be None rather than a policy accepting nothing,
            which would reject every peer and read as a silent outage.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )

        # Act
        with pytest.warns(IneffectivePeersWarning):
            provider = WorkerCredentialsProvider(lambda: creds, peers=peers)

        # Assert
        assert provider.peers is None

    @pytest.mark.parametrize(
        "peers",
        ["alpha.svc", ["alpha.svc", "beta.svc"], _accepts_prod],
        ids=["one-name", "several-names", "predicate"],
    )
    def test___init___should_not_warn_when_peers_names_a_peer(
        self, peers, test_certificates
    ):
        """Test a policy that gates something is accepted silently.

        Given:
            A peers value of any shape that survives normalization.
        When:
            The provider is constructed.
        Then:
            It should not emit IneffectivePeersWarning, so the warning
            above is attributable to the collapse to no names rather
            than to configuring peers at all. Nothing raises the
            category to an error suite-wide, so without this a
            regression that warned on every provider would pass
            unnoticed.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )

        # Act & assert
        with warnings.catch_warnings():
            warnings.simplefilter("error", IneffectivePeersWarning)
            WorkerCredentialsProvider(lambda: creds, peers=peers)

    @pytest.mark.parametrize("peers", ["   ", [], ["  ", ""]])
    def test___init___should_warn_when_peers_names_nothing(
        self, peers, test_certificates
    ):
        """Test the collapse to unconfigured is reported, not silent.

        Given:
            A blank name, an empty iterable, or an iterable of only
            blank names.
        When:
            The provider is constructed with those peer names.
        Then:
            It should emit an IneffectivePeersWarning naming the
            consequence, since the collapse moves the caller into the
            state where advertisements are ignored rather than widening
            what is accepted.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )

        # Act & assert
        with pytest.warns(
            IneffectivePeersWarning, match="leaves this provider unconfigured"
        ):
            WorkerCredentialsProvider(lambda: creds, peers=peers)

    def test___init___should_raise_when_peers_is_an_unsupported_shape(
        self, test_certificates
    ):
        """Test a peers value of no supported shape is rejected clearly.

        Given:
            An integer supplied as the accepted peers.
        When:
            The provider is constructed.
        Then:
            It should raise TypeError naming the shapes it accepts,
            rather than an AttributeError from string normalization.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )

        # Act & assert
        with pytest.raises(TypeError, match="peers must be a peer name"):
            WorkerCredentialsProvider(
                lambda: creds,
                peers=5,  # pyright: ignore[reportArgumentType]
            )

    def test___init___should_raise_when_a_peer_name_is_not_a_string(
        self, test_certificates
    ):
        """Test a non-string inside the accepted names is rejected.

        Given:
            An iterable mixing a name and an integer.
        When:
            The provider is constructed.
        Then:
            It should raise TypeError naming the offending element's
            type, so the bad entry is identifiable.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )

        # Act & assert
        with pytest.raises(TypeError, match="single peer name"):
            WorkerCredentialsProvider(
                lambda: creds,
                peers=["a.svc", 7],  # pyright: ignore[reportArgumentType]
            )

    @pytest.mark.filterwarnings(
        "ignore::wool.runtime.worker.exceptions.IneffectivePeersWarning"
    )
    @given(names=st.lists(st.text(), max_size=6))
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test___init___should_compile_every_accepted_name(self, names, test_certificates):
        """Test an iterable of names compiles to the stripped, non-blank set.

        Given:
            Any list of candidate peer names, including blank and
            padded ones.
        When:
            A provider is constructed with that list as its peers.
        Then:
            The compiled policy should be exactly the stripped
            non-blank names, collapsing to None when none survive —
            the same rule a single name follows.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )

        # Act
        provider = WorkerCredentialsProvider(lambda: creds, peers=names)

        # Assert
        surviving = {name.strip() for name in names if name.strip()}
        assert provider.peers == (frozenset(surviving) if surviving else None)

    @pytest.mark.filterwarnings(
        "ignore::wool.runtime.worker.exceptions.IneffectivePeersWarning"
    )
    def test_accepts_peer_should_reject_when_the_predicate_raises(
        self, test_certificates, caplog
    ):
        """Test a raising predicate refuses the peer rather than the caller.

        Given:
            A provider whose peers predicate raises for every candidate.
        When:
            A peer name is offered to accepts_peer.
        Then:
            It should return False and log the failure, since this runs
            inside the proxy's admission loop and an escaping exception
            would end that loop for the proxy's lifetime.
        """

        # Arrange
        def explode(peer):
            raise RuntimeError("predicate is broken")

        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(lambda: creds, peers=explode)

        # Act
        with caplog.at_level(logging.ERROR):
            accepted = provider.accepts_peer("alpha.svc")

        # Assert
        assert accepted is False
        assert "predicate raised" in caplog.text

    def test_peers_should_expose_configured_peers(self, test_certificates):
        """Test the peers property reflects construction.

        Given:
            A provider constructed with a peer name.
        When:
            The peers property is read.
        Then:
            It should be the configured name as a single-element set,
            the normal form every accepted-name shape compiles to.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(lambda: creds, peers="wool-worker")

        # Act
        peers = provider.peers

        # Assert
        assert peers == frozenset({"wool-worker"})

    @pytest.mark.parametrize("candidate", [None, "", "   ", "anything.svc"])
    def test_accepts_peer_should_admit_any_name_when_unconfigured(
        self, candidate, test_certificates
    ):
        """Test an unconfigured provider ignores advertisements entirely.

        Given:
            A provider with no peers policy, and any advertised name —
            absent, blank, or plain.
        When:
            The provider is asked whether it accepts that name.
        Then:
            It should accept it, since with nothing configured a
            connection verifies against the dialed address rather than
            against an advertisement.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(lambda: creds)

        # Act
        accepted = provider.accepts_peer(candidate)

        # Assert
        assert accepted is True

    @pytest.mark.parametrize(
        "peers",
        ["alpha.svc", ["alpha.svc", "beta.svc"], lambda name: True],
        ids=["one-name", "several-names", "predicate"],
    )
    @pytest.mark.parametrize("candidate", [None, "", "   "])
    def test_accepts_peer_should_reject_an_unnamed_worker(
        self, peers, candidate, test_certificates
    ):
        """Test a configured provider refuses a worker advertising nothing.

        Given:
            A provider whose policy names one peer, names several, or is
            a predicate that accepts everything, and a worker
            advertising nothing or a blank name.
        When:
            The provider is asked whether it accepts that advertisement.
        Then:
            It should refuse it whatever the policy's shape, since a
            worker advertising nothing offers no name to verify — an
            accept-list of one is not a licence to fall back on the
            dialed address.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(lambda: creds, peers=peers)

        # Act
        accepted = provider.accepts_peer(candidate)

        # Assert
        assert accepted is False

    @pytest.mark.parametrize(
        ("peers", "candidate", "expected"),
        [
            ("alpha.svc", "alpha.svc", True),
            ("alpha.svc", "beta.svc", False),
            (["alpha.svc", "beta.svc"], "beta.svc", True),
            (["alpha.svc", "beta.svc"], "gamma.svc", False),
            (lambda name: name.endswith(".svc"), "alpha.svc", True),
            (lambda name: name.endswith(".svc"), "alpha.other", False),
        ],
        ids=[
            "one-admits-match",
            "one-refuses-mismatch",
            "several-admits-member",
            "several-refuses-non-member",
            "predicate-admits-match",
            "predicate-refuses-mismatch",
        ],
    )
    def test_accepts_peer_should_decide_by_the_configured_policy(
        self, peers, candidate, expected, test_certificates
    ):
        """Test the verdict follows the policy, whatever its shape.

        Given:
            A provider whose policy names one peer, names several, or is
            a predicate, and a worker advertising a name that policy
            does or does not cover.
        When:
            The provider is asked whether it accepts that name.
        Then:
            It should follow the policy alone, so an accept-list of one
            behaves exactly as an accept-list of two and exactly as a
            predicate accepting that one name.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(lambda: creds, peers=peers)

        # Act
        accepted = provider.accepts_peer(candidate)

        # Assert
        assert accepted is expected

    def test_accepts_peer_should_normalize_before_consulting_the_policy(
        self, test_certificates
    ):
        """Test a padded advertisement is stripped before it is judged.

        Given:
            A provider naming one peer, and a worker advertising that
            name surrounded by whitespace.
        When:
            The provider is asked whether it accepts that name.
        Then:
            It should accept it, since every entry point normalizes a
            name the same way and padding cannot change a verdict.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(lambda: creds, peers="alpha.svc")

        # Act
        accepted = provider.accepts_peer("  alpha.svc  ")

        # Assert
        assert accepted is True

    def test_accepts_peer_should_not_invoke_the_predicate_when_unnamed(
        self, test_certificates
    ):
        """Test a user predicate is never handed a missing name.

        Given:
            A provider whose policy is a predicate recording every
            argument it receives, and a worker advertising nothing.
        When:
            The provider is asked whether it accepts that advertisement.
        Then:
            It should refuse without consulting the predicate, so a
            predicate written for strings cannot be handed None.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        seen = []

        def record(name):
            seen.append(name)
            return True

        provider = WorkerCredentialsProvider(lambda: creds, peers=record)

        # Act
        accepted = provider.accepts_peer(None)

        # Assert
        assert accepted is False
        assert seen == []

    @given(
        names=st.lists(st.text(), max_size=6), candidate=st.one_of(st.none(), st.text())
    )
    @pytest.mark.filterwarnings(
        "ignore::wool.runtime.worker.exceptions.IneffectivePeersWarning"
    )
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_accepts_peer_should_agree_with_the_compiled_policy(
        self, names, candidate, test_certificates
    ):
        """Test the verdict is a function of the compiled policy alone.

        Given:
            Any list of accepted names and any advertised name, both
            drawn from arbitrary text.
        When:
            The provider is asked whether it accepts that name.
        Then:
            It should accept unconditionally when no name survives
            compilation, and otherwise accept exactly the stripped
            non-blank advertisements the compiled set contains.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(lambda: creds, peers=names)

        # Act
        accepted = provider.accepts_peer(candidate)

        # Assert
        surviving = {name.strip() for name in names if name.strip()}
        stripped = candidate.strip() if candidate else ""
        expected = True if not surviving else bool(stripped) and stripped in surviving
        assert accepted is expected

    @pytest.mark.parametrize(
        ("peers", "expected"),
        [
            (None, ""),
            (["beta.svc", "alpha.svc"], "alpha.svc, beta.svc"),
            (lambda name: True, "a peer-name predicate"),
        ],
        ids=["unconfigured", "several-names", "predicate"],
    )
    def test_describe_peers_should_render_the_accepted_names(
        self, peers, expected, test_certificates
    ):
        """Test the diagnostic names what an operator has to satisfy.

        Given:
            A provider configuring no policy, several names in unsorted
            order, or a predicate.
        When:
            The accepted peers are described for a refusal diagnostic.
        Then:
            It should render nothing, the names comma-joined in sorted
            order, and a note that a predicate decides — so an empty
            pool is diagnosable from a log line and the text is stable
            across runs.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(lambda: creds, peers=peers)

        # Act
        described = provider.describe_peers()

        # Assert
        assert described == expected

    def test_coerce_should_wrap_bare_credentials(self, test_certificates):
        """Test coerce wraps a bare WorkerCredentials in a provider.

        Given:
            A bare WorkerCredentials instance.
        When:
            It is passed to WorkerCredentialsProvider.coerce.
        Then:
            It should return a WorkerCredentialsProvider resolving to the
            bare credentials.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )

        # Act
        coerced = WorkerCredentialsProvider.coerce(creds)

        # Assert
        assert isinstance(coerced, WorkerCredentialsProvider)
        assert coerced.credentials.get() == creds

    def test_coerce_should_return_provider_unchanged(self, test_certificates):
        """Test coerce passes an existing provider through by identity.

        Given:
            An existing WorkerCredentialsProvider.
        When:
            It is passed to WorkerCredentialsProvider.coerce.
        Then:
            It should return the same provider object unchanged.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(lambda: creds)

        # Act
        coerced = WorkerCredentialsProvider.coerce(provider)

        # Assert
        assert coerced is provider

    def test_coerce_should_return_none_when_none(self):
        """Test coerce passes None through.

        Given:
            No credentials (None).
        When:
            None is passed to WorkerCredentialsProvider.coerce.
        Then:
            It should return None.
        """
        # Act
        coerced = WorkerCredentialsProvider.coerce(None)

        # Assert
        assert coerced is None

    def test_coerce_should_pass_duck_typed_provider_through(self):
        """Test coerce passes a duck-typed provider through unchanged.

        Given:
            An object exposing the credentials resource and a reloadable
            flag — the full contract consumers read — without subclassing
            WorkerCredentialsProvider.
        When:
            It is passed to WorkerCredentialsProvider.coerce.
        Then:
            It should be returned unchanged, keeping duck-typed providers
            reachable.
        """

        # Arrange
        class DuckProvider:
            reloadable = False
            credentials = Refreshing(lambda: None, fresh_for=None)

        provider = DuckProvider()

        # Act
        coerced = WorkerCredentialsProvider.coerce(provider)

        # Assert
        assert coerced is provider

    def test_coerce_should_raise_when_provider_lacks_reloadable(self):
        """Test coerce rejects a provider missing half the contract.

        Given:
            An object exposing credentials but no reloadable flag, which
            every consumer of a coerced provider also reads.
        When:
            It is passed to WorkerCredentialsProvider.coerce.
        Then:
            It should raise TypeError here rather than letting the gap
            surface as an opaque AttributeError mid-dispatch.
        """

        # Arrange
        class HalfProvider:
            credentials = Refreshing(lambda: None, fresh_for=None)

        # Act & assert
        with pytest.raises(TypeError, match="reloadable"):
            WorkerCredentialsProvider.coerce(HalfProvider())

    def test_coerce_should_raise_when_channel_credentials(self):
        """Test coerce rejects a raw gRPC channel credentials object.

        Given:
            A grpc.ChannelCredentials built by grpc.ssl_channel_credentials(),
            the shape stale callers of the retyped credentials parameters
            still pass.
        When:
            It is passed to WorkerCredentialsProvider.coerce.
        Then:
            It should raise TypeError naming the accepted shapes at
            construction time rather than failing opaquely mid-dispatch.
        """
        # Arrange
        channel_credentials = grpc.ssl_channel_credentials()

        # Act & assert
        with pytest.raises(TypeError, match="WorkerCredentials"):
            WorkerCredentialsProvider.coerce(channel_credentials)

    def test_credentials_should_return_constant_material_when_not_reloadable(
        self, test_certificates
    ):
        """Test a non-reloadable provider resolves to constant credentials.

        Given:
            A non-reloadable provider over fixed credential material and a
            peers policy.
        When:
            The credentials are read more than once.
        Then:
            It should return the same credentials instance each time, and
            that instance should be the factory's material untouched --
            the policy governs admission and applies nothing here.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(lambda: creds, peers="wool-worker")

        # Act
        first = provider.credentials.get()
        second = provider.credentials.get()

        # Assert
        assert first is second
        assert first == creds

    @pytest.mark.filterwarnings(
        "ignore::wool.runtime.worker.exceptions.IneffectivePeersWarning"
    )
    @given(
        peers=st.one_of(
            st.none(),
            st.text(),
            st.lists(st.text(), max_size=4),
            st.sampled_from([_accepts_prod, _accepts_nothing]),
        ),
        reloadable=st.booleans(),
    )
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_credentials_should_return_the_factory_material_unchanged(
        self, peers, reloadable, test_certificates
    ):
        """Test no peers shape applies anything to the resolved material.

        Given:
            Credential material wrapped in a provider configured with any
            peers value -- none, a single name, an iterable of any
            length, or a predicate -- resolving either once or on every
            read.
        When:
            The credentials are read twice, so both the construction
            resolve and the cached read are covered.
        Then:
            Both reads should return the very object the factory built,
            whatever the policy's shape and on either resolution path —
            see `WorkerProxy` for what a policy does decide.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(
            lambda: creds, peers=peers, reloadable=reloadable
        )

        # Act
        first = provider.credentials.get()
        cached = provider.credentials.get()

        # Assert
        assert provider.peers == _expected_policy(peers)
        assert first is creds
        assert cached is creds

    def test_pickle_roundtrip_should_keep_a_peer_predicate(self, test_certificates):
        """Test a predicate policy survives the trip into a subprocess.

        Given:
            A provider whose peers is a locally defined predicate,
            which plain pickle cannot serialize.
        When:
            The provider is pickled and restored.
        Then:
            The restored policy should accept and reject exactly as the
            original did — the provider crosses into worker
            subprocesses through plain pickle, so the predicate must
            ride the cloudpickle path its factory already uses.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(
            lambda: creds, peers=lambda name: name.startswith("prod-")
        )

        # Act
        restored = pickle.loads(cloudpickle.dumps(provider))

        # Assert
        assert restored.peers("prod-api") is True
        assert restored.peers("staging-api") is False

    @pytest.mark.parametrize("reloadable", [False, True])
    def test_reloadable_should_reflect_the_flag(self, reloadable, test_certificates):
        """Test reloadable mirrors the constructor argument.

        Given:
            A provider over fixed material constructed with an explicit
            reloadable flag.
        When:
            The reloadable property is read.
        Then:
            It should equal the constructor argument.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )

        # Act
        provider = WorkerCredentialsProvider(lambda: creds, reloadable=reloadable)

        # Assert
        assert provider.reloadable is reloadable

    def test_credentials_should_not_call_factory_when_not_reloadable(
        self, test_certificates
    ):
        """Test a non-reloadable provider serves its construction-time material.

        Given:
            A non-reloadable provider constructed over a counting factory.
        When:
            The credentials are read several times.
        Then:
            It should serve the construction-time material without calling
            the factory again.
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

        provider = WorkerCredentialsProvider(factory)

        # Act
        provider.credentials.get()
        provider.credentials.get()

        # Assert
        assert len(calls) == 1

    def test_credentials_should_call_factory_once_when_within_debounce_window(
        self, test_certificates
    ):
        """Test rapid resolves inside the debounce window share one factory call.

        Given:
            A reloadable provider over a counting factory with its debounce
            window pinned open.
        When:
            The credentials are read several times in quick succession.
        Then:
            It should invoke the factory exactly once — the first
            resolution — with every call returning the same cached material.
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

        provider = WorkerCredentialsProvider(
            factory, reloadable=True, fresh_for=timedelta(seconds=60)
        )

        # Act
        results = [provider.credentials.get() for _ in range(5)]

        # Assert
        assert len(calls) == 1
        assert all(result == creds for result in results)

    def test_credentials_should_adopt_rotated_material_when_window_elapsed(
        self, test_certificates
    ):
        """Test rotated material is adopted after the debounce window elapses.

        Given:
            A reloadable provider whose factory returns rotated material on
            its second call, with one resolution already cached.
        When:
            The window elapses and the credentials are read.
        Then:
            It should run the refresh on that caller and return the rotated
            material to it, with the factory invoked exactly twice.
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
        calls = []

        def factory():
            calls.append(1)
            return next(materials)

        provider = WorkerCredentialsProvider(
            factory, reloadable=True, fresh_for=timedelta(0)
        )
        before = provider.credentials.get()

        # Act
        refreshed = provider.credentials.get()

        # Assert
        assert before == original
        assert refreshed == rotated
        assert len(calls) == 2

    def test_credentials_should_serve_stale_when_refresh_in_flight(
        self, test_certificates
    ):
        """Test callers are not queued behind a slow in-flight refresh.

        Given:
            A reloadable provider with cached material past its window, and
            another thread already inside a refresh whose factory call
            blocks on a gate — slower than any debounce window.
        When:
            Several resolves arrive while that refresh is still in flight.
        Then:
            It should serve every one of them the previous material
            immediately and invoke the factory exactly once for the whole
            stampede — drop-while-pending, not one refresh per caller —
            handing the new material to the refresh's own driver, which
            is what distinguishes a dropped caller from a dropped refresh.
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
        calls = []
        entered = threading.Event()
        release = threading.Event()

        def factory():
            calls.append(1)
            if len(calls) > 1:
                entered.set()
                assert release.wait(timeout=5.0)
                return rotated
            return original

        provider = WorkerCredentialsProvider(
            factory, reloadable=True, fresh_for=timedelta(0)
        )
        provider.credentials.get()
        adopted = []
        driver = threading.Thread(
            target=lambda: adopted.append(provider.credentials.get())
        )
        driver.start()
        assert entered.wait(timeout=5.0)

        # Act — stampede while the refresh is gated, then release it so the
        # single in-flight refresh commits and its driver returns.
        results = [provider.credentials.get() for _ in range(5)]
        release.set()
        driver.join(timeout=5.0)

        # Assert
        assert results == [original] * 5
        assert adopted == [rotated]
        assert len(calls) == 2  # The seed plus the single in-flight refresh.

    def test_credentials_should_share_one_invocation_when_first_read_concurrent(
        self, test_certificates
    ):
        """Test concurrent first resolves single-flight one factory call.

        Given:
            A reloadable provider with no cached material and a factory that
            blocks on a gate.
        When:
            Several threads read the credentials concurrently and the gate is then
            released.
        Then:
            It should invoke the factory exactly once, with every caller
            receiving that invocation's result.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        calls = []
        entered = threading.Event()
        release = threading.Event()

        def factory():
            calls.append(1)
            entered.set()
            assert release.wait(timeout=5.0)
            return creds

        provider = WorkerCredentialsProvider(
            factory, reloadable=True, fresh_for=timedelta(seconds=60)
        )
        results = []

        def read():
            results.append(provider.credentials.get())

        threads = [threading.Thread(target=read) for _ in range(4)]

        # Act
        for thread in threads:
            thread.start()
        assert entered.wait(timeout=5.0)
        time.sleep(0.05)  # Let the remaining callers join the flight.
        release.set()
        for thread in threads:
            thread.join(timeout=5.0)

        # Assert
        assert len(calls) == 1
        assert results == [creds] * 4

    def test_credentials_should_raise_same_error_to_all_waiters_when_first_read_fails(
        self, test_certificates
    ):
        """Test a failed first resolve propagates to every joined caller.

        Given:
            A reloadable provider with no cached material and a factory that
            raises after a gate is released.
        When:
            Several threads read the credentials concurrently and the gate is then
            released.
        Then:
            It should invoke the factory exactly once and raise the same
            exception object to every caller.
        """
        # Arrange
        calls = []
        entered = threading.Event()
        release = threading.Event()
        error = RuntimeError("secrets manager unavailable")

        def factory():
            calls.append(1)
            entered.set()
            assert release.wait(timeout=5.0)
            raise error

        provider = WorkerCredentialsProvider(
            factory, reloadable=True, fresh_for=timedelta(seconds=60)
        )
        raised = []

        def read():
            try:
                provider.credentials.get()
            except RuntimeError as exc:
                raised.append(exc)

        threads = [threading.Thread(target=read) for _ in range(4)]

        # Act
        for thread in threads:
            thread.start()
        assert entered.wait(timeout=5.0)
        time.sleep(0.05)  # Let the remaining callers join the flight.
        release.set()
        for thread in threads:
            thread.join(timeout=5.0)

        # Assert
        assert len(calls) == 1
        assert len(raised) == 4
        assert all(exc is error for exc in raised)

    def test_credentials_should_retry_immediately_when_first_read_fails(
        self, test_certificates
    ):
        """Test a failed resolve does not start a debounce window.

        Given:
            A reloadable provider whose factory raises on its first call and
            succeeds on its second, with a debounce window pinned open.
        When:
            The credentials are read again immediately after the failure.
        Then:
            It should invoke the factory again without waiting out the window
            and return its material.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        calls = []

        def factory():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("transient blip")
            return creds

        provider = WorkerCredentialsProvider(
            factory, reloadable=True, fresh_for=timedelta(seconds=60)
        )

        # Act & assert
        with pytest.raises(RuntimeError, match="transient blip"):
            provider.credentials.get()
        assert provider.credentials.get() == creds
        assert len(calls) == 2

    def test_credentials_should_serve_stale_and_warn_when_refresh_fails(
        self, test_certificates, caplog
    ):
        """Test a failed refresh keeps serving and logs a warning.

        Given:
            A reloadable provider with cached material past its window and a
            factory whose refresh call raises.
        When:
            The credentials are read past the window.
        Then:
            It should return the previous material even to the caller that
            drove the failing refresh, emit a warning with the exception
            attached on the provider module's logger, and let no exception
            escape.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        calls = []

        def factory():
            calls.append(1)
            if len(calls) > 1:
                raise RuntimeError("refresh blew up")
            return creds

        provider = WorkerCredentialsProvider(
            factory, reloadable=True, fresh_for=timedelta(0)
        )
        provider.credentials.get()

        # Act
        with caplog.at_level("WARNING", logger="wool.runtime.worker.auth"):
            served = provider.credentials.get()

        # Assert
        assert served == creds
        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert "refresh failed" in record.getMessage()
        assert record.exc_info is not None

    def test_credentials_should_retrigger_refresh_when_refresh_fails(
        self, test_certificates
    ):
        """Test a failed refresh does not advance the debounce timestamp.

        Given:
            A reloadable provider with cached material whose first refresh
            raises.
        When:
            The credentials are read again after the failed refresh settles.
        Then:
            It should trigger another refresh immediately — a failure never
            buys a debounce window — and adopt its material once it commits.
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
        calls = []

        def factory():
            calls.append(1)
            if len(calls) == 2:
                raise RuntimeError("transient blip")
            return rotated if len(calls) > 2 else original

        provider = WorkerCredentialsProvider(
            factory, reloadable=True, fresh_for=timedelta(0)
        )
        provider.credentials.get()

        # Act — trigger the failing refresh, then keep resolving: the failed
        # flight must not have started a window, so a subsequent call
        # retriggers a refresh that eventually commits the rotated material.
        provider.credentials.get()
        provider.credentials.get()

        # Assert
        assert provider.credentials.get() == rotated
        assert len(calls) >= 3

    def test_credentials_should_refresh_from_plain_thread_when_window_elapsed(
        self, test_certificates
    ):
        """Test a loop-free caller can drive a refresh itself.

        Given:
            A reloadable provider with cached material past its window, and a
            caller on a plain thread with no event loop — the handshake-thread
            shape.
        When:
            That thread reads the credentials.
        Then:
            It should run the refresh and receive the rotated material,
            needing no event loop to do so.
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
        provider = WorkerCredentialsProvider(
            lambda: next(materials), reloadable=True, fresh_for=timedelta(0)
        )
        provider.credentials.get()
        served = []

        # Act
        thread = threading.Thread(
            target=lambda: served.append(provider.credentials.get())
        )
        thread.start()
        thread.join(timeout=5.0)

        # Assert
        assert served == [rotated]
        assert provider.credentials.get() == rotated

    @pytest.mark.asyncio
    async def test_credentials_should_stay_consistent_when_read_from_threads_and_loop(
        self, test_certificates
    ):
        """Test concurrent handshake-thread and dispatch-path callers agree.

        Given:
            One reloadable provider with a wide freshness interval, read
            concurrently by plain threads via get() (the handshake-thread
            shape) and by the event loop via await (the dispatch path).
        When:
            All callers resolve at once.
        Then:
            It should invoke the factory exactly once and hand every caller
            equal material.
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

        provider = WorkerCredentialsProvider(
            factory, reloadable=True, fresh_for=timedelta(seconds=60)
        )
        thread_results = []
        threads = [
            threading.Thread(
                target=lambda: thread_results.append(provider.credentials.get())
            )
            for _ in range(3)
        ]

        async def read():
            return await provider.credentials

        # Act
        for thread in threads:
            thread.start()
        loop_results = await asyncio.gather(*(read() for _ in range(3)))
        for thread in threads:
            thread.join(timeout=5.0)

        # Assert
        assert len(calls) == 1
        assert list(loop_results) + thread_results == [creds] * 6

    def test_pickle_roundtrip_should_survive_plain_pickle_when_factory_is_a_lambda(
        self, test_certificates
    ):
        """Test a closure factory crosses a plain-pickle boundary.

        Given:
            A non-reloadable provider from as_provider, whose factory is a
            lambda closing over the material — the shape plain pickle
            cannot serialize, and the shape multiprocessing's spawn path
            uses when a provider crosses into a worker subprocess.
        When:
            It is round-tripped through pickle and multiprocessing's own
            ForkingPickler.
        Then:
            Both should succeed and the copy should serve equal material,
            because the factory is cloudpickled to bytes rather than left
            to the ambient pickler.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = creds.as_provider()

        # Act
        via_pickle = pickle.loads(pickle.dumps(provider))
        buffer = io.BytesIO()
        ForkingPickler(buffer).dump(provider)
        via_spawn = pickle.loads(buffer.getvalue())

        # Assert
        assert via_pickle.credentials.get() == creds
        assert via_spawn.credentials.get() == creds

    def test_credentials_should_warn_when_refreshes_churn(
        self, test_certificates, caplog
    ):
        """Test sustained value-unstable refreshes emit one rate-limited warning.

        Given:
            A reloadable provider whose factory yields unequal material on
            every refresh — each change silently costs a TLS handshake and a
            pooled channel.
        When:
            Several consecutive background refreshes commit.
        Then:
            It should emit exactly one churn warning — fired once the
            consecutive-unequal threshold is reached and then suppressed by
            the rate limit.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        calls = []

        def factory():
            calls.append(1)
            return WorkerCredentials(
                ca_cert=b"serial %d\n%s" % (len(calls), ca_pem),
                worker_key=key_pem,
                worker_cert=cert_pem,
            )

        provider = WorkerCredentialsProvider(
            factory, reloadable=True, fresh_for=timedelta(0)
        )
        provider.credentials.get()

        # Act
        with caplog.at_level("WARNING", logger="wool.runtime.worker.auth"):
            for _ in range(4):
                provider.credentials.get()

        # Assert
        churn = [r for r in caplog.records if "unequal" in r.getMessage()]
        assert len(churn) == 1

    def test_credentials_should_not_warn_when_material_stable(
        self, test_certificates, caplog
    ):
        """Test stable refresh output emits no churn warning.

        Given:
            A reloadable provider whose factory returns equal material on
            every refresh.
        When:
            Several consecutive background refreshes commit.
        Then:
            It should emit no churn warning.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(
            lambda: creds, reloadable=True, fresh_for=timedelta(0)
        )
        provider.credentials.get()

        # Act
        with caplog.at_level("WARNING", logger="wool.runtime.worker.auth"):
            for _ in range(4):
                provider.credentials.get()

        # Assert
        assert not [r for r in caplog.records if "unequal" in r.getMessage()]

    def test_credentials_should_not_warn_when_single_rotation(
        self, test_certificates, caplog
    ):
        """Test one rotation among stable refreshes is not flagged as churn.

        Given:
            A reloadable provider whose factory rotates its material once and
            then keeps returning the rotated material unchanged.
        When:
            Several consecutive background refreshes commit around the
            rotation.
        Then:
            It should emit no churn warning — a single rotation is expected,
            only sustained instability is churn.
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
        calls = []

        def factory():
            calls.append(1)
            return original if len(calls) == 1 else rotated

        provider = WorkerCredentialsProvider(
            factory, reloadable=True, fresh_for=timedelta(0)
        )
        provider.credentials.get()

        # Act
        with caplog.at_level("WARNING", logger="wool.runtime.worker.auth"):
            for _ in range(4):
                provider.credentials.get()

        # Assert
        assert not [r for r in caplog.records if "unequal" in r.getMessage()]

    def test_credentials_should_warn_again_when_churn_resumes_after_recovery(
        self, test_certificates, caplog
    ):
        """Test a settled churn run does not suppress the next one.

        Given:
            A reloadable provider whose factory churns past the warning
            threshold, settles on stable material, then churns again —
            all well inside one rate-limit interval.
        When:
            Refreshes are driven across the recovery.
        Then:
            It should warn for the second run too, treating it as a new
            incident rather than a continuation of the first.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        settled = b"settled\n" + ca_pem
        sequence = [
            b"first-run-a\n" + ca_pem,
            b"first-run-b\n" + ca_pem,
            b"first-run-c\n" + ca_pem,
            b"first-run-d\n" + ca_pem,
            b"first-run-e\n" + ca_pem,
            settled,
            settled,
            b"second-run-a\n" + ca_pem,
            b"second-run-b\n" + ca_pem,
            b"second-run-c\n" + ca_pem,
        ]
        calls = []

        def factory():
            calls.append(1)
            return WorkerCredentials(
                ca_cert=sequence[len(calls) - 1],
                worker_key=key_pem,
                worker_cert=cert_pem,
            )

        provider = WorkerCredentialsProvider(
            factory, reloadable=True, fresh_for=timedelta(0)
        )

        # Act
        with caplog.at_level("WARNING", logger="wool.runtime.worker.auth"):
            for _ in range(len(sequence)):
                provider.credentials.get()

        # Assert
        churn = [r for r in caplog.records if "unequal" in r.getMessage()]
        assert len(churn) == 2

    def test_credentials_should_report_suppressed_count_when_churn_warning_repeats(
        self, mocker: MockerFixture, test_certificates, caplog
    ):
        """Test a repeated churn warning accounts for what it suppressed.

        Given:
            A reloadable provider churning continuously, whose warning
            has already fired once and suppressed two further refreshes.
        When:
            The rate-limit interval elapses and churn continues.
        Then:
            It should emit a second warning reporting the two
            occurrences it suppressed in between.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        now = [1000.0]
        mocker.patch.object(wt.time, "monotonic", side_effect=lambda: now[0])
        calls = []

        def factory():
            calls.append(1)
            return WorkerCredentials(
                ca_cert=b"serial %d\n%s" % (len(calls), ca_pem),
                worker_key=key_pem,
                worker_cert=cert_pem,
            )

        provider = WorkerCredentialsProvider(
            factory, reloadable=True, fresh_for=timedelta(0)
        )

        # Act
        with caplog.at_level("WARNING", logger="wool.runtime.worker.auth"):
            for _ in range(6):
                provider.credentials.get()
            now[0] += 61.0
            provider.credentials.get()

        # Assert
        churn = [r.getMessage() for r in caplog.records if "unequal" in r.getMessage()]
        assert len(churn) == 2
        assert "(2 similar warnings suppressed)" in churn[1]

    @pytest.mark.filterwarnings(
        "ignore::wool.runtime.worker.exceptions.IneffectivePeersWarning"
    )
    @given(peers=st.one_of(st.none(), st.text()))
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_pickle_roundtrip_should_drop_callback_when_not_reloadable(
        self, peers, test_certificates
    ):
        """Test a non-reloadable provider pickles without its callback.

        Given:
            A non-reloadable provider built over a lambda — which the
            standard library pickler cannot serialize directly — with any
            peer name.
        When:
            It is pickled with the standard library pickler and unpickled.
        Then:
            It should round-trip — the eagerly resolved material rides along and the
            callback is dropped — resolving to equal credentials with the
            normalized peer name preserved.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem
        )
        provider = WorkerCredentialsProvider(lambda: creds, peers=peers)

        # Act
        restored = pickle.loads(pickle.dumps(provider))

        # Assert
        assert restored.credentials.get() == provider.credentials.get()
        expected = peers.strip() if peers is not None else None
        assert restored.peers == (frozenset({expected}) if expected else None)

    @pytest.mark.filterwarnings(
        "ignore::wool.runtime.worker.exceptions.IneffectivePeersWarning"
    )
    @given(peers=st.one_of(st.none(), st.text()))
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_pickle_roundtrip_should_keep_factory_when_reloadable(
        self, peers, temp_cert_files
    ):
        """Test a reloadable provider re-resolves through a pickle roundtrip.

        Given:
            A reloadable, file-backed provider (whose factory is picklable)
            with any peer name, resolved once.
        When:
            It is pickled with the standard library pickler and unpickled.
        Then:
            The restored provider should keep its factory and resolve to the
            same credentials — normalized peer name included — by re-reading
            the unchanged files.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        provider = WorkerCredentialsProvider(
            functools.partial(
                WorkerCredentials.from_files, ca_path, key_path, cert_path
            ),
            peers=peers,
            reloadable=True,
        )
        original = provider.credentials.get()

        # Act
        restored = pickle.loads(pickle.dumps(provider))

        # Assert
        assert restored.credentials.get() == original
        expected = peers.strip() if peers is not None else None
        assert restored.peers == (frozenset({expected}) if expected else None)

    def test_pickle_roundtrip_should_reset_cache_when_reloadable(
        self, temp_cert_files, test_certificates
    ):
        """Test an unpickled reloadable provider re-resolves from its factory.

        Given:
            A reloadable file-backed provider that has resolved once, pickled,
            after which the material on disk is rotated.
        When:
            The unpickled copy resolves for the first time.
        Then:
            It should return the rotated material — the cache and its
            timestamp were reset, forcing a fresh factory call — while the
            original provider still serves its cached material within the
            window.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        ca_path, key_path, cert_path = temp_cert_files
        provider = WorkerCredentialsProvider(
            functools.partial(
                WorkerCredentials.from_files, ca_path, key_path, cert_path
            ),
            reloadable=True,
        )
        original = provider.credentials.get()
        payload = pickle.dumps(provider)
        Path(ca_path).write_bytes(b"-----ROTATED-----\n" + ca_pem)

        # Act
        restored = pickle.loads(payload)
        adopted = restored.credentials.get()

        # Assert
        assert adopted != original
        assert adopted.ca_cert.startswith(b"-----ROTATED-----")
        assert provider.credentials.get() == original

    def test_pickle_roundtrip_should_permit_concurrent_resolve_when_restored(
        self, temp_cert_files
    ):
        """Test the unpickled copy's guard state is usable across threads.

        Given:
            An unpickled reloadable provider — whose lock and flight state
            were dropped in transit and recreated on restore.
        When:
            Several threads read the credentials on the copy concurrently.
        Then:
            It should serve every caller equal material without deadlock or
            error.
        """
        # Arrange
        ca_path, key_path, cert_path = temp_cert_files
        provider = WorkerCredentialsProvider(
            functools.partial(
                WorkerCredentials.from_files, ca_path, key_path, cert_path
            ),
            reloadable=True,
            fresh_for=timedelta(seconds=60),
        )
        restored = pickle.loads(pickle.dumps(provider))
        results = []

        def read():
            results.append(restored.credentials.get())

        threads = [threading.Thread(target=read) for _ in range(4)]

        # Act
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5.0)

        # Assert
        assert len(results) == 4
        assert all(result == results[0] for result in results)


@given(peer=st.one_of(st.none(), st.text()))
@settings(max_examples=50)
def test_normalize_peer_should_strip_and_collapse_blank(peer):
    """Test the one rule every peer-name entry point routes through.

    Given:
        Any candidate peer name, absent or drawn from arbitrary text.
    When:
        The name is normalized.
    Then:
        It should be the stripped name, or None when the name is absent
        or blank once stripped — so a name that survives normalization
        anywhere survives it identically everywhere.
    """
    # Act
    normalized = normalize_peer(peer)

    # Assert
    assert normalized == (peer.strip() or None if peer is not None else None)


@given(peer=st.one_of(st.none(), st.text()))
@settings(max_examples=50)
def test_normalize_peer_should_be_idempotent(peer):
    """Test normalizing an already-normalized name changes nothing.

    Given:
        Any candidate peer name, absent or drawn from arbitrary text.
    When:
        The name is normalized twice.
    Then:
        The second application should be a no-op, so the several layers
        that each normalize independently cannot disagree about a name.
    """
    # Act
    once = normalize_peer(peer)
    twice = normalize_peer(once)

    # Assert
    assert twice == once


@pytest.mark.parametrize(
    "peer",
    [["alpha.svc"], ("alpha.svc",), {"alpha.svc"}, 7],
    ids=["list", "tuple", "set", "int"],
)
def test_normalize_peer_should_raise_when_not_a_string(peer):
    """Test a value that is not a single name is refused by type.

    Given:
        A value that is neither a string nor absent, such as a
        collection of names.
    When:
        The value is normalized.
    Then:
        It should raise TypeError naming the received type and pointing
        a collection at a provider's peers, rather than failing later as
        an AttributeError from a missing strip.
    """
    # Act & assert
    with pytest.raises(TypeError, match="single peer name"):
        normalize_peer(peer)  # pyright: ignore[reportArgumentType]


def test_current_credentials_should_return_none_when_unset():
    """Test the ambient credentials default to unset.

    Given:
        No credentials_scope is active.
    When:
        current_credentials() is called.
    Then:
        It should return None.
    """
    # Act
    current = current_credentials()

    # Assert
    assert current is None


def test_credentials_scope_should_bind_credentials_within_scope(test_certificates):
    """Test bare credentials are bound as a coerced provider.

    Given:
        A bare WorkerCredentials instance.
    When:
        credentials_scope binds it and current_credentials() is called
        inside the scope.
    Then:
        It should return a WorkerCredentialsProvider resolving to the bare
        credentials — the stored value is the coerced provider, so readers
        need not re-coerce.
    """
    # Arrange
    key_pem, cert_pem, ca_pem = test_certificates
    creds = WorkerCredentials(ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem)

    # Act
    with credentials_scope(creds):
        current = current_credentials()

    # Assert
    assert isinstance(current, WorkerCredentialsProvider)
    assert current.credentials.get() == creds


def test_credentials_scope_should_bind_provider_unchanged_when_provider(
    test_certificates,
):
    """Test a provider payload is bound by identity, not re-wrapped.

    Given:
        An existing WorkerCredentialsProvider.
    When:
        credentials_scope binds it and current_credentials() is called
        inside the scope.
    Then:
        It should return the same provider object.
    """
    # Arrange
    key_pem, cert_pem, ca_pem = test_certificates
    creds = WorkerCredentials(ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem)
    provider = WorkerCredentialsProvider(lambda: creds)

    # Act
    with credentials_scope(provider):
        current = current_credentials()

    # Assert
    assert current is provider


def test_credentials_scope_should_reset_binding_on_exit(test_certificates):
    """Test the ambient binding is reset when the scope exits.

    Given:
        A credentials_scope bound over bare credentials.
    When:
        The scope exits normally and current_credentials() is called.
    Then:
        It should return None again.
    """
    # Arrange
    key_pem, cert_pem, ca_pem = test_certificates
    creds = WorkerCredentials(ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem)

    # Act
    with credentials_scope(creds):
        pass

    # Assert
    assert current_credentials() is None


def test_credentials_scope_should_reset_binding_when_body_raises(test_certificates):
    """Test the ambient binding is reset when the scope body raises.

    Given:
        A credentials_scope bound over bare credentials.
    When:
        The scope body raises and the exception propagates.
    Then:
        It should reset the binding to None on the way out.
    """
    # Arrange
    key_pem, cert_pem, ca_pem = test_certificates
    creds = WorkerCredentials(ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem)

    # Act
    with pytest.raises(RuntimeError, match="boom"):
        with credentials_scope(creds):
            raise RuntimeError("boom")

    # Assert
    assert current_credentials() is None


def test_credentials_scope_should_restore_outer_binding_when_nested(test_certificates):
    """Test exiting a nested scope restores the outer binding.

    Given:
        An outer credentials_scope binding one provider and an inner scope
        binding another.
    When:
        The inner scope exits.
    Then:
        It should restore the outer provider as the ambient binding.
    """
    # Arrange
    key_pem, cert_pem, ca_pem = test_certificates
    creds = WorkerCredentials(ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem)
    outer = WorkerCredentialsProvider(lambda: creds)
    inner = WorkerCredentialsProvider(lambda: creds)

    # Act
    with credentials_scope(outer):
        with credentials_scope(inner):
            pass
        current = current_credentials()

    # Assert
    assert current is outer
