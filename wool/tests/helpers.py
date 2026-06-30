import datetime
import ipaddress
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from typing import NamedTuple

from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

import wool

#: SANs covering the loopback addresses test workers bind to.
LOOPBACK_SANS = (
    x509.DNSName("localhost"),
    x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
)


class CertificateMaterial(NamedTuple):
    """PEM bytes for one certificate set, held in memory."""

    ca_pem: bytes
    key_pem: bytes
    cert_pem: bytes


class CertificateFiles(NamedTuple):
    """Paths and PEM bytes for one certificate set written to disk."""

    ca_path: str
    key_path: str
    cert_path: str
    ca_pem: bytes
    key_pem: bytes
    cert_pem: bytes


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


@contextmanager
def scoped_context() -> Generator[None]:
    """Test helper — bracket a block of Wool chain mutations.

    Per-test isolation lives in the ``pytest_pyfunc_call`` hook in
    ``tests/conftest.py``, which runs each sync test in a fresh
    :func:`contextvars.copy_context` (async tests self-isolate via their
    task's context copy). With ``__chain__`` typed
    :class:`~wool.runtime.context.chain.Chain` there is no settable
    "unarmed" value to install in place, so this manager no longer
    disarms; it is retained as a no-op scope around chain mutations.
    """
    yield


def context_is_unarmed() -> bool:
    """Test helper — return whether the current context carries no Wool state.

    A module-level, picklable function so it can be dispatched to a
    :class:`~concurrent.futures.ProcessPoolExecutor` worker, where it
    proves a bare ``run_in_executor`` offload carries no Wool chain
    into a worker process.
    """
    return wool.__chain__.get(None) is None


def generate_ca_and_leaf(sans, *, common_name="wool-worker", self_signed=False):
    """Generate a fresh CA and a leaf certificate carrying *sans*.

    The leaf is granted both server and client extended key usages so it
    works on both sides of a mutual-TLS connection. By default the leaf
    is signed by a freshly generated CA; with ``self_signed=True`` the
    leaf signs itself and doubles as its own trust root (the degenerate
    single-certificate case), so the returned CA PEM is the leaf PEM.

    Returns a `CertificateMaterial`, so callers bind the three PEMs by
    name — all three are `bytes`, and a permuted positional unpack
    mis-wires silently rather than raising.
    """

    def _key():
        return rsa.generate_private_key(
            public_exponent=65537, key_size=2048, backend=default_backend()
        )

    now = datetime.datetime.now(datetime.UTC)
    leaf_key = _key()
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    if self_signed:
        issuer = subject
        signing_key = leaf_key
        ca_cert = None
    else:
        signing_key = _key()
        issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "wool-test-ca")])
        ca_cert = (
            x509.CertificateBuilder()
            .subject_name(issuer)
            .issuer_name(issuer)
            .public_key(signing_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + datetime.timedelta(days=365))
            .add_extension(
                x509.BasicConstraints(ca=True, path_length=None), critical=True
            )
            .sign(signing_key, hashes.SHA256(), default_backend())
        )

    leaf_cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
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
        .sign(signing_key, hashes.SHA256(), default_backend())
    )

    leaf_pem = leaf_cert.public_bytes(serialization.Encoding.PEM)
    key_pem = leaf_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    ca_pem = (
        leaf_pem if ca_cert is None else ca_cert.public_bytes(serialization.Encoding.PEM)
    )
    return CertificateMaterial(ca_pem=ca_pem, key_pem=key_pem, cert_pem=leaf_pem)


def write_certificate_files(directory, ca_pem, key_pem, cert_pem):
    """Write the given PEM material as ``ca.pem``, ``key.pem``, ``cert.pem``.

    Writes under *directory*, overwriting any previous material — calling
    it again with the same directory is an in-place rotation. Returns a
    `CertificateFiles` carrying both the file paths and the PEM bytes.
    """
    ca_path = directory / "ca.pem"
    key_path = directory / "key.pem"
    cert_path = directory / "cert.pem"
    ca_path.write_bytes(ca_pem)
    key_path.write_bytes(key_pem)
    cert_path.write_bytes(cert_pem)
    return CertificateFiles(
        str(ca_path), str(key_path), str(cert_path), ca_pem, key_pem, cert_pem
    )


def generate_certificate_files(directory, sans, *, common_name="wool-worker"):
    """Generate a CA and leaf for *sans* and write them as PEM files.

    The generating layer over `write_certificate_files`: SANs in, paths
    out. Returns the same `CertificateFiles` that writing pre-generated
    material returns.
    """
    material = generate_ca_and_leaf(sans, common_name=common_name)
    return write_certificate_files(
        directory, material.ca_pem, material.key_pem, material.cert_pem
    )
