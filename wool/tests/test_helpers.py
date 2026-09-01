"""Tests for the certificate test helpers.

These pin the helpers themselves, since a silent divergence between the
two authority-issuing paths would weaken every suite that builds
certificates through them rather than failing visibly here.
"""

from cryptography.x509.oid import NameOID

from tests.helpers import LOOPBACK_SANS
from tests.helpers import generate_authority
from tests.helpers import generate_ca_and_leaf


def _issuer_common_name(pem: bytes) -> str:
    """Return the issuer common name of the leaf in a PEM bundle."""
    from cryptography import x509

    return (
        x509.load_pem_x509_certificate(pem)
        .issuer.get_attributes_for_oid(NameOID.COMMON_NAME)[0]
        .value
    )  # type: ignore[return-value]


def test_generate_ca_and_leaf_should_issue_from_one_authority_shape():
    """Test the implicit and explicit authority paths agree on the issuer.

    Given:
        A leaf issued with no authority supplied, and one issued under an
        explicitly generated authority.
    When:
        The issuer common name of each leaf is read.
    Then:
        It should be identical, so the two paths cannot drift into
        issuing under differently shaped authorities.
    """
    # Arrange
    implicit = generate_ca_and_leaf(LOOPBACK_SANS)
    explicit = generate_ca_and_leaf(LOOPBACK_SANS, authority=generate_authority())

    # Act
    implicit_issuer = _issuer_common_name(implicit.cert_pem)
    explicit_issuer = _issuer_common_name(explicit.cert_pem)

    # Assert
    assert implicit_issuer == explicit_issuer


def test_generate_ca_and_leaf_should_self_sign_when_requested():
    """Test a self-signed leaf is its own trust anchor.

    Given:
        A leaf requested as self-signed.
    When:
        The returned material is inspected.
    Then:
        The CA bundle should be the leaf itself, since there is no
        separate authority to trust.
    """
    # Act
    material = generate_ca_and_leaf(LOOPBACK_SANS, self_signed=True)

    # Assert
    assert material.ca_pem == material.cert_pem
