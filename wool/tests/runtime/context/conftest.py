import pytest

from tests.helpers import scoped_context


@pytest.fixture(autouse=True)
def isolated_context():
    """Install a fresh wool.Context for the duration of each test."""
    with scoped_context():
        yield
