import pytest

from tests.helpers import scoped_context


@pytest.fixture(autouse=True)
def isolated_context():
    """Run each test under a fresh, unarmed Wool context.

    Resets the wool-owned context ``contextvars.ContextVar`` so a
    :meth:`wool.ContextVar.set` in one test does not leak its armed
    context into the next.
    """
    with scoped_context():
        yield
