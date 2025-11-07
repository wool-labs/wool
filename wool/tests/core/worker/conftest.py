import uuid
from types import MappingProxyType

import pytest

from wool.core.discovery.base import WorkerInfo


@pytest.fixture
def worker_info():
    """Provides sample WorkerInfo for testing.

    Creates a WorkerInfo instance with typical field values for use in
    tests that need a well-formed worker instance.
    """
    return WorkerInfo(
        uid=uuid.UUID("12345678-1234-5678-1234-567812345678"),
        host="localhost",
        port=50051,
        pid=12345,
        version="1.0.0",
        tags=frozenset(["test", "worker"]),
        extra=MappingProxyType({"key": "value"}),
    )


@pytest.fixture
def worker_tags():
    """Provides sample worker tags for testing."""
    return ("gpu", "ml-capable", "production")


@pytest.fixture
def worker_extra():
    """Provides sample worker extra metadata for testing."""
    return {"region": "us-west-2", "instance_type": "t3.large"}
