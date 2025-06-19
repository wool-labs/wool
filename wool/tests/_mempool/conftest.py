import pytest

from wool._mempool._service import MemoryPoolService


# This fixture registers the async servicer with pytest-grpc
@pytest.fixture(scope="session")
def grpc_add_to_server():
    from wool._protobuf._mempool._mempool_pb2_grpc import (
        add_MemoryPoolServicer_to_server,
    )

    def _add_MemoryPoolService_to_server(server):
        add_MemoryPoolServicer_to_server(MemoryPoolService(), server)

    return [_add_MemoryPoolService_to_server]


# This fixture provides an instance of your servicer
@pytest.fixture(scope="session")
def grpc_servicer():
    return MemoryPoolService()


# This fixture provides the stub class for creating a client
@pytest.fixture(scope="session")
def grpc_stub_cls():
    from wool._protobuf._mempool._mempool_pb2_grpc import MemoryPoolStub

    return MemoryPoolStub
