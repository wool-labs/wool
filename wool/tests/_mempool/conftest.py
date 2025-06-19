import shutil

import pytest
import pytest_asyncio

from wool._mempool import MemoryPool
from wool._mempool._service import MemoryPoolService


@pytest_asyncio.fixture(scope="function")
async def seed():
    mempool = MemoryPool()
    await mempool.put(b"Ad meliora", mutable=True, ref="meliora")
    await mempool.put(b"Ad aevum", mutable=False, ref="aevum")
    yield mempool
    shutil.rmtree(mempool.path)
    del mempool


@pytest.fixture(scope="session")
def grpc_add_to_server():
    from wool._protobuf.mempool.mempool_pb2_grpc import (
        add_MemoryPoolServicer_to_server,
    )

    return add_MemoryPoolServicer_to_server


@pytest.fixture(scope="session")
def grpc_servicer():
    return MemoryPoolService()


@pytest.fixture(scope="session")
def grpc_stub_cls():
    from wool._protobuf.mempool.mempool_pb2_grpc import MemoryPoolStub

    return MemoryPoolStub
