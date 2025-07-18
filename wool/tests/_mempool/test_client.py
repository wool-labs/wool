from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncContextManager
from typing import AsyncGenerator
from typing import Callable
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from wool._mempool._client import MemoryPoolClient
from wool._mempool._client import pb
from wool._mempool._client import rpc
from wool._mempool._service import MemoryPoolService

SessionFactory = Callable[[], AsyncContextManager[MemoryPoolClient]]


@pytest.fixture(scope="function")
def mempool_session(grpc_aio_channel, mocker: MockerFixture) -> SessionFactory:
    @asynccontextmanager
    async def _mempool_session() -> AsyncGenerator[MemoryPoolClient]:
        MockMemoryPoolStub = mocker.patch.object(
            rpc, "MemoryPoolStub", spec=MemoryPoolService
        )
        MockMemoryPoolStub().session.return_value = MagicMock(
            read=AsyncMock(
                side_effect=[
                    pb.SessionResponse(
                        session=pb.Session(id="test_session_id")
                    ),
                    pb.SessionResponse(
                        event=pb.Event(
                            event_type="foo",
                            reference=pb.Reference(id="meliora"),
                        ),
                    ),
                    None,
                ]
            )
        )
        async with grpc_aio_channel() as channel:
            yield MemoryPoolClient(channel, event_handler=MagicMock())

    return _mempool_session


@pytest.mark.asyncio
class TestMemoryPoolSession:
    async def test_async_context_manager(
        self, seed, mempool_session: SessionFactory
    ):
        async with mempool_session() as session:
            async with session:
                assert session._session is not None
                await asyncio.sleep(0)
                assert cast(MagicMock, session._event_handler).call_count == 1

    async def test_map(
        self, seed, mempool_session: SessionFactory, mocker: MockerFixture
    ):
        async with mempool_session() as session:
            with pytest.raises(RuntimeError):
                await session.map("meliora")
            async with session:
                await session.map("meliora")
                assert session._stub.map.call_count == 1

    async def test_get(
        self, seed, mempool_session: SessionFactory, mocker: MockerFixture
    ):
        async with mempool_session() as session:
            with pytest.raises(RuntimeError):
                await session.get("meliora")
            async with session:
                await session.get("meliora")
                assert session._stub.get.call_count == 1

    async def test_put(
        self, seed, mempool_session: SessionFactory, mocker: MockerFixture
    ):
        async with mempool_session() as session:
            with pytest.raises(RuntimeError):
                await session.put(b"foo", mutable=True)
            async with session:
                await session.put(b"foo", mutable=True)
                assert session._stub.put.call_count == 1

    async def test_post(
        self, seed, mempool_session: SessionFactory, mocker: MockerFixture
    ):
        async with mempool_session() as session:
            with pytest.raises(RuntimeError):
                await session.post("meliora", b"aroilem")
            async with session:
                await session.post("meliora", b"aroilem")
                assert session._stub.post.call_count == 1

    async def test_acquire(
        self, seed, mempool_session: SessionFactory, mocker: MockerFixture
    ):
        async with mempool_session() as session:
            with pytest.raises(RuntimeError):
                await session.acquire("meliora")
            async with session:
                await session.acquire("meliora")
                assert session._stub.acquire.call_count == 1

    async def test_release(
        self, seed, mempool_session: SessionFactory, mocker: MockerFixture
    ):
        async with mempool_session() as session:
            with pytest.raises(RuntimeError):
                await session.release("meliora")
            async with session:
                await session.release("meliora")
                assert session._stub.release.call_count == 1
