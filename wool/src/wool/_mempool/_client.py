from __future__ import annotations

import asyncio
from typing import Final
from typing import Protocol

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

import grpc

try:
    from wool._protobuf.mempool import service_pb2 as pb
    from wool._protobuf.mempool import service_pb2_grpc as rpc
except ImportError as e:
    from wool._protobuf import ProtobufImportError

    raise ProtobufImportError(e) from e


class EventHandler(Protocol):
    @staticmethod
    def __call__(client: MemoryPoolClient, response: pb.Event) -> None: ...


class MemoryPoolClient:
    _event_stream: Final[grpc.aio.UnaryStreamCall]
    _listener: asyncio.Task
    _session: pb.Session | None

    def __init__(self, channel: grpc.aio.Channel, event_handler: EventHandler):
        self._stub = rpc.MemoryPoolStub(channel)
        self._event_stream = self._stub.session(pb.SessionRequest())
        self._event_handler = event_handler
        self._session = None

    async def __aenter__(self) -> Self:
        if self._session:
            raise RuntimeError(
                "Client session may not be re-entered. "
                "Use 'async with MemoryPoolClient(...)' only once."
            )
        response = await self._event_stream.read()
        assert isinstance(response, pb.SessionResponse)
        self._session = response.session
        self._listener = asyncio.create_task(self._listen())
        return self

    async def __aexit__(self, *_):
        if not self._session:
            raise RuntimeError(
                "Client session has not been entered. "
                "Use 'async with MemoryPoolClient(...)'."
            )
        assert self._listener
        self._listener.cancel()
        try:
            await self._listener
        except asyncio.CancelledError:
            pass

    def __del__(self):
        try:
            self._event_stream.cancel()
        except Exception:
            pass

    @property
    def id(self) -> str:
        if not self._session:
            raise RuntimeError(
                "Client session has not been entered. "
                "Use 'async with MemoryPoolClient(...)'."
            )
        return self._session.id

    async def map(self, ref: str):
        if not self._session:
            raise RuntimeError(
                "Client session has not been entered. "
                "Use 'async with MemoryPoolClient(...)'."
            )
        request = pb.AcquireRequest(
            session=self._session,
            reference=pb.Reference(id=ref),
        )
        await self._stub.map(request)

    async def get(self, ref: str) -> bytes:
        if not self._session:
            raise RuntimeError(
                "Client session has not been entered. "
                "Use 'async with MemoryPoolClient(...)'."
            )
        request = pb.GetRequest(
            session=self._session,
            reference=pb.Reference(id=ref),
        )
        response: pb.GetResponse = await self._stub.get(request)
        return response.dump

    async def put(self, dump: bytes, *, mutable: bool = False) -> str:
        if not self._session:
            raise RuntimeError(
                "Client session has not been entered. "
                "Use 'async with MemoryPoolClient(...)'."
            )
        request = pb.PutRequest(
            session=self._session,
            dump=dump,
            mutable=mutable,
        )
        response: pb.PutResponse = await self._stub.put(request)
        return response.reference.id

    async def post(self, ref: str, dump: bytes) -> bool:
        if not self._session:
            raise RuntimeError(
                "Client session has not been entered. "
                "Use 'async with MemoryPoolClient(...)'."
            )
        request = pb.PostRequest(
            session=self._session,
            reference=pb.Reference(id=ref),
            dump=dump,
        )
        response: pb.PostResponse = await self._stub.post(request)
        return response.updated

    async def acquire(self, ref: str):
        if not self._session:
            raise RuntimeError(
                "Client session has not been entered. "
                "Use 'async with MemoryPoolClient(...)'."
            )
        request = pb.AcquireRequest(
            session=self._session,
            reference=pb.Reference(id=ref),
        )
        await self._stub.acquire(request)

    async def release(self, ref: str):
        if not self._session:
            raise RuntimeError(
                "Client session has not been entered. "
                "Use 'async with MemoryPoolClient(...)'."
            )
        request = pb.ReleaseRequest(
            session=self._session,
            reference=pb.Reference(id=ref),
        )
        await self._stub.release(request)

    async def _listen(self):
        assert self._event_stream
        try:
            while True:
                if (response := await self._event_stream.read()) is None:
                    break
                assert isinstance(response, pb.SessionResponse), (
                    f"Unexpected event type: {type(response)}"
                )
                self._event_handler(self, response.event)
        except asyncio.CancelledError:
            self._event_stream.cancel()
