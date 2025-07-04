from __future__ import annotations

import asyncio
from typing import Final
from typing import Protocol

import grpc

from wool._protobuf.mempool import service_pb2 as proto
from wool._protobuf.mempool import service_pb2_grpc as rpc


class EventHandler(Protocol):
    @staticmethod
    def __call__(client: MemoryPoolSession, response: proto.Event) -> None: ...


class MemoryPoolSession:
    _entered: bool = False
    _listener: asyncio.Task | None = None
    _session: proto.Session | None = None
    _event_stream: Final[grpc.aio.UnaryStreamCall]

    def __init__(self, channel, handler: EventHandler):
        self._stub = rpc.MemoryPoolStub(channel)
        self._event_stream = self._stub.session(proto.SessionRequest())
        self._handler = handler

    async def __aenter__(self):
        if self._entered:
            raise RuntimeError(
                "Session may not be re-entered. "
                "Use 'async with MemoryPoolSession(...)' only once."
            )
        self._entered = True
        session = await self._event_stream.read()
        assert isinstance(session, proto.Session)
        self._session = session
        self._listener = asyncio.create_task(self._listen())
        return self

    async def __aexit__(self, *_):
        assert self._listener
        self._listener.cancel()
        await self._listener

    def __del__(self):
        try:
            self._event_stream.cancel()
        except Exception:
            pass

    async def put(self, dump: bytes, *, mutable: bool = False) -> str:
        if not self._session:
            raise RuntimeError(
                "Session has not been enetered. "
                "Use 'async with MemoryPoolSession(...)'."
            )
        request = proto.PutRequest(
            session=self._session,
            dump=dump,
            mutable=mutable,
        )
        response: proto.PutResponse = await self._stub.put(request)
        return response.reference.id

    async def post(self, ref: str, dump: bytes) -> bool:
        if not self._session:
            raise RuntimeError(
                "Session has not been enetered. "
                "Use 'async with MemoryPoolSession(...)'."
            )
        request = proto.PostRequest(
            session=self._session,
            reference=proto.Reference(id=ref),
            dump=dump,
        )
        response: proto.PostResponse = await self._stub.post(request)
        return response.updated

    async def get(self, ref: str) -> bytes:
        if not self._session:
            raise RuntimeError(
                "Session has not been enetered. "
                "Use 'async with MemoryPoolSession(...)'."
            )
        request = proto.GetRequest(
            session=self._session,
            reference=proto.Reference(id=ref),
        )
        response: proto.GetResponse = await self._stub.get(request)
        return response.dump

    async def _listen(self):
        assert self._event_stream
        try:
            async for event in self._event_stream:
                assert isinstance(event, proto.Event)
                self._handler(self, event)
        except asyncio.CancelledError:
            self._event_stream.cancel()
