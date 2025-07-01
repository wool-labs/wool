from __future__ import annotations

import asyncio
from typing import AsyncGenerator
from typing import Final
from weakref import WeakSet
from weakref import WeakValueDictionary

import shortuuid
from grpc.aio import ServicerContext

from wool._mempool import MemoryPool
from wool._protobuf.mempool import mempool_pb2 as proto
from wool._protobuf.mempool import mempool_pb2_grpc as rpc


class Session:
    """
    A session represents a client connection to the memory pool service and
    serves as the scope for any shared references acquired over its duration.
    """

    id: Final[str]
    queue: Final[asyncio.Queue[proto.SessionResponse]]
    references: Final[set[Reference]]
    sessions: Final[WeakValueDictionary[str, Session]] = WeakValueDictionary()

    @classmethod
    def get(cls, id: str) -> Session | None:
        return cls.sessions.get(id)

    def __init__(self):
        self.id = shortuuid.uuid()
        self.queue = asyncio.Queue()
        self.references = set()
        self.sessions[self.id] = self

    def __eq__(self, other) -> bool:
        if isinstance(other, Session):
            return self.id == other.id
        return False

    def __hash__(self) -> int:
        return hash(self.id)


class Reference:
    id: Final[str]
    mempool: Final[MemoryPool]
    sessions: Final[WeakSet[Session]]

    _references: Final[WeakValueDictionary[str, Reference]] = (
        WeakValueDictionary()
    )
    _to_delete: Final[set[str]] = set()

    @classmethod
    def new(cls, id: str, *, mempool: MemoryPool) -> Reference:
        if id in cls._references:
            raise ValueError(f"Session {id} already exists")
        return cls(id, mempool=mempool)

    def __new__(cls, id: str, *, mempool: MemoryPool):
        if id in cls._references:
            if id in cls._to_delete:
                cls._to_delete.remove(id)
            return cls._references[id]
        return super().__new__(cls)

    def __init__(self, id: str, *, mempool: MemoryPool):
        self.id = id
        self.mempool = mempool
        self.sessions = WeakSet()
        self._references[id] = self

    def __eq__(self, other) -> bool:
        if isinstance(other, Reference):
            return self.id == other.id
        return False

    def __hash__(self) -> int:
        return hash(self.id)

    def __del__(self):
        self._to_delete.add(self.id)

        id = self.id
        to_delete = self._to_delete
        mempool = self.mempool

        async def _delete():
            if id in to_delete:
                try:
                    to_delete.remove(id)
                    await mempool.delete(id)
                except FileNotFoundError:
                    pass

        try:
            asyncio.get_running_loop().create_task(_delete())
        except RuntimeError:
            asyncio.new_event_loop().run_until_complete(_delete())


class MemoryPoolService(rpc.MemoryPoolServicer):
    def __init__(self, mempool: MemoryPool | None = None):
        self._mempool = mempool or MemoryPool()
        self._shutdown = asyncio.Event()

    async def session(
        self, request: proto.SessionRequest, context: ServicerContext
    ) -> AsyncGenerator[proto.SessionResponse]:
        session = Session()
        yield proto.SessionResponse(session=proto.Session(id=session.id))
        while True:
            yield await session.queue.get()

    async def acquire(
        self, request: proto.AcquireRequest, context: ServicerContext
    ) -> proto.AcquireResponse:
        if not (session := Session.get(request.session.id)):
            raise ValueError(f"Session {request.session.id} not found")
        await self._mempool.map(request.reference.id)
        session.references.add(
            Reference(
                id=request.reference.id,
                mempool=self._mempool,
            )
        )
        return proto.AcquireResponse()

    async def put(
        self, request: proto.PutRequest, context: ServicerContext
    ) -> proto.PutResponse:
        if not (session := Session.get(request.session.id)):
            raise ValueError(f"Session {request.session.id} not found")
        reference = Reference(
            id=await self._mempool.put(request.dump, mutable=request.mutable),
            mempool=self._mempool,
        )
        session.references.add(
            Reference(
                id=reference.id,
                mempool=self._mempool,
            )
        )
        return proto.PutResponse(reference=proto.Reference(id=reference.id))

    async def post(
        self, request: proto.PostRequest, context: ServicerContext
    ) -> proto.PostResponse:
        if not (session := Session.get(request.session.id)):
            raise ValueError(f"Session {request.session.id} not found")
        updated = await self._mempool.post(request.reference.id, request.dump)
        if updated:
            for session in Reference(
                id=request.reference.id, mempool=self._mempool
            ).sessions:
                if session.id is not request.session.id:
                    event = proto.Event(
                        reference=request.reference,
                        event_type="post",
                    )
                    await session.queue.put(proto.SessionResponse(event=event))
        return proto.PostResponse(updated=updated)

    async def get(
        self, request: proto.GetRequest, context: ServicerContext
    ) -> proto.GetResponse:
        if not Session.get(request.session.id):
            raise ValueError(f"Session {request.session.id} not found")
        dump = await self._mempool.get(request.reference.id)
        return proto.GetResponse(dump=dump)

    async def release(
        self, request: proto.ReleaseRequest, context: ServicerContext
    ) -> proto.ReleaseResponse:
        if not (session := Session.get(request.session.id)):
            raise ValueError(f"Session {request.session.id} not found")
        reference = Reference(
            id=request.reference.id,
            mempool=self._mempool,
        )
        session.references.remove(reference)
        return proto.ReleaseResponse()
