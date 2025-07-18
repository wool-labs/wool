from __future__ import annotations

import asyncio
from typing import AsyncGenerator
from typing import Final
from weakref import WeakSet
from weakref import WeakValueDictionary

import shortuuid
from grpc.aio import ServicerContext

from wool._mempool import MemoryPool

try:
    from wool._protobuf.mempool import service_pb2 as pb
    from wool._protobuf.mempool import service_pb2_grpc as rpc
except ImportError as e:
    from wool._protobuf import ProtobufImportError

    raise ProtobufImportError(e) from e


class _Session:
    id: Final[str]
    queue: Final[asyncio.Queue[pb.SessionResponse]]
    references: Final[set[_Reference]]

    _sessions: Final[WeakValueDictionary[str, _Session]] = (
        WeakValueDictionary()
    )

    @classmethod
    def get(cls, id: str) -> _Session | None:
        return cls._sessions.get(id)

    def __init__(self):
        self.id = shortuuid.uuid()
        self.queue = asyncio.Queue()
        self.references = set()
        self._sessions[self.id] = self

    def __eq__(self, other) -> bool:
        if isinstance(other, _Session):
            return self.id == other.id
        return False

    def __hash__(self) -> int:
        return hash(self.id)


class _Reference:
    id: Final[str]
    sessions: Final[WeakSet[_Session]]

    _mempool: Final[MemoryPool]
    _references: Final[WeakValueDictionary[str, _Reference]] = (
        WeakValueDictionary()
    )
    _to_delete: Final[set[str]] = set()
    _initialized: bool = False

    @classmethod
    def get(cls, id: str) -> _Reference | None:
        return cls._references.get(id)

    @classmethod
    def new(cls, id: str, *, mempool: MemoryPool) -> _Reference:
        if id in cls._references:
            raise ValueError(f"Reference {id} already exists")
        return cls(id, mempool=mempool)

    def __new__(cls, id: str, *, mempool: MemoryPool):
        if id in cls._references:
            if id in cls._to_delete:
                cls._to_delete.remove(id)
            return cls._references[id]
        return super().__new__(cls)

    def __init__(self, id: str, *, mempool: MemoryPool):
        if not self._initialized:
            self.id = id
            self.sessions = WeakSet()
            self._mempool = mempool
            self._references[id] = self
            self._initialized = True

    def __eq__(self, other) -> bool:
        if isinstance(other, _Reference):
            return self.id == other.id
        return False

    def __hash__(self) -> int:
        return hash(self.id)

    def __del__(self):
        self._to_delete.add(self.id)

        async def _delete(id, to_delete, mempool):
            if id in to_delete:
                try:
                    to_delete.remove(id)
                    await mempool.delete(id)
                except FileNotFoundError:
                    pass

        try:
            asyncio.get_running_loop().create_task(
                _delete(self.id, self._to_delete, self._mempool)
            )
        except RuntimeError:
            asyncio.new_event_loop().run_until_complete(
                _delete(self.id, self._to_delete, self._mempool)
            )


class MemoryPoolService(rpc.MemoryPoolServicer):
    def __init__(self, mempool: MemoryPool | None = None):
        self._mempool = mempool or MemoryPool()
        self._shutdown = asyncio.Event()

    async def session(
        self, request: pb.SessionRequest, context: ServicerContext
    ) -> AsyncGenerator[pb.SessionResponse]:
        session = _Session()
        yield pb.SessionResponse(session=pb.Session(id=session.id))
        while True:
            yield await session.queue.get()

    async def map(
        self, request: pb.AcquireRequest, context: ServicerContext
    ) -> pb.AcquireResponse:
        if not (session := _Session.get(request.session.id)):
            raise ValueError(f"Session {request.session.id} not found")
        await self._mempool.map(request.reference.id or None)
        reference = _Reference(request.reference.id, mempool=self._mempool)
        await self.acquire(
            pb.AcquireRequest(
                session=pb.Session(id=session.id),
                reference=pb.Reference(id=reference.id),
            ),
            context,
        )
        return pb.AcquireResponse()

    async def put(
        self, request: pb.PutRequest, context: ServicerContext
    ) -> pb.PutResponse:
        if not (session := _Session.get(request.session.id)):
            raise ValueError(f"Session {request.session.id} not found")
        reference = _Reference(
            id=await self._mempool.put(request.dump, mutable=request.mutable),
            mempool=self._mempool,
        )
        await self.acquire(
            pb.AcquireRequest(
                session=pb.Session(id=session.id),
                reference=pb.Reference(id=reference.id),
            ),
            context,
        )
        return pb.PutResponse(reference=pb.Reference(id=reference.id))

    async def get(
        self, request: pb.GetRequest, context: ServicerContext
    ) -> pb.GetResponse:
        if not (session := _Session.get(request.session.id)):
            raise ValueError(f"Session {request.session.id} not found")
        if _Reference.get(request.reference.id) is None:
            await self.acquire(
                pb.AcquireRequest(
                    session=pb.Session(id=session.id),
                    reference=pb.Reference(id=request.reference.id),
                ),
                context,
            )
        dump = await self._mempool.get(request.reference.id)
        return pb.GetResponse(dump=dump)

    async def post(
        self, request: pb.PostRequest, context: ServicerContext
    ) -> pb.PostResponse:
        if not (session := _Session.get(request.session.id)):
            raise ValueError(f"Session {request.session.id} not found")
        if not (reference := _Reference.get(request.reference.id)):
            raise ValueError(f"Reference {request.reference.id} not found")
        if reference not in session.references:
            await self.acquire(
                pb.AcquireRequest(
                    session=pb.Session(id=session.id),
                    reference=pb.Reference(id=reference.id),
                ),
                context,
            )
        updated = await self._mempool.post(request.reference.id, request.dump)
        if updated:
            for session in _Reference(
                id=request.reference.id, mempool=self._mempool
            ).sessions:
                if session.id is not request.session.id:
                    event = pb.Event(
                        reference=request.reference,
                        event_type="post",
                    )
                    await session.queue.put(pb.SessionResponse(event=event))
        return pb.PostResponse(updated=updated)

    async def acquire(
        self, request: pb.AcquireRequest, context: ServicerContext
    ) -> pb.AcquireResponse:
        if not (session := _Session.get(request.session.id)):
            raise ValueError(f"Session {request.session.id} not found")
        if not (reference := _Reference.get(request.reference.id)):
            raise ValueError(f"Reference {request.reference.id} not found")
        session.references.add(reference)
        reference.sessions.add(session)
        return pb.AcquireResponse()

    async def release(
        self, request: pb.ReleaseRequest, context: ServicerContext
    ) -> pb.ReleaseResponse:
        if not (session := _Session.get(request.session.id)):
            raise ValueError(f"Session {request.session.id} not found")
        if not (reference := _Reference.get(request.reference.id)):
            raise ValueError(f"Reference {request.reference.id} not found")
        session.references.remove(reference)
        reference.sessions.remove(session)
        return pb.ReleaseResponse()
