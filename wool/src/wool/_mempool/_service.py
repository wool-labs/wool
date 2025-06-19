from __future__ import annotations

import asyncio
import pickle
from functools import wraps
from typing import Final
from weakref import WeakSet
from weakref import WeakValueDictionary
from weakref import ref

import shortuuid
from grpc import RpcError
from grpc import ServicerContext
from grpc import StatusCode

from wool._mempool import MemoryPool
from wool._protobuf._mempool._mempool_pb2 import Event
from wool._protobuf._mempool._mempool_pb2 import Post
from wool._protobuf._mempool._mempool_pb2 import Put
from wool._protobuf._mempool._mempool_pb2 import Request
from wool._protobuf._mempool._mempool_pb2 import Response
from wool._protobuf._mempool._mempool_pb2 import SessionRequest
from wool._protobuf._mempool._mempool_pb2_grpc import MemoryPoolServicer


def _raises_rpc_error(code: StatusCode):
    def wrapper(wrapped):
        @wraps(wrapped)
        async def wrapping(_, request, context: ServicerContext):
            try:
                return await wrapped(request, context)
            except Exception as e:
                context.set_code(code)
                context.set_details(str(e))
                raise RpcError from e

        return wrapping

    return wrapper


class Session:
    """
    A session represents a client connection to the memory pool service and
    serves as the scope for any shared references acquired over its duration.
    """

    id: Final[str]
    queue: Final[asyncio.Queue]
    references: Final[set[Reference]]

    _sessions: Final[WeakValueDictionary[str, Session]] = WeakValueDictionary()

    @classmethod
    def get(cls, id: str) -> Session | None:
        return cls._sessions.get(id)

    def __init__(self):
        self.id = shortuuid.uuid()
        self.queue = asyncio.Queue()
        self.references = set()
        self._sessions[self.id] = self

    def __del__(self):
        del self._sessions[self.id]

    def __eq__(self, other):
        if isinstance(other, Session):
            return self.id == other.id
        return False

    def __hash__(self) -> int:
        return hash(self.id)


class Reference:
    id: Final[str]
    mempool: Final[MemoryPool]
    sessions: Final[WeakSet[Session]]

    _references: Final[dict[str, ref[Reference]]] = dict()
    _to_delete: Final[set[str]] = set()

    @classmethod
    def new(cls, id: str, *, mempool) -> Reference:
        if id in cls._references:
            raise ValueError(f"Session {id} already exists")
        return cls(id, mempool=mempool)

    def __new__(cls, id: str, *, mempool):
        if id in cls._references:
            if id in cls._to_delete:
                cls._to_delete.remove(id)
            referent = cls._references[id]()
            assert referent
            return referent
        return super().__new__(cls)

    def __init__(self, id: str, *, mempool):
        self.id = id
        self.mempool = mempool
        self.sessions = WeakSet()
        self._references[id] = ref(self)

    def __eq__(self, other):
        if isinstance(other, Session):
            return self.id == other.id
        return False

    def __hash__(self) -> int:
        return hash(self.id)

    def __del__(self):
        id = self.id
        references = self._references
        to_delete = self._to_delete
        mempool = self.mempool

        async def _delete():
            if id in to_delete:
                del references[id]
                to_delete.remove(id)
                await mempool.delete(id)

        return lambda _, _delete=_delete: asyncio.create_task(_delete())


class MemoryPoolService(MemoryPoolServicer):
    def __init__(self):
        self._mempool = MemoryPool()
        self._shutdown = asyncio.Event()

    @_raises_rpc_error(StatusCode.INTERNAL)
    async def session(self, request: SessionRequest, context: ServicerContext):
        yield (session := Session())
        while True:
            yield await session.queue.get()

    @_raises_rpc_error(StatusCode.INTERNAL)
    async def acquire(
        self, request: Request, context: ServicerContext
    ) -> Response:
        dump = b""
        if not (session := Session.get(request.session.id)):
            raise ValueError(f"Session {request.session.id} not found")
        await self._mempool.map(request.reference.id)
        session.references.add(
            Reference(
                id=request.reference.id,
                mempool=self._mempool,
            )
        )
        return Response(dump=dump)

    @_raises_rpc_error(StatusCode.INTERNAL)
    async def put(self, request: Put, context: ServicerContext) -> Response:
        dump = b""
        dump = pickle.dumps(
            await self._mempool.put(request.dump, mutable=request.mutable)
        )
        return Response(dump=dump)

    @_raises_rpc_error(StatusCode.INTERNAL)
    async def post(self, request: Post, context: ServicerContext) -> Response:
        dump = b""
        dump = pickle.dumps(
            await self._mempool.post(request.reference.id, request.dump)
        )
        for session in Reference(
            id=request.reference.id, mempool=self._mempool
        ).sessions:
            if session.id is not request.session.id:
                await session.queue.put(
                    Event(
                        reference=request.reference,
                        event_type="post",
                    )
                )
        return Response(dump=dump)

    @_raises_rpc_error(StatusCode.INTERNAL)
    async def get(
        self, request: Request, context: ServicerContext
    ) -> Response:
        dump = b""
        dump = await self._mempool.get(request.reference.id)
        return Response(dump=dump)

    @_raises_rpc_error(StatusCode.INTERNAL)
    async def release(
        self, request: Request, context: ServicerContext
    ) -> Response:
        dump = pickle.dumps(request.reference.id)
        if not (session := Session.get(request.session.id)):
            raise ValueError(f"Session {request.session.id} not found")
        session.references.remove(
            Reference(
                id=request.reference.id,
                mempool=self._mempool,
            )
        )
        return Response(dump=dump)
