import asyncio
from contextlib import contextmanager
from time import perf_counter
from unittest.mock import MagicMock
from weakref import WeakSet

import pytest

from wool._mempool import MemoryPool
from wool._mempool._service import _Reference
from wool._mempool._service import _Session

try:
    from wool._protobuf.mempool import service_pb2 as pb
except ImportError as e:
    from wool._protobuf import ProtobufImportError

    raise ProtobufImportError(e) from e


@contextmanager
def timer():
    start = perf_counter()
    yield lambda: perf_counter() - start


@pytest.fixture(scope="function")
def session():
    session = _Session()
    yield session
    del session


class TestSession:
    def test___init__(self):
        session = _Session()
        assert session.id
        assert isinstance(session.queue, asyncio.Queue)
        assert isinstance(session.references, set)

    def test___del__(self):
        session = _Session()
        session_id = session.id
        del session
        assert _Session.get(session_id) is None

    def test___eq__(self):
        session1 = _Session()
        session2 = _Session()
        assert session1 == session1
        assert session1 != session2

    def test___hash__(self):
        session = _Session()
        assert hash(session) == hash(session.id)

    def test_get(self):
        session = _Session()
        assert _Session.get(session.id) == session


@pytest.mark.asyncio
class TestReference:
    @pytest.fixture(autouse=True)
    def _clear_service_state_around_tests(self):
        _Reference._references.clear()
        yield
        _Reference._references.clear()

    async def test_new(self):
        mempool = MagicMock(spec=MemoryPool)
        reference = _Reference.new("ref1", mempool=mempool)
        assert reference.id == "ref1"
        assert reference._mempool == mempool

    async def test___new__(self):
        mempool = MagicMock(spec=MemoryPool)
        this = _Reference("ref1", mempool=mempool)
        that = _Reference("ref1", mempool=mempool)
        assert this is that

    async def test___init__(self):
        mempool = MagicMock(spec=MemoryPool)
        reference = _Reference("ref1", mempool=mempool)
        assert reference.id == "ref1"
        assert reference._mempool == mempool
        assert isinstance(reference.sessions, WeakSet)

    async def test___eq__(self):
        mempool = MagicMock(spec=MemoryPool)
        this = _Reference("ref1", mempool=mempool)
        that = _Reference("ref2", mempool=mempool)
        assert this == this
        assert this != that

    async def test___hash__(self):
        mempool = MagicMock(spec=MemoryPool)
        reference = _Reference("ref1", mempool=mempool)
        assert hash(reference) == hash(reference.id)

    async def test___del__(self):
        mempool = MagicMock(spec=MemoryPool)
        reference = _Reference("ref1", mempool=mempool)
        reference_id = reference.id
        del reference
        await asyncio.gather(
            *(
                t
                for t in asyncio.all_tasks()
                if t is not asyncio.current_task()
            )
        )
        assert reference_id not in _Reference._references


@pytest.mark.asyncio
class TestMemoryPoolService:
    @pytest.fixture(autouse=True, scope="function")
    def _clear_service_state_around_tests(self):
        _Session._sessions.clear()
        _Reference._references.clear()
        yield
        _Session._sessions.clear()
        _Reference._references.clear()

    async def test_session(self, grpc_aio_stub):
        async with grpc_aio_stub() as stub:
            call = stub.session(pb.SessionRequest())
            response: pb.SessionResponse = await call.read()
            assert response.session.id in _Session._sessions
            call.cancel()
            with pytest.raises(asyncio.CancelledError):
                await call.read()
            with timer() as elapsed:
                while elapsed() < 1:
                    await asyncio.sleep(0)
                    if response.session.id not in _Session._sessions:
                        break
                else:
                    assert False, "Session was not deleted in time"

    async def test_acquire(
        self, grpc_aio_stub, session: _Session, seed: MemoryPool
    ):
        async with grpc_aio_stub() as stub:
            reference = _Reference.new("meliora", mempool=seed)

            request = pb.AcquireRequest(
                session=pb.Session(id=session.id),
                reference=pb.Reference(id=reference.id),
            )
            await stub.acquire(request)
            assert session.id in _Session._sessions
            assert reference in session.references

    async def test_put(
        self, grpc_aio_stub, session: _Session, seed: MemoryPool
    ):
        async with grpc_aio_stub() as stub:
            request = pb.PutRequest(
                session=pb.Session(id=session.id),
                dump=b"test_data",
                mutable=False,
            )
            response: pb.PutResponse = await stub.put(request)
            await seed.map(response.reference.id)
            assert response.reference.id in seed

    async def test_post(
        self, grpc_aio_stub, session: _Session, seed: MemoryPool
    ):
        async with grpc_aio_stub() as stub:
            reference = _Reference.new("meliora", mempool=seed)
            request = pb.PostRequest(
                session=pb.Session(id=session.id),
                reference=pb.Reference(id=reference.id),
                dump=b"updated_data",
            )
            response: pb.PostResponse = await stub.post(request)
            assert response.updated is True

    async def test_get(
        self, grpc_aio_stub, session: _Session, seed: MemoryPool
    ):
        async with grpc_aio_stub() as stub:
            reference = _Reference.new("meliora", mempool=seed)
            request = pb.GetRequest(
                session=pb.Session(id=session.id),
                reference=pb.Reference(id=reference.id),
            )
            response: pb.GetResponse = await stub.get(request)
            assert response.dump == b"Ad meliora"

    async def test_release(
        self, grpc_aio_stub, session: _Session, seed: MemoryPool
    ):
        async with grpc_aio_stub() as stub:
            reference = _Reference.new("meliora", mempool=seed)
            session.references.add(reference)
            request = pb.ReleaseRequest(
                session=pb.Session(id=session.id),
                reference=pb.Reference(id=reference.id),
            )
            await stub.acquire(
                pb.AcquireRequest(
                    session=pb.Session(id=session.id),
                    reference=pb.Reference(id=reference.id),
                )
            )
            await stub.release(request)
            assert reference not in session.references
