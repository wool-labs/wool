import asyncio
from contextlib import contextmanager
from time import perf_counter
from unittest.mock import MagicMock
from weakref import WeakSet

import pytest
import pytest_asyncio

from wool._mempool import MemoryPool
from wool._mempool._service import Reference
from wool._mempool._service import Session
from wool._protobuf.mempool import mempool_pb2 as proto
from wool._protobuf.mempool.mempool_pb2_grpc import MemoryPoolStub


@contextmanager
def timer():
    """
    Context manager to measure the execution time of a code block.

    :return: A function to retrieve the elapsed time.
    """
    start = perf_counter()
    yield lambda: perf_counter() - start


@pytest_asyncio.fixture(scope="session")
async def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    await asyncio.gather(
        *(t for t in asyncio.all_tasks() if t is not asyncio.current_task())
    )


@pytest.fixture(scope="function")
def session():
    session = Session()
    yield session
    del session


class TestSession:
    def test___init__(self):
        session = Session()
        assert session.id
        assert isinstance(session.queue, asyncio.Queue)
        assert isinstance(session.references, set)

    def test___del__(self):
        session = Session()
        session_id = session.id
        del session
        assert Session.get(session_id) is None

    def test___eq__(self):
        session1 = Session()
        session2 = Session()
        assert session1 == session1
        assert session1 != session2

    def test___hash__(self):
        session = Session()
        assert hash(session) == hash(session.id)

    def test_get(self):
        session = Session()
        assert Session.get(session.id) == session


@pytest.mark.asyncio
class TestReference:
    @pytest.fixture(autouse=True)
    def _clear_service_state_around_tests(self):
        Reference._references.clear()
        yield
        Reference._references.clear()

    async def test_new(self):
        mempool = MagicMock(spec=MemoryPool)
        reference = Reference.new("ref1", mempool=mempool)
        assert reference.id == "ref1"
        assert reference.mempool == mempool

    async def test___new__(self):
        mempool = MagicMock(spec=MemoryPool)
        this = Reference("ref1", mempool=mempool)
        that = Reference("ref1", mempool=mempool)
        assert this is that

    async def test___init__(self):
        mempool = MagicMock(spec=MemoryPool)
        reference = Reference("ref1", mempool=mempool)
        assert reference.id == "ref1"
        assert reference.mempool == mempool
        assert isinstance(reference.sessions, WeakSet)

    async def test___eq__(self):
        mempool = MagicMock(spec=MemoryPool)
        this = Reference("ref1", mempool=mempool)
        that = Reference("ref2", mempool=mempool)
        assert this == this
        assert this != that

    async def test___hash__(self):
        mempool = MagicMock(spec=MemoryPool)
        reference = Reference("ref1", mempool=mempool)
        assert hash(reference) == hash(reference.id)

    async def test___del__(self):
        mempool = MagicMock(spec=MemoryPool)
        reference = Reference("ref1", mempool=mempool)
        reference_id = reference.id
        del reference
        await asyncio.gather(
            *(
                t
                for t in asyncio.all_tasks()
                if t is not asyncio.current_task()
            )
        )
        assert reference_id not in Reference._references


@pytest.mark.asyncio
class TestMemoryPoolService:
    @pytest.fixture(autouse=True, scope="function")
    def _clear_service_state_around_tests(self):
        Session.sessions.clear()
        Reference._references.clear()
        yield
        Session.sessions.clear()
        Reference._references.clear()

    async def test_session(self, grpc_aio_stub: MemoryPoolStub):
        call = grpc_aio_stub.session(proto.SessionRequest())
        try:
            response: proto.SessionResponse = await call.read()
        except Exception as e:
            raise e
        assert response.session.id in Session.sessions
        call.cancel()
        try:
            await call.read()
        except asyncio.CancelledError:
            pass
        else:
            assert False, "Expected CancelledError"
        with timer() as elapsed:
            while elapsed() < 1:
                await asyncio.sleep(0)
                if response.session.id not in Session.sessions:
                    break
            else:
                assert False, "Session was not deleted in time"

    async def test_acquire(
        self, grpc_aio_stub: MemoryPoolStub, session: Session, seed: MemoryPool
    ):
        reference = Reference.new("meliora", mempool=seed)

        request = proto.AcquireRequest(
            session=proto.Session(id=session.id),
            reference=proto.Reference(id=reference.id),
        )
        await grpc_aio_stub.acquire(request)
        assert session.id in Session.sessions
        assert reference in session.references

    async def test_put(
        self, grpc_aio_stub: MemoryPoolStub, session: Session, seed: MemoryPool
    ):
        request = proto.PutRequest(
            session=proto.Session(id=session.id),
            dump=b"test_data",
            mutable=False,
        )
        response: proto.PutResponse = await grpc_aio_stub.put(request)
        await seed.map(response.reference.id)
        assert response.reference.id in seed

    async def test_post(
        self, grpc_aio_stub: MemoryPoolStub, session: Session, seed: MemoryPool
    ):
        reference = Reference.new("meliora", mempool=seed)
        request = proto.PostRequest(
            session=proto.Session(id=session.id),
            reference=proto.Reference(id=reference.id),
            dump=b"updated_data",
        )
        response: proto.PostResponse = await grpc_aio_stub.post(request)
        assert response.updated is True

    async def test_get(
        self, grpc_aio_stub: MemoryPoolStub, session: Session, seed: MemoryPool
    ):
        reference = Reference.new("meliora", mempool=seed)
        request = proto.GetRequest(
            session=proto.Session(id=session.id),
            reference=proto.Reference(id=reference.id),
        )
        response: proto.GetResponse = await grpc_aio_stub.get(request)
        assert response.dump == b"Ad meliora"

    async def test_release(
        self, grpc_aio_stub: MemoryPoolStub, session: Session, seed: MemoryPool
    ):
        reference = Reference.new("meliora", mempool=seed)
        session.references.add(reference)

        request = proto.ReleaseRequest(
            session=proto.Session(id=session.id),
            reference=proto.Reference(id=reference.id),
        )
        await grpc_aio_stub.release(request)
        assert reference not in session.references
