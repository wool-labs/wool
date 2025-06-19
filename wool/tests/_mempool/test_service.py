import asyncio
from unittest.mock import MagicMock
from weakref import WeakSet

import pytest
from grpc import RpcError
from grpc import StatusCode

from wool._mempool import MemoryPool
from wool._mempool._service import MemoryPoolService
from wool._mempool._service import Reference
from wool._mempool._service import Session
from wool._protobuf._mempool import _mempool_pb2 as protobuf
from wool._protobuf._mempool._mempool_pb2_grpc import MemoryPoolStub


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


class TestReference:
    def test_new(self):
        mempool = MagicMock(spec=MemoryPool)
        reference = Reference.new("ref1", mempool=mempool)
        assert reference.id == "ref1"
        assert reference.mempool == mempool

    def test___new__(self):
        mempool = MagicMock(spec=MemoryPool)
        this = Reference("ref1", mempool=mempool)
        that = Reference("ref1", mempool=mempool)
        assert this is that

    def test___init__(self):
        mempool = MagicMock(spec=MemoryPool)
        reference = Reference("ref1", mempool=mempool)
        assert reference.id == "ref1"
        assert reference.mempool == mempool
        assert isinstance(reference.sessions, WeakSet)

    def test___eq__(self):
        mempool = MagicMock(spec=MemoryPool)
        this = Reference("ref1", mempool=mempool)
        that = Reference("ref2", mempool=mempool)
        assert this == this
        assert this != that

    def test___hash__(self):
        mempool = MagicMock(spec=MemoryPool)
        reference = Reference("ref1", mempool=mempool)
        assert hash(reference) == hash(reference.id)

    def test___del__(self):
        mempool = MagicMock(spec=MemoryPool)
        reference = Reference("ref1", mempool=mempool)
        reference_id = reference.id
        del reference
        assert reference_id not in Reference._references


class TestMemoryPoolService:
    @pytest.fixture(autouse=True)
    def _clear_service_state_around_tests(self):
        Session._sessions.clear()
        Reference._references.clear()
        yield
        Session._sessions.clear()
        Reference._references.clear()

    def test___init__(self, grpc_servicer: MemoryPoolService):
        assert isinstance(grpc_servicer._mempool, MemoryPool)
        assert isinstance(grpc_servicer._shutdown, asyncio.Event)

    @pytest.mark.asyncio
    async def test_session(self, grpc_stub: MemoryPoolStub):
        call = grpc_stub.session(protobuf.SessionRequest())
        response_session_id = await call.read()

        assert response_session_id is not None
        assert response_session_id.id

        # Verify session was created in the service's context
        created_session_object = Session.get(response_session_id.id)
        assert created_session_object is not None
        assert created_session_object.id == response_session_id.id

        # Cancel the call to allow the service-side method to terminate
        call.cancel()
        try:
            await call.read()
        except RpcError as e:
            assert False

        # Ensure session cleanup
        await asyncio.sleep(0.01)
        assert Session.get(response_session_id.id) is None

    @pytest.mark.asyncio
    async def test_acquire(self, grpc_stub: MemoryPoolStub):
        reference_id = "test_reference"
        session = Session()

        request = AcquireRequest(
            session=SessionId(id=session.id),
            reference=ReferenceId(id=reference_id),
        )
        response = await grpc_stub.acquire(request)
        assert response.dump == b""

        session = Session.get(session.id)
        assert session
        assert reference_id in {ref.id for ref in session.references}

    @pytest.mark.asyncio
    async def test_put(self, grpc_stub: MemoryPoolStub):
        request = PutRequest(dump=b"test_data", mutable=False)
        response = await grpc_stub.put(request)
        assert response.id

    @pytest.mark.asyncio
    async def test_post(self, grpc_stub: MemoryPoolStub):
        reference_id = "test_reference"
        Session("test_session")
        request = PostRequest(
            reference=ReferenceId(id=reference_id),
            object=Object(dump=b"test_data"),
        )
        response = await grpc_stub.post(request)
        assert response.dump == b"test_data"

    @pytest.mark.asyncio
    async def test_get(
        self, grpc_stub: MemoryPoolStub, grpc_servicer: MemoryPoolService
    ):
        reference_id = "test_reference"
        grpc_servicer._mempool.put(reference_id, b"test_data")

        request = GetRequest(reference=ReferenceId(id=reference_id))
        response = await grpc_stub.get(request)
        assert response.dump == b"test_data"

    @pytest.mark.asyncio
    async def test_release(self, grpc_stub: MemoryPoolStub):
        session_id = "test_session"
        reference_id = "test_reference"
        session = Session(session_id)
        reference = Reference(reference_id, mempool=MemoryPool())
        session.references.add(reference)

        request = ReleaseRequest(
            session=SessionId(id=session_id),
            reference=ReferenceId(id=reference_id),
        )
        response = await grpc_stub.release(request)
        assert isinstance(response, Empty)

        # Verify release
        assert reference not in session.references
