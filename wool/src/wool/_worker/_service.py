import asyncio
import pickle
from typing import AsyncIterator

from _protobuf.worker import service_pb2 as proto
from _protobuf.worker import service_pb2_grpc as rpc
from grpc.aio import ServicerContext

from wool._mempool._session import MemoryPoolSession


def ack() -> proto.ClientMessage:
    return proto.ClientMessage(response=proto.Response(ack=True))


def nack() -> proto.ClientMessage:
    return proto.ClientMessage(response=proto.Response(ack=False))


class WorkerService(rpc.WorkerServicer):
    def __init__(self, mempool: MemoryPoolSession):
        self._mempool = mempool

    async def put(
        self,
        request_iterator: AsyncIterator[proto.ClientMessage],
        context: ServicerContext,
    ) -> AsyncIterator[proto.ServiceMessage]:
        # Deserialize the task and yield an acknowledgment
        message = await anext(request_iterator)
        task = pickle.loads(await self._mempool.get(message.task.id))
        yield self._ack

        # Await confirmation from the client
        message = await anext(request_iterator)
        if not message.response.ack:
            return

        # Create a future reference in the mempool and yield it to the client
        future_ref = await self._mempool.put(
            pickle.dumps(asyncio.Future()), mutable=True
        )
        yield proto.ServiceMessage(future=proto.Future(id=future_ref))

        # Await confirmation from the client
        message = await anext(request_iterator)
        if not message.response.ack:
            return

        # Schedule the task and exit
        self._schedule_task(task, future_ref)

    def _schedule_task(self, task, future_ref): ...

    @property
    def _ack(self):
        return proto.ServiceMessage(response=proto.Response(ack=True))

    @property
    def _nack(self):
        return proto.ServiceMessage(response=proto.Response(ack=False))
