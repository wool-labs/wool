import os
import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version
from typing import Protocol

try:
    __version__ = version("wool")
except PackageNotFoundError:
    __version__ = "unknown"

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

try:
    from wool.protocol.wire_pb2 import Ack
    from wool.protocol.wire_pb2 import ChannelOptions
    from wool.protocol.wire_pb2 import Context
    from wool.protocol.wire_pb2 import Message
    from wool.protocol.wire_pb2 import Nack
    from wool.protocol.wire_pb2 import Request
    from wool.protocol.wire_pb2 import Response
    from wool.protocol.wire_pb2 import RuntimeContext
    from wool.protocol.wire_pb2 import StopRequest
    from wool.protocol.wire_pb2 import Task
    from wool.protocol.wire_pb2 import TaskEnvelope
    from wool.protocol.wire_pb2 import Void
    from wool.protocol.wire_pb2 import WorkerMetadata
    from wool.protocol.wire_pb2_grpc import WorkerServicer
    from wool.protocol.wire_pb2_grpc import WorkerStub
    from wool.protocol.wire_pb2_grpc import add_WorkerServicer_to_server
except ImportError as e:
    from wool.protocol.exception import ProtobufImportError

    raise ProtobufImportError(e) from e


class AddServicerToServerProtocol(Protocol):
    @staticmethod
    def __call__(servicer, server) -> None: ...


add_to_server: dict[type[WorkerServicer], AddServicerToServerProtocol] = {
    WorkerServicer: add_WorkerServicer_to_server,
}

__all__ = [
    "Ack",
    "ChannelOptions",
    "Context",
    "Message",
    "Nack",
    "Request",
    "Response",
    "RuntimeContext",
    "StopRequest",
    "Task",
    "TaskEnvelope",
    "Void",
    "WorkerMetadata",
    "WorkerServicer",
    "WorkerStub",
    "add_WorkerServicer_to_server",
]
