import os
import sys
from typing import Protocol

# Path hack — grpc-tools emits a sibling-flat ``import wire_pb2``
# inside the generated ``wire_pb2_grpc.py``, which only resolves with
# the package directory on ``sys.path``. Upstream wontfix: see
# protocolbuffers/protobuf#1491 and grpc/grpc#29459 (which endorses
# this exact workaround). Removal tracked in wool-labs/wool#236.
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

try:
    from wool.protocol.wire_pb2 import Ack
    from wool.protocol.wire_pb2 import ChainManifest
    from wool.protocol.wire_pb2 import ChannelOptions
    from wool.protocol.wire_pb2 import ContextVar
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
    from wool.protocol.exceptions import ProtobufImportError

    raise ProtobufImportError(e) from e


class AddServicerToServerProtocol(Protocol):
    """Callable signature shape for ``add_<X>ServicerToServer`` helpers.

    grpc-tools generates one such helper per service. The
    :data:`add_to_server` table maps the service class to its
    matching helper so the worker process can look up the correct
    add-to-server function at startup.
    """

    @staticmethod
    def __call__(servicer, server) -> None: ...


add_to_server: dict[type[WorkerServicer], AddServicerToServerProtocol] = {
    WorkerServicer: add_WorkerServicer_to_server,
}


__all__ = [
    "Ack",
    "AddServicerToServerProtocol",
    "ChainManifest",
    "ChannelOptions",
    "ContextVar",
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
    "add_to_server",
]
