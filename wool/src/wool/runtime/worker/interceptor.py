from __future__ import annotations

import grpc
import grpc.aio

from wool import protocol
from wool.runtime.worker.proxy import is_version_compatible
from wool.runtime.worker.proxy import parse_version


class VersionInterceptor(grpc.aio.ServerInterceptor):
    """gRPC server interceptor for wire protocol version checking.

    Intercepts the ``dispatch`` RPC to extract the client version from
    field 1 of the raw request bytes using
    :class:`~wool.protocol.TaskEnvelope`.  A client is accepted
    when its protocol version is less than or equal to the local
    worker version within the same major version.  Requests with
    empty, missing, or unparseable version fields are rejected.
    """

    async def intercept_service(self, continuation, handler_call_details):
        handler = await continuation(handler_call_details)
        if handler is None or not handler_call_details.method.endswith("/dispatch"):
            return handler

        original_handler = handler.stream_stream
        original_deserializer = handler.request_deserializer
        assert original_handler is not None
        assert original_deserializer is not None

        async def version_checked_handler(request_iterator, context):
            # Read the first raw request to extract the version envelope
            first_bytes = await anext(aiter(request_iterator))

            # The first Request message wraps a Task; parse the Task
            # envelope from the nested task bytes.
            request_msg = protocol.Request()
            request_msg.ParseFromString(first_bytes)
            task_bytes = request_msg.task.SerializeToString()

            envelope = protocol.TaskEnvelope()
            try:
                envelope.ParseFromString(task_bytes)
            except Exception:
                yield protocol.Response(
                    nack=protocol.Nack(reason="Failed to parse version envelope")
                )
                return

            client_version = parse_version(envelope.version)
            local_version = parse_version(protocol.__version__)

            if client_version is None or local_version is None:
                yield protocol.Response(
                    nack=protocol.Nack(
                        reason=(
                            f"Unparseable version: "
                            f"client={envelope.version!r}, "
                            f"worker={protocol.__version__!r}"
                        )
                    )
                )
                return

            if not is_version_compatible(client_version, local_version):
                yield protocol.Response(
                    nack=protocol.Nack(
                        reason=(
                            f"Incompatible version: "
                            f"client={envelope.version}, "
                            f"worker={protocol.__version__}"
                        )
                    )
                )
                return

            # Re-assemble a chained iterator: yield the deserialized
            # first request, then the rest of the stream.
            first_request = original_deserializer(first_bytes)

            async def chained_iterator():
                yield first_request
                async for raw in request_iterator:
                    yield original_deserializer(raw)

            async for response in original_handler(chained_iterator(), context):  # pyright: ignore[reportArgumentType, reportGeneralTypeIssues]
                yield response

        return grpc.stream_stream_rpc_method_handler(
            version_checked_handler,
            request_deserializer=None,
            response_serializer=handler.response_serializer,
        )
