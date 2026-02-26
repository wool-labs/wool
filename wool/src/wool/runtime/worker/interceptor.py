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
    :class:`~wool.protocol.task.TaskEnvelope`.  A client is accepted
    when its protocol version is less than or equal to the local
    worker version within the same major version.  Requests with
    empty, missing, or unparseable version fields are rejected.
    """

    async def intercept_service(self, continuation, handler_call_details):
        handler = await continuation(handler_call_details)
        if handler is None or not handler_call_details.method.endswith("/dispatch"):
            return handler

        original_handler = handler.unary_stream
        original_deserializer = handler.request_deserializer
        assert original_handler is not None
        assert original_deserializer is not None

        async def version_checked_handler(request_bytes, context):
            envelope = protocol.task.TaskEnvelope()
            try:
                envelope.ParseFromString(request_bytes)
            except Exception:
                yield protocol.worker.Response(
                    nack=protocol.task.Nack(reason="Failed to parse version envelope")
                )
                return

            client_version = parse_version(envelope.version)
            local_version = parse_version(protocol.__version__)

            if client_version is None or local_version is None:
                yield protocol.worker.Response(
                    nack=protocol.task.Nack(
                        reason=(
                            f"Unparseable version: "
                            f"client={envelope.version!r}, "
                            f"worker={protocol.__version__!r}"
                        )
                    )
                )
                return

            if not is_version_compatible(client_version, local_version):
                yield protocol.worker.Response(
                    nack=protocol.task.Nack(
                        reason=(
                            f"Incompatible version: "
                            f"client={envelope.version}, "
                            f"worker={protocol.__version__}"
                        )
                    )
                )
                return

            request = original_deserializer(request_bytes)
            async for response in original_handler(request, context):  # pyright: ignore[reportGeneralTypeIssues]
                yield response

        return grpc.unary_stream_rpc_method_handler(
            version_checked_handler,
            request_deserializer=None,
            response_serializer=handler.response_serializer,
        )
