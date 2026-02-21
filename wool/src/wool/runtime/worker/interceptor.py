from __future__ import annotations

import grpc
import grpc.aio

import wool
from wool.runtime import protobuf as pb
from wool.runtime.worker.proxy import _parse_major_version


class VersionInterceptor(grpc.aio.ServerInterceptor):
    """gRPC server interceptor for wire protocol version checking.

    Intercepts the ``dispatch`` RPC to extract the client version from
    field 1 of the raw request bytes using
    :class:`~wool.runtime.protobuf.task.TaskVersionEnvelope`.  If the
    client major version differs from the local worker major version,
    the RPC is short-circuited with a
    :class:`~wool.runtime.protobuf.worker.Nack` response.

    Empty or missing version fields are treated as compatible
    (backwards compatibility with pre-versioned clients).
    """

    async def intercept_service(self, continuation, handler_call_details):
        handler = await continuation(handler_call_details)
        if handler is None or not handler_call_details.method.endswith("/dispatch"):
            return handler

        original_handler = handler.unary_stream
        original_deserializer = handler.request_deserializer

        async def version_checked_handler(request_bytes, context):
            envelope = pb.task.TaskVersionEnvelope()
            try:
                envelope.ParseFromString(request_bytes)
            except Exception:
                pass  # Parsing failure -> proceed without version check

            if envelope.version:
                client_major = _parse_major_version(envelope.version)
                local_major = _parse_major_version(wool.__version__)
                if (
                    client_major is not None
                    and local_major is not None
                    and client_major != local_major
                ):
                    yield pb.worker.Response(
                        nack=pb.worker.Nack(
                            reason=(
                                f"Major version mismatch: "
                                f"client={envelope.version}, "
                                f"worker={wool.__version__}"
                            )
                        )
                    )
                    return

            request = original_deserializer(request_bytes)
            async for response in original_handler(request, context):
                yield response

        return grpc.unary_stream_rpc_method_handler(
            version_checked_handler,
            request_deserializer=None,
            response_serializer=handler.response_serializer,
        )
