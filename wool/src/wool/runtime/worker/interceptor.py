from __future__ import annotations

import grpc
import grpc.aio

from wool import protocol
from wool.runtime.worker.proxy import is_version_compatible
from wool.runtime.worker.proxy import parse_version


class VersionInterceptor(grpc.aio.ServerInterceptor):
    """Server-side guard for wire protocol version compatibility.

    Inspects the first request on every ``dispatch`` stream and
    accepts the client when its protocol version is less than or
    equal to the worker's within the same major. Empty, missing,
    unparseable, and incompatible versions are rejected with a
    gRPC ``FAILED_PRECONDITION`` abort before the dispatch handler
    runs. Callers observe the rejection as a non-transient
    :class:`RpcError` on their first read from the stream — the
    load balancer treats it as worker-health-affecting and evicts
    the peer.

    Version handshake is transport-level. Parse-phase application
    failures (unpicklable task callable, malformed task id) ride
    the in-band Nack channel instead; the two paths are
    deliberately distinct so callers can distinguish "this peer
    speaks the wrong protocol" from "this task is malformed".
    """

    async def intercept_service(self, continuation, handler_call_details):
        handler = await continuation(handler_call_details)
        if handler is None or not handler_call_details.method.endswith("/dispatch"):
            return handler

        original_handler = handler.stream_stream
        original_deserializer = handler.request_deserializer
        assert original_handler is not None
        assert original_deserializer is not None

        async def version_checked_handler(
            request_iterator,
            context: grpc.aio.ServicerContext,
        ):
            # Read the first raw request to extract the version envelope
            first_bytes = await anext(aiter(request_iterator))

            # The first Request message wraps a Task; parse the Task
            # envelope from the nested task bytes. Wrap the outer
            # ``Request.ParseFromString`` together with the envelope
            # parse so malformed wire bytes (truncated, garbage, or
            # mis-shaped by a buggy client) abort with the deliberate
            # ``FAILED_PRECONDITION`` rather than leaking out as
            # ``UNKNOWN`` from an unhandled ``protobuf.DecodeError``.
            envelope = protocol.TaskEnvelope()
            try:
                request_msg = protocol.Request()
                request_msg.ParseFromString(first_bytes)
                task_bytes = request_msg.task.SerializeToString()
                envelope.ParseFromString(task_bytes)
            except Exception:
                await context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "Failed to parse version envelope",
                )

            client_version = parse_version(envelope.version)
            local_version = parse_version(protocol.__version__)

            if client_version is None or local_version is None:
                await context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    (
                        f"Unparseable version: "
                        f"client={envelope.version!r}, "
                        f"worker={protocol.__version__!r}"
                    ),
                )

            if not is_version_compatible(client_version, local_version):
                await context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    (
                        f"Incompatible version: "
                        f"client={envelope.version}, "
                        f"worker={protocol.__version__}"
                    ),
                )

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
