from __future__ import annotations

import uuid
from dataclasses import dataclass
from dataclasses import field
from types import MappingProxyType

import grpc

from wool.protocol import ChannelOptions as ChannelOptionsProtobuf
from wool.protocol import WorkerMetadata as WorkerMetadataProtobuf
from wool.runtime.worker.base import ChannelOptions


# public
@dataclass(frozen=True)
class WorkerMetadata:
    """Properties and metadata for a worker instance.

    Contains identifying information and capabilities of a worker that
    can be used for discovery, filtering, and routing decisions.

    :param uid:
        Unique identifier for the worker instance (UUID).
    :param address:
        gRPC target address (e.g. ``"host:port"``,
        ``"unix:path"``).
    :param pid:
        Process ID of the worker.
    :param version:
        Version string of the worker software.
    :param tags:
        Frozenset of capability tags for worker filtering and
        selection.
    :param extra:
        Additional arbitrary metadata as immutable key-value pairs.
    :param secure:
        Whether the worker requires secure TLS/mTLS connections.
    :param options:
        Transport configuration advertised by the worker.  Clients
        use these settings when connecting.
    """

    uid: uuid.UUID
    address: str = field(hash=False)
    pid: int = field(hash=False)
    version: str = field(hash=False)
    tags: frozenset[str] = field(default_factory=frozenset, hash=False)
    extra: MappingProxyType = field(
        default_factory=lambda: MappingProxyType({}), hash=False
    )
    secure: bool = field(default=False, hash=False)
    options: ChannelOptions = field(default_factory=ChannelOptions, hash=False)

    @classmethod
    def from_protobuf(cls, protobuf: WorkerMetadataProtobuf) -> WorkerMetadata:
        """Create a WorkerMetadata instance from a protobuf message.

        :param protobuf:
            The protobuf WorkerMetadata message to deserialize.
        :returns:
            A new WorkerMetadata instance with data from the protobuf message.
        :raises ValueError:
            If the UID in the protobuf message is not a valid UUID.
        """

        return cls(
            uid=uuid.UUID(protobuf.uid),
            address=protobuf.address,
            pid=protobuf.pid,
            version=protobuf.version,
            tags=frozenset(protobuf.tags),
            extra=MappingProxyType(dict(protobuf.extra)),
            secure=protobuf.secure,
            options=cls._options_from_protobuf(protobuf),
        )

    def to_protobuf(self) -> WorkerMetadataProtobuf:
        """Convert this WorkerMetadata instance to a protobuf message.

        :returns:
            A protobuf WorkerMetadata message containing this instance's data.
        """

        msg = WorkerMetadataProtobuf(
            uid=str(self.uid),
            address=self.address,
            pid=self.pid,
            version=self.version,
            tags=list(self.tags),
            extra=dict(self.extra),
            secure=self.secure,
        )
        msg.connection.CopyFrom(
            ChannelOptionsProtobuf(
                max_receive_message_length=self.options.max_receive_message_length,
                max_send_message_length=self.options.max_send_message_length,
                keepalive_time_ms=self.options.keepalive_time_ms,
                keepalive_timeout_ms=self.options.keepalive_timeout_ms,
                keepalive_permit_without_calls=self.options.keepalive_permit_without_calls,
                max_pings_without_data=self.options.max_pings_without_data,
                max_concurrent_streams=self.options.max_concurrent_streams,
                compression=self.options.compression.value,
            )
        )
        return msg

    @classmethod
    def _options_from_protobuf(cls, protobuf: WorkerMetadataProtobuf) -> ChannelOptions:
        """Reconstruct ChannelOptions from the protobuf connection config.

        :param protobuf:
            The protobuf WorkerMetadata message.
        :returns:
            A ChannelOptions instance.  Returns default ChannelOptions
            when no connection config is present.
        """
        if not protobuf.HasField("connection"):
            return ChannelOptions()

        conn = protobuf.connection
        return ChannelOptions(
            max_receive_message_length=conn.max_receive_message_length,
            max_send_message_length=conn.max_send_message_length,
            keepalive_time_ms=conn.keepalive_time_ms,
            keepalive_timeout_ms=conn.keepalive_timeout_ms,
            keepalive_permit_without_calls=conn.keepalive_permit_without_calls,
            max_pings_without_data=conn.max_pings_without_data,
            max_concurrent_streams=conn.max_concurrent_streams,
            compression=grpc.Compression(conn.compression),
        )
