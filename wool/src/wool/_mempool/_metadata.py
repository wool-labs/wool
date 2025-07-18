from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass

try:
    from wool._protobuf.mempool import metadata_pb2 as pb
except ImportError as e:
    from wool._protobuf import ProtobufImportError

    raise ProtobufImportError(e) from e


@dataclass
class MetadataMessage:
    ref: str
    mutable: bool
    size: int
    md5: bytes

    @classmethod
    def loads(cls, data: bytes) -> MetadataMessage:
        (metadata := pb.MetadataMessage()).ParseFromString(data)
        return cls(
            ref=metadata.ref,
            mutable=metadata.mutable,
            size=metadata.size,
            md5=metadata.md5,
        )

    def dumps(self) -> bytes:
        return pb.MetadataMessage(**asdict(self)).SerializeToString()


__all__ = ["MetadataMessage"]
