from __future__ import annotations

import logging
from dataclasses import dataclass

try:
    from wool._protobuf.mempool.metadata.metadata_pb2 import _MetadataMessage
except ImportError:
    logging.error(
        "Failed to import _MetadataMessage. "
        "Ensure protocol buffers are compiled."
    )
    raise


@dataclass
class MetadataMessage:
    ref: str
    mutable: bool
    size: int
    md5: bytes

    @classmethod
    def loads(cls, data: bytes) -> MetadataMessage:
        (metadata := _MetadataMessage()).ParseFromString(data)
        return cls(
            ref=metadata.ref,
            mutable=metadata.mutable,
            size=metadata.size,
            md5=metadata.md5,
        )

    def dumps(self) -> bytes:
        return _MetadataMessage(
            ref=self.ref, mutable=self.mutable, size=self.size, md5=self.md5
        ).SerializeToString()


__all__ = ["MetadataMessage"]
