from __future__ import annotations

import logging
from dataclasses import dataclass

try:
    from wool._mempool._metadata._metadata_pb2 import _Metadata
except ImportError:
    logging.error(
        "Failed to import _Metadata. Ensure protocol buffers are compiled."
    )
    raise


@dataclass
class Metadata:
    ref: str
    mutable: bool
    size: int
    md5: bytes

    @classmethod
    def loads(cls, data: bytes) -> Metadata:
        (metadata := _Metadata()).ParseFromString(data)
        return cls(
            ref=metadata.ref,
            mutable=metadata.mutable,
            size=metadata.size,
            md5=metadata.md5,
        )

    def dumps(self) -> bytes:
        return _Metadata(
            ref=self.ref, mutable=self.mutable, size=self.size, md5=self.md5
        ).SerializeToString()


__all__ = ["Metadata"]
