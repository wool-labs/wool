from __future__ import annotations

import asyncio
import hashlib
import mmap
import os
import pathlib
import shutil
from contextlib import asynccontextmanager
from typing import BinaryIO

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

import shortuuid

from wool._mempool._metadata import MetadataMessage


class SharedObject:
    _id: str
    _mempool: MemoryPool
    _file: BinaryIO
    _mmap: mmap.mmap
    _size: int
    _md5: bytes

    def __init__(self, id: str, *, mempool: MemoryPool):
        self._id = id
        self._mempool = mempool
        self._file = open(self._path / "dump", "r+b")
        self._mmap = mmap.mmap(self._file.fileno(), 0)
        self._size = self.metadata.size
        self._md5 = self.metadata.md5

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    @property
    def id(self) -> str:
        return self._id

    @property
    def metadata(self) -> SharedObjectMetadata:
        return SharedObjectMetadata(self.id, mempool=self._mempool)

    @property
    def mmap(self) -> mmap.mmap:
        return self._mmap

    @property
    def _path(self) -> pathlib.Path:
        return pathlib.Path(self._mempool.path, self.id)

    def close(self):
        self.metadata.close()
        self._mmap.close()
        self._file.close()

    def refresh(self) -> Self:
        if self._size != self.metadata.size or self._md5 != self.metadata.md5:
            self._mmap.close()
            self._file.close()
            self._file = open(self._path / "dump", "r+b")
            self._mmap = mmap.mmap(self._file.fileno(), 0)
            self._size = self.metadata.size
            self._md5 = self.metadata.md5
        return self


class SharedObjectMetadata:
    _id: str
    _mempool: MemoryPool
    _file: BinaryIO
    _mmap: mmap.mmap
    _instances: dict[str, SharedObjectMetadata] = {}

    def __new__(cls, id: str, *, mempool: MemoryPool):
        if id in cls._instances:
            return cls._instances[id]
        return super().__new__(cls)

    def __init__(self, id: str, mempool: MemoryPool):
        self._id = id
        self._mempool = mempool
        self._file = open(self._path / "meta", "r+b")
        self._mmap = mmap.mmap(self._file.fileno(), 0)
        self._instances[id] = self

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    @property
    def id(self) -> str:
        return self._id

    @property
    def mutable(self) -> bool:
        return self._metadata.mutable

    @property
    def size(self) -> int:
        return self._metadata.size

    @property
    def md5(self) -> bytes:
        return self._metadata.md5

    @property
    def mmap(self) -> mmap.mmap:
        return self._mmap

    @property
    def _path(self) -> pathlib.Path:
        return pathlib.Path(self._mempool.path, self.id)

    @property
    def _metadata(self) -> MetadataMessage:
        return MetadataMessage.loads(bytes(self._mmap))

    def close(self):
        self._mmap.close()
        self._file.close()
        del self._instances[self.id]


class MemoryPool:
    _objects: dict[str, SharedObject]
    _path: pathlib.Path

    def __init__(self, path: str | pathlib.Path = pathlib.Path(".mempool")):
        if isinstance(path, str):
            self._path = pathlib.Path(path)
        else:
            self._path = path
        self._lockdir = self._path / "locks"
        os.makedirs(self._lockdir, exist_ok=True)
        self._acquire(f"pid-{os.getpid()}")
        self._objects = dict()

    def __contains__(self, ref: str) -> bool:
        return ref in self._objects

    def __del__(self):
        self._release(f"pid-{os.getpid()}")

    @property
    def path(self) -> pathlib.Path:
        return self._path

    async def map(self, ref: str | None = None):
        if ref is not None and ref not in self._objects:
            async with self._delete_lock(ref):
                pass
            async with self._reference_lock(ref):
                self._map(ref)
        else:
            for entry in os.scandir(self._path):
                if entry.is_dir() and (ref := entry.name) != "locks":
                    async with self._delete_lock(ref):
                        pass
                    async with self._reference_lock(ref):
                        self._map(ref)

    async def put(
        self, dump: bytes, *, mutable: bool = False, ref: str | None = None
    ) -> str:
        async with self._delete_lock(ref := ref or str(shortuuid.uuid())):
            pass
        async with self._reference_lock(ref):
            self._put(ref, dump, mutable=mutable, exist_ok=False)
            return ref

    async def post(self, ref: str, dump: bytes) -> bool:
        async with self._delete_lock(ref):
            pass
        async with self._reference_lock(ref):
            if ref not in self._objects:
                self._map(ref)
            obj = self._objects[ref]
            if not obj.metadata.mutable:
                raise ValueError("Cannot modify an immutable reference")
            if (size := len(dump)) != obj.metadata.size:
                try:
                    obj.mmap.resize(size)
                    self._post(ref, obj, dump)
                except SystemError:
                    self._put(ref, dump, mutable=True, exist_ok=True)
                return True
            elif hashlib.md5(dump).digest() != obj.metadata.md5:
                self._post(ref, obj, dump)
                return True
            else:
                return False

    async def get(self, ref: str) -> bytes:
        async with self._delete_lock(ref):
            pass
        async with self._reference_lock(ref):
            if ref not in self._objects:
                self._map(ref)
            return bytes(self._objects[ref].refresh().mmap)

    async def delete(self, ref: str):
        async with self._delete_lock(ref):
            async with self._reference_lock(ref):
                if ref not in self._objects:
                    self._map(ref)
                self._objects.pop(ref).close()
                try:
                    shutil.rmtree(self.path / ref)
                except FileNotFoundError:
                    pass

    def _put(
        self,
        ref: str,
        dump: bytes,
        *,
        mutable: bool = False,
        exist_ok: bool = False,
    ):
        metadata = MetadataMessage(
            ref=ref,
            mutable=mutable,
            size=len(dump),
            md5=hashlib.md5(dump).digest(),
        )

        refpath = pathlib.Path(self._path, f"{ref}")
        os.makedirs(refpath, exist_ok=exist_ok)

        with open(refpath / "meta", "wb") as metafile:
            metafile.write(metadata.dumps())

        with open(refpath / "dump", "wb") as dumpfile:
            dumpfile.write(dump)

        self._map(ref)

    def _post(self, ref: str, obj: SharedObject, dump: bytes):
        if not obj.metadata.mutable:
            raise ValueError("Cannot modify an immutable reference")
        metadata = MetadataMessage(
            ref=ref,
            mutable=True,
            size=len(dump),
            md5=hashlib.md5(dump).digest(),
        )
        obj.metadata.mmap[:] = metadata.dumps()
        obj.metadata.mmap.flush()
        obj.mmap.seek(0)
        obj.mmap.write(dump)

    def _map(self, ref: str):
        obj = self._objects.pop(ref, None)
        if obj:
            obj.mmap.close()
        self._objects[ref] = SharedObject(id=ref, mempool=self)

    def _lockpath(self, key: str) -> pathlib.Path:
        return pathlib.Path(self._lockdir, f"{key}.lock")

    def _acquire(self, key: str) -> bool:
        try:
            os.symlink(f"{key}", self._lockpath(key))
            return True
        except FileExistsError:
            return False

    def _release(self, key: str):
        try:
            if os.path.islink(lock_path := self._lockpath(key)):
                os.unlink(lock_path)
        except FileNotFoundError:
            pass

    @asynccontextmanager
    async def _reference_lock(self, ref: str):
        try:
            while not self._acquire(ref):
                await asyncio.sleep(0)
            yield
        finally:
            self._release(ref)

    @asynccontextmanager
    async def _delete_lock(self, ref: str):
        key = f"delete-{ref}"
        if not self._acquire(f"delete-{ref}"):
            raise RuntimeError(
                f"Reference {ref} is currently locked for deletion"
            )
        else:
            yield
        self._release(key)
