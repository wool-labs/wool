import asyncio
import hashlib
import mmap
import os
import pathlib
from contextlib import asynccontextmanager

import shortuuid

from wool._mempool._metadata import Metadata


class MemoryPool:
    def __init__(self, path: str | pathlib.Path = pathlib.Path(".mempool")):
        if isinstance(path, str):
            self._path = pathlib.Path(path)
        else:
            self._path = path
        self._lockdir = self._path / "locks"
        os.makedirs(self._lockdir, exist_ok=True)
        self._files = {}
        self._mmaps = {}
        self._metadata: dict[str, Metadata] = {}

    @property
    def path(self):
        return self._path

    async def map(self):
        for entry in os.scandir(self._path):
            if entry.is_dir() and (ref := entry.name) != "locks":
                async with self._reflock(ref):
                    self._map(ref)

    async def put(self, dump, *, mutable=False):
        async with self._reflock(ref := str(shortuuid.uuid())):
            self._put(ref, dump, mutable=mutable, exist_ok=False)
            return ref

    async def post(self, ref, dump):
        async with self._reflock(ref):
            if ref not in self._mmaps:
                self._map(ref)
            metamap, dumpmap = self._mmaps[ref]
            metamap.seek(0)
            metadata = self._metadata.setdefault(
                ref, Metadata.loads(metamap.read())
            )
            if not metadata.mutable:
                raise ValueError("Cannot modify an immutable reference")
            if (size := len(dump)) != metadata.size:
                _, dumpfile = self._files[ref]
                try:
                    dumpmap.resize(size)
                    dumpmap.seek(0)
                    dumpmap.write(dump)
                    dumpmap.flush()
                except SystemError:
                    dumpmap.close()
                    dumpfile.close()
                    self._put(ref, dump, mutable=True, exist_ok=True)
                return True
            elif hashlib.md5(dump).digest() != metadata.md5:
                _, dumpmap = self._mmaps[ref]
                dumpmap.seek(0)
                dumpmap.write(dump)
                dumpmap.flush()
                return True
            else:
                return False

    async def get(self, ref):
        async with self._reflock(ref):
            if ref not in self._mmaps:
                self._map(ref)
            metamap, dumpmap = self._mmaps[ref]
            metamap.seek(0)
            metadata = Metadata.loads(metamap.read())
            cached_metadata = self._metadata.setdefault(ref, metadata)
            if (
                metadata.mutable
                and metadata.size != cached_metadata.size
            ):
                # Dump size has changed, so we need to re-map it
                self._map(ref)
            dumpmap.seek(0)
            return dumpmap.read()

    async def delete(self, ref):
        async with self._reflock(ref):
            metamap, dumpmap = self._mmaps.pop(ref, (None, None))
            metafile, dumpfile = self._files.pop(ref, (None, None))
            if metamap:
                metamap.close()
            if dumpmap:
                dumpmap.close()
            if metafile:
                metapath = metafile.name
                metafile.close()
                try:
                    os.remove(metapath)
                except FileNotFoundError:
                    pass
            if dumpfile:
                dumppath = dumpfile.name
                dumpfile.close()
                try:
                    os.remove(dumppath)
                except FileNotFoundError:
                    pass

    def _put(self, ref, dump, *, mutable=False, exist_ok=False):
        metadata = Metadata(
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
        self._metadata[ref] = metadata

    def _map(self, ref):
        refpath = pathlib.Path(self._path, f"{ref}")
        metafile = open(refpath / "meta", "r+b")
        metamap = mmap.mmap(metafile.fileno(), 0, flags=mmap.MAP_SHARED)
        dumpfile = open(refpath / "dump", "r+b")
        dumpmap = mmap.mmap(dumpfile.fileno(), 0, flags=mmap.MAP_SHARED)
        cached_metafile, cached_dumpfile = self._files.pop(ref, (None, None))
        if cached_metafile:
            cached_metafile.close()
        if cached_dumpfile:
            cached_dumpfile.close()
        cached_metamap, cached_dumpmap = self._mmaps.pop(ref, (None, None))
        if cached_metamap:
            cached_metamap.close()
        if cached_dumpmap:
            cached_dumpmap.close()
        self._files[ref] = (metafile, dumpfile)
        self._mmaps[ref] = (metamap, dumpmap)

    def _lockpath(self, ref):
        return pathlib.Path(self._lockdir, f"{ref}.lock")

    def _acquire(self, ref):
        try:
            os.symlink(f"{ref}", self._lockpath(ref))
            return True
        except FileExistsError:
            return False

    def _release(self, ref):
        try:
            if os.path.islink(lock_path := self._lockpath(ref)):
                os.unlink(lock_path)
        except FileNotFoundError:
            pass

    @asynccontextmanager
    async def _reflock(self, ref):
        try:
            while not self._acquire(ref):
                await asyncio.sleep(0)
            yield
        finally:
            self._release(ref)
