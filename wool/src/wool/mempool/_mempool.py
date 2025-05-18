import asyncio
import hashlib
import mmap
import os
import pathlib
import pickle
import uuid
from contextlib import asynccontextmanager

lockdir = pathlib.Path(".mempool", "locks")
os.makedirs(lockdir, exist_ok=True)


def _lockpath(ref):
    return pathlib.Path(lockdir, f"{ref}.lock")


def _acquire(ref):
    try:
        os.symlink(f"{ref}", _lockpath(ref))
        return True
    except FileExistsError:
        return False


def _release(ref):
    try:
        if os.path.islink(lock_path := _lockpath(ref)):
            os.unlink(lock_path)
    except FileNotFoundError:
        pass


@asynccontextmanager
async def _memlock(ref):
    try:
        while not _acquire(ref):
            await asyncio.sleep(0)
        yield
    finally:
        _release(ref)


class MemoryPool:
    def __init__(self):
        self._files = {}
        self._mmaps = {}
        self._metadata = {}

    async def put(self, dump, *, mutable=False):
        async with _memlock(ref := str(uuid.uuid4())):
            self._put(ref, dump, mutable=mutable)
            return ref

    async def get(self, ref):
        async with _memlock(ref):
            if ref not in self._mmaps:
                self._map(ref)
            metamap, dumpmap = self._mmaps[ref]
            metamap.seek(0)
            metadata = pickle.loads(metamap.read())
            cached_metadata = self._metadata.setdefault(ref, metadata)
            if (
                metadata["mutable"]
                and metadata["size"] != cached_metadata["size"]
            ):
                # Dump size has changed, so we need to re-map it
                self._map(ref)
            dumpmap.seek(0)
            return pickle.loads(dumpmap.read())

    async def post(self, ref, dump):
        async with _memlock(ref):
            if ref not in self._mmaps:
                self._map(ref)
            metamap, dumpmap = self._mmaps[ref]
            metamap.seek(0)
            metadata = self._metadata.setdefault(
                ref, pickle.loads(metamap.read())
            )
            if not metadata["mutable"]:
                raise ValueError("Cannot modify an immutable reference")
            if (size := len(dump)) != metadata["size"]:
                _, dumpfile = self._files[ref]
                try:
                    dumpmap.resize(size)
                    dumpmap.seek(0)
                    dumpmap.write(dump)
                    dumpmap.flush()
                except SystemError:
                    dumpmap.close()
                    dumpfile.close()
                    self._put(ref, dump, mutable=True)
                return True
            elif hashlib.md5(dump).digest() != metadata["md5"]:
                _, dumpmap = self._mmaps[ref]
                dumpmap.seek(0)
                dumpmap.write(dump)
                dumpmap.flush()
                return True
            else:
                return False

    async def delete(self, ref):
        async with _memlock(ref):
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

    def _put(self, ref, dump, *, mutable=False):
        metadata = {
            "ref": ref,
            "mutable": mutable,
            "size": len(dump),
            "md5": hashlib.md5(dump).digest(),
        }

        mempool = pathlib.Path(".mempool", f"{ref}")
        os.makedirs(mempool, exist_ok=True)

        with open(mempool / "meta", "wb") as metafile:
            metafile.write(pickle.dumps(metadata))

        with open(mempool / "dump", "wb") as dumpfile:
            dumpfile.write(dump)

        self._map(ref)
        self._metadata[ref] = metadata

    def _map(self, ref):
        mempool = pathlib.Path(".mempool", f"{ref}")
        metafile = open(mempool / "meta", "r+b")
        metamap = mmap.mmap(metafile.fileno(), 0, flags=mmap.MAP_SHARED)
        dumpfile = open(mempool / "dump", "r+b")
        dumpmap = mmap.mmap(dumpfile.fileno(), 0, flags=mmap.MAP_SHARED)
        self._files[ref] = (metafile, dumpfile)
        self._mmaps[ref] = (metamap, dumpmap)
