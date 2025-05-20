import os
import pathlib
import shutil
import tempfile

import pytest

from wool._mempool import MemoryPool

# Test plan:
# `post` where ref does not already exist in mempool
# `post` where ref does already exist in mempool, but is not mutable
# `post` where ref does already exist in mempool, but is not mapped in the current process (i.e. ref not in _mmaps dict)
# `post` where ref does already exist in mempool, and is mapped in the current process, and the size has changed
# `post` where ref does already exist in mempool, and is mapped in the current process, and the md5 has changed
# `post` where ref does already exist in mempool, and is mapped in the current process, but neither the size nor the md5 have changed
# `get` where ref does not already exist in mempool
# `get` where ref does already exist in mempool, but is not mapped in the current process (i.e. ref not in _mmaps dict)
# `get` where ref does already exist in mempool, and is mapped in the current process, and the size has changed
# `get` where ref does already exist in mempool, and is mapped in the current process, but the size has not changed
# `delete` where ref does not already exist in mempool
# `delete` where ref does already exist in mempool, but is not mapped in the current process (i.e. ref not in _mmaps dict)
# `delete` where ref does already exist in mempool, and is mapped in the current process


def _seed():
    return pathlib.Path("seeds", "test_mempool", "mempool")


@pytest.fixture
def seed():
    return _seed()


@pytest.fixture
def mempool():
    mempath = pathlib.Path(tempfile.mkdtemp())
    seed = _seed()
    for entry in os.scandir(seed):
        if entry.is_dir() and (ref := entry.name) != "locks":
            shutil.copytree(seed / ref, mempath / ref)
    yield MemoryPool(mempath)
    shutil.rmtree(mempath)


class TestMemoryPool:
    @pytest.mark.asyncio
    async def test_map(self, mempool, seed):
        assert not mempool._mmaps
        for _ in range(2):
            await mempool.map()
            for ref, (metamap, dumpmap) in mempool._mmaps.items():
                metamap.seek(0)
                with open(seed / ref / "meta", "rb") as metafile:
                    assert metamap.read() == metafile.read()
                dumpmap.seek(0)
                with open(seed / ref / "dump", "rb") as dumpfile:
                    assert dumpmap.read() == dumpfile.read()

    @pytest.mark.asyncio
    async def test_put(self, mempool):
        await mempool.map()
        prior = None
        for _ in range(2):
            ref = await mempool.put(b"test", mutable=True)
            assert ref != prior
            prior = ref
            metamap, dumpmap = mempool._mmaps[ref]
            metamap.seek(0)
            with open(mempool.path / ref / "meta", "rb") as metafile:
                assert metafile.read() == metamap.read()
            dumpmap.seek(0)
            with open(mempool.path / ref / "dump", "rb") as dumpfile:
                assert dumpfile.read() == dumpmap.read()

    @pytest.mark.asyncio
    async def test_post(self, mempool): ...

    @pytest.mark.asyncio
    async def test_get(self, mempool): ...

    @pytest.mark.asyncio
    async def test_delete(self, mempool): ...
