import hashlib
import pathlib
import shutil
import tempfile

import pytest
import pytest_asyncio

from wool._mempool import MemoryPool
from wool._mempool._metadata import MetadataMessage


@pytest_asyncio.fixture
async def seed():
    mempath = pathlib.Path(tempfile.mkdtemp())
    mempool = MemoryPool(mempath)
    await mempool.put(b"Ad meliora", mutable=True, ref="meliora")
    await mempool.put(b"Ad aevum", mutable=False, ref="aevum")
    yield mempool
    shutil.rmtree(mempath)


class TestMemoryPool:
    @pytest.mark.asyncio
    async def test_map(self, seed):
        mempool = MemoryPool(seed.path)
        for _ in range(2):
            await mempool.map()
            for ref in ("meliora", "aevum"):
                metamap, dumpmap = mempool._mmaps[ref]
                metamap.seek(0)
                assert MetadataMessage.loads(metamap.read()).ref == ref
                dumpmap.seek(0)
                assert dumpmap.read() == f"Ad {ref}".encode()

    @pytest.mark.parametrize("mutable", [True, False])
    @pytest.mark.asyncio
    async def test_put(self, seed, mutable):
        await (mempool := MemoryPool(seed.path)).map()
        prior = None
        for _ in range(2):
            ref = await mempool.put((expected := b"test"), mutable=mutable)
            assert ref != prior
            prior = ref
            metamap, dumpmap = mempool._mmaps[ref]
            with open(mempool.path / ref / "meta", "rb") as metafile:
                metamap.seek(0)
                metadump = metamap.read()
                assert metadump == metafile.read()
                metadata = MetadataMessage.loads(metadump)
                assert metadata.ref == ref
                assert metadata.mutable == mutable
                assert metadata.size == len(expected)
                assert metadata.md5 == hashlib.md5(expected).digest()
            with open(mempool.path / ref / "dump", "rb") as dumpfile:
                dumpmap.seek(0)
                dump = dumpmap.read()
                assert dump == dumpfile.read()
                assert dump == expected
            try:
                await mempool.put(dump, mutable=mutable, ref=ref)
            except FileExistsError:
                pass
            else:
                assert False, (
                    "Expected FileExistsError to be raised when putting an "
                    "existent reference."
                )

    @pytest.mark.parametrize(
        "dump", [b"Ad meliora", b"Carpe diem", b"Ad meliora"[::-1]]
    )
    @pytest.mark.asyncio
    async def test_post(self, seed: MemoryPool, dump: bytes):
        ref = "meliora"
        mempool = MemoryPool(seed.path)
        await mempool.map()
        await mempool.post(ref, dump)
        metadata = mempool.metadata[ref]
        assert metadata.ref == ref
        assert metadata.mutable is True
        assert metadata.size == len(dump)
        assert metadata.md5 == hashlib.md5(dump).digest()

    @pytest.mark.parametrize(
        "dump", [b"Ad aevum", b"Carpe diem", b"Ad aevum"[::-1]]
    )
    @pytest.mark.asyncio
    async def test_post_immutable(self, seed: MemoryPool, dump: bytes):
        ref = "aevum"
        mempool = MemoryPool(seed.path)
        await mempool.map()
        try:
            assert not await mempool.post(ref, dump)
        except ValueError:
            pass
        else:
            assert False, (
                "Expected ValueError when posting to an immutable reference"
            )

    @pytest.mark.parametrize(
        "dump", [b"Ad meliora", b"Carpe diem", b"Ad meliora"[::-1]]
    )
    @pytest.mark.asyncio
    async def test_post_unmapped(self, seed: MemoryPool, dump: bytes):
        ref = "meliora"
        mempool = MemoryPool(seed.path)
        await mempool.post(ref, dump := b"Hello")
        metadata = mempool.metadata[ref]
        assert metadata.ref == ref
        assert metadata.mutable is True
        assert metadata.size == len(dump)
        assert metadata.md5 == hashlib.md5(dump).digest()

    @pytest.mark.parametrize(
        "dump", [b"Ad aevum", b"Carpe diem", b"Ad aevum"[::-1]]
    )
    @pytest.mark.asyncio
    async def test_post_immutable_unmapped(
        self, seed: MemoryPool, dump: bytes
    ):
        ref = "aevum"
        mempool = MemoryPool(seed.path)
        try:
            assert not await mempool.post(ref, dump)
        except ValueError:
            pass
        else:
            assert False, (
                "Expected ValueError when posting to an immutable reference"
            )

    @pytest.mark.asyncio
    async def test_get(self, seed):
        # Non-existent ref
        try:
            await seed.get("foo")
        except FileNotFoundError:
            pass
        else:
            assert False, (
                "Expected FileNotFoundError when getting a non-existent ref"
            )

        # Existent ref, unmapped
        assert (
            await (mempool := MemoryPool(seed.path)).get("meliora")
            == b"Ad meliora"
        )

        # Existent ref, mapped
        assert await seed.get("meliora") == b"Ad meliora"

        # Existent ref, mapped, but size changed
        await seed.post("meliora", b"Carpe diem")
        assert await mempool.get("meliora") == b"Carpe diem"

    @pytest.mark.asyncio
    async def test_delete(self, seed):
        # Non-existent ref
        try:
            await seed.delete("foo")
        except FileNotFoundError:
            pass
        else:
            assert False, (
                "Expected FileNotFoundError when getting a non-existent ref"
            )

        # Existent ref, unmapped
        await (mempool := MemoryPool(seed.path)).delete("meliora")
        assert "meliora" not in mempool._mmaps
        assert "meliora" not in mempool._metadata
        assert not (mempool.path / "meliora").exists()

        # Existent ref, mapped
        await seed.delete("aevum")
        assert "aevum" not in seed._mmaps
        assert "aevum" not in seed._metadata
        assert not (seed.path / "aevum").exists()
