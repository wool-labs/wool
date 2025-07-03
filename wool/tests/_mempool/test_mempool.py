import hashlib

import pytest

from wool._mempool import MemoryPool
from wool._mempool._metadata import MetadataMessage


class TestMemoryPool:
    @pytest.mark.asyncio
    async def test_map(self, seed: MemoryPool):
        mempool = MemoryPool(seed.path)
        for _ in range(2):
            await mempool.map()
            for ref in ("meliora", "aevum"):
                metamap = mempool._objects[ref].metadata.mmap
                metamap.seek(0)
                assert MetadataMessage.loads(metamap.read()).ref == ref
                dumpmap = mempool._objects[ref].mmap
                dumpmap.seek(0)
                assert dumpmap.read() == f"Ad {ref}".encode()
        with pytest.raises(FileNotFoundError):
            await mempool.map("foo")

    @pytest.mark.parametrize("mutable", [True, False])
    @pytest.mark.asyncio
    async def test_put(self, seed: MemoryPool, mutable: bool):
        await (mempool := MemoryPool(seed.path)).map()
        prior = None
        for _ in range(2):
            ref = await mempool.put((expected := b"test"), mutable=mutable)
            assert ref != prior
            prior = ref
            dumpmap = mempool._objects[ref].mmap
            metamap = mempool._objects[ref].metadata.mmap
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
            with pytest.raises(FileExistsError):
                await mempool.put(dump, mutable=mutable, ref=ref)

    @pytest.mark.parametrize(
        "dump", [b"Ad meliora", b"Carpe diem", b"Ad meliora"[::-1]]
    )
    @pytest.mark.asyncio
    async def test_post(self, seed: MemoryPool, dump: bytes):
        ref = "meliora"
        mempool = MemoryPool(seed.path)
        await mempool.map()
        await mempool.post(ref, dump)
        metadata = mempool._objects[ref].metadata
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
        with pytest.raises(ValueError):
            await mempool.post(ref, dump)

    @pytest.mark.parametrize(
        "dump", [b"Ad meliora", b"Carpe diem", b"Ad meliora"[::-1]]
    )
    @pytest.mark.asyncio
    async def test_post_unmapped(self, seed: MemoryPool, dump: bytes):
        ref = "meliora"
        mempool = MemoryPool(seed.path)
        await mempool.post(ref, dump := b"Hello")
        metadata = mempool._objects[ref].metadata
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
        with pytest.raises(ValueError):
            await mempool.post(ref, dump)

    @pytest.mark.asyncio
    async def test_get(self, seed: MemoryPool):
        # Non-existent ref
        with pytest.raises(FileNotFoundError):
            await seed.get("foo")

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
        with pytest.raises(FileNotFoundError):
            await seed.delete("foo")

        # Existent ref, unmapped
        await (mempool := MemoryPool(seed.path)).delete("meliora")
        assert "meliora" not in mempool._objects
        assert not (mempool.path / "meliora").exists()

        # Existent ref, mapped
        await seed.delete("aevum")
        assert "aevum" not in seed._objects
        assert not (seed.path / "aevum").exists()
