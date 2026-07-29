"""Blob cache tests."""

import hashlib
import os

import pytest

from kitaru.worker.blob_cache import BlobCache, BlobCacheError


async def test_put_verifies_content_and_get_touches_entry(tmp_path) -> None:
    content = b"content"
    digest = hashlib.sha256(content).hexdigest()
    cache = BlobCache(tmp_path)

    path = await cache.put(digest, content)
    os.utime(path, ns=(1, 1))
    touched = await cache.get(digest)

    assert touched == path
    assert path.read_bytes() == content
    assert path.stat().st_mtime_ns > 1


async def test_put_rejects_digest_mismatch(tmp_path) -> None:
    cache = BlobCache(tmp_path)

    with pytest.raises(BlobCacheError, match="mismatch"):
        await cache.put("0" * 64, b"content")


async def test_budget_evicts_least_recently_used_complete_entry(tmp_path) -> None:
    cache = BlobCache(tmp_path, max_bytes=6)
    first = b"aaa"
    second = b"bbb"
    third = b"cccc"
    first_hash = hashlib.sha256(first).hexdigest()
    second_hash = hashlib.sha256(second).hexdigest()
    third_hash = hashlib.sha256(third).hexdigest()
    first_path = await cache.put(first_hash, first)
    second_path = await cache.put(second_hash, second)
    os.utime(first_path, ns=(1, 1))
    os.utime(second_path, ns=(2, 2))
    partial = tmp_path / "download.part"
    partial.write_bytes(b"keep")

    await cache.put(third_hash, third)

    assert not first_path.exists()
    assert not second_path.exists()
    assert partial.exists()
    assert cache.path(third_hash).read_bytes() == third


async def test_cache_miss_does_not_create_root(tmp_path) -> None:
    root = tmp_path / "missing"

    assert await BlobCache(root).get("0" * 64) is None
    assert not root.exists()
