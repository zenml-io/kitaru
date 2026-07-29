#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Tests for the content-addressed blob cache."""

import hashlib
import os
import time
from pathlib import Path

import pytest

from kitaru.worker.blob_cache import BlobCache, BlobCacheError


def _digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


async def test_get_misses_on_an_empty_cache(tmp_path: Path) -> None:
    """A cache with nothing stored reports a miss."""
    cache = BlobCache(tmp_path / "blobs")
    assert await cache.get("0" * 64) is None


async def test_put_then_get_hits(tmp_path: Path) -> None:
    """Content stored with put is retrievable through get."""
    cache = BlobCache(tmp_path / "blobs")
    content = b"hello world"
    digest = _digest(content)
    path = await cache.put(digest, content)
    assert path.read_bytes() == content

    hit = await cache.get(digest)
    assert hit == path
    assert hit.read_bytes() == content


async def test_put_verifies_the_digest(tmp_path: Path) -> None:
    """A mismatched digest is rejected without writing a complete file."""
    cache = BlobCache(tmp_path / "blobs")
    with pytest.raises(BlobCacheError):
        await cache.put("0" * 64, b"hello world")
    remaining = list((tmp_path / "blobs").iterdir())
    assert remaining == []


async def test_put_is_atomic_no_partial_file_visible_at_the_final_path(
    tmp_path: Path,
) -> None:
    """The cached path never exists as a partially written file."""
    cache = BlobCache(tmp_path / "blobs")
    content = b"atomic content"
    digest = _digest(content)
    path = await cache.put(digest, content)
    assert path.exists()
    assert path.read_bytes() == content
    leftovers = [entry for entry in (tmp_path / "blobs").iterdir() if entry != path]
    assert leftovers == []


async def test_get_touches_mtime(tmp_path: Path) -> None:
    """A get call bumps the cached file's mtime."""
    cache = BlobCache(tmp_path / "blobs")
    content = b"touch me"
    digest = _digest(content)
    path = await cache.put(digest, content)
    old_mtime = path.stat().st_mtime
    os.utime(path, (old_mtime - 1000, old_mtime - 1000))

    await cache.get(digest)
    assert path.stat().st_mtime > old_mtime - 1000


async def test_unbounded_cache_never_evicts(tmp_path: Path) -> None:
    """A cache with no max_bytes keeps every entry."""
    cache = BlobCache(tmp_path / "blobs", max_bytes=None)
    paths = []
    for index in range(5):
        content = f"entry-{index}".encode() * 100
        digest = _digest(content)
        paths.append(await cache.put(digest, content))
    assert all(path.exists() for path in paths)


async def test_eviction_removes_least_recently_used_first(tmp_path: Path) -> None:
    """Filling a budgeted cache evicts the oldest entry to make room."""
    entry_size = 100
    cache = BlobCache(tmp_path / "blobs", max_bytes=entry_size * 2)

    content_a = b"a" * entry_size
    content_b = b"b" * entry_size
    content_c = b"c" * entry_size
    digest_a, digest_b, digest_c = (
        _digest(content_a),
        _digest(content_b),
        _digest(content_c),
    )

    path_a = await cache.put(digest_a, content_a)
    time.sleep(0.01)
    path_b = await cache.put(digest_b, content_b)
    time.sleep(0.01)

    # Touch a so it is more recently used than b right before c is written,
    # which should push b out instead of a.
    await cache.get(digest_a)
    time.sleep(0.01)
    await cache.put(digest_c, content_c)

    assert path_a.exists()
    assert not path_b.exists()
    assert (tmp_path / "blobs" / digest_c).exists()


async def test_in_flight_part_files_are_never_evicted_as_complete(
    tmp_path: Path,
) -> None:
    """A stray .part file never counts toward eviction as a complete entry."""
    root = tmp_path / "blobs"
    cache = BlobCache(root, max_bytes=50)
    stray = root / "stray-upload.part"
    stray.write_bytes(b"x" * 1000)
    old_time = time.time() - 10_000
    os.utime(stray, (old_time, old_time))

    content = b"y" * 40
    digest = _digest(content)
    await cache.put(digest, content)

    assert stray.exists()


def test_path_is_pure(tmp_path: Path) -> None:
    """path() computes the location without touching the filesystem."""
    cache = BlobCache(tmp_path / "blobs")
    digest = "a" * 64
    assert cache.path(digest) == tmp_path / "blobs" / digest
