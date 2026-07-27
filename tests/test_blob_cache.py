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
"""Tests for the worker blob cache."""

import hashlib
import os
from pathlib import Path

import pytest

from kitaru.blob_cache import BlobCache, BlobCacheError

CONTENT = b"def score(session):\n    return 1.0\n"
DIGEST = hashlib.sha256(CONTENT).hexdigest()


def test_get_misses_on_empty_cache(tmp_path: Path) -> None:
    """Report a miss when nothing is cached."""
    assert BlobCache(tmp_path / "blobs").get(DIGEST) is None


def test_put_then_get_hits(tmp_path: Path) -> None:
    """Serve cached content back on a hit."""
    cache = BlobCache(tmp_path)
    path = cache.put(DIGEST, CONTENT)

    assert path.read_bytes() == CONTENT
    assert cache.get(DIGEST) == path


def test_put_creates_the_root(tmp_path: Path) -> None:
    """Create the cache directory on the first write."""
    cache = BlobCache(tmp_path / "nested" / "blobs")
    assert cache.put(DIGEST, CONTENT).is_file()


def test_put_leaves_no_partial_files(tmp_path: Path) -> None:
    """Write the content atomically, leaving only the cache entry."""
    cache = BlobCache(tmp_path)
    cache.put(DIGEST, CONTENT)

    assert [path.name for path in tmp_path.iterdir()] == [DIGEST]


def test_put_rejects_mismatched_content(tmp_path: Path) -> None:
    """Reject content that hashes to another digest."""
    cache = BlobCache(tmp_path)
    with pytest.raises(BlobCacheError, match="Content hashes to"):
        cache.put(DIGEST, b"other")
    assert cache.get(DIGEST) is None


def test_get_trusts_the_write_path(tmp_path: Path) -> None:
    """Serve a present entry back without re-reading its content."""
    cache = BlobCache(tmp_path)
    path = cache.put(DIGEST, CONTENT)
    path.write_bytes(b"tampered")

    assert cache.get(DIGEST) == path


def test_put_overwrites_an_existing_entry(tmp_path: Path) -> None:
    """Replace a corrupted entry on the next write."""
    cache = BlobCache(tmp_path)
    cache.path(DIGEST).parent.mkdir(parents=True, exist_ok=True)
    cache.path(DIGEST).write_bytes(b"tampered")

    assert cache.put(DIGEST, CONTENT).read_bytes() == CONTENT


def digest(content: bytes) -> str:
    """Return the hash of content.

    Args:
        content: Content to hash.

    Returns:
        Hash of the content.
    """
    return hashlib.sha256(content).hexdigest()


def age(cache: BlobCache, sha256: str, seconds: int) -> None:
    """Backdate a cache entry so eviction order is deterministic.

    Args:
        cache: Blob cache.
        sha256: Hash of the entry.
        seconds: Seconds to backdate the entry by.
    """
    path = cache.path(sha256)
    stamp = path.stat().st_mtime - seconds
    os.utime(path, (stamp, stamp))


def test_budget_evicts_least_recently_used_entries(tmp_path: Path) -> None:
    """Drop the oldest entries until the incoming content fits."""
    cache = BlobCache(tmp_path, max_bytes=30)
    for index, content in enumerate([b"a" * 10, b"b" * 10, b"c" * 10]):
        cache.put(digest(content), content)
        age(cache, digest(content), 300 - index * 100)

    fresh = b"d" * 10
    cache.put(digest(fresh), fresh)

    assert cache.get(digest(b"a" * 10)) is None
    assert cache.get(digest(b"b" * 10)) is not None
    assert cache.get(digest(b"c" * 10)) is not None
    assert cache.get(digest(fresh)) is not None


def test_get_touches_the_entry(tmp_path: Path) -> None:
    """Keep an entry that was read after the entries around it."""
    cache = BlobCache(tmp_path, max_bytes=20)
    old = b"a" * 10
    recent = b"b" * 10
    cache.put(digest(old), old)
    age(cache, digest(old), 300)
    cache.put(digest(recent), recent)
    age(cache, digest(recent), 200)
    assert cache.get(digest(old)) is not None

    fresh = b"c" * 10
    cache.put(digest(fresh), fresh)

    assert cache.get(digest(old)) is not None
    assert cache.get(digest(recent)) is None


def test_budget_stores_content_larger_than_the_budget(tmp_path: Path) -> None:
    """Store oversized content after dropping every other entry."""
    cache = BlobCache(tmp_path, max_bytes=20)
    small = b"a" * 10
    cache.put(digest(small), small)

    oversized = b"b" * 50
    path = cache.put(digest(oversized), oversized)

    assert path.read_bytes() == oversized
    assert cache.get(digest(small)) is None


def test_budget_ignores_the_entry_being_replaced(tmp_path: Path) -> None:
    """Keep the other entries when an entry is written again."""
    cache = BlobCache(tmp_path, max_bytes=20)
    kept = b"a" * 10
    rewritten = b"b" * 10
    cache.put(digest(kept), kept)
    age(cache, digest(kept), 300)
    cache.put(digest(rewritten), rewritten)
    cache.put(digest(rewritten), rewritten)

    assert cache.get(digest(kept)) is not None
    assert cache.get(digest(rewritten)) is not None


def test_unbounded_cache_never_evicts(tmp_path: Path) -> None:
    """Keep every entry when no budget is configured."""
    cache = BlobCache(tmp_path)
    for content in [b"a" * 100, b"b" * 100, b"c" * 100]:
        cache.put(digest(content), content)

    assert all(
        cache.get(digest(content)) is not None
        for content in [b"a" * 100, b"b" * 100, b"c" * 100]
    )
