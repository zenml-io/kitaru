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
"""Content-addressed on-disk cache for blob content."""

import asyncio
import contextlib
import hashlib
import os
import tempfile
import time
from pathlib import Path


class BlobCacheError(Exception):
    """Blob cache error."""


class BlobCache:
    """Content-addressed file cache keyed by sha256."""

    def __init__(self, root: Path, max_bytes: int | None = None) -> None:
        """Initialize the cache.

        Args:
            root: Directory holding cached blob files.
            max_bytes: Eviction budget in bytes, unbounded when None.
        """
        self._root = root
        self._max_bytes = max_bytes
        self._root.mkdir(parents=True, exist_ok=True)

    def path(self, sha256: str) -> Path:
        """Return the path a blob is cached at, whether or not it exists.

        Args:
            sha256: Content hash of the blob.

        Returns:
            Path the blob is stored at.
        """
        return self._root / sha256

    async def get(self, sha256: str) -> Path | None:
        """Return the cached path of a blob, touching its mtime on a hit.

        Args:
            sha256: Content hash of the blob.

        Returns:
            Cached path, or None on a miss.
        """
        return await asyncio.to_thread(self._get, sha256)

    async def put(self, sha256: str, content: bytes) -> Path:
        """Cache blob content, evicting older entries to make room.

        Args:
            sha256: Expected content hash of the blob.
            content: Blob content.

        Raises:
            BlobCacheError: The content does not hash to sha256.

        Returns:
            Path the content was cached at.
        """
        return await asyncio.to_thread(self._put, sha256, content)

    def _get(self, sha256: str) -> Path | None:
        """Return the cached path of a blob, touching its mtime on a hit.

        Args:
            sha256: Content hash of the blob.

        Returns:
            Cached path, or None on a miss.
        """
        path = self.path(sha256)
        try:
            now = time.time()
            os.utime(path, (now, now))
        except OSError:
            return None
        return path

    def _put(self, sha256: str, content: bytes) -> Path:
        """Verify and write blob content through a temp file and atomic rename.

        Args:
            sha256: Expected content hash of the blob.
            content: Blob content.

        Raises:
            BlobCacheError: The content does not hash to sha256.

        Returns:
            Path the content was cached at.
        """
        digest = hashlib.sha256(content).hexdigest()
        if digest != sha256:
            raise BlobCacheError(f"Content hashes to {digest}, expected {sha256}.")
        path = self.path(sha256)
        handle, temp_name = tempfile.mkstemp(
            dir=self._root, prefix=f".{sha256}.", suffix=".part"
        )
        try:
            with os.fdopen(handle, "wb") as file:
                file.write(content)
            if self._max_bytes is not None:
                self._evict_to_fit(len(content))
            try:
                os.replace(temp_name, path)
            except OSError:
                # Content-addressed, so an existing destination already holds
                # the right content even when the rename itself failed.
                if not path.exists():
                    raise
                Path(temp_name).unlink(missing_ok=True)
        except BaseException:
            Path(temp_name).unlink(missing_ok=True)
            raise
        return path

    def _evict_to_fit(self, incoming_bytes: int) -> None:
        """Evict least-recently-used complete entries until content fits.

        Args:
            incoming_bytes: Size of the content about to be written.
        """
        candidates: list[tuple[Path, float, int]] = []
        for entry in self._root.iterdir():
            if not entry.is_file() or entry.name.endswith(".part"):
                continue
            try:
                stat = entry.stat()
            except OSError:
                # Another process sharing the cache evicted the entry first.
                continue
            candidates.append((entry, stat.st_mtime, stat.st_size))
        candidates.sort(key=lambda candidate: candidate[1])

        assert self._max_bytes is not None
        total = incoming_bytes + sum(size for _, _, size in candidates)
        for entry, _, size in candidates:
            if total <= self._max_bytes:
                break
            with contextlib.suppress(OSError):
                entry.unlink()
                # Only counts as reclaimed space once the unlink succeeds.
                total -= size
