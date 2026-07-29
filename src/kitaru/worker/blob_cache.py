#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Content-addressed on-disk blob cache."""

import asyncio
import hashlib
import os
import uuid
from pathlib import Path


class BlobCacheError(Exception):
    """Blob cache operation failed."""


class BlobCache:
    """Content-addressed cache keyed by SHA-256 digest."""

    def __init__(self, root: Path, max_bytes: int | None = None) -> None:
        """Initialize the cache.

        Args:
            root: Directory containing cached files.
            max_bytes: Maximum complete-file size, unbounded when omitted.
        """
        if max_bytes is not None and max_bytes < 0:
            raise ValueError("max_bytes must be non-negative")
        self._root = root
        self._max_bytes = max_bytes

    def path(self, sha256: str) -> Path:
        """Return the cache path for a digest.

        Args:
            sha256: Content digest.

        Returns:
            Cache entry path.
        """
        return self._root / sha256

    async def get(self, sha256: str) -> Path | None:
        """Return and touch an entry when it exists.

        Args:
            sha256: Content digest.

        Returns:
            Entry path, or None on a miss.
        """
        return await asyncio.to_thread(self._get, sha256)

    async def put(self, sha256: str, content: bytes) -> Path:
        """Verify and atomically store content.

        Args:
            sha256: Expected content digest.
            content: Blob bytes.

        Raises:
            BlobCacheError: The digest does not match or the entry exceeds the
                cache budget.

        Returns:
            Stored entry path.
        """
        return await asyncio.to_thread(self._put, sha256, content)

    def _get(self, sha256: str) -> Path | None:
        entry = self.path(sha256)
        if not entry.is_file():
            return None
        entry.touch()
        return entry

    def _put(self, sha256: str, content: bytes) -> Path:
        actual = hashlib.sha256(content).hexdigest()
        if actual != sha256:
            raise BlobCacheError(
                f"Blob SHA-256 mismatch: expected {sha256}, got {actual}."
            )
        if self._max_bytes is not None and len(content) > self._max_bytes:
            raise BlobCacheError(
                f"Blob is {len(content)} bytes, larger than the "
                f"{self._max_bytes}-byte cache budget."
            )

        self._root.mkdir(parents=True, exist_ok=True)
        entry = self.path(sha256)
        if entry.is_file():
            entry.touch()
            return entry

        self._evict_for(len(content))
        partial = self._root / f"{sha256}.{uuid.uuid4().hex}.part"
        try:
            partial.write_bytes(content)
            os.replace(partial, entry)
        finally:
            partial.unlink(missing_ok=True)
        return entry

    def _evict_for(self, incoming_bytes: int) -> None:
        if self._max_bytes is None:
            return
        entries = [
            path
            for path in self._root.iterdir()
            if path.is_file() and not path.name.endswith(".part")
        ]
        sized = [(path, path.stat()) for path in entries]
        total = sum(stat.st_size for _, stat in sized)
        for path, stat in sorted(sized, key=lambda item: item[1].st_mtime_ns):
            if total + incoming_bytes <= self._max_bytes:
                break
            try:
                path.unlink()
            except FileNotFoundError:
                continue
            total -= stat.st_size
