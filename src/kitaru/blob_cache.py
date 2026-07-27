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
"""Content addressed cache of blobs downloaded by a worker."""

import hashlib
import os
import tempfile
from pathlib import Path

DEFAULT_CACHE_ROOT = Path.home() / ".cache" / "kitaru" / "blobs"
DEFAULT_PAYLOAD_CACHE_ROOT = Path.home() / ".cache" / "kitaru" / "payloads"


class BlobCacheError(Exception):
    """Raised when content does not hash to the expected digest."""


class BlobCache:
    """Blob cache keyed by content hash."""

    def __init__(self, root: Path | None = None, max_bytes: int | None = None) -> None:
        """Initialize the cache.

        Args:
            root: Directory holding the cached content.
            max_bytes: Size budget, unbounded when omitted.
        """
        self._root = root or DEFAULT_CACHE_ROOT
        self._max_bytes = max_bytes

    def path(self, sha256: str) -> Path:
        """Return the path content with a hash is cached under.

        Args:
            sha256: Hash of the content.

        Returns:
            Cache path.
        """
        return self._root / sha256

    def get(self, sha256: str) -> Path | None:
        """Return the cached path of content.

        The write verifies the hash and lands atomically, so a present
        entry holds the content its name states. A hit counts as a use for
        eviction ordering.

        Args:
            sha256: Hash of the content.

        Returns:
            Cache path, ``None`` on a miss.
        """
        path = self.path(sha256)
        if not path.is_file():
            return None
        os.utime(path)
        return path

    def put(self, sha256: str, content: bytes) -> Path:
        """Cache content under its hash, evicting to stay within the budget.

        Args:
            sha256: Expected hash of the content.
            content: Content to cache.

        Raises:
            BlobCacheError: The content hashes to another digest.

        Returns:
            Cache path.
        """
        digest = hashlib.sha256(content).hexdigest()
        if digest != sha256:
            raise BlobCacheError(f"Content hashes to {digest}, expected {sha256}")
        self._root.mkdir(parents=True, exist_ok=True)
        path = self.path(sha256)
        self._evict(path, len(content))
        handle, staged = tempfile.mkstemp(dir=self._root, suffix=".part")
        try:
            with os.fdopen(handle, "wb") as file:
                file.write(content)
            os.replace(staged, path)
        except BaseException:
            Path(staged).unlink(missing_ok=True)
            raise
        return path

    def _evict(self, incoming: Path, size: int) -> None:
        """Drop least recently used entries until an incoming entry fits.

        Content larger than the budget is stored after everything else was
        dropped.

        Args:
            incoming: Path the content is about to be written to.
            size: Size of the incoming content.
        """
        if self._max_bytes is None:
            return
        entries = [
            (entry.stat().st_mtime_ns, entry.stat().st_size, entry)
            for entry in self._root.iterdir()
            if entry.is_file() and entry != incoming and entry.suffix != ".part"
        ]
        total = sum(entry[1] for entry in entries)
        for _, entry_size, entry in sorted(entries):
            if total + size <= self._max_bytes:
                return
            entry.unlink(missing_ok=True)
            total -= entry_size
