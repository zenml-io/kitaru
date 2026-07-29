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
"""Task handler protocol and blob materialization."""

import uuid
from pathlib import Path
from typing import Protocol

from kitaru.api_models.v1.task import TaskSpecResponse
from kitaru.worker.blob_cache import BlobCache
from kitaru.worker.context import ExecutionContext
from kitaru.worker.process import TaskProcess


class TaskHandler(Protocol):
    """Build a subprocess invocation for one task kind."""

    async def prepare(
        self,
        ctx: ExecutionContext,
        task_id: uuid.UUID,
        spec: TaskSpecResponse,
    ) -> TaskProcess:
        """Build the task subprocess.

        Args:
            ctx: Shared execution dependencies.
            task_id: Claimed task id.
            spec: Full claimed task specification.

        Returns:
            Subprocess invocation.
        """
        ...


async def materialize_blob(
    ctx: ExecutionContext,
    cache: BlobCache,
    blob_id: uuid.UUID,
    sha256: str,
) -> Path:
    """Return a cached blob path, downloading on a miss.

    Args:
        ctx: Shared execution dependencies.
        cache: Destination content cache.
        blob_id: Blob id downloaded through the API.
        sha256: Expected digest.

    Returns:
        Cached blob path.
    """
    if cached := await cache.get(sha256):
        return cached
    content = await ctx.client.blobs.download(blob_id)
    return await cache.put(sha256, content)
