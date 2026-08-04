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
"""Task handler protocol and blob materialization."""

import uuid
from pathlib import Path
from typing import Protocol

from kitaru.api_models.v1.task import TaskSpecResponse
from kitaru.worker.blob_cache import BlobCache
from kitaru.worker.context import ExecutionContext
from kitaru.worker.process import TaskProcess


class TaskHandler(Protocol):
    """Per-kind strategy building the process for a claimed task."""

    async def prepare(
        self,
        ctx: ExecutionContext,
        task_id: uuid.UUID,
        spec: TaskSpecResponse,
        token: str,
    ) -> TaskProcess:
        """Build the process that executes a claimed task.

        Args:
            ctx: Execution context.
            task_id: Id of the task being prepared.
            spec: Execution spec of the task.
            token: Bearer token scoped to this task and attempt.

        Returns:
            Process ready to run.
        """
        ...


async def materialize_blob(
    ctx: ExecutionContext, cache: BlobCache, blob_id: uuid.UUID, sha256: str
) -> Path:
    """Return a blob's cached path, downloading and caching it on a miss.

    Args:
        ctx: Execution context.
        cache: Cache to check and populate.
        blob_id: Id of the blob to materialize.
        sha256: Expected content hash of the blob.

    Returns:
        Path of the cached blob content.
    """
    cached = await cache.get(sha256)
    if cached is not None:
        return cached
    content = await ctx.client.blobs.download(blob_id)
    return await cache.put(sha256, content)
