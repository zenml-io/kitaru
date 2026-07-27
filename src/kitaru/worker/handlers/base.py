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
"""Job handler protocol and blob materialization."""

import uuid
from pathlib import Path
from typing import Protocol

from kitaru.api_models.v1.jobs import JobSpecResponse
from kitaru.blob_cache import BlobCache
from kitaru.worker.context import ExecutionContext
from kitaru.worker.process import JobProcess


class JobHandler(Protocol):
    """Process builder for one job kind."""

    async def prepare(
        self, ctx: ExecutionContext, job_id: uuid.UUID, spec: JobSpecResponse
    ) -> JobProcess:
        """Build the subprocess invocation of a job.

        Args:
            ctx: Execution context.
            job_id: Id of the job.
            spec: Job spec.

        Returns:
            Subprocess invocation.
        """
        ...


async def materialize_blob(
    ctx: ExecutionContext, cache: BlobCache, blob_id: uuid.UUID, sha256: str
) -> Path:
    """Return the cached path of a blob, downloading it once.

    Args:
        ctx: Execution context.
        cache: Cache the content is materialized into.
        blob_id: Id of the blob.
        sha256: Hash of the blob content.

    Returns:
        Path of the cached file.
    """
    cached = cache.get(sha256)
    if cached is not None:
        return cached
    content = await ctx.client.blobs.download(blob_id)
    return cache.put(sha256, content)
