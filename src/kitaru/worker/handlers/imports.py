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
"""Import process handler."""

import asyncio
import uuid

from kitaru.api_models.v1.jobs import JobSpecResponse
from kitaru.worker.context import ExecutionContext
from kitaru.worker.handlers.base import materialize_blob
from kitaru.worker.process import (
    JobProcess,
    build_process_env,
    get_python_run_command,
    parse_inline_dependencies,
)

IMPORT_TIMEOUT_SECONDS = 600


class ImportHandler:
    """Import process handler."""

    async def prepare(
        self, ctx: ExecutionContext, job_id: uuid.UUID, spec: JobSpecResponse
    ) -> JobProcess:
        """Build the importer process invocation of an import job.

        The importer code and the payload are materialized into their
        caches concurrently, the harness reads both from disk.

        Args:
            ctx: Execution context.
            job_id: Id of the job.
            spec: Job spec.

        Returns:
            Subprocess invocation.
        """
        assert spec.importer is not None
        env = build_process_env(job_id, {}, spec.secret_env)
        code, payload = await asyncio.gather(
            materialize_blob(
                ctx,
                ctx.blob_cache,
                spec.importer.plugin.blob_id,
                spec.importer.plugin.sha256,
            ),
            materialize_blob(
                ctx,
                ctx.payload_cache,
                spec.importer.payload.blob_id,
                spec.importer.payload.sha256,
            ),
        )
        env["KITARU_JOB_PLUGIN_PATH"] = str(code)
        env["KITARU_JOB_PAYLOAD_PATH"] = str(payload)
        return JobProcess(
            command=get_python_run_command(
                "kitaru.job", ["import"], parse_inline_dependencies(code)
            ),
            working_dir=None,
            env=env,
            timeout_seconds=IMPORT_TIMEOUT_SECONDS,
        )
