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
"""Score process handler."""

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

SCORE_TIMEOUT_SECONDS = 300


class ScoreHandler:
    """Score process handler.

    A source scorer runs in the agent's run environment, a registry scorer
    runs the cached plugin code with no working directory.
    """

    async def prepare(
        self, ctx: ExecutionContext, job_id: uuid.UUID, spec: JobSpecResponse
    ) -> JobProcess:
        """Build the scorer process invocation of a score job.

        Args:
            ctx: Execution context.
            job_id: Id of the job.
            spec: Job spec.

        Returns:
            Subprocess invocation.
        """
        assert spec.scorer is not None
        run = spec.run
        env = build_process_env(
            job_id, run.env if run is not None else {}, spec.secret_env
        )
        timeout_seconds = (
            run.timeout_seconds
            if run is not None and run.timeout_seconds is not None
            else SCORE_TIMEOUT_SECONDS
        )
        if spec.scorer.plugin is None:
            assert run is not None
            return JobProcess(
                command=get_python_run_command("kitaru.job", ["score"], []),
                working_dir=run.working_dir,
                env=env,
                timeout_seconds=timeout_seconds,
            )
        path = await materialize_blob(
            ctx, ctx.blob_cache, spec.scorer.plugin.blob_id, spec.scorer.plugin.sha256
        )
        env["KITARU_JOB_PLUGIN_PATH"] = str(path)
        return JobProcess(
            command=get_python_run_command(
                "kitaru.job", ["score"], parse_inline_dependencies(path)
            ),
            working_dir=None,
            env=env,
            timeout_seconds=timeout_seconds,
        )
