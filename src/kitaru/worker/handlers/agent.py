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
"""Agent process handler for replay and session run jobs."""

import json
import uuid

from kitaru.api_models.v1.jobs import JobSpecResponse
from kitaru.worker.context import ExecutionContext
from kitaru.worker.process import JobProcess, build_process_env

# TODO: Serve this threshold from the server via the job spec.
MAX_INPUTS_ENV_BYTES = 32768


class AgentHandler:
    """Agent process handler, shared by replay and session run jobs."""

    async def prepare(
        self, ctx: ExecutionContext, job_id: uuid.UUID, spec: JobSpecResponse
    ) -> JobProcess:
        """Build the agent process invocation of a replay or session run.

        ``KITARU_JOB_INPUTS`` is set only when the JSON-encoded inputs fit
        the threshold, agent code fetches the spec otherwise.

        Args:
            ctx: Execution context.
            job_id: Id of the job.
            spec: Job spec.

        Returns:
            Subprocess invocation.
        """
        _ = ctx
        assert spec.run is not None
        env = build_process_env(job_id, spec.run.env, spec.secret_env)
        if spec.name is not None:
            env["KITARU_JOB_SESSION_NAME"] = spec.name
        encoded_inputs = json.dumps(spec.inputs)
        if len(encoded_inputs.encode("utf-8")) <= MAX_INPUTS_ENV_BYTES:
            env["KITARU_JOB_INPUTS"] = encoded_inputs
        return JobProcess(
            command=spec.run.command,
            working_dir=spec.run.working_dir,
            env=env,
            timeout_seconds=spec.run.timeout_seconds,
        )
