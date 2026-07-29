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
"""Agent task process handler."""

import json
import uuid

from kitaru.api_models.v1.task import AgentTaskDetails, TaskSpecResponse
from kitaru.worker.context import ExecutionContext
from kitaru.worker.process import (
    MAX_INPUTS_ENV_BYTES,
    TaskProcess,
    build_process_env,
)


class AgentHandler:
    """Build agent task subprocesses from their run specs."""

    async def prepare(
        self,
        ctx: ExecutionContext,
        task_id: uuid.UUID,
        spec: TaskSpecResponse,
    ) -> TaskProcess:
        """Build an agent subprocess.

        Args:
            ctx: Shared execution dependencies.
            task_id: Claimed task id.
            spec: Agent task specification.

        Raises:
            ValueError: The agent spec has no run configuration.

        Returns:
            Agent subprocess invocation.
        """
        _ = ctx
        if spec.run is None:
            raise ValueError("Agent task has no run specification.")
        details = spec.details
        if not isinstance(details, AgentTaskDetails):
            raise ValueError("Agent task has mismatched details.")
        env = build_process_env(task_id, spec.run.env, spec.env, spec.secret_env)
        encoded = json.dumps(details.inputs)
        if len(encoded.encode("utf-8")) <= MAX_INPUTS_ENV_BYTES:
            env["KITARU_TASK_INPUTS"] = encoded
        return TaskProcess(
            command=spec.run.command,
            working_dir=spec.run.working_dir,
            env=env,
            timeout_seconds=spec.timeout_seconds,
        )
