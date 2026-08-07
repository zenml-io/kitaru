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
"""Agent task handler."""

import json
import uuid

from kitaru.api_models.v1.task import (
    CommandAgentTaskDetails,
    FunctionAgentTaskDetails,
    TaskSpecResponse,
)
from kitaru.worker.context import ExecutionContext
from kitaru.worker.process import TaskProcess, build_process_env, get_python_run_argv

MAX_INPUTS_ENV_BYTES = 32768


class AgentHandler:
    """Builds the agent's own command or its run function process."""

    async def prepare(
        self,
        ctx: ExecutionContext,
        task_id: uuid.UUID,
        spec: TaskSpecResponse,
        token: str,
    ) -> TaskProcess:
        """Build the process running the agent's command or its run function.

        Args:
            ctx: Execution context.
            task_id: Id of the task being prepared.
            spec: Execution spec of the task, carrying agent task details.
            token: Bearer token scoped to this task and attempt.

        Returns:
            Process running the agent.
        """
        if isinstance(spec.details, FunctionAgentTaskDetails):
            env = build_process_env(task_id, {}, spec.env, spec.secret_env, token)
            argv = get_python_run_argv("kitaru.task", ["run-agent"], [])
            return TaskProcess(
                command=argv,
                working_dir=None,
                env=env,
                timeout_seconds=spec.timeout_seconds,
            )
        assert spec.run is not None, "agent task spec is missing a run spec"
        assert isinstance(spec.details, CommandAgentTaskDetails)
        env = build_process_env(task_id, spec.run.env, spec.env, spec.secret_env, token)
        inputs_json = json.dumps(spec.details.inputs)
        if len(inputs_json.encode("utf-8")) <= MAX_INPUTS_ENV_BYTES:
            env["KITARU_TASK_INPUTS"] = inputs_json
        if spec.details.replay_id is not None:
            env["KITARU_REPLAY_ID"] = str(spec.details.replay_id)
        return TaskProcess(
            command=spec.run.command,
            working_dir=spec.run.working_dir,
            env=env,
            timeout_seconds=spec.timeout_seconds,
        )
