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
"""Trigger task handler."""

import uuid

from kitaru.api_models.v1.task import TaskSpecResponse, TriggerTaskDetails
from kitaru.worker.context import ExecutionContext
from kitaru.worker.process import TaskProcess, build_process_env, get_python_run_argv


class TriggerHandler:
    """Builds the kitaru.task trigger process for a trigger task."""

    async def prepare(
        self,
        ctx: ExecutionContext,
        task_id: uuid.UUID,
        spec: TaskSpecResponse,
        token: str,
    ) -> TaskProcess:
        """Build the trigger process.

        Args:
            ctx: Execution context.
            task_id: Id of the task being prepared.
            spec: Execution spec of the task, carrying trigger task details.
            token: Bearer token scoped to this task and attempt.

        Returns:
            Process running the trigger function.
        """
        assert isinstance(spec.details, TriggerTaskDetails)
        env = build_process_env(task_id, {}, spec.env, spec.secret_env, token)
        argv = get_python_run_argv("kitaru.task", ["trigger"], [])
        return TaskProcess(
            command=argv,
            working_dir=None,
            env=env,
            timeout_seconds=spec.timeout_seconds,
        )
