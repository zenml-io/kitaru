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
"""Analyzer task handler."""

import uuid

from kitaru.api_models.v1.task import (
    AnalysisTaskDetails,
    ScriptPluginSpec,
    TaskSpecResponse,
)
from kitaru.worker.context import ExecutionContext
from kitaru.worker.handlers.base import materialize_blob
from kitaru.worker.process import (
    TaskProcess,
    build_process_env,
    get_python_run_argv,
    parse_inline_dependencies,
)


class AnalysisHandler:
    """Builds the kitaru.task analyze process for an analyzer task."""

    async def prepare(
        self,
        ctx: ExecutionContext,
        task_id: uuid.UUID,
        spec: TaskSpecResponse,
        token: str,
    ) -> TaskProcess:
        """Build the analyzer process, materializing a script plugin.

        Args:
            ctx: Execution context.
            task_id: Id of the task being prepared.
            spec: Execution spec of the task, carrying analysis task details.
            token: Bearer token scoped to this task and attempt.

        Returns:
            Process running the analyzer plugin.
        """
        assert isinstance(spec.details, AnalysisTaskDetails)
        plugin = spec.details.plugin
        env = build_process_env(task_id, {}, spec.env, spec.secret_env, token)
        if isinstance(plugin, ScriptPluginSpec):
            path = await materialize_blob(
                ctx, ctx.blob_cache, plugin.blob_id, plugin.sha256
            )
            env["KITARU_TASK_PLUGIN_PATH"] = str(path)
            dependencies = parse_inline_dependencies(path)
        else:
            dependencies = [plugin.requirement]
        argv = get_python_run_argv("kitaru.task", ["analyze"], dependencies)
        return TaskProcess(
            command=argv,
            working_dir=None,
            env=env,
            timeout_seconds=spec.timeout_seconds,
        )
