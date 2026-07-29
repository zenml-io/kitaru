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
"""Importer task process handler."""

import asyncio
import uuid

from kitaru.api_models.v1.task import (
    ImportTaskDetails,
    PackagePluginSpec,
    ScriptPluginSpec,
    TaskSpecResponse,
)
from kitaru.worker.context import ExecutionContext
from kitaru.worker.handlers.base import materialize_blob
from kitaru.worker.process import (
    TaskProcess,
    build_process_env,
    get_python_run_command,
    parse_inline_dependencies,
)


class ImportHandler:
    """Build importer task subprocesses."""

    async def prepare(
        self,
        ctx: ExecutionContext,
        task_id: uuid.UUID,
        spec: TaskSpecResponse,
    ) -> TaskProcess:
        """Build an importer subprocess.

        Args:
            ctx: Shared execution dependencies.
            task_id: Claimed task id.
            spec: Importer task specification.

        Raises:
            ValueError: The task details or plugin variant is unsupported.

        Returns:
            Importer subprocess invocation.
        """
        details = spec.details
        if not isinstance(details, ImportTaskDetails):
            raise ValueError("Importer task has mismatched details.")
        env = build_process_env(task_id, {}, spec.env, spec.secret_env)
        plugin = details.plugin
        if isinstance(plugin, ScriptPluginSpec):
            code, payload = await asyncio.gather(
                materialize_blob(ctx, ctx.blob_cache, plugin.blob_id, plugin.sha256),
                materialize_blob(
                    ctx,
                    ctx.payload_cache,
                    details.payload.blob_id,
                    details.payload.sha256,
                ),
            )
            env["KITARU_TASK_PLUGIN_PATH"] = str(code)
            dependencies = parse_inline_dependencies(code)
        elif isinstance(plugin, PackagePluginSpec):
            payload = await materialize_blob(
                ctx,
                ctx.payload_cache,
                details.payload.blob_id,
                details.payload.sha256,
            )
            dependencies = [plugin.requirement]
        else:
            raise ValueError(f"Unsupported importer plugin: {plugin!r}")
        env["KITARU_TASK_PAYLOAD_PATH"] = str(payload)
        return TaskProcess(
            command=get_python_run_command("kitaru.task", ["import"], dependencies),
            working_dir=None,
            env=env,
            timeout_seconds=spec.timeout_seconds,
        )
