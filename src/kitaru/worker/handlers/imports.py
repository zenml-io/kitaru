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
"""Importer task handler."""

import asyncio
import uuid
from collections.abc import Coroutine
from pathlib import Path
from typing import Any

from kitaru.api_models.v1.task import (
    BlobSourceSpec,
    ImportTaskDetails,
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


class ImportHandler:
    """Builds the kitaru.task import process for an importer task."""

    async def prepare(
        self,
        ctx: ExecutionContext,
        task_id: uuid.UUID,
        spec: TaskSpecResponse,
        token: str,
    ) -> TaskProcess:
        """Build the importer process, materializing its plugin and payload.

        Args:
            ctx: Execution context.
            task_id: Id of the task being prepared.
            spec: Execution spec of the task, carrying importer task details.
            token: Bearer token scoped to this task and attempt.

        Returns:
            Process running the importer plugin against its payload.
        """
        assert isinstance(spec.details, ImportTaskDetails)
        details = spec.details
        env = build_process_env(task_id, {}, spec.env, spec.secret_env, token)
        materializations: dict[str, Coroutine[Any, Any, Path]] = {}
        if isinstance(details.plugin, ScriptPluginSpec):
            materializations["KITARU_TASK_PLUGIN_PATH"] = materialize_blob(
                ctx, ctx.blob_cache, details.plugin.blob_id, details.plugin.sha256
            )
        if isinstance(details.source, BlobSourceSpec):
            materializations["KITARU_TASK_PAYLOAD_PATH"] = materialize_blob(
                ctx, ctx.payload_cache, details.source.blob_id, details.source.sha256
            )
        paths = await asyncio.gather(*materializations.values())
        env.update(
            {key: str(path) for key, path in zip(materializations, paths, strict=True)}
        )
        if isinstance(details.plugin, ScriptPluginSpec):
            dependencies = parse_inline_dependencies(
                Path(env["KITARU_TASK_PLUGIN_PATH"])
            )
        else:
            dependencies = [details.plugin.requirement]
        argv = get_python_run_argv("kitaru.task", ["import"], dependencies)
        return TaskProcess(
            command=argv,
            working_dir=None,
            env=env,
            timeout_seconds=spec.timeout_seconds,
        )
