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
"""Task execution spec building."""

from kitaru.api_models.v1.task import TaskKind
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.interfaces.secret_repository import SecretRepository
from kitaru.server.application.models.task import TaskPolicy
from kitaru.server.application.services.agent_version_resolution import (
    resolve_runnable_agent_version,
)
from kitaru.server.domain.plugin import PluginVersion, ScriptPluginSource
from kitaru.server.domain.task import (
    AgentTask,
    AgentTaskDetails,
    EvaluationTask,
    EvaluationTaskDetails,
    ImportTask,
    ImportTaskDetails,
    PackagePluginSpec,
    PayloadSpec,
    PluginSpec,
    ScriptPluginSpec,
    Task,
    TaskRunSpec,
    TaskSpec,
)

AGENT_ID_ENV = "KITARU_AGENT_ID"
AGENT_VERSION_ID_ENV = "KITARU_AGENT_VERSION_ID"


class TaskSpecBuilder:
    """Task execution spec builder."""

    def __init__(
        self,
        agent_version_repository: AgentVersionRepository,
        plugin_repository: PluginRepository,
        blob_repository: BlobRepository,
        secret_repository: SecretRepository,
        policy: TaskPolicy,
    ) -> None:
        """Initialize the builder.

        Args:
            agent_version_repository: Agent version repository.
            plugin_repository: Plugin repository.
            blob_repository: Blob repository.
            secret_repository: Secret repository.
            policy: Task execution policy.
        """
        self._agent_versions = agent_version_repository
        self._plugins = plugin_repository
        self._blobs = blob_repository
        self._secrets = secret_repository
        self._policy = policy

    async def build_spec(self, task: Task) -> TaskSpec:
        """Build the execution spec of a task, dispatching on its kind.

        Args:
            task: Task to build the spec for.

        Raises:
            ValueError: The task is not one of the three known kinds.

        Returns:
            Execution spec.
        """
        if isinstance(task, AgentTask):
            return await self._agent_spec(task)
        if isinstance(task, EvaluationTask):
            return await self._evaluation_spec(task)
        if isinstance(task, ImportTask):
            return await self._import_spec(task)
        raise ValueError(f"Task {task.id} has no spec builder")

    async def _agent_spec(self, task: AgentTask) -> TaskSpec:
        """Build the execution spec of an agent task.

        Args:
            task: Agent task.

        Raises:
            AgentVersionNotFound: The task names an unknown agent version.
            AgentVersionWithoutRunSpec: The agent version carries no run spec.
            SecretNotFound: The run spec names an unknown secret.

        Returns:
            Execution spec.
        """
        agent_version = await resolve_runnable_agent_version(
            task.agent_version_id, self._agent_versions
        )
        run_spec = agent_version.run_spec
        assert run_spec is not None
        secret_env: dict[str, str] = {}
        for secret_id in run_spec.secret_ids:
            secret = await self._secrets.get(secret_id)
            for key, value in secret.values.items():
                secret_env[key] = value.get_secret_value()
        return TaskSpec(
            task_id=task.id,
            kind=TaskKind.AGENT,
            timeout_seconds=run_spec.timeout_seconds,
            run_spec=TaskRunSpec(
                command=run_spec.command,
                working_dir=run_spec.working_dir,
                env=run_spec.env,
            ),
            env={
                **task.env,
                AGENT_ID_ENV: str(agent_version.agent_id),
                AGENT_VERSION_ID_ENV: str(agent_version.id),
            },
            secret_env=secret_env,
            details=AgentTaskDetails(inputs=task.inputs),
        )

    async def _evaluation_spec(self, task: EvaluationTask) -> TaskSpec:
        """Build the execution spec of an evaluator task.

        Args:
            task: Evaluation task.

        Raises:
            PluginVersionIdNotFound: The task names an unknown plugin version.
            BlobNotFound: The script plugin names an unknown blob.

        Returns:
            Execution spec.
        """
        plugin_version = await self._plugins.get_version_by_id(task.plugin_version_id)
        plugin = await self._plugins.get(plugin_version.plugin_id)
        return TaskSpec(
            task_id=task.id,
            kind=TaskKind.EVALUATOR,
            timeout_seconds=self._policy.evaluator_timeout_seconds,
            env=task.env,
            details=EvaluationTaskDetails(
                evaluator_name=plugin.name,
                params=task.params,
                plugin=await self._plugin_spec(plugin_version),
                input_session_id=task.input_session_id,
            ),
        )

    async def _import_spec(self, task: ImportTask) -> TaskSpec:
        """Build the execution spec of an importer task.

        Args:
            task: Import task.

        Raises:
            PluginVersionIdNotFound: The task names an unknown plugin version.
            BlobNotFound: The script plugin or the payload names an unknown
                blob.

        Returns:
            Execution spec.
        """
        plugin_version = await self._plugins.get_version_by_id(task.plugin_version_id)
        plugin = await self._plugins.get(plugin_version.plugin_id)
        payload = await self._blobs.get_metadata(task.payload_blob_id)
        return TaskSpec(
            task_id=task.id,
            kind=TaskKind.IMPORTER,
            timeout_seconds=self._policy.importer_timeout_seconds,
            env=task.env,
            details=ImportTaskDetails(
                plugin=await self._plugin_spec(plugin_version),
                payload=PayloadSpec(blob_id=payload.id, sha256=payload.sha256),
                provider=plugin.provider,
                agent_id=task.agent_id,
                params=task.params,
            ),
        )

    async def _plugin_spec(self, plugin_version: PluginVersion) -> PluginSpec:
        """Convert a plugin version's code source into its spec form.

        Args:
            plugin_version: Plugin version holding the code source.

        Raises:
            BlobNotFound: The script source names an unknown blob.

        Returns:
            Plugin spec the task process loads its code from.
        """
        source = plugin_version.source
        if isinstance(source, ScriptPluginSource):
            blob = await self._blobs.get_metadata(source.blob_id)
            return ScriptPluginSpec(
                entrypoint=source.entrypoint, blob_id=blob.id, sha256=blob.sha256
            )
        return PackagePluginSpec(
            entrypoint=source.entrypoint, requirement=source.requirement
        )
