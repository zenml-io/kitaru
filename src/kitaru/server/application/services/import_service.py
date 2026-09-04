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
"""Import use cases."""

import uuid

from kitaru.api_models.v1.job import JobKind
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.application.interfaces.import_repository import ImportRepository
from kitaru.server.application.interfaces.job_repository import JobRepository
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.imports import ImportCreate, ImportFilter
from kitaru.server.application.services.agent_version_resolution import resolve_agent_id
from kitaru.server.application.services.evaluator_resolution import validate_evaluators
from kitaru.server.application.services.plugin_resolution import (
    get_plugin_task_labels,
    resolve_plugin,
    resolve_plugin_version,
)
from kitaru.server.domain.imports import Import
from kitaru.server.domain.job import Job
from kitaru.server.domain.plugin import PluginKind
from kitaru.server.domain.task import ImportTask


class ImportService:
    """Import use cases."""

    def __init__(
        self,
        repository: ImportRepository,
        job_repository: JobRepository,
        task_repository: TaskRepository,
        agent_repository: AgentRepository,
        agent_version_repository: AgentVersionRepository,
        plugin_repository: PluginRepository,
        blob_repository: BlobRepository,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Import repository.
            job_repository: Job repository.
            task_repository: Task repository.
            agent_repository: Agent repository.
            agent_version_repository: Agent version repository.
            plugin_repository: Plugin repository, for importer and evaluator
                resolution.
            blob_repository: Blob repository, for the payload lookup.
        """
        self._repository = repository
        self._jobs = job_repository
        self._tasks = task_repository
        self._agents = agent_repository
        self._agent_versions = agent_version_repository
        self._plugins = plugin_repository
        self._blobs = blob_repository

    async def create_import(self, command: ImportCreate, actor: AuthContext) -> Import:
        """Create an import, its job, and the importer task running it.

        An omitted importer version resolves to the importer's latest. An
        agent version is stamped on every session the import creates, and
        the sessions carry none when the command names none.

        Args:
            command: Fields for the import.
            actor: Caller context.

        Raises:
            PluginNotFound: No importer has this name, or an evaluator config
                names an unknown evaluator.
            PluginVersionNotFound: The importer has no version with this
                number, or an evaluator config names an unknown version.
            BlobNotFound: No blob has the payload id.
            AgentNotFound: No agent has this id.
            AgentVersionNotFound: No agent version has this id.
            AgentVersionAgentMismatch: The agent version belongs to another
                agent.
            ValidationError: An evaluator config is scoped to another agent,
                or two configs resolve to the same evaluator version.

        Returns:
            Created import.
        """
        plugin = await resolve_plugin(
            command.importer, PluginKind.IMPORTER, self._plugins
        )
        plugin_version = await resolve_plugin_version(
            plugin, command.version, self._plugins
        )
        payload = await self._blobs.get(command.payload_blob_id)
        agent = await self._agents.get(command.agent_id)
        if command.agent_version_id is not None:
            await resolve_agent_id(
                command.agent_version_id, agent.id, self._agent_versions
            )
        evaluators = await validate_evaluators(
            command.evaluators, self._plugins, agent.id, actor
        )
        job = await self._jobs.create(
            Job(owner_id=actor.account.id, kind=JobKind.IMPORT)
        )
        import_ = await self._repository.create(
            Import(
                owner_id=actor.account.id,
                job_id=job.id,
                agent_id=agent.id,
                agent_version_id=command.agent_version_id,
                importer_version_id=plugin_version.id,
                payload_blob_id=payload.id,
                params=command.params,
                evaluators=evaluators,
            )
        )
        # The job was just created in this call and cannot have settled yet, so
        # the task skips add_task's settled check.
        await self._tasks.create(
            ImportTask(
                job_id=job.id,
                import_id=import_.id,
                labels=get_plugin_task_labels(plugin.name),
            )
        )
        return import_

    async def get_import(self, import_id: uuid.UUID, actor: AuthContext) -> Import:
        """Get an import by id.

        Args:
            import_id: Id of the import.
            actor: Caller context.

        Raises:
            ImportNotFound: No import has this id.

        Returns:
            Stored import.
        """
        _ = actor
        return await self._repository.get(import_id)

    async def list_imports(
        self, import_filter: ImportFilter, actor: AuthContext
    ) -> tuple[list[Import], str | None]:
        """List imports matching a filter.

        Args:
            import_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching imports and the next cursor.
        """
        _ = actor
        return await self._repository.query(import_filter)
