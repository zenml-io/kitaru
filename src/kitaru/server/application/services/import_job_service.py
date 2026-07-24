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
"""Trace import job use cases."""

import uuid

from kitaru.hashing import tool_call_cache_key
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.import_job_repository import (
    ImportJobRepository,
)
from kitaru.server.application.interfaces.session_node_repository import (
    SessionNodeRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.interfaces.trace_importer import (
    TraceImporterRegistry,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.import_jobs import (
    ImporterDescriptor,
    NormalizedNode,
    NormalizedSession,
)
from kitaru.server.domain.agent_version import AgentVersionNotFound
from kitaru.server.domain.ids import uuid7
from kitaru.server.domain.import_job import (
    ImportJob,
    ImportJobNotFound,
    InvalidImport,
)
from kitaru.server.domain.session import Session, SessionOrigin
from kitaru.server.domain.session_node import (
    NodeType,
    SessionNode,
    build_node_key,
    compute_rollups,
)


class ImportJobService:
    """Create and inspect import jobs."""

    def __init__(
        self,
        repository: ImportJobRepository,
        agent_version_repository: AgentVersionRepository,
        registry: TraceImporterRegistry,
    ) -> None:
        """Initialize the service."""
        self._repository = repository
        self._agent_version_repository = agent_version_repository
        self._registry = registry

    def list_importers(self) -> list[ImporterDescriptor]:
        """List importers available in this deployment."""
        return self._registry.list()

    def get_importer(self, importer_id: str) -> ImporterDescriptor:
        """Get one available importer."""
        return self._registry.get(importer_id).descriptor

    async def create_job(
        self,
        importer_id: str,
        agent_version_id: uuid.UUID,
        source_instance: str | None,
        filename: str,
        content: bytes,
        actor: AuthContext,
    ) -> ImportJob:
        """Create a pending import job."""
        importer = self._registry.get(importer_id)
        if len(content) > importer.descriptor.max_upload_bytes:
            raise InvalidImport(
                f"Import file exceeds {importer.descriptor.max_upload_bytes} bytes"
            )
        if not content:
            raise InvalidImport("Import file is empty")
        if len(filename) > 255:
            raise InvalidImport("Import filename exceeds 255 characters")
        if source_instance is not None and len(source_instance) > 255:
            raise InvalidImport("Source instance exceeds 255 characters")
        version = await self._agent_version_repository.get(agent_version_id)
        if version.owner_id != actor.account.id:
            raise AgentVersionNotFound(agent_version_id)
        job = ImportJob(
            owner_id=actor.account.id,
            agent_version_id=agent_version_id,
            importer_id=importer_id,
            importer_version=importer.descriptor.version,
            source_instance=source_instance,
            filename=filename,
            content=content,
        )
        return await self._repository.create(job)

    async def get_job(self, job_id: uuid.UUID, actor: AuthContext) -> ImportJob:
        """Get one import job."""
        job = await self._repository.get(job_id)
        if job.owner_id != actor.account.id:
            raise ImportJobNotFound(job_id)
        return job


class ImportedSessionService:
    """Persist one normalized source session atomically."""

    def __init__(
        self,
        session_repository: SessionRepository,
        node_repository: SessionNodeRepository,
        agent_version_repository: AgentVersionRepository,
    ) -> None:
        """Initialize the service."""
        self._session_repository = session_repository
        self._node_repository = node_repository
        self._agent_version_repository = agent_version_repository

    def _build_nodes(
        self, session_id: uuid.UUID, normalized: list[NormalizedNode]
    ) -> list[SessionNode]:
        """Build stored nodes in parent-before-child order."""
        ids = {node.source_id: uuid7() for node in normalized}
        remaining = list(normalized)
        ordered: list[NormalizedNode] = []
        completed: set[str] = set()
        while remaining:
            ready = [
                node
                for node in remaining
                if node.parent_source_id is None or node.parent_source_id in completed
            ]
            if not ready:
                raise InvalidImport("Normalized session node graph contains a cycle")
            for node in ready:
                ordered.append(node)
                completed.add(node.source_id)
                remaining.remove(node)

        stored: list[SessionNode] = []
        sibling_counts: dict[tuple[uuid.UUID | None, NodeType, str], int] = {}
        keys: dict[str, str] = {}
        for sequence, node in enumerate(ordered):
            parent_id = ids[node.parent_source_id] if node.parent_source_id else None
            parent_key = (
                keys.get(node.parent_source_id) if node.parent_source_id else None
            )
            sibling = (parent_id, node.node_type, node.name)
            occurrence = sibling_counts.get(sibling, 0) + 1
            sibling_counts[sibling] = occurrence
            key = build_node_key(parent_key, node.node_type, node.name, occurrence)
            keys[node.source_id] = key
            cache_key = (
                tool_call_cache_key(node.tool_name, node.inputs)
                if node.node_type is NodeType.TOOL_CALL
                else None
            )
            stored.append(
                SessionNode(
                    id=ids[node.source_id],
                    session_id=session_id,
                    key=key,
                    parent_id=parent_id,
                    sequence=sequence,
                    external_id=node.source_id,
                    trace_id=node.trace_id,
                    node_type=node.node_type,
                    name=node.name,
                    status=node.status,
                    error=node.error,
                    started_at=node.started_at,
                    ended_at=node.ended_at,
                    inputs=node.inputs,
                    outputs=node.outputs,
                    requested_model=node.requested_model,
                    model=node.model,
                    provider=node.provider,
                    tokens=node.tokens,
                    cost=node.cost,
                    model_params=node.model_params,
                    tool_name=node.tool_name,
                    cache_key=cache_key,
                    attributes={
                        **node.attributes,
                        "source_metadata": node.source_metadata,
                    },
                )
            )
        return stored

    async def import_session(
        self,
        job: ImportJob,
        normalized: NormalizedSession,
    ) -> tuple[Session, bool]:
        """Create or deduplicate one imported session.

        Returns:
            Stored session and whether it was newly created.
        """
        existing = await self._session_repository.get_imported_by_digest(
            job.owner_id,
            job.importer_id,
            normalized.source_instance,
            normalized.source_id,
            normalized.content_digest,
        )
        if existing is not None:
            return existing, False

        version = await self._agent_version_repository.get(job.agent_version_id)
        if version.owner_id != job.owner_id:
            raise InvalidImport(
                "Import job agent version does not belong to the job owner"
            )
        latest = await self._session_repository.get_latest_import(
            job.owner_id,
            job.importer_id,
            normalized.source_instance,
            normalized.source_id,
        )
        session = Session(
            owner_id=job.owner_id,
            agent_id=version.agent_id,
            agent_version_id=version.id,
            origin=SessionOrigin.IMPORTED,
            status=normalized.status,
            name=normalized.name,
            inputs=normalized.inputs,
            outputs=normalized.outputs,
            error=normalized.error,
            started_at=normalized.started_at,
            ended_at=normalized.ended_at,
            external_id=normalized.source_id,
            provider=job.importer_id,
            source_instance=normalized.source_instance,
            source_revision=(latest.source_revision or 0) + 1 if latest else 1,
            source_digest=normalized.content_digest,
            source_metadata=normalized.source_metadata,
            replay_readiness=normalized.readiness.model_dump(mode="json"),
            normalization_warnings=normalized.warnings,
            import_job_id=job.id,
            supersedes_session_id=latest.id if latest else None,
        )
        nodes = self._build_nodes(session.id, normalized.nodes)
        session.set_import_rollups(compute_rollups(nodes))
        session = await self._session_repository.create(session)
        await self._node_repository.upsert(nodes)
        return session, True
