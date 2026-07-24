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
"""Background import job worker."""

import asyncio
import logging
import uuid

from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.import_job_repository import (
    SQLImportJobRepository,
)
from kitaru.server.adapters.db.repositories.session_node_repository import (
    SQLSessionNodeRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.application.interfaces.trace_importer import TraceImporterRegistry
from kitaru.server.application.models.import_jobs import ImportContext
from kitaru.server.application.services.import_job_service import (
    ImportedSessionService,
)
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.import_job import ImportJobError

logger = logging.getLogger(__name__)


class ImportWorker:
    """Poll and process database-backed import jobs."""

    def __init__(
        self,
        database: DatabaseService,
        registry: TraceImporterRegistry,
        poll_seconds: float,
    ) -> None:
        """Initialize the worker."""
        self._database = database
        self._registry = registry
        self._poll_seconds = poll_seconds
        self._worker_id = f"import-{uuid.uuid4()}"

    async def run(self) -> None:
        """Process jobs until canceled."""
        while True:
            processed = await self._process_next()
            if not processed:
                await asyncio.sleep(self._poll_seconds)

    async def _process_next(self) -> bool:
        """Claim and process one pending job."""
        async for session in self._database.get_async_session():
            repository = SQLImportJobRepository(session)
            job = await repository.claim_next(self._worker_id)
            if job is None:
                return False
            await session.commit()

        try:
            importer = self._registry.get(job.importer_id)
            assert job.content is not None
            normalized_import = await asyncio.to_thread(
                importer.parse,
                job.content,
                ImportContext(
                    agent_version_id=job.agent_version_id,
                    source_instance=job.source_instance,
                ),
            )
        except Exception as exc:
            logger.exception("Import job %s failed during parsing", job.id)
            await self._fail_job(job.id, str(exc))
            return True

        imported_count = 0
        deduplicated_count = 0
        session_ids: list[uuid.UUID] = []
        errors = [
            ImportJobError(source_id=error.source_id, message=error.message)
            for error in normalized_import.errors
        ]
        for normalized in normalized_import.sessions:
            try:
                async for session in self._database.get_async_session():
                    service = ImportedSessionService(
                        session_repository=SQLSessionRepository(session),
                        node_repository=SQLSessionNodeRepository(session),
                        agent_version_repository=SQLAgentVersionRepository(session),
                    )
                    stored, created = await service.import_session(job, normalized)
                    await session.commit()
                    session_ids.append(stored.id)
                    if created:
                        imported_count += 1
                    else:
                        deduplicated_count += 1
            except Exception as exc:
                logger.exception(
                    "Import job %s failed for source session %s",
                    job.id,
                    normalized.source_id,
                )
                errors.append(
                    ImportJobError(source_id=normalized.source_id, message=str(exc))
                )

        async for session in self._database.get_async_session():
            repository = SQLImportJobRepository(session)
            current = await repository.get(job.id)
            current.complete(
                source_session_count=(
                    len(normalized_import.sessions) + len(normalized_import.errors)
                ),
                imported_count=imported_count,
                deduplicated_count=deduplicated_count,
                session_ids=session_ids,
                errors=errors,
            )
            await repository.update(current)
            await session.commit()
        return True

    async def _fail_job(self, job_id: uuid.UUID, error: str) -> None:
        """Mark a job failed."""
        async for session in self._database.get_async_session():
            repository = SQLImportJobRepository(session)
            job = await repository.get(job_id)
            job.fail(error)
            await repository.update(job)
            await session.commit()
