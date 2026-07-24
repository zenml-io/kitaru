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
"""Import job DTO conversions."""

from kitaru.api_models.v1.import_jobs import (
    ImporterResponse,
    ImportJobErrorResponse,
    ImportJobResponse,
    ImportJobStatus,
)
from kitaru.server.application.models.import_jobs import ImporterDescriptor
from kitaru.server.domain.import_job import ImportJob


def importer_to_response(importer: ImporterDescriptor) -> ImporterResponse:
    """Convert an importer description."""
    return ImporterResponse(
        id=importer.id,
        display_name=importer.display_name,
        version=importer.version,
        file_extensions=importer.file_extensions,
        max_upload_bytes=importer.max_upload_bytes,
    )


def import_job_to_response(job: ImportJob) -> ImportJobResponse:
    """Convert an import job entity."""
    assert job.created is not None
    assert job.updated is not None
    return ImportJobResponse(
        id=job.id,
        owner_id=job.owner_id,
        agent_version_id=job.agent_version_id,
        importer_id=job.importer_id,
        importer_version=job.importer_version,
        source_instance=job.source_instance,
        filename=job.filename,
        status=ImportJobStatus(job.status.value),
        started_at=job.started_at,
        ended_at=job.ended_at,
        source_session_count=job.source_session_count,
        imported_count=job.imported_count,
        deduplicated_count=job.deduplicated_count,
        failed_count=job.failed_count,
        session_ids=job.session_ids,
        errors=[
            ImportJobErrorResponse(
                source_id=error.source_id,
                message=error.message,
            )
            for error in job.errors
        ],
        error=job.error,
        created=job.created,
        updated=job.updated,
    )
