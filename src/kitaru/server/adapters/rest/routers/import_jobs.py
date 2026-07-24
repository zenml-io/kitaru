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
"""Trace importer and import job routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, UploadFile, status

from kitaru.api_models.v1.import_jobs import ImporterResponse, ImportJobResponse
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_import_job_service,
)
from kitaru.server.adapters.rest.mapping.import_jobs import (
    import_job_to_response,
    importer_to_response,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.import_job_service import ImportJobService

router = APIRouter()


@router.get("/importers")
async def list_importers(
    service: Annotated[ImportJobService, Depends(get_import_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> list[ImporterResponse]:
    """List trace importers available in this deployment."""
    _ = actor
    return [importer_to_response(item) for item in service.list_importers()]


@router.post(
    "/import-jobs",
    status_code=status.HTTP_201_CREATED,
)
async def create_import_job(
    service: Annotated[ImportJobService, Depends(get_import_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
    file: Annotated[UploadFile, File()],
    importer_id: Annotated[str, Form()],
    agent_version_id: Annotated[uuid.UUID, Form()],
    source_instance: Annotated[str | None, Form()] = None,
) -> ImportJobResponse:
    """Upload a JSONL export and create a background import job."""
    upload_limit = service.get_importer(importer_id).max_upload_bytes
    content = await file.read(upload_limit + 1)
    job = await service.create_job(
        importer_id=importer_id,
        agent_version_id=agent_version_id,
        source_instance=source_instance,
        filename=file.filename or "upload.jsonl",
        content=content,
        actor=actor,
    )
    return import_job_to_response(job)


@router.get("/import-jobs/{job_id}")
async def get_import_job(
    job_id: uuid.UUID,
    service: Annotated[ImportJobService, Depends(get_import_job_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> ImportJobResponse:
    """Get a trace import job."""
    job = await service.get_job(job_id, actor=actor)
    return import_job_to_response(job)
