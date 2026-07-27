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
"""Blob routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, Response, UploadFile, status

from kitaru.api_models.v1.blobs import BlobResponse
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_blob_service,
)
from kitaru.server.adapters.rest.mapping.blobs import blob_to_response
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.blob_service import BlobService
from kitaru.server.domain.blob import DEFAULT_MEDIA_TYPE

router = APIRouter()


@router.post("", status_code=status.HTTP_201_CREATED)
async def upload_blob(
    response: Response,
    file: Annotated[UploadFile, File()],
    service: Annotated[BlobService, Depends(get_blob_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> BlobResponse:
    """Upload a blob, deduplicating by content hash.

    Content already stored returns the stored blob, so clients observe
    HTTP 200 instead of 201 on success, and 422 when the content exceeds
    the size limit.

    Args:
        response: Outgoing response.
        file: Uploaded content.
        service: Blob service.
        actor: Caller context.

    Returns:
        Stored blob.
    """
    content = await file.read()
    blob, created = await service.upload_blob(
        content=content,
        media_type=file.content_type or DEFAULT_MEDIA_TYPE,
        actor=actor,
    )
    if not created:
        response.status_code = status.HTTP_200_OK
    return blob_to_response(blob)


@router.get("/{blob_id}/content", response_class=Response)
async def download_blob(
    blob_id: uuid.UUID,
    service: Annotated[BlobService, Depends(get_blob_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> Response:
    """Download the content of a blob.

    Clients observe HTTP 200 on success and 404 when no blob has this id.

    Args:
        blob_id: Id of the blob.
        service: Blob service.
        actor: Caller context.

    Returns:
        Blob content under its stored media type.
    """
    blob, content = await service.download_blob(blob_id, actor=actor)
    return Response(content=content, media_type=blob.media_type)
