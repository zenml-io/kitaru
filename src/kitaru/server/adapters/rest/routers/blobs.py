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
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Depends, File, Response, UploadFile, status

from kitaru.api_models.v1.blob import BlobResponse
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    authorize_with_task,
    authorize_with_worker_or_task,
    get_blob_service,
)
from kitaru.server.adapters.rest.mapping.blobs import blob_to_response
from kitaru.server.adapters.rest.route import KitaruAPIRoute
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.blob_service import BlobService

router = APIRouter(route_class=KitaruAPIRoute)

_UPLOAD_CHUNK_BYTES = 1024 * 1024


async def _iter_upload(upload: UploadFile) -> AsyncIterator[bytes]:
    """Read an upload in fixed-size chunks.

    Args:
        upload: Uploaded file.

    Yields:
        Chunks of the upload content.
    """
    while True:
        chunk = await upload.read(_UPLOAD_CHUNK_BYTES)
        if not chunk:
            break
        yield chunk


@router.post("")
async def upload_blob(
    response: Response,
    file: Annotated[UploadFile, File()],
    service: Annotated[BlobService, Depends(get_blob_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
) -> BlobResponse:
    """Upload a blob, deduping identical content by sha256.

    Clients observe HTTP 201 for a new blob, 200 on a dedup hit, and 413
    when the upload exceeds the size cap.

    Args:
        response: Response, its status code is set to 201 or 200.
        file: Uploaded file.
        service: Blob service.
        actor: Caller context.

    Returns:
        Stored blob metadata.
    """
    media_type = file.content_type or "application/octet-stream"
    blob, created = await service.upload_blob(
        _iter_upload(file), media_type=media_type, actor=actor
    )
    response.status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
    return blob_to_response(blob)


@router.get("/{blob_id}")
async def get_blob(
    blob_id: uuid.UUID,
    service: Annotated[BlobService, Depends(get_blob_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_task)],
) -> BlobResponse:
    """Get a blob's metadata by id.

    Clients observe HTTP 200 on success and 404 when no blob has this id.

    Args:
        blob_id: Id of the blob.
        service: Blob service.
        actor: Caller context.

    Returns:
        Stored blob metadata.
    """
    blob = await service.get_blob(blob_id, actor=actor)
    return blob_to_response(blob)


@router.get("/{blob_id}/content")
async def download_blob(
    blob_id: uuid.UUID,
    service: Annotated[BlobService, Depends(get_blob_service)],
    actor: Annotated[AuthContext, Depends(authorize_with_worker_or_task)],
) -> Response:
    """Download a blob's raw content.

    Clients observe HTTP 200 with the blob's media type and 404 when no
    blob has this id. The response never renders inline: it carries
    ``Content-Disposition: attachment`` and ``X-Content-Type-Options:
    nosniff``.

    Args:
        blob_id: Id of the blob.
        service: Blob service.
        actor: Caller context.

    Returns:
        Raw blob content.
    """
    blob = await service.download_blob(blob_id, actor=actor)
    return Response(
        content=blob.data,
        media_type=blob.media_type,
        headers={
            "Content-Disposition": "attachment",
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.delete("/{blob_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_blob(
    blob_id: uuid.UUID,
    service: Annotated[BlobService, Depends(get_blob_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a blob.

    Clients observe HTTP 204 on success, 404 when no blob has this id, and
    409 when the blob is referenced by a plugin version.

    Args:
        blob_id: Id of the blob.
        service: Blob service.
        actor: Caller context.
    """
    await service.delete_blob(blob_id, actor=actor)
