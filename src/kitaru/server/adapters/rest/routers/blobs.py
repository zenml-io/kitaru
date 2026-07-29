"""Blob routes."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, Response, UploadFile, status

from kitaru.api_models.v1.blob import BlobResponse
from kitaru.server.adapters.rest.commit_route import CommitRoute
from kitaru.server.adapters.rest.dependencies import authorize, get_blob_service
from kitaru.server.adapters.rest.mapping.blobs import blob_to_response
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.blob_service import BlobService

router = APIRouter(route_class=CommitRoute)


@router.post("", status_code=status.HTTP_201_CREATED)
async def upload_blob(
    file: Annotated[UploadFile, File()],
    response: Response,
    service: Annotated[BlobService, Depends(get_blob_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> BlobResponse:
    """Upload a blob; clients observe 200/201, 413, or 422."""
    blob, created = await service.upload_blob(
        file.file,
        media_type=file.content_type or "application/octet-stream",
        actor=actor,
    )
    response.status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
    return blob_to_response(blob)


@router.get("/{blob_id}")
async def get_blob(
    blob_id: uuid.UUID,
    service: Annotated[BlobService, Depends(get_blob_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> BlobResponse:
    """Get blob metadata; clients observe 200 or 404."""
    return blob_to_response(await service.get_blob(blob_id, actor=actor))


@router.get("/{blob_id}/content")
async def download_blob(
    blob_id: uuid.UUID,
    service: Annotated[BlobService, Depends(get_blob_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> Response:
    """Download blob content; clients observe 200 or 404."""
    blob = await service.download_blob(blob_id, actor=actor)
    assert blob.data is not None
    return Response(
        content=blob.data,
        media_type=blob.media_type,
        headers={
            "Content-Disposition": 'attachment; filename="blob"',
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.delete("/{blob_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_blob(
    blob_id: uuid.UUID,
    service: Annotated[BlobService, Depends(get_blob_service)],
    actor: Annotated[AuthContext, Depends(authorize)],
) -> None:
    """Delete a blob; clients observe 204, 404, or 409."""
    await service.delete_blob(blob_id, actor=actor)
