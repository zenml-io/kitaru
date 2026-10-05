#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Session import handler."""

import uuid

from kitaru.api_models.v1.base import JsonValue
from kitaru.api_models.v1.imports import (
    BlobImportSource,
    ImportCreateRequest,
)
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.common import SessionImportReceipt
from kitaru.mcp.models.workflows import SessionImportRequest


async def handle_session_import(
    state: MCPServerState, request: SessionImportRequest
) -> SessionImportReceipt:
    """Start one import and return immediately without polling."""
    blob_id: uuid.UUID | None = None
    query: dict[str, JsonValue] | None = None
    if isinstance(request.source, BlobImportSource):
        blob = await state.client.blobs.get(request.source.blob_id)
        blob_id = blob.id
    else:
        query = request.source.query.model_dump(mode="json", exclude_unset=True)
    importer_version = await state.client.importers.get_version(
        request.importer_id, request.importer_version
    )
    importer = await state.client.importers.get(request.importer_id)
    agent_version = await state.client.agent_versions.get(request.agent_version_id)
    dto = ImportCreateRequest(
        importer=importer.name,
        version=importer_version.version,
        agent_id=agent_version.agent_id,
        agent_version_id=agent_version.id,
        source=request.source,
        params=request.params,
        evaluators=request.evaluators,
        analyzers=request.analyzers,
        max_sessions=request.max_sessions,
    )
    created_import = await state.client.imports.create(
        dto, idempotency_key=request.idempotency_key
    )
    if created_import.job_id is None:
        raise MCPToolError("internal_error", "Import was created without a job id.")
    job = await state.client.jobs.get(created_import.job_id)
    return SessionImportReceipt(
        operation="session_import",
        idempotency="domain-deduplicated-only",
        blob_id=blob_id,
        query=query,
        importer_id=importer.id,
        importer_version_id=importer_version.id,
        agent_id=agent_version.agent_id,
        agent_version_id=agent_version.id,
        import_id=created_import.id,
        result=job,
    )
