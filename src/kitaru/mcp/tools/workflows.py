#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Session import handler."""

from kitaru.api_models.v1.imports import ImportCreateRequest
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.workflows import SessionImportRequest


async def handle_session_import(
    state: MCPServerState, request: SessionImportRequest
) -> object:
    """Start one import and return immediately without polling."""
    blob = await state.client.blobs.get(request.payload_blob_id)
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
        payload_blob_id=blob.id,
        params=request.params,
    )
    job = await state.client.imports.create(dto)
    return {
        "operation": "session_import",
        "idempotency": "domain-deduplicated-only",
        "blob_id": str(blob.id),
        "importer_id": str(importer.id),
        "importer_version_id": str(importer_version.id),
        "agent_id": str(agent_version.agent_id),
        "agent_version_id": str(agent_version.id),
        "result": job.model_dump(mode="json"),
    }
