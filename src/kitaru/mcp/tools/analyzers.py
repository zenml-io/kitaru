#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Analyzer parent and version management handler."""

from kitaru.api_models.v1.analyzer import (
    AnalyzerCreateRequest,
    AnalyzerUpdateRequest,
    AnalyzerVersionCreateRequest,
    AnalyzerVersionUpdateRequest,
)
from kitaru.api_models.v1.plugin import ScriptPluginSource
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.analyzers import (
    AnalyzerCreate,
    AnalyzersManageRequest,
    AnalyzerUpdate,
    AnalyzerVersionCreate,
)


async def handle_analyzers_manage(
    state: MCPServerState, request: AnalyzersManageRequest
) -> object:
    """Perform one analyzer parent or version mutation."""
    if isinstance(request, AnalyzerCreate):
        return await state.client.analyzers.create(
            AnalyzerCreateRequest(
                name=request.name,
                description=request.description,
                metadata=request.metadata,
            ),
            idempotency_key=request.idempotency_key,
        )
    if isinstance(request, AnalyzerUpdate):
        values = request.model_dump(
            include={"description", "metadata"}, exclude_unset=True
        )
        if request.clear_description:
            values["description"] = None
        return await state.client.analyzers.update(
            request.analyzer_id, AnalyzerUpdateRequest.model_validate(values)
        )
    if isinstance(request, AnalyzerVersionCreate):
        source = request.source
        if isinstance(source, ScriptPluginSource):
            blob = await state.client.blobs.get(source.blob_id)
            if blob.id != source.blob_id:
                raise MCPToolError(
                    "conflict", "Script source resolved to a different blob."
                )
        return await state.client.analyzers.create_version(
            request.analyzer_id,
            AnalyzerVersionCreateRequest(
                source=source, display_version=request.display_version
            ),
            idempotency_key=request.idempotency_key,
        )
    display_version = None if request.clear_display_version else request.display_version
    return await state.client.analyzers.update_version(
        request.analyzer_id,
        request.version,
        AnalyzerVersionUpdateRequest(display_version=display_version),
    )
