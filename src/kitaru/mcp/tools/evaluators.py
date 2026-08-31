#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Evaluator parent and version management handler."""

from kitaru.api_models.v1.evaluator import (
    EvaluatorCreateRequest,
    EvaluatorUpdateRequest,
    EvaluatorVersionCreateRequest,
    EvaluatorVersionUpdateRequest,
)
from kitaru.api_models.v1.plugin import ScriptPluginSource
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.evaluators import (
    EvaluatorCreate,
    EvaluatorsManageRequest,
    EvaluatorUpdate,
    EvaluatorVersionCreate,
)


async def handle_evaluators_manage(
    state: MCPServerState, request: EvaluatorsManageRequest
) -> object:
    """Perform one evaluator parent or version mutation."""
    if isinstance(request, EvaluatorCreate):
        return await state.client.evaluators.create(
            EvaluatorCreateRequest(
                name=request.name,
                description=request.description,
                metadata=request.metadata,
            ),
            idempotency_key=request.idempotency_key,
        )
    if isinstance(request, EvaluatorUpdate):
        values = request.model_dump(
            include={"description", "metadata"}, exclude_unset=True
        )
        if request.clear_description:
            values["description"] = None
        return await state.client.evaluators.update(
            request.evaluator_id, EvaluatorUpdateRequest.model_validate(values)
        )
    if isinstance(request, EvaluatorVersionCreate):
        source = request.source
        if isinstance(source, ScriptPluginSource):
            blob = await state.client.blobs.get(source.blob_id)
            if blob.id != source.blob_id:
                raise MCPToolError(
                    "conflict", "Script source resolved to a different blob."
                )
        return await state.client.evaluators.create_version(
            request.evaluator_id,
            EvaluatorVersionCreateRequest(
                source=source, display_version=request.display_version
            ),
            idempotency_key=request.idempotency_key,
        )
    display_version = None if request.clear_display_version else request.display_version
    return await state.client.evaluators.update_version(
        request.evaluator_id,
        request.version,
        EvaluatorVersionUpdateRequest(display_version=display_version),
    )
