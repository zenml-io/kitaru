#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Cohort management handler."""

from kitaru.api_models.v1.cohort import CohortCreateRequest, CohortUpdateRequest
from kitaru.api_models.v1.cohort_version import (
    CohortVersionCreateRequest,
    CohortVersionUpdateRequest,
)
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.management import (
    CohortCreate,
    CohortsManageRequest,
    CohortUpdate,
    CohortVersionCreate,
)


async def handle_cohorts_manage(
    state: MCPServerState, request: CohortsManageRequest
) -> object:
    """Perform one exact-ID cohort mutation."""
    if isinstance(request, CohortCreate):
        dto = CohortCreateRequest(
            name=request.name,
            description=request.description,
            agent_id=request.agent_id,
            metadata=request.metadata,
        )
        return await state.client.cohorts.create(
            dto, idempotency_key=request.idempotency_key
        )
    if isinstance(request, CohortUpdate):
        values = request.model_dump(
            include={"name", "description", "metadata"}, exclude_unset=True
        )
        if request.clear_description:
            values["description"] = None
        return await state.client.cohorts.update(
            request.cohort_id, CohortUpdateRequest.model_validate(values)
        )
    if isinstance(request, CohortVersionCreate):
        dto = CohortVersionCreateRequest(
            baseline_id=request.baseline_id,
            add_session_ids=request.add_session_ids,
            remove_session_ids=request.remove_session_ids,
            display_version=request.display_version,
        )
        return await state.client.cohorts.create_version(
            request.cohort_id, dto, idempotency_key=request.idempotency_key
        )
    values = {
        "display_version": None
        if request.clear_display_version
        else request.display_version
    }
    return await state.client.cohort_versions.update(
        request.version_id, CohortVersionUpdateRequest.model_validate(values)
    )
