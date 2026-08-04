#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Narrow destructive-mode handlers."""

from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.workflows import DeleteRequest, JobCancel, WorkflowCancelRequest


async def handle_workflow_cancel(
    state: MCPServerState, request: WorkflowCancelRequest
) -> object:
    """Request cancellation of one exact job or experiment run."""
    if isinstance(request, JobCancel):
        response = await state.client.jobs.cancel(request.id)
    else:
        response = await state.client.experiment_runs.cancel(request.id)
    return {
        "operation": request.operation,
        "id": str(request.id),
        "cancellation_requested": True,
        "result": response.model_dump(mode="json"),
    }


async def handle_delete(state: MCPServerState, request: DeleteRequest) -> object:
    """Delete one exact allowlisted resource."""
    if request.kind == "cohort":
        await state.client.cohorts.delete(request.id)
    elif request.kind == "cohort_version":
        await state.client.cohort_versions.delete(request.id)
    elif request.kind == "experiment":
        await state.client.experiments.delete(request.id)
    else:
        await state.client.experiment_runs.delete(request.id)
    return {"kind": request.kind, "id": str(request.id), "deleted": True}
