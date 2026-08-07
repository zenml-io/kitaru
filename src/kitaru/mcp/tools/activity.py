#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Bounded activity-read handler."""

from kitaru.api_models.v1.evaluation import EvaluationListParams
from kitaru.api_models.v1.experiment_run import (
    ExperimentRunJobsListParams,
    ExperimentRunListParams,
)
from kitaru.api_models.v1.job import JobListParams, JobTasksListParams
from kitaru.api_models.v1.replay import ReplayListParams
from kitaru.api_models.v1.session import SessionListParams
from kitaru.api_models.v1.session_node import SessionNodeListParams
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.activity import (
    ActivityChildrenRequest,
    ActivityGetRequest,
    ActivityListRequest,
    ActivityReadRequest,
    SessionNodesRequest,
)
from kitaru.mcp.models.common import ActivityItem, PageData
from kitaru.mcp.tools.registry import build_page_data


async def handle_activity_read(
    state: MCPServerState, request: ActivityReadRequest
) -> object:
    """Execute one exact or one-page activity read."""
    client = state.client
    if isinstance(request, ActivityGetRequest):
        if request.kind == "session":
            return await client.sessions.get(request.id)
        if request.kind == "replay":
            return await client.replays.get(request.id)
        if request.kind == "evaluation":
            return await client.evaluations.get(request.id)
        if request.kind == "experiment_run":
            return await client.experiment_runs.get(request.id)
        return await client.jobs.get(request.id)
    if isinstance(request, ActivityListRequest):
        common = request.model_dump(include={"cursor", "size", "sort", "filter"})
        if request.kind == "session":
            page = await client.sessions.list(SessionListParams.model_validate(common))
        elif request.kind == "replay":
            page = await client.replays.list(ReplayListParams.model_validate(common))
        elif request.kind == "evaluation":
            page = await client.evaluations.list(
                EvaluationListParams.model_validate(common)
            )
        elif request.kind == "experiment_run":
            page = await client.experiment_runs.list(
                ExperimentRunListParams.model_validate(common)
            )
        else:
            page = await client.jobs.list(JobListParams.model_validate(common))
        return build_page_data(page, request.size, PageData[ActivityItem])
    return await _get_children(state, request)


async def _get_children(
    state: MCPServerState, request: ActivityChildrenRequest
) -> object:
    if isinstance(request, SessionNodesRequest):
        page = await state.client.sessions.list_nodes(
            request.parent_id,
            SessionNodeListParams(
                cursor=request.cursor,
                size=request.size,
                include_payloads=request.include_payloads,
            ),
        )
    elif request.kind == "experiment_run_jobs":
        page = await state.client.experiment_runs.list_jobs(
            request.parent_id,
            ExperimentRunJobsListParams(
                cursor=request.cursor, size=request.size, sort=request.sort
            ),
        )
    else:
        page = await state.client.jobs.list_tasks(
            request.parent_id,
            JobTasksListParams(
                cursor=request.cursor, size=request.size, sort=request.sort
            ),
        )
    return build_page_data(page, request.size, PageData[ActivityItem])
