#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Worker coverage warnings preserve successful MCP activity reads."""

import json
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from mcp.types import CallToolResult, TextContent
from mcp_fakes import build_server_context

from kitaru.api_models.v1.info import AuthScheme, ServerInfoResponse
from kitaru.api_models.v1.job import JobKind, JobResponse, JobStatus
from kitaru.api_models.v1.task import (
    TaskDiagnosticResponse,
    TaskDiagnosticsResponse,
    TaskKind,
    TaskWaitReason,
)
from kitaru.client.exceptions import APIError, InvalidServerResponseError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.activity import ActivityGetRequest
from kitaru.mcp.models.common import ToolSuccessPayload
from kitaru.mcp.settings import MCPSettings
from kitaru.mcp.tools.activity import handle_activity_read


@pytest.mark.parametrize("covered", [False, True])
async def test_job_read_reports_analyzer_coverage(covered: bool) -> None:
    now = datetime.now(UTC)
    job = JobResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        kind=JobKind.IMPORT,
        status=JobStatus.RUNNING,
        created=now,
        updated=now,
    )

    async def get_job(job_id: uuid.UUID) -> JobResponse:
        assert job_id == job.id
        return job

    async def get_diagnostics(
        *, job_id: uuid.UUID | None, experiment_run_id: uuid.UUID | None
    ) -> TaskDiagnosticsResponse:
        assert job_id == job.id
        assert experiment_run_id is None
        return TaskDiagnosticsResponse(
            live_worker_count=1,
            pending_task_count=1,
            groups=[
                TaskDiagnosticResponse(
                    kind=TaskKind.ANALYZER,
                    agent_version_id=None,
                    labels={},
                    reasons=[] if covered else [TaskWaitReason.CLAIM_MISMATCH],
                    task_count=1,
                    task_ids=[uuid.uuid4()],
                    job_ids=[job.id],
                )
            ],
            truncated=False,
        )

    client = SimpleNamespace(
        jobs=SimpleNamespace(get=get_job),
        tasks=SimpleNamespace(diagnostics=get_diagnostics),
    )
    server, context = build_server_context(client)
    result = await server.call_tool(
        "kitaru_activity_read",
        {"request": {"operation": "get", "kind": "job", "id": str(job.id)}},
        context,
    )

    assert isinstance(result, CallToolResult)
    payload = result.structured_content
    assert payload is not None
    assert payload["ok"] is True
    assert payload["data"]["id"] == str(job.id)
    assert payload["data"]["status"] == "running"
    if covered:
        assert payload["warnings"] == []
    else:
        warning = " ".join(payload["warnings"])
        assert "analyzer" in warning
        assert "--claim analyzer" in warning
        assert "replace" in warning
    assert json.loads(cast(TextContent, result.content[0]).text) == payload


@pytest.mark.parametrize(
    "error",
    [
        APIError(404, "Endpoint unavailable"),
        InvalidServerResponseError("Unexpected HTML response"),
        json.JSONDecodeError("Invalid JSON response", "{", 1),
        TimeoutError(),
    ],
)
async def test_diagnostics_unavailable_preserves_job_read(error: Exception) -> None:
    now = datetime.now(UTC)
    job = JobResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        kind=JobKind.IMPORT,
        status=JobStatus.RUNNING,
        created=now,
        updated=now,
    )

    async def get_job(_job_id: uuid.UUID) -> JobResponse:
        return job

    async def get_diagnostics(**_kwargs: Any) -> TaskDiagnosticsResponse:
        raise error

    server, context = build_server_context(
        SimpleNamespace(
            jobs=SimpleNamespace(get=get_job),
            tasks=SimpleNamespace(diagnostics=get_diagnostics),
        )
    )
    result = await server.call_tool(
        "kitaru_activity_read",
        {"request": {"operation": "get", "kind": "job", "id": str(job.id)}},
        context,
    )
    assert isinstance(result, CallToolResult)
    payload = result.structured_content
    assert payload is not None
    assert payload["ok"] is True
    assert payload["data"]["id"] == str(job.id)
    assert payload["warnings"] == [
        "Worker coverage diagnostics are unavailable; the activity read succeeded."
    ]


async def test_experiment_run_get_links_to_selected_run() -> None:
    run_id = uuid.uuid4()
    experiment_id = uuid.uuid4()

    async def get_run(_id: uuid.UUID) -> object:
        assert _id == run_id
        return SimpleNamespace(
            id=run_id, experiment_id=experiment_id, number=3, status="running"
        )

    async def get_info() -> ServerInfoResponse:
        return ServerInfoResponse(
            version="0.0.0", auth_scheme=AuthScheme.LOCAL, ui_version="1.2.3"
        )

    async def get_diagnostics(
        *, job_id: uuid.UUID | None, experiment_run_id: uuid.UUID | None
    ) -> TaskDiagnosticsResponse:
        assert job_id is None
        assert experiment_run_id == run_id
        return TaskDiagnosticsResponse(
            live_worker_count=1, pending_task_count=0, groups=[], truncated=False
        )

    client = SimpleNamespace(
        base_url="https://api.example.com/base/",
        experiment_runs=SimpleNamespace(get=get_run),
        info=SimpleNamespace(get=get_info),
        tasks=SimpleNamespace(diagnostics=get_diagnostics),
    )
    result = await handle_activity_read(
        MCPServerState(MCPSettings(), cast(Any, client)),
        ActivityGetRequest(operation="get", kind="experiment_run", id=run_id),
    )

    assert isinstance(result, ToolSuccessPayload)
    assert result.links == {
        "inspect": f"https://api.example.com/base/experiments/{experiment_id}?run=3"
    }
