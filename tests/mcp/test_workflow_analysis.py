#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
"""Analysis workflow-start contracts."""

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.analysis import AnalysisCreateRequest
from kitaru.api_models.v1.job import JobKind, JobResponse, JobStatus
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.common import WorkflowStartResult
from kitaru.mcp.models.workflows import AnalysisStart
from kitaru.mcp.settings import MCPSettings
from kitaru.mcp.tools.workflow_start import handle_workflow_start


def _job() -> JobResponse:
    now = datetime(2026, 10, 30, tzinfo=UTC)
    return JobResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        created=now,
        updated=now,
        kind=JobKind.ANALYSIS,
        status=JobStatus.PENDING,
    )


def test_analysis_start_requires_an_analyzer() -> None:
    with pytest.raises(ValidationError):
        AnalysisStart(operation="analysis", agent_id=uuid.uuid4(), analyzers=[])


async def test_analysis_start_forwards_the_filter_and_returns_a_typed_receipt() -> None:
    calls: list[tuple[AnalysisCreateRequest, str | None]] = []
    job = _job()

    async def create(
        request: AnalysisCreateRequest, idempotency_key: str | None = None
    ) -> JobResponse:
        calls.append((request, idempotency_key))
        return job

    client = SimpleNamespace(analyses=SimpleNamespace(create=create))
    state = MCPServerState(MCPSettings(), cast(Any, client))
    agent_id = uuid.uuid4()
    session_filter = {
        "and": [
            {"field": "tag", "op": "eq", "value": "refunds"},
            {"field": "started_at", "op": "ge", "value": "2026-10-23T00:00:00Z"},
        ]
    }

    receipt = await handle_workflow_start(
        state,
        AnalysisStart.model_validate(
            {
                "operation": "analysis",
                "agent_id": str(agent_id),
                "analyzers": [{"analyzer": "trends", "params": {"k": 3}}],
                "filter": session_filter,
                "idempotency_key": "analysis-1",
            }
        ),
    )

    [(request, idempotency_key)] = calls
    assert idempotency_key == "analysis-1"
    assert request.agent_id == agent_id
    assert request.filter is not None
    assert request.filter.model_dump(mode="json", by_alias=True) == session_filter
    assert request.analyzers[0].params == {"k": 3}
    result = WorkflowStartResult.model_validate({"ok": True, "data": receipt})
    assert result.data is not None
    assert result.data.operation == "analysis"
    assert cast(dict[str, Any], receipt)["filter"] == session_filter
