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


def test_analysis_start_rejects_duplicate_ids_and_naive_timestamps() -> None:
    agent_id = uuid.uuid4()
    analyzers = [{"analyzer": "kitaru/openai-post-import-insights"}]
    session_id = uuid.uuid4()
    with pytest.raises(ValidationError, match="unique"):
        AnalysisStart(
            operation="analysis",
            agent_id=agent_id,
            analyzers=analyzers,
            session_ids=[session_id, session_id],
        )
    with pytest.raises(ValidationError, match="timezone"):
        AnalysisStart(
            operation="analysis",
            agent_id=agent_id,
            analyzers=analyzers,
            started_after=datetime.fromisoformat("2026-10-01T00:00:00"),
        )
    with pytest.raises(ValidationError):
        AnalysisStart(operation="analysis", agent_id=agent_id, analyzers=[])


async def test_analysis_start_forwards_scope_and_returns_a_typed_receipt() -> None:
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
    cohort_version_id = uuid.uuid4()
    started_after = datetime(2026, 10, 23, tzinfo=UTC)

    receipt = await handle_workflow_start(
        state,
        AnalysisStart(
            operation="analysis",
            agent_id=agent_id,
            analyzers=[{"analyzer": "trends", "params": {"k": 3}}],
            tag="refunds",
            cohort_version_id=cohort_version_id,
            started_after=started_after,
            idempotency_key="analysis-1",
        ),
    )

    [(request, idempotency_key)] = calls
    assert idempotency_key == "analysis-1"
    assert request.agent_id == agent_id
    assert request.tag == "refunds"
    assert request.cohort_version_id == cohort_version_id
    assert request.started_after == started_after
    assert request.session_ids is None
    assert request.analyzers[0].params == {"k": 3}
    result = WorkflowStartResult.model_validate({"ok": True, "data": receipt})
    assert result.data is not None
    assert result.data.operation == "analysis"
    assert cast(dict[str, Any], receipt)["scope"] == {
        "tag": "refunds",
        "cohort_version_id": str(cohort_version_id),
        "started_after": started_after.isoformat().replace("+00:00", "Z"),
    }
