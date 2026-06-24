"""Tests for client-side cohort selection."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from kitaru._client._models import CheckpointCall, Execution, ExecutionStatus
from kitaru.cohort import (
    CohortQuery,
    coerce_exec_ids,
    cohort,
    execution_replay_at_status,
)
from kitaru.errors import KitaruUsageError


def _checkpoint(name: str, *, call_id: str | None = None) -> CheckpointCall:
    return CheckpointCall(
        call_id=call_id or f"call-{name}",
        name=name,
        status=ExecutionStatus.COMPLETED,
        started_at=None,
        ended_at=None,
        metadata={},
        original_call_id=None,
        parent_call_ids=[],
        failure=None,
        attempts=[],
        artifacts=[],
    )


def _execution(
    exec_id: str,
    *,
    original_exec_id: str | None = None,
    deployment_version: int | None = None,
    checkpoints: list[CheckpointCall] | None = None,
    started_at: datetime | None = None,
    cost: float | None = None,
) -> Execution:
    metadata: dict = {}
    if deployment_version is not None:
        metadata["kitaru_deployment"] = {"version": deployment_version}
    if cost is not None:
        metadata["llm_usage_summary_v1"] = {"display_cost_usd": cost}
    resolved_checkpoints = (
        [_checkpoint("lookup_policy_tool")] if checkpoints is None else checkpoints
    )
    return Execution(
        exec_id=exec_id,
        flow_id="flow-1",
        flow_name="support_copilot_flow",
        status=ExecutionStatus.COMPLETED,
        started_at=started_at,
        ended_at=started_at,
        stack_name=None,
        metadata=metadata,
        status_reason=None,
        failure=None,
        pending_wait=None,
        frozen_execution_spec=None,
        original_exec_id=original_exec_id,
        checkpoints=resolved_checkpoints,
        artifacts=[],
        _client=Mock(),
    )


def test_execution_replay_at_status_present_and_missing() -> None:
    execution = _execution("kr-1")
    assert (
        execution_replay_at_status(execution=execution, at="lookup_policy_tool")
        == "present"
    )
    assert execution_replay_at_status(execution=execution, at="missing") == "missing"


def test_coerce_exec_ids_from_cohort_result() -> None:
    from kitaru.cohort import CohortResult

    result = CohortResult(
        exec_ids=["kr-a", "kr-b"],
        flow="support_copilot_flow",
        at="lookup_policy_tool",
        deployment=None,
        deployment_version=None,
        order_by="-started_at",
        scanned=2,
        matched=2,
        partial=False,
        filtered={},
    )
    assert coerce_exec_ids(result) == ["kr-a", "kr-b"]
    assert coerce_exec_ids(["kr-x"]) == ["kr-x"]


def test_resolve_filters_originals_deployment_and_checkpoint() -> None:
    client = Mock()
    client.executions.list.return_value = [
        _execution("kr-replay", original_exec_id="kr-parent", deployment_version=3),
        _execution("kr-wrong-dep", deployment_version=2),
        _execution(
            "kr-missing-at",
            deployment_version=3,
            checkpoints=[_checkpoint("other_tool")],
        ),
        _execution(
            "kr-good",
            deployment_version=3,
            started_at=datetime(2026, 6, 1, tzinfo=UTC),
            cost=9.5,
        ),
        _execution(
            "kr-best",
            deployment_version=3,
            started_at=datetime(2026, 6, 2, tzinfo=UTC),
            cost=12.0,
        ),
    ]
    client.deployments.get.side_effect = AssertionError("should not resolve tag")

    result = cohort(
        flow="support_copilot_flow",
        at="lookup_policy_tool",
        deployment_version=3,
        order_by="-display_cost_usd",
        limit=2,
        client=client,
    ).resolve(max_scan=10)

    assert result.exec_ids == ["kr-best", "kr-good"]
    assert result.filtered["originals"] == 1
    assert result.filtered["deployment"] == 1
    assert result.filtered["checkpoint"] == 1


def test_resolve_hydrates_list_summaries_for_checkpoint_filter() -> None:
    client = Mock()
    summary = _execution("kr-good", checkpoints=[], cost=5.0)
    hydrated = _execution(
        "kr-good",
        checkpoints=[_checkpoint("lookup_policy_tool")],
        cost=5.0,
    )
    client.executions.list.return_value = [summary]
    client.executions.get.return_value = hydrated

    result = cohort(
        flow="support_copilot_flow",
        at="lookup_policy_tool",
        order_by="-display_cost_usd",
        limit=1,
        client=client,
    ).resolve(max_scan=5)

    assert result.exec_ids == ["kr-good"]
    client.executions.get.assert_called_once_with("kr-good")


def test_resolve_empty_cohort_raises_usage_error() -> None:
    client = Mock()
    client.executions.list.return_value = []
    query = cohort(
        flow="support_copilot_flow",
        at="lookup_policy_tool",
        client=client,
    )
    with pytest.raises(KitaruUsageError, match="matched 0 executions"):
        query.resolve(max_scan=5)


def test_resolve_rejects_both_deployment_pins() -> None:
    with pytest.raises(KitaruUsageError, match="not both"):
        CohortQuery(
            flow="support_copilot_flow",
            at="lookup_policy_tool",
            deployment="prod",
            deployment_version=2,
        ).resolve()
