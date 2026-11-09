#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Advisory worker coverage reads and explanations."""

import asyncio
from json import JSONDecodeError
from types import SimpleNamespace

import httpx
import pytest

from kitaru.api_models.v1.task import TaskDiagnosticsResponse
from kitaru.cli.task_diagnostics import get_task_diagnostics
from kitaru.task_diagnostics import get_task_diagnostic_warnings


async def test_unavailable_diagnostics_do_not_fail_primary_operation() -> None:
    class Tasks:
        async def diagnostics(self, **kwargs) -> TaskDiagnosticsResponse:
            raise httpx.ConnectError("unavailable")

    result, warnings = await get_task_diagnostics(SimpleNamespace(tasks=Tasks()))
    assert result is None
    assert "unavailable" in warnings[0]


async def test_invalid_json_diagnostics_do_not_fail_primary_operation() -> None:
    class Tasks:
        async def diagnostics(self, **kwargs) -> TaskDiagnosticsResponse:
            raise JSONDecodeError("Expecting value", "not-json", 0)

    result, warnings = await get_task_diagnostics(SimpleNamespace(tasks=Tasks()))
    assert result is None
    assert "unavailable" in warnings[0]


async def test_diagnostic_read_respects_remaining_wait_budget() -> None:
    class Tasks:
        async def diagnostics(self, **kwargs) -> TaskDiagnosticsResponse:
            await asyncio.sleep(10)
            raise AssertionError("deadline did not cancel diagnostic")

    result, warnings = await get_task_diagnostics(
        SimpleNamespace(tasks=Tasks()), timeout=0.001
    )
    assert result is None
    assert warnings


@pytest.mark.parametrize(
    "reason",
    [
        "no_live_workers",
        "claim_mismatch",
        "job_mismatch",
        "selector_mismatch",
        "credential_mismatch",
    ],
)
def test_uncovered_work_explains_requirements(reason: str) -> None:
    diagnostics = TaskDiagnosticsResponse.model_validate(
        {
            "live_worker_count": 1,
            "pending_task_count": 2,
            "truncated": False,
            "groups": [
                {
                    "kind": "analyzer",
                    "labels": {"kitaru/requires-credentials": "openai"},
                    "reasons": [reason],
                    "task_count": 2,
                    "task_ids": [],
                    "job_ids": [],
                }
            ],
        }
    )
    [warning] = get_task_diagnostic_warnings(diagnostics)
    assert "2 analyzer task(s) pending" in warning
    assert "--claim analyzer" in warning
    if reason == "credential_mismatch":
        assert "openai" in warning


def test_compatible_pending_work_is_not_misreported_as_missing_worker() -> None:
    diagnostics = TaskDiagnosticsResponse.model_validate(
        {
            "live_worker_count": 1,
            "pending_task_count": 1,
            "truncated": False,
            "groups": [
                {
                    "kind": "agent",
                    "labels": {},
                    "reasons": [],
                    "task_count": 1,
                    "task_ids": [],
                    "job_ids": [],
                }
            ],
        }
    )
    assert get_task_diagnostic_warnings(diagnostics) == []


@pytest.mark.parametrize("invalid_response", ["html", "malformed"])
async def test_invalid_diagnostic_response_is_advisory(invalid_response: str) -> None:
    from kitaru.client.exceptions import InvalidServerResponseError

    class Tasks:
        async def diagnostics(self, **kwargs) -> TaskDiagnosticsResponse:
            if invalid_response == "html":
                raise InvalidServerResponseError("Expected JSON, got HTML")
            return TaskDiagnosticsResponse.model_validate({"unexpected": "response"})

    result, warnings = await get_task_diagnostics(SimpleNamespace(tasks=Tasks()))
    assert result is None
    assert "unavailable" in warnings[0]
