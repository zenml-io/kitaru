#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Persisted agent-session cleanup through task reports and recovery."""

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from conftest import db_settings, lifespan_client
from kitaru.analytics.client import AnalyticsClient
from kitaru.server.adapters.rest.dependencies import (
    get_server_analytics,
    get_task_service,
)
from kitaru.server.database.service import DatabaseService


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    async with lifespan_client(db_settings(TASK_SWEEP_INTERVAL_SECONDS=0)) as client:
        yield client


async def _start_replay(
    client: httpx.AsyncClient,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, str]]:
    agent = (await client.post("/api/v1/agents", json={"name": "cleanup"})).json()
    version = (
        await client.post(
            f"/api/v1/agents/{agent['id']}/versions",
            json={"run_spec": {"command": "run.sh", "timeout_seconds": 60}},
        )
    ).json()
    baseline = (
        await client.post(
            "/api/v1/sessions",
            json={
                "agent_version_id": version["id"],
                "origin": "recorded",
                "status": "completed",
                "inputs": {"ticket": 1},
                "outputs": {"answer": "original"},
            },
        )
    ).json()
    blob = (
        await client.post(
            "/api/v1/blobs",
            files={"file": ("score.py", b"def score(): pass", "text/plain")},
        )
    ).json()
    evaluator = (
        await client.post("/api/v1/evaluators", json={"name": "cleanup-score"})
    ).json()
    registered = await client.post(
        f"/api/v1/evaluators/{evaluator['id']}/versions",
        json={
            "source": {"type": "script", "blob_id": blob["id"], "entrypoint": "score"}
        },
    )
    assert registered.status_code == 201, registered.text
    replay = (
        await client.post(
            "/api/v1/replays",
            json={
                "baseline_session_id": baseline["id"],
                "evaluators": [{"evaluator": "cleanup-score"}],
                "baseline_evaluation_mode": "none",
            },
        )
    ).json()
    assert "id" in replay, replay
    worker = (
        await client.post(
            "/api/v1/workers",
            json={
                "name": "cleanup-worker",
                "scope": {"claims": [{"kind": "agent"}]},
                "runtime": {"platform": "bare"},
                "metadata": {},
            },
        )
    ).json()
    claimed = await client.post(
        "/api/v1/tasks/claim",
        json={"max_tasks": 1},
        headers={"Authorization": f"Bearer {worker['token']}"},
    )
    assert claimed.status_code == 200, claimed.text
    entry = claimed.json()["tasks"][0]
    task = entry["task"]
    headers = {"Authorization": f"Bearer {entry['token']}"}
    started = await client.patch(
        f"/api/v1/tasks/{task['id']}", json={"status": "running"}, headers=headers
    )
    assert started.status_code == 200, started.text
    result = await client.post(
        "/api/v1/sessions",
        json={
            "origin": "replay",
            "inputs": {"ticket": 1},
            "outputs": {"partial": "keep me"},
            "output_text_selector": "/partial",
        },
        headers=headers,
    )
    assert result.status_code == 201, result.text
    return replay, task, result.json(), headers


@pytest.mark.parametrize("status", ["failed", "timed_out", "canceled"])
async def test_unsuccessful_agent_report_closes_only_its_result_session(
    client: httpx.AsyncClient, status: str
) -> None:
    replay, task, result, headers = await _start_replay(client)
    response = await client.patch(
        f"/api/v1/tasks/{task['id']}",
        json={"status": status, "error": "agent stopped"},
        headers=headers,
    )
    assert response.status_code == 200, response.text
    terminal_task = response.json()
    session = (await client.get(f"/api/v1/sessions/{result['id']}")).json()
    assert session["status"] == "failed"
    assert session["ended_at"] == terminal_task["ended_at"]
    assert session["error"] == (
        "Agent task canceled." if status == "canceled" else "agent stopped"
    )
    assert session["inputs"] == {"ticket": 1}
    assert session["outputs"] == {"partial": "keep me"}
    assert session["output_text_selector"] == "/partial"
    baseline = (
        await client.get(f"/api/v1/sessions/{replay['baseline_session_id']}")
    ).json()
    assert baseline["status"] == "completed"
    assert baseline["outputs"] == {"answer": "original"}
    stored_replay = (await client.get(f"/api/v1/replays/{replay['id']}")).json()
    assert stored_replay["result_session_id"] == result["id"]
    assert stored_replay["status"] == ("canceled" if status == "canceled" else "failed")


@pytest.mark.parametrize("status", ["completed", "failed"])
async def test_late_task_failure_preserves_finalized_session(
    client: httpx.AsyncClient, status: str
) -> None:
    _, task, result, headers = await _start_replay(client)
    finalized = await client.patch(
        f"/api/v1/sessions/{result['id']}",
        json={
            "status": status,
            "error": "recorded error" if status == "failed" else None,
            "ended_at": "2026-01-01T00:00:00Z",
        },
    )
    assert finalized.status_code == 200, finalized.text
    before = (await client.get(f"/api/v1/sessions/{result['id']}")).json()
    response = await client.patch(
        f"/api/v1/tasks/{task['id']}",
        json={"status": "timed_out", "error": "executor timeout"},
        headers=headers,
    )
    assert response.status_code == 200, response.text
    stored = (await client.get(f"/api/v1/sessions/{result['id']}")).json()
    for field in (
        "status",
        "error",
        "ended_at",
        "inputs",
        "outputs",
        "output_text_selector",
    ):
        assert stored[field] == before[field]


@pytest.mark.parametrize("retry_limit", [1, 2])
async def test_stale_agent_session_is_closed_only_when_attempt_is_terminal(
    retry_limit: int,
) -> None:
    settings = db_settings(TASK_SWEEP_INTERVAL_SECONDS=0, TASK_RETRY_LIMIT=retry_limit)
    async with lifespan_client(settings) as client:
        replay, task, result, _ = await _start_replay(client)
        database = DatabaseService(settings)
        try:
            async for session in database.get_async_session():
                tracker = get_server_analytics(session, AnalyticsClient(enabled=False))
                service = get_task_service(session, database.engine, settings, tracker)
                await service.sweep_stale_task(
                    uuid.UUID(task["id"]), datetime.now(UTC) + timedelta(days=1)
                )
                await session.commit()
        finally:
            await database.cleanup()
        stored = (await client.get(f"/api/v1/sessions/{result['id']}")).json()
        stored_replay = (await client.get(f"/api/v1/replays/{replay['id']}")).json()
        if retry_limit == 1:
            assert stored["status"] == "failed"
            assert stored["error"]
            assert stored["ended_at"] is not None
            assert stored_replay["status"] == "failed"
        else:
            assert stored["status"] == "in_progress"
            assert stored["task_id"] is None
            assert stored["error"] is None
            assert stored["ended_at"] is None
            assert stored_replay["result_session_id"] is None
            assert stored_replay["status"] == "pending"
        assert stored["outputs"] == {"partial": "keep me"}
