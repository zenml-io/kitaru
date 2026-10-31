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
"""Worker coverage as an import appends analyzer tasks on PostgreSQL."""

from typing import Any

import httpx

from conftest import db_settings, lifespan_client


async def _register_worker(client: httpx.AsyncClient, kind: str) -> str:
    response = await client.post(
        "/api/v1/workers",
        json={
            "name": f"{kind}-worker",
            "scope": {"claims": [{"kind": kind}]},
            "runtime": {"platform": "bare"},
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["token"]


async def _claim_task(client: httpx.AsyncClient, worker_token: str) -> dict[str, Any]:
    response = await client.post(
        "/api/v1/tasks/claim",
        json={"max_tasks": 1},
        headers={"Authorization": f"Bearer {worker_token}"},
    )
    assert response.status_code == 200, response.text
    (claimed,) = response.json()["tasks"]
    response = await client.patch(
        f"/api/v1/tasks/{claimed['task']['id']}",
        json={"status": "running"},
        headers={"Authorization": f"Bearer {claimed['token']}"},
    )
    assert response.status_code == 200, response.text
    return claimed


async def _complete_task(
    client: httpx.AsyncClient, claimed: dict[str, Any], result: Any
) -> None:
    response = await client.patch(
        f"/api/v1/tasks/{claimed['task']['id']}",
        json={"status": "completed", "result": result},
        headers={"Authorization": f"Bearer {claimed['token']}"},
    )
    assert response.status_code == 200, response.text


async def test_import_reports_uncovered_analyzer_until_matching_worker_starts() -> None:
    """Diagnose appended analysis without changing the import's job status."""
    async with lifespan_client(db_settings()) as client:
        response = await client.post("/api/v1/agents", json={"name": "import-agent"})
        assert response.status_code == 201, response.text
        agent_id = response.json()["id"]
        response = await client.post(
            "/api/v1/imports",
            json={
                "agent_id": agent_id,
                "importer": "kitaru/kitaru-jsonl",
                "source": {"type": "api", "query": {"trace_ids": []}},
                "analyzers": [
                    {"analyzer": "kitaru/post-import-insights", "min_sessions": 1}
                ],
            },
        )
        assert response.status_code == 201, response.text
        import_ = response.json()
        job_id = import_["job_id"]
        importer_token = await _register_worker(client, "importer")

        response = await client.get(
            "/api/v1/tasks/diagnostics", params={"job_id": job_id}
        )
        assert response.status_code == 200, response.text
        initial = response.json()
        assert initial["pending_task_count"] == 1
        assert initial["groups"][0]["kind"] == "importer"
        assert initial["groups"][0]["reasons"] == []

        imported = await _claim_task(client, importer_token)
        assert imported["task"]["kind"] == "importer"
        response = await client.post(
            "/api/v1/sessions",
            json={
                "origin": "imported",
                "status": "completed",
                "inputs": {"question": "hello"},
                "outputs": {"answer": "hello"},
            },
            headers={"Authorization": f"Bearer {imported['token']}"},
        )
        assert response.status_code == 201, response.text
        assert response.json()["import_id"] == import_["id"]
        await _complete_task(
            client, imported, {"created": 1, "skipped": 0, "failed": 0}
        )

        response = await client.get(f"/api/v1/jobs/{job_id}")
        assert response.json()["status"] == "running"
        response = await client.get(
            "/api/v1/tasks/diagnostics", params={"job_id": job_id}
        )
        assert response.status_code == 200, response.text
        blocked = response.json()
        assert blocked["live_worker_count"] == 1
        assert blocked["pending_task_count"] == 1
        (analysis,) = blocked["groups"]
        assert analysis["kind"] == "analyzer"
        assert analysis["task_count"] == 1
        assert analysis["job_ids"] == [job_id]
        assert analysis["reasons"] == ["claim_mismatch"]
        response = await client.post(
            "/api/v1/tasks/claim",
            json={"max_tasks": 1},
            headers={"Authorization": f"Bearer {importer_token}"},
        )
        assert response.status_code == 200, response.text
        assert response.json()["tasks"] == []

        analyzer_token = await _register_worker(client, "analyzer")
        response = await client.get(
            "/api/v1/tasks/diagnostics", params={"job_id": job_id}
        )
        assert response.status_code == 200, response.text
        covered = response.json()
        assert covered["live_worker_count"] == 2
        assert covered["pending_task_count"] == 1
        assert covered["groups"][0]["reasons"] == []
        analyzer = await _claim_task(client, analyzer_token)
        assert analyzer["task"]["id"] == analysis["task_ids"][0]
        await _complete_task(client, analyzer, [])

        response = await client.get(f"/api/v1/jobs/{job_id}")
        assert response.json()["status"] == "completed"
        response = await client.get(
            "/api/v1/tasks/diagnostics", params={"job_id": job_id}
        )
        assert response.status_code == 200, response.text
        assert response.json()["pending_task_count"] == 0
        assert response.json()["groups"] == []
