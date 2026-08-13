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
"""End-to-end replay pipeline tests against PostgreSQL."""

import json
import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client
from kitaru.cache_keys import compute_tool_cache_key

RUNTIME = {"platform": "bare"}


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(db_settings()) as client:
        yield client


async def _setup_replayable_session(client: httpx.AsyncClient) -> tuple[str, str, str]:
    """Create an agent, a runnable version, and a recorded baseline session.

    Returns:
        Agent id, agent version id, and baseline session id.
    """
    agent = (await client.post("/v1/agents", json={"name": "assistant"})).json()
    version = (
        await client.post(
            f"/v1/agents/{agent['id']}/versions",
            json={"run_spec": {"command": "run.sh", "timeout_seconds": 60}},
        )
    ).json()
    session = (
        await client.post(
            "/v1/sessions",
            json={
                "agent_id": agent["id"],
                "agent_version_id": version["id"],
                "origin": "recorded",
                "inputs": {"q": "hi"},
                "outputs": None,
            },
        )
    ).json()
    return agent["id"], version["id"], session["id"]


async def _register_evaluator(
    client: httpx.AsyncClient, name: str = "accuracy"
) -> None:
    blob = (
        await client.post(
            "/v1/blobs",
            files={"file": ("score.py", b"def score(): pass", "text/plain")},
        )
    ).json()
    evaluator = (
        await client.post("/v1/evaluators", json={"name": name, "metadata": {}})
    ).json()
    await client.post(
        f"/v1/evaluators/{evaluator['id']}/versions",
        json={
            "source": {"type": "script", "blob_id": blob["id"], "entrypoint": "score"}
        },
    )


async def test_replay_pipeline_completes_through_the_api(
    client: httpx.AsyncClient,
) -> None:
    """A replay runs through claim, agent completion, and evaluator completion."""
    agent_id, version_id, baseline_id = await _setup_replayable_session(client)
    await _register_evaluator(client)

    replay = (
        await client.post(
            "/v1/replays",
            json={
                "baseline_session_id": baseline_id,
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    assert replay["status"] == "pending"

    registration = (
        await client.post(
            "/v1/workers",
            json={"name": "worker-1", "scope": {}, "runtime": RUNTIME, "metadata": {}},
        )
    ).json()
    worker_headers = {"Authorization": f"Bearer {registration['token']}"}
    claimed = (
        await client.post(
            "/v1/tasks/claim", json={"max_tasks": 10}, headers=worker_headers
        )
    ).json()
    agent_entry = claimed["tasks"][0]
    agent_task = agent_entry["task"]
    assert agent_task["kind"] == "agent"
    agent_task_headers = {"Authorization": f"Bearer {agent_entry['token']}"}

    await client.patch(
        f"/v1/tasks/{agent_task['id']}",
        json={"status": "running"},
        headers=agent_task_headers,
    )
    result_session = (
        await client.post(
            "/v1/sessions",
            json={
                "origin": "replay",
                "inputs": None,
                "outputs": None,
            },
            headers=agent_task_headers,
        )
    ).json()
    assert result_session["agent_id"] == agent_id
    assert result_session["agent_version_id"] == version_id
    await client.patch(
        f"/v1/sessions/{result_session['id']}",
        json={"status": "completed", "outputs": {}},
    )
    response = await client.patch(
        f"/v1/tasks/{agent_task['id']}",
        json={"status": "completed"},
        headers=agent_task_headers,
    )
    assert response.status_code == 200

    replay_after = (await client.get(f"/v1/replays/{replay['id']}")).json()
    assert replay_after["status"] == "evaluating"
    assert replay_after["result_session_id"] == result_session["id"]

    filtered = (
        await client.get(
            "/v1/replays",
            params={
                "filter": json.dumps(
                    {
                        "field": "result_session_id",
                        "op": "eq",
                        "value": result_session["id"],
                    }
                )
            },
        )
    ).json()["items"]
    assert [item["id"] for item in filtered] == [replay["id"]]
    unmatched = (
        await client.get(
            "/v1/replays",
            params={
                "filter": json.dumps(
                    {
                        "field": "result_session_id",
                        "op": "eq",
                        "value": str(uuid.uuid4()),
                    }
                )
            },
        )
    ).json()["items"]
    assert unmatched == []
    # A second replay whose agent task never produces a session, so the is_null
    # assertion below distinguishes the correct answer from an empty page.
    other_baseline = (
        await client.post(
            "/v1/sessions",
            json={
                "agent_id": agent_id,
                "agent_version_id": version_id,
                "origin": "recorded",
                "inputs": {"q": "hi again"},
                "outputs": None,
            },
        )
    ).json()
    unfinished = (
        await client.post(
            "/v1/replays",
            json={
                "baseline_session_id": other_baseline["id"],
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    pending = (
        await client.get(
            "/v1/replays",
            params={
                "filter": json.dumps({"field": "result_session_id", "op": "is_null"})
            },
        )
    ).json()["items"]
    assert [item["id"] for item in pending] == [unfinished["id"]]

    claimed = (
        await client.post(
            "/v1/tasks/claim", json={"max_tasks": 10}, headers=worker_headers
        )
    ).json()
    eval_entry = claimed["tasks"][0]
    eval_task = eval_entry["task"]
    assert eval_task["kind"] == "evaluator"
    eval_task_headers = {"Authorization": f"Bearer {eval_entry['token']}"}

    await client.patch(
        f"/v1/tasks/{eval_task['id']}",
        json={"status": "running"},
        headers=eval_task_headers,
    )
    await client.patch(
        f"/v1/tasks/{eval_task['id']}",
        json={
            "status": "completed",
            "result": [{"name": "accuracy", "score": 0.9}],
        },
        headers=eval_task_headers,
    )

    replay_final = (await client.get(f"/v1/replays/{replay['id']}")).json()
    assert replay_final["status"] == "completed"

    filter_expression = {
        "field": "session_id",
        "op": "eq",
        "value": result_session["id"],
    }
    evaluations = (
        await client.get(
            "/v1/evaluations", params={"filter": json.dumps(filter_expression)}
        )
    ).json()["items"]
    assert len(evaluations) == 1
    assert evaluations[0]["name"] == "accuracy"
    assert evaluations[0]["score"] == 0.9
    assert evaluations[0]["task_id"] == eval_task["id"]


async def test_tool_lookup_baseline_scope_persists_across_requests(
    client: httpx.AsyncClient,
) -> None:
    """A tool_lookup hit persists once the recorded node is ingested."""
    _, _, baseline_id = await _setup_replayable_session(client)
    await _register_evaluator(client)

    replay = (
        await client.post(
            "/v1/replays",
            json={
                "baseline_session_id": baseline_id,
                "evaluators": [{"evaluator": "accuracy"}],
                "tool_policy": {
                    "default": {"type": "passthrough"},
                    "tools": {
                        "search": {
                            "type": "history",
                            "scope": "baseline",
                            "on_miss": "fail",
                        }
                    },
                },
            },
        )
    ).json()

    tool_inputs = {"query": "hi"}
    await client.post(
        f"/v1/sessions/{baseline_id}/nodes",
        json={
            "nodes": [
                {
                    "index": 0,
                    "node_type": "tool_call",
                    "name": "search",
                    "status": "completed",
                    "tool_name": "search",
                    "inputs": tool_inputs,
                    "outputs": {"result": "hit"},
                    "attributes": {},
                    "metadata": {},
                }
            ]
        },
    )

    cache_key = compute_tool_cache_key("search", tool_inputs)
    response = await client.post(
        f"/v1/replays/{replay['id']}/tool-lookup",
        json={"tool_name": "search", "cache_key": cache_key},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["found"] is True
    assert body["result"] == {"result": "hit"}

    miss = (
        await client.post(
            f"/v1/replays/{replay['id']}/tool-lookup",
            json={"tool_name": "search", "cache_key": "b" * 64},
        )
    ).json()
    assert miss["found"] is False
    assert miss["result"] is None
