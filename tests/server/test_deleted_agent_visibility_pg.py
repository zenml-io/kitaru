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
"""Leak hunt for deleted agents: visibility, name reuse, and new work creation."""

import asyncio
import json
from typing import Any

import httpx

from conftest import db_settings, lifespan_client

WORKER_RUNTIME = {"platform": "bare"}
WORKER_SCOPE = {"claims": [{"kind": "agent"}]}


async def _agent(client: httpx.AsyncClient, name: str) -> dict[str, Any]:
    response = await client.post("/api/v1/agents", json={"name": name})
    assert response.status_code == 201, response.text
    return response.json()


async def _runnable_version(client: httpx.AsyncClient, agent_id: str) -> dict[str, Any]:
    secret = (
        await client.post(
            "/api/v1/secrets", json={"name": "run-secret", "values": {"k": "v"}}
        )
    ).json()
    response = await client.post(
        f"/api/v1/agents/{agent_id}/versions",
        json={
            "run_spec": {
                "command": "run.sh",
                "timeout_seconds": 60,
                "secret_ids": [secret["id"]],
            }
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


async def _session(
    client: httpx.AsyncClient, agent_id: str, agent_version_id: str | None = None
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "agent_id": agent_id,
        "origin": "recorded",
        "inputs": {"q": "hi"},
        "outputs": None,
    }
    if agent_version_id is not None:
        body["agent_version_id"] = agent_version_id
    response = await client.post("/api/v1/sessions", json=body)
    assert response.status_code == 201, response.text
    return response.json()


async def _evaluator(client: httpx.AsyncClient) -> None:
    blob = (
        await client.post(
            "/api/v1/blobs",
            files={"file": ("score.py", b"def score(): pass", "text/plain")},
        )
    ).json()
    evaluator = (
        await client.post(
            "/api/v1/evaluators", json={"name": "accuracy", "metadata": {}}
        )
    ).json()
    response = await client.post(
        f"/api/v1/evaluators/{evaluator['id']}/versions",
        json={
            "source": {"type": "script", "blob_id": blob["id"], "entrypoint": "score"}
        },
    )
    assert response.status_code == 201, response.text


async def _build_agent_with_subtree(client: httpx.AsyncClient) -> dict[str, Any]:
    """Build an agent with a runnable version, sessions, a cohort, and an experiment.

    Returns:
        Ids needed to exercise every read/write surface after deletion.
    """
    await _evaluator(client)
    agent = await _agent(client, "assistant")
    version = await _runnable_version(client, agent["id"])
    session_ids = [
        (await _session(client, agent["id"], version["id"]))["id"] for _ in range(3)
    ]
    cohort = (
        await client.post(
            "/api/v1/cohorts", json={"name": "cohort-1", "agent_id": agent["id"]}
        )
    ).json()
    cohort_version = (
        await client.post(
            f"/api/v1/cohorts/{cohort['id']}/versions",
            json={"add_session_ids": [session_ids[0]]},
        )
    ).json()
    investigation = (
        await client.post(
            "/api/v1/investigations",
            json={"agent_id": agent["id"], "name": "inv-1", "sessions": []},
        )
    ).json()
    experiment = (
        await client.post(
            "/api/v1/experiments",
            json={
                "name": "exp-1",
                "agent_id": agent["id"],
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    return {
        "agent_id": agent["id"],
        "version_id": version["id"],
        "session_ids": session_ids,
        "cohort_id": cohort["id"],
        "cohort_version_id": cohort_version["id"],
        "investigation_id": investigation["id"],
        "experiment_id": experiment["id"],
    }


async def test_deleted_agent_never_appears_across_read_surfaces() -> None:
    """Every read surface hides the deleted agent while its subtree stays visible."""
    settings = db_settings(DB_POOL_SIZE=20, DB_MAX_OVERFLOW=10)
    async with lifespan_client(settings) as client:
        setup = await _build_agent_with_subtree(client)
        agent_id = setup["agent_id"]

        response = await client.delete(f"/api/v1/agents/{agent_id}")
        assert response.status_code == 204, response.text

        # The agent itself is gone from every agent-scoped read.
        assert (await client.get(f"/api/v1/agents/{agent_id}")).status_code == 404
        agents = (await client.get("/api/v1/agents")).json()["items"]
        assert agent_id not in {item["id"] for item in agents}
        assert "assistant" not in {item["name"] for item in agents}
        filtered = (
            await client.get(
                "/api/v1/agents",
                params={
                    "filter": json.dumps({"field": "id", "op": "eq", "value": agent_id})
                },
            )
        ).json()["items"]
        assert filtered == []
        filtered_by_name = (
            await client.get(
                "/api/v1/agents",
                params={
                    "filter": json.dumps(
                        {"field": "name", "op": "eq", "value": "assistant"}
                    )
                },
            )
        ).json()["items"]
        assert filtered_by_name == []

        # The retained subtree stays readable through its own routes, still
        # carrying the (now invisible) agent id.
        assert (
            await client.get(f"/api/v1/agent-versions/{setup['version_id']}")
        ).status_code == 200
        versions = (
            await client.get(
                f"/api/v1/agents/{agent_id}/versions",
            )
        ).json()["items"]
        assert setup["version_id"] in {item["id"] for item in versions}
        for session_id in setup["session_ids"]:
            session = (await client.get(f"/api/v1/sessions/{session_id}")).json()
            assert session["agent_id"] == agent_id
        sessions_by_agent = (
            await client.get(
                "/api/v1/sessions",
                params={
                    "filter": json.dumps(
                        {"field": "agent_id", "op": "eq", "value": agent_id}
                    )
                },
            )
        ).json()["items"]
        assert {item["id"] for item in sessions_by_agent} == set(setup["session_ids"])
        assert (
            await client.get(f"/api/v1/cohorts/{setup['cohort_id']}")
        ).status_code == 200
        assert (
            await client.get(f"/api/v1/cohort-versions/{setup['cohort_version_id']}")
        ).status_code == 200
        assert (
            await client.get(f"/api/v1/investigations/{setup['investigation_id']}")
        ).status_code == 200
        assert (
            await client.get(f"/api/v1/experiments/{setup['experiment_id']}")
        ).status_code == 200


async def test_deleted_agent_children_do_not_transfer_to_reused_name() -> None:
    """Reusing a deleted agent's name creates an unrelated agent with no children."""
    settings = db_settings(DB_POOL_SIZE=20, DB_MAX_OVERFLOW=10)
    async with lifespan_client(settings) as client:
        first = await _build_agent_with_subtree(client)
        assert (
            await client.delete(f"/api/v1/agents/{first['agent_id']}")
        ).status_code == 204

        second = await _agent(client, "assistant")
        assert second["id"] != first["agent_id"]

        # The new agent starts with an empty subtree: none of the first
        # agent's children were reattached to it.
        second_versions = (
            await client.get(f"/api/v1/agents/{second['id']}/versions")
        ).json()["items"]
        assert second_versions == []
        second_sessions = (
            await client.get(
                "/api/v1/sessions",
                params={
                    "filter": json.dumps(
                        {"field": "agent_id", "op": "eq", "value": second["id"]}
                    )
                },
            )
        ).json()["items"]
        assert second_sessions == []

        # The first agent's children still point at the first agent's id,
        # not the second one's.
        for session_id in first["session_ids"]:
            session = (await client.get(f"/api/v1/sessions/{session_id}")).json()
            assert session["agent_id"] == first["agent_id"]
        first_versions = (
            await client.get(f"/api/v1/agent-versions/{first['version_id']}")
        ).json()
        assert first_versions["id"] == first["version_id"]

        # Delete the second agent and reuse the name a third time. The chain
        # keeps producing distinct, unconfused agents.
        assert (
            await client.delete(f"/api/v1/agents/{second['id']}")
        ).status_code == 204
        third = await _agent(client, "assistant")
        assert third["id"] not in {first["agent_id"], second["id"]}
        third_versions = (
            await client.get(f"/api/v1/agents/{third['id']}/versions")
        ).json()["items"]
        assert third_versions == []

        listed = (await client.get("/api/v1/agents")).json()["items"]
        assert [item["name"] for item in listed].count("assistant") == 1
        assert {item["id"] for item in listed} == {third["id"]}


async def test_concurrent_name_reuse_creates_never_double_live() -> None:
    """Race many creates of a deleted agent's name and allow only one live survivor."""
    settings = db_settings(DB_POOL_SIZE=40, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        agent = await _agent(client, "assistant")
        assert (await client.delete(f"/api/v1/agents/{agent['id']}")).status_code == 204

        responses = await asyncio.gather(
            *(
                client.post("/api/v1/agents", json={"name": "assistant"})
                for _ in range(12)
            )
        )
        statuses = {response.status_code for response in responses}
        assert statuses <= {201, 409}, [r.text for r in responses]
        created_ids = [
            response.json()["id"]
            for response in responses
            if response.status_code == 201
        ]
        assert len(created_ids) == 1, created_ids

        listed = (await client.get("/api/v1/agents")).json()["items"]
        live_named_assistant = [item for item in listed if item["name"] == "assistant"]
        assert len(live_named_assistant) == 1
        assert live_named_assistant[0]["id"] == created_ids[0]
        assert live_named_assistant[0]["id"] != agent["id"]


async def test_session_run_rejects_deleted_agents_version() -> None:
    """Reject a new session run whose agent version belongs to a deleted agent."""
    settings = db_settings(DB_POOL_SIZE=20, DB_MAX_OVERFLOW=10)
    async with lifespan_client(settings) as client:
        agent = await _agent(client, "assistant")
        version = await _runnable_version(client, agent["id"])

        assert (await client.delete(f"/api/v1/agents/{agent['id']}")).status_code == 204

        response = await client.post(
            "/api/v1/session-runs",
            json={"agent_version_id": version["id"], "inputs": {"q": "hi"}},
        )
        assert response.status_code == 404, response.text


async def test_replay_rejects_baseline_of_deleted_agent() -> None:
    """Reject a new standalone replay whose baseline session's agent is deleted."""
    settings = db_settings(DB_POOL_SIZE=20, DB_MAX_OVERFLOW=10)
    async with lifespan_client(settings) as client:
        await _evaluator(client)
        agent = await _agent(client, "assistant")
        version = await _runnable_version(client, agent["id"])
        session = await _session(client, agent["id"], version["id"])

        assert (await client.delete(f"/api/v1/agents/{agent['id']}")).status_code == 204

        response = await client.post(
            "/api/v1/replays",
            json={
                "baseline_session_id": session["id"],
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
        assert response.status_code == 404, response.text


async def test_experiment_run_rejects_deleted_experiment_agent() -> None:
    """Reject a new experiment run whose experiment's agent is deleted."""
    settings = db_settings(DB_POOL_SIZE=20, DB_MAX_OVERFLOW=10)
    async with lifespan_client(settings) as client:
        await _evaluator(client)
        agent = await _agent(client, "assistant")
        version = await _runnable_version(client, agent["id"])
        session = await _session(client, agent["id"], version["id"])
        cohort = (
            await client.post(
                "/api/v1/cohorts", json={"name": "cohort-1", "agent_id": agent["id"]}
            )
        ).json()
        cohort_version = (
            await client.post(
                f"/api/v1/cohorts/{cohort['id']}/versions",
                json={"add_session_ids": [session["id"]]},
            )
        ).json()
        experiment = (
            await client.post(
                "/api/v1/experiments",
                json={
                    "name": "exp-1",
                    "agent_id": agent["id"],
                    "evaluators": [{"evaluator": "accuracy"}],
                },
            )
        ).json()

        assert (await client.delete(f"/api/v1/agents/{agent['id']}")).status_code == 204

        # The experiment itself is retained and stays readable.
        assert (
            await client.get(f"/api/v1/experiments/{experiment['id']}")
        ).status_code == 200

        response = await client.post(
            f"/api/v1/experiments/{experiment['id']}/runs",
            json={
                "cohort_version_id": cohort_version["id"],
                "agent_version_id": version["id"],
            },
        )
        assert response.status_code == 404, response.text
