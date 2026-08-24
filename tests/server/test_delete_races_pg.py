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
"""Concurrent-delete races across every deletable resource type."""

import asyncio
import uuid

import httpx
from test_cancel_settle_concurrency_pg import (
    _agent_version,
    _baseline_session,
    _cohort_version,
    _experiment_with_evaluator,
    _poll_job_settled,
    _register_evaluator,
    _run_job_ids,
    _standalone_replay,
    _start_run,
    assert_no_server_error,
)

from conftest import db_settings, lifespan_client

RACERS = 16


async def _race_deletes(
    client: httpx.AsyncClient, url: str, racers: int = RACERS
) -> list[httpx.Response]:
    """Fire many concurrent deletes at one resource url.

    Returns:
        Responses collected from the race.
    """
    return list(await asyncio.gather(*(client.delete(url) for _ in range(racers))))


def _assert_exactly_one_204(responses: list[httpx.Response]) -> None:
    """Check a delete race settles with exactly one winner and no server error.

    Args:
        responses: Responses collected from a race.
    """
    assert_no_server_error(responses)
    statuses = [response.status_code for response in responses]
    assert set(statuses) <= {204, 404}, statuses
    assert statuses.count(204) == 1, statuses


async def _secret(client: httpx.AsyncClient) -> str:
    """Create a secret.

    Returns:
        Secret id.
    """
    response = await client.post(
        "/api/v1/secrets",
        json={"name": f"secret-{uuid.uuid4().hex[:8]}", "values": {"k": "v"}},
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


async def _tag(client: httpx.AsyncClient) -> str:
    """Create a tag.

    Returns:
        Tag id.
    """
    response = await client.post(
        "/api/v1/tags", json={"name": f"tag-{uuid.uuid4().hex[:8]}"}
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


async def _cohort(client: httpx.AsyncClient, agent_id: str) -> str:
    """Create a cohort.

    Returns:
        Cohort id.
    """
    response = await client.post(
        "/api/v1/cohorts",
        json={"name": f"cohort-{uuid.uuid4().hex[:8]}", "agent_id": agent_id},
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


async def _investigation(client: httpx.AsyncClient, agent_id: str) -> str:
    """Create an investigation with no scored sessions.

    Returns:
        Investigation id.
    """
    response = await client.post(
        "/api/v1/investigations",
        json={
            "agent_id": agent_id,
            "name": f"inv-{uuid.uuid4().hex[:8]}",
            "sessions": [],
        },
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


async def _blob(client: httpx.AsyncClient) -> str:
    """Upload a blob unattached to any plugin version.

    Returns:
        Blob id.
    """
    response = await client.post(
        "/api/v1/blobs",
        files={"file": ("payload.txt", b"hello", "text/plain")},
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


async def _plugin(client: httpx.AsyncClient) -> str:
    """Register an evaluator plugin with no version.

    Returns:
        Evaluator id.
    """
    response = await client.post(
        "/api/v1/evaluators",
        json={"name": f"accuracy-{uuid.uuid4().hex[:8]}", "metadata": {}},
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


async def _api_key(client: httpx.AsyncClient) -> str:
    """Create an API key.

    Returns:
        API key id.
    """
    response = await client.post(
        "/api/v1/api-keys", json={"name": f"key-{uuid.uuid4().hex[:8]}"}
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


async def _worker(client: httpx.AsyncClient) -> str:
    """Register a worker.

    Returns:
        Worker id.
    """
    response = await client.post(
        "/api/v1/workers",
        json={
            "name": f"worker-{uuid.uuid4().hex[:8]}",
            "scope": {"claims": [{"kind": "agent"}]},
            "runtime": {"platform": "bare"},
            "metadata": {},
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["worker"]["id"]


async def _session_node(client: httpx.AsyncClient, session_id: str) -> str:
    """Add a node to a session.

    Returns:
        Node id.
    """
    response = await client.post(
        f"/api/v1/sessions/{session_id}/nodes",
        json={
            "nodes": [
                {
                    "index": 0,
                    "node_type": "llm_call",
                    "name": "call",
                    "status": "completed",
                    "inputs": {"q": "hi"},
                    "outputs": None,
                    "attributes": None,
                    "metadata": {},
                }
            ]
        },
    )
    assert response.status_code == 200, response.text
    nodes = (await client.get(f"/api/v1/sessions/{session_id}/nodes")).json()
    return nodes["items"][0]["id"]


async def _annotation(client: httpx.AsyncClient, session_id: str, node_id: str) -> str:
    """Create an annotation on a session node.

    Returns:
        Annotation id.
    """
    response = await client.post(
        "/api/v1/annotations",
        json={
            "session_id": session_id,
            "selector": {"node_id": node_id},
            "value": "note",
        },
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


# --- One race per resource kind --------------------------------------------


async def test_replay_delete_race() -> None:
    """Race concurrent deletes of one standalone replay."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        agent_id, version_id = await _agent_version(client)
        baseline_id = await _baseline_session(client, agent_id, version_id)
        evaluator_name = await _register_evaluator(client)
        replay = await _standalone_replay(client, baseline_id, evaluator_name)
        url = f"/api/v1/replays/{replay['id']}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        assert (await client.get(url)).status_code == 404


async def test_session_delete_race() -> None:
    """Race concurrent deletes of one session."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        agent_id, version_id = await _agent_version(client)
        session_id = await _baseline_session(client, agent_id, version_id)
        url = f"/api/v1/sessions/{session_id}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        assert (await client.get(url)).status_code == 404


async def test_experiment_delete_race() -> None:
    """Race concurrent deletes of one experiment with a run and a replay."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        agent_id, version_id = await _agent_version(client)
        baseline_id = await _baseline_session(client, agent_id, version_id)
        evaluator_name = await _register_evaluator(client)
        experiment = await _experiment_with_evaluator(client, agent_id, evaluator_name)
        cohort_version_id = await _cohort_version(client, agent_id, [baseline_id])
        await _start_run(client, experiment["id"], cohort_version_id, version_id)
        url = f"/api/v1/experiments/{experiment['id']}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        assert (await client.get(url)).status_code == 404


async def test_experiment_run_delete_race() -> None:
    """Race concurrent deletes of one experiment run with a replay."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        agent_id, version_id = await _agent_version(client)
        baseline_id = await _baseline_session(client, agent_id, version_id)
        evaluator_name = await _register_evaluator(client)
        experiment = await _experiment_with_evaluator(client, agent_id, evaluator_name)
        cohort_version_id = await _cohort_version(client, agent_id, [baseline_id])
        run = await _start_run(client, experiment["id"], cohort_version_id, version_id)
        job_ids = await _run_job_ids(client, run["id"])
        assert job_ids
        url = f"/api/v1/experiment-runs/{run['id']}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        assert (await client.get(url)).status_code == 404


async def test_job_delete_race() -> None:
    """Race concurrent deletes of one canceled, settled job."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        agent_id, version_id = await _agent_version(client)
        baseline_id = await _baseline_session(client, agent_id, version_id)
        evaluator_name = await _register_evaluator(client)
        replay = await _standalone_replay(client, baseline_id, evaluator_name)
        job_id = replay["job_id"]

        cancel_response = await client.post(f"/api/v1/jobs/{job_id}/cancel")
        assert cancel_response.status_code == 200, cancel_response.text
        job = await _poll_job_settled(client, job_id)
        assert job["status"] == "canceled", job

        url = f"/api/v1/jobs/{job_id}"
        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        assert (await client.get(url)).status_code == 404


async def test_agent_delete_race() -> None:
    """Race concurrent deletes of one agent."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        agent_id, _version_id = await _agent_version(client)
        url = f"/api/v1/agents/{agent_id}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        assert (await client.get(url)).status_code == 404


async def test_agent_version_delete_race() -> None:
    """Race concurrent deletes of one agent version."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        _agent_id, version_id = await _agent_version(client)
        url = f"/api/v1/agent-versions/{version_id}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        assert (await client.get(url)).status_code == 404


async def test_cohort_delete_race() -> None:
    """Race concurrent deletes of one cohort."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        agent_id, _version_id = await _agent_version(client)
        cohort_id = await _cohort(client, agent_id)
        url = f"/api/v1/cohorts/{cohort_id}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        assert (await client.get(url)).status_code == 404


async def test_cohort_version_delete_race() -> None:
    """Race concurrent deletes of one cohort version."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        agent_id, _version_id = await _agent_version(client)
        cohort_version_id = await _cohort_version(client, agent_id, [])
        url = f"/api/v1/cohort-versions/{cohort_version_id}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        assert (await client.get(url)).status_code == 404


async def test_secret_delete_race() -> None:
    """Race concurrent deletes of one secret."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        secret_id = await _secret(client)
        url = f"/api/v1/secrets/{secret_id}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        assert (await client.get(url)).status_code == 404


async def test_investigation_delete_race() -> None:
    """Race concurrent deletes of one investigation."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        agent_id, _version_id = await _agent_version(client)
        investigation_id = await _investigation(client, agent_id)
        url = f"/api/v1/investigations/{investigation_id}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        assert (await client.get(url)).status_code == 404


async def test_tag_delete_race() -> None:
    """Race concurrent deletes of one tag."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        tag_id = await _tag(client)
        url = f"/api/v1/tags/{tag_id}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        # Tags have no get-by-id endpoint, so a rename probes existence.
        rename = await client.patch(url, json={"name": "renamed"})
        assert rename.status_code == 404, rename.text


async def test_tag_link_delete_race() -> None:
    """Race concurrent deletes of one tag link."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        agent_id, version_id = await _agent_version(client)
        session_id = await _baseline_session(client, agent_id, version_id)
        tag_id = await _tag(client)
        link_response = await client.post(
            f"/api/v1/tags/{tag_id}/links",
            json={"resource_type": "session", "resource_id": session_id},
        )
        assert link_response.status_code == 201, link_response.text
        url = f"/api/v1/tags/{tag_id}/links/session/{session_id}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        # Tags have no get-by-id endpoint, so a rename probes survival.
        rename = await client.patch(f"/api/v1/tags/{tag_id}", json={"name": "renamed"})
        assert rename.status_code == 200, rename.text


async def test_blob_delete_race() -> None:
    """Race concurrent deletes of one unattached blob."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        blob_id = await _blob(client)
        url = f"/api/v1/blobs/{blob_id}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        assert (await client.get(url)).status_code == 404


async def test_plugin_delete_race() -> None:
    """Race concurrent deletes of one evaluator plugin."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        evaluator_id = await _plugin(client)
        url = f"/api/v1/evaluators/{evaluator_id}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        assert (await client.get(url)).status_code == 404


async def test_api_key_delete_race() -> None:
    """Race concurrent deletes of one API key."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        api_key_id = await _api_key(client)
        url = f"/api/v1/api-keys/{api_key_id}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        assert (await client.get(url)).status_code == 404


async def test_worker_delete_race() -> None:
    """Race concurrent deletes of one worker."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        worker_id = await _worker(client)
        url = f"/api/v1/workers/{worker_id}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        assert (await client.get(url)).status_code == 404


async def test_annotation_delete_race() -> None:
    """Race concurrent deletes of one annotation."""
    settings = db_settings(DB_POOL_SIZE=RACERS + 10, DB_MAX_OVERFLOW=20)
    async with lifespan_client(settings) as client:
        agent_id, version_id = await _agent_version(client)
        session_id = await _baseline_session(client, agent_id, version_id)
        node_id = await _session_node(client, session_id)
        annotation_id = await _annotation(client, session_id, node_id)
        url = f"/api/v1/annotations/{annotation_id}"

        responses = await _race_deletes(client, url)
        _assert_exactly_one_204(responses)
        assert (await client.get(url)).status_code == 404
