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
"""End-to-end experiment tests against PostgreSQL."""

import json
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created experiments.
    async with lifespan_client(db_settings()) as client:
        yield client


@pytest.fixture
async def agent_id(client: httpx.AsyncClient) -> str:
    """Provide the id of an agent for experiments to belong to."""
    created = (await client.post("/v1/agents", json={"name": "assistant"})).json()
    return created["id"]


async def _create_evaluator(client: httpx.AsyncClient, name: str = "accuracy") -> None:
    evaluator = (await client.post("/v1/evaluators", json={"name": name})).json()
    await client.post(
        f"/v1/evaluators/{evaluator['id']}/versions",
        json={
            "source": {
                "type": "package",
                "requirement": "kitaru-scorer==1.0.0",
                "entrypoint": "pkg:score",
            }
        },
    )


async def test_experiments_persist_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Prove the per-request commit through separate requests."""
    await _create_evaluator(client)
    response = await client.post(
        "/v1/experiments",
        json={
            "name": "exp1",
            "agent_id": agent_id,
            "evaluators": [{"evaluator": "accuracy"}],
        },
    )
    assert response.status_code == 201
    created = response.json()

    response = await client.get(f"/v1/experiments/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created

    response = await client.get("/v1/experiments")
    assert response.status_code == 200
    body = response.json()
    assert body["next_cursor"] is None
    assert body["items"][0] == created


async def test_duplicate_name_conflict(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Translate the database constraint into HTTP 409."""
    await _create_evaluator(client)
    body = {
        "name": "exp1",
        "agent_id": agent_id,
        "evaluators": [{"evaluator": "accuracy"}],
    }
    response = await client.post("/v1/experiments", json=body)
    assert response.status_code == 201
    response = await client.post("/v1/experiments", json=body)
    assert response.status_code == 409
    assert response.json() == {"detail": "Experiment name 'exp1' is already registered"}


async def test_update_persists_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Persist an update across requests."""
    await _create_evaluator(client)
    created = (
        await client.post(
            "/v1/experiments",
            json={
                "name": "exp1",
                "agent_id": agent_id,
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    response = await client.patch(
        f"/v1/experiments/{created['id']}", json={"description": "Reviews"}
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/experiments/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["description"] == "Reviews"
    assert body["updated"] > created["updated"]


async def test_update_new_evaluators_replaces_config_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Persist a new replay config across requests when evaluators change."""
    await _create_evaluator(client, name="accuracy")
    await _create_evaluator(client, name="relevance")
    created = (
        await client.post(
            "/v1/experiments",
            json={
                "name": "exp1",
                "agent_id": agent_id,
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()

    response = await client.patch(
        f"/v1/experiments/{created['id']}",
        json={"evaluators": [{"evaluator": "relevance"}]},
    )
    assert response.status_code == 200

    response = await client.get(f"/v1/experiments/{created['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["evaluators"] == [
        {"evaluator": "relevance", "version": 1, "params": {}}
    ]


async def test_delete_persists_across_requests(
    client: httpx.AsyncClient, agent_id: str
) -> None:
    """Persist a deletion across requests."""
    await _create_evaluator(client)
    created = (
        await client.post(
            "/v1/experiments",
            json={
                "name": "exp1",
                "agent_id": agent_id,
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    response = await client.delete(f"/v1/experiments/{created['id']}")
    assert response.status_code == 204

    response = await client.get(f"/v1/experiments/{created['id']}")
    assert response.status_code == 404


async def test_query_by_tag(client: httpx.AsyncClient, agent_id: str) -> None:
    """Filter experiments by a tag linked to the resource."""
    await _create_evaluator(client)
    tagged = (
        await client.post(
            "/v1/experiments",
            json={
                "name": "tagged-exp",
                "agent_id": agent_id,
                "evaluators": [{"evaluator": "accuracy"}],
            },
        )
    ).json()
    await client.post(
        "/v1/experiments",
        json={
            "name": "untagged-exp",
            "agent_id": agent_id,
            "evaluators": [{"evaluator": "accuracy"}],
        },
    )

    tag = (await client.post("/v1/tags", json={"name": "smoke"})).json()
    await client.post(
        f"/v1/tags/{tag['id']}/links",
        json={"resource_type": "experiment", "resource_id": tagged["id"]},
    )

    filter_expression = {"field": "tag", "op": "eq", "value": "smoke"}
    response = await client.get(
        "/v1/experiments", params={"filter": json.dumps(filter_expression)}
    )
    assert response.status_code == 200
    body = response.json()
    assert [item["id"] for item in body["items"]] == [tagged["id"]]
