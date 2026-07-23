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
"""End-to-end experiment run tests against PostgreSQL."""

from collections.abc import AsyncGenerator

import httpx
import pytest
from test_experiments_api_pg import SCORING_POLICY, seed_cohort

from conftest import db_settings, lifespan_client


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    # Under the none auth scheme every request runs as the account
    # bootstrapped at startup, which owns all created runs.
    async with lifespan_client(db_settings()) as client:
        yield client


async def test_run_flow_persists_across_requests(client: httpx.AsyncClient) -> None:
    """Start runs, count the numbers, and read progress and replays."""
    cohort_id = await seed_cohort(client)
    response = await client.post(
        "/v1/experiments",
        json={
            "name": "swap-model",
            "cohort_id": cohort_id,
            "scoring_policy": SCORING_POLICY,
        },
    )
    assert response.status_code == 201
    experiment_id = response.json()["id"]

    response = await client.post(f"/v1/experiments/{experiment_id}/runs", json={})
    assert response.status_code == 201
    first = response.json()
    response = await client.post(
        f"/v1/experiments/{experiment_id}/runs", json={"score_baselines": True}
    )
    assert response.status_code == 201
    second = response.json()
    assert [first["number"], second["number"]] == [1, 2]
    assert second["score_baselines"] is True

    response = await client.get("/v1/experiment-runs")
    assert response.status_code == 200
    assert response.json()["total"] == 2

    response = await client.get(f"/v1/experiment-runs/{first['id']}")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "pending"
    assert body["progress"]["pending"] == 2
    assert body["progress"]["total"] == 2

    response = await client.get(f"/v1/experiment-runs/{first['id']}/replays")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert all(item["status"] == "pending" for item in body["items"])

    response = await client.get(f"/v1/experiments/{experiment_id}/runs")
    assert response.status_code == 200
    assert response.json()["total"] == 2
