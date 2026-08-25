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
"""End-to-end sample data seeding tests against PostgreSQL."""

import json
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import db_settings, lifespan_client
from kitaru.server.application.services.sample_data_seeding import (
    SampleDataSeeder,
    load_sample_data,
)


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    async with lifespan_client(db_settings()) as client:
        yield client


@pytest.fixture
async def seeded(client: httpx.AsyncClient) -> dict[str, str]:
    """Provide the response body of a freshly seeded workspace."""
    response = await client.post("/api/v1/ui/sample-data")
    assert response.status_code == 201
    return response.json()


def _agent_filter(agent_id: str) -> dict[str, str]:
    """Build query params selecting the resources of one agent."""
    return {
        "filter": json.dumps({"field": "agent_id", "op": "eq", "value": agent_id}),
        "size": "100",
    }


async def test_seed_sample_data(
    client: httpx.AsyncClient, seeded: dict[str, str]
) -> None:
    """Seed an agent with sessions, evaluations, and derived resources."""
    data = load_sample_data()

    agent = (await client.get(f"/api/v1/agents/{seeded['agent_id']}")).json()
    assert agent["name"] == data.agent.name

    sessions = (
        await client.get("/api/v1/ui/sessions", params=_agent_filter(agent["id"]))
    ).json()
    assert len(sessions["items"]) == len(data.sessions)
    failing = [
        (item["session"]["external_id"], evaluation["name"])
        for item in sessions["items"]
        for evaluation in item["evaluations"]
        if evaluation["passed"] is False
    ]
    assert failing == [("kitaru-template:returns-ticket-007", "policy_grounded_refund")]


async def test_seed_sample_data_stores_session_nodes(
    client: httpx.AsyncClient, seeded: dict[str, str]
) -> None:
    """Store every recorded node of every seeded session."""
    data = load_sample_data()

    sessions = (
        await client.get(
            "/api/v1/ui/sessions", params=_agent_filter(seeded["agent_id"])
        )
    ).json()
    counts = {}
    for item in sessions["items"]:
        nodes = (
            await client.get(
                f"/api/v1/sessions/{item['session']['id']}/nodes",
                params={"size": "100"},
            )
        ).json()
        counts[item["session"]["external_id"]] = len(nodes["items"])
    assert counts == {
        item.session.external_id: len(item.nodes) for item in data.sessions
    }


async def test_seed_sample_data_tags_sessions(
    client: httpx.AsyncClient, seeded: dict[str, str]
) -> None:
    """Tag every seeded session with the sample data tag."""
    data = load_sample_data()

    tagged = (
        await client.get(
            "/api/v1/sessions",
            params={
                "filter": json.dumps(
                    {"field": "tag", "op": "eq", "value": data.session_tag}
                ),
                "size": "100",
            },
        )
    ).json()
    assert {item["id"] for item in tagged["items"]} == {
        item["session"]["id"]
        for item in (
            await client.get(
                "/api/v1/ui/sessions", params=_agent_filter(seeded["agent_id"])
            )
        ).json()["items"]
    }


async def test_seed_sample_data_freezes_the_cohort(
    client: httpx.AsyncClient, seeded: dict[str, str]
) -> None:
    """Freeze the reviewed sessions into the cohort's first version."""
    data = load_sample_data()

    cohorts = (
        await client.get("/api/v1/cohorts", params=_agent_filter(seeded["agent_id"]))
    ).json()
    assert [item["name"] for item in cohorts["items"]] == [data.cohort.name]
    versions = (
        await client.get(f"/api/v1/cohorts/{cohorts['items'][0]['id']}/versions")
    ).json()
    assert [item["display_version"] for item in versions["items"]] == [
        data.cohort.display_version
    ]
    assert versions["items"][0]["session_count"] == len(data.cohort.member_external_ids)


async def test_seed_sample_data_registers_the_evaluator(
    client: httpx.AsyncClient, seeded: dict[str, str]
) -> None:
    """Register the evaluator script as a runnable script version."""
    data = load_sample_data()

    evaluators = (await client.get("/api/v1/evaluators", params={"size": "100"})).json()
    evaluator = next(
        item for item in evaluators["items"] if item["name"] == data.evaluator.name
    )
    versions = (
        await client.get(f"/api/v1/evaluators/{evaluator['id']}/versions")
    ).json()
    assert [item["display_version"] for item in versions["items"]] == [
        data.evaluator.display_version
    ]
    source = versions["items"][0]["source"]
    assert source["type"] == "script"
    content = await client.get(f"/api/v1/blobs/{source['blob_id']}/content")
    assert content.text == data.evaluator.source


async def test_seed_sample_data_defines_an_unstarted_experiment(
    client: httpx.AsyncClient, seeded: dict[str, str]
) -> None:
    """Define the experiment without starting a run."""
    data = load_sample_data()

    experiments = (
        await client.get(
            "/api/v1/experiments", params=_agent_filter(seeded["agent_id"])
        )
    ).json()
    assert [item["name"] for item in experiments["items"]] == [data.experiment.name]
    runs = (
        await client.get(
            "/api/v1/experiment-runs",
            params={
                "filter": json.dumps(
                    {
                        "field": "experiment_id",
                        "op": "eq",
                        "value": experiments["items"][0]["id"],
                    }
                )
            },
        )
    ).json()
    assert runs["items"] == []


async def test_seed_sample_data_pins_investigation_highlights(
    client: httpx.AsyncClient, seeded: dict[str, str]
) -> None:
    """Pin every investigation highlight to a node of its own session."""
    data = load_sample_data()

    investigations = (
        await client.get(
            "/api/v1/investigations", params=_agent_filter(seeded["agent_id"])
        )
    ).json()
    assert [item["name"] for item in investigations["items"]] == [
        data.investigation.name
    ]
    linked = (
        await client.get(
            f"/api/v1/investigations/{investigations['items'][0]['id']}/sessions"
        )
    ).json()
    assert len(linked["items"]) == len(data.investigation.sessions)
    for item in linked["items"]:
        nodes = (
            await client.get(
                f"/api/v1/sessions/{item['session_id']}/nodes", params={"size": "100"}
            )
        ).json()
        node_ids = {node["id"] for node in nodes["items"]}
        highlights = [
            highlight
            for question in item["questions"]
            for highlight in question["highlights"]
        ]
        assert highlights
        assert all(
            highlight["selector"]["node_id"] in node_ids for highlight in highlights
        )


async def test_seed_sample_data_twice_conflicts(
    client: httpx.AsyncClient, seeded: dict[str, str]
) -> None:
    """Reject a second seed while the sample agent is registered."""
    response = await client.post("/api/v1/ui/sample-data")

    assert response.status_code == 409


async def test_seed_sample_data_with_agent_name(client: httpx.AsyncClient) -> None:
    """Seed under the agent name the request carries."""
    response = await client.post(
        "/api/v1/ui/sample-data", json={"agent_name": "returns-agent"}
    )

    assert response.status_code == 201
    agent_id = response.json()["agent_id"]
    agent = (await client.get(f"/api/v1/agents/{agent_id}")).json()
    assert agent["name"] == "returns-agent"


async def test_seed_sample_data_failure_leaves_the_agent(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep the committed agent when the seed fails."""

    async def fail_seed(*args: object, **kwargs: object) -> None:
        raise RuntimeError("seed failed")

    monkeypatch.setattr(SampleDataSeeder, "seed", fail_seed)
    with pytest.raises(RuntimeError, match="seed failed"):
        await client.post("/api/v1/ui/sample-data")
    monkeypatch.undo()

    data = load_sample_data()
    agents = (await client.get("/api/v1/agents")).json()
    assert [agent["name"] for agent in agents["items"]] == [data.agent.name]
    response = await client.post("/api/v1/ui/sample-data")
    assert response.status_code == 409
