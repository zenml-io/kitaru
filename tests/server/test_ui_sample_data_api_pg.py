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

import asyncio
import json
import uuid
from collections.abc import AsyncGenerator, Sequence

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import func, select

from conftest import db_settings, lifespan_client
from kitaru.analytics.client import AnalyticsClient
from kitaru.server.adapters.db.orm.experiment import ReplayConfigORM
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.application.models.agent_version import AgentVersionFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.session import SessionCreate
from kitaru.server.application.services.agent_version_service import AgentVersionService
from kitaru.server.application.services.sample_data_seeding import (
    SampleDataSeeder,
    load_sample_data,
)
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.investigation import Investigation
from kitaru.server.domain.session import Session


@pytest.fixture
async def client(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app running its full lifespan."""
    unused_clients: list[httpx.AsyncClient] = []

    def analytics_client(**kwargs: object) -> AnalyticsClient:
        result = AnalyticsClient()
        unused_clients.append(result._http)
        result._http = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200)),
            base_url="https://analytics.invalid",
        )
        return result

    monkeypatch.setattr("kitaru.server.api.app.AnalyticsClient", analytics_client)
    async with lifespan_client(db_settings()) as client:
        yield client
    for unused in unused_clients:
        await unused.aclose()


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


async def test_seed_returns_domain_objects_and_keeps_the_tour_response(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Return domain entities and map the unchanged tour response at REST."""
    results: list[tuple[Agent, Session, Investigation]] = []
    seed = SampleDataSeeder.seed

    async def capture_seed(
        self: SampleDataSeeder,
        agent: Agent,
        version: AgentVersion,
        actor: AuthContext,
    ) -> tuple[Agent, Session, Investigation]:
        result = await seed(self, agent, version, actor)
        results.append(result)
        return result

    monkeypatch.setattr(SampleDataSeeder, "seed", capture_seed)
    response = await client.post("/api/v1/ui/sample-data")
    assert response.status_code == 201
    assert len(results) == 1
    assert isinstance(results[0], tuple)
    agent, session, investigation = results[0]
    assert isinstance(agent, Agent)
    assert isinstance(session, Session)
    assert isinstance(investigation, Investigation)
    assert session.external_id == load_sample_data().failing_session_external_id
    assert response.json() == {
        "agent_id": str(agent.id),
        "agent_name": agent.name,
        "failing_session_id": str(session.id),
        "failing_session_number": session.number,
        "investigation_id": str(investigation.id),
    }


@pytest.mark.parametrize("existing_count", [3, 10])
async def test_recovery_locks_every_session_before_repair_or_ingestion(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
    existing_count: int,
) -> None:
    """Lock the full set once when legacy IDs disagree with fixture order."""
    data = load_sample_data()
    agent = (
        await client.post("/api/v1/agents", json=data.agent.model_dump(mode="json"))
    ).json()
    legacy: dict[str, str] = {}
    for item in reversed(data.sessions[:existing_count]):
        response = await client.post(
            "/api/v1/sessions",
            json=item.session.model_dump(mode="json") | {"agent_id": agent["id"]},
        )
        assert response.status_code == 201
        legacy[response.json()["external_id"]] = response.json()["id"]
    fixture_order = [
        legacy[item.session.external_id]
        for item in data.sessions[:existing_count]
        if item.session.external_id is not None
    ]
    assert fixture_order != sorted(fixture_order)
    batches: list[set[uuid.UUID]] = []
    get_many = SQLSessionRepository.get_many
    get = SQLSessionRepository.get

    async def capture_batch(
        self: SQLSessionRepository,
        session_ids: Sequence[uuid.UUID],
        include_payloads: bool,
        exclusive: bool = False,
    ) -> dict[uuid.UUID, Session]:
        if exclusive:
            assert not batches
            assert len(session_ids) == len(data.sessions)
            assert list(session_ids) == sorted(session_ids)
        result = await get_many(self, session_ids, include_payloads, exclusive)
        if exclusive:
            batches.append(set(result))
        return result

    async def require_complete_batch(
        self: SQLSessionRepository,
        session_id: uuid.UUID,
        include_payloads: bool,
        exclusive: bool = False,
    ) -> Session:
        if exclusive:
            assert len(batches) == 1, "Lock the complete set before per-session writes"
            assert session_id in batches[0]
        return await get(self, session_id, include_payloads, exclusive)

    monkeypatch.setattr(SQLSessionRepository, "get_many", capture_batch)
    monkeypatch.setattr(SQLSessionRepository, "get", require_complete_batch)
    response = await client.post(
        "/api/v1/ui/sample-data", json={"resume_existing": True}
    )
    assert response.status_code == 201
    assert len(batches) == 1
    assert {uuid.UUID(value) for value in legacy.values()} <= batches[0]
    sessions = (
        await client.get("/api/v1/sessions", params=_agent_filter(agent["id"]))
    ).json()["items"]
    assert len(sessions) == len(data.sessions)
    assert {
        row["external_id"]: row["id"]
        for row in sessions
        if row["external_id"] in legacy
    } == legacy
    assert all(row["agent_version_id"] is not None for row in sessions)


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


async def test_sample_data_idempotency_replays_tour_identifiers(
    client: httpx.AsyncClient,
) -> None:
    """Return the same completed tour after a response is lost."""
    headers = {"Idempotency-Key": "sample-tour-retry"}
    body = {"agent_name": "sample-returns-agent", "resume_existing": True}
    first = await client.post("/api/v1/ui/sample-data", json=body, headers=headers)
    second = await client.post("/api/v1/ui/sample-data", json=body, headers=headers)
    assert first.status_code == second.status_code == 201
    assert first.json() == second.json()
    result = first.json()
    assert result["agent_name"] == body["agent_name"]
    session = (
        await client.get(f"/api/v1/sessions/{result['failing_session_id']}")
    ).json()
    assert session["external_id"] == "kitaru-template:returns-ticket-007"
    assert session["number"] == result["failing_session_number"]
    assert session["agent_version_id"] is not None
    investigation = await client.get(
        f"/api/v1/investigations/{result['investigation_id']}"
    )
    assert investigation.status_code == 200


async def test_sample_data_resume_keeps_existing_resources(
    client: httpx.AsyncClient,
) -> None:
    """Resume completed sample data without multiplying resources or evaluations."""
    body = {"resume_existing": True}
    first = await client.post("/api/v1/ui/sample-data", json=body)
    before = (
        await client.get(
            "/api/v1/ui/sessions", params=_agent_filter(first.json()["agent_id"])
        )
    ).json()
    transport = client._transport
    assert isinstance(transport, httpx.ASGITransport)
    assert isinstance(transport.app, FastAPI)
    database: DatabaseService = transport.app.state.database
    async with database.engine.connect() as connection:
        configs_before = await connection.scalar(
            select(func.count()).select_from(ReplayConfigORM)
        )
    second = await client.post("/api/v1/ui/sample-data", json=body)
    async with database.engine.connect() as connection:
        configs_after = await connection.scalar(
            select(func.count()).select_from(ReplayConfigORM)
        )
    assert configs_before == configs_after == 1
    after = (
        await client.get(
            "/api/v1/ui/sessions", params=_agent_filter(first.json()["agent_id"])
        )
    ).json()
    assert first.status_code == second.status_code == 201
    assert first.json() == second.json()
    assert [item["session"]["id"] for item in before["items"]] == [
        item["session"]["id"] for item in after["items"]
    ]
    assert [item["evaluations"] for item in before["items"]] == [
        item["evaluations"] for item in after["items"]
    ]


async def test_sample_data_resume_rejects_ordinary_agent(
    client: httpx.AsyncClient,
) -> None:
    """Leave an ordinary same-name agent untouched."""
    agent = await client.post(
        "/api/v1/agents",
        json={"name": "sample-returns-agent", "description": "My production agent"},
    )
    assert agent.status_code == 201
    response = await client.post(
        "/api/v1/ui/sample-data",
        json={"agent_name": "sample-returns-agent", "resume_existing": True},
    )
    assert response.status_code == 409
    sessions = (await client.get("/api/v1/sessions")).json()
    assert sessions["items"] == []


async def test_sample_data_failure_rolls_back_key_and_seed_writes(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retry a failed seed with the same key after its request writes roll back."""
    original = SampleDataSeeder._seed_experiment

    async def fail_experiment(*args: object, **kwargs: object) -> None:
        raise RuntimeError("sample experiment failed")

    body = {"resume_existing": True}
    headers = {"Idempotency-Key": "failed-sample-attempt"}
    monkeypatch.setattr(SampleDataSeeder, "_seed_experiment", fail_experiment)
    with pytest.raises(RuntimeError, match="sample experiment failed"):
        await client.post("/api/v1/ui/sample-data", json=body, headers=headers)
    assert (await client.get("/api/v1/sessions")).json()["items"] == []
    assert (await client.get("/api/v1/cohorts")).json()["items"] == []
    agents = (await client.get("/api/v1/agents")).json()["items"]
    assert len(agents) == 1
    versions = (await client.get(f"/api/v1/agents/{agents[0]['id']}/versions")).json()[
        "items"
    ]
    assert len(versions) == 1
    monkeypatch.setattr(SampleDataSeeder, "_seed_experiment", original)
    response = await client.post("/api/v1/ui/sample-data", json=body, headers=headers)
    assert response.status_code == 201
    replay = await client.post("/api/v1/ui/sample-data", json=body, headers=headers)
    assert replay.status_code == 201
    assert replay.json() == response.json()


async def test_sample_data_concurrent_same_key_replays_completed_seed(
    client: httpx.AsyncClient,
) -> None:
    """Serialize two attempts with the same key into one complete seed."""

    async def load() -> httpx.Response:
        return await client.post(
            "/api/v1/ui/sample-data",
            json={"resume_existing": True},
            headers={"Idempotency-Key": "concurrent-sample-attempt"},
        )

    first, second = await asyncio.wait_for(asyncio.gather(load(), load()), timeout=20)
    assert first.status_code == second.status_code == 201
    assert first.json() == second.json()
    sessions = (await client.get("/api/v1/sessions", params={"size": 100})).json()
    assert len(sessions["items"]) == len(load_sample_data().sessions)


@pytest.mark.parametrize("legacy_version_name", [None, "baseline-v1", "recorded-v2"])
async def test_sample_data_repairs_partial_legacy_seed(
    client: httpx.AsyncClient,
    legacy_version_name: str | None,
) -> None:
    """Complete nodes and missing evaluations while retaining existing judgments."""
    data = load_sample_data()
    agent = (
        await client.post("/api/v1/agents", json=data.agent.model_dump(mode="json"))
    ).json()
    version_id = None
    if legacy_version_name is not None:
        version_response = await client.post(
            f"/api/v1/agents/{agent['id']}/versions",
            json=data.agent_version.model_dump(mode="json")
            | {"display_version": legacy_version_name},
        )
        assert version_response.status_code == 201
        version_id = version_response.json()["id"]
    item = data.sessions[0]
    command = item.session.model_dump(mode="json") | {
        "agent_id": agent["id"],
        "agent_version_id": version_id,
    }
    session = (await client.post("/api/v1/sessions", json=command)).json()
    node_response = await client.post(
        f"/api/v1/sessions/{session['id']}/nodes",
        json={"nodes": [node.model_dump(mode="json") for node in item.nodes[:1]]},
    )
    assert node_response.status_code == 200
    original_node_ids = {node["id"] for node in node_response.json()}
    evaluation_response = await client.post(
        f"/api/v1/sessions/{session['id']}/evaluations",
        json={
            "evaluations": [
                item.evaluations[0].model_dump(mode="json")
                | {"explanation": "Preserve this reviewed judgment"}
            ]
        },
    )
    assert evaluation_response.status_code == 200
    evaluation_id = evaluation_response.json()[0]["id"]
    response = await client.post(
        "/api/v1/ui/sample-data", json={"resume_existing": True}
    )
    assert response.status_code == 201
    sessions = (
        await client.get("/api/v1/ui/sessions", params=_agent_filter(agent["id"]))
    ).json()["items"]
    assert len(sessions) == len(data.sessions)
    repaired = next(row for row in sessions if row["session"]["id"] == session["id"])
    assert len(repaired["evaluations"]) == len(item.evaluations)
    reviewed = next(
        row for row in repaired["evaluations"] if row["id"] == evaluation_id
    )
    assert reviewed["explanation"] == "Preserve this reviewed judgment"
    nodes = (
        await client.get(
            f"/api/v1/sessions/{session['id']}/nodes", params={"size": 100}
        )
    ).json()["items"]
    assert len(nodes) == len(item.nodes)
    versions = (await client.get(f"/api/v1/agents/{agent['id']}/versions")).json()[
        "items"
    ]
    baseline = [row for row in versions if row["display_version"] == "baseline-v1"]
    assert len(baseline) == 1
    assert original_node_ids <= {node["id"] for node in nodes}
    assert repaired["session"]["agent_version_id"] == (version_id or baseline[0]["id"])
    assert all(
        row["session"]["agent_version_id"] == baseline[0]["id"]
        for row in sessions
        if row["session"]["id"] != session["id"]
    )
    assert len(versions) == (2 if legacy_version_name == "recorded-v2" else 1)


async def test_sample_data_keeps_foreign_session_separate(
    client: httpx.AsyncClient,
) -> None:
    """Keep another agent's matching external ID out of the sample data."""
    agent = (
        await client.post("/api/v1/agents", json={"name": "ordinary-agent"})
    ).json()
    item = load_sample_data().sessions[0]
    command = item.session.model_dump(mode="json") | {"agent_id": agent["id"]}
    session = (await client.post("/api/v1/sessions", json=command)).json()
    response = await client.post(
        "/api/v1/ui/sample-data", json={"resume_existing": True}
    )
    assert response.status_code == 201
    foreign = (await client.get(f"/api/v1/sessions/{session['id']}")).json()
    assert {key: foreign[key] for key in session} == session
    nodes = (await client.get(f"/api/v1/sessions/{session['id']}/nodes")).json()
    assert nodes["items"] == []
    sample = (
        await client.get(
            "/api/v1/sessions", params=_agent_filter(response.json()["agent_id"])
        )
    ).json()["items"]
    assert len(sample) == len(load_sample_data().sessions)
    assert session["id"] not in {row["id"] for row in sample}


async def test_sample_data_rejects_unmarked_existing_session(
    client: httpx.AsyncClient,
) -> None:
    """Do not overwrite ordinary data recorded under a sample agent."""
    data = load_sample_data()
    agent = (
        await client.post("/api/v1/agents", json=data.agent.model_dump(mode="json"))
    ).json()
    item = data.sessions[0]
    command = item.session.model_dump(mode="json") | {
        "agent_id": agent["id"],
        "metadata": {},
    }
    session = (await client.post("/api/v1/sessions", json=command)).json()
    response = await client.post(
        "/api/v1/ui/sample-data", json={"resume_existing": True}
    )
    assert response.status_code == 409
    stored = (await client.get(f"/api/v1/sessions/{session['id']}")).json()
    assert {key: stored[key] for key in session} == session
    nodes = (await client.get(f"/api/v1/sessions/{session['id']}/nodes")).json()
    assert nodes["items"] == []


async def test_sample_data_concurrent_recovery_creates_one_baseline(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Serialize baseline creation while two independent recovery intents race."""
    data = load_sample_data()
    agent = (
        await client.post("/api/v1/agents", json=data.agent.model_dump(mode="json"))
    ).json()
    original = AgentVersionService.list_versions

    async def delayed_empty_versions(
        self: AgentVersionService,
        version_filter: AgentVersionFilter,
        actor: AuthContext,
    ) -> tuple[list[AgentVersion], str | None]:
        result = await original(self, version_filter, actor)
        if str(version_filter.agent_id) == agent["id"] and not result[0]:
            await asyncio.sleep(0.05)
        return result

    monkeypatch.setattr(AgentVersionService, "list_versions", delayed_empty_versions)

    async def load(key: str) -> httpx.Response:
        return await client.post(
            "/api/v1/ui/sample-data",
            json={"resume_existing": True},
            headers={"Idempotency-Key": key},
        )

    first, second = await asyncio.wait_for(
        asyncio.gather(load("first-recovery"), load("second-recovery")), timeout=20
    )
    assert first.status_code == second.status_code == 201
    assert first.json() == second.json()
    versions = (await client.get(f"/api/v1/agents/{agent['id']}/versions")).json()[
        "items"
    ]
    assert [row["display_version"] for row in versions] == ["baseline-v1"]
    sessions = (
        await client.get("/api/v1/sessions", params=_agent_filter(agent["id"]))
    ).json()["items"]
    assert len(sessions) == len(data.sessions)
    assert {row["agent_version_id"] for row in sessions} == {versions[0]["id"]}


async def test_sample_data_bootstrap_overlaps_existing_seed_safely(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Prepare a second recovery while the first seed holds a session foreign key."""
    data = load_sample_data()
    agent = (
        await client.post("/api/v1/agents", json=data.agent.model_dump(mode="json"))
    ).json()
    first_session_created = asyncio.Event()
    second_bootstrap_locked = asyncio.Event()
    create_session = SessionService.create_session
    list_versions = AgentVersionService.list_versions

    async def pause_first_session(
        self: SessionService, command: SessionCreate, actor: AuthContext
    ) -> Session:
        session = await create_session(self, command, actor)
        if not first_session_created.is_set():
            first_session_created.set()
            await second_bootstrap_locked.wait()
        return session

    async def observe_second_bootstrap(
        self: AgentVersionService,
        version_filter: AgentVersionFilter,
        actor: AuthContext,
    ) -> tuple[list[AgentVersion], str | None]:
        result = await list_versions(self, version_filter, actor)
        if first_session_created.is_set():
            second_bootstrap_locked.set()
        return result

    monkeypatch.setattr(SessionService, "create_session", pause_first_session)
    monkeypatch.setattr(AgentVersionService, "list_versions", observe_second_bootstrap)

    async def load(key: str) -> httpx.Response:
        return await client.post(
            "/api/v1/ui/sample-data",
            json={"resume_existing": True},
            headers={"Idempotency-Key": key},
        )

    first_task = asyncio.create_task(load("seed-before-bootstrap"))
    await asyncio.wait_for(first_session_created.wait(), timeout=10)
    second_task = asyncio.create_task(load("bootstrap-during-seed"))
    first, second = await asyncio.wait_for(
        asyncio.gather(first_task, second_task), timeout=20
    )
    assert first.status_code == second.status_code == 201
    assert first.json() == second.json()
    versions = (await client.get(f"/api/v1/agents/{agent['id']}/versions")).json()[
        "items"
    ]
    assert [row["display_version"] for row in versions] == ["baseline-v1"]
    sessions = (
        await client.get("/api/v1/sessions", params=_agent_filter(agent["id"]))
    ).json()["items"]
    assert len(sessions) == len(data.sessions)


async def test_sample_data_links_all_unversioned_legacy_sessions(
    client: httpx.AsyncClient,
) -> None:
    """Bind an old server's complete seed without replacing recorded data."""
    data = load_sample_data()
    agent = (
        await client.post("/api/v1/agents", json=data.agent.model_dump(mode="json"))
    ).json()
    session_ids: dict[str, str] = {}
    node_ids: dict[str, set[str]] = {}
    judgments: dict[str, dict[str, str | None]] = {}
    for item in data.sessions:
        session_response = await client.post(
            "/api/v1/sessions",
            json=item.session.model_dump(mode="json") | {"agent_id": agent["id"]},
        )
        assert session_response.status_code == 201
        session = session_response.json()
        assert session["agent_version_id"] is None
        external_id = item.session.external_id
        assert external_id is not None
        session_ids[external_id] = session["id"]
        nodes = await client.post(
            f"/api/v1/sessions/{session['id']}/nodes",
            json={"nodes": [node.model_dump(mode="json") for node in item.nodes]},
        )
        assert nodes.status_code == 200
        node_ids[session["id"]] = {node["id"] for node in nodes.json()}
        evaluations = await client.post(
            f"/api/v1/sessions/{session['id']}/evaluations",
            json={
                "evaluations": [
                    result.model_dump(mode="json")
                    | {"explanation": "Retain this legacy review"}
                    for result in item.evaluations
                ]
            },
        )
        assert evaluations.status_code == 200
        judgments[session["id"]] = {
            row["id"]: row["explanation"] for row in evaluations.json()
        }
    response = await client.post(
        "/api/v1/ui/sample-data", json={"resume_existing": True}
    )
    assert response.status_code == 201
    versions = (await client.get(f"/api/v1/agents/{agent['id']}/versions")).json()[
        "items"
    ]
    assert [version["display_version"] for version in versions] == ["baseline-v1"]
    sessions = (
        await client.get("/api/v1/ui/sessions", params=_agent_filter(agent["id"]))
    ).json()["items"]
    assert len(sessions) == len(data.sessions) == 10
    assert {
        row["session"]["external_id"]: row["session"]["id"] for row in sessions
    } == session_ids
    assert {row["session"]["agent_version_id"] for row in sessions} == {
        versions[0]["id"]
    }
    assert (
        response.json()["failing_session_id"]
        == session_ids[data.failing_session_external_id]
    )
    for row in sessions:
        session_id = row["session"]["id"]
        assert {
            evaluation["id"]: evaluation["explanation"]
            for evaluation in row["evaluations"]
        } == judgments[session_id]
        nodes = (
            await client.get(
                f"/api/v1/sessions/{session_id}/nodes", params={"size": 100}
            )
        ).json()["items"]
        assert {node["id"] for node in nodes} == node_ids[session_id]
