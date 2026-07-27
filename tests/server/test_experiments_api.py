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
"""Tests for the experiment routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import EXPERIMENT_APP_ACCOUNT_ID, experiment_app

SCORING_POLICY = {
    "scorers": [
        {
            "type": "source",
            "name": "conciseness",
            "source": "my_pkg.scorers:conciseness",
        }
    ],
    "pass_threshold": 0.5,
}

SCORING_POLICY_RESPONSE = {
    "scorers": [
        {
            "type": "source",
            "name": "conciseness",
            "source": "my_pkg.scorers:conciseness",
            "params": {},
            "weight": 1.0,
            "fail_below": None,
        }
    ],
    "pass_threshold": 0.5,
}


@pytest.fixture
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed services."""
    transport = httpx.ASGITransport(app=experiment_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def create_agent(client: httpx.AsyncClient, name: str = "support-bot") -> str:
    """Store an agent through the API.

    Args:
        client: HTTP client for the app.
        name: Agent name.

    Returns:
        Id of the created agent.
    """
    response = await client.post("/v1/agents", json={"name": name})
    assert response.status_code == 201
    return response.json()["id"]


async def create_runnable_version(
    client: httpx.AsyncClient,
    agent_id: str,
    version: str = "v1",
    **run_spec_overrides: object,
) -> str:
    """Store a runnable agent version through the API.

    Args:
        client: HTTP client for the app.
        agent_id: Id of the agent.
        version: Version label.
        **run_spec_overrides: Run spec overrides.

    Returns:
        Id of the created agent version.
    """
    response = await client.post(
        f"/v1/agents/{agent_id}/versions",
        json={
            "version": version,
            "run_spec": {
                "command": "python agent.py",
                "timeout_seconds": 600,
                **run_spec_overrides,
            },
        },
    )
    assert response.status_code == 201
    return response.json()["id"]


async def create_completed_session(client: httpx.AsyncClient, agent_id: str) -> str:
    """Store a completed recorded session through the API.

    Args:
        client: HTTP client for the app.
        agent_id: Id of the agent.

    Returns:
        Id of the created session.
    """
    response = await client.post(
        "/v1/sessions", json={"agent_id": agent_id, "origin": "recorded"}
    )
    assert response.status_code == 201
    session_id = response.json()["id"]
    response = await client.patch(
        f"/v1/sessions/{session_id}", json={"status": "completed"}
    )
    assert response.status_code == 200
    return session_id


async def create_cohort(
    client: httpx.AsyncClient,
    agent_id: str,
    session_count: int = 1,
    name: str = "baseline",
) -> str:
    """Store a cohort with completed member sessions through the API.

    Args:
        client: HTTP client for the app.
        agent_id: Id of the agent.
        session_count: Number of member sessions.
        name: Cohort name.

    Returns:
        Id of the created cohort.
    """
    session_ids = [
        await create_completed_session(client, agent_id) for _ in range(session_count)
    ]
    response = await client.post(
        "/v1/cohorts",
        json={"name": name, "agent_id": agent_id, "session_ids": session_ids},
    )
    assert response.status_code == 201
    return response.json()["id"]


async def create_experiment(
    client: httpx.AsyncClient, cohort_id: str, **overrides: object
) -> dict:
    """Store an experiment through the API.

    Args:
        client: HTTP client for the app.
        cohort_id: Id of the cohort.
        **overrides: Create request body overrides.

    Returns:
        Created experiment body.
    """
    body: dict[str, object] = {
        "name": "swap-model",
        "cohort_id": cohort_id,
        "scoring_policy": SCORING_POLICY,
        **overrides,
    }
    response = await client.post("/v1/experiments", json=body)
    assert response.status_code == 201
    return response.json()


async def test_create_experiment_defaults(client: httpx.AsyncClient) -> None:
    """Create an experiment and observe the passthrough default."""
    agent_id = await create_agent(client)
    cohort_id = await create_cohort(client, agent_id)
    response = await client.post(
        "/v1/experiments",
        json={
            "name": "swap-model",
            "description": "Swap the model",
            "cohort_id": cohort_id,
            "scoring_policy": SCORING_POLICY,
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "swap-model"
    assert body["description"] == "Swap the model"
    assert body["owner_id"] == str(EXPERIMENT_APP_ACCOUNT_ID)
    assert body["cohort_id"] == cohort_id
    assert body["override"] is None
    assert body["tool_policy"] == {"default": {"type": "passthrough"}, "tools": {}}
    assert body["scoring_policy"] == SCORING_POLICY_RESPONSE
    assert body["created"] is not None
    assert body["updated"] is not None
    assert uuid.UUID(body["id"])


async def test_create_experiment_inlines_config(client: httpx.AsyncClient) -> None:
    """Round-trip an inline config through the experiment response."""
    agent_id = await create_agent(client)
    cohort_id = await create_cohort(client, agent_id)
    override = {
        "model": {"gpt-4o": "claude-sonnet-5"},
        "system_prompt": "Be terse.",
        "prompt": None,
        "model_params": {"temperature": 0.2},
    }
    tool_policy = {
        "default": {
            "type": "history",
            "scope": "original_session",
            "on_miss": "fail",
        },
        "tools": {
            "search": {"type": "passthrough"},
            "get_weather": {
                "type": "static",
                "cases": [
                    {
                        "match": {"city": "Berlin"},
                        "match_mode": "exact",
                        "result": {"temperature": 21},
                    }
                ],
                "on_miss": "error_result",
            },
        },
    }
    body = await create_experiment(
        client, cohort_id, override=override, tool_policy=tool_policy
    )
    assert body["override"] == override
    assert body["tool_policy"] == tool_policy

    response = await client.get(f"/v1/experiments/{body['id']}")
    assert response.status_code == 200
    assert response.json() == body


async def test_create_experiment_invalid_tool_policy_type(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 for an unknown tool policy type."""
    agent_id = await create_agent(client)
    cohort_id = await create_cohort(client, agent_id)
    response = await client.post(
        "/v1/experiments",
        json={
            "name": "swap-model",
            "cohort_id": cohort_id,
            "scoring_policy": SCORING_POLICY,
            "tool_policy": {"default": {"type": "bogus"}, "tools": {}},
        },
    )
    assert response.status_code == 422


async def test_create_experiment_empty_scorers(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for an empty scorer list."""
    agent_id = await create_agent(client)
    cohort_id = await create_cohort(client, agent_id)
    response = await client.post(
        "/v1/experiments",
        json={
            "name": "swap-model",
            "cohort_id": cohort_id,
            "scoring_policy": {"scorers": [], "pass_threshold": 0.5},
        },
    )
    assert response.status_code == 422


async def test_create_experiment_invalid_source(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a source without a module and attribute."""
    agent_id = await create_agent(client)
    cohort_id = await create_cohort(client, agent_id)
    response = await client.post(
        "/v1/experiments",
        json={
            "name": "swap-model",
            "cohort_id": cohort_id,
            "scoring_policy": {
                "scorers": [
                    {
                        "type": "source",
                        "name": "conciseness",
                        "source": "conciseness",
                    }
                ],
                "pass_threshold": 0.5,
            },
        },
    )
    assert response.status_code == 422


async def test_create_experiment_missing_scoring_policy(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 without a scoring policy."""
    agent_id = await create_agent(client)
    cohort_id = await create_cohort(client, agent_id)
    response = await client.post(
        "/v1/experiments", json={"name": "swap-model", "cohort_id": cohort_id}
    )
    assert response.status_code == 422


async def test_create_experiment_unknown_cohort(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown cohort id."""
    missing_id = uuid.uuid4()
    response = await client.post(
        "/v1/experiments",
        json={
            "name": "swap-model",
            "cohort_id": str(missing_id),
            "scoring_policy": SCORING_POLICY,
        },
    )
    assert response.status_code == 404
    assert response.json() == {"detail": f"Cohort {missing_id} was not found"}


async def test_create_experiment_duplicate_name(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a duplicate experiment name."""
    agent_id = await create_agent(client)
    cohort_id = await create_cohort(client, agent_id)
    await create_experiment(client, cohort_id)
    response = await client.post(
        "/v1/experiments",
        json={
            "name": "swap-model",
            "cohort_id": cohort_id,
            "scoring_policy": SCORING_POLICY,
        },
    )
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Experiment name 'swap-model' is already registered"
    }


async def test_list_experiments(client: httpx.AsyncClient) -> None:
    """List experiments with filters and pagination."""
    agent_id = await create_agent(client)
    cohort_id = await create_cohort(client, agent_id)
    for name in ["one", "two", "three"]:
        await create_experiment(client, cohort_id, name=name)

    response = await client.get("/v1/experiments")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 3
    assert [item["name"] for item in body["items"]] == ["one", "two", "three"]

    response = await client.get("/v1/experiments", params={"page": 2, "page_size": 2})
    assert response.status_code == 200
    assert [item["name"] for item in response.json()["items"]] == ["three"]

    response = await client.get("/v1/experiments", params={"name": "two"})
    assert response.status_code == 200
    assert response.json()["total"] == 1


async def test_list_experiments_by_tag(client: httpx.AsyncClient) -> None:
    """List experiments attached to a tag name."""
    agent_id = await create_agent(client)
    cohort_id = await create_cohort(client, agent_id)
    tagged = await create_experiment(client, cohort_id, name="tagged")
    await create_experiment(client, cohort_id, name="other")
    response = await client.post("/v1/tags", json={"name": "prod"})
    assert response.status_code == 201
    tag_id = response.json()["id"]
    response = await client.post(
        f"/v1/tags/{tag_id}/links",
        json={"resource_type": "experiment", "resource_id": tagged["id"]},
    )
    assert response.status_code == 201

    response = await client.get("/v1/experiments", params={"tag": "prod"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == tagged["id"]


async def test_get_experiment_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown experiment id."""
    missing_id = uuid.uuid4()
    response = await client.get(f"/v1/experiments/{missing_id}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Experiment {missing_id} was not found"}


async def test_update_experiment_config(client: httpx.AsyncClient) -> None:
    """Replace config parts before any run and keep the rest."""
    agent_id = await create_agent(client)
    cohort_id = await create_cohort(client, agent_id)
    created = await create_experiment(client, cohort_id)
    response = await client.patch(
        f"/v1/experiments/{created['id']}",
        json={"override": {"model": "claude-sonnet-5"}},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["override"] == {
        "model": "claude-sonnet-5",
        "system_prompt": None,
        "prompt": None,
        "model_params": None,
    }
    assert body["tool_policy"] == created["tool_policy"]
    assert body["scoring_policy"] == created["scoring_policy"]


async def test_update_experiment_absent_fields_unchanged(
    client: httpx.AsyncClient,
) -> None:
    """Keep every field on an update with an empty body."""
    agent_id = await create_agent(client)
    cohort_id = await create_cohort(client, agent_id)
    created = await create_experiment(client, cohort_id)
    response = await client.patch(f"/v1/experiments/{created['id']}", json={})
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == created["name"]
    assert body["description"] == created["description"]
    assert body["cohort_id"] == created["cohort_id"]
    assert body["override"] == created["override"]
    assert body["tool_policy"] == created["tool_policy"]
    assert body["scoring_policy"] == created["scoring_policy"]


async def test_update_experiment_null_clears_nullable_fields(
    client: httpx.AsyncClient,
) -> None:
    """Clear the description and override on explicit nulls."""
    agent_id = await create_agent(client)
    cohort_id = await create_cohort(client, agent_id)
    created = await create_experiment(client, cohort_id)
    response = await client.patch(
        f"/v1/experiments/{created['id']}",
        json={
            "description": "First try",
            "override": {"model": "claude-sonnet-5"},
        },
    )
    assert response.status_code == 200
    response = await client.patch(
        f"/v1/experiments/{created['id']}",
        json={"description": None, "override": None},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["description"] is None
    assert body["override"] is None


async def test_update_experiment_null_required_fields_rejected(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 422 for explicit nulls on required fields."""
    agent_id = await create_agent(client)
    cohort_id = await create_cohort(client, agent_id)
    created = await create_experiment(client, cohort_id)
    response = await client.patch(
        f"/v1/experiments/{created['id']}", json={"name": None}
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Experiment name cannot be null"}
    response = await client.patch(
        f"/v1/experiments/{created['id']}", json={"cohort_id": None}
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Experiment cohort id cannot be null"}
    response = await client.patch(
        f"/v1/experiments/{created['id']}", json={"tool_policy": None}
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Experiment tool policy cannot be null"}
    response = await client.patch(
        f"/v1/experiments/{created['id']}", json={"scoring_policy": None}
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "Experiment scoring policy cannot be null"}


async def test_update_experiment_frozen_after_run(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a config change once a run exists."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    cohort_id = await create_cohort(client, agent_id)
    created = await create_experiment(client, cohort_id)
    response = await client.post(f"/v1/experiments/{created['id']}/runs", json={})
    assert response.status_code == 201

    response = await client.patch(
        f"/v1/experiments/{created['id']}",
        json={"override": {"model": "claude-sonnet-5"}},
    )
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Experiment {created['id']} is frozen by existing runs"
    }

    response = await client.patch(
        f"/v1/experiments/{created['id']}", json={"name": "renamed"}
    )
    assert response.status_code == 200
    assert response.json()["name"] == "renamed"


async def test_delete_experiment(client: httpx.AsyncClient) -> None:
    """Delete a run-less experiment and observe HTTP 204."""
    agent_id = await create_agent(client)
    cohort_id = await create_cohort(client, agent_id)
    created = await create_experiment(client, cohort_id)
    response = await client.delete(f"/v1/experiments/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/experiments/{created['id']}")
    assert response.status_code == 404


async def test_delete_experiment_with_runs(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 when deleting an experiment with runs."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    cohort_id = await create_cohort(client, agent_id)
    created = await create_experiment(client, cohort_id)
    response = await client.post(f"/v1/experiments/{created['id']}/runs", json={})
    assert response.status_code == 201

    response = await client.delete(f"/v1/experiments/{created['id']}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Experiment {created['id']} is referenced by experiment runs"
    }


async def test_delete_cohort_referenced_by_experiment(
    client: httpx.AsyncClient,
) -> None:
    """Observe HTTP 409 when deleting a cohort an experiment references."""
    agent_id = await create_agent(client)
    cohort_id = await create_cohort(client, agent_id)
    created = await create_experiment(client, cohort_id)

    response = await client.delete(f"/v1/cohorts/{cohort_id}")
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Cohort {cohort_id} is referenced by experiments"
    }

    response = await client.delete(f"/v1/experiments/{created['id']}")
    assert response.status_code == 204
    response = await client.delete(f"/v1/cohorts/{cohort_id}")
    assert response.status_code == 204


async def test_create_run_fans_out(client: httpx.AsyncClient) -> None:
    """Start a run and observe the pending job fan-out."""
    agent_id = await create_agent(client)
    version_id = await create_runnable_version(client, agent_id)
    cohort_id = await create_cohort(client, agent_id, session_count=3)
    created = await create_experiment(client, cohort_id)
    response = await client.post(
        f"/v1/experiments/{created['id']}/runs", json={"score_baselines": True}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["experiment_id"] == created["id"]
    assert body["number"] == 1
    assert body["status"] == "pending"
    assert body["agent_version_id"] == version_id
    assert body["score_baselines"] is True
    assert body["execution_target"] == "pool"
    assert body["executor_handle"] is None
    assert body["summary"] is None
    assert body["progress"] == {
        "pending": 3,
        "claimed": 0,
        "running": 0,
        "completed": 0,
        "failed": 0,
        "timed_out": 0,
        "canceled": 0,
        "total": 3,
    }

    response = await client.post(f"/v1/experiments/{created['id']}/runs", json={})
    assert response.status_code == 201
    assert response.json()["number"] == 2


async def test_create_run_explicit_execution_target(
    client: httpx.AsyncClient,
) -> None:
    """Start an on demand run with an explicit execution target."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id, image="ghcr.io/acme/agent:v1")
    cohort_id = await create_cohort(client, agent_id)
    created = await create_experiment(client, cohort_id)
    response = await client.post(
        f"/v1/experiments/{created['id']}/runs",
        json={"execution_target": "on_demand"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["execution_target"] == "on_demand"
    assert body["executor_handle"] is None


async def test_create_run_default_execution_target_from_run_spec(
    client: httpx.AsyncClient,
) -> None:
    """Default the execution target to the run spec default."""
    agent_id = await create_agent(client)
    await create_runnable_version(
        client,
        agent_id,
        image="ghcr.io/acme/agent:v1",
        default_execution_target="on_demand",
    )
    cohort_id = await create_cohort(client, agent_id)
    created = await create_experiment(client, cohort_id)
    response = await client.post(f"/v1/experiments/{created['id']}/runs", json={})
    assert response.status_code == 201
    assert response.json()["execution_target"] == "on_demand"


async def test_create_run_on_demand_without_image(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for an on demand run without an image."""
    agent_id = await create_agent(client)
    version_id = await create_runnable_version(client, agent_id)
    cohort_id = await create_cohort(client, agent_id)
    created = await create_experiment(client, cohort_id)
    response = await client.post(
        f"/v1/experiments/{created['id']}/runs",
        json={"execution_target": "on_demand"},
    )
    assert response.status_code == 409
    assert response.json() == {"detail": f"Agent version {version_id} has no run image"}


async def test_create_run_no_runnable_version(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 when no runnable version resolves."""
    agent_id = await create_agent(client)
    cohort_id = await create_cohort(client, agent_id)
    created = await create_experiment(client, cohort_id)
    response = await client.post(f"/v1/experiments/{created['id']}/runs", json={})
    assert response.status_code == 409
    assert response.json() == {"detail": f"Agent {agent_id} has no runnable version"}


async def test_create_run_cross_agent_version(client: httpx.AsyncClient) -> None:
    """Observe HTTP 422 for a version of another agent."""
    agent_id = await create_agent(client)
    other_id = await create_agent(client, name="triage-bot")
    other_version_id = await create_runnable_version(client, other_id)
    cohort_id = await create_cohort(client, agent_id)
    created = await create_experiment(client, cohort_id)
    response = await client.post(
        f"/v1/experiments/{created['id']}/runs",
        json={"agent_version_id": other_version_id},
    )
    assert response.status_code == 422
    assert response.json() == {
        "detail": f"Agent version {other_version_id} does not belong to "
        f"agent {agent_id}"
    }


async def test_create_run_unknown_experiment(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown experiment id."""
    missing_id = uuid.uuid4()
    response = await client.post(f"/v1/experiments/{missing_id}/runs", json={})
    assert response.status_code == 404
    assert response.json() == {"detail": f"Experiment {missing_id} was not found"}


async def test_list_experiment_runs(client: httpx.AsyncClient) -> None:
    """List the runs of an experiment."""
    agent_id = await create_agent(client)
    await create_runnable_version(client, agent_id)
    cohort_id = await create_cohort(client, agent_id)
    created = await create_experiment(client, cohort_id)
    for _ in range(2):
        response = await client.post(f"/v1/experiments/{created['id']}/runs", json={})
        assert response.status_code == 201

    response = await client.get(f"/v1/experiments/{created['id']}/runs")
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert [item["number"] for item in body["items"]] == [1, 2]
    assert all(item["progress"]["total"] == 1 for item in body["items"])

    response = await client.get(f"/v1/experiments/{uuid.uuid4()}/runs")
    assert response.status_code == 404


async def test_agent_version_frozen_by_job(client: httpx.AsyncClient) -> None:
    """Observe HTTP 409 for a run spec change on a replayed version."""
    agent_id = await create_agent(client)
    version_id = await create_runnable_version(client, agent_id)
    cohort_id = await create_cohort(client, agent_id)
    created = await create_experiment(client, cohort_id)
    response = await client.post(f"/v1/experiments/{created['id']}/runs", json={})
    assert response.status_code == 201

    response = await client.patch(
        f"/v1/agent-versions/{version_id}",
        json={"run_spec": {"command": "python agent2.py", "timeout_seconds": 60}},
    )
    assert response.status_code == 409
    assert response.json() == {
        "detail": f"Agent version {version_id} is frozen by existing jobs"
    }

    response = await client.patch(
        f"/v1/agent-versions/{version_id}", json={"description": "Still editable"}
    )
    assert response.status_code == 200
    assert response.json()["description"] == "Still editable"
