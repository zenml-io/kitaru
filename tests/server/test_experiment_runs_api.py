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
"""Tests for the experiment run routes."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    ReplayServices,
    build_replay_services,
    create_agent,
    create_agent_version,
    create_blob,
    create_cohort,
    create_plugin,
    create_session,
)
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_experiment_run_service,
    get_experiment_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.experiment import ExperimentCreate
from kitaru.server.application.models.replay_config import EvaluatorConfigInput
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import RunSpec
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def services() -> ReplayServices:
    """Provide fake-backed experiment, replay, and run services."""
    return build_replay_services()


@pytest.fixture
async def client(services: ReplayServices) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed experiment run services."""
    app = create_app(
        APISettings(DB_HOST="localhost", SECRET_ENCRYPTION_KEY="test-encryption-key")
    )
    app.dependency_overrides[get_experiment_service] = lambda: (
        services.experiment_service
    )
    app.dependency_overrides[get_experiment_run_service] = lambda: (
        services.experiment_run_service
    )
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
async def run_setup(services: ReplayServices) -> dict[str, str]:
    """Create an experiment and a non-empty cohort ready for a run."""
    agent = await create_agent(services.agents, ACCOUNT.id)
    version = await create_agent_version(
        services.agent_versions,
        agent_id=agent.id,
        owner_id=ACCOUNT.id,
        run_spec=RunSpec(command="run.sh"),
    )
    plugin = await create_plugin(
        services.plugins, ACCOUNT.id, kind=PluginKind.EVALUATOR, name="accuracy"
    )
    blob = await create_blob(services.blobs, ACCOUNT.id, content=b"score")
    await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=blob.id, entrypoint="score"),
        display_version=None,
    )
    session = await create_session(
        services.sessions,
        ACCOUNT.id,
        agent_id=agent.id,
        agent_version_id=version.id,
        origin=SessionOrigin.RECORDED,
    )
    cohort = await create_cohort(services.cohorts, ACCOUNT.id, agent.id, [session.id])
    experiment, _ = await services.experiment_service.create_experiment(
        ExperimentCreate(
            name="exp1",
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=AuthContext(account=ACCOUNT),
    )
    return {
        "experiment_id": str(experiment.id),
        "cohort_id": str(cohort.id),
        "agent_version_id": str(version.id),
    }


async def test_start_run(client: httpx.AsyncClient, run_setup: dict[str, str]) -> None:
    """Start a run and observe HTTP 201 with progress inlined."""
    response = await client.post(
        f"/v1/experiments/{run_setup['experiment_id']}/runs",
        json={
            "cohort_id": run_setup["cohort_id"],
            "agent_version_id": run_setup["agent_version_id"],
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "running"
    assert body["number"] == 1
    assert body["progress"]["total"] == 1
    assert body["progress"]["pending"] == 1


async def test_get_run(client: httpx.AsyncClient, run_setup: dict[str, str]) -> None:
    """Get an experiment run by id."""
    created = (
        await client.post(
            f"/v1/experiments/{run_setup['experiment_id']}/runs",
            json={
                "cohort_id": run_setup["cohort_id"],
                "agent_version_id": run_setup["agent_version_id"],
            },
        )
    ).json()
    response = await client.get(f"/v1/experiment-runs/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


async def test_get_run_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for a missing run."""
    response = await client.get(f"/v1/experiment-runs/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_list_runs_filters_by_experiment(
    client: httpx.AsyncClient, run_setup: dict[str, str]
) -> None:
    """List runs filters by experiment id."""
    await client.post(
        f"/v1/experiments/{run_setup['experiment_id']}/runs",
        json={
            "cohort_id": run_setup["cohort_id"],
            "agent_version_id": run_setup["agent_version_id"],
        },
    )
    response = await client.get(
        "/v1/experiment-runs", params={"experiment_id": run_setup["experiment_id"]}
    )
    assert response.status_code == 200
    assert len(response.json()["items"]) == 1

    response = await client.get(
        "/v1/experiment-runs", params={"experiment_id": str(uuid.uuid4())}
    )
    assert response.json()["items"] == []


async def test_list_run_jobs(
    client: httpx.AsyncClient, run_setup: dict[str, str]
) -> None:
    """List the jobs backing a run's replays."""
    created = (
        await client.post(
            f"/v1/experiments/{run_setup['experiment_id']}/runs",
            json={
                "cohort_id": run_setup["cohort_id"],
                "agent_version_id": run_setup["agent_version_id"],
            },
        )
    ).json()
    response = await client.get(f"/v1/experiment-runs/{created['id']}/jobs")
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["status"] == "pending"


async def test_cancel_run(client: httpx.AsyncClient, run_setup: dict[str, str]) -> None:
    """Cancel a run and observe it drain to canceled since all tasks are pending."""
    created = (
        await client.post(
            f"/v1/experiments/{run_setup['experiment_id']}/runs",
            json={
                "cohort_id": run_setup["cohort_id"],
                "agent_version_id": run_setup["agent_version_id"],
            },
        )
    ).json()
    response = await client.post(f"/v1/experiment-runs/{created['id']}/cancel")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "canceled"
    assert body["progress"]["canceled"] == 1


async def test_cancel_run_conflicts_when_not_running(
    client: httpx.AsyncClient, run_setup: dict[str, str]
) -> None:
    """Observe HTTP 409 when canceling a run that already settled."""
    created = (
        await client.post(
            f"/v1/experiments/{run_setup['experiment_id']}/runs",
            json={
                "cohort_id": run_setup["cohort_id"],
                "agent_version_id": run_setup["agent_version_id"],
            },
        )
    ).json()
    await client.post(f"/v1/experiment-runs/{created['id']}/cancel")
    response = await client.post(f"/v1/experiment-runs/{created['id']}/cancel")
    assert response.status_code == 409


async def test_delete_run(client: httpx.AsyncClient, run_setup: dict[str, str]) -> None:
    """Delete a run and observe HTTP 204."""
    created = (
        await client.post(
            f"/v1/experiments/{run_setup['experiment_id']}/runs",
            json={
                "cohort_id": run_setup["cohort_id"],
                "agent_version_id": run_setup["agent_version_id"],
            },
        )
    ).json()
    response = await client.delete(f"/v1/experiment-runs/{created['id']}")
    assert response.status_code == 204
    response = await client.get(f"/v1/experiment-runs/{created['id']}")
    assert response.status_code == 404


async def test_delete_run_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 when deleting an unknown run."""
    response = await client.delete(f"/v1/experiment-runs/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_start_run_rejects_empty_cohort(
    client: httpx.AsyncClient, services: ReplayServices, run_setup: dict[str, str]
) -> None:
    """Observe HTTP 422 when the cohort has no sessions."""
    empty_cohort = await create_cohort(
        services.cohorts, ACCOUNT.id, uuid.uuid4(), [], name="empty-cohort"
    )
    response = await client.post(
        f"/v1/experiments/{run_setup['experiment_id']}/runs",
        json={
            "cohort_id": str(empty_cohort.id),
            "agent_version_id": run_setup["agent_version_id"],
        },
    )
    assert response.status_code == 422
