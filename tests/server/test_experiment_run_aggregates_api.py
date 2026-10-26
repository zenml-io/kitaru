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
"""Parity tests for aggregate result sessions, grouping, auth, and read routing."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest
from experiment_run_aggregate_fixtures import (
    ACCOUNT,
    add_replay_with_result_session,
    build_app,
    build_evaluation_service,
    create_evaluator_version,
    create_run,
    open_client,
    store_linked_evaluation,
)
from fastapi import FastAPI, Request

from conftest import (
    ReplayServices,
    build_replay_services,
    create_job,
    create_replay,
)
from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_experiment_run_evaluation_service,
)
from kitaru.server.adapters.rest.request_state import request_uses_read_engine
from kitaru.server.application.services.experiment_run_evaluation_service import (
    ExperimentRunEvaluationService,
)
from kitaru.server.domain.replay_config import ReplayConfig, default_tool_policy


@pytest.fixture
def services() -> ReplayServices:
    """Provide fake-backed experiment run, replay, and evaluation services."""
    return build_replay_services()


@pytest.fixture
def evaluation_service(services: ReplayServices) -> ExperimentRunEvaluationService:
    """Provide a fake-backed experiment run evaluation service."""
    return build_evaluation_service(services)


@pytest.fixture
def app(evaluation_service: ExperimentRunEvaluationService) -> FastAPI:
    """Provide the app with fake-backed aggregation and authentication services."""
    return build_app(evaluation_service)


@pytest.fixture(params=["/api/v1/ui", "/api/v1"], ids=["ui", "public"])
async def client(
    app: FastAPI, request: pytest.FixtureRequest
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Exercise the same aggregate contract through both API routes."""
    async with open_client(app, request.param) as client:
        yield client


async def test_aggregates_missing_result_session(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Preserve baseline evidence before a replay produces its result session."""
    run_id = await create_run(services)
    version_id = await create_evaluator_version(services)
    baseline = uuid.uuid4()
    job = await create_job(services.jobs, ACCOUNT.id)
    config = await services.experiments.create_replay_config(
        ReplayConfig(
            owner_id=ACCOUNT.id, tool_policy=default_tool_policy(), evaluators=[]
        )
    )
    replay = await create_replay(
        services.replays,
        ACCOUNT.id,
        job_id=job.id,
        replay_config_id=config.id,
        baseline_session_id=baseline,
        experiment_run_id=run_id,
    )
    await store_linked_evaluation(
        services,
        replay.id,
        baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.7,
    )

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

    assert response.status_code == 200
    aggregate = response.json()[0]
    assert aggregate["baseline"]["count"] == 1
    assert aggregate["baseline"]["mean"] == 0.7
    assert aggregate["result"]["count"] == 0
    assert aggregate["replays"][0]["baseline"]["score"] == 0.7
    assert aggregate["replays"][0]["result"] is None


async def test_aggregates_groups_by_name_and_data_type(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Sort groups by name and type while retaining string values without a mean."""
    run_id = await create_run(services)
    version_id = await create_evaluator_version(services)
    baseline, result = uuid.uuid4(), uuid.uuid4()
    replay_id = await add_replay_with_result_session(services, run_id, baseline, result)
    for session_id, name, data_type, score, value in [
        (baseline, "quality", EvaluationDataType.STR, None, "needs review"),
        (result, "quality", EvaluationDataType.FLOAT, 0.8, None),
        (result, "accuracy", EvaluationDataType.BOOL, True, None),
    ]:
        await store_linked_evaluation(
            services,
            replay_id,
            session_id,
            name,
            data_type,
            version_id,
            score=score,
            value=value,
        )

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

    assert response.status_code == 200
    aggregates = response.json()
    assert [(item["name"], item["data_type"]) for item in aggregates] == [
        ("accuracy", "bool"),
        ("quality", "float"),
        ("quality", "str"),
    ]
    string_group = aggregates[2]
    assert string_group["baseline"]["count"] == 1
    assert string_group["baseline"]["mean"] is None
    assert string_group["baseline"]["value_counts"] is None
    assert string_group["result"]["count"] == 0
    assert string_group["replays"][0]["baseline"]["value"] == "needs review"


async def test_aggregates_require_authentication(
    app: FastAPI, client: httpx.AsyncClient
) -> None:
    """Reject unauthenticated reads before looking up an experiment run."""
    del app.dependency_overrides[authorize]

    response = await client.get(f"experiment-runs/{uuid.uuid4()}/evaluation-aggregates")

    assert response.status_code == 401
    assert response.json() == {"detail": "Missing bearer credential."}


@pytest.mark.parametrize("strong_consistency", [False, True])
async def test_aggregates_route_read_consistency(
    app: FastAPI,
    client: httpx.AsyncClient,
    services: ReplayServices,
    evaluation_service: ExperimentRunEvaluationService,
    strong_consistency: bool,
) -> None:
    """Route ordinary reads to the replica and honor the primary read preference."""
    run_id = await create_run(services)
    read_engine_requests: list[bool] = []

    def provide_service(request: Request) -> ExperimentRunEvaluationService:
        read_engine_requests.append(request_uses_read_engine(request))
        return evaluation_service

    app.dependency_overrides[get_experiment_run_evaluation_service] = provide_service
    headers = {"Prefer": "consistency=strong"} if strong_consistency else {}

    response = await client.get(
        f"experiment-runs/{run_id}/evaluation-aggregates", headers=headers
    )

    assert response.status_code == 200
    assert read_engine_requests == [not strong_consistency]
    assert response.headers.get("Preference-Applied") == (
        "consistency=strong" if strong_consistency else None
    )
