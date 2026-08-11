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
"""Tests for the UI experiment run evaluation aggregate route."""

import uuid
from collections.abc import AsyncGenerator

import httpx
import pytest

from conftest import (
    ReplayServices,
    build_replay_services,
    create_experiment_run,
    create_job,
    create_replay,
)
from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_evaluation_service,
    get_experiment_run_service,
    get_replay_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.api.config import APISettings
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.domain.account import Account
from kitaru.server.domain.evaluation import Evaluation
from kitaru.server.domain.replay_config import ReplayConfig, default_tool_policy
from kitaru.server.domain.task import AgentTask

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


@pytest.fixture
def services() -> ReplayServices:
    """Provide fake-backed experiment run, replay, and evaluation services."""
    return build_replay_services()


@pytest.fixture
async def client(services: ReplayServices) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provide an HTTP client for the app with fake-backed UI aggregate services."""
    app = create_app(
        APISettings(
            DB_HOST="localhost",
            SECRET_ENCRYPTION_KEY="test-encryption-key",
            JWT_SIGNING_KEY="test-signing-key-0123456789abcdef",
        )
    )
    evaluation_service = EvaluationService(
        repository=services.evaluations, session_repository=services.sessions
    )
    app.dependency_overrides[get_experiment_run_service] = lambda: (
        services.experiment_run_service
    )
    app.dependency_overrides[get_replay_service] = lambda: services.replay_service
    app.dependency_overrides[get_evaluation_service] = lambda: evaluation_service
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def _create_run(services: ReplayServices) -> uuid.UUID:
    """Store an experiment run with no replays and return its id."""
    run = await create_experiment_run(
        services.experiment_runs,
        ACCOUNT.id,
        experiment_id=uuid.uuid4(),
        cohort_version_id=uuid.uuid4(),
        agent_version_id=uuid.uuid4(),
    )
    return run.id


async def _add_replay_with_result_session(
    services: ReplayServices,
    run_id: uuid.UUID,
    baseline_session_id: uuid.UUID,
    result_session_id: uuid.UUID,
) -> None:
    """Attach a replay with a linked baseline and result session to a run."""
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
        baseline_session_id=baseline_session_id,
        experiment_run_id=run_id,
    )
    await services.tasks.create(
        AgentTask(
            job_id=replay.job_id,
            agent_version_id=uuid.uuid4(),
            result_session_id=result_session_id,
        )
    )


async def _store_evaluation(
    services: ReplayServices,
    session_id: uuid.UUID,
    name: str,
    data_type: EvaluationDataType,
    score: float | bool | None = None,
    value: str | None = None,
    passed: bool | None = None,
) -> None:
    """Store an evaluation directly through the fake evaluation repository."""
    evaluation = Evaluation(
        owner_id=ACCOUNT.id,
        session_id=session_id,
        name=name,
        data_type=data_type,
        score=score,
        value=value,
        passed=passed,
    )
    await services.evaluations.merge_session_evaluations(session_id, [evaluation])


async def test_aggregates_float_evaluations(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Aggregate float evaluations of result sessions, excluding baselines."""
    run_id = await _create_run(services)
    first_baseline, first_result = uuid.uuid4(), uuid.uuid4()
    second_baseline, second_result = uuid.uuid4(), uuid.uuid4()
    await _add_replay_with_result_session(
        services, run_id, first_baseline, first_result
    )
    await _add_replay_with_result_session(
        services, run_id, second_baseline, second_result
    )
    await _store_evaluation(
        services, first_result, "accuracy", EvaluationDataType.FLOAT, score=0.5
    )
    await _store_evaluation(
        services, second_result, "accuracy", EvaluationDataType.FLOAT, score=1.0
    )
    await _store_evaluation(
        services, first_baseline, "accuracy", EvaluationDataType.FLOAT, score=0.1
    )

    response = await client.get(
        f"/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    assert response.json() == [
        {
            "name": "accuracy",
            "data_type": "float",
            "count": 2,
            "average": 0.75,
            "pass_rate": None,
            "value_counts": None,
        }
    ]


async def test_aggregates_bool_and_pass_rate(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Aggregate bool evaluations into a true share and a pass rate."""
    run_id = await _create_run(services)
    results = [uuid.uuid4() for _ in range(3)]
    for result_session_id in results:
        await _add_replay_with_result_session(
            services, run_id, uuid.uuid4(), result_session_id
        )
    scores = [True, True, False]
    passed_flags = [True, False, True]
    for result_session_id, score, passed in zip(
        results, scores, passed_flags, strict=True
    ):
        await _store_evaluation(
            services,
            result_session_id,
            "ok",
            EvaluationDataType.BOOL,
            score=score,
            passed=passed,
        )

    response = await client.get(
        f"/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    aggregate = body[0]
    assert aggregate["name"] == "ok"
    assert aggregate["data_type"] == "bool"
    assert aggregate["count"] == 3
    assert aggregate["average"] == pytest.approx(2 / 3)
    assert aggregate["pass_rate"] == pytest.approx(2 / 3)
    assert aggregate["value_counts"] is None


async def test_aggregates_categorical_value_counts(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Aggregate categorical evaluations into per-value occurrence counts."""
    run_id = await _create_run(services)
    results = [uuid.uuid4() for _ in range(3)]
    for result_session_id in results:
        await _add_replay_with_result_session(
            services, run_id, uuid.uuid4(), result_session_id
        )
    values = ["good", "good", "bad"]
    for result_session_id, value in zip(results, values, strict=True):
        await _store_evaluation(
            services,
            result_session_id,
            "label",
            EvaluationDataType.CATEGORICAL,
            score=0.0,
            value=value,
        )

    response = await client.get(
        f"/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    assert response.json() == [
        {
            "name": "label",
            "data_type": "categorical",
            "count": 3,
            "average": None,
            "pass_rate": None,
            "value_counts": {"good": 2, "bad": 1},
        }
    ]


async def test_aggregates_empty_run(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Return an empty list for a run with no replays."""
    run_id = await _create_run(services)

    response = await client.get(
        f"/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    assert response.json() == []


async def test_aggregates_run_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown experiment run id."""
    response = await client.get(
        f"/v1/ui/experiment-runs/{uuid.uuid4()}/evaluation-aggregates"
    )
    assert response.status_code == 404
