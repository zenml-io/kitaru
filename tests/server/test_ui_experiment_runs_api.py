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
from kitaru.server.domain.plugin import PackagePluginSource, Plugin, PluginKind
from kitaru.server.domain.replay_config import ReplayConfig, default_tool_policy

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
) -> uuid.UUID:
    """Attach a replay with baseline and result sessions to a run and return its id."""
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
    replay.link_result_session(result_session_id)
    await services.replays.update(replay)
    return replay.id


async def _reassign_baseline_session(
    services: ReplayServices, replay_id: uuid.UUID, baseline_session_id: uuid.UUID
) -> None:
    """Move a stored replay onto another baseline session."""
    replay = await services.replays.get(replay_id)
    await services.replays.update(
        replay.model_copy(update={"baseline_session_id": baseline_session_id})
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
    await services.evaluations.create_session_evaluations(session_id, [evaluation])


async def _create_evaluator_version(
    services: ReplayServices, name: str = "accuracy-scorer"
) -> uuid.UUID:
    """Create an evaluator plugin version through the fake plugin repository."""
    plugin = await services.plugins.create(
        Plugin(owner_id=ACCOUNT.id, kind=PluginKind.EVALUATOR, name=name)
    )
    version = await services.plugins.create_version(
        plugin.id,
        PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"),
        display_version="v1",
    )
    return version.id


async def _store_linked_evaluation(
    services: ReplayServices,
    replay_id: uuid.UUID,
    session_id: uuid.UUID,
    name: str,
    data_type: EvaluationDataType,
    evaluator_version_id: uuid.UUID,
    score: float | bool | None = None,
    value: str | None = None,
    passed: bool | None = None,
) -> None:
    """Store an evaluator-produced evaluation and link it to a replay."""
    evaluation = Evaluation(
        owner_id=ACCOUNT.id,
        evaluator_version_id=evaluator_version_id,
        session_id=session_id,
        name=name,
        data_type=data_type,
        score=score,
        value=value,
        passed=passed,
    )
    await services.evaluations.create_task_evaluations(
        [evaluation], replay_id=replay_id
    )


async def _store_standalone_evaluation(
    services: ReplayServices,
    session_id: uuid.UUID,
    name: str,
    data_type: EvaluationDataType,
    evaluator_version_id: uuid.UUID,
    score: float | bool | None = None,
) -> None:
    """Store an evaluator-produced evaluation that is not linked to a replay."""
    evaluation = Evaluation(
        owner_id=ACCOUNT.id,
        evaluator_version_id=evaluator_version_id,
        session_id=session_id,
        name=name,
        data_type=data_type,
        score=score,
    )
    await services.evaluations.create_task_evaluations([evaluation], replay_id=None)


async def test_aggregates_float_evaluations(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Aggregate float evaluations of baseline and result sessions separately."""
    run_id = await _create_run(services)
    first_baseline, first_result = uuid.uuid4(), uuid.uuid4()
    second_baseline, second_result = uuid.uuid4(), uuid.uuid4()
    first_replay_id = await _add_replay_with_result_session(
        services, run_id, first_baseline, first_result
    )
    second_replay_id = await _add_replay_with_result_session(
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
        f"/api/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    assert response.json() == [
        {
            "name": "accuracy",
            "evaluator_version_id": None,
            "evaluator_name": None,
            "evaluator_version": None,
            "data_type": "float",
            "baseline": {
                "count": 1,
                "mean": 0.1,
                "min": 0.1,
                "max": 0.1,
                "pass_rate": None,
                "value_counts": None,
            },
            "result": {
                "count": 2,
                "mean": 0.75,
                "min": 0.5,
                "max": 1.0,
                "pass_rate": None,
                "value_counts": None,
            },
            "replays": [
                {
                    "replay_id": str(first_replay_id),
                    "baseline": {"score": 0.1, "value": None, "passed": None},
                    "result": {"score": 0.5, "value": None, "passed": None},
                },
                {
                    "replay_id": str(second_replay_id),
                    "baseline": None,
                    "result": {"score": 1.0, "value": None, "passed": None},
                },
            ],
        }
    ]


async def test_aggregates_bool_and_pass_rate(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Aggregate bool evaluations of result sessions into a share and pass rate."""
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
        f"/api/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    aggregate = body[0]
    assert aggregate["name"] == "ok"
    assert aggregate["data_type"] == "bool"
    assert aggregate["baseline"] == {
        "count": 0,
        "mean": None,
        "min": None,
        "max": None,
        "pass_rate": None,
        "value_counts": None,
    }
    assert aggregate["result"]["count"] == 3
    assert aggregate["result"]["mean"] == pytest.approx(2 / 3)
    assert aggregate["result"]["min"] == 0.0
    assert aggregate["result"]["max"] == 1.0
    assert aggregate["result"]["pass_rate"] == pytest.approx(2 / 3)
    assert aggregate["result"]["value_counts"] is None


async def test_aggregates_categorical_value_counts(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Aggregate categorical evaluations of result sessions into per-value counts."""
    run_id = await _create_run(services)
    results = [uuid.uuid4() for _ in range(3)]
    replay_ids = [
        await _add_replay_with_result_session(
            services, run_id, uuid.uuid4(), result_session_id
        )
        for result_session_id in results
    ]
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
        f"/api/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    aggregate = body[0]
    assert aggregate["name"] == "label"
    assert aggregate["data_type"] == "categorical"
    assert aggregate["baseline"] == {
        "count": 0,
        "mean": None,
        "min": None,
        "max": None,
        "pass_rate": None,
        "value_counts": {},
    }
    assert aggregate["result"] == {
        "count": 3,
        "mean": None,
        "min": None,
        "max": None,
        "pass_rate": None,
        "value_counts": {"good": 2, "bad": 1},
    }
    assert [entry["replay_id"] for entry in aggregate["replays"]] == [
        str(replay_id) for replay_id in replay_ids
    ]


async def test_aggregates_dedupes_shared_baseline_session(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Aggregate baseline stats once for a session shared by two replays."""
    run_id = await _create_run(services)
    baseline = uuid.uuid4()
    first_result, second_result = uuid.uuid4(), uuid.uuid4()
    first_replay_id = await _add_replay_with_result_session(
        services, run_id, baseline, first_result
    )
    second_replay_id = await _add_replay_with_result_session(
        services, run_id, uuid.uuid4(), second_result
    )
    await _reassign_baseline_session(services, second_replay_id, baseline)
    await _store_evaluation(
        services, baseline, "accuracy", EvaluationDataType.FLOAT, score=0.42
    )

    response = await client.get(
        f"/api/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    aggregate = body[0]
    assert aggregate["baseline"] == {
        "count": 1,
        "mean": 0.42,
        "min": 0.42,
        "max": 0.42,
        "pass_rate": None,
        "value_counts": None,
    }
    replays_by_id = {entry["replay_id"]: entry for entry in aggregate["replays"]}
    expected_baseline_value = {"score": 0.42, "value": None, "passed": None}
    assert replays_by_id[str(first_replay_id)]["baseline"] == expected_baseline_value
    assert replays_by_id[str(second_replay_id)]["baseline"] == expected_baseline_value


async def test_aggregates_caps_replays_to_most_recent(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Cap the replays array at the 50 most recent replays, oldest first."""
    run_id = await _create_run(services)
    replay_ids = []
    for index in range(55):
        result_session_id = uuid.uuid4()
        replay_id = await _add_replay_with_result_session(
            services, run_id, uuid.uuid4(), result_session_id
        )
        replay_ids.append(replay_id)
        await _store_evaluation(
            services,
            result_session_id,
            "m",
            EvaluationDataType.FLOAT,
            score=index / 100,
        )

    response = await client.get(
        f"/api/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    aggregate = body[0]
    assert aggregate["result"]["count"] == 55
    assert len(aggregate["replays"]) == 50
    assert aggregate["replays"][0]["replay_id"] == str(replay_ids[5])


async def test_aggregates_include_baseline_only_names(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Include an evaluation name that only exists on a baseline session."""
    run_id = await _create_run(services)
    baseline, result = uuid.uuid4(), uuid.uuid4()
    await _add_replay_with_result_session(services, run_id, baseline, result)
    await _store_evaluation(
        services, baseline, "baseline_only", EvaluationDataType.FLOAT, score=0.3
    )

    response = await client.get(
        f"/api/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    aggregate = body[0]
    assert aggregate["name"] == "baseline_only"
    assert aggregate["baseline"] == {
        "count": 1,
        "mean": 0.3,
        "min": 0.3,
        "max": 0.3,
        "pass_rate": None,
        "value_counts": None,
    }
    assert aggregate["result"] == {
        "count": 0,
        "mean": None,
        "min": None,
        "max": None,
        "pass_rate": None,
        "value_counts": None,
    }


async def test_aggregates_link_scoped_math(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Aggregate only the evaluations linked to the run's replays."""
    run_id = await _create_run(services)
    version_id = await _create_evaluator_version(services)
    first_baseline, first_result = uuid.uuid4(), uuid.uuid4()
    second_baseline, second_result = uuid.uuid4(), uuid.uuid4()
    first_replay_id = await _add_replay_with_result_session(
        services, run_id, first_baseline, first_result
    )
    second_replay_id = await _add_replay_with_result_session(
        services, run_id, second_baseline, second_result
    )
    await _store_linked_evaluation(
        services,
        first_replay_id,
        first_baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.1,
    )
    await _store_linked_evaluation(
        services,
        first_replay_id,
        first_result,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.5,
    )
    await _store_linked_evaluation(
        services,
        second_replay_id,
        second_baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.3,
    )
    await _store_linked_evaluation(
        services,
        second_replay_id,
        second_result,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.7,
    )
    # A standalone evaluation job scored the same baseline session under the
    # same evaluator, but was never linked to a replay, so it must not move
    # the aggregate.
    await _store_standalone_evaluation(
        services,
        first_baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        999.0,
    )

    response = await client.get(
        f"/api/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    aggregate = body[0]
    assert aggregate["name"] == "accuracy"
    assert aggregate["evaluator_version_id"] == str(version_id)
    assert aggregate["evaluator_name"] == "accuracy-scorer"
    assert aggregate["evaluator_version"] == 1
    assert aggregate["baseline"] == {
        "count": 2,
        "mean": 0.2,
        "min": 0.1,
        "max": 0.3,
        "pass_rate": None,
        "value_counts": None,
    }
    assert aggregate["result"] == {
        "count": 2,
        "mean": pytest.approx(0.6),
        "min": 0.5,
        "max": 0.7,
        "pass_rate": None,
        "value_counts": None,
    }
    replays_by_id = {entry["replay_id"]: entry for entry in aggregate["replays"]}
    assert replays_by_id[str(first_replay_id)] == {
        "replay_id": str(first_replay_id),
        "baseline": {"score": 0.1, "value": None, "passed": None},
        "result": {"score": 0.5, "value": None, "passed": None},
    }
    assert replays_by_id[str(second_replay_id)] == {
        "replay_id": str(second_replay_id),
        "baseline": {"score": 0.3, "value": None, "passed": None},
        "result": {"score": 0.7, "value": None, "passed": None},
    }


async def test_aggregates_groups_by_evaluator_version(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Two evaluator versions of the same name produce two separate groups."""
    run_id = await _create_run(services)
    first_version_id = await _create_evaluator_version(services, name="accuracy-v1")
    second_version_id = await _create_evaluator_version(services, name="accuracy-v2")
    baseline, result = uuid.uuid4(), uuid.uuid4()
    replay_id = await _add_replay_with_result_session(
        services, run_id, baseline, result
    )
    await _store_linked_evaluation(
        services,
        replay_id,
        baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        first_version_id,
        score=0.2,
    )
    await _store_linked_evaluation(
        services,
        replay_id,
        baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        second_version_id,
        score=0.4,
    )

    response = await client.get(
        f"/api/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    by_version = {entry["evaluator_version_id"]: entry for entry in body}
    assert by_version.keys() == {str(first_version_id), str(second_version_id)}
    first_group = by_version[str(first_version_id)]
    assert first_group["evaluator_name"] == "accuracy-v1"
    assert first_group["baseline"]["count"] == 1
    assert first_group["baseline"]["mean"] == 0.2
    second_group = by_version[str(second_version_id)]
    assert second_group["evaluator_name"] == "accuracy-v2"
    assert second_group["baseline"]["count"] == 1
    assert second_group["baseline"]["mean"] == 0.4


async def test_aggregates_manual_and_linked_groups_coexist(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """A manual group and a linked group of the same name appear side by side."""
    run_id = await _create_run(services)
    version_id = await _create_evaluator_version(services)
    baseline, result = uuid.uuid4(), uuid.uuid4()
    replay_id = await _add_replay_with_result_session(
        services, run_id, baseline, result
    )
    await _store_linked_evaluation(
        services,
        replay_id,
        baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.2,
    )
    await _store_evaluation(
        services, baseline, "accuracy", EvaluationDataType.FLOAT, score=0.9
    )

    response = await client.get(
        f"/api/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    by_version = {entry["evaluator_version_id"]: entry for entry in body}
    assert by_version.keys() == {str(version_id), None}
    assert by_version[str(version_id)]["baseline"]["mean"] == 0.2
    manual_group = by_version[None]
    assert manual_group["evaluator_name"] is None
    assert manual_group["evaluator_version"] is None
    assert manual_group["baseline"]["mean"] == 0.9


async def test_aggregates_dangling_evaluator_version(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """A linked row survives its evaluator's deletion, grouped under a null name."""
    run_id = await _create_run(services)
    plugin = await services.plugins.create(
        Plugin(owner_id=ACCOUNT.id, kind=PluginKind.EVALUATOR, name="doomed")
    )
    version = await services.plugins.create_version(
        plugin.id,
        PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"),
        display_version="v1",
    )
    baseline, result = uuid.uuid4(), uuid.uuid4()
    replay_id = await _add_replay_with_result_session(
        services, run_id, baseline, result
    )
    await _store_linked_evaluation(
        services,
        replay_id,
        baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        version.id,
        score=0.5,
    )

    await services.plugins.delete(plugin.id)

    response = await client.get(
        f"/api/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    aggregate = body[0]
    assert aggregate["evaluator_version_id"] == str(version.id)
    assert aggregate["evaluator_name"] is None
    assert aggregate["evaluator_version"] is None
    assert aggregate["baseline"]["count"] == 1
    assert aggregate["baseline"]["mean"] == 0.5


async def test_aggregates_adoption_pinning_ignores_newer_evaluation(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """A later same-identity evaluation not linked to the run leaves it unaffected."""
    run_id = await _create_run(services)
    version_id = await _create_evaluator_version(services)
    baseline, result = uuid.uuid4(), uuid.uuid4()
    replay_id = await _add_replay_with_result_session(
        services, run_id, baseline, result
    )
    await _store_linked_evaluation(
        services,
        replay_id,
        baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.2,
    )

    # A later run re-evaluates the same baseline session with the same
    # evaluator, producing a new row that is never linked to this run.
    await _store_standalone_evaluation(
        services, baseline, "accuracy", EvaluationDataType.FLOAT, version_id, 0.9
    )

    response = await client.get(
        f"/api/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    aggregate = body[0]
    assert aggregate["baseline"]["count"] == 1
    assert aggregate["baseline"]["mean"] == 0.2


async def test_aggregates_standalone_unlinked_rows_invisible(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """A standalone evaluation job's rows never appear in run aggregates."""
    run_id = await _create_run(services)
    version_id = await _create_evaluator_version(services)
    baseline, result = uuid.uuid4(), uuid.uuid4()
    await _add_replay_with_result_session(services, run_id, baseline, result)
    await _store_standalone_evaluation(
        services, baseline, "orphan", EvaluationDataType.FLOAT, version_id, 0.5
    )

    response = await client.get(
        f"/api/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    assert response.json() == []


async def test_aggregates_empty_run(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Return an empty list for a run with no replays."""
    run_id = await _create_run(services)

    response = await client.get(
        f"/api/v1/ui/experiment-runs/{run_id}/evaluation-aggregates"
    )

    assert response.status_code == 200
    assert response.json() == []


async def test_aggregates_run_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown experiment run id."""
    response = await client.get(
        f"/api/v1/ui/experiment-runs/{uuid.uuid4()}/evaluation-aggregates"
    )
    assert response.status_code == 404
