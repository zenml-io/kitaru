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
"""Parity tests for public and UI experiment run evaluation aggregates."""

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
    reassign_baseline_session,
    store_evaluation,
    store_linked_evaluation,
    store_standalone_evaluation,
)
from fastapi import FastAPI

from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.server.application.services.experiment_run_evaluation_service import (
    ExperimentRunEvaluationService,
)
from kitaru.server.domain.plugin import PackagePluginSource, Plugin, PluginKind
from kitaru_test_support import (
    ReplayServices,
    build_replay_services,
)


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


async def test_aggregates_float_evaluations(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Aggregate float evaluations of baseline and result sessions separately."""
    run_id = await create_run(services)
    version_id = await create_evaluator_version(services)
    first_baseline, first_result = uuid.uuid4(), uuid.uuid4()
    second_baseline, second_result = uuid.uuid4(), uuid.uuid4()
    first_replay_id = await add_replay_with_result_session(
        services, run_id, first_baseline, first_result
    )
    second_replay_id = await add_replay_with_result_session(
        services, run_id, second_baseline, second_result
    )
    await store_linked_evaluation(
        services,
        first_replay_id,
        first_result,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.5,
    )
    await store_linked_evaluation(
        services,
        second_replay_id,
        second_result,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=1.0,
    )
    await store_linked_evaluation(
        services,
        first_replay_id,
        first_baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.1,
    )

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

    assert response.status_code == 200
    assert response.json() == [
        {
            "name": "accuracy",
            "evaluator_version_id": str(version_id),
            "evaluator_name": "accuracy-scorer",
            "evaluator_version": 1,
            "data_type": "float",
            "baseline": {
                "count": 1,
                "mean": 0.1,
                "min": 0.1,
                "max": 0.1,
                "pass_rate": None,
                "value_counts": None,
                "min_score": None,
                "max_score": None,
                "target_score": None,
            },
            "result": {
                "count": 2,
                "mean": 0.75,
                "min": 0.5,
                "max": 1.0,
                "pass_rate": None,
                "value_counts": None,
                "min_score": None,
                "max_score": None,
                "target_score": None,
            },
            "replays": [
                {
                    "replay_id": str(first_replay_id),
                    "baseline": {
                        "score": 0.1,
                        "value": None,
                        "passed": None,
                        "min_score": None,
                        "max_score": None,
                        "target_score": None,
                    },
                    "result": {
                        "score": 0.5,
                        "value": None,
                        "passed": None,
                        "min_score": None,
                        "max_score": None,
                        "target_score": None,
                    },
                },
                {
                    "replay_id": str(second_replay_id),
                    "baseline": None,
                    "result": {
                        "score": 1.0,
                        "value": None,
                        "passed": None,
                        "min_score": None,
                        "max_score": None,
                        "target_score": None,
                    },
                },
            ],
        }
    ]


async def test_aggregates_score_bounds_when_uniform(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Carry the score bound fields when every row in a group agrees on them."""
    run_id = await create_run(services)
    version_id = await create_evaluator_version(services)
    first_result, second_result = uuid.uuid4(), uuid.uuid4()
    first_replay_id = await add_replay_with_result_session(
        services, run_id, uuid.uuid4(), first_result
    )
    second_replay_id = await add_replay_with_result_session(
        services, run_id, uuid.uuid4(), second_result
    )
    await store_linked_evaluation(
        services,
        first_replay_id,
        first_result,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.5,
        min_score=0.0,
        max_score=1.0,
        target_score=0.9,
    )
    await store_linked_evaluation(
        services,
        second_replay_id,
        second_result,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=1.0,
        min_score=0.0,
        max_score=1.0,
        target_score=0.9,
    )

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

    assert response.status_code == 200
    result = response.json()[0]["result"]
    assert result["mean"] == 0.75
    assert result["min"] == 0.5
    assert result["max"] == 1.0
    assert result["min_score"] == 0.0
    assert result["max_score"] == 1.0
    assert result["target_score"] == 0.9
    replay_values = [entry["result"] for entry in response.json()[0]["replays"]]
    assert all(value["min_score"] == 0.0 for value in replay_values)
    assert all(value["max_score"] == 1.0 for value in replay_values)
    assert all(value["target_score"] == 0.9 for value in replay_values)


async def test_aggregates_score_bounds_null_when_mixed(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Null the score bound fields when rows in a group disagree on them."""
    run_id = await create_run(services)
    version_id = await create_evaluator_version(services)
    first_result, second_result = uuid.uuid4(), uuid.uuid4()
    first_replay_id = await add_replay_with_result_session(
        services, run_id, uuid.uuid4(), first_result
    )
    second_replay_id = await add_replay_with_result_session(
        services, run_id, uuid.uuid4(), second_result
    )
    await store_linked_evaluation(
        services,
        first_replay_id,
        first_result,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.5,
        min_score=0.0,
        max_score=1.0,
        target_score=0.9,
    )
    await store_linked_evaluation(
        services,
        second_replay_id,
        second_result,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=1.0,
        min_score=0.0,
        max_score=2.0,
        target_score=0.9,
    )

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

    assert response.status_code == 200
    result = response.json()[0]["result"]
    assert result["mean"] == 0.75
    assert result["min"] == 0.5
    assert result["max"] == 1.0
    assert result["min_score"] == 0.0
    assert result["max_score"] is None
    assert result["target_score"] == 0.9


async def test_aggregates_score_bounds_null_when_one_row_lacks_it(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Null a score bound field when any row in the group has no value for it."""
    run_id = await create_run(services)
    version_id = await create_evaluator_version(services)
    first_result, second_result = uuid.uuid4(), uuid.uuid4()
    first_replay_id = await add_replay_with_result_session(
        services, run_id, uuid.uuid4(), first_result
    )
    second_replay_id = await add_replay_with_result_session(
        services, run_id, uuid.uuid4(), second_result
    )
    await store_linked_evaluation(
        services,
        first_replay_id,
        first_result,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.5,
        min_score=0.0,
        max_score=1.0,
        target_score=0.9,
    )
    await store_linked_evaluation(
        services,
        second_replay_id,
        second_result,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=1.0,
    )

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

    assert response.status_code == 200
    result = response.json()[0]["result"]
    assert result["mean"] == 0.75
    assert result["min"] == 0.5
    assert result["max"] == 1.0
    assert result["min_score"] is None
    assert result["max_score"] is None
    assert result["target_score"] is None


async def test_aggregates_bool_and_pass_rate(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Aggregate bool evaluations of result sessions into a share and pass rate."""
    run_id = await create_run(services)
    version_id = await create_evaluator_version(services)
    results = [uuid.uuid4() for _ in range(3)]
    replay_ids = [
        await add_replay_with_result_session(
            services, run_id, uuid.uuid4(), result_session_id
        )
        for result_session_id in results
    ]
    scores = [True, True, False]
    passed_flags = [True, False, True]
    for result_session_id, replay_id, score, passed in zip(
        results, replay_ids, scores, passed_flags, strict=True
    ):
        await store_linked_evaluation(
            services,
            replay_id,
            result_session_id,
            "ok",
            EvaluationDataType.BOOL,
            version_id,
            score=score,
            passed=passed,
        )

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

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
        "min_score": None,
        "max_score": None,
        "target_score": None,
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
    run_id = await create_run(services)
    version_id = await create_evaluator_version(services)
    results = [uuid.uuid4() for _ in range(3)]
    replay_ids = [
        await add_replay_with_result_session(
            services, run_id, uuid.uuid4(), result_session_id
        )
        for result_session_id in results
    ]
    values = ["good", "good", "bad"]
    for result_session_id, replay_id, value in zip(
        results, replay_ids, values, strict=True
    ):
        await store_linked_evaluation(
            services,
            replay_id,
            result_session_id,
            "label",
            EvaluationDataType.CATEGORICAL,
            version_id,
            score=0.0,
            value=value,
        )

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

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
        "min_score": None,
        "max_score": None,
        "target_score": None,
    }
    assert aggregate["result"] == {
        "count": 3,
        "mean": None,
        "min": None,
        "max": None,
        "pass_rate": None,
        "value_counts": {"good": 2, "bad": 1},
        "min_score": None,
        "max_score": None,
        "target_score": None,
    }
    assert [entry["replay_id"] for entry in aggregate["replays"]] == [
        str(replay_id) for replay_id in replay_ids
    ]


async def test_aggregates_dedupes_shared_baseline_session(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Aggregate baseline stats once for a session shared by two replays."""
    run_id = await create_run(services)
    baseline = uuid.uuid4()
    first_result, second_result = uuid.uuid4(), uuid.uuid4()
    first_replay_id = await add_replay_with_result_session(
        services, run_id, baseline, first_result
    )
    second_replay_id = await add_replay_with_result_session(
        services, run_id, uuid.uuid4(), second_result
    )
    await reassign_baseline_session(services, second_replay_id, baseline)
    version_id = await create_evaluator_version(services)
    await store_linked_evaluation(
        services,
        first_replay_id,
        baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.42,
    )

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

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
        "min_score": None,
        "max_score": None,
        "target_score": None,
    }
    replays_by_id = {entry["replay_id"]: entry for entry in aggregate["replays"]}
    expected_baseline_value = {
        "score": 0.42,
        "value": None,
        "passed": None,
        "min_score": None,
        "max_score": None,
        "target_score": None,
    }
    assert replays_by_id[str(first_replay_id)]["baseline"] == expected_baseline_value
    assert replays_by_id[str(second_replay_id)]["baseline"] == expected_baseline_value


async def test_aggregates_caps_replays_to_most_recent(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Cap the replays array at the 50 most recent replays, oldest first."""
    run_id = await create_run(services)
    version_id = await create_evaluator_version(services)
    replay_ids = []
    for index in range(55):
        result_session_id = uuid.uuid4()
        replay_id = await add_replay_with_result_session(
            services, run_id, uuid.uuid4(), result_session_id
        )
        replay_ids.append(replay_id)
        await store_linked_evaluation(
            services,
            replay_id,
            result_session_id,
            "m",
            EvaluationDataType.FLOAT,
            version_id,
            score=index / 100,
        )

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    aggregate = body[0]
    assert aggregate["result"]["count"] == 55
    assert aggregate["result"]["mean"] == pytest.approx(0.27)
    assert aggregate["result"]["min"] == 0.0
    assert aggregate["result"]["max"] == 0.54
    assert len(aggregate["replays"]) == 50
    assert [entry["replay_id"] for entry in aggregate["replays"]] == [
        str(replay_id) for replay_id in replay_ids[5:]
    ]


async def test_aggregates_include_baseline_only_names(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Include an evaluation name that only exists on a baseline session."""
    run_id = await create_run(services)
    version_id = await create_evaluator_version(services)
    baseline, result = uuid.uuid4(), uuid.uuid4()
    replay_id = await add_replay_with_result_session(services, run_id, baseline, result)
    await store_linked_evaluation(
        services,
        replay_id,
        baseline,
        "baseline_only",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.3,
    )

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

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
        "min_score": None,
        "max_score": None,
        "target_score": None,
    }
    assert aggregate["result"] == {
        "count": 0,
        "mean": None,
        "min": None,
        "max": None,
        "pass_rate": None,
        "value_counts": None,
        "min_score": None,
        "max_score": None,
        "target_score": None,
    }


async def test_aggregates_link_scoped_math(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Aggregate only the evaluations linked to the run's replays."""
    run_id = await create_run(services)
    version_id = await create_evaluator_version(services)
    first_baseline, first_result = uuid.uuid4(), uuid.uuid4()
    second_baseline, second_result = uuid.uuid4(), uuid.uuid4()
    first_replay_id = await add_replay_with_result_session(
        services, run_id, first_baseline, first_result
    )
    second_replay_id = await add_replay_with_result_session(
        services, run_id, second_baseline, second_result
    )
    await store_linked_evaluation(
        services,
        first_replay_id,
        first_baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.1,
    )
    await store_linked_evaluation(
        services,
        first_replay_id,
        first_result,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.5,
    )
    await store_linked_evaluation(
        services,
        second_replay_id,
        second_baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.3,
    )
    await store_linked_evaluation(
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
    await store_standalone_evaluation(
        services,
        first_baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        999.0,
    )

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

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
        "min_score": None,
        "max_score": None,
        "target_score": None,
    }
    assert aggregate["result"] == {
        "count": 2,
        "mean": pytest.approx(0.6),
        "min": 0.5,
        "max": 0.7,
        "pass_rate": None,
        "value_counts": None,
        "min_score": None,
        "max_score": None,
        "target_score": None,
    }
    replays_by_id = {entry["replay_id"]: entry for entry in aggregate["replays"]}
    assert replays_by_id[str(first_replay_id)] == {
        "replay_id": str(first_replay_id),
        "baseline": {
            "score": 0.1,
            "value": None,
            "passed": None,
            "min_score": None,
            "max_score": None,
            "target_score": None,
        },
        "result": {
            "score": 0.5,
            "value": None,
            "passed": None,
            "min_score": None,
            "max_score": None,
            "target_score": None,
        },
    }
    assert replays_by_id[str(second_replay_id)] == {
        "replay_id": str(second_replay_id),
        "baseline": {
            "score": 0.3,
            "value": None,
            "passed": None,
            "min_score": None,
            "max_score": None,
            "target_score": None,
        },
        "result": {
            "score": 0.7,
            "value": None,
            "passed": None,
            "min_score": None,
            "max_score": None,
            "target_score": None,
        },
    }


async def test_aggregates_groups_by_evaluator_version(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Two evaluator versions of the same name produce two separate groups."""
    run_id = await create_run(services)
    first_version_id = await create_evaluator_version(services, name="accuracy-v1")
    second_version_id = await create_evaluator_version(services, name="accuracy-v2")
    baseline, result = uuid.uuid4(), uuid.uuid4()
    replay_id = await add_replay_with_result_session(services, run_id, baseline, result)
    await store_linked_evaluation(
        services,
        replay_id,
        baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        first_version_id,
        score=0.2,
    )
    await store_linked_evaluation(
        services,
        replay_id,
        baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        second_version_id,
        score=0.4,
    )

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

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


async def test_aggregates_manual_rows_invisible(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """A manual evaluation of a run's session does not enter the aggregate."""
    run_id = await create_run(services)
    version_id = await create_evaluator_version(services)
    baseline, result = uuid.uuid4(), uuid.uuid4()
    replay_id = await add_replay_with_result_session(services, run_id, baseline, result)
    await store_linked_evaluation(
        services,
        replay_id,
        baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        version_id,
        score=0.2,
    )
    await store_evaluation(
        services, baseline, "human_quality", EvaluationDataType.FLOAT, score=0.9
    )

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["evaluator_version_id"] == str(version_id)
    assert body[0]["baseline"]["mean"] == 0.2


async def test_aggregates_dangling_evaluator_version(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """A linked row survives its evaluator's deletion, grouped under a null name."""
    run_id = await create_run(services)
    plugin = await services.plugins.create(
        Plugin(owner_id=ACCOUNT.id, kind=PluginKind.EVALUATOR, name="doomed")
    )
    version = await services.plugins.create_version(
        plugin.id,
        PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"),
        display_version="v1",
    )
    baseline, result = uuid.uuid4(), uuid.uuid4()
    replay_id = await add_replay_with_result_session(services, run_id, baseline, result)
    await store_linked_evaluation(
        services,
        replay_id,
        baseline,
        "accuracy",
        EvaluationDataType.FLOAT,
        version.id,
        score=0.5,
    )

    await services.plugins.delete(plugin.id)

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

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
    run_id = await create_run(services)
    version_id = await create_evaluator_version(services)
    baseline, result = uuid.uuid4(), uuid.uuid4()
    replay_id = await add_replay_with_result_session(services, run_id, baseline, result)
    await store_linked_evaluation(
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
    await store_standalone_evaluation(
        services, baseline, "accuracy", EvaluationDataType.FLOAT, version_id, 0.9
    )

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

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
    run_id = await create_run(services)
    version_id = await create_evaluator_version(services)
    baseline, result = uuid.uuid4(), uuid.uuid4()
    await add_replay_with_result_session(services, run_id, baseline, result)
    await store_standalone_evaluation(
        services, baseline, "orphan", EvaluationDataType.FLOAT, version_id, 0.5
    )

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

    assert response.status_code == 200
    assert response.json() == []


async def test_aggregates_empty_run(
    client: httpx.AsyncClient, services: ReplayServices
) -> None:
    """Return an empty list for a run with no replays."""
    run_id = await create_run(services)

    response = await client.get(f"experiment-runs/{run_id}/evaluation-aggregates")

    assert response.status_code == 200
    assert response.json() == []


async def test_aggregates_run_not_found(client: httpx.AsyncClient) -> None:
    """Observe HTTP 404 for an unknown experiment run id."""
    response = await client.get(f"experiment-runs/{uuid.uuid4()}/evaluation-aggregates")
    assert response.status_code == 404
