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
"""Shared app setup and data helpers for experiment run evaluation aggregate tests."""

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from conftest import (
    FakeAccountRepository,
    FakeApiKeyRepository,
    FakePasswordHasher,
    ReplayServices,
    create_experiment_run,
    create_job,
    create_replay,
    local_settings,
    stub_auth_session,
)
from kitaru.api_models.v1.evaluation import EvaluationDataType
from kitaru.server.adapters.auth.auth_service import AuthService
from kitaru.server.adapters.rest.dependencies import (
    authorize,
    get_auth_service,
    get_auth_session,
    get_experiment_run_evaluation_service,
)
from kitaru.server.api.app import create_app
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.experiment_run_evaluation_service import (
    ExperimentRunEvaluationService,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.evaluation import Evaluation
from kitaru.server.domain.plugin import PackagePluginSource, Plugin, PluginKind
from kitaru.server.domain.replay_config import ReplayConfig, default_tool_policy

ACCOUNT = Account(id=uuid.uuid4(), name="ann")


def build_evaluation_service(
    services: ReplayServices,
) -> ExperimentRunEvaluationService:
    """Build a fake-backed experiment run evaluation service."""
    return ExperimentRunEvaluationService(
        repository=services.experiment_runs,
        replay_repository=services.replays,
        evaluation_repository=services.evaluations,
    )


def build_app(evaluation_service: ExperimentRunEvaluationService) -> FastAPI:
    """Build the app with fake-backed aggregation and authentication services."""
    settings = local_settings()
    app = create_app(settings)
    auth_service = AuthService(
        settings=settings,
        account_repository=FakeAccountRepository(),
        api_key_repository=FakeApiKeyRepository(),
        password_hasher=FakePasswordHasher(),
    )
    app.dependency_overrides[get_auth_service] = lambda: auth_service
    app.dependency_overrides[get_auth_session] = stub_auth_session
    app.dependency_overrides[get_experiment_run_evaluation_service] = lambda: (
        evaluation_service
    )
    app.dependency_overrides[authorize] = lambda: AuthContext(account=ACCOUNT)
    return app


@asynccontextmanager
async def open_client(app: FastAPI, prefix: str) -> AsyncIterator[httpx.AsyncClient]:
    """Open an HTTP client whose relative paths resolve under an API prefix."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url=f"http://test{prefix}/"
    ) as client:
        yield client


async def create_run(services: ReplayServices) -> uuid.UUID:
    """Store an experiment run with no replays and return its id."""
    run = await create_experiment_run(
        services.experiment_runs,
        ACCOUNT.id,
        experiment_id=uuid.uuid4(),
        cohort_version_id=uuid.uuid4(),
        agent_version_id=uuid.uuid4(),
    )
    return run.id


async def add_replay_with_result_session(
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


async def reassign_baseline_session(
    services: ReplayServices, replay_id: uuid.UUID, baseline_session_id: uuid.UUID
) -> None:
    """Move a stored replay onto another baseline session."""
    replay = await services.replays.get(replay_id)
    await services.replays.update(
        replay.model_copy(update={"baseline_session_id": baseline_session_id})
    )


async def store_evaluation(
    services: ReplayServices,
    session_id: uuid.UUID,
    name: str,
    data_type: EvaluationDataType,
    score: float | bool | None = None,
    value: str | None = None,
    passed: bool | None = None,
    min_score: float | None = None,
    max_score: float | None = None,
    target_score: float | None = None,
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
        min_score=min_score,
        max_score=max_score,
        target_score=target_score,
    )
    await services.evaluations.create_session_evaluations(session_id, [evaluation])


async def create_evaluator_version(
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


async def store_linked_evaluation(
    services: ReplayServices,
    replay_id: uuid.UUID,
    session_id: uuid.UUID,
    name: str,
    data_type: EvaluationDataType,
    evaluator_version_id: uuid.UUID,
    score: float | bool | None = None,
    value: str | None = None,
    passed: bool | None = None,
    min_score: float | None = None,
    max_score: float | None = None,
    target_score: float | None = None,
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
        min_score=min_score,
        max_score=max_score,
        target_score=target_score,
    )
    await services.evaluations.create_task_evaluations(
        [evaluation], replay_id=replay_id
    )


async def store_standalone_evaluation(
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
