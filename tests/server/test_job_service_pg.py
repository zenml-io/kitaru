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
"""Job service tests against PostgreSQL."""

import uuid
from collections import Counter
from collections.abc import AsyncGenerator
from typing import NamedTuple

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from conftest import capture_statements, pg_session_with_engine, postgres_available
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.api_models.v1.task import TaskOnFailure, TaskStatus
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.blob_repository import SQLBlobRepository
from kitaru.server.adapters.db.repositories.connection_repository import (
    SQLConnectionRepository,
)
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.plugin_repository import (
    SQLPluginRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.task_repository import SQLTaskRepository
from kitaru.server.application.events import EventDispatcher
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.job import EvaluationBatchCreate
from kitaru.server.application.models.plugin import EvaluatorConfigInput
from kitaru.server.application.models.task import TaskPolicy
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.plugin_resolution import (
    get_plugin_task_labels,
)
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.blob import Blob, BlobStorageBackend
from kitaru.server.domain.plugin import Plugin, PluginKind, ScriptPluginSource
from kitaru.server.domain.session import Session
from kitaru.server.domain.task import EvaluationTask

EVALUATOR_NAMES = ["kitaru/accuracy", "tone"]
SESSION_COUNT = 50


class Setup(NamedTuple):
    """SQL-backed job service, plus the sessions and evaluators it scores."""

    session: AsyncSession
    engine: AsyncEngine
    service: JobService
    actor: AuthContext
    input_session_ids: list[uuid.UUID]
    evaluator_version_ids: list[uuid.UUID]


@pytest.fixture
async def setup() -> AsyncGenerator[Setup, None]:
    """Provide a job service on a database holding sessions and evaluators."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        agent = await SQLAgentRepository(session).create(
            Agent(owner_id=owner.id, name="assistant")
        )
        sessions = SQLSessionRepository(session, engine)
        input_session_ids = [
            (
                await sessions.create(
                    Session(
                        owner_id=owner.id,
                        agent_id=agent.id,
                        number=number,
                        origin=SessionOrigin.RECORDED,
                        status=SessionStatus.COMPLETED,
                    )
                )
            ).id
            for number in range(1, SESSION_COUNT + 1)
        ]
        code_blob, _ = await SQLBlobRepository(session).create(
            Blob(
                owner_id=owner.id,
                sha256="1" * 64,
                size=4,
                media_type="text/x-python",
                stored_in=BlobStorageBackend.DATABASE,
            )
        )
        plugins = SQLPluginRepository(session)
        evaluator_version_ids = []
        for name in EVALUATOR_NAMES:
            plugin = await plugins.create(
                Plugin(owner_id=owner.id, kind=PluginKind.EVALUATOR, name=name)
            )
            version = await plugins.create_version(
                plugin.id,
                ScriptPluginSource(blob_id=code_blob.id, entrypoint="score"),
                display_version=None,
            )
            evaluator_version_ids.append(version.id)
        service = JobService(
            repository=SQLJobRepository(session),
            task_repository=SQLTaskRepository(session),
            session_repository=sessions,
            agent_version_repository=SQLAgentVersionRepository(session),
            plugin_repository=plugins,
            connection_repository=SQLConnectionRepository(session),
            transitions=TaskTransitions(
                task_repository=SQLTaskRepository(session),
                job_repository=SQLJobRepository(session),
                dispatcher=EventDispatcher(),
            ),
            policy=TaskPolicy(),
        )
        yield Setup(
            session=session,
            engine=engine,
            service=service,
            actor=AuthContext(account=owner),
            input_session_ids=input_session_ids,
            evaluator_version_ids=evaluator_version_ids,
        )


def _build_command(setup: Setup) -> EvaluationBatchCreate:
    return EvaluationBatchCreate(
        input_session_ids=setup.input_session_ids,
        evaluators=[
            EvaluatorConfigInput(evaluator=name, params={"rubric": name})
            for name in EVALUATOR_NAMES
        ],
    )


async def test_create_evaluations_stores_one_task_per_pair(setup: Setup) -> None:
    """Store one pending continue task per (session, evaluator) pair, in order."""
    job = await setup.service.create_evaluations(
        _build_command(setup), actor=setup.actor
    )

    tasks = await SQLTaskRepository(setup.session).list_by_job(job.id)
    evaluation_tasks = [task for task in tasks if isinstance(task, EvaluationTask)]
    assert len(evaluation_tasks) == len(tasks)
    assert [
        (task.input_session_id, task.plugin_version_id, task.params, task.labels)
        for task in evaluation_tasks
    ] == [
        (session_id, version_id, {"rubric": name}, get_plugin_task_labels(name))
        for session_id in setup.input_session_ids
        for name, version_id in zip(
            EVALUATOR_NAMES, setup.evaluator_version_ids, strict=True
        )
    ]
    assert all(
        task.status is TaskStatus.PENDING
        and task.on_failure is TaskOnFailure.CONTINUE
        and task.connection_id is None
        for task in evaluation_tasks
    )


async def test_create_evaluations_inserts_every_pair_in_one_statement(
    setup: Setup,
) -> None:
    """Insert all 100 pair tasks with one INSERT inside one savepoint."""
    with capture_statements(setup.engine) as statements:
        await setup.service.create_evaluations(_build_command(setup), actor=setup.actor)

    task_inserts = [s for s in statements if s.startswith("INSERT INTO task ")]
    assert len(task_inserts) == 1
    # One savepoint for the job row and one for the task rows.
    assert Counter(s.split()[0] for s in statements)["SAVEPOINT"] == 2
