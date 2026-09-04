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
"""End-to-end tests for insight recording off completed analysis tasks."""

import uuid
from typing import Any

import pytest

from conftest import (
    ReplayServices,
    build_replay_services,
    build_task_actor,
    build_worker_actor,
    create_agent,
    create_analysis_task,
    create_blob,
    create_job,
    create_plugin,
    create_worker,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.job import JobKind
from kitaru.api_models.v1.task import TaskStatus
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.insight import InsightFilter
from kitaru.server.application.models.task import TaskUpdate
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.plugin import PluginKind, PluginVersion, ScriptPluginSource
from kitaru.server.domain.task import AnalysisTask
from kitaru.server.filtering import FilterCondition
from kitaru.server.utils import hash_params

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def services() -> ReplayServices:
    """Provide fake-backed services sharing the production subscribers."""
    return build_replay_services()


async def _analyzer_version(
    services: ReplayServices, name: str = "trends"
) -> PluginVersion:
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, kind=PluginKind.ANALYZER, name=name
    )
    blob = await create_blob(services.blobs, ACTOR.account.id, content=name.encode())
    return await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=blob.id, entrypoint="analyze"),
        display_version=None,
    )


async def _analysis_task_with_job(
    services: ReplayServices,
    agent: Agent,
    plugin_version_id: uuid.UUID,
    session_ids: list[uuid.UUID] | None = None,
    params: dict[str, Any] | None = None,
) -> AnalysisTask:
    job = await create_job(services.jobs, ACTOR.account.id, kind=JobKind.SESSION_RUN)
    return await create_analysis_task(
        services.tasks,
        job.id,
        plugin_version_id=plugin_version_id,
        agent_id=agent.id,
        input_session_ids=session_ids if session_ids is not None else [uuid.uuid4()],
        params=params if params is not None else {"focus": "errors"},
    )


async def _claim_and_start(
    services: ReplayServices, task: AnalysisTask
) -> AnalysisTask:
    worker = await create_worker(services.workers, ACTOR.account.id)
    (claimed,) = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    running = claimed.task
    assert isinstance(running, AnalysisTask)
    await services.task_service.update_task(
        running.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(ACTOR.account, running.id, running.attempt, worker.id),
    )
    return running


async def _finish(
    services: ReplayServices, task: AnalysisTask, command: TaskUpdate
) -> None:
    worker_id = task.worker_id
    assert worker_id is not None
    await services.task_service.update_task(
        task.id,
        command,
        actor=build_task_actor(ACTOR.account, task.id, task.attempt, worker_id),
    )


async def _complete(
    services: ReplayServices, task: AnalysisTask, command: TaskUpdate
) -> None:
    running = await _claim_and_start(services, task)
    await _finish(services, running, command)


def _insight_result(name: str) -> dict[str, Any]:
    return {
        "name": name,
        "title": name.title(),
        "data": {"type": "text", "content": "ok"},
    }


async def _agent_insights(services: ReplayServices, agent_id: uuid.UUID) -> list[Any]:
    items, _ = await services.insights.query(
        InsightFilter(
            expression=FilterCondition(field="agent_id", op=FilterOp.EQ, value=agent_id)
        )
    )
    return [item.insight for item in items]


async def test_completed_task_writes_one_insight_per_result(
    services: ReplayServices,
) -> None:
    """A completed analysis task writes one insight per result under its agent."""
    agent = await create_agent(services.agents, ACTOR.account.id)
    version = await _analyzer_version(services)
    session_ids = [uuid.uuid4(), uuid.uuid4()]
    params = {"focus": "errors"}
    task = await _analysis_task_with_job(
        services, agent, version.id, session_ids, params
    )

    await _complete(
        services,
        task,
        TaskUpdate(
            status=TaskStatus.COMPLETED,
            result=[_insight_result("summary"), _insight_result("risks")],
        ),
    )

    insights = await _agent_insights(services, agent.id)
    assert {insight.name for insight in insights} == {"summary", "risks"}
    for insight in insights:
        assert insight.agent_id == agent.id
        assert insight.analyzer_version_id == version.id
        assert insight.task_id == task.id
        assert insight.analyzer_params == params
        assert insight.params_hash == hash_params(params)


async def test_failed_task_writes_nothing(services: ReplayServices) -> None:
    """A failed analysis task writes no insight."""
    agent = await create_agent(services.agents, ACTOR.account.id)
    version = await _analyzer_version(services)
    task = await _analysis_task_with_job(services, agent, version.id)

    await _complete(
        services, task, TaskUpdate(status=TaskStatus.FAILED, error="analysis failed")
    )

    assert await _agent_insights(services, agent.id) == []


async def test_vanished_plugin_version_writes_nothing(
    services: ReplayServices,
) -> None:
    """An analysis task whose analyzer version was deleted writes no insight."""
    agent = await create_agent(services.agents, ACTOR.account.id)
    version = await _analyzer_version(services)
    task = await _analysis_task_with_job(services, agent, version.id)
    running = await _claim_and_start(services, task)
    # Delete the analyzer while its task is mid-flight.
    await services.plugins.delete(version.plugin_id)

    await _finish(
        services,
        running,
        TaskUpdate(status=TaskStatus.COMPLETED, result=[_insight_result("summary")]),
    )

    assert await _agent_insights(services, agent.id) == []


async def test_deleted_agent_writes_nothing(services: ReplayServices) -> None:
    """An analysis task whose agent was deleted writes no insight."""
    agent = await create_agent(services.agents, ACTOR.account.id)
    version = await _analyzer_version(services)
    task = await _analysis_task_with_job(services, agent, version.id)
    await services.agents.mark_deleted(agent.id)

    await _complete(
        services,
        task,
        TaskUpdate(status=TaskStatus.COMPLETED, result=[_insight_result("summary")]),
    )

    assert await _agent_insights(services, agent.id) == []


async def test_insight_names_repeat_across_tasks_without_conflict(
    services: ReplayServices,
) -> None:
    """Two analysis tasks can each produce an insight with the same name."""
    agent = await create_agent(services.agents, ACTOR.account.id)
    version = await _analyzer_version(services)
    first = await _analysis_task_with_job(services, agent, version.id)

    await _complete(
        services,
        first,
        TaskUpdate(status=TaskStatus.COMPLETED, result=[_insight_result("summary")]),
    )

    second = await _analysis_task_with_job(services, agent, version.id)
    await _complete(
        services,
        second,
        TaskUpdate(status=TaskStatus.COMPLETED, result=[_insight_result("summary")]),
    )

    insights = await _agent_insights(services, agent.id)
    assert len(insights) == 2
    assert {insight.task_id for insight in insights} == {first.id, second.id}
