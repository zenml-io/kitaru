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
"""Tests for replay use cases: creation validation, reads, and tool lookup."""

import uuid

import pytest

from conftest import (
    ReplayServices,
    build_replay_services,
    create_agent,
    create_agent_version,
    create_blob,
    create_cohort,
    create_cohort_version,
    create_plugin,
    create_replay,
    create_session,
    create_worker,
)
from kitaru.api_models.v1.replay_config import HistoryScope, ToolPolicyOnMiss
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.replay import ReplayCreate
from kitaru.server.application.models.replay_config import EvaluatorConfigInput
from kitaru.server.application.models.task import TaskFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionWithoutRunSpec,
    RunSpec,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.experiment_run import ExperimentRun
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource
from kitaru.server.domain.replay_config import (
    HistoryConfig,
    PassthroughConfig,
    ReplayConfig,
    ToolPolicy,
)
from kitaru.server.domain.session import Session
from kitaru.server.domain.session_node import SessionNode
from kitaru.server.domain.task import AgentTask

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def services() -> ReplayServices:
    """Provide fake-backed replay, experiment, and run services."""
    return build_replay_services()


async def _agent_version(
    services: ReplayServices, with_run_spec: bool = True, name: str = "assistant"
) -> AgentVersion:
    agent = await create_agent(services.agents, ACTOR.account.id, name=name)
    return await create_agent_version(
        services.agent_versions,
        agent_id=agent.id,
        owner_id=ACTOR.account.id,
        run_spec=RunSpec(command="run.sh") if with_run_spec else None,
    )


async def _evaluator(services: ReplayServices, name: str = "accuracy") -> uuid.UUID:
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, kind=PluginKind.EVALUATOR, name=name
    )
    blob = await create_blob(services.blobs, ACTOR.account.id, content=name.encode())
    version = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=blob.id, entrypoint="score"),
        display_version=None,
    )
    return version.id


async def _session(
    services: ReplayServices, agent_version: AgentVersion, **overrides: object
) -> Session:
    values: dict[str, object] = {
        "agent_id": agent_version.agent_id,
        "agent_version_id": agent_version.id,
        "origin": SessionOrigin.RECORDED,
    }
    values.update(overrides)
    return await create_session(services.sessions, ACTOR.account.id, **values)


async def test_create_replay_runs_the_named_agent_version(
    services: ReplayServices,
) -> None:
    """The named agent version runs, not the one the baseline recorded."""
    recorded = await _agent_version(services)
    replayed = await _agent_version(services, name="successor")
    await _evaluator(services)
    baseline = await _session(services, recorded)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            agent_version_id=replayed.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    agent_task = tasks[0]
    assert isinstance(agent_task, AgentTask)
    assert agent_task.agent_version_id == replayed.id


async def test_create_replay_resolves_baseline_agent_version(
    services: ReplayServices,
) -> None:
    """An omitted agent version resolves to the baseline session's recorded one."""
    agent_version = await _agent_version(services)
    await _evaluator(services)
    baseline = await _session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    agent_task = tasks[0]
    assert isinstance(agent_task, AgentTask)
    assert agent_task.agent_version_id == agent_version.id


async def test_create_replay_requires_agent_version_when_baseline_has_none(
    services: ReplayServices,
) -> None:
    """A baseline with no recorded agent version requires an explicit one."""
    await _evaluator(services)
    baseline = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        origin=SessionOrigin.RECORDED,
        agent_version_id=None,
    )
    with pytest.raises(ValidationError, match="carries no agent version"):
        await services.replay_service.create_replay(
            ReplayCreate(
                baseline_session_id=baseline.id,
                evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
            ),
            actor=ACTOR,
        )


async def test_create_replay_rejects_an_unrunnable_baseline_agent_version(
    services: ReplayServices,
) -> None:
    """A baseline whose recorded version has no run spec is rejected."""
    agent_version = await _agent_version(services, with_run_spec=False)
    await _evaluator(services)
    baseline = await _session(services, agent_version)
    with pytest.raises(AgentVersionWithoutRunSpec):
        await services.replay_service.create_replay(
            ReplayCreate(
                baseline_session_id=baseline.id,
                evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
            ),
            actor=ACTOR,
        )


async def test_create_replay_rejects_agent_version_without_run_spec(
    services: ReplayServices,
) -> None:
    """An agent version without a run spec is rejected at creation."""
    agent_version = await _agent_version(services, with_run_spec=False)
    await _evaluator(services)
    baseline = await _session(services, agent_version)
    with pytest.raises(AgentVersionWithoutRunSpec):
        await services.replay_service.create_replay(
            ReplayCreate(
                baseline_session_id=baseline.id,
                agent_version_id=agent_version.id,
                evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
            ),
            actor=ACTOR,
        )


async def test_create_replay_rejects_cohort_version_scoped_history(
    services: ReplayServices,
) -> None:
    """A standalone replay cannot use cohort-version-scoped history."""
    agent_version = await _agent_version(services)
    await _evaluator(services)
    baseline = await _session(services, agent_version)
    tool_policy = ToolPolicy(
        default=HistoryConfig(
            scope=HistoryScope.COHORT_VERSION, on_miss=ToolPolicyOnMiss.FAIL
        )
    )
    with pytest.raises(
        ValidationError, match="cannot use cohort-version-scoped history"
    ):
        await services.replay_service.create_replay(
            ReplayCreate(
                baseline_session_id=baseline.id,
                agent_version_id=agent_version.id,
                evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
                tool_policy=tool_policy,
            ),
            actor=ACTOR,
        )


def _cache_node(
    session_id: uuid.UUID, index: int, cache_key: str, outputs: object
) -> SessionNode:
    return SessionNode(
        session_id=session_id,
        index=index,
        node_type=NodeType.TOOL_CALL,
        name="search",
        status=NodeStatus.COMPLETED,
        tool_name="search",
        cache_key=cache_key,
        outputs=outputs,
    )


async def _replay_with_history_scope(
    services: ReplayServices,
    scope: HistoryScope,
    baseline: Session,
    experiment_run_id: uuid.UUID | None = None,
) -> uuid.UUID:
    config = await services.experiments.create_replay_config(
        ReplayConfig(
            owner_id=ACTOR.account.id,
            tool_policy=ToolPolicy(
                default=HistoryConfig(scope=scope, on_miss=ToolPolicyOnMiss.FAIL)
            ),
            evaluators=[],
        )
    )
    replay = await create_replay(
        services.replays,
        ACTOR.account.id,
        job_id=uuid.uuid4(),
        replay_config_id=config.id,
        baseline_session_id=baseline.id,
        experiment_run_id=experiment_run_id,
    )
    return replay.id


async def test_tool_lookup_baseline_scope_hit_and_miss(
    services: ReplayServices,
) -> None:
    """Baseline scope matches nodes only within the baseline session."""
    agent_version = await _agent_version(services)
    baseline = await _session(services, agent_version)
    other = await _session(services, agent_version)
    cache_key = "a" * 64
    await services.session_nodes.upsert_batch(
        baseline.id, [_cache_node(baseline.id, 0, cache_key, {"result": "hit"})]
    )
    await services.session_nodes.upsert_batch(
        other.id, [_cache_node(other.id, 0, "b" * 64, {"result": "elsewhere"})]
    )
    replay_id = await _replay_with_history_scope(
        services, HistoryScope.BASELINE, baseline
    )

    hit = await services.replay_service.tool_lookup(
        replay_id, "search", cache_key, actor=ACTOR
    )
    assert hit.found is True
    assert hit.result == {"result": "hit"}

    miss = await services.replay_service.tool_lookup(
        replay_id, "search", "c" * 64, actor=ACTOR
    )
    assert miss.found is False
    assert miss.result is None


async def test_tool_lookup_agent_scope_across_sessions(
    services: ReplayServices,
) -> None:
    """Agent scope matches recorded nodes across every session of the agent."""
    agent_version = await _agent_version(services)
    baseline = await _session(services, agent_version)
    sibling = await _session(services, agent_version)
    replay_result_session = await _session(
        services, agent_version, origin=SessionOrigin.REPLAY
    )
    cache_key = "d" * 64
    await services.session_nodes.upsert_batch(
        sibling.id, [_cache_node(sibling.id, 0, cache_key, {"result": "from-sibling"})]
    )
    await services.session_nodes.upsert_batch(
        replay_result_session.id,
        [
            _cache_node(
                replay_result_session.id, 0, cache_key, {"result": "replay-node"}
            )
        ],
    )
    replay_id = await _replay_with_history_scope(services, HistoryScope.AGENT, baseline)

    result = await services.replay_service.tool_lookup(
        replay_id, "search", cache_key, actor=ACTOR
    )
    assert result.found is True
    assert result.result == {"result": "from-sibling"}


async def test_tool_lookup_cohort_version_scope_within_the_runs_cohort_version(
    services: ReplayServices,
) -> None:
    """Cohort version scope matches nodes only within the run's cohort version."""
    agent_version = await _agent_version(services)
    baseline = await _session(services, agent_version)
    cohort_member = await _session(services, agent_version)
    outside_cohort_version = await _session(services, agent_version)
    cohort = await create_cohort(
        services.cohorts, ACTOR.account.id, agent_version.agent_id
    )
    cohort_version = await create_cohort_version(
        services.cohort_versions,
        ACTOR.account.id,
        cohort.id,
        [baseline.id, cohort_member.id],
    )
    cache_key = "e" * 64
    await services.session_nodes.upsert_batch(
        cohort_member.id,
        [_cache_node(cohort_member.id, 0, cache_key, {"result": "in-cohort-version"})],
    )
    await services.session_nodes.upsert_batch(
        outside_cohort_version.id,
        [_cache_node(outside_cohort_version.id, 0, cache_key, {"result": "outside"})],
    )

    run_id = uuid.uuid4()
    await services.experiment_runs.create(
        ExperimentRun(
            id=run_id,
            owner_id=ACTOR.account.id,
            experiment_id=uuid.uuid4(),
            number=1,
            cohort_version_id=cohort_version.id,
            agent_version_id=agent_version.id,
        )
    )
    replay_id = await _replay_with_history_scope(
        services, HistoryScope.COHORT_VERSION, baseline, experiment_run_id=run_id
    )

    result = await services.replay_service.tool_lookup(
        replay_id, "search", cache_key, actor=ACTOR
    )
    assert result.found is True
    assert result.result == {"result": "in-cohort-version"}


async def test_tool_lookup_non_history_tool_is_rejected(
    services: ReplayServices,
) -> None:
    """A tool whose config is not a history config is rejected."""
    agent_version = await _agent_version(services)
    baseline = await _session(services, agent_version)
    config = await services.experiments.create_replay_config(
        ReplayConfig(
            owner_id=ACTOR.account.id,
            tool_policy=ToolPolicy(default=PassthroughConfig()),
            evaluators=[],
        )
    )
    replay = await create_replay(
        services.replays,
        ACTOR.account.id,
        job_id=uuid.uuid4(),
        replay_config_id=config.id,
        baseline_session_id=baseline.id,
    )
    with pytest.raises(ValidationError, match="not configured for history"):
        await services.replay_service.tool_lookup(
            replay.id, "search", "f" * 64, actor=ACTOR
        )


async def test_tool_lookup_newest_node_wins(services: ReplayServices) -> None:
    """The highest-id node wins when more than one matches."""
    agent_version = await _agent_version(services)
    baseline = await _session(services, agent_version)
    cache_key = "g" * 64
    await services.session_nodes.upsert_batch(
        baseline.id, [_cache_node(baseline.id, 0, cache_key, {"result": "older"})]
    )
    await services.session_nodes.upsert_batch(
        baseline.id, [_cache_node(baseline.id, 1, cache_key, {"result": "newer"})]
    )
    replay_id = await _replay_with_history_scope(
        services, HistoryScope.BASELINE, baseline
    )

    result = await services.replay_service.tool_lookup(
        replay_id, "search", cache_key, actor=ACTOR
    )
    assert result.found is True
    assert result.result == {"result": "newer"}


async def test_get_replay_result_session_id_appears_after_agent_task_links_it(
    services: ReplayServices,
) -> None:
    """result_session_id starts null and appears once the agent task links a session."""
    agent_version = await _agent_version(services)
    await _evaluator(services)
    baseline = await _session(services, agent_version)
    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            agent_version_id=agent_version.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    assert bundle.result_session_id is None

    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    agent_task = tasks[0]
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(worker.id, 10, actor=ACTOR)
    stored_task = await services.tasks.get(agent_task.id)
    assert isinstance(stored_task, AgentTask)
    stored_task.result_session_id = uuid.uuid4()
    await services.tasks.update(stored_task)

    refreshed = await services.replay_service.get_replay(bundle.replay.id, actor=ACTOR)
    assert refreshed.result_session_id == stored_task.result_session_id
