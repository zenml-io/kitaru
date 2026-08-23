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
from typing import Any

import pytest

from conftest import (
    ReplayServices,
    build_replay_services,
    create_agent,
    create_agent_task,
    create_agent_version,
    create_blob,
    create_cohort,
    create_cohort_version,
    create_plugin,
    create_replay,
    create_session,
    create_worker,
    get_replay_job_id,
)
from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.replay_config import HistoryScope, ToolPolicyOnMiss
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import WorkerClaim, WorkerScope
from kitaru.server.application.models.auth import (
    AuthContext,
    TaskPrincipal,
    WorkerAuthContext,
    WorkerPrincipal,
)
from kitaru.server.application.models.replay import ReplayCreate, ReplayFilter
from kitaru.server.application.models.replay_config import EvaluatorConfigInput
from kitaru.server.application.models.task import TaskFilter
from kitaru.server.application.services.replay_service import ReplayService
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionWithoutRunSpec,
    RunSpec,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.experiment_run import ExperimentRun
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource
from kitaru.server.domain.replay import (
    ReplayAccessDenied,
    ReplayInUse,
    ReplayNotFound,
)
from kitaru.server.domain.replay_config import (
    HistoryConfig,
    PassthroughConfig,
    ReplayConfig,
    ReplayOverride,
    ToolPolicy,
)
from kitaru.server.domain.session import Session
from kitaru.server.domain.session_node import SessionNode
from kitaru.server.domain.task import AgentTask
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


class _RecordingAnalytics(ServerAnalytics):
    """Analytics tracker recording track calls instead of buffering them."""

    def __init__(self) -> None:
        """Initialize the tracker."""
        self.tracked: list[tuple[uuid.UUID, AnalyticsEvent | str, dict[str, Any]]] = []

    def track(
        self,
        user_id: uuid.UUID,
        event: AnalyticsEvent | str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Record a track call instead of buffering it.

        Args:
            user_id: User id.
            event: Event name.
            properties: Event properties.
        """
        self.tracked.append((user_id, event, properties or {}))


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


async def _evaluator(
    services: ReplayServices, name: str = "accuracy", agent_id: uuid.UUID | None = None
) -> uuid.UUID:
    plugin = await create_plugin(
        services.plugins,
        ACTOR.account.id,
        kind=PluginKind.EVALUATOR,
        name=name,
        agent_id=agent_id,
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


async def test_create_replay_evaluator_scoped_to_other_agent(
    services: ReplayServices,
) -> None:
    """Reject an evaluator scoped to an agent other than the baseline's."""
    agent_version = await _agent_version(services)
    await _evaluator(services, agent_id=uuid.uuid4())
    baseline = await _session(services, agent_version)
    with pytest.raises(ValidationError, match="scoped to a different agent"):
        await services.replay_service.create_replay(
            ReplayCreate(
                baseline_session_id=baseline.id,
                agent_version_id=agent_version.id,
                evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
            ),
            actor=ACTOR,
        )


async def test_create_replay_evaluator_scoped_to_baseline_agent(
    services: ReplayServices,
) -> None:
    """Resolve an evaluator scoped to the baseline session's own agent."""
    agent_version = await _agent_version(services)
    baseline = await _session(services, agent_version)
    await _evaluator(services, agent_id=baseline.agent_id)
    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            agent_version_id=agent_version.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    assert bundle.replay.baseline_session_id == baseline.id


def _cache_node(
    session_id: uuid.UUID,
    index: int,
    cache_key: str,
    outputs: object,
    status: NodeStatus = NodeStatus.COMPLETED,
    error: str | None = None,
) -> SessionNode:
    return SessionNode(
        session_id=session_id,
        index=index,
        node_type=NodeType.TOOL_CALL,
        name="search",
        status=status,
        error=error,
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
        replay_id, "search", cache_key, None, actor=ACTOR
    )
    assert hit is not None
    assert hit.result == {"result": "hit"}

    miss = await services.replay_service.tool_lookup(
        replay_id, "search", "c" * 64, None, actor=ACTOR
    )
    assert miss is None


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
        replay_id, "search", cache_key, None, actor=ACTOR
    )
    assert result is not None
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
        replay_id, "search", cache_key, None, actor=ACTOR
    )
    assert result is not None
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
            replay.id, "search", "f" * 64, None, actor=ACTOR
        )


async def test_tool_lookup_without_occurrence_newest_node_wins(
    services: ReplayServices,
) -> None:
    """The highest-id node wins when several match and no occurrence is given."""
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
        replay_id, "search", cache_key, None, actor=ACTOR
    )
    assert result is not None
    assert result.result == {"result": "newer"}


async def test_tool_lookup_occurrence_replays_baseline_order(
    services: ReplayServices,
) -> None:
    """Occurrences resolve repeated identical calls in baseline order."""
    agent_version = await _agent_version(services)
    baseline = await _session(services, agent_version)
    cache_key = "h" * 64
    await services.session_nodes.upsert_batch(
        baseline.id,
        [
            _cache_node(baseline.id, index, cache_key, {"ticket": ticket})
            for index, ticket in enumerate(["a", "b", "c"])
        ],
    )
    replay_id = await _replay_with_history_scope(
        services, HistoryScope.BASELINE, baseline
    )

    results = [
        await services.replay_service.tool_lookup(
            replay_id, "search", cache_key, occurrence, actor=ACTOR
        )
        for occurrence in range(3)
    ]
    assert all(result is not None for result in results)
    assert [result.result for result in results if result is not None] == [
        {"ticket": "a"},
        {"ticket": "b"},
        {"ticket": "c"},
    ]

    exhausted = await services.replay_service.tool_lookup(
        replay_id, "search", cache_key, 3, actor=ACTOR
    )
    assert exhausted is None


async def test_tool_lookup_occurrence_interleaves_cache_keys(
    services: ReplayServices,
) -> None:
    """Occurrences advance per cache key, unaffected by other keys in between."""
    agent_version = await _agent_version(services)
    baseline = await _session(services, agent_version)
    first_key = "i" * 64
    second_key = "j" * 64
    await services.session_nodes.upsert_batch(
        baseline.id,
        [
            _cache_node(baseline.id, 0, first_key, {"ticket": "a"}),
            _cache_node(baseline.id, 1, second_key, {"ticket": "x"}),
            _cache_node(baseline.id, 2, first_key, {"ticket": "b"}),
        ],
    )
    replay_id = await _replay_with_history_scope(
        services, HistoryScope.BASELINE, baseline
    )

    first = await services.replay_service.tool_lookup(
        replay_id, "search", first_key, 0, actor=ACTOR
    )
    second = await services.replay_service.tool_lookup(
        replay_id, "search", second_key, 0, actor=ACTOR
    )
    third = await services.replay_service.tool_lookup(
        replay_id, "search", first_key, 1, actor=ACTOR
    )
    assert first is not None
    assert second is not None
    assert third is not None
    assert first.result == {"ticket": "a"}
    assert second.result == {"ticket": "x"}
    assert third.result == {"ticket": "b"}


async def test_tool_lookup_occurrence_rejected_for_non_baseline_scope(
    services: ReplayServices,
) -> None:
    """An occurrence is rejected when the tool's history scope is not baseline."""
    agent_version = await _agent_version(services)
    baseline = await _session(services, agent_version)
    replay_id = await _replay_with_history_scope(services, HistoryScope.AGENT, baseline)

    with pytest.raises(ValidationError, match="does not support occurrence"):
        await services.replay_service.tool_lookup(
            replay_id, "search", "k" * 64, 0, actor=ACTOR
        )


async def test_tool_lookup_occurrence_matches_a_failed_node(
    services: ReplayServices,
) -> None:
    """Baseline scope with an occurrence matches a failed node and its error."""
    agent_version = await _agent_version(services)
    baseline = await _session(services, agent_version)
    cache_key = "l" * 64
    await services.session_nodes.upsert_batch(
        baseline.id,
        [
            _cache_node(
                baseline.id,
                0,
                cache_key,
                None,
                status=NodeStatus.FAILED,
                error="tool raised an exception",
            )
        ],
    )
    replay_id = await _replay_with_history_scope(
        services, HistoryScope.BASELINE, baseline
    )

    result = await services.replay_service.tool_lookup(
        replay_id, "search", cache_key, 0, actor=ACTOR
    )

    assert result is not None
    assert result.status == NodeStatus.FAILED
    assert result.error == "tool raised an exception"


async def test_tool_lookup_occurrence_skips_an_in_progress_node(
    services: ReplayServices,
) -> None:
    """Baseline scope with an occurrence skips an in-progress node."""
    agent_version = await _agent_version(services)
    baseline = await _session(services, agent_version)
    cache_key = "m" * 64
    await services.session_nodes.upsert_batch(
        baseline.id,
        [
            _cache_node(baseline.id, 0, cache_key, None, status=NodeStatus.IN_PROGRESS),
            _cache_node(baseline.id, 1, cache_key, {"ticket": "b"}),
        ],
    )
    replay_id = await _replay_with_history_scope(
        services, HistoryScope.BASELINE, baseline
    )

    result = await services.replay_service.tool_lookup(
        replay_id, "search", cache_key, 0, actor=ACTOR
    )

    assert result is not None
    assert result.result == {"ticket": "b"}


async def test_tool_lookup_without_occurrence_skips_a_failed_node(
    services: ReplayServices,
) -> None:
    """Baseline scope without an occurrence skips a failed node."""
    agent_version = await _agent_version(services)
    baseline = await _session(services, agent_version)
    cache_key = "n" * 64
    await services.session_nodes.upsert_batch(
        baseline.id,
        [_cache_node(baseline.id, 0, cache_key, None, status=NodeStatus.FAILED)],
    )
    replay_id = await _replay_with_history_scope(
        services, HistoryScope.BASELINE, baseline
    )

    result = await services.replay_service.tool_lookup(
        replay_id, "search", cache_key, None, actor=ACTOR
    )

    assert result is None


async def test_tool_lookup_agent_scope_skips_a_failed_node(
    services: ReplayServices,
) -> None:
    """Agent scope skips a failed node."""
    agent_version = await _agent_version(services)
    baseline = await _session(services, agent_version)
    sibling = await _session(services, agent_version)
    cache_key = "o" * 64
    await services.session_nodes.upsert_batch(
        sibling.id,
        [_cache_node(sibling.id, 0, cache_key, None, status=NodeStatus.FAILED)],
    )
    replay_id = await _replay_with_history_scope(services, HistoryScope.AGENT, baseline)

    result = await services.replay_service.tool_lookup(
        replay_id, "search", cache_key, None, actor=ACTOR
    )

    assert result is None


async def test_tool_lookup_cohort_version_scope_skips_a_failed_node(
    services: ReplayServices,
) -> None:
    """Cohort version scope skips a failed node."""
    agent_version = await _agent_version(services)
    baseline = await _session(services, agent_version)
    cohort_member = await _session(services, agent_version)
    cohort = await create_cohort(
        services.cohorts, ACTOR.account.id, agent_version.agent_id
    )
    cohort_version = await create_cohort_version(
        services.cohort_versions,
        ACTOR.account.id,
        cohort.id,
        [baseline.id, cohort_member.id],
    )
    cache_key = "p" * 64
    await services.session_nodes.upsert_batch(
        cohort_member.id,
        [_cache_node(cohort_member.id, 0, cache_key, None, status=NodeStatus.FAILED)],
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
        replay_id, "search", cache_key, None, actor=ACTOR
    )

    assert result is None


async def test_get_replay_result_session_id_appears_once_linked_on_the_replay(
    services: ReplayServices,
) -> None:
    """result_session_id starts null and appears once linked on the replay row."""
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
    assert bundle.replay.result_session_id is None

    replay = await services.replays.get_by_job_id(get_replay_job_id(bundle.replay))
    assert replay is not None
    result_session_id = uuid.uuid4()
    replay.link_result_session(result_session_id)
    await services.replays.update(replay)

    refreshed = await services.replay_service.get_replay(bundle.replay.id, actor=ACTOR)
    assert refreshed.replay.result_session_id == result_session_id


async def test_replay_agent_tasks_claim_only_their_scoped_version(
    services: ReplayServices,
) -> None:
    """Two version-scoped workers each claim only their replay's agent version."""
    first_version = await _agent_version(services, name="first")
    second_version = await _agent_version(services, name="second")
    first_baseline = await _session(services, first_version)
    second_baseline = await _session(services, second_version)

    first_bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=first_baseline.id,
            agent_version_id=first_version.id,
            evaluators=[],
        ),
        actor=ACTOR,
    )
    second_bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=second_baseline.id,
            agent_version_id=second_version.id,
            evaluators=[],
        ),
        actor=ACTOR,
    )

    first_worker = await create_worker(
        services.workers,
        ACTOR.account.id,
        name="worker-first",
        scope=WorkerScope(
            claims=[WorkerClaim(kind=TaskKind.AGENT, agent_version_id=first_version.id)]
        ),
    )
    second_worker = await create_worker(
        services.workers,
        ACTOR.account.id,
        name="worker-second",
        scope=WorkerScope(
            claims=[
                WorkerClaim(kind=TaskKind.AGENT, agent_version_id=second_version.id)
            ]
        ),
    )

    claimed_first = await services.task_service.claim_tasks(
        10,
        actor=WorkerAuthContext(
            account=ACTOR.account, principal=WorkerPrincipal(worker_id=first_worker.id)
        ),
    )
    claimed_second = await services.task_service.claim_tasks(
        10,
        actor=WorkerAuthContext(
            account=ACTOR.account, principal=WorkerPrincipal(worker_id=second_worker.id)
        ),
    )

    first_tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=first_bundle.replay.job_id), actor=ACTOR
    )
    second_tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=second_bundle.replay.job_id), actor=ACTOR
    )
    assert [item.task.id for item in claimed_first] == [task.id for task in first_tasks]
    assert [item.task.id for item in claimed_second] == [
        task.id for task in second_tasks
    ]


def _replay_service_with_analytics(
    services: ReplayServices, analytics: ServerAnalytics
) -> ReplayService:
    return ReplayService(
        repository=services.replays,
        experiment_repository=services.experiments,
        experiment_run_repository=services.experiment_runs,
        job_repository=services.jobs,
        task_repository=services.tasks,
        session_repository=services.sessions,
        session_node_repository=services.session_nodes,
        agent_version_repository=services.agent_versions,
        plugin_repository=services.plugins,
        analytics=analytics,
    )


async def test_create_replay_tracks_replay_created(services: ReplayServices) -> None:
    """Fire REPLAY_CREATED with the override, tool policy and evaluator info."""
    agent_version = await _agent_version(services)
    await _evaluator(services)
    baseline = await _session(services, agent_version)
    analytics = _RecordingAnalytics()
    service = _replay_service_with_analytics(services, analytics)

    await service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
            override=ReplayOverride(prompt="hi"),
        ),
        actor=ACTOR,
    )

    assert len(analytics.tracked) == 1
    user_id, event, properties = analytics.tracked[0]
    assert user_id == ACTOR.account.id
    assert event == AnalyticsEvent.REPLAY_CREATED
    assert properties == {
        "model_override": False,
        "system_prompt_override": False,
        "prompt_override": True,
        "model_params_override": False,
        "tool_policy_default": "passthrough",
        "tool_override_count": 0,
        "tool_override_types": [],
        "evaluator_count": 1,
    }


async def test_create_replay_without_analytics_tracker(
    services: ReplayServices,
) -> None:
    """Create a replay normally when no analytics tracker is configured."""
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
    assert bundle.replay.owner_id == ACTOR.account.id


async def _replay_bundle(services: ReplayServices):
    """Create a replay whose job carries one agent task."""
    agent_version = await _agent_version(services)
    await _evaluator(services)
    baseline = await _session(services, agent_version)
    return await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            agent_version_id=agent_version.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )


def _task_actor_for(task_id: uuid.UUID, job_id: uuid.UUID) -> AuthContext:
    """Build an auth context for a task principal running a task of the given job."""
    return AuthContext(
        account=ACTOR.account,
        principal=TaskPrincipal(
            task_id=task_id, attempt=1, worker_id=uuid.uuid4(), job_id=job_id
        ),
    )


async def test_get_replay_allows_a_task_principal_from_the_replays_job(
    services: ReplayServices,
) -> None:
    """Allow a task principal whose task belongs to the replay's job."""
    bundle = await _replay_bundle(services)
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    refreshed = await services.replay_service.get_replay(
        bundle.replay.id, actor=_task_actor_for(tasks[0].id, bundle.replay.job_id)
    )
    assert refreshed.replay.id == bundle.replay.id


async def test_get_replay_denies_a_task_principal_from_another_job(
    services: ReplayServices,
) -> None:
    """Reject a task principal whose task belongs to another job."""
    bundle = await _replay_bundle(services)
    foreign_job_id = uuid.uuid4()
    foreign_task = await create_agent_task(services.tasks, foreign_job_id)
    with pytest.raises(ReplayAccessDenied):
        await services.replay_service.get_replay(
            bundle.replay.id, actor=_task_actor_for(foreign_task.id, foreign_job_id)
        )


async def test_tool_lookup_denies_a_task_principal_from_another_job(
    services: ReplayServices,
) -> None:
    """Reject a foreign task principal before the tool config is read."""
    bundle = await _replay_bundle(services)
    foreign_job_id = uuid.uuid4()
    foreign_task = await create_agent_task(services.tasks, foreign_job_id)
    with pytest.raises(ReplayAccessDenied):
        await services.replay_service.tool_lookup(
            bundle.replay.id,
            "search",
            "cache-key",
            None,
            actor=_task_actor_for(foreign_task.id, foreign_job_id),
        )


async def test_list_replays_filters_by_result_session(
    services: ReplayServices,
) -> None:
    """List replays filters on the result session linked on the replay row."""
    agent_version = await _agent_version(services)
    await _evaluator(services)

    async def replay_with_result_session() -> tuple[uuid.UUID, uuid.UUID]:
        baseline = await _session(services, agent_version)
        bundle = await services.replay_service.create_replay(
            ReplayCreate(
                baseline_session_id=baseline.id,
                evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
            ),
            actor=ACTOR,
        )
        replay = await services.replays.get_by_job_id(get_replay_job_id(bundle.replay))
        assert replay is not None
        result_session_id = uuid.uuid4()
        replay.link_result_session(result_session_id)
        await services.replays.update(replay)
        return bundle.replay.id, result_session_id

    wanted_replay_id, wanted_session_id = await replay_with_result_session()
    await replay_with_result_session()

    matches, _ = await services.replay_service.list_replays(
        ReplayFilter(
            expression=FilterCondition(
                field="result_session_id", op=FilterOp.EQ, value=wanted_session_id
            )
        ),
        actor=ACTOR,
    )
    assert [bundle.replay.id for bundle in matches] == [wanted_replay_id]
    assert matches[0].replay.result_session_id == wanted_session_id

    empty, _ = await services.replay_service.list_replays(
        ReplayFilter(
            expression=FilterCondition(
                field="result_session_id", op=FilterOp.EQ, value=uuid.uuid4()
            )
        ),
        actor=ACTOR,
    )
    assert empty == []


async def test_list_replays_filters_on_a_missing_result_session(
    services: ReplayServices,
) -> None:
    """An is_null result session filter keeps the replays with no result yet."""
    agent_version = await _agent_version(services)
    await _evaluator(services)

    baseline = await _session(services, agent_version)
    linked = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    replay = await services.replays.get_by_job_id(get_replay_job_id(linked.replay))
    assert replay is not None
    replay.link_result_session(uuid.uuid4())
    await services.replays.update(replay)

    other_baseline = await _session(services, agent_version)
    unlinked = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=other_baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )

    pending, _ = await services.replay_service.list_replays(
        ReplayFilter(
            expression=FilterCondition(field="result_session_id", op=FilterOp.IS_NULL)
        ),
        actor=ACTOR,
    )
    assert [bundle.replay.id for bundle in pending] == [unlinked.replay.id]


async def test_delete_replay_removes_a_standalone_replay(
    services: ReplayServices,
) -> None:
    """Deleting a standalone replay removes it."""
    bundle = await _replay_bundle(services)
    await services.replay_service.delete_replay(bundle.replay.id, actor=ACTOR)
    with pytest.raises(ReplayNotFound):
        await services.replays.get(bundle.replay.id)


async def test_delete_replay_rejects_a_replay_of_an_experiment_run(
    services: ReplayServices,
) -> None:
    """Deleting a replay that belongs to an experiment run conflicts."""
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
        experiment_run_id=uuid.uuid4(),
    )
    with pytest.raises(ReplayInUse):
        await services.replay_service.delete_replay(replay.id, actor=ACTOR)
    assert await services.replays.get(replay.id) == replay


async def test_delete_replay_not_found(services: ReplayServices) -> None:
    """Deleting a replay that does not exist raises."""
    with pytest.raises(ReplayNotFound):
        await services.replay_service.delete_replay(uuid.uuid4(), actor=ACTOR)
