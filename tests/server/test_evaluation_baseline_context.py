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
"""Baseline metadata available only to replay-result evaluation tasks."""

import uuid
from typing import NamedTuple

import pytest

from kitaru.server.adapters.rest.mapping.tasks import spec_to_response
from kitaru.server.application.models.auth import AuthContext, GrantKind
from kitaru.server.application.services.resource_access import (
    build_task_grants,
    check_task_session_read,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import RunSpec
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.session import Session, SessionAccessDenied
from kitaru.server.domain.task import EvaluationTask, EvaluationTaskDetails
from kitaru_test_support import (
    ReplayServices,
    build_replay_services,
    build_task_actor,
    create_agent,
    create_agent_task,
    create_agent_version,
    create_blob,
    create_evaluation_task,
    create_job,
    create_plugin,
    create_session,
)

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


class EvaluationCase(NamedTuple):
    """A replay result and its evaluator, sharing fake-backed services."""

    services: ReplayServices
    baseline: Session
    result: Session
    task: EvaluationTask


@pytest.fixture
async def case() -> EvaluationCase:
    """Create a replay whose baseline and result have distinct metadata."""
    services = build_replay_services()
    agent = await create_agent(services.agents, ACTOR.account.id)
    version = await create_agent_version(
        services.agent_versions,
        agent.id,
        ACTOR.account.id,
        run_spec=RunSpec(command="run.sh"),
    )
    baseline = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent.id,
        agent_version_id=version.id,
        inputs={"question": "case-1"},
        metadata={"case_kind": "refund", "expected": {"amount": 42}, "gold": "yes"},
    )
    result = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent.id,
        agent_version_id=version.id,
        metadata={"gold": "result-only"},
    )
    job = await create_job(services.jobs, ACTOR.account.id)
    await services.replays.create(
        Replay(
            owner_id=ACTOR.account.id,
            job_id=job.id,
            replay_config_id=uuid.uuid4(),
            baseline_session_id=baseline.id,
            result_session_id=result.id,
        )
    )
    plugin = await create_plugin(
        services.plugins,
        ACTOR.account.id,
        PluginKind.EVALUATOR,
        name="accuracy",
    )
    blob = await create_blob(services.blobs, ACTOR.account.id, content=b"code")
    evaluator = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=blob.id, entrypoint="evaluate"),
        display_version=None,
    )
    task = await create_evaluation_task(
        services.tasks,
        job.id,
        plugin_version_id=evaluator.id,
        input_session_id=result.id,
    )
    return EvaluationCase(services, baseline, result, task)


@pytest.mark.parametrize("separate_job", [False, True])
@pytest.mark.parametrize("empty_metadata", [False, True])
async def test_evaluator_spec_carries_baseline_metadata(
    case: EvaluationCase,
    separate_job: bool,
    empty_metadata: bool,
) -> None:
    """Resolve the baseline from the scored result, including later evaluation."""
    services, baseline, result, task = case
    if empty_metadata:
        baseline.update_metadata({})
        await services.sessions.update(baseline)
    if separate_job:
        job = await create_job(services.jobs, ACTOR.account.id)
        task = await create_evaluation_task(
            services.tasks,
            job.id,
            plugin_version_id=task.plugin_version_id,
            input_session_id=result.id,
        )
    spec = await services.task_service.get_spec(task.id, actor=ACTOR)
    assert isinstance(spec.details, EvaluationTaskDetails)
    assert spec.details.baseline is not None
    assert spec.details.baseline.session_id == baseline.id
    assert spec.details.baseline.metadata == baseline.metadata
    assert spec.details.input_session_id == result.id
    response = spec_to_response(spec).model_dump(mode="json")
    assert response["details"]["baseline"] == {
        "session_id": str(baseline.id),
        "metadata": baseline.metadata,
    }
    stored_result = await services.sessions.get(result.id, include_payloads=False)
    assert stored_result.metadata == {"gold": "result-only"}


async def test_evaluating_the_original_baseline_has_no_reference(
    case: EvaluationCase,
) -> None:
    """A shared replay job alone does not give an evaluator reference context."""
    services, baseline, _, task = case
    baseline_task = await create_evaluation_task(
        services.tasks,
        task.job_id,
        plugin_version_id=task.plugin_version_id,
        input_session_id=baseline.id,
    )
    spec = await services.task_service.get_spec(baseline_task.id, actor=ACTOR)
    assert isinstance(spec.details, EvaluationTaskDetails)
    assert spec.details.baseline is None


async def test_reference_context_does_not_grant_baseline_reads(
    case: EvaluationCase,
) -> None:
    """Metadata context grants neither the original session nor its trace."""
    services, baseline, result, task = case
    spec = await services.task_service.get_spec(task.id, actor=ACTOR)
    grants = build_task_grants(spec)
    assert grants[GrantKind.SESSION] == frozenset({result.id})
    actor = build_task_actor(
        ACTOR.account,
        task.id,
        1,
        uuid.uuid4(),
        granted_session_ids=tuple(grants[GrantKind.SESSION]),
    )
    with pytest.raises(SessionAccessDenied):
        await check_task_session_read(baseline, actor, services.sessions)


async def test_agent_spec_does_not_receive_baseline_metadata(
    case: EvaluationCase,
) -> None:
    """Reference labels stay out of agent inputs and its execution spec."""
    services, baseline, result, evaluator = case
    assert baseline.inputs is not None
    task = await create_agent_task(
        services.tasks,
        evaluator.job_id,
        agent_version_id=result.agent_version_id,
        inputs=baseline.inputs.value,
    )
    spec = await services.task_service.get_spec(task.id, actor=ACTOR)
    response = spec_to_response(spec).model_dump(mode="json")
    assert response["details"]["inputs"] == {"question": "case-1"}
    assert "baseline" not in response["details"]
    assert "metadata" not in response["details"]
    assert GrantKind.SESSION not in build_task_grants(spec)


async def test_reference_metadata_is_read_when_building_the_spec(
    case: EvaluationCase,
) -> None:
    """A new spec reflects corrected labels without changing result metadata."""
    services, baseline, _, task = case
    baseline.update_metadata({"gold": "corrected"})
    await services.sessions.update(baseline)
    spec = await services.task_service.get_spec(task.id, actor=ACTOR)
    assert isinstance(spec.details, EvaluationTaskDetails)
    assert spec.details.baseline is not None
    assert spec.details.baseline.metadata == {"gold": "corrected"}


async def test_reference_context_follows_only_the_immediate_baseline(
    case: EvaluationCase,
) -> None:
    """A chained result references its producing replay, without walking ancestry."""
    services, original, intermediate, intermediate_task = case
    result = await create_session(
        services.sessions, ACTOR.account.id, intermediate.agent_id
    )
    job = await create_job(services.jobs, ACTOR.account.id)
    await services.replays.create(
        Replay(
            owner_id=ACTOR.account.id,
            job_id=job.id,
            replay_config_id=uuid.uuid4(),
            baseline_session_id=intermediate.id,
            result_session_id=result.id,
        )
    )
    result_task = await create_evaluation_task(
        services.tasks,
        job.id,
        plugin_version_id=intermediate_task.plugin_version_id,
        input_session_id=result.id,
    )
    for task, baseline in ((intermediate_task, original), (result_task, intermediate)):
        spec = await services.task_service.get_spec(task.id, actor=ACTOR)
        assert isinstance(spec.details, EvaluationTaskDetails)
        assert spec.details.baseline is not None
        assert spec.details.baseline.session_id == baseline.id
        assert spec.details.baseline.metadata == baseline.metadata
