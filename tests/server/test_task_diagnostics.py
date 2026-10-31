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
"""Coverage explanations for pending task queues."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from kitaru.api_models.v1.task import TaskKind, TaskWaitReason
from kitaru.api_models.v1.worker import LabelSelector, WorkerClaim, WorkerScope
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.account import Account
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.task import AgentTask, ImportTask
from kitaru_test_support import (
    build_job_and_task_services,
    build_replay_services,
    create_job,
    create_replay,
    create_worker,
)


@pytest.mark.parametrize(
    "failure",
    ["none", "stale", "claim", "version", "job", "label", "credential", "covered"],
)
async def test_pending_coverage_reason(failure: str) -> None:
    services = build_job_and_task_services()
    actor = AuthContext(account=Account(name="diagnostics"))
    job = await create_job(services.jobs, actor.account.id)
    version_id = uuid.uuid4()
    task = AgentTask(job_id=job.id, agent_version_id=version_id)
    claim = WorkerClaim(kind=TaskKind.AGENT)
    selectors = None
    job_pin = None
    expected = []
    if failure in {"none", "stale"}:
        expected = [TaskWaitReason.NO_LIVE_WORKERS]
    elif failure == "claim":
        claim = WorkerClaim(kind=TaskKind.IMPORTER)
        expected = [TaskWaitReason.CLAIM_MISMATCH]
    elif failure == "version":
        claim = WorkerClaim(kind=TaskKind.AGENT, agent_version_id=uuid.uuid4())
        expected = [TaskWaitReason.CLAIM_MISMATCH]
    elif failure == "job":
        job_pin = uuid.uuid4()
        expected = [TaskWaitReason.JOB_MISMATCH]
    elif failure == "label":
        selectors = [LabelSelector(key="region", values=["eu"], required=True)]
        expected = [TaskWaitReason.SELECTOR_MISMATCH]
    elif failure == "credential":
        task.labels["kitaru/requires-credentials"] = "openai"
        expected = [TaskWaitReason.CREDENTIAL_MISMATCH]
    await services.tasks.create(task)
    if failure != "none":
        await create_worker(
            services.workers,
            actor.account.id,
            scope=WorkerScope(claims=[claim], selectors=selectors, job_id=job_pin),
            last_seen_at=datetime.now(UTC)
            - timedelta(seconds=120 if failure == "stale" else 0),
        )
    result = await services.task_service.get_diagnostics(job.id, None, actor=actor)
    assert result.pending_task_count == 1
    assert result.groups[0].reasons == expected
    assert result.groups[0].task_ids == [task.id]
    assert result.groups[0].agent_version_id == version_id


async def test_all_pages_and_mixed_coverage() -> None:
    services = build_job_and_task_services()
    actor = AuthContext(account=Account(name="diagnostics"))
    job = await create_job(services.jobs, actor.account.id)
    for _ in range(1001):
        await services.tasks.create(ImportTask(job_id=job.id, import_id=uuid.uuid4()))
    await services.tasks.create(AgentTask(job_id=job.id, agent_version_id=uuid.uuid4()))
    await create_worker(
        services.workers,
        actor.account.id,
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.IMPORTER)]),
    )
    result = await services.task_service.get_diagnostics(None, None, actor=actor)
    assert result.pending_task_count == 1002
    by_kind = {group.kind: group for group in result.groups}
    assert by_kind[TaskKind.IMPORTER].task_count == 1001
    assert by_kind[TaskKind.IMPORTER].reasons == []
    assert len(by_kind[TaskKind.IMPORTER].task_ids) == 3
    assert by_kind[TaskKind.AGENT].reasons == [TaskWaitReason.CLAIM_MISMATCH]
    assert not result.truncated


async def test_group_limit_preserves_total() -> None:
    services = build_job_and_task_services()
    actor = AuthContext(account=Account(name="diagnostics"))
    job = await create_job(services.jobs, actor.account.id)
    for _ in range(101):
        await services.tasks.create(
            AgentTask(job_id=job.id, agent_version_id=uuid.uuid4())
        )
    result = await services.task_service.get_diagnostics(None, None, actor=actor)
    assert result.pending_task_count == 101
    assert len(result.groups) == 100
    assert result.truncated


async def test_scope_filters_are_exclusive() -> None:
    services = build_job_and_task_services()
    actor = AuthContext(account=Account(name="diagnostics"))
    with pytest.raises(ValidationError, match="not both"):
        await services.task_service.get_diagnostics(
            uuid.uuid4(), uuid.uuid4(), actor=actor
        )


async def test_run_diagnostics_exclude_unrelated_pending_work() -> None:
    services = build_replay_services()
    actor = AuthContext(account=Account(name="diagnostics"))
    run_id = uuid.uuid4()
    included_job = await create_job(services.jobs, actor.account.id)
    other_job = await create_job(services.jobs, actor.account.id)
    included_task = await services.tasks.create(
        ImportTask(job_id=included_job.id, import_id=uuid.uuid4())
    )
    await services.tasks.create(ImportTask(job_id=other_job.id, import_id=uuid.uuid4()))
    await create_replay(
        services.replays,
        actor.account.id,
        included_job.id,
        replay_config_id=uuid.uuid4(),
        baseline_session_id=uuid.uuid4(),
        experiment_run_id=run_id,
    )
    result = await services.task_service.get_diagnostics(None, run_id, actor=actor)
    assert result.pending_task_count == 1
    assert result.groups[0].task_ids == [included_task.id]
    empty = await services.task_service.get_diagnostics(None, uuid.uuid4(), actor=actor)
    assert empty.pending_task_count == 0
