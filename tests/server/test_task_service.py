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
"""Tests for task use cases."""

import uuid
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Any

import pytest
from pydantic import SecretStr

from conftest import (
    FakeJobRepository,
    FakeTaskRepository,
    JobAndTaskServices,
    build_job_and_task_services,
    build_task_actor,
    build_worker_actor,
    create_agent,
    create_agent_task,
    create_agent_version,
    create_blob,
    create_evaluation_task,
    create_import_task,
    create_job,
    create_plugin,
    create_secret,
    create_session,
    create_worker,
)
from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.task import LabelSelector, TaskKind, TaskStatus, WorkerScope
from kitaru.server.application.events import EventDispatcher
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.task import TaskFilter, TaskPolicy, TaskUpdate
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import RunSpec
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource
from kitaru.server.domain.task import (
    AgentTask,
    ImportTask,
    ImportTaskDetails,
    Task,
    TaskAttemptMismatch,
    TaskResultSessionMissing,
    TaskResultSessionNotCompleted,
    TaskResultTooLarge,
    TaskUpdateRequiresStatus,
)
from kitaru.server.domain.worker import WorkerNotFound

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def services() -> JobAndTaskServices:
    """Provide fake-backed job and task services."""
    return build_job_and_task_services()


async def _pending_job(services: JobAndTaskServices) -> uuid.UUID:
    job = await create_job(services.jobs, ACTOR.account.id)
    return job.id


async def _claimable_agent_task(
    services: JobAndTaskServices,
    job_id: uuid.UUID,
    **overrides: Any,
) -> AgentTask:
    """Store an agent task backed by a real agent version, so its spec builds."""
    agent = await create_agent(
        services.agents, ACTOR.account.id, name=f"a{uuid.uuid4().hex[:8]}"
    )
    version = await create_agent_version(
        services.agent_versions,
        agent_id=agent.id,
        owner_id=ACTOR.account.id,
        run_spec=RunSpec(command="run.sh", timeout_seconds=60),
    )
    return await create_agent_task(
        services.tasks, job_id, agent_version_id=version.id, **overrides
    )


async def _claimable_import_task(
    services: JobAndTaskServices,
    job_id: uuid.UUID,
    **overrides: Any,
) -> ImportTask:
    """Store an importer task backed by a real plugin version, so its spec builds."""
    plugin = await create_plugin(
        services.plugins,
        ACTOR.account.id,
        PluginKind.IMPORTER,
        name=f"imp{uuid.uuid4().hex[:8]}",
    )
    version = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="run"),
        display_version=None,
    )
    code_blob = await create_blob(
        services.blobs, ACTOR.account.id, content=uuid.uuid4().bytes
    )
    version = version.model_copy(
        update={"source": ScriptPluginSource(blob_id=code_blob.id, entrypoint="run")}
    )
    await services.plugins.update_version(version)
    payload = await create_blob(
        services.blobs, ACTOR.account.id, content=uuid.uuid4().bytes
    )
    return await create_import_task(
        services.tasks,
        job_id,
        plugin_version_id=version.id,
        payload_blob_id=payload.id,
        **overrides,
    )


async def test_claim_tasks_not_found(services: JobAndTaskServices) -> None:
    """Claiming against an unknown worker id conflicts."""
    with pytest.raises(WorkerNotFound):
        await services.task_service.claim_tasks(
            10, actor=build_worker_actor(ACTOR.account, uuid.uuid4())
        )


async def test_claim_tasks_orders_by_id_and_respects_max(
    services: JobAndTaskServices,
) -> None:
    """Claim hands out the oldest pending tasks first, capped by max_tasks."""
    job_id = await _pending_job(services)
    for _ in range(3):
        await _claimable_agent_task(services, job_id)
    worker = await create_worker(services.workers, ACTOR.account.id)

    claimed = await services.task_service.claim_tasks(
        2, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    assert len(claimed) == 2
    assert [item.task.status for item in claimed] == [TaskStatus.CLAIMED] * 2
    assert claimed[0].task.attempt == 1
    assert claimed[0].task.worker_id == worker.id


async def test_claim_starts_the_job_once(services: JobAndTaskServices) -> None:
    """The first claim of a job's task moves the job to running."""
    job_id = await _pending_job(services)
    await _claimable_agent_task(services, job_id)
    await _claimable_agent_task(services, job_id)
    worker = await create_worker(services.workers, ACTOR.account.id)

    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    job = await services.jobs.get(job_id)
    assert job.status.value == "running"
    assert job.started_at is not None


async def test_claim_scope_kind_filter(services: JobAndTaskServices) -> None:
    """A kind-scoped worker claims only tasks of that kind."""
    job_id = await _pending_job(services)
    await _claimable_agent_task(services, job_id)
    import_task = await _claimable_import_task(services, job_id)
    worker = await create_worker(
        services.workers, ACTOR.account.id, scope=WorkerScope(kinds=[TaskKind.IMPORTER])
    )

    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    assert len(claimed) == 1
    assert claimed[0].task.id == import_task.id
    assert claimed[0].task.kind is TaskKind.IMPORTER


async def test_claim_scope_job_pin(services: JobAndTaskServices) -> None:
    """A job-pinned worker claims only that job's tasks."""
    job_id = await _pending_job(services)
    other_job_id = await _pending_job(services)
    pinned = await _claimable_agent_task(services, job_id)
    await _claimable_agent_task(services, other_job_id)
    worker = await create_worker(
        services.workers, ACTOR.account.id, scope=WorkerScope(job_id=job_id)
    )

    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    assert [item.task.id for item in claimed] == [pinned.id]


async def test_claim_scope_required_selector(services: JobAndTaskServices) -> None:
    """A required selector only matches tasks carrying a matching label."""
    job_id = await _pending_job(services)
    labeled = await _claimable_agent_task(services, job_id, labels={"env": "prod"})
    await _claimable_agent_task(services, job_id, labels={"env": "dev"})
    await _claimable_agent_task(services, job_id)
    worker = await create_worker(
        services.workers,
        ACTOR.account.id,
        scope=WorkerScope(
            selectors=[LabelSelector(key="env", values=["prod"], required=True)]
        ),
    )

    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    assert [item.task.id for item in claimed] == [labeled.id]


async def test_claim_scope_non_required_selector_matches_unlabeled(
    services: JobAndTaskServices,
) -> None:
    """A non-required selector also matches tasks that lack the key."""
    job_id = await _pending_job(services)
    matching = await _claimable_agent_task(services, job_id, labels={"env": "prod"})
    unlabeled = await _claimable_agent_task(services, job_id)
    await _claimable_agent_task(services, job_id, labels={"env": "dev"})
    worker = await create_worker(
        services.workers,
        ACTOR.account.id,
        scope=WorkerScope(
            selectors=[LabelSelector(key="env", values=["prod"], required=False)]
        ),
    )

    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    assert {item.task.id for item in claimed} == {matching.id, unlabeled.id}


async def test_claim_sweeps_stale_tasks_first(services: JobAndTaskServices) -> None:
    """A stale claimed task requeues before the claim query runs."""
    job_id = await _pending_job(services)
    stale = await _claimable_agent_task(services, job_id)
    stuck_worker = await create_worker(services.workers, ACTOR.account.id, name="stuck")
    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, stuck_worker.id)
    )
    assert len(claimed) == 1

    stored = await services.tasks.get(stale.id)
    stored.claimed_at = datetime.now(UTC) - timedelta(hours=1)
    await services.tasks.update(stored)

    other_worker = await create_worker(services.workers, ACTOR.account.id, name="other")
    reclaimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, other_worker.id)
    )
    assert len(reclaimed) == 1
    assert reclaimed[0].task.id == stale.id
    assert reclaimed[0].task.attempt == 2
    assert reclaimed[0].task.worker_id == other_worker.id


async def test_sweep_abandons_at_the_retry_cap(services: JobAndTaskServices) -> None:
    """A stale task at the retry cap is abandoned instead of requeued."""
    job_id = await _pending_job(services)
    task = await _claimable_agent_task(services, job_id)
    worker = await create_worker(services.workers, ACTOR.account.id)
    for _ in range(services.task_service._policy.retry_limit):
        claimed = await services.task_service.claim_tasks(
            10, actor=build_worker_actor(ACTOR.account, worker.id)
        )
        assert len(claimed) == 1
        stored = await services.tasks.get(task.id)
        stored.claimed_at = datetime.now(UTC) - timedelta(hours=1)
        await services.tasks.update(stored)

    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    stored = await services.tasks.get(task.id)
    assert stored.status is TaskStatus.ABANDONED


async def test_sweep_cancels_a_cancel_requested_stale_task(
    services: JobAndTaskServices,
) -> None:
    """A stale task with a pending cancel request settles canceled, not requeued."""
    job_id = await _pending_job(services)
    task = await _claimable_agent_task(services, job_id)
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )

    stored = await services.tasks.get(task.id)
    stored.claimed_at = datetime.now(UTC) - timedelta(hours=1)
    stored.cancel_requested_at = datetime.now(UTC)
    await services.tasks.update(stored)

    other_worker = await create_worker(services.workers, ACTOR.account.id, name="other")
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, other_worker.id)
    )
    stored = await services.tasks.get(task.id)
    assert stored.status is TaskStatus.CANCELED


async def test_sweep_requeue_unlinks_the_result_session(
    services: JobAndTaskServices,
) -> None:
    """Requeuing a stale agent task frees its result session slot."""
    job_id = await _pending_job(services)
    task = await _claimable_agent_task(services, job_id)
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )

    session = await create_session(
        services.sessions, ACTOR.account.id, agent_id=uuid.uuid4(), task_id=task.id
    )
    stored = await services.tasks.get(task.id)
    assert isinstance(stored, AgentTask)
    stored.result_session_id = session.id
    stored.claimed_at = datetime.now(UTC) - timedelta(hours=1)
    await services.tasks.update(stored)

    other_worker = await create_worker(services.workers, ACTOR.account.id, name="other")
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, other_worker.id)
    )

    reloaded_task = await services.tasks.get(task.id)
    assert isinstance(reloaded_task, AgentTask)
    assert reloaded_task.result_session_id is None
    reloaded_session = await services.sessions.get(session.id)
    assert reloaded_session.task_id is None


async def test_sweep_stale_tasks_works_outside_the_claim_path() -> None:
    """sweep_stale_tasks abandons a stale task and settles its job, called directly.

    claim_tasks calls the same method, this proves it also works standalone
    so a caller outside the claim path, such as a background sweep loop, can
    reuse it without duplicating the sweep logic.
    """
    services = build_job_and_task_services(policy=TaskPolicy(retry_limit=1))
    job_id = await _pending_job(services)
    task = await _claimable_agent_task(services, job_id)
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )

    stored = await services.tasks.get(task.id)
    stored.claimed_at = datetime.now(UTC) - timedelta(hours=1)
    await services.tasks.update(stored)

    await services.task_service.sweep_stale_tasks(datetime.now(UTC))

    stored = await services.tasks.get(task.id)
    assert stored.status is TaskStatus.ABANDONED
    job = await services.jobs.get(job_id)
    assert job.status.value == "failed"


async def test_heartbeat_stamps_owned_reported_tasks(
    services: JobAndTaskServices,
) -> None:
    """A heartbeat stamps the tasks the worker still owns."""
    job_id = await _pending_job(services)
    await _claimable_agent_task(services, job_id)
    worker = await create_worker(services.workers, ACTOR.account.id)
    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    task_id = claimed[0].task.id

    cancel_ids = await services.task_service.heartbeat_worker(
        worker.id, [task_id], actor=build_worker_actor(ACTOR.account, worker.id)
    )
    assert cancel_ids == []
    stored = await services.tasks.get(task_id)
    assert stored.heartbeat_at is not None


async def test_heartbeat_returns_cancel_requested_missing_and_reassigned(
    services: JobAndTaskServices,
) -> None:
    """cancel_task_ids covers cancel-requested, missing, and reassigned tasks."""
    job_id = await _pending_job(services)
    await _claimable_agent_task(services, job_id)
    await _claimable_agent_task(services, job_id)
    worker = await create_worker(services.workers, ACTOR.account.id)
    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    cancel_requested_id = claimed[0].task.id
    reassigned_id = claimed[1].task.id
    missing_id = uuid.uuid4()

    stored = await services.tasks.get(cancel_requested_id)
    stored.cancel_requested_at = datetime.now(UTC)
    await services.tasks.update(stored)

    other_worker = await create_worker(services.workers, ACTOR.account.id, name="other")
    stored = await services.tasks.get(reassigned_id)
    stored.worker_id = other_worker.id
    await services.tasks.update(stored)

    cancel_ids = await services.task_service.heartbeat_worker(
        worker.id,
        [cancel_requested_id, reassigned_id, missing_id],
        actor=build_worker_actor(ACTOR.account, worker.id),
    )
    assert set(cancel_ids) == {cancel_requested_id, reassigned_id, missing_id}


async def test_heartbeat_reports_terminal_tasks(services: JobAndTaskServices) -> None:
    """A terminal task the worker still reports comes back to stop."""
    job_id = await _pending_job(services)
    task = await _claimable_agent_task(services, job_id)
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    await services.task_service.update_task(
        task.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(ACTOR.account, task.id, 1, worker.id),
    )
    session = await create_session(
        services.sessions, ACTOR.account.id, agent_id=uuid.uuid4(), task_id=task.id
    )
    stored = await services.tasks.get(task.id)
    assert isinstance(stored, AgentTask)
    stored.result_session_id = session.id
    await services.tasks.update(stored)
    session.status = SessionStatus.COMPLETED
    await services.sessions.update(session)
    await services.task_service.update_task(
        task.id,
        TaskUpdate(status=TaskStatus.COMPLETED),
        actor=build_task_actor(ACTOR.account, task.id, 1, worker.id),
    )

    cancel_ids = await services.task_service.heartbeat_worker(
        worker.id, [task.id], actor=build_worker_actor(ACTOR.account, worker.id)
    )
    assert cancel_ids == [task.id]


async def test_update_task_requires_status(services: JobAndTaskServices) -> None:
    """An update without a status is rejected."""
    job_id = await _pending_job(services)
    task = await create_agent_task(services.tasks, job_id)
    with pytest.raises(TaskUpdateRequiresStatus):
        await services.task_service.update_task(
            task.id,
            TaskUpdate(),
            actor=build_task_actor(ACTOR.account, task.id, 0, uuid.uuid4()),
        )


async def test_update_task_attempt_fencing(services: JobAndTaskServices) -> None:
    """A transition fenced by a stale attempt conflicts."""
    job_id = await _pending_job(services)
    await _claimable_agent_task(services, job_id)
    worker = await create_worker(services.workers, ACTOR.account.id)
    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    task_id = claimed[0].task.id

    with pytest.raises(TaskAttemptMismatch):
        await services.task_service.update_task(
            task_id,
            TaskUpdate(status=TaskStatus.RUNNING),
            actor=build_task_actor(ACTOR.account, task_id, 0, worker.id),
        )


async def test_agent_completion_requires_a_completed_result_session(
    services: JobAndTaskServices,
) -> None:
    """An agent task cannot complete while its result session is in progress."""
    job_id = await _pending_job(services)
    task = await _claimable_agent_task(services, job_id)
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    await services.task_service.update_task(
        task.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(ACTOR.account, task.id, 1, worker.id),
    )

    with pytest.raises(TaskResultSessionMissing):
        await services.task_service.update_task(
            task.id,
            TaskUpdate(status=TaskStatus.COMPLETED),
            actor=build_task_actor(ACTOR.account, task.id, 1, worker.id),
        )

    session = await create_session(
        services.sessions, ACTOR.account.id, agent_id=uuid.uuid4(), task_id=task.id
    )
    stored = await services.tasks.get(task.id)
    assert isinstance(stored, AgentTask)
    stored.result_session_id = session.id
    await services.tasks.update(stored)

    with pytest.raises(TaskResultSessionNotCompleted):
        await services.task_service.update_task(
            task.id,
            TaskUpdate(status=TaskStatus.COMPLETED),
            actor=build_task_actor(ACTOR.account, task.id, 1, worker.id),
        )

    session.status = SessionStatus.COMPLETED
    await services.sessions.update(session)
    completed = await services.task_service.update_task(
        task.id,
        TaskUpdate(status=TaskStatus.COMPLETED),
        actor=build_task_actor(ACTOR.account, task.id, 1, worker.id),
    )
    assert completed.status is TaskStatus.COMPLETED


async def test_update_task_rejects_an_oversized_result() -> None:
    """A completion result over the size cap is rejected."""
    services = build_job_and_task_services(policy=TaskPolicy(max_result_bytes=32))
    job_id = await _pending_job(services)
    task = await _claimable_import_task(services, job_id)
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    await services.task_service.update_task(
        task.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(ACTOR.account, task.id, 1, worker.id),
    )

    with pytest.raises(TaskResultTooLarge):
        await services.task_service.update_task(
            task.id,
            TaskUpdate(
                status=TaskStatus.COMPLETED,
                result={"created": 0, "padding": "x" * 100},
            ),
            actor=build_task_actor(ACTOR.account, task.id, 1, worker.id),
        )


async def test_get_task_and_list_tasks_report_effective_status(
    services: JobAndTaskServices,
) -> None:
    """Reads report the status the next sweep would write for a stale task."""
    job_id = await _pending_job(services)
    task = await _claimable_agent_task(services, job_id)
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    stored = await services.tasks.get(task.id)
    stored.claimed_at = datetime.now(UTC) - timedelta(hours=1)
    await services.tasks.update(stored)

    effective = await services.task_service.get_task(task.id, actor=ACTOR)
    assert effective.status is TaskStatus.PENDING
    # The stale read is diagnostic only, it does not write the sweep's outcome.
    assert (await services.tasks.get(task.id)).status is TaskStatus.CLAIMED

    items, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=job_id), actor=ACTOR
    )
    assert items[0].status is TaskStatus.PENDING


async def test_agent_spec_merges_secrets_in_order_with_later_wins(
    services: JobAndTaskServices,
) -> None:
    """Secret env merges in secret_ids order, a later secret overriding a key."""
    agent = await create_agent(services.agents, ACTOR.account.id)
    version = await create_agent_version(
        services.agent_versions,
        agent_id=agent.id,
        owner_id=ACTOR.account.id,
    )
    first_secret = await create_secret(
        services.secrets,
        ACTOR.account.id,
        name="first",
        values={"KEY": SecretStr("one")},
    )
    second_secret = await create_secret(
        services.secrets,
        ACTOR.account.id,
        name="second",
        values={"KEY": SecretStr("two")},
    )
    version.run_spec = RunSpec(
        command="run.sh",
        env={"STATIC": "x"},
        secret_ids=[first_secret.id, second_secret.id],
        timeout_seconds=120,
    )
    await services.agent_versions.update(version)

    job_id = await _pending_job(services)
    task = await create_agent_task(
        services.tasks,
        job_id,
        agent_version_id=version.id,
        env={"KITARU_SESSION_NAME": "s"},
    )
    spec = await services.task_service.get_spec(task.id, actor=ACTOR)
    assert spec.timeout_seconds == 120
    assert spec.env == {"KITARU_SESSION_NAME": "s"}
    assert spec.secret_env == {"KEY": "two"}
    assert spec.run_spec is not None
    assert spec.run_spec.command == "run.sh"
    assert spec.run_spec.env == {"STATIC": "x"}


async def test_evaluation_spec_uses_the_evaluator_timeout_and_no_run(
    services: JobAndTaskServices,
) -> None:
    """An evaluator spec carries the server timeout and no run spec."""
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, PluginKind.EVALUATOR, name="scorer"
    )
    version = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="score"),
        display_version=None,
    )
    blob = await create_blob(services.blobs, ACTOR.account.id, content=b"code")
    version = version.model_copy(
        update={"source": ScriptPluginSource(blob_id=blob.id, entrypoint="score")}
    )
    await services.plugins.update_version(version)

    job_id = await _pending_job(services)
    task = await create_evaluation_task(
        services.tasks, job_id, plugin_version_id=version.id
    )
    spec = await services.task_service.get_spec(task.id, actor=ACTOR)
    assert (
        spec.timeout_seconds == services.task_service._policy.evaluator_timeout_seconds
    )
    assert spec.run_spec is None
    assert spec.details.kind.value == "evaluator"


async def test_import_spec_carries_the_payload_sha256_and_provider(
    services: JobAndTaskServices,
) -> None:
    """An importer spec carries the payload's sha256 and the plugin's provider."""
    plugin = await create_plugin(
        services.plugins,
        ACTOR.account.id,
        PluginKind.IMPORTER,
        name="csv-importer",
        provider="acme",
    )
    version = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="run"),
        display_version=None,
    )
    code_blob = await create_blob(services.blobs, ACTOR.account.id, content=b"code")
    version = version.model_copy(
        update={"source": ScriptPluginSource(blob_id=code_blob.id, entrypoint="run")}
    )
    await services.plugins.update_version(version)
    payload = await create_blob(
        services.blobs, ACTOR.account.id, content=b"payload-data"
    )

    job_id = await _pending_job(services)
    task = await create_import_task(
        services.tasks,
        job_id,
        plugin_version_id=version.id,
        payload_blob_id=payload.id,
    )
    spec = await services.task_service.get_spec(task.id, actor=ACTOR)
    assert (
        spec.timeout_seconds == services.task_service._policy.importer_timeout_seconds
    )
    assert isinstance(spec.details, ImportTaskDetails)
    assert spec.details.provider == "acme"
    assert spec.details.payload.blob_id == payload.id
    assert spec.details.payload.sha256 == payload.sha256


class _RecordingAnalytics(ServerAnalytics):
    """Analytics tracker recording track calls instead of buffering them."""

    def __init__(self) -> None:
        """Initialize the recorder."""
        self.tracked: list[tuple[uuid.UUID, str, dict[str, Any]]] = []

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


def _build_transitions(
    analytics: ServerAnalytics | None,
) -> tuple[TaskTransitions, FakeTaskRepository, FakeJobRepository]:
    """Wire a transitions dispatch directly over fresh fake repositories."""
    tasks = FakeTaskRepository()
    jobs = FakeJobRepository(tasks=tasks)
    transitions = TaskTransitions(
        task_repository=tasks,
        job_repository=jobs,
        dispatcher=EventDispatcher(),
        analytics=analytics,
    )
    return transitions, tasks, jobs


async def _complete_task(transitions: TaskTransitions, task: Task, result: Any) -> Task:
    """Drive a pending task through claim, start, and complete via apply_status."""
    now = datetime.now(UTC)
    claimed = await transitions.apply_status(
        task, partial(Task.claim, worker_id=uuid.uuid4(), now=now)
    )
    started = await transitions.apply_status(claimed, partial(Task.start, now=now))
    return await transitions.apply_status(
        started, partial(Task.complete, result=result, now=now)
    )


async def test_apply_status_importer_terminal_tracks_import_completed() -> None:
    """Track an import_completed event when an importer task turns terminal."""
    analytics = _RecordingAnalytics()
    transitions, tasks, jobs = _build_transitions(analytics)
    job = await create_job(jobs, ACTOR.account.id)
    task = await create_import_task(tasks, job.id)
    await create_agent_task(tasks, job.id)

    completed = await _complete_task(transitions, task, result={"created": 3})

    assert completed.status is TaskStatus.COMPLETED
    assert len(analytics.tracked) == 1
    tracked_user_id, tracked_event, tracked_properties = analytics.tracked[0]
    assert tracked_user_id == ACTOR.account.id
    assert tracked_event == AnalyticsEvent.IMPORT_COMPLETED
    assert tracked_properties["status"] == "completed"
    assert tracked_properties["plugin_version_id"] == task.plugin_version_id
    assert tracked_properties["session_count"] == 3
    assert tracked_properties["duration_seconds"] >= 0.0


async def test_apply_status_evaluator_terminal_tracks_evaluation_completed() -> None:
    """Track an evaluation_completed event when an evaluator task turns terminal."""
    analytics = _RecordingAnalytics()
    transitions, tasks, jobs = _build_transitions(analytics)
    job = await create_job(jobs, ACTOR.account.id)
    task = await create_evaluation_task(tasks, job.id)
    await create_agent_task(tasks, job.id)

    result = [{"name": "quality", "score": 1.0}]
    completed = await _complete_task(transitions, task, result=result)

    assert completed.status is TaskStatus.COMPLETED
    assert len(analytics.tracked) == 1
    tracked_user_id, tracked_event, tracked_properties = analytics.tracked[0]
    assert tracked_user_id == ACTOR.account.id
    assert tracked_event == AnalyticsEvent.EVALUATION_COMPLETED
    assert tracked_properties["status"] == "completed"
    assert tracked_properties["plugin_version_id"] == task.plugin_version_id
    assert "session_count" not in tracked_properties


async def test_apply_status_agent_terminal_tracks_nothing() -> None:
    """Skip tracking when an agent task turns terminal."""
    analytics = _RecordingAnalytics()
    transitions, tasks, jobs = _build_transitions(analytics)
    job = await create_job(jobs, ACTOR.account.id)
    task = await create_agent_task(tasks, job.id)
    await create_agent_task(tasks, job.id)
    now = datetime.now(UTC)
    claimed = await transitions.apply_status(
        task, partial(Task.claim, worker_id=uuid.uuid4(), now=now)
    )
    started = await transitions.apply_status(claimed, partial(Task.start, now=now))
    started.link_result_session(uuid.uuid4())

    completed = await transitions.apply_status(
        started, partial(Task.complete, result=None, now=now)
    )

    assert completed.status is TaskStatus.COMPLETED
    assert analytics.tracked == []


async def test_advance_job_settlement_tracks_job_completed() -> None:
    """Track a job_completed event carrying task_count once every task drains."""
    analytics = _RecordingAnalytics()
    transitions, tasks, jobs = _build_transitions(analytics)
    job = await create_job(jobs, ACTOR.account.id)
    import_task = await create_import_task(tasks, job.id)
    evaluation_task = await create_evaluation_task(tasks, job.id)

    await _complete_task(transitions, import_task, result={"created": 1})
    await _complete_task(
        transitions, evaluation_task, result=[{"name": "quality", "score": 1.0}]
    )

    job_events = [
        entry for entry in analytics.tracked if entry[1] == AnalyticsEvent.JOB_COMPLETED
    ]
    assert len(job_events) == 1
    tracked_user_id, _, tracked_properties = job_events[0]
    assert tracked_user_id == ACTOR.account.id
    assert tracked_properties["status"] == "completed"
    assert tracked_properties["task_count"] == 2
    assert tracked_properties["task_kinds"] == ["evaluator", "importer"]


async def test_apply_status_non_terminal_writes_track_nothing() -> None:
    """Skip tracking when a transition leaves the task non-terminal."""
    analytics = _RecordingAnalytics()
    transitions, tasks, jobs = _build_transitions(analytics)
    job = await create_job(jobs, ACTOR.account.id)
    task = await create_import_task(tasks, job.id)
    now = datetime.now(UTC)

    claimed = await transitions.apply_status(
        task, partial(Task.claim, worker_id=uuid.uuid4(), now=now)
    )
    await transitions.apply_status(claimed, partial(Task.start, now=now))

    assert analytics.tracked == []


async def test_apply_status_with_analytics_none_is_safe() -> None:
    """Drive a task to terminal without an analytics tracker configured."""
    transitions, tasks, jobs = _build_transitions(None)
    job = await create_job(jobs, ACTOR.account.id)
    task = await create_import_task(tasks, job.id)

    completed = await _complete_task(transitions, task, result={"created": 1})

    assert completed.status is TaskStatus.COMPLETED
