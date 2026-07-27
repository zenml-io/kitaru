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
"""Round-trip tests for the jobs SDK resource."""

import uuid
from collections.abc import AsyncGenerator

import pytest
from test_experiments import SCORING_POLICY, create_cohort, create_experiment

from conftest import asgi_api_client, experiment_app
from kitaru.api_models.v1.agent_versions import (
    AgentVersionCreateRequest,
    RunSpec,
)
from kitaru.api_models.v1.agents import AgentCreateRequest
from kitaru.api_models.v1.experiment_runs import ExperimentRunCreateRequest
from kitaru.api_models.v1.jobs import (
    HistoryPolicy,
    JobClaimRequest,
    JobKind,
    JobStatus,
    JobUpdateRequest,
    ReplayCreateRequest,
    StandaloneJobClaimRequest,
    ToolLookupRequest,
    ToolPolicyConfig,
)
from kitaru.api_models.v1.session_nodes import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
)
from kitaru.api_models.v1.sessions import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
    SessionUpdateRequest,
)
from kitaru.api_models.v1.workers import WorkerCreateRequest
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.hashing import tool_call_cache_key
from kitaru.server.domain.ids import uuid7


@pytest.fixture
async def api_client() -> AsyncGenerator[KitaruAPIClient, None]:
    """Provide an API client routed to the app with fake-backed services."""
    async with asgi_api_client(experiment_app()) as client:
        yield client


async def create_session(api_client: KitaruAPIClient) -> tuple[uuid.UUID, uuid.UUID]:
    """Store an agent, a runnable version, and a completed session.

    Args:
        api_client: API client routed to the app.

    Returns:
        Ids of the created session and agent version.
    """
    agent = await api_client.agents.create(AgentCreateRequest(name="support-bot"))
    version = await api_client.agent_versions.create(
        agent.id,
        AgentVersionCreateRequest(
            version="v1",
            run_spec=RunSpec(command="python agent.py", timeout_seconds=600),
        ),
    )
    session = await api_client.sessions.create(
        SessionCreateRequest(agent_id=agent.id, origin=SessionOrigin.RECORDED)
    )
    await api_client.sessions.update(
        session.id, SessionUpdateRequest(status=SessionStatus.COMPLETED)
    )
    return session.id, version.id


async def test_create_get_list_round_trip(api_client: KitaruAPIClient) -> None:
    """Round-trip a standalone job through create, get, and list."""
    session_id, version_id = await create_session(api_client)
    created = await api_client.replays.create(
        ReplayCreateRequest(input_session_id=session_id, scoring_policy=SCORING_POLICY)
    )
    assert created.experiment_run_id is None
    assert created.input_session_id == session_id
    assert created.agent_version_id == version_id
    assert created.status is JobStatus.PENDING
    assert created.tool_policy == ToolPolicyConfig(default=HistoryPolicy())
    assert created.scoring_policy == SCORING_POLICY

    loaded = await api_client.jobs.get(created.id)
    assert loaded == created

    page = await api_client.jobs.list(
        input_session_id=session_id,
        status=JobStatus.PENDING,
        standalone=True,
    )
    assert page.total == 1
    assert page.items[0].id == created.id

    page = await api_client.jobs.list(standalone=False)
    assert page.total == 0


async def test_list_by_run_and_worker(api_client: KitaruAPIClient) -> None:
    """List jobs filtered by experiment run and claiming worker."""
    cohort_id, _ = await create_cohort(api_client)
    experiment = await create_experiment(api_client, cohort_id)
    created = await api_client.experiments.create_run(
        experiment.id, ExperimentRunCreateRequest()
    )
    worker = await api_client.workers.create(WorkerCreateRequest(name="worker-1"))
    claimed = await api_client.jobs.claim(
        JobClaimRequest(worker_id=worker.id, max_jobs=1, experiment_run_id=created.id)
    )
    assert len(claimed.jobs) == 1
    job_id = claimed.jobs[0].job.id
    assert claimed.jobs[0].spec.job_id == job_id

    page = await api_client.jobs.list(experiment_run_id=created.id)
    assert page.total == 1
    assert page.items[0].id == job_id

    page = await api_client.jobs.list(experiment_run_id=created.id, worker_id=worker.id)
    assert page.total == 1
    assert page.items[0].id == job_id

    page = await api_client.jobs.list(worker_id=uuid.uuid4())
    assert page.total == 0


async def test_get_not_found(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 404 as a NotFoundError."""
    with pytest.raises(NotFoundError):
        await api_client.jobs.get(uuid.uuid4())


async def test_runner_round_trip(api_client: KitaruAPIClient) -> None:
    """Round-trip a job through spec, update, and diff."""
    session_id, _ = await create_session(api_client)
    created = await api_client.replays.create(
        ReplayCreateRequest(input_session_id=session_id, scoring_policy=SCORING_POLICY)
    )
    spec = await api_client.jobs.get_spec(created.id)
    assert spec.job_id == created.id
    assert spec.input_session_id == session_id
    assert spec.scorer is None
    assert spec.run is not None
    assert spec.run.command == "python agent.py"
    assert spec.secret_env == {}

    running = await api_client.jobs.update(
        created.id, JobUpdateRequest(status=JobStatus.RUNNING)
    )
    assert running.status is JobStatus.RUNNING

    session = await api_client.sessions.get(session_id)
    result = await api_client.sessions.create(
        SessionCreateRequest(
            agent_id=session.agent_id,
            origin=SessionOrigin.RECORDED,
            job_id=created.id,
        )
    )
    assert result.origin is SessionOrigin.REPLAY
    await api_client.sessions.update(
        result.id, SessionUpdateRequest(status=SessionStatus.COMPLETED)
    )

    scoring = await api_client.jobs.update(
        created.id, JobUpdateRequest(status=JobStatus.SCORING)
    )
    assert scoring.status is JobStatus.SCORING

    worker = await api_client.workers.create(WorkerCreateRequest(name="scorer-1"))
    claimed = await api_client.jobs.claim(
        JobClaimRequest(worker_id=worker.id, max_jobs=5, parent_job_id=created.id)
    )
    assert {claimed_job.job.parent_job_id for claimed_job in claimed.jobs} == {
        created.id
    }
    assert {claimed_job.job.kind for claimed_job in claimed.jobs} == {JobKind.SCORE}
    assert {claimed_job.spec.kind for claimed_job in claimed.jobs} == {JobKind.SCORE}
    scored = await api_client.jobs.list(kind=JobKind.SCORE, input_session_id=result.id)
    assert scored.total == 1
    children = await api_client.jobs.list(kind=JobKind.SCORE)
    for child in children.items:
        await api_client.jobs.update(
            child.id, JobUpdateRequest(status=JobStatus.RUNNING)
        )
        await api_client.jobs.update(
            child.id, JobUpdateRequest(status=JobStatus.COMPLETED, score=0.8)
        )
    completed = await api_client.jobs.get(created.id)
    assert completed.status is JobStatus.COMPLETED
    assert completed.result_session_id == result.id
    assert completed.diff is not None

    diff = await api_client.jobs.get_diff(created.id)
    assert diff.replay_id == created.id
    assert diff.original_session_id == session_id
    assert diff.result_session_id == result.id
    assert diff.node_pairs == []


async def test_worker_lifecycle_round_trip(api_client: KitaruAPIClient) -> None:
    """Round-trip a job through claim, release, retry, and delete."""
    session_id, _ = await create_session(api_client)
    created = await api_client.replays.create(
        ReplayCreateRequest(input_session_id=session_id, scoring_policy=SCORING_POLICY)
    )
    worker = await api_client.workers.create(WorkerCreateRequest(name="worker-1"))
    claimed = await api_client.jobs.claim_standalone(
        created.id, StandaloneJobClaimRequest(worker_id=worker.id)
    )
    assert claimed.status is JobStatus.CLAIMED
    assert claimed.worker_id == worker.id

    released = await api_client.jobs.release(created.id)
    assert released.status is JobStatus.PENDING
    assert released.attempt == 2
    assert released.worker_id is None

    await api_client.jobs.claim_standalone(
        created.id, StandaloneJobClaimRequest(worker_id=worker.id)
    )
    failed = await api_client.jobs.update(
        created.id,
        JobUpdateRequest(status=JobStatus.FAILED, error="agent exited with code 1"),
    )
    assert failed.status is JobStatus.FAILED

    retried = await api_client.jobs.retry(created.id)
    assert retried.status is JobStatus.PENDING
    assert retried.attempt == 3
    assert retried.error is None
    assert retried.result_session_id is None

    await api_client.jobs.delete(created.id)
    with pytest.raises(NotFoundError):
        await api_client.jobs.get(created.id)


async def test_claim_conflict(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 for a claim on a non-pending job."""
    session_id, _ = await create_session(api_client)
    created = await api_client.replays.create(
        ReplayCreateRequest(input_session_id=session_id, scoring_policy=SCORING_POLICY)
    )
    worker = await api_client.workers.create(WorkerCreateRequest(name="worker-1"))
    other = await api_client.workers.create(WorkerCreateRequest(name="worker-2"))
    await api_client.jobs.claim_standalone(
        created.id, StandaloneJobClaimRequest(worker_id=worker.id)
    )
    with pytest.raises(APIError) as exc_info:
        await api_client.jobs.claim_standalone(
            created.id, StandaloneJobClaimRequest(worker_id=other.id)
        )
    assert exc_info.value.status_code == 409


async def test_tool_lookup_round_trip(api_client: KitaruAPIClient) -> None:
    """Round-trip a history tool lookup."""
    agent = await api_client.agents.create(AgentCreateRequest(name="lookup-bot"))
    await api_client.agent_versions.create(
        agent.id,
        AgentVersionCreateRequest(
            version="v1",
            run_spec=RunSpec(command="python agent.py", timeout_seconds=600),
        ),
    )
    session = await api_client.sessions.create(
        SessionCreateRequest(agent_id=agent.id, origin=SessionOrigin.RECORDED)
    )
    inputs = {"city": "Berlin"}
    await api_client.session_nodes.upsert(
        session.id,
        SessionNodeBatchRequest(
            nodes=[
                SessionNodeCreateRequest(
                    id=uuid7(),
                    sequence=0,
                    node_type=NodeType.TOOL_CALL,
                    name="get_weather",
                    status=NodeStatus.COMPLETED,
                    tool_name="get_weather",
                    inputs=inputs,
                    outputs={"temp": 21},
                )
            ]
        ),
    )
    await api_client.sessions.update(
        session.id, SessionUpdateRequest(status=SessionStatus.COMPLETED)
    )
    created = await api_client.replays.create(
        ReplayCreateRequest(input_session_id=session.id, scoring_policy=SCORING_POLICY)
    )
    found = await api_client.jobs.tool_lookup(
        created.id,
        ToolLookupRequest(
            tool_name="get_weather",
            inputs=inputs,
            cache_key=tool_call_cache_key("get_weather", inputs),
        ),
    )
    assert found.found is True
    assert found.result == {"temp": 21}

    miss = await api_client.jobs.tool_lookup(
        created.id,
        ToolLookupRequest(
            tool_name="get_weather",
            inputs={"city": "Paris"},
            cache_key=tool_call_cache_key("get_weather", {"city": "Paris"}),
        ),
    )
    assert miss.found is False
    assert miss.result is None


async def test_update_illegal_transition(api_client: KitaruAPIClient) -> None:
    """Surface HTTP 409 for an illegal runner transition."""
    session_id, _ = await create_session(api_client)
    created = await api_client.replays.create(
        ReplayCreateRequest(input_session_id=session_id, scoring_policy=SCORING_POLICY)
    )
    with pytest.raises(APIError) as exc_info:
        await api_client.jobs.update(
            created.id,
            JobUpdateRequest(status=JobStatus.SCORING),
        )
    assert exc_info.value.status_code == 409
