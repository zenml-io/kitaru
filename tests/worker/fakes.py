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
"""Shared fakes and factories for worker tests."""

import uuid
from collections import deque
from datetime import UTC, datetime
from typing import Any, cast

from kitaru.api_models.v1.job import JobKind, JobResponse, JobStatus
from kitaru.api_models.v1.session import SessionOrigin, SessionResponse, SessionStatus
from kitaru.api_models.v1.task import (
    AgentTaskDetails,
    EvaluationTaskDetails,
    ImportTaskDetails,
    PackagePluginSpec,
    PayloadSpec,
    ScriptPluginSpec,
    TaskClaimResponse,
    TaskKind,
    TaskOnFailure,
    TaskResponse,
    TaskRunSpec,
    TaskSpecResponse,
    TaskStatus,
    TaskWithSpec,
)
from kitaru.api_models.v1.worker import (
    WorkerHeartbeatResponse,
    WorkerRegistrationResponse,
    WorkerResponse,
    WorkerRuntime,
    WorkerScope,
)
from kitaru.client.api_client import KitaruAPIClient

OWNER_ID = uuid.uuid4()


def _now() -> datetime:
    return datetime.now(UTC)


def make_task(
    kind: TaskKind = TaskKind.AGENT,
    status: TaskStatus = TaskStatus.CLAIMED,
    attempt: int = 1,
    job_id: uuid.UUID | None = None,
    result_session_id: uuid.UUID | None = None,
    **overrides: Any,
) -> TaskResponse:
    """Build a task response with sane defaults for the fields tests vary.

    Args:
        kind: Task kind.
        status: Task status.
        attempt: Current attempt number.
        job_id: Owning job, a fresh id when omitted.
        result_session_id: Session an agent task produced.
        overrides: Additional fields to override on the response.

    Returns:
        Task response.
    """
    fields = {
        "id": uuid.uuid4(),
        "job_id": job_id or uuid.uuid4(),
        "kind": kind,
        "status": status,
        "on_failure": TaskOnFailure.ABORT,
        "attempt": attempt,
        "labels": {},
        "result_session_id": result_session_id,
        "result": None,
        "created": _now(),
        "updated": _now(),
    }
    fields.update(overrides)
    return TaskResponse.model_validate(fields)


def make_agent_spec(
    task_id: uuid.UUID,
    command: str = "true",
    timeout_seconds: int = 30,
    inputs: Any = None,
    run_env: dict[str, str] | None = None,
    extra_env: dict[str, str] | None = None,
    secret_env: dict[str, str] | None = None,
    working_dir: str | None = None,
    replay_id: uuid.UUID | None = None,
) -> TaskSpecResponse:
    """Build an agent task spec.

    Args:
        task_id: Task the spec belongs to.
        command: Shell command to run.
        timeout_seconds: Process timeout.
        inputs: Inputs passed to the agent's command.
        run_env: Process environment from the run spec.
        extra_env: Creator-set environment extras.
        secret_env: Secrets merged into the process environment.
        working_dir: Working directory.
        replay_id: Replay the task runs for.

    Returns:
        Agent task spec.
    """
    return TaskSpecResponse(
        task_id=task_id,
        kind=TaskKind.AGENT,
        timeout_seconds=timeout_seconds,
        run=TaskRunSpec(command=command, working_dir=working_dir, env=run_env or {}),
        env=extra_env or {},
        secret_env=secret_env or {},
        details=AgentTaskDetails(inputs=inputs, replay_id=replay_id),
    )


def make_evaluator_spec(
    task_id: uuid.UUID,
    plugin: ScriptPluginSpec | PackagePluginSpec,
    timeout_seconds: int = 30,
    input_session_id: uuid.UUID | None = None,
    extra_env: dict[str, str] | None = None,
    secret_env: dict[str, str] | None = None,
) -> TaskSpecResponse:
    """Build an evaluator task spec.

    Args:
        task_id: Task the spec belongs to.
        plugin: Evaluator plugin to load.
        timeout_seconds: Process timeout.
        input_session_id: Session being scored.
        extra_env: Creator-set environment extras.
        secret_env: Secrets merged into the process environment.

    Returns:
        Evaluator task spec.
    """
    return TaskSpecResponse(
        task_id=task_id,
        kind=TaskKind.EVALUATOR,
        timeout_seconds=timeout_seconds,
        run=None,
        env=extra_env or {},
        secret_env=secret_env or {},
        details=EvaluationTaskDetails(
            evaluator_name="quality",
            params={},
            plugin=plugin,
            input_session_id=input_session_id or uuid.uuid4(),
        ),
    )


def make_importer_spec(
    task_id: uuid.UUID,
    plugin: ScriptPluginSpec | PackagePluginSpec,
    payload: PayloadSpec,
    timeout_seconds: int = 30,
    agent_id: uuid.UUID | None = None,
    extra_env: dict[str, str] | None = None,
    secret_env: dict[str, str] | None = None,
) -> TaskSpecResponse:
    """Build an importer task spec.

    Args:
        task_id: Task the spec belongs to.
        plugin: Importer plugin to load.
        payload: Payload to parse.
        timeout_seconds: Process timeout.
        agent_id: Agent imported sessions are created under.
        extra_env: Creator-set environment extras.
        secret_env: Secrets merged into the process environment.

    Returns:
        Importer task spec.
    """
    return TaskSpecResponse(
        task_id=task_id,
        kind=TaskKind.IMPORTER,
        timeout_seconds=timeout_seconds,
        run=None,
        env=extra_env or {},
        secret_env=secret_env or {},
        details=ImportTaskDetails(
            plugin=plugin,
            payload=payload,
            agent_id=agent_id or uuid.uuid4(),
            params={},
        ),
    )


def make_worker_response(**overrides: Any) -> WorkerResponse:
    """Build a worker response with sane defaults.

    Args:
        overrides: Additional fields to override on the response.

    Returns:
        Worker response.
    """
    fields = {
        "id": uuid.uuid4(),
        "owner_id": OWNER_ID,
        "name": "worker-1",
        "scope": WorkerScope(),
        "runtime": WorkerRuntime(platform="bare"),
        "concurrency": 1,
        "last_seen_at": _now(),
        "live": True,
        "metadata": {},
        "created": _now(),
        "updated": _now(),
    }
    fields.update(overrides)
    return WorkerResponse.model_validate(fields)


def make_worker_registration_response(
    worker: WorkerResponse | None = None, token: str = "worker-token", **overrides: Any
) -> WorkerRegistrationResponse:
    """Build a worker registration response with sane defaults.

    Args:
        worker: Registered worker, a fresh default worker response when omitted.
        token: Bearer token scoped to the worker.
        overrides: Additional fields to override on the response.

    Returns:
        Worker registration response.
    """
    fields = {
        "worker": worker or make_worker_response(),
        "token": token,
        "token_expires_at": _now(),
    }
    fields.update(overrides)
    return WorkerRegistrationResponse.model_validate(fields)


def make_job_response(
    kind: JobKind = JobKind.SESSION_RUN,
    status: JobStatus = JobStatus.RUNNING,
    **overrides: Any,
) -> JobResponse:
    """Build a job response with sane defaults.

    Args:
        kind: Job kind.
        status: Job status.
        overrides: Additional fields to override on the response.

    Returns:
        Job response.
    """
    fields = {
        "id": uuid.uuid4(),
        "owner_id": OWNER_ID,
        "kind": kind,
        "status": status,
        "created": _now(),
        "updated": _now(),
    }
    fields.update(overrides)
    return JobResponse.model_validate(fields)


def make_session_response(
    status: SessionStatus = SessionStatus.COMPLETED, **overrides: Any
) -> SessionResponse:
    """Build a session response with sane defaults.

    Args:
        status: Session status.
        overrides: Additional fields to override on the response.

    Returns:
        Session response.
    """
    fields = {
        "id": uuid.uuid4(),
        "owner_id": OWNER_ID,
        "agent_id": uuid.uuid4(),
        "number": 1,
        "origin": SessionOrigin.RECORDED,
        "status": status,
        "inputs": None,
        "outputs": None,
        "metadata": {},
        "llm_call_count": 0,
        "tool_call_count": 0,
        "created": _now(),
        "updated": _now(),
    }
    fields.update(overrides)
    return SessionResponse.model_validate(fields)


def make_claimed(
    task: TaskResponse, spec: TaskSpecResponse, token: str = "task-token"
) -> TaskWithSpec:
    """Pair a task and its spec as a claim response entry.

    Args:
        task: Task.
        spec: Task spec.
        token: Bearer token scoped to this task and attempt.

    Returns:
        Task with spec.
    """
    return TaskWithSpec(task=task, spec=spec, token=token)


class FakeWorkersResource:
    """Scriptable fake of the workers SDK resource."""

    def __init__(self) -> None:
        """Initialize the resource with an empty call log."""
        self.created: list[Any] = []
        self.create_response = make_worker_registration_response()
        self.heartbeats: list[tuple[uuid.UUID, Any]] = []
        self.heartbeat_responses: deque[Any] = deque()

    async def create(self, request: Any) -> WorkerRegistrationResponse:
        """Record the request and return the scripted worker."""
        self.created.append(request)
        return self.create_response

    async def heartbeat(self, worker_id: uuid.UUID, request: Any) -> Any:
        """Record the request and return or raise the next scripted result."""
        self.heartbeats.append((worker_id, request))
        if self.heartbeat_responses:
            result = self.heartbeat_responses.popleft()
            if isinstance(result, BaseException):
                raise result
            return result
        return WorkerHeartbeatResponse(cancel_task_ids=[])


class FakeTasksResource:
    """Scriptable fake of the tasks SDK resource."""

    def __init__(self) -> None:
        """Initialize the resource with empty call logs and response queues."""
        self.claim_calls: list[Any] = []
        self.claim_responses: deque[Any] = deque()
        self.update_calls: list[tuple[uuid.UUID, Any]] = []
        self.update_responses: deque[Any] = deque()
        self.get_calls: list[uuid.UUID] = []
        self.get_responses: deque[Any] = deque()

    async def claim(self, request: Any) -> TaskClaimResponse:
        """Record the request and return the next scripted claim result."""
        self.claim_calls.append(request)
        if not self.claim_responses:
            return TaskClaimResponse(tasks=[])
        result = self.claim_responses.popleft()
        if isinstance(result, BaseException):
            raise result
        return result

    async def update(self, task_id: uuid.UUID, request: Any) -> TaskResponse:
        """Record the request and return the next scripted update result."""
        self.update_calls.append((task_id, request))
        if not self.update_responses:
            raise AssertionError(f"No scripted update response for task {task_id}")
        result = self.update_responses.popleft()
        if isinstance(result, BaseException):
            raise result
        return result

    async def get(self, task_id: uuid.UUID) -> TaskResponse:
        """Record the call and return the next scripted task."""
        self.get_calls.append(task_id)
        if not self.get_responses:
            raise AssertionError(f"No scripted get response for task {task_id}")
        result = self.get_responses.popleft()
        if isinstance(result, BaseException):
            raise result
        return result


class FakeJobsResource:
    """Scriptable fake of the jobs SDK resource."""

    def __init__(self) -> None:
        """Initialize the resource with empty call logs and response queues."""
        self.get_calls: list[uuid.UUID] = []
        self.get_responses: deque[Any] = deque()

    async def get(self, job_id: uuid.UUID) -> JobResponse:
        """Record the call and return the next scripted job."""
        self.get_calls.append(job_id)
        if not self.get_responses:
            raise AssertionError(f"No scripted job response for {job_id}")
        result = self.get_responses.popleft()
        if isinstance(result, BaseException):
            raise result
        return result


class FakeSessionsResource:
    """Scriptable fake of the sessions SDK resource."""

    def __init__(self) -> None:
        """Initialize the resource with an empty response map."""
        self.get_calls: list[uuid.UUID] = []
        self.responses: dict[uuid.UUID, Any] = {}

    async def get(self, session_id: uuid.UUID) -> SessionResponse:
        """Record the call and return the mapped session."""
        self.get_calls.append(session_id)
        result = self.responses[session_id]
        if isinstance(result, BaseException):
            raise result
        return result


class FakeBlobsResource:
    """Scriptable fake of the blobs SDK resource."""

    def __init__(self) -> None:
        """Initialize the resource with an empty content map."""
        self.download_calls: list[uuid.UUID] = []
        self.content: dict[uuid.UUID, bytes] = {}

    async def download(self, blob_id: uuid.UUID) -> bytes:
        """Record the call and return the mapped content."""
        self.download_calls.append(blob_id)
        return self.content[blob_id]


class FakeKitaruAPIClient:
    """Hand-rolled fake standing in for KitaruAPIClient in worker tests."""

    def __init__(self) -> None:
        """Initialize every resource fake."""
        self.base_url = "https://api.example.com"
        self.workers = FakeWorkersResource()
        self.tasks = FakeTasksResource()
        self.jobs = FakeJobsResource()
        self.sessions = FakeSessionsResource()
        self.blobs = FakeBlobsResource()
        self.closed = False

    async def __aenter__(self) -> "FakeKitaruAPIClient":
        """Enter the context manager."""
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Exit the context manager, marking the client closed."""
        self.closed = True

    def with_token(self, token: str) -> "FakeKitaruAPIClient":
        """Return this fake, since it has no separate transport to view."""
        return self

    def with_auth(self, auth: object) -> "FakeKitaruAPIClient":
        """Return this fake, since it has no separate transport to view."""
        return self


def as_client(fake: FakeKitaruAPIClient) -> KitaruAPIClient:
    """Type a fake as the real client for constructor signatures."""
    return cast(KitaruAPIClient, fake)
