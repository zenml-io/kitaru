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
"""Fake API client and model builders shared by worker package tests.

Named ``fakes`` rather than ``conftest`` so static analysis does not
conflate it with the top-level ``tests/conftest.py``, whose fixtures serve
the unrelated server-side fakes.
"""

import uuid
from datetime import UTC, datetime
from types import TracebackType
from typing import Any

from kitaru.api_models.v1.agent_versions import ExecutionTarget
from kitaru.api_models.v1.experiment_runs import (
    ExperimentRunProgress,
    ExperimentRunResponse,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.jobs import (
    ClaimedJobResponse,
    JobClaimRequest,
    JobClaimResponse,
    JobKind,
    JobResponse,
    JobSpecImporter,
    JobSpecPayload,
    JobSpecPlugin,
    JobSpecResponse,
    JobSpecRun,
    JobSpecScorer,
    JobStatus,
    JobUpdateRequest,
    PassthroughPolicy,
    RegistryScorerConfig,
    SourceScorerConfig,
    ToolPolicyConfig,
)
from kitaru.api_models.v1.plugins import PluginFormat
from kitaru.api_models.v1.sessions import (
    SessionOrigin,
    SessionProvider,
    SessionResponse,
    SessionStatus,
)
from kitaru.api_models.v1.workers import (
    WorkerCreateRequest,
    WorkerHeartbeatRequest,
    WorkerHeartbeatResponse,
    WorkerResponse,
)

NOW = datetime.now(UTC)


def make_spec(
    job_id: uuid.UUID,
    command: str = "true",
    inputs: Any = None,
    run_env: dict[str, str] | None = None,
    secret_env: dict[str, str] | None = None,
    timeout_seconds: int | None = 60,
    working_dir: str | None = None,
    input_session_id: uuid.UUID | None = None,
    kind: JobKind = JobKind.REPLAY,
    name: str | None = None,
    scorer: JobSpecScorer | None = None,
    importer: JobSpecImporter | None = None,
    run: bool = True,
) -> JobSpecResponse:
    """Build a job spec."""
    return JobSpecResponse(
        job_id=job_id,
        kind=kind,
        inputs=inputs,
        override=None,
        tool_policy=(
            ToolPolicyConfig(default=PassthroughPolicy())
            if kind is JobKind.REPLAY
            else None
        ),
        scorer=scorer,
        importer=importer,
        run=(
            JobSpecRun(
                command=command,
                working_dir=working_dir,
                env=run_env or {},
                timeout_seconds=timeout_seconds,
            )
            if run
            else None
        ),
        secret_env=secret_env or {},
        input_session_id=(
            None
            if kind in (JobKind.SESSION_RUN, JobKind.IMPORT)
            else input_session_id or uuid.uuid4()
        ),
        name=name,
    )


def make_plugin(sha256: str, entrypoint: str = "score") -> JobSpecPlugin:
    """Build a job spec plugin reference."""
    return JobSpecPlugin(
        format=PluginFormat.INLINE,
        entrypoint=entrypoint,
        blob_id=uuid.uuid4(),
        sha256=sha256,
    )


def make_payload(sha256: str) -> JobSpecPayload:
    """Build a job spec payload reference."""
    return JobSpecPayload(blob_id=uuid.uuid4(), sha256=sha256)


def make_score_spec(
    job_id: uuid.UUID,
    session_id: uuid.UUID,
    plugin: JobSpecPlugin | None,
    working_dir: str | None = None,
    run_env: dict[str, str] | None = None,
    secret_env: dict[str, str] | None = None,
    timeout_seconds: int | None = 60,
) -> JobSpecResponse:
    """Build a score job spec, registered when a plugin is given."""
    config = (
        RegistryScorerConfig(name="quality", version=1)
        if plugin is not None
        else SourceScorerConfig(name="quality", source="test_module:constant_scorer")
    )
    scorer = JobSpecScorer(config=config, plugin=plugin, input_session_id=session_id)
    return make_spec(
        job_id,
        kind=JobKind.SCORE,
        scorer=scorer,
        run=plugin is None,
        working_dir=working_dir,
        run_env=run_env,
        secret_env=secret_env,
        timeout_seconds=timeout_seconds,
        input_session_id=session_id,
    )


def make_import_spec(
    job_id: uuid.UUID,
    plugin: JobSpecPlugin,
    payload: JobSpecPayload,
) -> JobSpecResponse:
    """Build an import job spec."""
    return make_spec(
        job_id,
        kind=JobKind.IMPORT,
        importer=JobSpecImporter(
            plugin=plugin,
            payload=payload,
            provider=SessionProvider.OTLP,
            agent_id=uuid.uuid4(),
            params={},
        ),
        run=False,
    )


def make_job(
    job_id: uuid.UUID,
    status: JobStatus = JobStatus.RUNNING,
    result_session_id: uuid.UUID | None = None,
    kind: JobKind = JobKind.REPLAY,
    parent_job_id: uuid.UUID | None = None,
) -> JobResponse:
    """Build a job."""
    return JobResponse(
        id=job_id,
        kind=kind,
        experiment_run_id=None,
        agent_version_id=uuid.uuid4(),
        agent_id=None,
        parent_job_id=parent_job_id,
        input_session_id=uuid.uuid4() if kind is not JobKind.SESSION_RUN else None,
        result_session_id=result_session_id,
        scorer=None,
        plugin_version_id=None,
        payload_blob_id=None,
        status=status,
        attempt=1,
        worker_id=None,
        execution_target=ExecutionTarget.POOL,
        executor_handle=None,
        inputs=None,
        name=None,
        claimed_at=None,
        heartbeat_at=None,
        started_at=None,
        ended_at=None,
        error=None,
        result=None,
        created=NOW,
        updated=NOW,
    )


def make_session(
    session_id: uuid.UUID, status: SessionStatus = SessionStatus.COMPLETED
) -> SessionResponse:
    """Build a session."""
    return SessionResponse(
        id=session_id,
        owner_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        agent_version_id=None,
        origin=SessionOrigin.REPLAY,
        status=status,
        name=None,
        inputs=None,
        outputs=None,
        expected=None,
        error=None,
        started_at=None,
        ended_at=None,
        external_id=None,
        metadata={},
        provider=None,
        framework=None,
        adapter_version=None,
        log_uri=None,
        scores={},
        cost=None,
        tokens=None,
        llm_call_count=0,
        tool_call_count=0,
        created=NOW,
        updated=NOW,
    )


def make_run(status: ExperimentRunStatus) -> ExperimentRunResponse:
    """Build an experiment run."""
    return ExperimentRunResponse(
        id=uuid.uuid4(),
        owner_id=uuid.uuid4(),
        experiment_id=uuid.uuid4(),
        number=1,
        status=status,
        agent_version_id=uuid.uuid4(),
        score_baselines=False,
        execution_target=ExecutionTarget.POOL,
        executor_handle=None,
        started_at=None,
        ended_at=None,
        summary=None,
        error=None,
        progress=ExperimentRunProgress(
            pending=0,
            claimed=0,
            running=0,
            completed=1,
            failed=0,
            timed_out=0,
            canceled=0,
            total=1,
        ),
        created=NOW,
        updated=NOW,
    )


class FakeJobsResource:
    """Fake jobs resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def get(self, job_id: uuid.UUID) -> JobResponse:
        """Return the configured job."""
        return self._client.jobs_by_id[job_id]

    async def update(self, job_id: uuid.UUID, request: JobUpdateRequest) -> JobResponse:
        """Record the update and apply it to the stored job.

        The configured error, when set, only rejects the completion
        transition, matching the server's 409 on a rejected success.
        """
        self._client.updates.append((job_id, request))
        if (
            self._client.update_error is not None
            and request.status is JobStatus.COMPLETED
        ):
            raise self._client.update_error
        job = self._client.jobs_by_id[job_id]
        changes: dict[str, Any] = {}
        if request.status is not None:
            changes["status"] = request.status
        if "error" in request.model_fields_set:
            changes["error"] = request.error
        if "result" in request.model_fields_set:
            changes["result"] = request.result
        job = job.model_copy(update=changes)
        self._client.jobs_by_id[job_id] = job
        return job

    async def claim(self, request: JobClaimRequest) -> JobClaimResponse:
        """Record the claim, raise a queued error, or pop the next batch."""
        self._client.claim_requests.append(request)
        if self._client.claim_errors:
            outcome = self._client.claim_errors.pop(0)
            if outcome is not None:
                raise outcome
        batches = self._client.claim_batches
        batch = batches.pop(0) if batches else []
        return JobClaimResponse(
            jobs=[
                ClaimedJobResponse(job=job, spec=self._client.specs_by_id[job.id])
                for job in batch
            ]
        )


class FakeSessionsResource:
    """Fake sessions resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def get(self, session_id: uuid.UUID) -> SessionResponse:
        """Return the configured session."""
        return self._client.sessions_by_id[session_id]


class FakeBlobsResource:
    """Fake blobs resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def download(self, blob_id: uuid.UUID) -> bytes:
        """Record the download and return the configured content."""
        self._client.blob_downloads.append(blob_id)
        return self._client.blob_contents[blob_id]


class FakeExperimentRunsResource:
    """Fake experiment runs resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def get(self, run_id: uuid.UUID) -> ExperimentRunResponse:
        """Return the configured run."""
        return self._client.run


class FakeWorkersResource:
    """Fake workers resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def create(self, request: WorkerCreateRequest) -> WorkerResponse:
        """Record the registration and return the configured worker."""
        self._client.worker_registrations.append(request)
        return WorkerResponse(
            id=self._client.worker_id,
            owner_id=uuid.uuid4(),
            name=request.name,
            scope=request.scope,
            last_seen_at=NOW,
            live=True,
            metadata=request.metadata,
            created=NOW,
            updated=NOW,
        )

    async def heartbeat(
        self, worker_id: uuid.UUID, request: WorkerHeartbeatRequest
    ) -> WorkerHeartbeatResponse:
        """Count the heartbeat and report the configured abandonment."""
        self._client.heartbeat_count += 1
        self._client.heartbeat_worker_ids.append(worker_id)
        self._client.heartbeat_job_ids.append(list(request.job_ids))
        if self._client.heartbeat_error is not None:
            raise self._client.heartbeat_error
        return WorkerHeartbeatResponse(
            abandon=list(request.job_ids) if self._client.cancel_on_heartbeat else []
        )


class FakeClient:
    """Fake API client implementing the resource methods the worker uses."""

    def __init__(
        self,
        jobs: list[JobResponse] | None = None,
        specs: list[JobSpecResponse] | None = None,
        sessions_by_id: dict[uuid.UUID, SessionResponse] | None = None,
        claim_batches: list[list[JobResponse]] | None = None,
        run: ExperimentRunResponse | None = None,
        cancel_on_heartbeat: bool = False,
        blob_contents: dict[uuid.UUID, bytes] | None = None,
    ) -> None:
        """Initialize the client."""
        self.jobs_by_id = {job.id: job for job in jobs or []}
        self.specs_by_id = {spec.job_id: spec for spec in specs or []}
        self.sessions_by_id = sessions_by_id or {}
        self.claim_batches = claim_batches or []
        self.run = run or make_run(ExperimentRunStatus.COMPLETED)
        self.cancel_on_heartbeat = cancel_on_heartbeat
        self.blob_contents = blob_contents or {}
        self.updates: list[tuple[uuid.UUID, JobUpdateRequest]] = []
        self.blob_downloads: list[uuid.UUID] = []
        self.claim_requests: list[JobClaimRequest] = []
        self.worker_registrations: list[WorkerCreateRequest] = []
        self.worker_id = uuid.uuid4()
        self.heartbeat_count = 0
        self.heartbeat_worker_ids: list[uuid.UUID] = []
        self.heartbeat_job_ids: list[list[uuid.UUID]] = []
        self.update_error: Exception | None = None
        self.claim_errors: list[Exception | None] = []
        self.heartbeat_error: Exception | None = None
        self.jobs = FakeJobsResource(self)
        self.sessions = FakeSessionsResource(self)
        self.blobs = FakeBlobsResource(self)
        self.experiment_runs = FakeExperimentRunsResource(self)
        self.workers = FakeWorkersResource(self)

    def add_job(self, job: JobResponse, spec: JobSpecResponse) -> None:
        """Register another job and its spec."""
        self.jobs_by_id[job.id] = job
        self.specs_by_id[spec.job_id] = spec

    def claimed(self, job_id: uuid.UUID) -> ClaimedJobResponse:
        """Build the claimed job response of a registered job."""
        return ClaimedJobResponse(
            job=self.jobs_by_id[job_id], spec=self.specs_by_id[job_id]
        )

    def statuses(self, job_id: uuid.UUID | None = None) -> list[JobStatus | None]:
        """Return the statuses of the recorded updates."""
        return [
            request.status
            for updated_id, request in self.updates
            if job_id is None or updated_id == job_id
        ]

    def last_update(self) -> JobUpdateRequest:
        """Return the last recorded update."""
        return self.updates[-1][1]

    async def __aenter__(self) -> "FakeClient":
        """Enter the context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Exit the context manager."""
