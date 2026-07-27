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
"""Tests for the job runner against a fake API client."""

import asyncio
import contextlib
import hashlib
import json
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any, cast

import pytest

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
    ScoringPolicy,
    SourceScorerConfig,
    StandaloneJobClaimRequest,
    ToolPolicyConfig,
)
from kitaru.api_models.v1.plugins import PluginFormat
from kitaru.api_models.v1.session_nodes import SessionNodeResponse
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
from kitaru.blob_cache import BlobCache
from kitaru.client.api_client import KitaruAPIClient
from kitaru.runner import (
    IMPORT_TIMEOUT_SECONDS,
    JobProcess,
    JobRunner,
    Runner,
    WorkerHeartbeat,
    import_command,
    inline_dependencies,
    score_command,
)

NOW = datetime.now(UTC)

SCORING_POLICY = ScoringPolicy(
    scorers=[
        SourceScorerConfig(
            name="quality",
            source="test_runner:constant_scorer",
            params={"value": 0.8},
        )
    ],
    pass_threshold=0.5,
)

PLUGIN_CODE = b"def score(session):\n    return 0.5\n"
PLUGIN_SHA256 = hashlib.sha256(PLUGIN_CODE).hexdigest()

IMPORTER_CODE = b"def parse(payload):\n    return []\n"
IMPORTER_SHA256 = hashlib.sha256(IMPORTER_CODE).hexdigest()
TRACE_PAYLOAD = b'{"trace_id": "trace-1"}\n'
TRACE_SHA256 = hashlib.sha256(TRACE_PAYLOAD).hexdigest()

_TERMINAL_STATUSES = frozenset(
    {
        JobStatus.COMPLETED,
        JobStatus.FAILED,
        JobStatus.TIMED_OUT,
        JobStatus.CANCELED,
    }
)


def make_spec(
    job_id: uuid.UUID,
    command: str = "true",
    inputs: Any = None,
    run_env: dict[str, str] | None = None,
    secret_env: dict[str, str] | None = None,
    timeout_seconds: int = 60,
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


def make_score_spec(
    job_id: uuid.UUID,
    session_id: uuid.UUID,
    registered: bool,
    working_dir: str | None = None,
    run_env: dict[str, str] | None = None,
    secret_env: dict[str, str] | None = None,
    timeout_seconds: int = 60,
) -> JobSpecResponse:
    """Build a score job spec of one of the two arms."""
    if registered:
        scorer = JobSpecScorer(
            config=RegistryScorerConfig(name="quality", version=1),
            plugin=JobSpecPlugin(
                format=PluginFormat.INLINE,
                entrypoint="score",
                blob_id=uuid.uuid4(),
                sha256=PLUGIN_SHA256,
            ),
            input_session_id=session_id,
        )
    else:
        scorer = JobSpecScorer(
            config=SourceScorerConfig(
                name="quality", source="test_runner:constant_scorer"
            ),
            plugin=None,
            input_session_id=session_id,
        )
    return make_spec(
        job_id,
        kind=JobKind.SCORE,
        scorer=scorer,
        run=not registered,
        working_dir=working_dir,
        run_env=run_env,
        secret_env=secret_env,
        timeout_seconds=timeout_seconds,
        input_session_id=session_id,
    )


def make_import_spec(
    job_id: uuid.UUID,
    code_sha256: str = IMPORTER_SHA256,
    payload_sha256: str = TRACE_SHA256,
) -> JobSpecResponse:
    """Build an import job spec."""
    return make_spec(
        job_id,
        kind=JobKind.IMPORT,
        importer=JobSpecImporter(
            plugin=JobSpecPlugin(
                format=PluginFormat.INLINE,
                entrypoint="parse",
                blob_id=uuid.uuid4(),
                sha256=code_sha256,
            ),
            payload=JobSpecPayload(blob_id=uuid.uuid4(), sha256=payload_sha256),
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
        passed=None,
        score=None,
        scores=None,
        diff=None,
        stats=None,
        override=None,
        tool_policy=(
            ToolPolicyConfig(default=PassthroughPolicy())
            if kind is JobKind.REPLAY
            else None
        ),
        scoring_policy=SCORING_POLICY if kind is JobKind.REPLAY else None,
        created=NOW,
        updated=NOW,
    )


def make_session(
    session_id: uuid.UUID,
    status: SessionStatus = SessionStatus.COMPLETED,
    scores: dict[str, float] | None = None,
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
        scores=scores or {},
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
            scoring=0,
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

    async def get_spec(self, job_id: uuid.UUID) -> JobSpecResponse:
        """Record the fetch and return the configured spec."""
        self._client.spec_fetches.append(job_id)
        return self._client.specs_by_id[job_id]

    async def get(self, job_id: uuid.UUID) -> JobResponse:
        """Return the configured job."""
        return self._client.jobs_by_id[job_id]

    async def update(self, job_id: uuid.UUID, request: JobUpdateRequest) -> JobResponse:
        """Record the update and apply it to the stored job."""
        self._client.updates.append((job_id, request))
        return self._client.apply_update(job_id, request)

    async def claim(self, request: JobClaimRequest) -> JobClaimResponse:
        """Record the claim and pop the next configured batch with its specs."""
        self._client.claim_requests.append(request)
        batches = self._client.claim_batches
        batch = batches.pop(0) if batches else []
        return JobClaimResponse(
            jobs=[
                ClaimedJobResponse(job=job, spec=self._client.specs_by_id[job.id])
                for job in batch
            ]
        )

    async def claim_standalone(
        self, job_id: uuid.UUID, request: StandaloneJobClaimRequest
    ) -> JobResponse:
        """Record the claim and return the configured job."""
        self._client.standalone_claims.append(request)
        return self._client.apply_update(
            job_id, JobUpdateRequest(status=JobStatus.CLAIMED)
        )


class FakeSessionsResource:
    """Fake sessions resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def get(self, session_id: uuid.UUID) -> SessionResponse:
        """Return the configured session."""
        return self._client.sessions_by_id[session_id]


class FakeSessionNodesResource:
    """Fake session nodes resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def list(
        self, session_id: uuid.UUID, include_payloads: bool = False
    ) -> list[SessionNodeResponse]:
        """Record the request and return no nodes."""
        self._client.node_requests.append((session_id, include_payloads))
        return []


class FakeBlobsResource:
    """Fake blobs resource."""

    def __init__(self, client: "FakeClient") -> None:
        """Initialize the resource."""
        self._client = client

    async def download(self, blob_id: uuid.UUID) -> bytes:
        """Record the download and return the configured content."""
        self._client.blob_downloads.append(blob_id)
        return self._client.blob_contents.get(blob_id, self._client.blob_content)


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
            agent_ids=request.agent_ids,
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
        return WorkerHeartbeatResponse(
            abandon=list(request.job_ids) if self._client.cancel_on_heartbeat else []
        )


class FakeClient:
    """Fake API client implementing the resource methods the runner uses."""

    def __init__(
        self,
        spec: JobSpecResponse,
        job: JobResponse,
        sessions_by_id: dict[uuid.UUID, SessionResponse] | None = None,
        claim_batches: list[list[JobResponse]] | None = None,
        run: ExperimentRunResponse | None = None,
        cancel_on_heartbeat: bool = False,
        blob_content: bytes = PLUGIN_CODE,
        blob_contents: dict[uuid.UUID, bytes] | None = None,
    ) -> None:
        """Initialize the client."""
        self.specs_by_id = {spec.job_id: spec}
        self.jobs_by_id = {job.id: job}
        self.sessions_by_id = sessions_by_id or {}
        self.claim_batches = claim_batches or []
        self.run = run or make_run(ExperimentRunStatus.COMPLETED)
        self.cancel_on_heartbeat = cancel_on_heartbeat
        self.blob_content = blob_content
        self.blob_contents = blob_contents or {}
        self.updates: list[tuple[uuid.UUID, JobUpdateRequest]] = []
        self.node_requests: list[tuple[uuid.UUID, bool]] = []
        self.blob_downloads: list[uuid.UUID] = []
        self.spec_fetches: list[uuid.UUID] = []
        self.claim_requests: list[JobClaimRequest] = []
        self.standalone_claims: list[StandaloneJobClaimRequest] = []
        self.worker_registrations: list[WorkerCreateRequest] = []
        self.worker_id = uuid.uuid4()
        self.heartbeat_count = 0
        self.heartbeat_worker_ids: list[uuid.UUID] = []
        self.heartbeat_job_ids: list[list[uuid.UUID]] = []
        self.jobs = FakeJobsResource(self)
        self.sessions = FakeSessionsResource(self)
        self.session_nodes = FakeSessionNodesResource(self)
        self.blobs = FakeBlobsResource(self)
        self.experiment_runs = FakeExperimentRunsResource(self)
        self.workers = FakeWorkersResource(self)

    def add_job(self, job: JobResponse, spec: JobSpecResponse) -> None:
        """Register another job and its spec."""
        self.jobs_by_id[job.id] = job
        self.specs_by_id[spec.job_id] = spec

    def apply_update(self, job_id: uuid.UUID, request: JobUpdateRequest) -> JobResponse:
        """Apply an update to a stored job, settling its parent like the server."""
        job = self.jobs_by_id[job_id]
        changes: dict[str, Any] = {}
        if request.status is not None:
            changes["status"] = request.status
        if request.score is not None:
            changes["score"] = request.score
        job = job.model_copy(update=changes)
        self.jobs_by_id[job_id] = job
        if job.parent_job_id is not None and job.status in _TERMINAL_STATUSES:
            self._settle_parent(job.parent_job_id)
        return job

    def _settle_parent(self, parent_job_id: uuid.UUID) -> None:
        """Complete a tracked parent job once all its children are terminal."""
        if parent_job_id not in self.jobs_by_id:
            return
        children = [
            child
            for child in self.jobs_by_id.values()
            if child.parent_job_id == parent_job_id
        ]
        if all(child.status in _TERMINAL_STATUSES for child in children):
            parent = self.jobs_by_id[parent_job_id]
            self.jobs_by_id[parent_job_id] = parent.model_copy(
                update={"status": JobStatus.COMPLETED}
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


async def execute_job(
    fake: FakeClient,
    job_id: uuid.UUID,
    heartbeat_interval: float = 60.0,
    spec: JobSpecResponse | None = None,
    **kwargs: Any,
) -> JobResponse:
    """Execute one claimed job with a job runner backed by the fake client."""
    job_runner = JobRunner(api_url="http://server", api_key="key", **kwargs)
    client = cast(KitaruAPIClient, fake)
    heartbeat = WorkerHeartbeat(client, fake.worker_id, heartbeat_interval)
    task = asyncio.create_task(heartbeat.run())
    try:
        return await job_runner.execute(client, job_id, heartbeat, spec=spec)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def make_runner(
    monkeypatch: pytest.MonkeyPatch, fake: FakeClient, **kwargs: Any
) -> Runner:
    """Build a runner backed by the fake client."""
    monkeypatch.setattr("kitaru.runner.KitaruAPIClient", lambda base_url, api_key: fake)
    return Runner(api_url="http://server", api_key="key", **kwargs)


def capture_processes(
    monkeypatch: pytest.MonkeyPatch, returncode: int = 0, tail: str = ""
) -> list[JobProcess]:
    """Record job process invocations instead of running them."""
    captured: list[JobProcess] = []

    async def fake_run(
        self: JobRunner,
        job_process: JobProcess,
        canceled: asyncio.Event | None = None,
    ) -> tuple[int | None, str]:
        captured.append(job_process)
        return returncode, tail

    monkeypatch.setattr(JobRunner, "_run_process", fake_run)
    return captured


async def test_replay_success_hands_over_to_scoring() -> None:
    """Move a verified replay to scoring instead of completing it."""
    job_id = uuid.uuid4()
    result_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_spec(job_id, command="true"),
        job=make_job(job_id, result_session_id=result_id),
        sessions_by_id={result_id: make_session(result_id)},
    )
    final = await execute_job(fake, job_id)

    assert fake.statuses() == [JobStatus.RUNNING, JobStatus.SCORING]
    assert final.status is JobStatus.SCORING
    assert fake.last_update().score is None
    assert fake.node_requests == []


async def test_replay_with_unfinished_session_fails() -> None:
    """Fail a replay whose result session did not complete."""
    job_id = uuid.uuid4()
    result_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_spec(job_id, command="true"),
        job=make_job(job_id, result_session_id=result_id),
        sessions_by_id={result_id: make_session(result_id, SessionStatus.FAILED)},
    )
    await execute_job(fake, job_id)

    failed = fake.last_update()
    assert failed.status is JobStatus.FAILED
    assert failed.error is not None
    assert "not completed" in failed.error


async def test_nonzero_exit_fails_with_log_tail() -> None:
    """Fail a job whose agent process exits non-zero."""
    job_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_spec(job_id, command="echo boom >&2 && exit 3"),
        job=make_job(job_id),
    )
    final = await execute_job(fake, job_id)

    assert fake.statuses() == [JobStatus.RUNNING, JobStatus.FAILED]
    failed = fake.last_update()
    assert failed.error is not None
    assert "Agent process exited with code 3" in failed.error
    assert "boom" in failed.error
    assert final.status is JobStatus.FAILED


async def test_timeout_kills_process() -> None:
    """Kill the agent process and time the job out on expiry."""
    job_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_spec(job_id, command="sleep 30", timeout_seconds=1),
        job=make_job(job_id),
    )
    started = time.monotonic()
    await execute_job(fake, job_id)

    assert time.monotonic() - started < 10
    assert fake.statuses() == [JobStatus.RUNNING, JobStatus.TIMED_OUT]
    error = fake.last_update().error
    assert error is not None
    assert "timed out after 1 seconds" in error


async def test_heartbeat_abandon_kills_process() -> None:
    """Kill the agent process and cancel a job the heartbeat abandons."""
    job_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_spec(job_id, command="sleep 30", timeout_seconds=30),
        job=make_job(job_id),
        cancel_on_heartbeat=True,
    )
    started = time.monotonic()
    await execute_job(fake, job_id, heartbeat_interval=0.05)

    assert time.monotonic() - started < 10
    assert fake.heartbeat_count >= 1
    assert fake.statuses() == [JobStatus.RUNNING, JobStatus.CANCELED]


async def test_missing_result_session_fails() -> None:
    """Fail a job whose agent recorded no result session."""
    job_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_spec(job_id, command="true"),
        job=make_job(job_id, result_session_id=None),
    )
    await execute_job(fake, job_id)

    failed = fake.last_update()
    assert failed.status is JobStatus.FAILED
    assert failed.error is not None
    assert "without recording a result session" in failed.error


async def run_env_dump(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    inputs: Any,
    run_env: dict[str, str] | None = None,
    secret_env: dict[str, str] | None = None,
) -> tuple[uuid.UUID, dict[str, str]]:
    """Execute a job dumping its environment to a file."""
    monkeypatch.delenv("KITARU_JOB_INPUTS", raising=False)
    job_id = uuid.uuid4()
    result_id = uuid.uuid4()
    out_file = tmp_path / "env.txt"
    fake = FakeClient(
        spec=make_spec(
            job_id,
            command='env > "$KITARU_TEST_ENV_FILE"',
            inputs=inputs,
            run_env={**(run_env or {}), "KITARU_TEST_ENV_FILE": str(out_file)},
            secret_env=secret_env,
        ),
        job=make_job(job_id, result_session_id=result_id),
        sessions_by_id={result_id: make_session(result_id)},
    )
    await execute_job(fake, job_id)
    return job_id, read_env(out_file)


def read_env(path: Path) -> dict[str, str]:
    """Parse an environment dump written by env(1)."""
    return dict(
        line.split("=", 1) for line in path.read_text().splitlines() if "=" in line
    )


async def test_env_contract_and_merge_order(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Layer run env over the process env, secrets over both, contract on top."""
    monkeypatch.setenv("KITARU_TEST_OS_VAR", "os")
    job_id, env = await run_env_dump(
        monkeypatch,
        tmp_path,
        inputs={"question": "hi"},
        run_env={"KITARU_TEST_OS_VAR": "run", "KITARU_TEST_SHARED": "run"},
        secret_env={"KITARU_TEST_SHARED": "secret", "KITARU_API_URL": "secret"},
    )
    assert env["KITARU_TEST_OS_VAR"] == "run"
    assert env["KITARU_TEST_SHARED"] == "secret"
    assert env["KITARU_API_URL"] == "http://server"
    assert env["KITARU_API_KEY"] == "key"
    assert env["KITARU_JOB_ID"] == str(job_id)
    assert json.loads(env["KITARU_JOB_INPUTS"]) == {"question": "hi"}


async def test_inherited_plugin_path_is_cleared(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Clear inherited plugin and payload paths before an agent process."""
    monkeypatch.setenv("KITARU_JOB_PLUGIN_PATH", "/inherited")
    monkeypatch.setenv("KITARU_JOB_PAYLOAD_PATH", "/inherited")
    _, env = await run_env_dump(monkeypatch, tmp_path, inputs=None)
    assert "KITARU_JOB_PLUGIN_PATH" not in env
    assert "KITARU_JOB_PAYLOAD_PATH" not in env


async def test_inputs_env_omitted_over_threshold(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Omit KITARU_JOB_INPUTS when the encoded inputs exceed the threshold."""
    _, env = await run_env_dump(monkeypatch, tmp_path, inputs="x" * 40_000)
    assert "KITARU_JOB_INPUTS" not in env
    assert "KITARU_JOB_ID" in env


async def test_session_run_completes_without_scoring(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Complete a session run without scoring and deliver the name env."""
    monkeypatch.delenv("KITARU_JOB_SESSION_NAME", raising=False)
    job_id = uuid.uuid4()
    result_id = uuid.uuid4()
    out_file = tmp_path / "env.txt"
    fake = FakeClient(
        spec=make_spec(
            job_id,
            command='env > "$KITARU_TEST_ENV_FILE"',
            inputs={"question": "hi"},
            run_env={"KITARU_TEST_ENV_FILE": str(out_file)},
            kind=JobKind.SESSION_RUN,
            name="smoke",
        ),
        job=make_job(job_id, result_session_id=result_id, kind=JobKind.SESSION_RUN),
        sessions_by_id={result_id: make_session(result_id)},
    )
    final = await execute_job(fake, job_id)

    assert fake.statuses() == [JobStatus.RUNNING, JobStatus.COMPLETED]
    assert final.status is JobStatus.COMPLETED
    assert fake.node_requests == []
    env = read_env(out_file)
    assert env["KITARU_JOB_SESSION_NAME"] == "smoke"
    assert json.loads(env["KITARU_JOB_INPUTS"]) == {"question": "hi"}


async def test_score_job_registry_arm_runs_the_cached_code(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Materialize the code blob and hand its path to the harness."""
    monkeypatch.delenv("KITARU_JOB_INPUTS", raising=False)
    job_id = uuid.uuid4()
    session_id = uuid.uuid4()
    spec = make_score_spec(job_id, session_id, registered=True)
    fake = FakeClient(
        spec=spec, job=make_job(job_id, kind=JobKind.SCORE, parent_job_id=uuid.uuid4())
    )
    processes = capture_processes(monkeypatch)
    final = await execute_job(fake, job_id, blob_cache=BlobCache(tmp_path))

    assert spec.scorer is not None and spec.scorer.plugin is not None
    assert fake.blob_downloads == [spec.scorer.plugin.blob_id]
    process = processes[0]
    assert process.command == f"{sys.executable} -m kitaru.score"
    assert process.working_dir is None
    assert process.timeout_seconds == 300
    cached = tmp_path / PLUGIN_SHA256
    assert process.env["KITARU_JOB_PLUGIN_PATH"] == str(cached)
    assert cached.read_bytes() == PLUGIN_CODE
    assert process.env["KITARU_JOB_ID"] == str(job_id)
    assert process.env["KITARU_API_URL"] == "http://server"
    assert process.env["KITARU_API_KEY"] == "key"
    assert "KITARU_JOB_INPUTS" not in process.env
    assert fake.statuses() == [JobStatus.RUNNING, JobStatus.COMPLETED]
    assert final.status is JobStatus.COMPLETED


async def test_score_job_registry_arm_reuses_the_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Skip the download when the code is already cached."""
    job_id = uuid.uuid4()
    cache = BlobCache(tmp_path)
    cache.put(PLUGIN_SHA256, PLUGIN_CODE)
    fake = FakeClient(
        spec=make_score_spec(job_id, uuid.uuid4(), registered=True),
        job=make_job(job_id, kind=JobKind.SCORE),
    )
    capture_processes(monkeypatch)
    await execute_job(fake, job_id, blob_cache=cache)

    assert fake.blob_downloads == []


async def test_score_job_registry_arm_uses_uv_for_dependencies(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Run the harness through uv when the code declares dependencies."""
    job_id = uuid.uuid4()
    code = (
        b"# /// script\n"
        b'# dependencies = ["httpx==0.27.0", "orjson"]\n'
        b"# ///\n"
        b"def score(session):\n    return 1.0\n"
    )
    spec = make_score_spec(job_id, uuid.uuid4(), registered=True)
    assert spec.scorer is not None and spec.scorer.plugin is not None
    spec = spec.model_copy(
        update={
            "scorer": spec.scorer.model_copy(
                update={
                    "plugin": spec.scorer.plugin.model_copy(
                        update={"sha256": hashlib.sha256(code).hexdigest()}
                    )
                }
            )
        }
    )
    fake = FakeClient(
        spec=spec, job=make_job(job_id, kind=JobKind.SCORE), blob_content=code
    )
    processes = capture_processes(monkeypatch)
    await execute_job(fake, job_id, blob_cache=BlobCache(tmp_path))

    assert processes[0].command == (
        "uv run --with httpx==0.27.0 --with orjson python -m kitaru.score"
    )


async def test_score_job_source_arm_runs_in_the_agent_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Run the harness in the run environment of the agent version."""
    monkeypatch.setenv("KITARU_JOB_PLUGIN_PATH", "/inherited")
    job_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_score_spec(
            job_id,
            uuid.uuid4(),
            registered=False,
            working_dir=str(tmp_path),
            run_env={"AGENT_VAR": "run"},
            secret_env={"AGENT_SECRET": "secret"},
            timeout_seconds=45,
        ),
        job=make_job(job_id, kind=JobKind.SCORE),
    )
    processes = capture_processes(monkeypatch)
    final = await execute_job(fake, job_id)

    process = processes[0]
    assert process.command == f"{sys.executable} -m kitaru.score"
    assert process.working_dir == str(tmp_path)
    assert process.timeout_seconds == 45
    assert process.env["AGENT_VAR"] == "run"
    assert process.env["AGENT_SECRET"] == "secret"
    assert process.env["KITARU_JOB_ID"] == str(job_id)
    assert "KITARU_JOB_PLUGIN_PATH" not in process.env
    assert fake.blob_downloads == []
    assert final.status is JobStatus.COMPLETED


async def test_score_job_nonzero_exit_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail a score job whose harness exits non-zero."""
    job_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_score_spec(job_id, uuid.uuid4(), registered=False),
        job=make_job(job_id, kind=JobKind.SCORE),
    )
    capture_processes(monkeypatch, returncode=1, tail="stderr tail:\nboom")
    await execute_job(fake, job_id)

    failed = fake.last_update()
    assert failed.status is JobStatus.FAILED
    assert failed.error is not None
    assert "Scorer process exited with code 1" in failed.error
    assert "boom" in failed.error


async def test_score_job_download_failure_fails_the_job(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Fail a score job whose code does not match its hash."""
    job_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_score_spec(job_id, uuid.uuid4(), registered=True),
        job=make_job(job_id, kind=JobKind.SCORE),
        blob_content=b"tampered",
    )
    capture_processes(monkeypatch)
    await execute_job(fake, job_id, blob_cache=BlobCache(tmp_path))

    failed = fake.last_update()
    assert failed.status is JobStatus.FAILED
    assert failed.error is not None
    assert "Failed to prepare the scorer process" in failed.error


async def test_import_job_runs_the_cached_code_and_payload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Materialize code and payload and hand both paths to the harness."""
    job_id = uuid.uuid4()
    spec = make_import_spec(job_id)
    assert spec.importer is not None
    fake = FakeClient(
        spec=spec,
        job=make_job(job_id, kind=JobKind.IMPORT),
        blob_contents={
            spec.importer.plugin.blob_id: IMPORTER_CODE,
            spec.importer.payload.blob_id: TRACE_PAYLOAD,
        },
    )
    processes = capture_processes(monkeypatch)
    final = await execute_job(
        fake,
        job_id,
        blob_cache=BlobCache(tmp_path / "blobs"),
        payload_cache=BlobCache(tmp_path / "payloads"),
    )

    assert fake.blob_downloads == [
        spec.importer.plugin.blob_id,
        spec.importer.payload.blob_id,
    ]
    process = processes[0]
    assert process.command == f"{sys.executable} -m kitaru.imports"
    assert process.working_dir is None
    assert process.timeout_seconds == IMPORT_TIMEOUT_SECONDS
    code = tmp_path / "blobs" / IMPORTER_SHA256
    payload = tmp_path / "payloads" / TRACE_SHA256
    assert process.env["KITARU_JOB_PLUGIN_PATH"] == str(code)
    assert process.env["KITARU_JOB_PAYLOAD_PATH"] == str(payload)
    assert code.read_bytes() == IMPORTER_CODE
    assert payload.read_bytes() == TRACE_PAYLOAD
    assert process.env["KITARU_JOB_ID"] == str(job_id)
    assert process.env["KITARU_API_URL"] == "http://server"
    assert process.env["KITARU_API_KEY"] == "key"
    assert fake.statuses() == [JobStatus.RUNNING, JobStatus.COMPLETED]
    assert final.status is JobStatus.COMPLETED


async def test_import_job_reuses_the_caches(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Skip both downloads when the code and payload are already cached."""
    job_id = uuid.uuid4()
    blob_cache = BlobCache(tmp_path / "blobs")
    payload_cache = BlobCache(tmp_path / "payloads")
    blob_cache.put(IMPORTER_SHA256, IMPORTER_CODE)
    payload_cache.put(TRACE_SHA256, TRACE_PAYLOAD)
    fake = FakeClient(
        spec=make_import_spec(job_id), job=make_job(job_id, kind=JobKind.IMPORT)
    )
    capture_processes(monkeypatch)
    await execute_job(fake, job_id, blob_cache=blob_cache, payload_cache=payload_cache)

    assert fake.blob_downloads == []


async def test_import_job_uses_uv_for_dependencies(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Run the harness through uv when the importer declares dependencies."""
    job_id = uuid.uuid4()
    code = (
        b"# /// script\n"
        b'# dependencies = ["orjson"]\n'
        b"# ///\n"
        b"def parse(payload):\n    return []\n"
    )
    spec = make_import_spec(job_id, code_sha256=hashlib.sha256(code).hexdigest())
    assert spec.importer is not None
    fake = FakeClient(
        spec=spec,
        job=make_job(job_id, kind=JobKind.IMPORT),
        blob_contents={
            spec.importer.plugin.blob_id: code,
            spec.importer.payload.blob_id: TRACE_PAYLOAD,
        },
    )
    processes = capture_processes(monkeypatch)
    await execute_job(
        fake,
        job_id,
        blob_cache=BlobCache(tmp_path / "blobs"),
        payload_cache=BlobCache(tmp_path / "payloads"),
    )

    assert processes[0].command == import_command(["orjson"])


async def test_import_job_nonzero_exit_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Fail an import job whose harness exits non-zero."""
    job_id = uuid.uuid4()
    spec = make_import_spec(job_id)
    assert spec.importer is not None
    fake = FakeClient(
        spec=spec,
        job=make_job(job_id, kind=JobKind.IMPORT),
        blob_contents={
            spec.importer.plugin.blob_id: IMPORTER_CODE,
            spec.importer.payload.blob_id: TRACE_PAYLOAD,
        },
    )
    capture_processes(monkeypatch, returncode=1, tail="stderr tail:\nboom")
    await execute_job(
        fake,
        job_id,
        blob_cache=BlobCache(tmp_path / "blobs"),
        payload_cache=BlobCache(tmp_path / "payloads"),
    )

    failed = fake.last_update()
    assert failed.status is JobStatus.FAILED
    assert failed.error is not None
    assert "Importer process exited with code 1" in failed.error
    assert "boom" in failed.error


async def test_import_job_download_failure_fails_the_job(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Fail an import job whose payload does not match its hash."""
    job_id = uuid.uuid4()
    spec = make_import_spec(job_id)
    assert spec.importer is not None
    fake = FakeClient(
        spec=spec,
        job=make_job(job_id, kind=JobKind.IMPORT),
        blob_contents={spec.importer.plugin.blob_id: IMPORTER_CODE},
        blob_content=b"tampered",
    )
    capture_processes(monkeypatch)
    await execute_job(
        fake,
        job_id,
        blob_cache=BlobCache(tmp_path / "blobs"),
        payload_cache=BlobCache(tmp_path / "payloads"),
    )

    failed = fake.last_update()
    assert failed.status is JobStatus.FAILED
    assert failed.error is not None
    assert "Failed to prepare the importer process" in failed.error


def test_inline_dependencies_reads_the_script_block(tmp_path: Path) -> None:
    """Read the dependencies of a PEP 723 script block."""
    path = tmp_path / "scorer"
    path.write_text(
        "# /// script\n"
        '# requires-python = ">=3.11"\n'
        "# dependencies = [\n"
        '#   "httpx",\n'
        '#   "orjson>=3",\n'
        "# ]\n"
        "# ///\n"
        "def score(session):\n    return 1.0\n"
    )
    assert inline_dependencies(path) == ["httpx", "orjson>=3"]


def test_inline_dependencies_without_a_block(tmp_path: Path) -> None:
    """Report no dependencies for code without inline metadata."""
    path = tmp_path / "scorer"
    path.write_text("def score(session):\n    return 1.0\n")
    assert inline_dependencies(path) == []


def test_inline_dependencies_ignores_other_block_types(tmp_path: Path) -> None:
    """Ignore inline metadata blocks that are not script blocks."""
    path = tmp_path / "scorer"
    path.write_text('# /// other\n# dependencies = ["httpx"]\n# ///\n')
    assert inline_dependencies(path) == []


def test_inline_dependencies_rejects_multiple_blocks(tmp_path: Path) -> None:
    """Reject code declaring more than one script block."""
    path = tmp_path / "scorer"
    path.write_text(
        "# /// script\n"
        "# dependencies = []\n"
        "# ///\n"
        "\n"
        "# /// script\n"
        "# dependencies = []\n"
        "# ///\n"
    )
    with pytest.raises(ValueError, match="multiple inline script blocks"):
        inline_dependencies(path)


def test_score_command_without_dependencies() -> None:
    """Run the harness with the worker interpreter when nothing is declared."""
    assert score_command([]) == f"{sys.executable} -m kitaru.score"


def test_score_command_with_dependencies() -> None:
    """Quote the declared dependencies into the uv command."""
    assert score_command(["httpx>=0.27", "orjson"]) == (
        "uv run --with 'httpx>=0.27' --with orjson python -m kitaru.score"
    )


async def test_run_job_registers_and_claims_standalone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Register the worker, claim the standalone job, and execute it."""
    job_id = uuid.uuid4()
    result_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_spec(job_id, command="true", kind=JobKind.SESSION_RUN),
        job=make_job(job_id, result_session_id=result_id, kind=JobKind.SESSION_RUN),
        sessions_by_id={result_id: make_session(result_id)},
    )
    runner = make_runner(monkeypatch, fake, worker_name="worker-1")
    final = await runner.run_job(job_id)

    assert fake.worker_registrations[0].name == "worker-1"
    assert fake.worker_registrations[0].agent_ids == []
    assert len(fake.standalone_claims) == 1
    assert fake.standalone_claims[0].worker_id == fake.worker_id
    assert fake.claim_requests == []
    assert final.status is JobStatus.COMPLETED


async def test_run_job_drives_score_children_to_completion(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Claim and execute the score jobs of a replay until it is terminal."""
    job_id = uuid.uuid4()
    result_id = uuid.uuid4()
    child_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_spec(job_id, command="true"),
        job=make_job(job_id, result_session_id=result_id),
        sessions_by_id={result_id: make_session(result_id)},
    )
    child = make_job(
        child_id, kind=JobKind.SCORE, parent_job_id=job_id, status=JobStatus.CLAIMED
    )
    fake.add_job(child, make_score_spec(child_id, result_id, registered=True))
    fake.claim_batches = [[child]]
    capture_processes(monkeypatch)
    runner = make_runner(monkeypatch, fake, poll_interval=0.01)
    final = await runner.run_job(job_id)

    assert fake.statuses(job_id) == [JobStatus.RUNNING, JobStatus.SCORING]
    assert fake.statuses(child_id) == [JobStatus.RUNNING, JobStatus.COMPLETED]
    assert [request.parent_job_id for request in fake.claim_requests] == [
        job_id,
        job_id,
    ]
    assert final.status is JobStatus.COMPLETED


async def test_default_worker_name_sanitizes_hostname(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replace hostname characters the server rejects in worker names."""
    monkeypatch.setattr("kitaru.runner.socket.gethostname", lambda: "host.local")
    job_id = uuid.uuid4()
    result_id = uuid.uuid4()
    fake = FakeClient(
        spec=make_spec(job_id, command="true", kind=JobKind.SESSION_RUN),
        job=make_job(job_id, result_session_id=result_id, kind=JobKind.SESSION_RUN),
        sessions_by_id={result_id: make_session(result_id)},
    )
    runner = make_runner(monkeypatch, fake)
    await runner.run_job(job_id)

    name = fake.worker_registrations[0].name
    assert name.startswith("host-local-")
    assert "." not in name


async def test_run_experiment_run_claims_until_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Claim and execute jobs until the claim drains and the run ends."""
    job_id = uuid.uuid4()
    result_id = uuid.uuid4()
    run_id = uuid.uuid4()
    job = make_job(job_id, result_session_id=result_id)
    fake = FakeClient(
        spec=make_spec(job_id, command="true"),
        job=job,
        sessions_by_id={result_id: make_session(result_id)},
        claim_batches=[[job]],
        run=make_run(ExperimentRunStatus.COMPLETED),
    )
    runner = make_runner(
        monkeypatch, fake, worker_name="worker-1", concurrency=2, claim_batch_size=5
    )
    final = await runner.run_experiment_run(run_id)

    assert final.status is ExperimentRunStatus.COMPLETED
    assert len(fake.claim_requests) == 2
    assert fake.worker_registrations[0].name == "worker-1"
    assert fake.worker_registrations[0].agent_ids == []
    assert fake.claim_requests[0].worker_id == fake.worker_id
    assert fake.claim_requests[0].max_jobs == 5
    assert fake.claim_requests[0].experiment_run_id == run_id
    assert fake.claim_requests[0].parent_job_id is None
    assert fake.statuses() == [JobStatus.RUNNING, JobStatus.SCORING]


async def test_run_worker_claims_until_stopped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Register with agent ids and claim pool jobs until the stop event."""
    agent_id = uuid.uuid4()
    job_id = uuid.uuid4()
    result_id = uuid.uuid4()
    job = make_job(job_id, result_session_id=result_id)
    fake = FakeClient(
        spec=make_spec(job_id, command="true"),
        job=job,
        sessions_by_id={result_id: make_session(result_id)},
        claim_batches=[[job]],
    )
    runner = make_runner(monkeypatch, fake, worker_name="worker-1", poll_interval=0.01)
    stop = asyncio.Event()
    async with asyncio.timeout(10):
        task = asyncio.create_task(runner.run_worker(agent_ids=[agent_id], stop=stop))
        while len(fake.updates) < 2:
            await asyncio.sleep(0.01)
        stop.set()
        await task

    assert fake.worker_registrations[0].name == "worker-1"
    assert fake.worker_registrations[0].agent_ids == [agent_id]
    assert fake.claim_requests[0].agent_ids == [agent_id]
    assert fake.claim_requests[0].experiment_run_id is None
    assert fake.statuses() == [JobStatus.RUNNING, JobStatus.SCORING]


async def test_run_worker_without_stop_polls_until_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Poll empty claims until the task is cancelled when no stop event is set."""
    job_id = uuid.uuid4()
    fake = FakeClient(spec=make_spec(job_id), job=make_job(job_id))
    runner = make_runner(monkeypatch, fake, poll_interval=0.01)
    async with asyncio.timeout(10):
        task = asyncio.create_task(runner.run_worker())
        while len(fake.claim_requests) < 2:
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert fake.worker_registrations[0].agent_ids == []
    assert fake.claim_requests[0].agent_ids is None


async def test_claim_loop_executes_the_shipped_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Execute a claimed job from the claim spec without fetching it again."""
    job_id = uuid.uuid4()
    result_id = uuid.uuid4()
    job = make_job(job_id, result_session_id=result_id)
    fake = FakeClient(
        spec=make_spec(job_id, command="true"),
        job=job,
        sessions_by_id={result_id: make_session(result_id)},
        claim_batches=[[job]],
    )
    runner = make_runner(monkeypatch, fake, poll_interval=0.01)
    stop = asyncio.Event()
    async with asyncio.timeout(10):
        task = asyncio.create_task(runner.run_worker(stop=stop))
        while len(fake.updates) < 2:
            await asyncio.sleep(0.01)
        stop.set()
        await task

    assert fake.spec_fetches == []
    assert fake.statuses() == [JobStatus.RUNNING, JobStatus.SCORING]


async def test_worker_heartbeat_reports_every_in_flight_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Send one heartbeat carrying the ids of all jobs the worker runs."""
    first_id = uuid.uuid4()
    second_id = uuid.uuid4()
    first = make_job(first_id, kind=JobKind.SESSION_RUN)
    second = make_job(second_id, kind=JobKind.SESSION_RUN)
    fake = FakeClient(
        spec=make_spec(first_id, command="sleep 5", kind=JobKind.SESSION_RUN),
        job=first,
        claim_batches=[[first, second]],
    )
    fake.add_job(
        second, make_spec(second_id, command="sleep 5", kind=JobKind.SESSION_RUN)
    )
    runner = make_runner(
        monkeypatch,
        fake,
        concurrency=2,
        poll_interval=0.01,
        heartbeat_interval=0.05,
    )
    stop = asyncio.Event()
    async with asyncio.timeout(10):
        task = asyncio.create_task(runner.run_worker(stop=stop))
        while not fake.heartbeat_job_ids:
            await asyncio.sleep(0.01)
        reported = fake.heartbeat_job_ids[0]
        stop.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert sorted(reported) == sorted([first_id, second_id])
    assert fake.heartbeat_worker_ids[0] == fake.worker_id
