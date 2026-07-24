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
"""Client-side job runner."""

import asyncio
import contextlib
import json
import logging
import os
import signal
import socket
import tempfile
import uuid
from typing import Any

from kitaru.api_models.v1.experiment_runs import (
    ExperimentRunResponse,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.jobs import (
    JobClaimRequest,
    JobKind,
    JobResponse,
    JobSpecResponse,
    JobStatus,
    JobUpdateRequest,
    ReplayOverride,
    StandaloneJobClaimRequest,
)
from kitaru.api_models.v1.sessions import (
    SessionResponse,
    SessionScoresRequest,
    SessionStatus,
)
from kitaru.api_models.v1.workers import WorkerCreateRequest
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError
from kitaru.scoring import (
    ScoringError,
    SessionView,
    evaluate_scoring_policy,
    run_scorer,
)

logger = logging.getLogger(__name__)

DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10.0
LOG_TAIL_MAX_BYTES = 8192
# TODO: Serve this threshold from the server via the job spec.
MAX_INPUTS_ENV_BYTES = 32768
RUN_POLL_INTERVAL_SECONDS = 2.0

_TERMINAL_RUN_STATUSES = frozenset(
    {
        ExperimentRunStatus.COMPLETED,
        ExperimentRunStatus.FAILED,
        ExperimentRunStatus.CANCELED,
    }
)

# Contract variables the runner controls, cleared from the inherited
# environment before each agent process.
_CONTRACT_ENV_VARS = (
    "KITARU_JOB_ID",
    "KITARU_INPUTS",
    "KITARU_OVERRIDE",
    "KITARU_SESSION_NAME",
    "KITARU_SESSION_ID_FILE",
)


class RunnerError(RuntimeError):
    """Raised when a session run does not produce a recorded session."""


class _TailBuffer:
    """Bounded tail of a byte stream."""

    def __init__(self, max_bytes: int = LOG_TAIL_MAX_BYTES) -> None:
        """Initialize the buffer.

        Args:
            max_bytes: Maximum number of bytes to keep.
        """
        self._max_bytes = max_bytes
        self._data = b""

    def write(self, data: bytes) -> None:
        """Append data, dropping the oldest bytes beyond the maximum.

        Args:
            data: Bytes to append.
        """
        self._data = (self._data + data)[-self._max_bytes :]

    def text(self) -> str:
        """Return the buffered tail as text.

        Returns:
            Decoded tail.
        """
        return self._data.decode("utf-8", errors="replace").strip()


async def _drain_stream(stream: asyncio.StreamReader, tail: _TailBuffer) -> None:
    """Read a stream to EOF into a tail buffer.

    Args:
        stream: Stream to read.
        tail: Buffer receiving the data.
    """
    while chunk := await stream.read(4096):
        tail.write(chunk)


def _kill_process_group(process: asyncio.subprocess.Process) -> None:
    """Kill the process group of a running agent process.

    Args:
        process: Agent process started in its own session.
    """
    if process.returncode is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGKILL)


def _read_session_id(path: str) -> uuid.UUID | None:
    """Read the session id the adapter wrote to the handoff file.

    Args:
        path: Path of the session id file.

    Returns:
        Session id, ``None`` when the file is missing or empty.
    """
    try:
        with open(path, encoding="utf-8") as file:
            content = file.read().strip()
    except OSError:
        return None
    if not content:
        return None
    return uuid.UUID(content)


def _log_tail(stdout: _TailBuffer, stderr: _TailBuffer) -> str:
    """Format the captured output tails for an error message.

    Args:
        stdout: Captured stdout tail.
        stderr: Captured stderr tail.

    Returns:
        Formatted tail, empty when nothing was captured.
    """
    parts = []
    if stdout.text():
        parts.append(f"stdout tail:\n{stdout.text()}")
    if stderr.text():
        parts.append(f"stderr tail:\n{stderr.text()}")
    return "\n".join(parts)


def _with_tail(error: str, tail: str) -> str:
    """Append a log tail to an error message.

    Args:
        error: Error message.
        tail: Captured log tail.

    Returns:
        Error message with the tail, unchanged when the tail is empty.
    """
    if not tail:
        return error
    return f"{error}\n{tail}"


def _encode_inputs(inputs: Any) -> tuple[str, bool]:
    """JSON-encode inputs and report whether they fit the env threshold.

    Args:
        inputs: Inputs to encode.

    Returns:
        Encoded inputs and whether they fit ``MAX_INPUTS_ENV_BYTES``.
    """
    encoded = json.dumps(inputs)
    return encoded, len(encoded.encode("utf-8")) <= MAX_INPUTS_ENV_BYTES


class Runner:
    """Job runner."""

    def __init__(
        self,
        api_url: str,
        api_key: str,
        worker_id: str | None = None,
        concurrency: int = 1,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        claim_batch_size: int | None = None,
    ) -> None:
        """Initialize the runner.

        Args:
            api_url: Server base URL.
            api_key: API key sent as a bearer token.
            worker_id: Id of this worker, hostname and pid when omitted.
            concurrency: Maximum number of jobs executed at once.
            heartbeat_interval: Seconds between heartbeats.
            claim_batch_size: Maximum jobs claimed per request, the
                concurrency when omitted.
        """
        self._api_url = api_url
        self._api_key = api_key
        self._worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}"
        self._concurrency = concurrency
        self._heartbeat_interval = heartbeat_interval
        self._claim_batch_size = claim_batch_size or concurrency

    async def run_experiment_run(self, run_id: uuid.UUID) -> ExperimentRunResponse:
        """Execute the jobs of an experiment run until it is terminal.

        Args:
            run_id: Id of the experiment run.

        Raises:
            APIError: A claim or experiment run read failed.

        Returns:
            Terminal experiment run.
        """
        async with KitaruAPIClient(
            base_url=self._api_url, api_key=self._api_key
        ) as client:
            worker_id = await self._register_worker(client)
            semaphore = asyncio.Semaphore(self._concurrency)
            while True:
                claim = await client.jobs.claim(
                    JobClaimRequest(
                        worker_id=worker_id,
                        max_jobs=self._claim_batch_size,
                        experiment_run_id=run_id,
                    ),
                )
                if claim.jobs:
                    await asyncio.gather(
                        *(
                            self._execute_claimed(client, semaphore, job.id)
                            for job in claim.jobs
                        )
                    )
                    continue
                run = await client.experiment_runs.get(run_id)
                if run.status in _TERMINAL_RUN_STATUSES:
                    return run
                await asyncio.sleep(RUN_POLL_INTERVAL_SECONDS)

    async def run_job(self, job_id: uuid.UUID) -> JobResponse:
        """Execute a standalone job.

        Args:
            job_id: Id of the job.

        Raises:
            APIError: The job does not exist, the claim was rejected,
                its spec does not resolve, or a status update was
                rejected.

        Returns:
            Terminal job.
        """
        async with KitaruAPIClient(
            base_url=self._api_url, api_key=self._api_key
        ) as client:
            worker_id = await self._register_worker(client)
            await client.jobs.claim_standalone(
                job_id, StandaloneJobClaimRequest(worker_id=worker_id)
            )
            return await self._execute_job(client, job_id)

    async def run_session(
        self,
        agent_version_id: uuid.UUID,
        inputs: Any = None,
        override: ReplayOverride | None = None,
    ) -> SessionResponse:
        """Execute an agent version once, recording a fresh session.

        Args:
            agent_version_id: Id of the agent version to execute.
            inputs: Session inputs, the agent's default inputs when omitted.
            override: Execution override, a set prompt replaces the inputs.

        Raises:
            APIError: The agent version, a secret, or the recorded session
                could not be read.
            RunnerError: The agent version has no run spec, the inputs
                exceed the environment threshold, the process timed out, or
                it exited without recording a session.

        Returns:
            Recorded session.
        """
        async with KitaruAPIClient(
            base_url=self._api_url, api_key=self._api_key
        ) as client:
            version = await client.agent_versions.get(agent_version_id)
            if version.run_spec is None:
                raise RunnerError(f"Agent version {agent_version_id} has no run spec")
            secrets = await asyncio.gather(
                *(
                    client.secrets.get(secret_id, include_values=True)
                    for secret_id in version.run_spec.secret_ids
                )
            )
            secret_env: dict[str, str] = {}
            for secret in secrets:
                secret_env.update(
                    {
                        name: value.get_secret_value()
                        for name, value in secret.values.items()
                    }
                )
            env = self._agent_env(version.run_spec.env, secret_env)
            if override is not None:
                if override.prompt is not None:
                    inputs = override.prompt
                # The prompt is delivered through the inputs, the adapter
                # only applies the remaining override fields.
                encoded_override = override.model_dump_json(
                    exclude_none=True, exclude={"prompt"}
                )
                if encoded_override != "{}":
                    env["KITARU_OVERRIDE"] = encoded_override
            if inputs is not None:
                encoded_inputs, fits = _encode_inputs(inputs)
                if not fits:
                    raise RunnerError(
                        f"Session inputs exceed {MAX_INPUTS_ENV_BYTES} bytes"
                    )
                env["KITARU_INPUTS"] = encoded_inputs
            with tempfile.TemporaryDirectory(prefix="kitaru-run-") as tmp_dir:
                session_id_path = os.path.join(tmp_dir, "session_id")
                env["KITARU_SESSION_ID_FILE"] = session_id_path
                returncode, tail = await self._run_agent_process(
                    version.run_spec.command,
                    version.run_spec.working_dir,
                    env,
                    version.run_spec.timeout_seconds,
                )
                session_id = _read_session_id(session_id_path)
            if returncode is None:
                error = (
                    "Session run timed out after "
                    f"{version.run_spec.timeout_seconds} seconds."
                )
                if session_id is not None:
                    error = f"{error} Recorded session: {session_id}."
                raise RunnerError(_with_tail(error, tail))
            if session_id is None:
                error = (
                    f"Agent process exited with code {returncode} "
                    "without recording a session."
                )
                raise RunnerError(_with_tail(error, tail))
            return await client.sessions.get(session_id)

    async def _register_worker(self, client: KitaruAPIClient) -> uuid.UUID:
        """Register this runner as a worker, upserting by name.

        Args:
            client: API client.

        Raises:
            APIError: The registration failed.

        Returns:
            Id of the registered worker.
        """
        worker = await client.workers.create(WorkerCreateRequest(name=self._worker_id))
        return worker.id

    async def _execute_claimed(
        self,
        client: KitaruAPIClient,
        semaphore: asyncio.Semaphore,
        job_id: uuid.UUID,
    ) -> None:
        """Execute a claimed job within the concurrency bound.

        Args:
            client: API client.
            semaphore: Concurrency bound.
            job_id: Id of the job.
        """
        async with semaphore:
            try:
                await self._execute_job(client, job_id)
            except Exception:
                logger.exception("Job %s failed", job_id)

    async def _execute_job(
        self, client: KitaruAPIClient, job_id: uuid.UUID
    ) -> JobResponse:
        """Execute one job from spec fetch to its terminal status.

        Args:
            client: API client.
            job_id: Id of the job.

        Raises:
            APIError: The spec does not resolve or a status update was
                rejected.

        Returns:
            Terminal job.
        """
        try:
            spec = await client.jobs.get_spec(job_id)
        except APIError as exc:
            with contextlib.suppress(APIError):
                await self._fail(
                    client, job_id, f"Failed to resolve the job spec: {exc}"
                )
            raise
        canceled = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(client, job_id, canceled)
        )
        try:
            await client.jobs.update(job_id, JobUpdateRequest(status=JobStatus.RUNNING))
            returncode, tail = await self._run_agent_process(
                spec.run.command,
                spec.run.working_dir,
                self._build_env(job_id, spec),
                spec.run.timeout_seconds,
                canceled,
            )
            if returncode is not None:
                if returncode == 0:
                    if spec.kind is JobKind.SESSION_RUN:
                        return await self._complete_session_run(client, job_id)
                    return await self._score_and_complete(client, job_id, spec)
                error = f"Agent process exited with code {returncode}."
                return await self._fail(client, job_id, _with_tail(error, tail))
            if canceled.is_set():
                return await client.jobs.update(
                    job_id, JobUpdateRequest(status=JobStatus.CANCELED)
                )
            error = _with_tail(
                f"Job timed out after {spec.run.timeout_seconds} seconds.", tail
            )
            return await client.jobs.update(
                job_id,
                JobUpdateRequest(status=JobStatus.TIMED_OUT, error=error),
            )
        finally:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task

    async def _run_agent_process(
        self,
        command: str,
        working_dir: str | None,
        env: dict[str, str],
        timeout_seconds: int,
        canceled: asyncio.Event | None = None,
    ) -> tuple[int | None, str]:
        """Run the agent process until exit, timeout, or cancellation.

        Args:
            command: Bash command starting the agent.
            working_dir: Working directory for the command.
            env: Environment variables for the agent process.
            timeout_seconds: Wall clock limit.
            canceled: Event whose set kills the process.

        Returns:
            Exit code and captured log tail, the exit code ``None`` when
            the process was killed on timeout or cancellation.
        """
        stdout_tail = _TailBuffer()
        stderr_tail = _TailBuffer()
        process = await asyncio.create_subprocess_exec(
            "sh",
            "-c",
            command,
            cwd=working_dir,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        drains = [
            asyncio.create_task(_drain_stream(stream, tail))
            for stream, tail in (
                (process.stdout, stdout_tail),
                (process.stderr, stderr_tail),
            )
            if stream is not None
        ]
        canceled = canceled or asyncio.Event()
        try:
            exit_task = asyncio.create_task(process.wait())
            cancel_task = asyncio.create_task(canceled.wait())
            try:
                done, _ = await asyncio.wait(
                    {exit_task, cancel_task},
                    timeout=timeout_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                cancel_task.cancel()
            if exit_task in done:
                returncode = exit_task.result()
                await asyncio.gather(*drains)
                return returncode, _log_tail(stdout_tail, stderr_tail)
            _kill_process_group(process)
            await exit_task
            await asyncio.gather(*drains)
            return None, _log_tail(stdout_tail, stderr_tail)
        finally:
            _kill_process_group(process)
            await process.wait()
            for drain in drains:
                drain.cancel()
            await asyncio.gather(*drains, return_exceptions=True)

    async def _complete_session_run(
        self, client: KitaruAPIClient, job_id: uuid.UUID
    ) -> JobResponse:
        """Complete a session run job without scoring.

        Args:
            client: API client.
            job_id: Id of the job.

        Raises:
            APIError: A read or status update failed.

        Returns:
            Terminal job.
        """
        job = await client.jobs.get(job_id)
        if job.result_session_id is None:
            return await self._fail(
                client,
                job_id,
                "Agent process exited successfully without recording a result session.",
            )
        result_session = await client.sessions.get(job.result_session_id)
        if result_session.status is not SessionStatus.COMPLETED:
            return await self._fail(
                client,
                job_id,
                f"Result session {result_session.id} is {result_session.status}, "
                "not completed.",
            )
        return await client.jobs.update(
            job_id, JobUpdateRequest(status=JobStatus.COMPLETED)
        )

    async def _score_and_complete(
        self,
        client: KitaruAPIClient,
        job_id: uuid.UUID,
        spec: JobSpecResponse,
    ) -> JobResponse:
        """Score the result session and complete the job.

        Args:
            client: API client.
            job_id: Id of the job.
            spec: Job spec.

        Raises:
            APIError: A read or status update failed.

        Returns:
            Terminal job.
        """
        job = await client.jobs.get(job_id)
        if job.result_session_id is None:
            return await self._fail(
                client,
                job_id,
                "Agent process exited successfully without recording a result session.",
            )
        result_session = await client.sessions.get(job.result_session_id)
        if result_session.status is not SessionStatus.COMPLETED:
            return await self._fail(
                client,
                job_id,
                f"Result session {result_session.id} is {result_session.status}, "
                "not completed.",
            )
        nodes = await client.session_nodes.list(
            job.result_session_id, include_payloads=True
        )
        view = SessionView(session=result_session, nodes=nodes)
        assert spec.scoring_policy is not None
        try:
            result = evaluate_scoring_policy(spec.scoring_policy, view)
            if spec.score_baselines:
                await self._score_baselines(client, spec)
        except ScoringError as exc:
            return await self._fail(client, job_id, str(exc))
        return await client.jobs.update(
            job_id,
            JobUpdateRequest(
                status=JobStatus.COMPLETED,
                passed=result.passed,
                score=result.score,
                scores=result.scores,
            ),
        )

    async def _score_baselines(
        self, client: KitaruAPIClient, spec: JobSpecResponse
    ) -> None:
        """Score the original session for scorers missing from its scores map.

        Args:
            client: API client.
            spec: Job spec.

        Raises:
            ScoringError: A scorer failed to load, raised, or returned an
                invalid score.
        """
        assert spec.original_session_id is not None
        assert spec.scoring_policy is not None
        original = await client.sessions.get(spec.original_session_id)
        missing = [
            config
            for config in spec.scoring_policy.scorers
            if config.name not in original.scores
        ]
        if not missing:
            return
        nodes = await client.session_nodes.list(original.id, include_payloads=True)
        view = SessionView(session=original, nodes=nodes)
        scores = {config.name: run_scorer(config, view) for config in missing}
        await client.sessions.merge_scores(
            original.id, SessionScoresRequest(scores=scores)
        )

    async def _heartbeat_loop(
        self,
        client: KitaruAPIClient,
        job_id: uuid.UUID,
        canceled: asyncio.Event,
    ) -> None:
        """Send heartbeats until the job is canceled server-side.

        Args:
            client: API client.
            job_id: Id of the job.
            canceled: Event set when the server reports cancellation.
        """
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            try:
                response = await client.jobs.heartbeat(job_id)
            except APIError as exc:
                logger.warning("Heartbeat for job %s failed: %s", job_id, exc)
                continue
            if response.canceled:
                canceled.set()
                return

    async def _fail(
        self, client: KitaruAPIClient, job_id: uuid.UUID, error: str
    ) -> JobResponse:
        """Fail the job with an error message.

        Args:
            client: API client.
            job_id: Id of the job.
            error: Error message.

        Raises:
            APIError: The status update was rejected.

        Returns:
            Failed job.
        """
        return await client.jobs.update(
            job_id,
            JobUpdateRequest(status=JobStatus.FAILED, error=error),
        )

    def _agent_env(
        self, run_env: dict[str, str], secret_env: dict[str, str]
    ) -> dict[str, str]:
        """Build the base agent process environment.

        Layers the run spec env and secret env over the process
        environment, sets the API contract variables, and clears inherited
        contract variables.

        Args:
            run_env: Literal environment variables of the run spec.
            secret_env: Resolved secret environment variables.

        Returns:
            Environment variables for the agent process.
        """
        env = dict(os.environ)
        env.update(run_env)
        env.update(secret_env)
        for name in _CONTRACT_ENV_VARS:
            env.pop(name, None)
        env["KITARU_API_URL"] = self._api_url
        env["KITARU_API_KEY"] = self._api_key
        return env

    def _build_env(self, job_id: uuid.UUID, spec: JobSpecResponse) -> dict[str, str]:
        """Build the job agent process environment.

        ``KITARU_INPUTS`` is set only when the JSON-encoded inputs fit the
        threshold.

        Args:
            job_id: Id of the job.
            spec: Job spec.

        Returns:
            Environment variables for the agent process.
        """
        env = self._agent_env(spec.run.env, spec.secret_env)
        env["KITARU_JOB_ID"] = str(job_id)
        if spec.name is not None:
            env["KITARU_SESSION_NAME"] = spec.name
        encoded_inputs, fits = _encode_inputs(spec.inputs)
        if fits:
            env["KITARU_INPUTS"] = encoded_inputs
        return env
