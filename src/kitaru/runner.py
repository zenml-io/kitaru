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
import re
import signal
import socket
import uuid
from collections.abc import Awaitable, Callable
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
    StandaloneJobClaimRequest,
)
from kitaru.api_models.v1.sessions import (
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
    "KITARU_SESSION_NAME",
)


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


def _default_worker_name() -> str:
    """Derive a worker name from the hostname and pid.

    Characters outside the server's worker name charset are replaced
    with dashes.

    Returns:
        Worker name.
    """
    hostname = re.sub(r"[^A-Za-z0-9_-]", "-", socket.gethostname())
    return f"{hostname}-{os.getpid()}".strip("-_")


def _encode_inputs(inputs: Any) -> tuple[str, bool]:
    """JSON-encode inputs and report whether they fit the env threshold.

    Args:
        inputs: Inputs to encode.

    Returns:
        Encoded inputs and whether they fit ``MAX_INPUTS_ENV_BYTES``.
    """
    encoded = json.dumps(inputs)
    return encoded, len(encoded.encode("utf-8")) <= MAX_INPUTS_ENV_BYTES


class JobRunner:
    """Runner of one claimed job."""

    def __init__(
        self,
        api_url: str,
        api_key: str,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        """Initialize the job runner.

        Args:
            api_url: Server base URL.
            api_key: API key sent as a bearer token.
            heartbeat_interval: Seconds between heartbeats.
        """
        self._api_url = api_url
        self._api_key = api_key
        self._heartbeat_interval = heartbeat_interval

    async def execute(self, client: KitaruAPIClient, job_id: uuid.UUID) -> JobResponse:
        """Execute a claimed job from spec fetch to its terminal status.

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


class Runner:
    """Job runner."""

    def __init__(
        self,
        api_url: str,
        api_key: str,
        worker_name: str | None = None,
        concurrency: int = 1,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        claim_batch_size: int | None = None,
        poll_interval: float = RUN_POLL_INTERVAL_SECONDS,
    ) -> None:
        """Initialize the runner.

        Args:
            api_url: Server base URL.
            api_key: API key sent as a bearer token.
            worker_name: Name this runner registers its worker under,
                hostname and pid when omitted.
            concurrency: Maximum number of jobs executed at once.
            heartbeat_interval: Seconds between heartbeats.
            claim_batch_size: Maximum jobs claimed per request, the
                concurrency when omitted.
            poll_interval: Seconds between claims when a claim returns no
                jobs.
        """
        self._api_url = api_url
        self._api_key = api_key
        self._worker_name = worker_name or _default_worker_name()
        self._concurrency = concurrency
        self._claim_batch_size = claim_batch_size or concurrency
        self._poll_interval = poll_interval
        self._job_runner = JobRunner(
            api_url=api_url, api_key=api_key, heartbeat_interval=heartbeat_interval
        )

    async def run_job(self, job_id: uuid.UUID) -> JobResponse:
        """Claim and execute a standalone job.

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
            return await self._job_runner.execute(client, job_id)

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

            async def run_is_terminal() -> bool:
                run = await client.experiment_runs.get(run_id)
                return run.status in _TERMINAL_RUN_STATUSES

            await self._claim_loop(
                client, worker_id, run_is_terminal, experiment_run_id=run_id
            )
            return await client.experiment_runs.get(run_id)

    async def run_worker(
        self,
        agent_ids: list[uuid.UUID] | None = None,
        stop: asyncio.Event | None = None,
    ) -> None:
        """Claim and execute pool jobs until stopped.

        Args:
            agent_ids: Ids of the agents to claim for, any agent when
                omitted.
            stop: Event ending the loop once its claims drain, the loop
                runs until cancellation when omitted.

        Raises:
            APIError: The registration or a claim failed.
        """
        async with KitaruAPIClient(
            base_url=self._api_url, api_key=self._api_key
        ) as client:
            worker_id = await self._register_worker(client, agent_ids)

            async def stopped() -> bool:
                return stop is not None and stop.is_set()

            await self._claim_loop(client, worker_id, stopped, agent_ids=agent_ids)

    async def _claim_loop(
        self,
        client: KitaruAPIClient,
        worker_id: uuid.UUID,
        should_stop: Callable[[], Awaitable[bool]],
        agent_ids: list[uuid.UUID] | None = None,
        experiment_run_id: uuid.UUID | None = None,
    ) -> None:
        """Claim and execute jobs until an empty claim meets the stop condition.

        Args:
            client: API client.
            worker_id: Id of the registered worker.
            should_stop: Condition checked after an empty claim.
            agent_ids: Ids of the agents to claim for.
            experiment_run_id: Id of the experiment run to claim for.

        Raises:
            APIError: A claim or stop condition read failed.
        """
        semaphore = asyncio.Semaphore(self._concurrency)
        while True:
            claim = await client.jobs.claim(
                JobClaimRequest(
                    worker_id=worker_id,
                    max_jobs=self._claim_batch_size,
                    agent_ids=agent_ids,
                    experiment_run_id=experiment_run_id,
                )
            )
            if claim.jobs:
                await asyncio.gather(
                    *(
                        self._execute_claimed(client, semaphore, job.id)
                        for job in claim.jobs
                    )
                )
                continue
            if await should_stop():
                return
            await asyncio.sleep(self._poll_interval)

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
                await self._job_runner.execute(client, job_id)
            except Exception:
                logger.exception("Job %s failed", job_id)

    async def _register_worker(
        self, client: KitaruAPIClient, agent_ids: list[uuid.UUID] | None = None
    ) -> uuid.UUID:
        """Register this runner as a worker, upserting by name.

        Args:
            client: API client.
            agent_ids: Ids of the agents this worker serves.

        Raises:
            APIError: The registration failed.

        Returns:
            Id of the registered worker.
        """
        worker = await client.workers.create(
            WorkerCreateRequest(name=self._worker_name, agent_ids=agent_ids or [])
        )
        return worker.id
