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
import shlex
import signal
import socket
import sys
import tomllib
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any, NamedTuple

from kitaru.api_models.v1.experiment_runs import (
    ExperimentRunResponse,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.jobs import (
    ClaimedJobResponse,
    JobClaimRequest,
    JobKind,
    JobResponse,
    JobSpecResponse,
    JobStatus,
    JobUpdateRequest,
    StandaloneJobClaimRequest,
)
from kitaru.api_models.v1.sessions import SessionStatus
from kitaru.api_models.v1.workers import (
    WorkerCreateRequest,
    WorkerHeartbeatRequest,
)
from kitaru.blob_cache import DEFAULT_PAYLOAD_CACHE_ROOT, BlobCache
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError

logger = logging.getLogger(__name__)

DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10.0
LOG_TAIL_MAX_BYTES = 8192
# TODO: Serve this threshold from the server via the job spec.
MAX_INPUTS_ENV_BYTES = 32768
RUN_POLL_INTERVAL_SECONDS = 2.0
SCORE_TIMEOUT_SECONDS = 300
IMPORT_TIMEOUT_SECONDS = 600
PAYLOAD_CACHE_MAX_BYTES = 1024**3

_TERMINAL_RUN_STATUSES = frozenset(
    {
        ExperimentRunStatus.COMPLETED,
        ExperimentRunStatus.FAILED,
        ExperimentRunStatus.CANCELED,
    }
)

_TERMINAL_JOB_STATUSES = frozenset(
    {
        JobStatus.COMPLETED,
        JobStatus.FAILED,
        JobStatus.TIMED_OUT,
        JobStatus.CANCELED,
    }
)

# Contract variables the runner controls, cleared from the inherited
# environment before each job process.
_CONTRACT_ENV_VARS = (
    "KITARU_JOB_ID",
    "KITARU_JOB_INPUTS",
    "KITARU_JOB_SESSION_NAME",
    "KITARU_JOB_PLUGIN_PATH",
    "KITARU_JOB_PAYLOAD_PATH",
)

# Label of the process a job kind runs, used in exit code errors.
_PROCESS_LABELS = {JobKind.SCORE: "Scorer", JobKind.IMPORT: "Importer"}

# PEP 723 inline script metadata, the regular expression of the
# specification.
_INLINE_METADATA_PATTERN = re.compile(
    r"(?m)^# /// (?P<type>[a-zA-Z0-9-]+)$\s(?P<content>(^#(| .*)$\s)+)^# ///$"
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
    """Kill the process group of a running job process.

    Args:
        process: Job process started in its own session.
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


def inline_dependencies(path: Path) -> list[str]:
    """Read the dependencies a file declares as PEP 723 inline metadata.

    Args:
        path: Path of the file.

    Raises:
        ValueError: The file declares more than one script block.

    Returns:
        Declared dependencies, empty without a script block.
    """
    blocks = [
        match
        for match in _INLINE_METADATA_PATTERN.finditer(path.read_text(encoding="utf-8"))
        if match.group("type") == "script"
    ]
    if not blocks:
        return []
    if len(blocks) > 1:
        raise ValueError(f"{path} declares multiple inline script blocks")
    content = "".join(
        line[2:] if line.startswith("# ") else line[1:]
        for line in blocks[0].group("content").splitlines(keepends=True)
    )
    return [str(entry) for entry in tomllib.loads(content).get("dependencies", [])]


def _harness_command(module: str, dependencies: list[str]) -> str:
    """Build the command running a harness module.

    Dependencies are resolved by uv, which needs a project or an
    interpreter of its own.

    Args:
        module: Harness module to run.
        dependencies: Dependencies of the registered code.

    Returns:
        Bash command starting the harness.
    """
    if not dependencies:
        return shlex.join([sys.executable, "-m", module])
    args = ["uv", "run"]
    for dependency in dependencies:
        args.extend(["--with", dependency])
    args.extend(["python", "-m", module])
    return shlex.join(args)


def score_command(dependencies: list[str]) -> str:
    """Build the command running the score harness.

    Args:
        dependencies: Dependencies of the scorer code.

    Returns:
        Bash command starting the harness.
    """
    return _harness_command("kitaru.score", dependencies)


def import_command(dependencies: list[str]) -> str:
    """Build the command running the import harness.

    Args:
        dependencies: Dependencies of the importer code.

    Returns:
        Bash command starting the harness.
    """
    return _harness_command("kitaru.imports", dependencies)


def _encode_inputs(inputs: Any) -> tuple[str, bool]:
    """JSON-encode inputs and report whether they fit the env threshold.

    Args:
        inputs: Inputs to encode.

    Returns:
        Encoded inputs and whether they fit ``MAX_INPUTS_ENV_BYTES``.
    """
    encoded = json.dumps(inputs)
    return encoded, len(encoded.encode("utf-8")) <= MAX_INPUTS_ENV_BYTES


class JobProcess(NamedTuple):
    """Subprocess invocation of a job."""

    command: str
    working_dir: str | None
    env: dict[str, str]
    timeout_seconds: int


class WorkerHeartbeat:
    """Batched heartbeat of a worker's in-flight jobs."""

    def __init__(
        self,
        client: KitaruAPIClient,
        worker_id: uuid.UUID,
        interval: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        """Initialize the heartbeat.

        Args:
            client: API client.
            worker_id: Id of the registered worker.
            interval: Seconds between heartbeats.
        """
        self._client = client
        self._worker_id = worker_id
        self._interval = interval
        self._canceled: dict[uuid.UUID, asyncio.Event] = {}

    def register(self, job_id: uuid.UUID) -> asyncio.Event:
        """Report a job as in flight until it is unregistered.

        Args:
            job_id: Id of the job.

        Returns:
            Event set once the server asks the worker to abandon the job.
        """
        canceled = asyncio.Event()
        self._canceled[job_id] = canceled
        return canceled

    def unregister(self, job_id: uuid.UUID) -> None:
        """Stop reporting a job as in flight.

        Args:
            job_id: Id of the job.
        """
        self._canceled.pop(job_id, None)

    async def run(self) -> None:
        """Send one heartbeat per interval until cancellation."""
        while True:
            await asyncio.sleep(self._interval)
            job_ids = list(self._canceled)
            if not job_ids:
                continue
            try:
                response = await self._client.workers.heartbeat(
                    self._worker_id, WorkerHeartbeatRequest(job_ids=job_ids)
                )
            except APIError as exc:
                logger.warning(
                    "Heartbeat for worker %s failed: %s", self._worker_id, exc
                )
                continue
            for job_id in response.abandon:
                canceled = self._canceled.get(job_id)
                if canceled is not None:
                    canceled.set()


class JobRunner:
    """Runner of one claimed job."""

    def __init__(
        self,
        api_url: str,
        api_key: str,
        blob_cache: BlobCache | None = None,
        payload_cache: BlobCache | None = None,
    ) -> None:
        """Initialize the job runner.

        Args:
            api_url: Server base URL.
            api_key: API key sent as a bearer token.
            blob_cache: Cache the plugin code is materialized into.
            payload_cache: Cache the import payloads are materialized into.
        """
        self._api_url = api_url
        self._api_key = api_key
        self._blob_cache = blob_cache or BlobCache()
        self._payload_cache = payload_cache or BlobCache(
            DEFAULT_PAYLOAD_CACHE_ROOT, max_bytes=PAYLOAD_CACHE_MAX_BYTES
        )

    async def execute(
        self,
        client: KitaruAPIClient,
        job_id: uuid.UUID,
        heartbeat: WorkerHeartbeat,
        spec: JobSpecResponse | None = None,
    ) -> JobResponse:
        """Execute a claimed job from its spec to its next status.

        Replays hand over to their score jobs instead of completing.

        Args:
            client: API client.
            job_id: Id of the job.
            heartbeat: Heartbeat reporting the job while it runs.
            spec: Spec the claim shipped, fetched when omitted.

        Raises:
            APIError: The spec does not resolve or a status update was
                rejected.

        Returns:
            Job in the status the run produced.
        """
        if spec is None:
            try:
                spec = await client.jobs.get_spec(job_id)
            except APIError as exc:
                with contextlib.suppress(APIError):
                    await self._fail(
                        client, job_id, f"Failed to resolve the job spec: {exc}"
                    )
                raise
        canceled = heartbeat.register(job_id)
        try:
            await client.jobs.update(job_id, JobUpdateRequest(status=JobStatus.RUNNING))
            if spec.kind is JobKind.SCORE:
                try:
                    process = await self._score_process(client, job_id, spec)
                except Exception as exc:
                    return await self._fail(
                        client, job_id, f"Failed to prepare the scorer process: {exc}"
                    )
            elif spec.kind is JobKind.IMPORT:
                try:
                    process = await self._import_process(client, job_id, spec)
                except Exception as exc:
                    return await self._fail(
                        client, job_id, f"Failed to prepare the importer process: {exc}"
                    )
            else:
                process = self._agent_process(job_id, spec)
            returncode, tail = await self._run_process(process, canceled)
            if returncode is not None:
                if returncode == 0:
                    return await self._finalize(client, job_id, spec)
                label = _PROCESS_LABELS.get(spec.kind, "Agent")
                error = f"{label} process exited with code {returncode}."
                return await self._fail(client, job_id, _with_tail(error, tail))
            if canceled.is_set():
                return await client.jobs.update(
                    job_id, JobUpdateRequest(status=JobStatus.CANCELED)
                )
            error = _with_tail(
                f"Job timed out after {process.timeout_seconds} seconds.", tail
            )
            return await client.jobs.update(
                job_id,
                JobUpdateRequest(status=JobStatus.TIMED_OUT, error=error),
            )
        finally:
            heartbeat.unregister(job_id)

    async def _run_process(
        self,
        job_process: JobProcess,
        canceled: asyncio.Event | None = None,
    ) -> tuple[int | None, str]:
        """Run a job process until exit, timeout, or cancellation.

        Args:
            job_process: Subprocess invocation.
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
            job_process.command,
            cwd=job_process.working_dir,
            env=job_process.env,
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
                    timeout=job_process.timeout_seconds,
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

    async def _result_session_error(
        self, client: KitaruAPIClient, job_id: uuid.UUID
    ) -> str | None:
        """Report why the result session of a job does not hold up.

        Args:
            client: API client.
            job_id: Id of the job.

        Raises:
            APIError: A read failed.

        Returns:
            Error message, ``None`` when the session is completed.
        """
        job = await client.jobs.get(job_id)
        if job.result_session_id is None:
            return (
                "Agent process exited successfully without recording a result session."
            )
        result_session = await client.sessions.get(job.result_session_id)
        if result_session.status is not SessionStatus.COMPLETED:
            return (
                f"Result session {result_session.id} is {result_session.status}, "
                "not completed."
            )
        return None

    async def _finalize(
        self,
        client: KitaruAPIClient,
        job_id: uuid.UUID,
        spec: JobSpecResponse,
    ) -> JobResponse:
        """Move a job whose process succeeded to its next status.

        A replay hands over to its score jobs, the other kinds complete.

        Args:
            client: API client.
            job_id: Id of the job.
            spec: Job spec.

        Raises:
            APIError: A read or status update failed.

        Returns:
            Updated job.
        """
        if spec.kind in (JobKind.SCORE, JobKind.IMPORT):
            return await client.jobs.update(
                job_id, JobUpdateRequest(status=JobStatus.COMPLETED)
            )
        error = await self._result_session_error(client, job_id)
        if error is not None:
            return await self._fail(client, job_id, error)
        status = (
            JobStatus.COMPLETED
            if spec.kind is JobKind.SESSION_RUN
            else JobStatus.SCORING
        )
        return await client.jobs.update(job_id, JobUpdateRequest(status=status))

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

    def _process_env(
        self, job_id: uuid.UUID, run_env: dict[str, str], secret_env: dict[str, str]
    ) -> dict[str, str]:
        """Build the base job process environment.

        Layers the run spec env and secret env over the process
        environment, sets the API contract variables, and clears inherited
        contract variables.

        Args:
            job_id: Id of the job.
            run_env: Literal environment variables of the run spec.
            secret_env: Resolved secret environment variables.

        Returns:
            Environment variables for the job process.
        """
        env = dict(os.environ)
        env.update(run_env)
        env.update(secret_env)
        for name in _CONTRACT_ENV_VARS:
            env.pop(name, None)
        env["KITARU_API_URL"] = self._api_url
        env["KITARU_API_KEY"] = self._api_key
        env["KITARU_JOB_ID"] = str(job_id)
        return env

    def _agent_process(self, job_id: uuid.UUID, spec: JobSpecResponse) -> JobProcess:
        """Build the agent process invocation of a replay or session run.

        ``KITARU_JOB_INPUTS`` is set only when the JSON-encoded inputs fit
        the threshold.

        Args:
            job_id: Id of the job.
            spec: Job spec.

        Returns:
            Subprocess invocation.
        """
        assert spec.run is not None
        env = self._process_env(job_id, spec.run.env, spec.secret_env)
        if spec.name is not None:
            env["KITARU_JOB_SESSION_NAME"] = spec.name
        encoded_inputs, fits = _encode_inputs(spec.inputs)
        if fits:
            env["KITARU_JOB_INPUTS"] = encoded_inputs
        return JobProcess(
            command=spec.run.command,
            working_dir=spec.run.working_dir,
            env=env,
            timeout_seconds=spec.run.timeout_seconds,
        )

    async def _materialize_blob(
        self,
        client: KitaruAPIClient,
        cache: BlobCache,
        blob_id: uuid.UUID,
        sha256: str,
    ) -> Path:
        """Return the cached path of a blob, downloading it once.

        Args:
            client: API client.
            cache: Cache the content is materialized into.
            blob_id: Id of the blob.
            sha256: Hash of the blob content.

        Raises:
            APIError: The download failed.
            BlobCacheError: The downloaded content has another hash.

        Returns:
            Path of the cached file.
        """
        cached = cache.get(sha256)
        if cached is not None:
            return cached
        content = await client.blobs.download(blob_id)
        return cache.put(sha256, content)

    async def _score_process(
        self,
        client: KitaruAPIClient,
        job_id: uuid.UUID,
        spec: JobSpecResponse,
    ) -> JobProcess:
        """Build the harness process invocation of a score job.

        Registered code runs from the blob cache on any worker, a source
        reference runs in the run environment of the agent version.

        Args:
            client: API client.
            job_id: Id of the job.
            spec: Job spec.

        Raises:
            APIError: The code download failed.
            BlobCacheError: The downloaded code has another hash.

        Returns:
            Subprocess invocation.
        """
        assert spec.scorer is not None
        run = spec.run
        env = self._process_env(
            job_id, run.env if run is not None else {}, spec.secret_env
        )
        if spec.scorer.plugin is None:
            assert run is not None
            return JobProcess(
                command=score_command([]),
                working_dir=run.working_dir,
                env=env,
                timeout_seconds=run.timeout_seconds,
            )
        path = await self._materialize_blob(
            client,
            self._blob_cache,
            spec.scorer.plugin.blob_id,
            spec.scorer.plugin.sha256,
        )
        env["KITARU_JOB_PLUGIN_PATH"] = str(path)
        return JobProcess(
            command=score_command(inline_dependencies(path)),
            working_dir=None,
            env=env,
            timeout_seconds=(
                run.timeout_seconds if run is not None else SCORE_TIMEOUT_SECONDS
            ),
        )

    async def _import_process(
        self,
        client: KitaruAPIClient,
        job_id: uuid.UUID,
        spec: JobSpecResponse,
    ) -> JobProcess:
        """Build the harness process invocation of an import job.

        The importer code and the payload are materialized into their
        caches, the harness reads both from disk.

        Args:
            client: API client.
            job_id: Id of the job.
            spec: Job spec.

        Raises:
            APIError: A download failed.
            BlobCacheError: A download has another hash.

        Returns:
            Subprocess invocation.
        """
        assert spec.importer is not None
        env = self._process_env(job_id, {}, spec.secret_env)
        code, payload = await asyncio.gather(
            self._materialize_blob(
                client,
                self._blob_cache,
                spec.importer.plugin.blob_id,
                spec.importer.plugin.sha256,
            ),
            self._materialize_blob(
                client,
                self._payload_cache,
                spec.importer.payload.blob_id,
                spec.importer.payload.sha256,
            ),
        )
        env["KITARU_JOB_PLUGIN_PATH"] = str(code)
        env["KITARU_JOB_PAYLOAD_PATH"] = str(payload)
        return JobProcess(
            command=import_command(inline_dependencies(code)),
            working_dir=None,
            env=env,
            timeout_seconds=IMPORT_TIMEOUT_SECONDS,
        )


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
        self._heartbeat_interval = heartbeat_interval
        self._job_runner = JobRunner(api_url=api_url, api_key=api_key)

    @contextlib.asynccontextmanager
    async def _heartbeating(
        self, client: KitaruAPIClient, worker_id: uuid.UUID
    ) -> AsyncIterator[WorkerHeartbeat]:
        """Run one worker heartbeat for the duration of the block.

        Args:
            client: API client.
            worker_id: Id of the registered worker.

        Yields:
            Heartbeat the executed jobs register with.
        """
        heartbeat = WorkerHeartbeat(client, worker_id, self._heartbeat_interval)
        task = asyncio.create_task(heartbeat.run())
        try:
            yield heartbeat
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def run_job(self, job_id: uuid.UUID) -> JobResponse:
        """Claim and execute a standalone job, including its score jobs.

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
            async with self._heartbeating(client, worker_id) as heartbeat:
                job = await self._job_runner.execute(client, job_id, heartbeat)
            if job.status is not JobStatus.SCORING:
                return job
            return await self._run_children(client, worker_id, job_id)

    async def _run_children(
        self, client: KitaruAPIClient, worker_id: uuid.UUID, job_id: uuid.UUID
    ) -> JobResponse:
        """Execute the jobs fanned out from a job until it is terminal.

        Args:
            client: API client.
            worker_id: Id of the registered worker.
            job_id: Id of the parent job.

        Raises:
            APIError: A claim or job read failed.

        Returns:
            Terminal parent job.
        """

        async def parent_is_terminal() -> bool:
            job = await client.jobs.get(job_id)
            return job.status in _TERMINAL_JOB_STATUSES

        await self._claim_loop(
            client, worker_id, parent_is_terminal, parent_job_id=job_id
        )
        return await client.jobs.get(job_id)

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
        parent_job_id: uuid.UUID | None = None,
    ) -> None:
        """Claim and execute jobs until an empty claim meets the stop condition.

        Args:
            client: API client.
            worker_id: Id of the registered worker.
            should_stop: Condition checked after an empty claim.
            agent_ids: Ids of the agents to claim for.
            experiment_run_id: Id of the experiment run to claim for.
            parent_job_id: Id of the job whose fanned out jobs to claim
                for.

        Raises:
            APIError: A claim or stop condition read failed.
        """
        semaphore = asyncio.Semaphore(self._concurrency)
        async with self._heartbeating(client, worker_id) as heartbeat:
            while True:
                claim = await client.jobs.claim(
                    JobClaimRequest(
                        worker_id=worker_id,
                        max_jobs=self._claim_batch_size,
                        agent_ids=agent_ids,
                        experiment_run_id=experiment_run_id,
                        parent_job_id=parent_job_id,
                    )
                )
                if claim.jobs:
                    await asyncio.gather(
                        *(
                            self._execute_claimed(client, semaphore, heartbeat, claimed)
                            for claimed in claim.jobs
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
        heartbeat: WorkerHeartbeat,
        claimed: ClaimedJobResponse,
    ) -> None:
        """Execute a claimed job within the concurrency bound.

        Args:
            client: API client.
            semaphore: Concurrency bound.
            heartbeat: Heartbeat reporting the job while it runs.
            claimed: Claimed job and its spec.
        """
        async with semaphore:
            try:
                await self._job_runner.execute(
                    client, claimed.job.id, heartbeat, spec=claimed.spec
                )
            except Exception:
                logger.exception("Job %s failed", claimed.job.id)

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
