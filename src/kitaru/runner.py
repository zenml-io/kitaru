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
"""Client-side replay runner."""

import asyncio
import contextlib
import json
import logging
import os
import signal
import socket
import uuid

from kitaru.api_models.v1.experiment_runs import (
    ExperimentRunResponse,
    ExperimentRunStatus,
)
from kitaru.api_models.v1.replays import (
    ReplayClaimRequest,
    ReplayResponse,
    ReplaySpecResponse,
    ReplayStatus,
    ReplayUpdateRequest,
)
from kitaru.api_models.v1.sessions import SessionScoresRequest, SessionStatus
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
# TODO: Serve this threshold from the server via the replay spec.
MAX_INPUTS_ENV_BYTES = 32768
RUN_POLL_INTERVAL_SECONDS = 2.0

_TERMINAL_RUN_STATUSES = frozenset(
    {
        ExperimentRunStatus.COMPLETED,
        ExperimentRunStatus.FAILED,
        ExperimentRunStatus.CANCELED,
    }
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


class Runner:
    """Replay runner."""

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
            concurrency: Maximum number of replays executed at once.
            heartbeat_interval: Seconds between heartbeats.
            claim_batch_size: Maximum replays claimed per request, the
                concurrency when omitted.
        """
        self._api_url = api_url
        self._api_key = api_key
        self._worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}"
        self._concurrency = concurrency
        self._heartbeat_interval = heartbeat_interval
        self._claim_batch_size = claim_batch_size or concurrency

    async def run_experiment_run(self, run_id: uuid.UUID) -> ExperimentRunResponse:
        """Execute the replays of an experiment run until it is terminal.

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
            semaphore = asyncio.Semaphore(self._concurrency)
            while True:
                claim = await client.experiment_runs.claim(
                    run_id,
                    ReplayClaimRequest(
                        worker_id=self._worker_id,
                        max_replays=self._claim_batch_size,
                    ),
                )
                if claim.replays:
                    await asyncio.gather(
                        *(
                            self._execute_claimed(client, semaphore, replay.id)
                            for replay in claim.replays
                        )
                    )
                    continue
                run = await client.experiment_runs.get(run_id)
                if run.status in _TERMINAL_RUN_STATUSES:
                    return run
                await asyncio.sleep(RUN_POLL_INTERVAL_SECONDS)

    async def run_replay(self, replay_id: uuid.UUID) -> ReplayResponse:
        """Execute a standalone replay.

        Args:
            replay_id: Id of the replay.

        Raises:
            APIError: The replay does not exist, its spec does not resolve,
                or a status update was rejected.

        Returns:
            Terminal replay.
        """
        async with KitaruAPIClient(
            base_url=self._api_url, api_key=self._api_key
        ) as client:
            await client.replays.get(replay_id)
            return await self._execute_replay(client, replay_id)

    async def _execute_claimed(
        self,
        client: KitaruAPIClient,
        semaphore: asyncio.Semaphore,
        replay_id: uuid.UUID,
    ) -> None:
        """Execute a claimed replay within the concurrency bound.

        Args:
            client: API client.
            semaphore: Concurrency bound.
            replay_id: Id of the replay.
        """
        async with semaphore:
            try:
                await self._execute_replay(client, replay_id)
            except Exception:
                logger.exception("Replay %s failed", replay_id)

    async def _execute_replay(
        self, client: KitaruAPIClient, replay_id: uuid.UUID
    ) -> ReplayResponse:
        """Execute one replay from spec fetch to its terminal status.

        Args:
            client: API client.
            replay_id: Id of the replay.

        Raises:
            APIError: The spec does not resolve or a status update was
                rejected.

        Returns:
            Terminal replay.
        """
        try:
            spec = await client.replays.get_spec(replay_id)
        except APIError as exc:
            with contextlib.suppress(APIError):
                await self._fail(
                    client, replay_id, f"Failed to resolve the replay spec: {exc}"
                )
            raise
        stdout_tail = _TailBuffer()
        stderr_tail = _TailBuffer()
        process = await asyncio.create_subprocess_exec(
            "sh",
            "-c",
            spec.run.command,
            cwd=spec.run.working_dir,
            env=self._build_env(replay_id, spec),
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
        canceled = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(client, replay_id, canceled)
        )
        try:
            await client.replays.update(
                replay_id, ReplayUpdateRequest(status=ReplayStatus.RUNNING)
            )
            exit_task = asyncio.create_task(process.wait())
            cancel_task = asyncio.create_task(canceled.wait())
            try:
                done, _ = await asyncio.wait(
                    {exit_task, cancel_task},
                    timeout=spec.run.timeout_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                cancel_task.cancel()
            if exit_task in done:
                returncode = exit_task.result()
                await asyncio.gather(*drains)
                if returncode == 0:
                    return await self._score_and_complete(client, replay_id, spec)
                error = f"Agent process exited with code {returncode}."
                if tail := _log_tail(stdout_tail, stderr_tail):
                    error = f"{error}\n{tail}"
                return await self._fail(client, replay_id, error)
            _kill_process_group(process)
            await exit_task
            await asyncio.gather(*drains)
            if canceled.is_set():
                return await client.replays.update(
                    replay_id, ReplayUpdateRequest(status=ReplayStatus.CANCELED)
                )
            error = f"Replay timed out after {spec.run.timeout_seconds} seconds."
            if tail := _log_tail(stdout_tail, stderr_tail):
                error = f"{error}\n{tail}"
            return await client.replays.update(
                replay_id,
                ReplayUpdateRequest(status=ReplayStatus.TIMED_OUT, error=error),
            )
        finally:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
            _kill_process_group(process)
            await process.wait()
            for drain in drains:
                drain.cancel()
            await asyncio.gather(*drains, return_exceptions=True)

    async def _score_and_complete(
        self,
        client: KitaruAPIClient,
        replay_id: uuid.UUID,
        spec: ReplaySpecResponse,
    ) -> ReplayResponse:
        """Score the result session and complete the replay.

        Args:
            client: API client.
            replay_id: Id of the replay.
            spec: Replay spec.

        Raises:
            APIError: A read or status update failed.

        Returns:
            Terminal replay.
        """
        replay = await client.replays.get(replay_id)
        if replay.result_session_id is None:
            return await self._fail(
                client,
                replay_id,
                "Agent process exited successfully without recording a result session.",
            )
        result_session = await client.sessions.get(replay.result_session_id)
        if result_session.status is not SessionStatus.COMPLETED:
            return await self._fail(
                client,
                replay_id,
                f"Result session {result_session.id} is {result_session.status}, "
                "not completed.",
            )
        nodes = await client.session_nodes.list(
            replay.result_session_id, include_payloads=True
        )
        view = SessionView(session=result_session, nodes=nodes)
        try:
            result = evaluate_scoring_policy(spec.scoring_policy, view)
            if spec.score_baselines:
                await self._score_baselines(client, spec)
        except ScoringError as exc:
            return await self._fail(client, replay_id, str(exc))
        return await client.replays.update(
            replay_id,
            ReplayUpdateRequest(
                status=ReplayStatus.COMPLETED,
                passed=result.passed,
                score=result.score,
                scores=result.scores,
            ),
        )

    async def _score_baselines(
        self, client: KitaruAPIClient, spec: ReplaySpecResponse
    ) -> None:
        """Score the original session for scorers missing from its scores map.

        Args:
            client: API client.
            spec: Replay spec.

        Raises:
            ScoringError: A scorer failed to load, raised, or returned an
                invalid score.
        """
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
        replay_id: uuid.UUID,
        canceled: asyncio.Event,
    ) -> None:
        """Send heartbeats until the replay is canceled server-side.

        Args:
            client: API client.
            replay_id: Id of the replay.
            canceled: Event set when the server reports cancellation.
        """
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            try:
                response = await client.replays.heartbeat(replay_id)
            except APIError as exc:
                logger.warning("Heartbeat for replay %s failed: %s", replay_id, exc)
                continue
            if response.canceled:
                canceled.set()
                return

    async def _fail(
        self, client: KitaruAPIClient, replay_id: uuid.UUID, error: str
    ) -> ReplayResponse:
        """Fail the replay with an error message.

        Args:
            client: API client.
            replay_id: Id of the replay.
            error: Error message.

        Raises:
            APIError: The status update was rejected.

        Returns:
            Failed replay.
        """
        return await client.replays.update(
            replay_id,
            ReplayUpdateRequest(status=ReplayStatus.FAILED, error=error),
        )

    def _build_env(
        self, replay_id: uuid.UUID, spec: ReplaySpecResponse
    ) -> dict[str, str]:
        """Build the agent process environment.

        Layers the run spec env and secret env over the process
        environment, with the Kitaru env contract on top. ``KITARU_INPUTS``
        is set only when the JSON-encoded inputs fit the threshold.

        Args:
            replay_id: Id of the replay.
            spec: Replay spec.

        Returns:
            Environment variables for the agent process.
        """
        env = dict(os.environ)
        env.update(spec.run.env)
        env.update(spec.secret_env)
        env["KITARU_API_URL"] = self._api_url
        env["KITARU_API_KEY"] = self._api_key
        env["KITARU_REPLAY_ID"] = str(replay_id)
        env.pop("KITARU_INPUTS", None)
        encoded_inputs = json.dumps(spec.inputs)
        if len(encoded_inputs.encode("utf-8")) <= MAX_INPUTS_ENV_BYTES:
            env["KITARU_INPUTS"] = encoded_inputs
        return env
