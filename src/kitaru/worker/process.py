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
"""Subprocess supervision and process/environment building for task execution."""

import asyncio
import contextlib
import logging
import os
import re
import sys
import tomllib
import uuid
from pathlib import Path
from typing import Any, NamedTuple

from kitaru.worker.platforms import WorkerPlatform, current_platform

logger = logging.getLogger(__name__)

LOG_TAIL_MAX_BYTES = 8192

# Bound on the wait for the process to exit after it has been killed.
PROCESS_KILL_WAIT_TIMEOUT_SECONDS = 5.0

# Bound on draining stdout/stderr to EOF after the process tree is killed.
PROCESS_DRAIN_TIMEOUT_SECONDS = 1.0

# Interval polled for a process's exit code.
_EXIT_POLL_INTERVAL_SECONDS = 0.05

# Contract variables the worker owns. Any inherited copy is cleared before a
# task process starts, then reset from the worker's own state.
_CONTRACT_ENV_VARIABLES = frozenset(
    {
        "KITARU_API_URL",
        "KITARU_API_KEY",
        "KITARU_API_TOKEN",
        "KITARU_TASK_ID",
        "KITARU_TASK_INPUTS",
        "KITARU_REPLAY_ID",
        "KITARU_TASK_PLUGIN_PATH",
        "KITARU_TASK_PAYLOAD_PATH",
        "KITARU_TASK_RESULT_PATH",
        "KITARU_CREDENTIALS_PATH",
    }
)

# PEP 723's reference regex for the inline script metadata block.
_PEP723_BLOCK_REGEX = (
    r"^# /// (?P<type>[A-Za-z0-9-]+)$\s(?P<content>(^#(| .*)$\s)+)^# ///$"
)


class TaskProcess(NamedTuple):
    """Task process."""

    command: str | list[str]
    working_dir: str | None
    env: dict[str, str]
    timeout_seconds: int


class ProcessResult(NamedTuple):
    """Process result."""

    returncode: int | None
    tail: str


class TailBuffer:
    """Bounded buffer keeping only the most recently written bytes."""

    def __init__(self, max_bytes: int = LOG_TAIL_MAX_BYTES) -> None:
        """Initialize the buffer.

        Args:
            max_bytes: Maximum bytes retained.
        """
        self._max_bytes = max_bytes
        self._data = bytearray()

    def write(self, chunk: bytes) -> None:
        """Append a chunk, dropping the oldest bytes past the bound.

        Args:
            chunk: Bytes read from the stream.
        """
        self._data.extend(chunk)
        overflow = len(self._data) - self._max_bytes
        if overflow > 0:
            del self._data[:overflow]

    def decode(self) -> str:
        """Decode the buffered bytes as text.

        Returns:
            Buffered content, replacing bytes that are not valid UTF-8.
        """
        return self._data.decode("utf-8", errors="replace")


async def run_task_process(
    process: TaskProcess, canceled: asyncio.Event
) -> ProcessResult:
    """Run a task process to completion, cancellation, or timeout.

    Spawns through the platform for the running OS and always kills the
    whole process tree during teardown, on every exit path including
    cancellation of the caller, so a descendant that outlives the process
    never keeps the caller waiting.

    Args:
        process: Process to run.
        canceled: Event that kills the process tree when set.

    Returns:
        Exit code, None when killed on cancel or timeout, and the captured
        stdout/stderr tail.
    """
    platform = current_platform()
    logger.debug("Spawning task process: %s", process.command)
    child = await platform.spawn(process.command, process.working_dir, process.env)
    logger.debug("Task process started with pid %d.", child.pid)
    stdout_tail = TailBuffer()
    stderr_tail = TailBuffer()
    stdout_drain = asyncio.create_task(_drain(child.stdout, stdout_tail))
    stderr_drain = asyncio.create_task(_drain(child.stderr, stderr_tail))
    wait_task = asyncio.create_task(_wait_for_exit(child))
    cancel_task = asyncio.create_task(canceled.wait())

    returncode: int | None = None
    try:
        await asyncio.wait(
            {wait_task, cancel_task},
            timeout=process.timeout_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if wait_task.done():
            returncode = wait_task.result()
    finally:
        await _teardown(
            platform, child, wait_task, cancel_task, stdout_drain, stderr_drain
        )

    tail = _format_tail(stdout_tail.decode(), stderr_tail.decode())
    return ProcessResult(returncode=returncode, tail=tail)


async def _teardown(
    platform: WorkerPlatform,
    child: asyncio.subprocess.Process,
    wait_task: asyncio.Task[int],
    cancel_task: asyncio.Task[bool],
    stdout_drain: asyncio.Task[None],
    stderr_drain: asyncio.Task[None],
) -> None:
    """Kill the process tree and bound every remaining await.

    Runs on every exit path of run_task_process, including cancellation of
    the caller, so no path awaits anything unbounded after the kill.

    Args:
        platform: Platform the process was spawned through.
        child: Process to kill.
        wait_task: Task awaiting the process's exit.
        cancel_task: Task awaiting the cancel event.
        stdout_drain: Task draining stdout into its tail buffer.
        stderr_drain: Task draining stderr into its tail buffer.
    """
    await platform.kill_tree(child)
    await _await_bounded((wait_task,), PROCESS_KILL_WAIT_TIMEOUT_SECONDS)
    await _await_bounded((stdout_drain, stderr_drain), PROCESS_DRAIN_TIMEOUT_SECONDS)
    if not cancel_task.done():
        cancel_task.cancel()
    await asyncio.gather(cancel_task, return_exceptions=True)


async def _await_bounded(tasks: tuple[asyncio.Task[Any], ...], timeout: float) -> None:
    """Await tasks up to a timeout, then cancel and await whatever remains.

    Args:
        tasks: Tasks to await.
        timeout: Seconds to wait before canceling.
    """
    with contextlib.suppress(TimeoutError, asyncio.CancelledError):
        await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True), timeout=timeout
        )
    pending = [task for task in tasks if not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


async def _wait_for_exit(child: asyncio.subprocess.Process) -> int:
    """Poll for a process's exit code.

    Process.wait() only resolves once every pipe transport attached to the
    process has also closed. A descendant that inherited the pipes and
    outlives the process can withhold that closure indefinitely, so poll
    the exit code directly instead.

    Args:
        child: Process to wait for.

    Returns:
        Exit code.
    """
    while child.returncode is None:
        await asyncio.sleep(_EXIT_POLL_INTERVAL_SECONDS)
    return child.returncode


async def _drain(stream: asyncio.StreamReader | None, tail: TailBuffer) -> None:
    """Read a stream to EOF, feeding chunks into a tail buffer.

    Args:
        stream: Stream to read, a no-op when None.
        tail: Buffer collecting the tail.
    """
    if stream is None:
        return
    while True:
        chunk = await stream.read(65536)
        if not chunk:
            break
        tail.write(chunk)


def _format_tail(stdout: str, stderr: str) -> str:
    """Format the captured stdout and stderr tails for a status message.

    Args:
        stdout: Captured stdout tail.
        stderr: Captured stderr tail.

    Returns:
        Labeled tails joined by a newline, empty when both are empty.
    """
    parts = []
    if stdout:
        parts.append(f"stdout tail:\n{stdout}")
    if stderr:
        parts.append(f"stderr tail:\n{stderr}")
    return "\n".join(parts)


def build_process_env(
    task_id: uuid.UUID,
    run_env: dict[str, str],
    extra_env: dict[str, str],
    secret_env: dict[str, str],
    token: str,
) -> dict[str, str]:
    """Build a task process environment from the inherited and spec layers.

    Layers the inherited environment, the run spec env, the creator-set
    extras, and the secret env, then clears any inherited contract variable
    and resets KITARU_API_URL, KITARU_API_TOKEN, and KITARU_TASK_ID from the
    worker's own state. An inherited KITARU_API_KEY is cleared and never
    reset, since the task process authenticates with its own task token.

    Args:
        task_id: Id of the task the process runs.
        run_env: Process environment from the run spec.
        extra_env: Creator-set environment extras from the spec.
        secret_env: Secrets merged into the process environment.
        token: Bearer token scoped to this task and attempt.

    Returns:
        Process environment ready to launch the task process with.
    """
    env = dict(os.environ)
    env.update(run_env)
    env.update(extra_env)
    env.update(secret_env)
    for name in _CONTRACT_ENV_VARIABLES:
        env.pop(name, None)
    env["KITARU_API_URL"] = os.environ["KITARU_API_URL"]
    env["KITARU_API_TOKEN"] = token
    env["KITARU_TASK_ID"] = str(task_id)
    return env


def parse_inline_dependencies(path: Path) -> list[str]:
    """Parse PEP 723 inline script dependencies declared in a file.

    Args:
        path: Script file to parse.

    Raises:
        ValueError: The file declares more than one script metadata block.

    Returns:
        Declared dependencies, empty when no script metadata block exists.
    """
    text = path.read_text(encoding="utf-8")
    blocks = [
        match
        for match in re.finditer(_PEP723_BLOCK_REGEX, text, flags=re.MULTILINE)
        if match.group("type") == "script"
    ]
    if not blocks:
        return []
    if len(blocks) > 1:
        raise ValueError(f"Multiple inline script metadata blocks found in {path}.")
    content = "".join(
        line[2:] if line.startswith("# ") else line[1:]
        for line in blocks[0].group("content").splitlines(keepends=True)
    )
    metadata = tomllib.loads(content)
    dependencies = metadata.get("dependencies", [])
    return list(dependencies)


def get_python_run_argv(
    module: str, args: list[str], dependencies: list[str]
) -> list[str]:
    """Build the argv running a python module with its arguments.

    Runs the module directly when there are no dependencies, otherwise
    through ``uv run`` with each dependency passed as ``--with``.

    Args:
        module: Module to run.
        args: Arguments passed to the module.
        dependencies: Extra dependencies the module needs.

    Returns:
        Argument vector.
    """
    if not dependencies:
        return [sys.executable, "-m", module, *args]
    parts = ["uv", "run"]
    for dependency in dependencies:
        parts.extend(["--with", dependency])
    parts.extend(["python", "-m", module, *args])
    return parts
