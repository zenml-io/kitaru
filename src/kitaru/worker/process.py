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
import shlex
import signal
import sys
import tomllib
import uuid
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import NamedTuple

logger = logging.getLogger(__name__)

LOG_TAIL_MAX_BYTES = 8192

# Contract variables the worker owns. Any inherited copy is cleared before a
# task process starts, then reset from the worker's own state.
_CONTRACT_ENV_VARIABLES = frozenset(
    {
        "KITARU_API_URL",
        "KITARU_API_KEY",
        "KITARU_TASK_TOKEN",
        "KITARU_TASK_ID",
        "KITARU_TASK_INPUTS",
        "KITARU_TASK_PLUGIN_PATH",
        "KITARU_TASK_PAYLOAD_PATH",
        "KITARU_TASK_RESULT_PATH",
    }
)

# PEP 723's reference regex for the inline script metadata block.
_PEP723_BLOCK_REGEX = (
    r"^# /// (?P<type>[A-Za-z0-9-]+)$\s(?P<content>(^#(| .*)$\s)+)^# ///$"
)


class TaskProcess(NamedTuple):
    """Task process."""

    command: str
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

    Starts the command through a shell in its own session so the whole
    process group can be killed, and always reaps the process before
    returning.

    Args:
        process: Process to run.
        canceled: Event that kills the process group when set.

    Returns:
        Exit code, None when killed on cancel or timeout, and the captured
        stdout/stderr tail.
    """
    logger.debug("Spawning task process: %s", process.command)
    child = await asyncio.create_subprocess_shell(
        process.command,
        cwd=process.working_dir,
        env=process.env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    logger.debug("Task process started with pid %d.", child.pid)
    stdout_tail = TailBuffer()
    stderr_tail = TailBuffer()
    stdout_drain = asyncio.create_task(_drain(child.stdout, stdout_tail))
    stderr_drain = asyncio.create_task(_drain(child.stderr, stderr_tail))
    wait_task = asyncio.create_task(child.wait())
    cancel_task = asyncio.create_task(canceled.wait())

    try:
        await asyncio.wait(
            {wait_task, cancel_task},
            timeout=process.timeout_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if wait_task.done():
            returncode: int | None = wait_task.result()
        else:
            returncode = None
            with contextlib.suppress(ProcessLookupError):
                os.killpg(child.pid, signal.SIGKILL)
            await wait_task
    finally:
        for task in (wait_task, cancel_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(
            wait_task, cancel_task, stdout_drain, stderr_drain, return_exceptions=True
        )

    tail = _format_tail(stdout_tail.decode(), stderr_tail.decode())
    return ProcessResult(returncode=returncode, tail=tail)


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
    and resets KITARU_API_URL, KITARU_TASK_TOKEN, and KITARU_TASK_ID from the
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
    env["KITARU_TASK_TOKEN"] = token
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


def filter_uninstalled_requirements(requirements: list[str]) -> list[str]:
    """Keep package requirements not already satisfied by the worker.

    Package plugin requirements are exact pins validated by the server. Reusing
    an installed exact version lets repository and image workers execute local
    plugin packages without resolving them from a package index.

    Args:
        requirements: Exact pinned package requirements.

    Returns:
        Requirements whose distributions are absent or have another version.
    """
    missing = []
    for requirement in requirements:
        name, separator, required_version = requirement.partition("==")
        if "[" in name:
            missing.append(requirement)
            continue
        try:
            installed_version = version(name)
        except PackageNotFoundError:
            missing.append(requirement)
            continue
        if not separator or installed_version != required_version:
            missing.append(requirement)
    return missing


def get_python_run_command(
    module: str, args: list[str], dependencies: list[str]
) -> str:
    """Build the shell command running a python module with its arguments.

    Runs the module directly when there are no dependencies, otherwise
    through ``uv run`` with each dependency passed as ``--with``.

    Args:
        module: Module to run.
        args: Arguments passed to the module.
        dependencies: Extra dependencies the module needs.

    Returns:
        Quoted shell command.
    """
    if not dependencies:
        parts = [sys.executable, "-m", module, *args]
    else:
        parts = ["uv", "run"]
        for dependency in dependencies:
            parts.extend(["--with", dependency])
        parts.extend(["python", "-m", module, *args])
    return shlex.join(parts)
