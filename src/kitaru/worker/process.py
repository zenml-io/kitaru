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
"""Job process supervision."""

import asyncio
import contextlib
import os
import re
import shlex
import signal
import sys
import tomllib
import uuid
from pathlib import Path
from typing import NamedTuple

LOG_TAIL_MAX_BYTES = 8192

# Contract variables the worker controls, cleared from the inherited
# environment before each job process.
_CONTRACT_ENV_VARS = (
    "KITARU_JOB_ID",
    "KITARU_JOB_INPUTS",
    "KITARU_JOB_SESSION_NAME",
    "KITARU_JOB_PLUGIN_PATH",
    "KITARU_JOB_PAYLOAD_PATH",
    "KITARU_JOB_RESULT_PATH",
)

# PEP 723 inline script metadata, the regular expression of the
# specification.
_INLINE_METADATA_PATTERN = re.compile(
    r"(?m)^# /// (?P<type>[a-zA-Z0-9-]+)$\s(?P<content>(^#(| .*)$\s)+)^# ///$"
)


class JobProcess(NamedTuple):
    """Subprocess invocation of a job."""

    command: str
    working_dir: str | None
    env: dict[str, str]
    timeout_seconds: int | None


class ProcessResult(NamedTuple):
    """Result of a supervised job process."""

    returncode: int | None
    tail: str


class TailBuffer:
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


async def _drain_stream(stream: asyncio.StreamReader, tail: TailBuffer) -> None:
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


def _format_tail(stdout: TailBuffer, stderr: TailBuffer) -> str:
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


async def run_job_process(
    process: JobProcess, canceled: asyncio.Event
) -> ProcessResult:
    """Run a job process until exit, timeout, or cancellation.

    Args:
        process: Subprocess invocation.
        canceled: Event whose set kills the process.

    Returns:
        Exit code and captured log tail, the exit code ``None`` when the
        process was killed on timeout or cancellation.
    """
    stdout_tail = TailBuffer()
    stderr_tail = TailBuffer()
    subprocess = await asyncio.create_subprocess_exec(
        "sh",
        "-c",
        process.command,
        cwd=process.working_dir,
        env=process.env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    drains = [
        asyncio.create_task(_drain_stream(stream, tail))
        for stream, tail in (
            (subprocess.stdout, stdout_tail),
            (subprocess.stderr, stderr_tail),
        )
        if stream is not None
    ]
    try:
        exit_task = asyncio.create_task(subprocess.wait())
        cancel_task = asyncio.create_task(canceled.wait())
        try:
            done, _ = await asyncio.wait(
                {exit_task, cancel_task},
                timeout=process.timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            cancel_task.cancel()
        if exit_task in done:
            returncode = exit_task.result()
            await asyncio.gather(*drains)
            return ProcessResult(returncode, _format_tail(stdout_tail, stderr_tail))
        _kill_process_group(subprocess)
        await exit_task
        await asyncio.gather(*drains)
        return ProcessResult(None, _format_tail(stdout_tail, stderr_tail))
    finally:
        _kill_process_group(subprocess)
        await subprocess.wait()
        for drain in drains:
            drain.cancel()
        await asyncio.gather(*drains, return_exceptions=True)


def build_process_env(
    job_id: uuid.UUID, run_env: dict[str, str], secret_env: dict[str, str]
) -> dict[str, str]:
    """Build the base job process environment.

    Layers the run spec env and secret env over the inherited process
    environment, clears inherited contract variables, and re-asserts the
    API contract variables so neither layer can override them.

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
    env["KITARU_API_URL"] = os.environ["KITARU_API_URL"]
    env["KITARU_API_KEY"] = os.environ["KITARU_API_KEY"]
    env["KITARU_JOB_ID"] = str(job_id)
    return env


def parse_inline_dependencies(path: Path) -> list[str]:
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


def get_python_run_command(
    module: str, args: list[str], dependencies: list[str]
) -> str:
    """Build the command running a python module with arguments.

    Args:
        module: Module to run.
        args: Arguments to the module.
        dependencies: Dependencies resolved by uv, empty to run with the
            worker's own interpreter.

    Returns:
        Shell command starting the module.
    """
    if not dependencies:
        return shlex.join([sys.executable, "-m", module, *args])
    command = ["uv", "run"]
    for dependency in dependencies:
        command.extend(["--with", dependency])
    command.extend(["python", "-m", module, *args])
    return shlex.join(command)
