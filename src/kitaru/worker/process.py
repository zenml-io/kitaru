#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Task subprocess construction and supervision."""

import asyncio
import contextlib
import os
import shlex
import signal
import sys
import tomllib
import uuid
from pathlib import Path
from typing import NamedTuple

LOG_TAIL_MAX_BYTES = 8192
MAX_INPUTS_ENV_BYTES = 32768
MAX_RESULT_BYTES = 1024 * 1024

_CONTRACT_ENV_NAMES = {
    "KITARU_API_URL",
    "KITARU_API_KEY",
    "KITARU_TASK_ID",
    "KITARU_TASK_INPUTS",
    "KITARU_TASK_PLUGIN_PATH",
    "KITARU_TASK_PAYLOAD_PATH",
    "KITARU_TASK_RESULT_PATH",
}


class TaskProcess(NamedTuple):
    """Subprocess invocation for one task."""

    command: str
    working_dir: str | None
    env: dict[str, str]
    timeout_seconds: int


class ProcessResult(NamedTuple):
    """Observed subprocess outcome."""

    returncode: int | None
    tail: str


class TailBuffer:
    """Bounded byte buffer retaining the latest output."""

    def __init__(self, max_bytes: int = LOG_TAIL_MAX_BYTES) -> None:
        """Initialize the buffer.

        Args:
            max_bytes: Number of trailing bytes to retain.
        """
        self._max_bytes = max_bytes
        self._data = bytearray()

    def write(self, data: bytes) -> None:
        """Append output and discard bytes beyond the bound.

        Args:
            data: Output bytes.
        """
        if self._max_bytes == 0:
            return
        self._data.extend(data)
        excess = len(self._data) - self._max_bytes
        if excess > 0:
            del self._data[:excess]

    def text(self) -> str:
        """Decode retained output.

        Returns:
            UTF-8 text with undecodable bytes replaced.
        """
        return self._data.decode("utf-8", errors="replace")


async def run_task_process(
    process: TaskProcess, canceled: asyncio.Event
) -> ProcessResult:
    """Run and supervise a task subprocess.

    Args:
        process: Subprocess invocation.
        canceled: Event requesting process cancellation.

    Returns:
        Process outcome and bounded log tails.
    """
    child = await asyncio.create_subprocess_exec(
        "sh",
        "-c",
        process.command,
        cwd=process.working_dir,
        env=process.env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    stdout_tail = TailBuffer()
    stderr_tail = TailBuffer()

    async def drain(stream: asyncio.StreamReader | None, buffer: TailBuffer) -> None:
        if stream is None:
            return
        while chunk := await stream.read(65536):
            buffer.write(chunk)

    drains = [
        asyncio.create_task(drain(child.stdout, stdout_tail)),
        asyncio.create_task(drain(child.stderr, stderr_tail)),
    ]
    exit_task = asyncio.create_task(child.wait())
    cancel_task = asyncio.create_task(canceled.wait())
    timeout_task = asyncio.create_task(asyncio.sleep(process.timeout_seconds))
    kill_requested = False
    try:
        done, _ = await asyncio.wait(
            {exit_task, cancel_task, timeout_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if exit_task not in done and child.returncode is None:
            kill_requested = True
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                kill_requested = False
        await exit_task
    finally:
        cancel_task.cancel()
        timeout_task.cancel()
        await asyncio.gather(cancel_task, timeout_task, return_exceptions=True)
        if child.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(child.pid, signal.SIGKILL)
            await child.wait()
        await asyncio.gather(*drains, return_exceptions=True)

    sections = []
    if stdout := stdout_tail.text():
        sections.append(f"stdout tail:\n{stdout}")
    if stderr := stderr_tail.text():
        sections.append(f"stderr tail:\n{stderr}")
    killed = kill_requested and child.returncode == -signal.SIGKILL
    return ProcessResult(
        returncode=None if killed else child.returncode,
        tail="\n".join(sections),
    )


def build_process_env(
    task_id: uuid.UUID,
    run_env: dict[str, str],
    extra_env: dict[str, str],
    secret_env: dict[str, str],
) -> dict[str, str]:
    """Build a task environment with the worker contract enforced.

    Args:
        task_id: Claimed task id.
        run_env: Environment from the run spec.
        extra_env: Creator-set task environment.
        secret_env: Resolved secret environment.

    Returns:
        Complete subprocess environment.
    """
    env = dict(os.environ)
    env.update(run_env)
    env.update(extra_env)
    env.update(secret_env)
    for name in _CONTRACT_ENV_NAMES:
        env.pop(name, None)
    for name in ("KITARU_API_URL", "KITARU_API_KEY"):
        value = os.environ.get(name)
        if value is not None:
            env[name] = value
    env["KITARU_TASK_ID"] = str(task_id)
    return env


def parse_inline_dependencies(path: Path) -> list[str]:
    """Read dependencies from a PEP 723 inline script block.

    Args:
        path: Python script path.

    Raises:
        ValueError: The script contains multiple metadata blocks or invalid
            dependency metadata.

    Returns:
        Declared dependencies, or an empty list when no block exists.
    """
    lines = path.read_text(encoding="utf-8").splitlines()
    blocks: list[list[str]] = []
    index = 0
    while index < len(lines):
        if lines[index] != "# /// script":
            index += 1
            continue
        block: list[str] = []
        index += 1
        while index < len(lines) and lines[index] != "# ///":
            line = lines[index]
            if line == "#":
                block.append("")
            elif line.startswith("# "):
                block.append(line[2:])
            else:
                break
            index += 1
        if index < len(lines) and lines[index] == "# ///":
            blocks.append(block)
        index += 1
    if len(blocks) > 1:
        raise ValueError("Script contains more than one PEP 723 script block.")
    if not blocks:
        return []
    data = tomllib.loads("\n".join(blocks[0]))
    dependencies = data.get("dependencies", [])
    if not isinstance(dependencies, list) or not all(
        isinstance(item, str) for item in dependencies
    ):
        raise ValueError("PEP 723 dependencies must be a list of strings.")
    return dependencies


def get_python_run_command(
    module: str, args: list[str], dependencies: list[str]
) -> str:
    """Build a shell-safe Python module command.

    Args:
        module: Module executed with ``-m``.
        args: Module arguments.
        dependencies: Packages installed by uv for the invocation.

    Returns:
        Shell command.
    """
    if dependencies:
        parts = ["uv", "run"]
        for dependency in dependencies:
            parts.extend(["--with", dependency])
        parts.extend(["python", "-m", module, *args])
    else:
        parts = [sys.executable, "-m", module, *args]
    return shlex.join(parts)
