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
"""Tests for job process supervision, env building, and command building."""

import asyncio
import sys
import time
import uuid
from pathlib import Path

import pytest

from kitaru.worker.process import (
    JobProcess,
    TailBuffer,
    build_process_env,
    get_python_run_command,
    parse_inline_dependencies,
    run_job_process,
)


def process(command: str, timeout_seconds: int | None = 30) -> JobProcess:
    """Build a job process for a bare command with an empty environment."""
    return JobProcess(
        command=command, working_dir=None, env={}, timeout_seconds=timeout_seconds
    )


async def test_exit_code_and_tail_are_captured() -> None:
    """Report the exit code and the captured stdout and stderr tails."""
    result = await run_job_process(
        process("echo out && echo err >&2 && exit 3"), asyncio.Event()
    )

    assert result.returncode == 3
    assert "stdout tail:\nout" in result.tail
    assert "stderr tail:\nerr" in result.tail


async def test_timeout_kills_the_process_group() -> None:
    """Kill a process past its timeout and report no exit code."""
    started = time.monotonic()
    result = await run_job_process(
        process("sleep 30", timeout_seconds=1), asyncio.Event()
    )

    assert time.monotonic() - started < 10
    assert result.returncode is None


async def test_cancel_event_kills_the_process() -> None:
    """Kill a process once the cancel event is set, with no timeout bound."""
    canceled = asyncio.Event()

    async def cancel_soon() -> None:
        await asyncio.sleep(0.05)
        canceled.set()

    started = time.monotonic()
    task = asyncio.create_task(cancel_soon())
    result = await run_job_process(process("sleep 30", timeout_seconds=None), canceled)
    await task

    assert time.monotonic() - started < 10
    assert result.returncode is None


async def test_no_tail_when_nothing_was_written() -> None:
    """Report an empty tail when the process wrote nothing."""
    result = await run_job_process(process("true"), asyncio.Event())

    assert result.returncode == 0
    assert result.tail == ""


def test_tail_buffer_keeps_only_the_trailing_bytes() -> None:
    """Drop the oldest bytes once the buffer exceeds its cap."""
    buffer = TailBuffer(max_bytes=4)

    buffer.write(b"abcdefgh")

    assert buffer.text() == "efgh"


async def test_build_process_env_layers_and_strips_contract_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Layer run env then secret env, and re-assert the API contract vars."""
    monkeypatch.setenv("KITARU_API_URL", "http://server")
    monkeypatch.setenv("KITARU_API_KEY", "secret-key")
    monkeypatch.setenv("KITARU_TEST_OS_VAR", "os")
    monkeypatch.setenv("KITARU_JOB_PLUGIN_PATH", "/inherited")
    monkeypatch.setenv("KITARU_JOB_PAYLOAD_PATH", "/inherited")
    job_id = uuid.uuid4()

    env = build_process_env(
        job_id,
        run_env={"KITARU_TEST_OS_VAR": "run", "KITARU_TEST_SHARED": "run"},
        secret_env={"KITARU_TEST_SHARED": "secret", "KITARU_API_URL": "override"},
    )

    assert env["KITARU_TEST_OS_VAR"] == "run"
    assert env["KITARU_TEST_SHARED"] == "secret"
    assert env["KITARU_API_URL"] == "http://server"
    assert env["KITARU_API_KEY"] == "secret-key"
    assert env["KITARU_JOB_ID"] == str(job_id)
    assert "KITARU_JOB_PLUGIN_PATH" not in env
    assert "KITARU_JOB_PAYLOAD_PATH" not in env


def test_parse_inline_dependencies_reads_the_script_block(tmp_path: Path) -> None:
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
    assert parse_inline_dependencies(path) == ["httpx", "orjson>=3"]


def test_parse_inline_dependencies_without_a_block(tmp_path: Path) -> None:
    """Report no dependencies for code without inline metadata."""
    path = tmp_path / "scorer"
    path.write_text("def score(session):\n    return 1.0\n")
    assert parse_inline_dependencies(path) == []


def test_parse_inline_dependencies_ignores_other_block_types(tmp_path: Path) -> None:
    """Ignore inline metadata blocks that are not script blocks."""
    path = tmp_path / "scorer"
    path.write_text('# /// other\n# dependencies = ["httpx"]\n# ///\n')
    assert parse_inline_dependencies(path) == []


def test_parse_inline_dependencies_rejects_multiple_blocks(tmp_path: Path) -> None:
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
        parse_inline_dependencies(path)


def test_get_python_run_command_without_dependencies() -> None:
    """Run the module with the worker's own interpreter when nothing is declared."""
    assert get_python_run_command("kitaru.job", ["score"], []) == (
        f"{sys.executable} -m kitaru.job score"
    )


def test_get_python_run_command_with_dependencies() -> None:
    """Quote the declared dependencies into the uv command."""
    assert (
        get_python_run_command("kitaru.job", ["score"], ["httpx>=0.27", "orjson"])
        == "uv run --with 'httpx>=0.27' --with orjson python -m kitaru.job score"
    )
