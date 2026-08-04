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
"""Tests for subprocess supervision and process/environment building."""

import asyncio
import sys
import time
import uuid
from pathlib import Path

import pytest

from kitaru.worker.process import (
    TailBuffer,
    TaskProcess,
    build_process_env,
    get_python_run_command,
    parse_inline_dependencies,
    run_task_process,
)


def test_tail_buffer_keeps_only_the_most_recent_bytes() -> None:
    """Writing past the bound drops the oldest bytes first."""
    buffer = TailBuffer(max_bytes=5)
    buffer.write(b"abc")
    buffer.write(b"defgh")
    assert buffer.decode() == "defgh"


def test_tail_buffer_decodes_invalid_utf8_with_replacement() -> None:
    """Invalid UTF-8 bytes decode using the replacement character."""
    buffer = TailBuffer(max_bytes=10)
    buffer.write(b"\xff\xfe")
    assert "�" in buffer.decode()


async def test_run_task_process_captures_stdout_and_exit_code() -> None:
    """A successful command reports its exit code and stdout tail."""
    process = TaskProcess(
        command="echo hello", working_dir=None, env={}, timeout_seconds=5
    )
    result = await run_task_process(process, asyncio.Event())
    assert result.returncode == 0
    assert "stdout tail:\nhello" in result.tail


async def test_run_task_process_captures_stderr() -> None:
    """Stderr output is captured into its own labeled tail section."""
    process = TaskProcess(
        command="echo oops 1>&2", working_dir=None, env={}, timeout_seconds=5
    )
    result = await run_task_process(process, asyncio.Event())
    assert result.returncode == 0
    assert "stderr tail:\noops" in result.tail
    assert "stdout tail:" not in result.tail


async def test_run_task_process_nonzero_exit() -> None:
    """A failing command reports its nonzero exit code."""
    process = TaskProcess(command="exit 3", working_dir=None, env={}, timeout_seconds=5)
    result = await run_task_process(process, asyncio.Event())
    assert result.returncode == 3


async def test_run_task_process_empty_tail_when_nothing_captured() -> None:
    """A silent command reports an empty tail."""
    process = TaskProcess(command="true", working_dir=None, env={}, timeout_seconds=5)
    result = await run_task_process(process, asyncio.Event())
    assert result.tail == ""


async def test_run_task_process_killed_on_timeout() -> None:
    """A command outliving its timeout is killed and reports no exit code."""
    process = TaskProcess(
        command="sleep 5", working_dir=None, env={}, timeout_seconds=0
    )
    started = time.monotonic()
    result = await run_task_process(process, asyncio.Event())
    elapsed = time.monotonic() - started
    assert result.returncode is None
    assert elapsed < 4


async def test_run_task_process_killed_on_cancel() -> None:
    """Setting the cancel event kills a long-running command early."""
    canceled = asyncio.Event()
    process = TaskProcess(
        command="sleep 30", working_dir=None, env={}, timeout_seconds=30
    )
    run = asyncio.create_task(run_task_process(process, canceled))
    await asyncio.sleep(0.2)
    canceled.set()
    started = time.monotonic()
    result = await run
    elapsed = time.monotonic() - started
    assert result.returncode is None
    assert elapsed < 5


async def test_run_task_process_records_exit_before_a_later_cancel() -> None:
    """An exit recorded before the cancel event fires wins over the cancel."""
    canceled = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.call_later(0.1, canceled.set)
    process = TaskProcess(command="exit 7", working_dir=None, env={}, timeout_seconds=5)
    result = await run_task_process(process, canceled)
    assert result.returncode == 7


async def test_run_task_process_kills_the_whole_process_group() -> None:
    """Killing the process group also stops a child the command spawns."""
    canceled = asyncio.Event()
    process = TaskProcess(
        command="sh -c 'sleep 30 & wait'",
        working_dir=None,
        env={},
        timeout_seconds=30,
    )
    run = asyncio.create_task(run_task_process(process, canceled))
    await asyncio.sleep(0.2)
    canceled.set()
    result = await asyncio.wait_for(run, timeout=5)
    assert result.returncode is None


def test_build_process_env_layers_and_strips_contract_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The env layers run/extra/secret env and re-asserts contract variables."""
    monkeypatch.setenv("KITARU_API_URL", "https://api.example.com")
    monkeypatch.setenv("KITARU_API_KEY", "worker-key")
    monkeypatch.setenv("SOME_INHERITED_VAR", "inherited")
    monkeypatch.setenv("KITARU_TASK_ID", "leaked-from-parent")

    task_id = uuid.uuid4()
    env = build_process_env(
        task_id,
        run_env={"RUN_VAR": "run"},
        extra_env={"KITARU_SESSION_NAME": "run-1"},
        secret_env={"SECRET_VAR": "secret"},
        token="task-token",
    )

    assert env["SOME_INHERITED_VAR"] == "inherited"
    assert env["RUN_VAR"] == "run"
    assert env["KITARU_SESSION_NAME"] == "run-1"
    assert env["SECRET_VAR"] == "secret"
    assert env["KITARU_API_URL"] == "https://api.example.com"
    assert env["KITARU_API_TOKEN"] == "task-token"
    assert env["KITARU_TASK_ID"] == str(task_id)
    assert "KITARU_API_KEY" not in env


def test_build_process_env_extras_cannot_override_contract_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A contract variable smuggled through a layer is cleared and reset."""
    monkeypatch.setenv("KITARU_API_URL", "https://api.example.com")
    monkeypatch.delenv("KITARU_API_KEY", raising=False)

    task_id = uuid.uuid4()
    env = build_process_env(
        task_id,
        run_env={"KITARU_API_URL": "https://evil.example.com"},
        extra_env={"KITARU_TASK_ID": "spoofed", "KITARU_API_TOKEN": "spoofed-token"},
        secret_env={"KITARU_API_KEY": "spoofed-key"},
        token="task-token",
    )

    assert env["KITARU_API_URL"] == "https://api.example.com"
    assert env["KITARU_TASK_ID"] == str(task_id)
    assert env["KITARU_API_TOKEN"] == "task-token"
    assert "KITARU_API_KEY" not in env


def test_build_process_env_never_sets_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """KITARU_API_TOKEN is set and KITARU_API_KEY is absent, even when inherited."""
    monkeypatch.setenv("KITARU_API_URL", "https://api.example.com")

    monkeypatch.delenv("KITARU_API_KEY", raising=False)
    env = build_process_env(uuid.uuid4(), {}, {}, {}, token="task-token")
    assert env["KITARU_API_TOKEN"] == "task-token"
    assert "KITARU_API_KEY" not in env

    monkeypatch.setenv("KITARU_API_KEY", "worker-key")
    env = build_process_env(uuid.uuid4(), {}, {}, {}, token="task-token")
    assert "KITARU_API_KEY" not in env


def test_parse_inline_dependencies_reads_the_script_block(tmp_path: Path) -> None:
    """A PEP 723 script block yields its declared dependencies."""
    script = tmp_path / "plugin.py"
    script.write_text(
        "# /// script\n"
        "# dependencies = [\n"
        '#   "requests<3",\n'
        '#   "rich",\n'
        "# ]\n"
        "# ///\n"
        "\n"
        "def evaluate(session, **params):\n"
        "    pass\n"
    )
    assert parse_inline_dependencies(script) == ["requests<3", "rich"]


def test_parse_inline_dependencies_returns_empty_without_a_block(
    tmp_path: Path,
) -> None:
    """A file with no script metadata block yields no dependencies."""
    script = tmp_path / "plugin.py"
    script.write_text("def evaluate(session, **params):\n    pass\n")
    assert parse_inline_dependencies(script) == []


def test_parse_inline_dependencies_rejects_multiple_blocks(tmp_path: Path) -> None:
    """More than one script metadata block raises ValueError."""
    script = tmp_path / "plugin.py"
    script.write_text(
        "# /// script\n"
        "# dependencies = []\n"
        "# ///\n"
        "\n"
        "# /// script\n"
        "# dependencies = []\n"
        "# ///\n"
    )
    with pytest.raises(ValueError, match="Multiple inline script metadata blocks"):
        parse_inline_dependencies(script)


def test_get_python_run_command_without_dependencies() -> None:
    """A bare module invocation runs directly under sys.executable."""
    command = get_python_run_command("kitaru.task", ["evaluate"], [])
    assert command == f"{sys.executable} -m kitaru.task evaluate"


def test_get_python_run_command_with_dependencies() -> None:
    """Dependencies route the invocation through uv run --with."""
    command = get_python_run_command(
        "kitaru.task", ["import"], ["requests<3", "rich>=13"]
    )
    assert command == (
        "uv run --with 'requests<3' --with 'rich>=13' python -m kitaru.task import"
    )
