"""Task process helper and supervision tests."""

import asyncio
import os
import shlex
import sys
import uuid
from typing import cast

import pytest

from kitaru.worker.process import (
    TaskProcess,
    build_process_env,
    get_python_run_command,
    parse_inline_dependencies,
    run_task_process,
)


def test_build_process_env_layers_values_and_protects_contract(monkeypatch) -> None:
    task_id = uuid.uuid4()
    monkeypatch.setenv("INHERITED", "yes")
    monkeypatch.setenv("KITARU_API_URL", "https://worker")
    monkeypatch.setenv("KITARU_API_KEY", "worker-key")
    monkeypatch.setenv("KITARU_TASK_PLUGIN_PATH", "stale")

    env = build_process_env(
        task_id,
        {
            "VALUE": "run",
            "KITARU_API_URL": "bad-run",
            "KITARU_TASK_INPUTS": "bad",
        },
        {"VALUE": "extra", "KITARU_API_KEY": "bad-extra"},
        {"VALUE": "secret", "KITARU_TASK_RESULT_PATH": "bad"},
    )

    assert env["INHERITED"] == "yes"
    assert env["VALUE"] == "secret"
    assert env["KITARU_API_URL"] == "https://worker"
    assert env["KITARU_API_KEY"] == "worker-key"
    assert env["KITARU_TASK_ID"] == str(task_id)
    assert "KITARU_TASK_INPUTS" not in env
    assert "KITARU_TASK_PLUGIN_PATH" not in env
    assert "KITARU_TASK_RESULT_PATH" not in env


def test_parse_inline_dependencies_uses_pep_723_comment_rules(tmp_path) -> None:
    script = tmp_path / "plugin.py"
    script.write_text(
        "\n".join(
            [
                "# /// script",
                '# requires-python = ">=3.11"',
                "#",
                '# dependencies = ["httpx>=0.27", "pkg; python_version < \'3.14\'"]',
                "# ///",
                "print('ok')",
            ]
        ),
        encoding="utf-8",
    )

    assert parse_inline_dependencies(script) == [
        "httpx>=0.27",
        "pkg; python_version < '3.14'",
    ]


def test_parse_inline_dependencies_rejects_multiple_blocks(tmp_path) -> None:
    script = tmp_path / "plugin.py"
    block = "# /// script\n# dependencies = []\n# ///\n"
    script.write_text(block + block, encoding="utf-8")

    with pytest.raises(ValueError, match="more than one"):
        parse_inline_dependencies(script)


def test_python_command_uses_current_interpreter_without_dependencies() -> None:
    command = get_python_run_command("kitaru.task", ["evaluate"], [])

    assert shlex.split(command) == [
        sys.executable,
        "-m",
        "kitaru.task",
        "evaluate",
    ]


def test_python_command_uses_uv_with_each_dependency() -> None:
    command = get_python_run_command(
        "kitaru.task", ["import"], ["one==1", "two[extra]==2"]
    )

    assert shlex.split(command) == [
        "uv",
        "run",
        "--with",
        "one==1",
        "--with",
        "two[extra]==2",
        "python",
        "-m",
        "kitaru.task",
        "import",
    ]


async def test_process_captures_bounded_stdout_and_stderr_tails() -> None:
    command = f"{shlex.quote(sys.executable)} -c " + shlex.quote(
        "import sys; print('x' * 9000); print('stderr-value', file=sys.stderr)"
    )

    result = await run_task_process(
        TaskProcess(command, None, dict(os.environ), 5), asyncio.Event()
    )

    assert result.returncode == 0
    assert "stdout tail:\n" in result.tail
    assert "stderr tail:\nstderr-value" in result.tail
    assert result.tail.count("x") < 9000


async def test_process_returns_none_when_canceled() -> None:
    canceled = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.call_later(0.05, canceled.set)

    result = await run_task_process(
        TaskProcess("sleep 10", None, dict(os.environ), 5), canceled
    )

    assert result.returncode is None


async def test_cancel_kills_the_whole_process_group(tmp_path) -> None:
    sentinel = tmp_path / "child-finished"
    child = (
        "import pathlib,time; time.sleep(0.2); "
        f"pathlib.Path({str(sentinel)!r}).write_text('finished')"
    )
    command = f"{shlex.quote(sys.executable)} -c {shlex.quote(child)} & wait"
    canceled = asyncio.Event()
    asyncio.get_running_loop().call_later(0.05, canceled.set)

    result = await run_task_process(
        TaskProcess(command, None, dict(os.environ), 5), canceled
    )
    await asyncio.sleep(0.25)

    assert result.returncode is None
    assert not sentinel.exists()


async def test_recorded_exit_wins_over_later_cancel() -> None:
    canceled = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.call_later(0.1, canceled.set)

    result = await run_task_process(
        TaskProcess("exit 7", None, dict(os.environ), 5), canceled
    )

    assert result.returncode == 7


async def test_process_returns_none_on_timeout() -> None:
    result = await run_task_process(
        TaskProcess("sleep 10", None, dict(os.environ), cast(int, 0.05)),
        asyncio.Event(),
    )

    assert result.returncode is None
