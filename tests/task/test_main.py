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
"""Tests for the task process entrypoint."""

import pytest

from kitaru.task import __main__ as task_main


@pytest.mark.parametrize("kind", ["evaluate", "import"])
def test_main_runs_selected_kind(monkeypatch: pytest.MonkeyPatch, kind: str) -> None:
    """Dispatch a supported kind with the required task id."""
    monkeypatch.setenv("KITARU_TASK_ID", "task-1")
    calls: list[tuple[str, str]] = []

    async def run(selected_kind: str, task_id: str) -> None:
        calls.append((selected_kind, task_id))

    monkeypatch.setattr(task_main, "_run", run)
    assert task_main.main([kind]) == 0
    assert calls == [(kind, "task-1")]


@pytest.mark.parametrize("arguments", [[], ["unknown"], ["evaluate", "extra"]])
def test_main_rejects_invalid_arguments(arguments: list[str], capsys) -> None:
    """Return one and explain the accepted process kinds."""
    assert task_main.main(arguments) == 1
    assert "evaluate" in capsys.readouterr().err


def test_main_reports_missing_task_id(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Return one when the worker omitted the task id contract."""
    monkeypatch.delenv("KITARU_TASK_ID", raising=False)
    assert task_main.main(["evaluate"]) == 1
    assert "KITARU_TASK_ID is not set" in capsys.readouterr().err


def test_main_reports_flow_failure(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Print a flow error to stderr for the worker to capture."""
    monkeypatch.setenv("KITARU_TASK_ID", "task-1")

    async def run(kind: str, task_id: str) -> None:
        raise RuntimeError("plugin failed")

    monkeypatch.setattr(task_main, "_run", run)
    assert task_main.main(["evaluate"]) == 1
    assert capsys.readouterr().err == "plugin failed\n"
