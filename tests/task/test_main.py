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
"""Tests for the process entry point."""

import pytest

from kitaru.client.api_client import KitaruAPIClient
from kitaru.task import __main__ as task_main
from kitaru.task.__main__ import main


def test_main_rejects_bad_kind(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Exit 2 with an argparse error when the kind is not evaluate or import."""
    monkeypatch.setattr("sys.argv", ["kitaru.task", "bogus"])
    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 2
    assert "invalid choice" in capsys.readouterr().err


def test_main_missing_task_id_exits_one(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Exit 1 with the error on stderr when KITARU_TASK_ID is unset."""
    monkeypatch.setattr("sys.argv", ["kitaru.task", "evaluate"])
    monkeypatch.delenv("KITARU_TASK_ID", raising=False)
    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 1
    assert "KITARU_TASK_ID" in capsys.readouterr().err


def test_main_missing_api_url_exits_one(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Exit 1 with the error on stderr when KITARU_API_URL is unset."""
    monkeypatch.setattr("sys.argv", ["kitaru.task", "import"])
    monkeypatch.setenv("KITARU_TASK_ID", "task-123")
    monkeypatch.setenv("KITARU_API_TOKEN", "task-token")
    monkeypatch.delenv("KITARU_API_URL", raising=False)
    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 1
    assert "KITARU_API_URL" in capsys.readouterr().err


def test_main_missing_task_token_exits_one(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Exit 1 with the error on stderr when KITARU_API_TOKEN is unset."""
    monkeypatch.setattr("sys.argv", ["kitaru.task", "import"])
    monkeypatch.setenv("KITARU_TASK_ID", "task-123")
    monkeypatch.setenv("KITARU_API_URL", "http://server.test")
    monkeypatch.delenv("KITARU_API_TOKEN", raising=False)
    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 1
    assert "KITARU_API_TOKEN" in capsys.readouterr().err


@pytest.mark.parametrize("kind", ["analyze", "evaluate", "import"])
def test_main_runs_selected_kind(monkeypatch: pytest.MonkeyPatch, kind: str) -> None:
    """Dispatch the requested kind with the client and task id."""
    monkeypatch.setattr("sys.argv", ["kitaru.task", kind])
    monkeypatch.setenv("KITARU_TASK_ID", "task-123")
    monkeypatch.setenv("KITARU_API_URL", "http://server.test")
    monkeypatch.setenv("KITARU_API_TOKEN", "task-token")
    calls: list[tuple[str, str]] = []

    async def fake_run_flow(
        selected_kind: str, client: KitaruAPIClient, task_id: str
    ) -> None:
        calls.append((selected_kind, task_id))

    monkeypatch.setattr(task_main, "_run_flow", fake_run_flow)
    main()
    assert calls == [(kind, "task-123")]


def test_main_reports_flow_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Exit 1 with the flow's error on stderr when the flow raises."""
    monkeypatch.setattr("sys.argv", ["kitaru.task", "evaluate"])
    monkeypatch.setenv("KITARU_TASK_ID", "task-123")
    monkeypatch.setenv("KITARU_API_URL", "http://server.test")
    monkeypatch.setenv("KITARU_API_TOKEN", "task-token")

    async def failing_run_flow(
        kind: str, client: KitaruAPIClient, task_id: str
    ) -> None:
        raise RuntimeError("plugin failed")

    monkeypatch.setattr(task_main, "_run_flow", failing_run_flow)
    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 1
    assert "plugin failed" in capsys.readouterr().err
