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
"""Tests for the agent-facing root accessors."""

import json
import uuid

import httpx
import pytest

from kitaru.api_models.v1.task import AgentTaskDetails, TaskKind, TaskSpecResponse
from kitaru.task import get_task_id, get_task_inputs


def _agent_spec(task_id: str, inputs: object) -> TaskSpecResponse:
    return TaskSpecResponse(
        task_id=uuid.UUID(task_id),
        kind=TaskKind.AGENT,
        timeout_seconds=60,
        env={},
        secret_env={},
        details=AgentTaskDetails(
            agent_id=uuid.uuid4(),
            agent_version_id=uuid.uuid4(),
            inputs=inputs,
        ),
    )


def test_get_task_id_present(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return the task id when KITARU_TASK_ID is set."""
    monkeypatch.setenv("KITARU_TASK_ID", "task-123")
    assert get_task_id() == "task-123"


def test_get_task_id_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return None outside task mode."""
    monkeypatch.delenv("KITARU_TASK_ID", raising=False)
    assert get_task_id() is None


def test_get_task_inputs_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return None outside task mode."""
    monkeypatch.delenv("KITARU_TASK_ID", raising=False)
    assert get_task_inputs() is None


def test_get_task_inputs_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return the JSON-decoded KITARU_TASK_INPUTS without a spec fetch."""
    monkeypatch.setenv("KITARU_TASK_ID", "task-123")
    monkeypatch.setenv("KITARU_TASK_INPUTS", '{"prompt": "hi"}')

    def _unexpected_get(*args: object, **kwargs: object) -> httpx.Response:
        raise AssertionError("should not fetch the spec when inputs are inlined")

    monkeypatch.setattr(httpx, "get", _unexpected_get)
    assert get_task_inputs() == {"prompt": "hi"}


def test_get_task_inputs_spec_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fetch the spec with a bare synchronous request when inputs are unset."""
    task_id = str(uuid.uuid4())
    monkeypatch.setenv("KITARU_TASK_ID", task_id)
    monkeypatch.delenv("KITARU_TASK_INPUTS", raising=False)
    monkeypatch.setenv("KITARU_API_URL", "http://server.test")
    monkeypatch.setenv("KITARU_API_TOKEN", "secret-key")

    spec = _agent_spec(task_id, {"prompt": "hi"})
    seen: dict[str, object] = {}

    def _fake_get(url: str, headers: dict[str, str]) -> httpx.Response:
        seen["url"] = url
        seen["headers"] = headers
        return httpx.Response(
            200, json=spec.model_dump(mode="json"), request=httpx.Request("GET", url)
        )

    monkeypatch.setattr(httpx, "get", _fake_get)
    assert get_task_inputs() == {"prompt": "hi"}
    assert seen["url"] == f"http://server.test/v1/tasks/{task_id}/spec"
    assert seen["headers"] == {"Authorization": "Bearer secret-key"}


def test_get_task_inputs_requires_api_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise RuntimeError when the fallback is needed but the API URL is unset."""
    monkeypatch.setenv("KITARU_TASK_ID", "task-123")
    monkeypatch.delenv("KITARU_TASK_INPUTS", raising=False)
    monkeypatch.delenv("KITARU_API_URL", raising=False)
    with pytest.raises(RuntimeError, match="KITARU_API_URL"):
        get_task_inputs()


def test_get_task_inputs_invalid_json_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Surface malformed KITARU_TASK_INPUTS instead of swallowing the error."""
    monkeypatch.setenv("KITARU_TASK_ID", "task-123")
    monkeypatch.setenv("KITARU_TASK_INPUTS", "{")
    with pytest.raises(json.JSONDecodeError):
        get_task_inputs()
