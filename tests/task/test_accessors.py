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
"""Tests for agent-facing task accessors."""

import asyncio
import json

import httpx
import pytest

from kitaru.task import get_task_id, get_task_inputs


def test_accessors_return_none_outside_task_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return no task context when the task id is absent."""
    monkeypatch.delenv("KITARU_TASK_ID", raising=False)
    monkeypatch.setenv("KITARU_TASK_INPUTS", '{"ignored": true}')
    assert get_task_id() is None
    assert get_task_inputs() is None


def test_inputs_are_read_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Decode inline task inputs without making an API request."""
    monkeypatch.setenv("KITARU_TASK_ID", "task-1")
    monkeypatch.setenv("KITARU_TASK_INPUTS", '{"prompt": "hello"}')
    monkeypatch.setattr(
        httpx,
        "get",
        lambda *args, **kwargs: pytest.fail("unexpected fallback request"),
    )
    assert get_task_id() == "task-1"
    assert get_task_inputs() == {"prompt": "hello"}


def test_inputs_fallback_fetches_task_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fetch large inputs synchronously from the task spec."""
    monkeypatch.setenv("KITARU_TASK_ID", "task-1")
    monkeypatch.delenv("KITARU_TASK_INPUTS", raising=False)
    monkeypatch.setenv("KITARU_API_URL", "https://api.example.test/")
    monkeypatch.setenv("KITARU_API_KEY", "secret")
    request: dict[str, object] = {}

    def get(url: str, **kwargs: object) -> httpx.Response:
        request["url"] = url
        request.update(kwargs)
        return httpx.Response(
            200,
            json={"details": {"kind": "agent", "inputs": {"large": True}}},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(httpx, "get", get)

    async def read_inside_loop() -> object:
        return get_task_inputs()

    assert asyncio.run(read_inside_loop()) == {"large": True}
    assert request == {
        "url": "https://api.example.test/v1/tasks/task-1/spec",
        "headers": {"Authorization": "Bearer secret"},
    }


def test_inputs_fallback_requires_api_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject an incomplete task process contract."""
    monkeypatch.setenv("KITARU_TASK_ID", "task-1")
    monkeypatch.delenv("KITARU_TASK_INPUTS", raising=False)
    monkeypatch.delenv("KITARU_API_URL", raising=False)
    with pytest.raises(RuntimeError, match="KITARU_API_URL is not set"):
        get_task_inputs()


def test_invalid_inline_inputs_are_not_hidden(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Surface malformed JSON supplied by the worker."""
    monkeypatch.setenv("KITARU_TASK_ID", "task-1")
    monkeypatch.setenv("KITARU_TASK_INPUTS", "{")
    with pytest.raises(json.JSONDecodeError):
        get_task_inputs()
