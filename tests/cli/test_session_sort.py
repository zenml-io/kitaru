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
"""CLI tests for session list sorting."""

import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.api_models.v1.session import SessionListParams
from kitaru.cli import app as app_module
from kitaru.cli import invocation_context


@dataclass
class _StubSessionModel:
    id: uuid.UUID
    values: dict[str, Any] = field(default_factory=dict)

    def model_dump(self, *, mode: str) -> dict[str, Any]:
        assert mode == "json"
        return {"id": str(self.id), **self.values}


class _RecordingSessions:
    def __init__(self) -> None:
        self.session = _StubSessionModel(uuid.uuid4(), {"name": "demo"})
        self.list_calls: list[SessionListParams] = []

    async def list(self, params: SessionListParams) -> Any:
        self.list_calls.append(params)
        return SimpleNamespace(items=[self.session], next_cursor=None)


@pytest.mark.parametrize(
    "sort",
    [
        "created:asc",
        "created:desc",
        "started_at:asc",
        "started_at:desc",
        "number:asc",
        "number:desc",
    ],
)
def test_session_list_accepts_supported_sort_fields(
    sort: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Forward every supported session sort field and direction to the API client."""
    resource = _RecordingSessions()
    client = SimpleNamespace(sessions=resource)

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(invocation_context, "open_asset_client", fake_open_client)

    assert app_module.main(["session", "list", "--sort", sort]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "session.list"
    assert [call.sort for call in resource.list_calls] == [sort]


@pytest.mark.parametrize(
    "sort",
    [
        "ended_at:asc",
        "name:desc",
        "started_at",
        "number:up",
        "started_at:asc:extra",
    ],
)
def test_session_list_rejects_unsupported_sort_values(
    sort: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Reject unsupported session sorts locally before opening an API client."""

    def fail_open_client() -> Any:
        raise AssertionError("open_asset_client should not be called on invalid --sort")

    monkeypatch.setattr(invocation_context, "open_asset_client", fail_open_client)

    assert app_module.main(["session", "list", "--sort", sort]) == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"]["kind"] == "invalid_arguments"
    assert (
        payload["error"]["message"]
        == "--sort must use created, started_at, or number with :asc or :desc."
    )


@pytest.mark.parametrize(
    ("argv", "sort"),
    [
        (["agent", "list"], "started_at:asc"),
        (["agent", "list"], "number:desc"),
        (["import", "list"], "started_at:desc"),
    ],
)
def test_non_session_list_commands_keep_created_only_sort(
    argv: list[str],
    sort: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Non-session list commands still accept only created:asc and created:desc."""
    client = SimpleNamespace()

    @asynccontextmanager
    async def fake_open_client():
        yield client

    monkeypatch.setattr(invocation_context, "open_asset_client", fake_open_client)

    assert app_module.main([*argv, "--sort", sort]) == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"]["kind"] == "invalid_arguments"
    assert payload["error"]["message"] == "--sort must be created:asc or created:desc."


def test_session_list_schema_and_help_document_sort_fields(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Offline schema and --help document created, started_at, and number sorting."""
    assert app_module.main(["schema", "session", "list", "--output", "json"]) == 0
    [command] = json.loads(capsys.readouterr().out)["items"]
    sort_param = next(
        param for param in command["parameters"] if param["name"] == "--sort"
    )
    assert "created, started_at, or number" in sort_param["description"]
    assert ":asc or :desc" in sort_param["description"]

    assert app_module.main(["session", "list", "--help"]) == 0
    help_text = capsys.readouterr().out
    assert "created, started_at, or number" in help_text
    assert ":asc or :desc" in help_text
