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
"""Tests for task environment and result-file I/O."""

import json
from datetime import UTC, datetime

import pytest
from pydantic import BaseModel

from kitaru.task.task_io import get_required_env, write_task_result


class ExampleResult(BaseModel):
    """Model used to verify JSON-mode serialization."""

    happened_at: datetime


@pytest.mark.parametrize("value", [None, ""])
def test_required_env_rejects_missing_or_empty(
    monkeypatch: pytest.MonkeyPatch, value: str | None
) -> None:
    """Reject missing and empty contract variables equally."""
    if value is None:
        monkeypatch.delenv("REQUIRED", raising=False)
    else:
        monkeypatch.setenv("REQUIRED", value)
    with pytest.raises(RuntimeError, match="REQUIRED is not set"):
        get_required_env("REQUIRED")


def test_required_env_returns_value(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return a nonempty contract value unchanged."""
    monkeypatch.setenv("REQUIRED", "value")
    assert get_required_env("REQUIRED") == "value"


def test_write_task_result_serializes_models_and_plain_json(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """Write lists containing models through Pydantic JSON mode."""
    result_path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(result_path))
    write_task_result(
        [
            ExampleResult(happened_at=datetime(2026, 7, 29, 12, tzinfo=UTC)),
            {"plain": True},
        ]
    )
    assert json.loads(result_path.read_text()) == [
        {"happened_at": "2026-07-29T12:00:00Z"},
        {"plain": True},
    ]


def test_write_task_result_requires_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a missing result-file contract."""
    monkeypatch.delenv("KITARU_TASK_RESULT_PATH", raising=False)
    with pytest.raises(RuntimeError, match="KITARU_TASK_RESULT_PATH is not set"):
        write_task_result({})
