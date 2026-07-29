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
"""Tests for env reading and result-file writing."""

import json
from pathlib import Path

import pytest
from pydantic import BaseModel

from kitaru.task.task_io import get_required_env, write_task_result


class _Thing(BaseModel):
    """Plain model used to exercise write_task_result."""

    name: str
    count: int


def test_get_required_env_returns_value(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return the value of a set variable."""
    monkeypatch.setenv("SOME_VAR", "value")
    assert get_required_env("SOME_VAR") == "value"


def test_get_required_env_raises_on_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise RuntimeError when the variable is unset."""
    monkeypatch.delenv("SOME_VAR", raising=False)
    with pytest.raises(RuntimeError, match="SOME_VAR"):
        get_required_env("SOME_VAR")


def test_get_required_env_raises_on_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise RuntimeError when the variable is set but empty."""
    monkeypatch.setenv("SOME_VAR", "")
    with pytest.raises(RuntimeError, match="SOME_VAR"):
        get_required_env("SOME_VAR")


def test_write_task_result_model(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Encode a single BaseModel via model_dump."""
    path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(path))
    write_task_result(_Thing(name="a", count=1))
    assert json.loads(path.read_text()) == {"name": "a", "count": 1}


def test_write_task_result_list_of_models(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Encode a list of BaseModels, each via model_dump."""
    path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(path))
    write_task_result([_Thing(name="a", count=1), _Thing(name="b", count=2)])
    assert json.loads(path.read_text()) == [
        {"name": "a", "count": 1},
        {"name": "b", "count": 2},
    ]


def test_write_task_result_plain_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Encode a plain JSON value unchanged."""
    path = tmp_path / "result.json"
    monkeypatch.setenv("KITARU_TASK_RESULT_PATH", str(path))
    write_task_result({"created": 3, "failures": []})
    assert json.loads(path.read_text()) == {"created": 3, "failures": []}


def test_write_task_result_missing_env_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise RuntimeError when KITARU_TASK_RESULT_PATH is unset."""
    monkeypatch.delenv("KITARU_TASK_RESULT_PATH", raising=False)
    with pytest.raises(RuntimeError, match="KITARU_TASK_RESULT_PATH"):
        write_task_result({"a": 1})
