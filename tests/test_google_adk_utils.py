"""Validation helper tests for the Google ADK adapter."""

from __future__ import annotations

import importlib
import sys
from types import ModuleType

import pytest

from kitaru.errors import KitaruUsageError


def _utils(monkeypatch: pytest.MonkeyPatch):
    google = ModuleType("google")
    google.__path__ = []  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.adk", ModuleType("google.adk"))
    return importlib.import_module("kitaru.adapters.google_adk._utils")


def test_valid_checkpoint_strategies(monkeypatch: pytest.MonkeyPatch) -> None:
    utils = _utils(monkeypatch)

    assert utils.validate_checkpoint_strategy("runner_call") == "runner_call"
    assert utils.validate_checkpoint_strategy("calls") == "calls"


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("turn", "runner_call"),
        ("interaction", "another adapter"),
        ("model_call", "checkpoint_strategy='calls'"),
        ("wat", "Expected one of"),
    ],
)
def test_invalid_checkpoint_strategy_messages(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
    message: str,
) -> None:
    utils = _utils(monkeypatch)

    with pytest.raises(KitaruUsageError, match=message):
        utils.validate_checkpoint_strategy(value)


def test_checkpoint_config_rejects_isolated_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    utils = _utils(monkeypatch)

    with pytest.raises(KitaruUsageError, match="runtime='isolated'"):
        utils.validate_checkpoint_config(
            {"runtime": "isolated"},
            context="run_checkpoint_config",
        )


def test_checkpoint_config_validates_cache_and_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    utils = _utils(monkeypatch)

    assert utils.validate_checkpoint_config(
        {"runtime": "inline", "cache": True, "retries": 1},
        context="run_checkpoint_config",
    ) == {"runtime": "inline", "cache": True, "retries": 1}

    with pytest.raises(KitaruUsageError, match="cache must be a boolean"):
        utils.validate_checkpoint_config({"cache": "yes"}, context="config")


def test_tool_checkpoint_overrides_validate_tool_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    utils = _utils(monkeypatch)

    assert utils.validate_tool_checkpoint_overrides(
        {"search": False, "lookup": {"type": "tool_call"}},
        context="tool_checkpoint_config_by_name",
    ) == {"search": False, "lookup": {"type": "tool_call"}}

    with pytest.raises(KitaruUsageError, match="non-empty tool name"):
        utils.validate_tool_checkpoint_overrides(
            {"": {}},
            context="tool_checkpoint_config_by_name",
        )
