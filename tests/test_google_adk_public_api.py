"""Public API and model tests for the Google ADK adapter."""

from __future__ import annotations

import importlib
import sys
from types import ModuleType

import pytest
from pydantic import ValidationError


def _adapter(monkeypatch: pytest.MonkeyPatch):
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.google_adk"):
            monkeypatch.delitem(sys.modules, cached, raising=False)
    google = ModuleType("google")
    google.__path__ = []  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.adk", ModuleType("google.adk"))
    return importlib.import_module("kitaru.adapters.google_adk")


def test_public_exports(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = _adapter(monkeypatch)

    expected = {
        "ADKAdapterEvent",
        "ADKCallCheckpointPolicy",
        "ADKCapturePolicy",
        "ADKEventError",
        "ADKRunEvent",
        "ADKRunRequest",
        "ADKRunResult",
        "ADKUsageSummary",
        "CheckpointConfig",
        "CheckpointRuntime",
        "CheckpointStrategy",
        "KitaruADKModel",
        "KitaruADKPlugin",
        "KitaruADKRunner",
        "KitaruADKTool",
        "ToolCheckpointOverride",
        "validate_checkpoint_strategy",
        "wrap_tool",
    }
    assert set(adapter.__all__) == expected


def test_request_validation_rejects_empty_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = _adapter(monkeypatch)

    with pytest.raises(ValidationError, match="stable non-empty string"):
        adapter.ADKRunRequest(user_id="", session_id="session", message="hello")


def test_request_copies_default_dicts(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = _adapter(monkeypatch)

    first = adapter.ADKRunRequest(user_id="u", session_id="s", message="one")
    second = adapter.ADKRunRequest(user_id="u", session_id="s", message="two")
    first.run_kwargs["x"] = 1

    assert second.run_kwargs == {}


def test_result_and_usage_models_validate(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = _adapter(monkeypatch)

    usage = adapter.ADKUsageSummary(
        model_name="gemini-test",
        input_tokens=3,
        output_tokens=4,
        total_tokens=7,
    )
    result = adapter.ADKRunResult(
        status="completed",
        final_output="done",
        events=[{"kind": "fake"}],
        usage=usage,
    )

    assert result.usage.total_tokens == 7
    assert result.final_output == "done"
