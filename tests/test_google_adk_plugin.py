"""Plugin wrapper tests for the Google ADK adapter."""

from __future__ import annotations

import asyncio
import importlib
from typing import Any

import pytest

from google_adk_fakes import install_fake_google_adk, purge_google_adk_adapter_modules
from kitaru.errors import KitaruUsageError


def _modules(monkeypatch: pytest.MonkeyPatch):
    purge_google_adk_adapter_modules(monkeypatch)
    install_fake_google_adk(monkeypatch)
    adapter = importlib.import_module("kitaru.adapters.google_adk")
    plugin_module = importlib.import_module("kitaru.adapters.google_adk._plugin")
    return adapter, plugin_module


def test_model_proceed_callable_runs_inside_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, plugin_module = _modules(monkeypatch)
    checkpoint_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(plugin_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(plugin_module.runtime, "is_inside_checkpoint", lambda: False)

    def fake_checkpoint(**kwargs: Any) -> Any:
        checkpoint_calls.append(kwargs)
        return kwargs["body"]()

    monkeypatch.setattr(plugin_module, "run_sync_in_checkpoint", fake_checkpoint)

    plugin = adapter.KitaruADKPlugin(runner_name="runner")
    result = plugin.wrap_model_call(
        input_envelope={"prompt": "hi"},
        proceed=lambda: "model-result",
        model_name="gemini-test",
    )

    assert result == "model-result"
    assert checkpoint_calls[0]["checkpoint_inputs"]["model_input"] == {"prompt": "hi"}


def test_tool_proceed_callable_runs_inside_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, plugin_module = _modules(monkeypatch)
    checkpoint_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(plugin_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(plugin_module.runtime, "is_inside_checkpoint", lambda: False)

    def fake_checkpoint(**kwargs: Any) -> Any:
        checkpoint_calls.append(kwargs)
        return kwargs["body"]()

    monkeypatch.setattr(plugin_module, "run_sync_in_checkpoint", fake_checkpoint)

    plugin = adapter.KitaruADKPlugin(runner_name="runner")
    result = plugin.wrap_tool_call(
        input_envelope={"query": "hi"},
        proceed=lambda: {"ok": True},
        tool_name="search",
    )

    assert result == {"ok": True}
    assert checkpoint_calls[0]["checkpoint_inputs"]["tool_args"] == {"query": "hi"}


def test_disabled_tool_override_calls_proceed_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, plugin_module = _modules(monkeypatch)
    checkpoint_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(plugin_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(plugin_module.runtime, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(
        plugin_module,
        "run_sync_in_checkpoint",
        lambda **kwargs: checkpoint_calls.append(kwargs),
    )

    policy = adapter.ADKCallCheckpointPolicy(
        tool_checkpoint_config_by_name={"search": False}
    )
    plugin = adapter.KitaruADKPlugin(runner_name="runner", call_policy=policy)

    assert (
        plugin.wrap_tool_call(
            input_envelope={"query": "hi"},
            proceed=lambda: "direct",
            tool_name="search",
        )
        == "direct"
    )
    assert checkpoint_calls == []


def test_metadata_only_nested_policy_does_not_open_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, plugin_module = _modules(monkeypatch)
    checkpoint_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(plugin_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(plugin_module.runtime, "is_inside_checkpoint", lambda: True)
    monkeypatch.setattr(
        plugin_module,
        "run_sync_in_checkpoint",
        lambda **kwargs: checkpoint_calls.append(kwargs),
    )

    policy = adapter.ADKCallCheckpointPolicy(nested_checkpoint_policy="metadata_only")
    plugin = adapter.KitaruADKPlugin(runner_name="runner", call_policy=policy)

    assert (
        plugin.wrap_model_call(
            input_envelope={"prompt": "hi"},
            proceed=lambda: "direct",
        )
        == "direct"
    )
    assert checkpoint_calls == []


def test_plugin_only_callback_raises_without_observation_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _plugin_module = _modules(monkeypatch)
    plugin = adapter.KitaruADKPlugin(runner_name="runner")

    with pytest.raises(KitaruUsageError, match="before/after/error observation only"):
        asyncio.run(plugin.before_model_callback(llm_request={"prompt": "hi"}))


def test_observation_only_plugin_callback_records_metadata_without_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _plugin_module = _modules(monkeypatch)
    tracker = importlib.import_module(
        "kitaru.adapters.google_adk._tracking"
    ).EventTracker("runner")
    plugin = adapter.KitaruADKPlugin(
        runner_name="runner",
        tracker=tracker,
        observation_only=True,
    )

    asyncio.run(plugin.before_model_callback(llm_request={"prompt": "hi"}))

    assert len(tracker.events) == 1
    assert tracker.events[0].status == "metadata_only"
