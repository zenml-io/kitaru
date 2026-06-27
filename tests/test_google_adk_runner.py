"""Runner wrapper tests for the Google ADK adapter."""

from __future__ import annotations

import importlib
from typing import Any

import pytest

from google_adk_fakes import install_fake_google_adk, purge_google_adk_adapter_modules
from kitaru.errors import KitaruUsageError


def _modules(monkeypatch: pytest.MonkeyPatch):
    purge_google_adk_adapter_modules(monkeypatch)
    install_fake_google_adk(monkeypatch)
    adapter = importlib.import_module("kitaru.adapters.google_adk")
    agent_module = importlib.import_module("kitaru.adapters.google_adk._agent")
    return adapter, agent_module


class FakeRunner:
    name = "fake_runner"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def run(self, *, user_id: str, session_id: str, new_message: Any, **kwargs: Any):
        self.calls.append(
            {
                "user_id": user_id,
                "session_id": session_id,
                "new_message": new_message,
                "kwargs": kwargs,
            }
        )
        return [{"final_output": f"echo:{new_message}"}]


class AsyncOnlyRunner:
    name = "async_runner"

    async def run_async(self, *, user_id: str, session_id: str, new_message: Any):
        yield {"final_output": new_message}


def test_runner_call_wraps_sync_runner_in_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, agent_module = _modules(monkeypatch)
    runner = FakeRunner()
    checkpoint_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(agent_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module.runtime, "is_inside_checkpoint", lambda: False)

    def fake_checkpoint(**kwargs: Any) -> Any:
        checkpoint_calls.append(kwargs)
        return kwargs["body"]()

    monkeypatch.setattr(agent_module, "run_sync_in_checkpoint", fake_checkpoint)

    wrapped = adapter.KitaruADKRunner(runner, checkpoint_strategy="runner_call")
    result = wrapped.run_sync(
        adapter.ADKRunRequest(user_id="u", session_id="s", message="hi")
    )

    assert result.status == "completed"
    assert result.final_output == "echo:hi"
    assert result.event_log_artifact_name is None
    assert result.run_summary_artifact_name is None
    assert (
        checkpoint_calls[0]["checkpoint_inputs"]["adk_input"]["adapter"] == "google_adk"
    )
    assert runner.calls[0]["new_message"] == "hi"


def test_runner_call_outside_flow_calls_runner_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, agent_module = _modules(monkeypatch)
    runner = FakeRunner()

    monkeypatch.setattr(agent_module.runtime, "is_inside_flow", lambda: False)
    monkeypatch.setattr(agent_module.runtime, "is_inside_checkpoint", lambda: False)

    wrapped = adapter.KitaruADKRunner(runner, checkpoint_strategy="runner_call")
    result = wrapped.run_sync(
        adapter.ADKRunRequest(user_id="u", session_id="s", message="direct")
    )

    assert result.final_output == "echo:direct"
    assert runner.calls[0]["kwargs"] == {}


def test_calls_mode_defaults_to_clear_usage_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, agent_module = _modules(monkeypatch)
    runner = FakeRunner()

    monkeypatch.setattr(agent_module.runtime, "is_inside_flow", lambda: False)
    monkeypatch.setattr(agent_module.runtime, "is_inside_checkpoint", lambda: False)

    wrapped = adapter.KitaruADKRunner(runner, checkpoint_strategy="calls")

    with pytest.raises(KitaruUsageError, match="before/after/error observation only"):
        wrapped.run_sync(
            adapter.ADKRunRequest(user_id="u", session_id="s", message="calls")
        )


def test_calls_mode_metadata_policy_rejects_runner_plugin_injection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, agent_module = _modules(monkeypatch)
    runner = FakeRunner()
    request = adapter.ADKRunRequest(
        user_id="u",
        session_id="s",
        message="calls",
        run_kwargs={"plugins": ["existing"]},
    )

    monkeypatch.setattr(agent_module.runtime, "is_inside_flow", lambda: False)
    monkeypatch.setattr(agent_module.runtime, "is_inside_checkpoint", lambda: False)

    policy = adapter.ADKCallCheckpointPolicy(require_true_call_hooks=False)
    wrapped = adapter.KitaruADKRunner(
        runner,
        checkpoint_strategy="calls",
        call_checkpoint_policy=policy,
    )

    with pytest.raises(KitaruUsageError, match="verified public ADK runner signatures"):
        wrapped.run_sync(request)

    assert runner.calls == []
    assert request.run_kwargs == {"plugins": ["existing"]}


def test_run_sync_rejects_async_only_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter, _agent_module = _modules(monkeypatch)
    wrapped = adapter.KitaruADKRunner(AsyncOnlyRunner())

    with pytest.raises(KitaruUsageError, match="only exposes `run_async"):
        wrapped.run_sync(
            adapter.ADKRunRequest(user_id="u", session_id="s", message="x")
        )
