"""Fake-SDK tests for Gemini interaction checkpoint semantics."""

from __future__ import annotations

import importlib
import sys
import types
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru.errors import KitaruRuntimeError, KitaruUsageError


@pytest.fixture
def gemini_adapter(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.gemini"):
            monkeypatch.delitem(sys.modules, cached, raising=False)
    google = types.ModuleType("google")
    google.__path__ = []  # type: ignore[attr-defined]
    genai = types.ModuleType("google.genai")
    google_module: Any = google
    google_module.genai = genai
    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.genai", genai)
    return importlib.import_module("kitaru.adapters.gemini")


class FakeInteractions:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = list(responses)
        self.create_calls: list[dict[str, Any]] = []
        self.get_calls: list[tuple[str, dict[str, Any]]] = []

    def create(self, **kwargs: Any) -> Any:
        self.create_calls.append(kwargs)
        return self.responses.pop(0)

    def get(self, id: str, **kwargs: Any) -> Any:
        self.get_calls.append((id, kwargs))
        return self.responses.pop(0)


class FakeClient:
    def __init__(self, responses: list[Any]) -> None:
        self.interactions = FakeInteractions(responses)


class ClientWithPublicState:
    def __init__(self) -> None:
        self.interactions = object()
        self.api_key = "secret"


def _completed_interaction(**updates: Any) -> SimpleNamespace:
    defaults = {
        "id": "interaction-1",
        "status": "completed",
        "previous_interaction_id": None,
        "model": "gemini-test",
        "agent": None,
        "outputs": [SimpleNamespace(type="text", text="hello from gemini")],
        "usage": SimpleNamespace(total_tokens=5),
    }
    defaults.update(updates)
    return SimpleNamespace(**defaults)


def _patch_flow_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> list[dict[str, Any]]:
    agent = importlib.import_module("kitaru.adapters.gemini._agent")
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(agent, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent, "is_inside_checkpoint", lambda: False)

    def fake_checkpoint(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return kwargs["body"]()

    monkeypatch.setattr(agent, "run_sync_in_checkpoint", fake_checkpoint)
    return calls


def test_run_sync_creates_one_synthetic_interaction_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    checkpoint_calls = _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient([_completed_interaction()])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello",
        model="gemini-test",
        generation_config={"temperature": 0.1},
    )

    result = runner.run_sync(request)

    assert len(checkpoint_calls) == 1
    assert checkpoint_calls[0]["config"]["type"] == "agent_call"
    assert checkpoint_calls[0]["step_name"] == "gemini_gemini_interaction"
    assert result.status == "completed"
    assert result.interaction_id == "interaction-1"
    assert result.output_text == "hello from gemini"
    assert result.steps[0].type == "text"
    assert result.usage == {"total_tokens": 5}
    assert "outputs rather than `steps`" in " ".join(result.warnings)
    create_kwargs = client.interactions.create_calls[0]
    assert create_kwargs["input"] == "hello"
    assert create_kwargs["model"] == "gemini-test"
    assert create_kwargs["generation_config"] == {"temperature": 0.1}
    assert "agent" not in create_kwargs


def test_requires_action_normalizes_function_call(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient(
        [
            _completed_interaction(
                status="requires_action",
                outputs=[
                    {
                        "type": "function_call",
                        "id": "call-1",
                        "name": "lookup",
                        "arguments": {"city": "Delft"},
                    }
                ],
                usage=None,
            )
        ]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "lookup weather",
        model="gemini-test",
        tools=[{"type": "function", "name": "lookup"}],
    )

    result = runner.run_sync(request)

    assert result.status == "requires_action"
    assert result.steps[0].type == "function_call"
    assert result.steps[0].call_id == "call-1"
    assert result.steps[0].tool_name == "lookup"


def test_function_result_request_constructs_matching_create_payload(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient(
        [_completed_interaction(previous_interaction_id="interaction-1")]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.function_result(
        previous_interaction_id="interaction-1",
        function_call_id="call-1",
        function_name="lookup",
        function_result={"answer": 42},
        model="gemini-test",
    )

    runner.run_sync(request)

    create_kwargs = client.interactions.create_calls[0]
    assert create_kwargs["previous_interaction_id"] == "interaction-1"
    assert create_kwargs["input"] == [
        {
            "type": "function_result",
            "call_id": "call-1",
            "name": "lookup",
            "result": {"answer": 42},
        }
    ]


def test_poll_fetches_existing_interaction_without_create(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient([_completed_interaction(id="interaction-existing")])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.poll("interaction-existing")

    result = runner.run_sync(request)

    assert result.interaction_id == "interaction-existing"
    assert client.interactions.create_calls == []
    assert client.interactions.get_calls == [("interaction-existing", {})]
    assert result.poll_count == 1


def test_background_polling_reuses_created_interaction_id(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    _patch_flow_checkpoint(monkeypatch, gemini_adapter)
    client = FakeClient(
        [
            _completed_interaction(id="background-1", status="in_progress"),
            _completed_interaction(id="background-1", status="completed"),
        ]
    )
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
        poll_interval_s=0.001,
    )
    request = gemini_adapter.GeminiInteractionRequest.start(
        "long task",
        agent="deep-research",
        background=True,
        timeout_s=0.01,
    )

    result = runner.run_sync(request)

    assert result.status == "completed"
    assert len(client.interactions.create_calls) == 1
    assert client.interactions.get_calls == [("background-1", {})]
    assert result.poll_count == 1


def test_nested_checkpoint_rejected_before_sdk_invocation(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    agent = importlib.import_module("kitaru.adapters.gemini._agent")
    monkeypatch.setattr(agent, "is_inside_checkpoint", lambda: True)
    client = FakeClient([_completed_interaction()])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
    )
    request = gemini_adapter.GeminiInteractionRequest.start("hello", model="m")

    with pytest.raises(KitaruUsageError, match="inside an existing Kitaru checkpoint"):
        runner.run_sync(request)

    assert client.interactions.create_calls == []


def test_direct_execution_inside_checkpoint_warns(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    agent = importlib.import_module("kitaru.adapters.gemini._agent")
    monkeypatch.setattr(agent, "is_inside_checkpoint", lambda: True)
    client = FakeClient([_completed_interaction()])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
        allow_direct_execution_inside_checkpoint=True,
    )
    request = gemini_adapter.GeminiInteractionRequest.start("hello", model="m")

    result = runner.run_sync(request)

    assert "ran directly inside an existing Kitaru checkpoint" in " ".join(
        result.warnings
    )
    assert len(client.interactions.create_calls) == 1


def test_cache_identity_for_client_is_shallow(
    gemini_adapter: types.ModuleType,
) -> None:
    serialization = importlib.import_module("kitaru.adapters.gemini._serialization")
    client = ClientWithPublicState()

    identity = serialization.to_cache_identity(client)

    assert list(identity) == ["python_type"]
    assert identity["python_type"].endswith(".ClientWithPublicState")


def test_request_manifest_redacts_secret_like_fields(
    gemini_adapter: types.ModuleType,
) -> None:
    serialization = importlib.import_module("kitaru.adapters.gemini._serialization")
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello",
        model="m",
        generation_config={"api_key": "secret", "temperature": 0.2},
        response_format={"authorization": "Bearer secret"},
    )

    manifest = serialization.redacted_request_manifest(
        request,
        client={"token": "secret"},
    )

    assert manifest["request"]["generation_config"]["api_key"] == "[REDACTED]"
    assert manifest["request"]["generation_config"]["temperature"] == 0.2
    assert manifest["request"]["response_format"]["authorization"] == "[REDACTED]"
    assert manifest["client"]["token"] == "[REDACTED]"


def test_capture_failures_are_non_fatal_by_default(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    agent = importlib.import_module("kitaru.adapters.gemini._agent")
    monkeypatch.setattr(agent, "is_inside_checkpoint", lambda: True)
    client = FakeClient([_completed_interaction()])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
        allow_direct_execution_inside_checkpoint=True,
    )
    monkeypatch.setattr(
        runner,
        "_save_artifact",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    request = gemini_adapter.GeminiInteractionRequest.start("hello", model="m")

    result = runner.run_sync(request)

    assert "artifact capture failed" in " ".join(result.warnings)
    assert result.metadata["capture_failures"]


def test_strict_capture_failure_raises_after_sdk_success(
    monkeypatch: pytest.MonkeyPatch,
    gemini_adapter: types.ModuleType,
) -> None:
    agent = importlib.import_module("kitaru.adapters.gemini._agent")
    monkeypatch.setattr(agent, "is_inside_checkpoint", lambda: True)
    client = FakeClient([_completed_interaction()])
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        client=client,
        capture=gemini_adapter.GeminiInteractionCapturePolicy(
            fail_on_artifact_capture_error=True
        ),
        allow_direct_execution_inside_checkpoint=True,
    )
    monkeypatch.setattr(
        runner,
        "_save_artifact",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    request = gemini_adapter.GeminiInteractionRequest.start("hello", model="m")

    with pytest.raises(KitaruRuntimeError, match="retrying may duplicate"):
        runner.run_sync(request)


def test_failed_event_error_redacts_exception_message(
    gemini_adapter: types.ModuleType,
) -> None:
    events = importlib.import_module("kitaru.adapters.gemini._events")

    error = events.error_from_exception(RuntimeError("prompt: secret user content"))

    assert error.exception_type == "RuntimeError"
    assert "secret user content" not in error.message
    assert "redacted" in error.message.lower()
