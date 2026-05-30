"""Foundation tests for the Gemini Interactions adapter scaffold."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import sys
import types
from typing import Any, cast

import pytest
from pydantic import ValidationError

from kitaru.analytics import AnalyticsEvent
from kitaru.errors import KitaruFeatureNotAvailableError, KitaruUsageError
from tests._checkpoint_handle_helpers import (
    assert_checkpoint_handle_error,
    checkpoint_output_handle,
)


def _purge_gemini_adapter_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.gemini"):
            monkeypatch.delitem(sys.modules, cached, raising=False)


@pytest.fixture
def fake_google_genai(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    google = types.ModuleType("google")
    google.__path__ = []  # type: ignore[attr-defined]
    genai = types.ModuleType("google.genai")
    google_module: Any = google
    google_module.genai = genai
    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.genai", genai)
    return genai


@pytest.fixture
def gemini_adapter(
    monkeypatch: pytest.MonkeyPatch,
    fake_google_genai: types.ModuleType,
) -> types.ModuleType:
    _purge_gemini_adapter_modules(monkeypatch)
    return importlib.import_module("kitaru.adapters.gemini")


def test_public_import_surface_uses_interaction_vocabulary(
    gemini_adapter: types.ModuleType,
) -> None:
    assert gemini_adapter.KitaruGeminiInteractionsRunner
    assert gemini_adapter.GeminiInteractionRequest
    assert gemini_adapter.GeminiInteractionResult
    assert gemini_adapter.GeminiInteractionStepSummary
    assert gemini_adapter.GeminiInteractionCapturePolicy
    assert gemini_adapter.GeminiInteractionRunEvent

    public_names = set(gemini_adapter.__all__)
    assert "calls" not in public_names
    assert "runner_call" not in public_names
    assert "durability_mode" not in public_names

    signature = inspect.signature(gemini_adapter.KitaruGeminiInteractionsRunner)
    assert "checkpoint_strategy" in signature.parameters
    assert "client_factory" in signature.parameters
    assert "allow_direct_execution_inside_checkpoint" in signature.parameters


def test_import_without_google_genai_raises_feature_not_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _purge_gemini_adapter_modules(monkeypatch)
    monkeypatch.setitem(sys.modules, "google", None)
    monkeypatch.delitem(sys.modules, "google.genai", raising=False)

    with pytest.raises(KitaruFeatureNotAvailableError, match="google-genai"):
        importlib.import_module("kitaru.adapters.gemini")


def test_transitive_google_genai_import_error_is_not_masked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _purge_gemini_adapter_modules(monkeypatch)
    for cached in list(sys.modules):
        if cached == "google" or cached.startswith("google."):
            monkeypatch.delitem(sys.modules, cached, raising=False)

    class BrokenGoogleImporter:
        def find_spec(self, fullname: str, path: object = None, target: object = None):
            if fullname == "google.genai":
                raise ModuleNotFoundError("No module named 'httpx'", name="httpx")
            return None

    importer = BrokenGoogleImporter()
    monkeypatch.setattr(sys, "meta_path", [importer, *sys.meta_path])

    with pytest.raises(ModuleNotFoundError, match="httpx"):
        importlib.import_module("kitaru.adapters.gemini")


def test_gemini_analytics_events_do_not_use_granular_vocabulary() -> None:
    values = [
        event.value
        for event in AnalyticsEvent
        if event.name.startswith("GEMINI_INTERACTIONS_")
    ]

    assert values
    assert all("tool checkpoint" not in value.lower() for value in values)
    assert all("steps checkpoint" not in value.lower() for value in values)


def test_runner_accepts_interaction_strategy(gemini_adapter: types.ModuleType) -> None:
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(
        name="gemini",
        checkpoint_strategy="interaction",
    )

    assert runner.name == "gemini"
    assert runner.checkpoint_strategy == "interaction"


@pytest.mark.parametrize(
    "strategy",
    [
        "calls",
        "runner_call",
        "granular",
        "model_call",
        "tool_call",
        "client_tools",
        "antigravity_steps",
        "managed_agent_steps",
        "step",
        "run",
    ],
)
def test_runner_rejects_granular_strategies(
    gemini_adapter: types.ModuleType,
    strategy: str,
) -> None:
    with pytest.raises(KitaruUsageError, match=r"only supports.*interaction"):
        gemini_adapter.KitaruGeminiInteractionsRunner(
            name="gemini",
            checkpoint_strategy=strategy,
        )


def test_runner_requires_stable_name(gemini_adapter: types.ModuleType) -> None:
    with pytest.raises(KitaruUsageError, match="stable `name`"):
        gemini_adapter.KitaruGeminiInteractionsRunner(name="")


def test_runner_rejects_client_and_factory_together(
    gemini_adapter: types.ModuleType,
) -> None:
    with pytest.raises(KitaruUsageError, match="mutually exclusive"):
        gemini_adapter.KitaruGeminiInteractionsRunner(
            name="gemini",
            client=object(),
            client_factory=object,
        )


def test_runner_rejects_non_boolean_nested_checkpoint_opt_in(
    gemini_adapter: types.ModuleType,
) -> None:
    with pytest.raises(KitaruUsageError, match="must be a boolean"):
        gemini_adapter.KitaruGeminiInteractionsRunner(
            name="gemini",
            allow_direct_execution_inside_checkpoint="yes",
        )


def test_run_sync_rejects_running_event_loop(gemini_adapter: types.ModuleType) -> None:
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(name="gemini")
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello", model="gemini-test"
    )

    async def call_sync() -> None:
        with pytest.raises(KitaruUsageError, match="already running event loop"):
            runner.run_sync(request)

    asyncio.run(call_sync())


def test_run_sync_requires_flow_or_checkpoint_scope(
    gemini_adapter: types.ModuleType,
) -> None:
    runner = gemini_adapter.KitaruGeminiInteractionsRunner(name="gemini")
    request = gemini_adapter.GeminiInteractionRequest.start(
        "hello", model="gemini-test"
    )

    with pytest.raises(KitaruUsageError, match="inside a Kitaru flow body"):
        runner.run_sync(request)


def test_gemini_request_constructors_and_validation(
    gemini_adapter: types.ModuleType,
) -> None:
    start = gemini_adapter.GeminiInteractionRequest.start(
        "hello",
        model="gemini-test",
    )
    resume = gemini_adapter.GeminiInteractionRequest.resume(
        "continue",
        "interaction-1",
        model="gemini-test",
    )
    function_result = gemini_adapter.GeminiInteractionRequest.function_result(
        previous_interaction_id="interaction-1",
        function_call_id="call-1",
        function_name="lookup",
        function_result={"ok": True},
        model="gemini-test",
    )
    poll = gemini_adapter.GeminiInteractionRequest.poll("interaction-2")

    assert start.kind == "start"
    assert start.target_kind == "model"
    assert resume.previous_interaction_id == "interaction-1"
    assert function_result.input[0]["type"] == "function_result"
    assert function_result.input[0]["call_id"] == "call-1"
    assert poll.kind == "poll"

    with pytest.raises(ValidationError, match="exactly one"):
        gemini_adapter.GeminiInteractionRequest.start("hello")
    with pytest.raises(ValidationError, match="exactly one"):
        gemini_adapter.GeminiInteractionRequest.start(
            "hello",
            model="gemini-test",
            agent="deep-research",
        )
    with pytest.raises(ValidationError, match="requires previous_interaction_id"):
        gemini_adapter.GeminiInteractionRequest(kind="resume", input="hello", model="m")
    with pytest.raises(ValidationError, match="requires interaction_id"):
        gemini_adapter.GeminiInteractionRequest(kind="poll")
    with pytest.raises(ValidationError, match=r"kind='poll'.*forbids input"):
        gemini_adapter.GeminiInteractionRequest(
            kind="poll",
            interaction_id="interaction-2",
            model="gemini-test",
        )
    with pytest.raises(ValidationError, match="background=True requires store=True"):
        gemini_adapter.GeminiInteractionRequest.start(
            "hello",
            model="gemini-test",
            background=True,
            store=False,
        )
    null_result = gemini_adapter.GeminiInteractionRequest.function_result(
        previous_interaction_id="interaction-1",
        function_call_id="call-null",
        function_result=None,
        model="gemini-test",
    )
    assert null_result.function_result_payload is None


def test_antigravity_preset_is_preview_labeled(
    gemini_adapter: types.ModuleType,
) -> None:
    request = gemini_adapter.GeminiInteractionRequest.antigravity("inspect this")

    assert request.agent == "antigravity-preview-05-2026"
    assert request.target_kind == "agent"
    assert request.model is None
    assert request.environment == "remote"
    assert request.store is True
    assert request.metadata["adapter_preview"] is True
    assert request.metadata["agent_family"] == "antigravity"


def test_gemini_request_rejects_checkpoint_handles(
    gemini_adapter: types.ModuleType,
) -> None:
    handle = checkpoint_output_handle()

    with pytest.raises(ValidationError) as input_exc:
        gemini_adapter.GeminiInteractionRequest.start(
            cast(str, handle),
            model="gemini-test",
        )
    assert_checkpoint_handle_error(input_exc, field_name="GeminiInteractionRequest")

    with pytest.raises(ValidationError) as session_exc:
        gemini_adapter.GeminiInteractionRequest.resume(
            "continue",
            cast(str, handle),
            model="gemini-test",
        )
    assert_checkpoint_handle_error(session_exc, field_name="GeminiInteractionRequest")

    with pytest.raises(ValidationError) as tools_exc:
        gemini_adapter.GeminiInteractionRequest.start(
            "hello",
            model="gemini-test",
            tools=[{"type": "function", "name": cast(str, handle)}],
        )
    assert_checkpoint_handle_error(tools_exc, field_name="GeminiInteractionRequest")

    with pytest.raises(ValidationError) as config_exc:
        gemini_adapter.GeminiInteractionRequest.start(
            "hello",
            model="gemini-test",
            generation_config={"temperature": cast(float, handle)},
        )
    assert_checkpoint_handle_error(config_exc, field_name="GeminiInteractionRequest")

    with pytest.raises(ValidationError) as metadata_exc:
        gemini_adapter.GeminiInteractionRequest.start(
            "hello",
            model="gemini-test",
            metadata={"source": cast(str, handle)},
        )
    assert_checkpoint_handle_error(metadata_exc, field_name="GeminiInteractionRequest")


def test_capture_policy_accepts_strict_capture_failure_knobs(
    gemini_adapter: types.ModuleType,
) -> None:
    policy = gemini_adapter.GeminiInteractionCapturePolicy(
        fail_on_artifact_capture_error=True,
        fail_on_event_persistence_error=True,
    )

    assert policy.fail_on_artifact_capture_error is True
