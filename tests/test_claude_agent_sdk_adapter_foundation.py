"""Foundation tests for the Claude Agent SDK adapter scaffold."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import sys
import types
from types import SimpleNamespace
from typing import cast

import pytest
from pydantic import ValidationError

from kitaru.analytics import AnalyticsEvent
from kitaru.errors import KitaruFeatureNotAvailableError, KitaruUsageError
from tests._checkpoint_handle_helpers import (
    assert_checkpoint_handle_error,
    checkpoint_output_handle,
)


def _purge_claude_adapter_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.claude_agent_sdk"):
            monkeypatch.delitem(sys.modules, cached, raising=False)


@pytest.fixture
def fake_claude_sdk(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    sdk = types.ModuleType("claude_agent_sdk")

    class ResultMessage:
        pass

    sdk.__dict__["ResultMessage"] = ResultMessage
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", sdk)
    return sdk


@pytest.fixture
def claude_adapter(
    monkeypatch: pytest.MonkeyPatch,
    fake_claude_sdk: types.ModuleType,
) -> types.ModuleType:
    _purge_claude_adapter_modules(monkeypatch)
    return importlib.import_module("kitaru.adapters.claude_agent_sdk")


def test_public_import_surface_uses_invocation_vocabulary(
    claude_adapter: types.ModuleType,
) -> None:
    assert claude_adapter.KitaruClaudeRunner
    assert claude_adapter.ClaudeRunRequest
    assert claude_adapter.ClaudeRunResult
    assert claude_adapter.ClaudeCapturePolicy
    assert claude_adapter.ClaudeRunEvent
    assert claude_adapter.KitaruClaudeRunner.run_stream
    assert claude_adapter.KitaruClaudeRunner.run_stream_sync
    assert claude_adapter.CLAUDE_STREAM_STARTED == "claude_agent_sdk.stream.started"
    assert claude_adapter.CLAUDE_STREAM_EVENT == "claude_agent_sdk.stream.event"
    assert claude_adapter.CLAUDE_STREAM_COMPLETED == "claude_agent_sdk.stream.completed"
    assert claude_adapter.CLAUDE_STREAM_FAILED == "claude_agent_sdk.stream.failed"
    assert claude_adapter.CLAUDE_STREAM_EVENT_KINDS == (
        claude_adapter.CLAUDE_STREAM_STARTED,
        claude_adapter.CLAUDE_STREAM_EVENT,
        claude_adapter.CLAUDE_STREAM_COMPLETED,
        claude_adapter.CLAUDE_STREAM_FAILED,
    )
    assert claude_adapter.CLAUDE_STREAM_TERMINAL_EVENT_KINDS == (
        claude_adapter.CLAUDE_STREAM_COMPLETED,
        claude_adapter.CLAUDE_STREAM_FAILED,
    )

    public_names = set(claude_adapter.__all__)
    assert "calls" not in public_names
    assert "runner_call" not in public_names
    assert "durability_mode" not in public_names

    signature = inspect.signature(claude_adapter.KitaruClaudeRunner)
    assert "checkpoint_strategy" in signature.parameters
    assert "options_factory" in signature.parameters
    assert "allow_direct_execution_inside_checkpoint" in signature.parameters
    assert "durability_mode" not in signature.parameters


def test_import_without_claude_agent_sdk_raises_feature_not_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _purge_claude_adapter_modules(monkeypatch)
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", None)

    with pytest.raises(KitaruFeatureNotAvailableError, match="claude-agent-sdk"):
        importlib.import_module("kitaru.adapters.claude_agent_sdk")


def test_transitive_claude_agent_sdk_import_error_is_not_masked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _purge_claude_adapter_modules(monkeypatch)
    for cached in list(sys.modules):
        if cached == "claude_agent_sdk" or cached.startswith("claude_agent_sdk."):
            monkeypatch.delitem(sys.modules, cached, raising=False)

    class BrokenClaudeImporter:
        def find_spec(self, fullname: str, path: object = None, target: object = None):
            if fullname == "claude_agent_sdk":
                raise ModuleNotFoundError(
                    "No module named 'anthropic'", name="anthropic"
                )
            return None

    importer = BrokenClaudeImporter()
    monkeypatch.setattr(sys, "meta_path", [importer, *sys.meta_path])

    with pytest.raises(ModuleNotFoundError, match="anthropic"):
        importlib.import_module("kitaru.adapters.claude_agent_sdk")


def test_claude_analytics_events_do_not_use_granular_vocabulary() -> None:
    values = [
        event.value
        for event in AnalyticsEvent
        if event.name.startswith("CLAUDE_AGENT_SDK_")
    ]

    assert values
    assert all("calls" not in value.lower() for value in values)
    assert all("tool" not in value.lower() for value in values)


def test_runner_accepts_invocation_strategy(claude_adapter: types.ModuleType) -> None:
    runner = claude_adapter.KitaruClaudeRunner(
        name="claude", checkpoint_strategy="invocation"
    )

    assert runner.name == "claude"
    assert runner.checkpoint_strategy == "invocation"


@pytest.mark.parametrize(
    "strategy",
    ["calls", "runner_call", "granular", "model_call", "tool_call", "step", "run"],
)
def test_runner_rejects_granular_strategies(
    claude_adapter: types.ModuleType,
    strategy: str,
) -> None:
    with pytest.raises(KitaruUsageError, match=r"only supports.*invocation"):
        claude_adapter.KitaruClaudeRunner(name="claude", checkpoint_strategy=strategy)


def test_runner_requires_stable_name(claude_adapter: types.ModuleType) -> None:
    with pytest.raises(KitaruUsageError, match="stable `name`"):
        claude_adapter.KitaruClaudeRunner(name="")


def test_runner_rejects_options_and_factory_together(
    claude_adapter: types.ModuleType,
) -> None:
    with pytest.raises(KitaruUsageError, match="mutually exclusive"):
        claude_adapter.KitaruClaudeRunner(
            name="claude",
            options=SimpleNamespace(),
            options_factory=lambda request: None,
        )


def test_runner_rejects_non_boolean_nested_checkpoint_opt_in(
    claude_adapter: types.ModuleType,
) -> None:
    with pytest.raises(KitaruUsageError, match="must be a boolean"):
        claude_adapter.KitaruClaudeRunner(
            name="claude",
            allow_direct_execution_inside_checkpoint="yes",
        )


def test_run_sync_rejects_running_event_loop(claude_adapter: types.ModuleType) -> None:
    runner = claude_adapter.KitaruClaudeRunner(name="claude")
    request = claude_adapter.ClaudeRunRequest.start("hello")

    async def call_sync() -> None:
        with pytest.raises(KitaruUsageError, match="already running event loop"):
            runner.run_sync(request)

    asyncio.run(call_sync())


def test_run_sync_requires_flow_or_checkpoint_scope(
    claude_adapter: types.ModuleType,
) -> None:
    runner = claude_adapter.KitaruClaudeRunner(name="claude")
    request = claude_adapter.ClaudeRunRequest.start("hello")

    with pytest.raises(KitaruUsageError, match="inside a Kitaru flow body"):
        runner.run_sync(request)


def test_claude_run_request_start_and_resume_validation(
    claude_adapter: types.ModuleType,
) -> None:
    start = claude_adapter.ClaudeRunRequest.start(
        "hello", cwd="/tmp/project", max_turns=2
    )
    resume = claude_adapter.ClaudeRunRequest.resume("continue", "session-123")

    assert start.kind == "start"
    assert start.prompt == "hello"
    assert resume.kind == "resume"
    assert resume.resume_session_id == "session-123"

    with pytest.raises(ValidationError, match="non-empty"):
        claude_adapter.ClaudeRunRequest.start("   ")
    with pytest.raises(ValidationError, match="forbids resume_session_id"):
        claude_adapter.ClaudeRunRequest(
            kind="start",
            prompt="hello",
            resume_session_id="session-123",
        )
    with pytest.raises(ValidationError, match="requires resume_session_id"):
        claude_adapter.ClaudeRunRequest(kind="resume", prompt="hello")
    with pytest.raises(ValidationError, match="positive"):
        claude_adapter.ClaudeRunRequest.start("hello", max_turns=0)


def test_claude_run_request_rejects_checkpoint_handles(
    claude_adapter: types.ModuleType,
) -> None:
    handle = checkpoint_output_handle()

    with pytest.raises(ValidationError) as prompt_exc:
        claude_adapter.ClaudeRunRequest.start(cast(str, handle))
    assert_checkpoint_handle_error(
        prompt_exc,
        field_name="ClaudeRunRequest.prompt",
    )

    with pytest.raises(ValidationError) as resume_prompt_exc:
        claude_adapter.ClaudeRunRequest.resume(
            cast(str, handle),
            "session-123",
        )
    assert_checkpoint_handle_error(
        resume_prompt_exc,
        field_name="ClaudeRunRequest.prompt",
    )

    with pytest.raises(ValidationError) as session_exc:
        claude_adapter.ClaudeRunRequest.resume(
            "continue",
            cast(str, handle),
        )
    assert_checkpoint_handle_error(
        session_exc,
        field_name="ClaudeRunRequest.resume_session_id",
    )

    with pytest.raises(ValidationError) as cwd_exc:
        claude_adapter.ClaudeRunRequest.start("hello", cwd=cast(str, handle))
    assert_checkpoint_handle_error(
        cwd_exc,
        field_name="ClaudeRunRequest.cwd",
    )


def test_capture_policy_accepts_strict_capture_failure_knobs(
    claude_adapter: types.ModuleType,
) -> None:
    policy = claude_adapter.ClaudeCapturePolicy(
        fail_on_artifact_capture_error=True,
        fail_on_event_persistence_error=True,
    )

    assert policy.fail_on_artifact_capture_error is True
    assert policy.fail_on_event_persistence_error is True


def test_capture_policy_rejects_unknown_fields(
    claude_adapter: types.ModuleType,
) -> None:
    with pytest.raises(ValidationError):
        claude_adapter.ClaudeCapturePolicy(durability_mode="calls")


def test_checkpoint_config_validation_rejects_invalid_values(
    claude_adapter: types.ModuleType,
) -> None:
    with pytest.raises(KitaruUsageError, match="runtime='isolated'"):
        claude_adapter.KitaruClaudeRunner(
            name="claude",
            checkpoint_config={"runtime": "isolated"},
        )
    with pytest.raises(KitaruUsageError, match="Expected 'inline'"):
        claude_adapter.KitaruClaudeRunner(
            name="claude",
            checkpoint_config={"runtime": "banana"},
        )
    with pytest.raises(KitaruUsageError, match="non-negative integer"):
        claude_adapter.KitaruClaudeRunner(
            name="claude",
            checkpoint_config={"retries": -1},
        )
    with pytest.raises(KitaruUsageError, match="non-empty string"):
        claude_adapter.KitaruClaudeRunner(
            name="claude",
            checkpoint_config={"type": ""},
        )
    with pytest.raises(KitaruUsageError, match="cache must be a boolean"):
        claude_adapter.KitaruClaudeRunner(
            name="claude",
            checkpoint_config={"cache": "yes"},
        )


def test_claude_event_kind_is_invocation_only(claude_adapter: types.ModuleType) -> None:
    event = claude_adapter.ClaudeRunEvent(
        event_id="evt_1",
        kind="invocation",
        status="completed",
        sequence_index=1,
        run_label="abc123",
        runner_name="claude",
    )

    assert event.kind == "invocation"
    with pytest.raises(ValidationError):
        claude_adapter.ClaudeRunEvent(
            event_id="evt_2",
            kind="tool_call",
            status="completed",
            sequence_index=2,
            run_label="abc123",
            runner_name="claude",
        )
