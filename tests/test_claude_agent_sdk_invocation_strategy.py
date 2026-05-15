"""Invocation-boundary tests for the Claude Agent SDK adapter."""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from kitaru.errors import KitaruRuntimeError, KitaruUsageError


def _purge_claude_adapter_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.claude_agent_sdk"):
            monkeypatch.delitem(sys.modules, cached, raising=False)


@pytest.fixture
def fake_sdk(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    sdk = types.ModuleType("claude_agent_sdk")
    calls: list[dict[str, object]] = []
    messages: list[object] = []
    sdk.__dict__["calls"] = calls
    sdk.__dict__["messages"] = messages

    class AssistantMessage:
        def __init__(self, text: str) -> None:
            self.text = text

    class ClaudeAgentOptions:
        def __init__(
            self,
            *,
            cwd: str | None = None,
            resume: str | None = None,
            max_turns: int | None = None,
        ) -> None:
            self.cwd = cwd
            self.resume = resume
            self.max_turns = max_turns

    class ResultMessage:
        def __init__(
            self,
            *,
            session_id: str = "session-123",
            result: str = "done",
            is_error: bool = False,
        ) -> None:
            self.session_id = session_id
            self.result = result
            self.is_error = is_error
            self.usage = {"input_tokens": 3, "output_tokens": 5}
            self.total_cost_usd = 0.04
            self.model_usage = {"claude-sonnet": {"input_tokens": 3}}
            self.stop_reason = "end_turn"
            self.subtype = "success"
            self.num_turns = 1
            self.duration_ms = 12.5
            self.duration_api_ms = 10.0

    async def query(*, prompt: str, options: object = None):
        calls.append({"prompt": prompt, "options": options})
        for message in messages:
            yield message

    sdk.__dict__["AssistantMessage"] = AssistantMessage
    sdk.__dict__["ClaudeAgentOptions"] = ClaudeAgentOptions
    sdk.__dict__["ResultMessage"] = ResultMessage
    sdk.__dict__["query"] = query
    messages[:] = [AssistantMessage("thinking"), ResultMessage()]
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", sdk)
    return sdk


@pytest.fixture
def claude_adapter(
    monkeypatch: pytest.MonkeyPatch,
    fake_sdk: types.ModuleType,
) -> types.ModuleType:
    _purge_claude_adapter_modules(monkeypatch)
    return importlib.import_module("kitaru.adapters.claude_agent_sdk")


def _patch_inline_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    agent_module = importlib.import_module("kitaru.adapters.claude_agent_sdk._agent")
    tracking_module = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._tracking"
    )
    monkeypatch.setattr(agent_module, "is_inside_flow", lambda: False)
    monkeypatch.setattr(agent_module, "is_inside_checkpoint", lambda: True)
    monkeypatch.setattr(tracking_module, "is_inside_flow", lambda: False)
    monkeypatch.setattr(tracking_module, "is_inside_checkpoint", lambda: True)


def _patch_direct_execution_persistence(
    monkeypatch: pytest.MonkeyPatch,
    *,
    save_artifact: Any | None = None,
    save_event: Any | None = None,
    log_event: Any | None = None,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.claude_agent_sdk._agent")
    tracking_module = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._tracking"
    )
    monkeypatch.setattr(
        agent_module.KitaruClaudeRunner,
        "_save_artifact",
        staticmethod(save_artifact or (lambda name, value, *, type: None)),
    )
    monkeypatch.setattr(
        tracking_module.kitaru,
        "save",
        save_event or (lambda name, value, *, type: None),
    )
    monkeypatch.setattr(
        tracking_module.kitaru,
        "log",
        log_event or (lambda **kwargs: None),
    )


def _raise_runtime_error(message: str) -> None:
    raise RuntimeError(message)


def test_nested_checkpoint_rejected_before_sdk_call_by_default(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_inline_scope(monkeypatch)
    runner = claude_adapter.KitaruClaudeRunner(name="claude")

    with pytest.raises(KitaruUsageError, match="existing Kitaru checkpoint"):
        runner.run_sync(claude_adapter.ClaudeRunRequest.start("hello"))

    assert fake_sdk.__dict__["calls"] == []


def test_nested_checkpoint_explicit_opt_in_runs_directly_with_warning(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_inline_scope(monkeypatch)
    agent_module = importlib.import_module("kitaru.adapters.claude_agent_sdk._agent")
    checkpoint_calls: list[dict[str, object]] = []

    def fake_run_sync_in_checkpoint(**kwargs: object) -> None:
        checkpoint_calls.append(kwargs)

    monkeypatch.setattr(
        agent_module, "run_sync_in_checkpoint", fake_run_sync_in_checkpoint
    )
    _patch_direct_execution_persistence(monkeypatch)
    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
    )

    result = runner.run_sync(claude_adapter.ClaudeRunRequest.start("hello"))

    assert fake_sdk.__dict__["calls"] == [{"prompt": "hello", "options": None}]
    assert checkpoint_calls == []
    assert result.metadata["direct_execution_inside_checkpoint"] is True
    assert any(
        "ran directly inside an existing" in warning for warning in result.warnings
    )


def test_options_manifest_redacts_sequence_pairs(
    claude_adapter: types.ModuleType,
) -> None:
    serialization = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._serialization"
    )
    request = claude_adapter.ClaudeRunRequest.start("hello")

    manifest = serialization.redacted_options_manifest(
        {
            "headers": [
                ("Authorization", "Bearer secret"),
                ["x-api-key", "api-secret"],
                ("User-Agent", "kitaru-test"),
            ],
            "env": (("COOKIE", "session=abc"), ("PATH", "/usr/bin")),
        },
        request,
    )

    options = cast(dict[str, Any], manifest["options"])
    assert options["headers"] == [
        ["Authorization", "[REDACTED]"],
        ["x-api-key", "[REDACTED]"],
        ["User-Agent", "kitaru-test"],
    ]
    assert options["env"] == [["COOKIE", "[REDACTED]"], ["PATH", "/usr/bin"]]


def test_runner_invocation_extracts_result_and_artifact_names(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_inline_scope(monkeypatch)
    saved: list[str] = []
    logs: list[dict[str, object]] = []
    _patch_direct_execution_persistence(
        monkeypatch,
        save_artifact=lambda name, value, *, type: saved.append(name),
        save_event=lambda name, value, *, type: saved.append(name),
        log_event=lambda **kwargs: logs.append(kwargs),
    )

    options = SimpleNamespace(api_key="secret-key", allowed_tools=["Read"])
    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="Claude Reviewer",
        options=options,
    )
    request = claude_adapter.ClaudeRunRequest.start("review this")

    result = runner.run_sync(request)

    assert fake_sdk.__dict__["calls"] == [{"prompt": "review this", "options": options}]
    assert result.status == "completed"
    assert result.session_id == "session-123"
    assert result.final_text == "done"
    assert result.usage == {"input_tokens": 3, "output_tokens": 5}
    assert result.cost_usd == 0.04
    assert result.model_usage == {"claude-sonnet": {"input_tokens": 3}}
    assert result.stop_reason == "end_turn"
    assert result.subtype == "success"
    assert result.num_turns == 1
    assert result.duration_ms == 12.5
    assert result.event_log_artifact_name is not None
    assert result.run_summary_artifact_name is not None
    assert result.messages_artifact_name is not None
    assert result.options_manifest_artifact_name is not None
    assert result.output_artifact_name is not None
    assert result.messages_artifact_name in saved
    assert result.options_manifest_artifact_name in saved
    assert result.event_log_artifact_name in saved
    assert logs


def test_artifact_capture_failure_is_non_fatal_by_default(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_inline_scope(monkeypatch)

    def fake_save(name: str, value: object, *, type: str) -> None:
        if name.endswith("messages"):
            raise RuntimeError("simulated capture failure")

    _patch_direct_execution_persistence(monkeypatch, save_artifact=fake_save)
    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
    )

    result = runner.run_sync(claude_adapter.ClaudeRunRequest.start("hello"))

    assert result.final_text == "done"
    assert result.messages_artifact_name is None
    assert result.output_artifact_name is not None
    assert any("messages artifact" in warning for warning in result.warnings)
    failures = cast(list[dict[str, str]], result.metadata["capture_failures"])
    assert failures[0]["kind"] == "messages"
    assert failures[0]["exception_type"] == "RuntimeError"


def test_artifact_capture_failure_can_be_strict(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_inline_scope(monkeypatch)

    def fail_save_artifact(name: str, value: object, *, type: str) -> None:
        _raise_runtime_error("strict capture failure")

    _patch_direct_execution_persistence(monkeypatch, save_artifact=fail_save_artifact)
    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
        capture=claude_adapter.ClaudeCapturePolicy(fail_on_artifact_capture_error=True),
    )

    with pytest.raises(KitaruRuntimeError, match="artifact capture failed"):
        runner.run_sync(claude_adapter.ClaudeRunRequest.start("hello"))


def test_failed_invocation_capture_failure_preserves_sdk_error(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_inline_scope(monkeypatch)
    result_message = fake_sdk.__dict__["ResultMessage"]
    cast(list[object], fake_sdk.__dict__["messages"])[:] = [
        result_message(is_error=True, result="permission denied")
    ]

    def fail_save_artifact(name: str, value: object, *, type: str) -> None:
        _raise_runtime_error("manifest save failed")

    _patch_direct_execution_persistence(monkeypatch, save_artifact=fail_save_artifact)
    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
        capture=claude_adapter.ClaudeCapturePolicy(fail_on_artifact_capture_error=True),
    )

    with pytest.raises(RuntimeError, match="permission denied"):
        runner.run_sync(claude_adapter.ClaudeRunRequest.start("hello"))


def test_event_persistence_failure_is_non_fatal_by_default(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_inline_scope(monkeypatch)

    def fake_event_save(name: str, value: object, *, type: str) -> None:
        if name.endswith("event_log"):
            raise RuntimeError("event log save failed")

    def fail_log_event(**kwargs: object) -> None:
        _raise_runtime_error("log failed")

    _patch_direct_execution_persistence(
        monkeypatch, save_event=fake_event_save, log_event=fail_log_event
    )
    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
    )

    result = runner.run_sync(claude_adapter.ClaudeRunRequest.start("hello"))

    assert result.final_text == "done"
    assert result.event_log_artifact_name is None
    assert result.run_summary_artifact_name is not None
    failures = cast(
        list[dict[str, str | None]],
        result.metadata["event_persistence_failures"],
    )
    assert {failure["operation"] for failure in failures} == {
        "save_event_log",
        "log_metadata",
    }
    assert any("event/log persistence failed" in warning for warning in result.warnings)


def test_event_persistence_failure_can_be_strict(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_inline_scope(monkeypatch)

    def fail_log_event(**kwargs: object) -> None:
        _raise_runtime_error("log failed")

    _patch_direct_execution_persistence(monkeypatch, log_event=fail_log_event)
    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
        capture=claude_adapter.ClaudeCapturePolicy(
            fail_on_event_persistence_error=True
        ),
    )

    with pytest.raises(KitaruRuntimeError, match="log failed"):
        runner.run_sync(claude_adapter.ClaudeRunRequest.start("hello"))
    assert fake_sdk.__dict__["calls"] == [{"prompt": "hello", "options": None}]


def test_options_factory_receives_request_and_redacts_manifest(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_inline_scope(monkeypatch)
    manifests: list[dict[str, object]] = []

    def fake_save(name: str, value: object, *, type: str) -> None:
        if name.endswith("options_manifest") and isinstance(value, dict):
            manifests.append(cast(dict[str, object], value))

    _patch_direct_execution_persistence(monkeypatch, save_artifact=fake_save)

    seen_requests = []

    def options_factory(request: object) -> dict[str, object]:
        seen_requests.append(request)
        return {
            "api_key": "anthropic-api-key-placeholder",
            "Authorization": "Bearer secret",
            "cwd": cast(Any, request).cwd,
            "callback": lambda: None,
        }

    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
        options_factory=options_factory,
    )
    request = claude_adapter.ClaudeRunRequest.start("hello", cwd="/tmp/repo")

    runner.run_sync(request)

    assert seen_requests == [request]
    assert manifests
    manifest = manifests[0]
    options = cast(dict[str, Any], manifest["options"])
    request_manifest = cast(dict[str, Any], manifest["request"])
    callback_manifest = cast(dict[str, Any], options["callback"])
    assert options["api_key"] == "[REDACTED]"
    assert options["Authorization"] == "[REDACTED]"
    assert callback_manifest["configured"] is True
    assert request_manifest["prompt_sha256"]
    assert "hello" not in str(manifest)


def test_request_scoped_fields_build_claude_options(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_inline_scope(monkeypatch)
    _patch_direct_execution_persistence(monkeypatch)
    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
    )

    runner.run_sync(
        claude_adapter.ClaudeRunRequest.resume(
            "continue",
            "session-456",
            cwd="/tmp/repo",
            max_turns=3,
        )
    )

    options = cast(list[dict[str, object]], fake_sdk.__dict__["calls"])[0]["options"]
    assert cast(Any, options).cwd == "/tmp/repo"
    assert cast(Any, options).resume == "session-456"
    assert cast(Any, options).max_turns == 3


def test_static_options_reject_request_scoped_fields(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_inline_scope(monkeypatch)
    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
        options=SimpleNamespace(cwd="/tmp/static"),
    )

    with pytest.raises(KitaruUsageError, match="request-scoped SDK options"):
        runner.run_sync(
            claude_adapter.ClaudeRunRequest.start("hello", cwd="/tmp/request")
        )


def test_emit_events_false_suppresses_event_artifacts_and_log(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_inline_scope(monkeypatch)
    saved: list[str] = []
    logs: list[dict[str, object]] = []
    _patch_direct_execution_persistence(
        monkeypatch,
        save_artifact=lambda name, value, *, type: saved.append(name),
        save_event=lambda name, value, *, type: saved.append(name),
        log_event=lambda **kwargs: logs.append(kwargs),
    )
    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
        capture=claude_adapter.ClaudeCapturePolicy(emit_events=False),
    )

    result = runner.run_sync(claude_adapter.ClaudeRunRequest.start("hello"))

    assert result.event_log_artifact_name is None
    assert result.run_summary_artifact_name is None
    assert not any(name.endswith("event_log") for name in saved)
    assert not any(name.endswith("run_summary") for name in saved)
    assert logs == []


def test_synthetic_checkpoint_is_used_from_flow_scope(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.claude_agent_sdk._agent")
    tracking_module = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._tracking"
    )
    monkeypatch.setattr(agent_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(tracking_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tracking_module, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(tracking_module.kitaru, "log", lambda **kwargs: None)
    calls: list[dict[str, object]] = []

    def fake_run_sync_in_checkpoint(**kwargs: object) -> object:
        calls.append(kwargs)
        body = cast(Any, kwargs["body"])
        return body()

    monkeypatch.setattr(
        agent_module, "run_sync_in_checkpoint", fake_run_sync_in_checkpoint
    )
    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
        checkpoint_config={"cache": False, "retries": 1},
    )

    result = runner.run_sync(claude_adapter.ClaudeRunRequest.start("hello"))

    assert result.final_text == "done"
    assert calls
    assert calls[0]["step_name"] == "claude_claude_invocation"
    assert calls[0]["config"] == {"cache": False, "retries": 1, "type": "agent_call"}
    assert isinstance(calls[0]["cache_key"], str)


def test_missing_transcript_file_adds_warning(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_inline_scope(monkeypatch)
    _patch_direct_execution_persistence(monkeypatch)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)

    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
    )
    result = runner.run_sync(
        claude_adapter.ClaudeRunRequest.start("hello", cwd=str(tmp_path / "repo"))
    )

    assert result.transcript_path is not None
    assert result.transcript_artifact_name is None
    assert any(
        "transcript file was not found" in warning for warning in result.warnings
    )


def test_transcript_file_is_captured_when_available(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_inline_scope(monkeypatch)
    captured: dict[str, object] = {}
    monkeypatch.setattr(Path, "home", lambda: tmp_path)

    cwd = tmp_path / "repo"
    cwd.mkdir()
    encoded_cwd = "".join(
        char if char.isalnum() else "-" for char in str(cwd.resolve())
    )
    transcript_dir = tmp_path / ".claude" / "projects" / encoded_cwd
    transcript_dir.mkdir(parents=True)
    transcript_file = transcript_dir / "session-123.jsonl"
    transcript_file.write_text('{"type":"result"}\n', encoding="utf-8")

    def fake_save(name: str, value: object, *, type: str) -> None:
        if name.endswith("transcript"):
            captured["value"] = value

    _patch_direct_execution_persistence(monkeypatch, save_artifact=fake_save)
    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
    )

    result = runner.run_sync(
        claude_adapter.ClaudeRunRequest.start("hello", cwd=str(cwd))
    )

    assert result.transcript_path == str(transcript_file)
    assert captured["value"] == {
        "path": str(transcript_file),
        "format": "jsonl",
        "content": '{"type":"result"}\n',
    }


def test_transcript_lookup_uses_static_options_cwd_when_request_cwd_missing(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_inline_scope(monkeypatch)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    _patch_direct_execution_persistence(monkeypatch)

    options_cwd = tmp_path / "static-options-cwd"
    options_cwd.mkdir()
    encoded_cwd = "".join(
        char if char.isalnum() else "-" for char in str(options_cwd.resolve())
    )
    transcript_dir = tmp_path / ".claude" / "projects" / encoded_cwd
    transcript_dir.mkdir(parents=True)
    transcript_file = transcript_dir / "session-123.jsonl"
    transcript_file.write_text('{"type":"result"}\n', encoding="utf-8")

    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
        options=SimpleNamespace(cwd=str(options_cwd)),
    )
    result = runner.run_sync(claude_adapter.ClaudeRunRequest.start("hello"))

    assert result.transcript_path == str(transcript_file)


def test_transcript_lookup_uses_options_factory_cwd_when_request_cwd_missing(
    claude_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_inline_scope(monkeypatch)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    _patch_direct_execution_persistence(monkeypatch)

    factory_cwd = tmp_path / "factory-cwd"
    factory_cwd.mkdir()
    encoded_cwd = "".join(
        char if char.isalnum() else "-" for char in str(factory_cwd.resolve())
    )
    transcript_dir = tmp_path / ".claude" / "projects" / encoded_cwd
    transcript_dir.mkdir(parents=True)
    transcript_file = transcript_dir / "session-123.jsonl"
    transcript_file.write_text('{"type":"result"}\n', encoding="utf-8")

    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
        options_factory=lambda request: {"cwd": str(factory_cwd)},
    )
    result = runner.run_sync(claude_adapter.ClaudeRunRequest.start("hello"))

    assert result.transcript_path == str(transcript_file)


def test_runner_raises_when_sdk_returns_no_result_message(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_inline_scope(monkeypatch)
    _patch_direct_execution_persistence(monkeypatch)
    assistant_message = fake_sdk.__dict__["AssistantMessage"]
    cast(list[object], fake_sdk.__dict__["messages"])[:] = [
        assistant_message("not final")
    ]
    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
    )

    with pytest.raises(RuntimeError, match="did not return a final ResultMessage"):
        runner.run_sync(claude_adapter.ClaudeRunRequest.start("hello"))


def test_runner_raises_when_sdk_result_is_error(
    claude_adapter: types.ModuleType,
    fake_sdk: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_inline_scope(monkeypatch)
    tracking_module = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._tracking"
    )
    monkeypatch.setattr(
        tracking_module.kitaru, "save", lambda name, value, *, type: None
    )
    monkeypatch.setattr(tracking_module.kitaru, "log", lambda **kwargs: None)
    result_message = fake_sdk.__dict__["ResultMessage"]
    cast(list[object], fake_sdk.__dict__["messages"])[:] = [
        result_message(is_error=True, result="permission denied")
    ]
    runner = claude_adapter.KitaruClaudeRunner(
        allow_direct_execution_inside_checkpoint=True,
        name="claude",
    )

    with pytest.raises(RuntimeError, match="error ResultMessage"):
        runner.run_sync(claude_adapter.ClaudeRunRequest.start("hello"))


def test_transcript_path_rejects_non_ascii_cwd(
    claude_adapter: types.ModuleType,
) -> None:
    transcripts = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._transcripts"
    )

    with pytest.raises(ValueError, match="ASCII cwd"):
        transcripts.resolve_claude_transcript_path("session-123", cwd="/tmp/délft")


@pytest.mark.parametrize(
    "session_id", ["../escape", "..\\escape", "foo/bar", "foo\\bar"]
)
def test_transcript_path_rejects_path_like_session_ids(
    claude_adapter: types.ModuleType,
    session_id: str,
) -> None:
    transcripts = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._transcripts"
    )

    with pytest.raises(ValueError, match="path separators or traversal"):
        transcripts.resolve_claude_transcript_path(session_id, cwd="/tmp/repo")


def test_runner_rejects_unknown_strategy_directly(
    claude_adapter: types.ModuleType,
) -> None:
    utils = importlib.import_module("kitaru.adapters.claude_agent_sdk._utils")

    with pytest.raises(KitaruUsageError, match="Expected 'invocation'"):
        utils.validate_checkpoint_strategy("banana")
