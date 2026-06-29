"""Model/tool wrapper tests for the Google ADK adapter."""

from __future__ import annotations

import asyncio
import importlib
import sys
from typing import Any

import pytest

from google_adk_fakes import install_fake_google_adk, purge_google_adk_adapter_modules
from kitaru.errors import KitaruFeatureNotAvailableError, KitaruUsageError


def _modules(monkeypatch: pytest.MonkeyPatch):
    purge_google_adk_adapter_modules(monkeypatch)
    install_fake_google_adk(monkeypatch)
    adapter = importlib.import_module("kitaru.adapters.google_adk")
    model_module = importlib.import_module("kitaru.adapters.google_adk._model")
    tool_module = importlib.import_module("kitaru.adapters.google_adk._tool")
    return adapter, model_module, tool_module


class FakeModel:
    model = "gemini-fake"

    def supported_models(self) -> list[str]:
        return ["gemini-fake", "gemini-fake-pro"]

    async def generate_content_async(self, llm_request: Any, stream: bool = False):
        yield {"text": f"response:{llm_request['prompt']}", "stream": stream}


class FakeTool:
    name = "search"
    description = "Search fake data."

    async def run_async(self, *, args: Any, tool_context: Any) -> Any:
        return {"result": args["query"], "context": type(tool_context).__name__}


class FakeToolContext:
    def __init__(self, state: dict[str, Any] | None = None) -> None:
        self.state = dict(state or {})


class ReadOnlyStateContext:
    def __init__(self) -> None:
        self.state = object()


class StatefulTool(FakeTool):
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def run_async(self, *, args: Any, tool_context: Any) -> Any:
        self.calls.append(dict(args))
        tool_context.state["lookup_marker"] = "local-cat-fact"
        return {"answer": "local-cat-fact", "query": args["query"]}


class NestedStatefulTool(FakeTool):
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def run_async(self, *, args: Any, tool_context: Any) -> Any:
        self.calls.append(dict(args))
        tool_context.state["nested"] = {"answer": "local-cat-fact"}
        return {"answer": "local-cat-fact", "query": args["query"]}


class ReplaySafeMCPBackedTool(FakeTool):
    name = "mcp_lookup"
    description = "Replay-safe stand-in for an ADK-exposed MCP tool."

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def run_async(self, *, args: Any, tool_context: Any) -> Any:
        self.calls.append(dict(args))
        tool_context.state["mcp_lookup_marker"] = "replay-safe-mcp-result"
        return {"source": "mcp", "answer": args["query"]}


class SyncProcessTool(FakeTool):
    def __init__(self) -> None:
        self.processed: list[tuple[Any, Any]] = []

    def process_llm_request(self, *, tool_context: Any, llm_request: Any) -> None:
        self.processed.append((tool_context, llm_request))


class AsyncProcessTool(FakeTool):
    def __init__(self) -> None:
        self.processed: list[tuple[Any, Any]] = []

    async def process_llm_request(self, *, tool_context: Any, llm_request: Any) -> None:
        self.processed.append((tool_context, llm_request))


async def _collect_model(model: Any, request: dict[str, Any]) -> list[Any]:
    return [event async for event in model.generate_content_async(request)]


class PreviewPart:
    def __init__(self, text: str) -> None:
        self.text = text


class PreviewContent:
    def __init__(self, parts: list[Any]) -> None:
        self.parts = parts


class ExplodingPreviewPart:
    @property
    def text(self) -> str:
        raise AssertionError("preview walked past the requested character budget")


def _tool_confirmation_handoff(adapter: Any) -> Any:
    return adapter.ADKHandoffRequest(
        kind="tool_confirmation",
        function_call_id="tool-call-1",
        request_function_call_id="request-call-1",
        tool_name="dangerous_lookup",
        tool_args={"query": "cats"},
        message="Confirm dangerous lookup?",
    )


def test_final_output_preview_extracts_adk_content_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    output = PreviewContent(parts=[PreviewPart("hello"), PreviewPart(" world")])

    assert adapter.final_output_preview(output) == "hello world"
    assert (
        adapter.final_output_preview({"content": {"parts": [{"text": "nested"}]}})
        == "nested"
    )
    assert adapter.final_output_preview("abcdef", max_chars=4) == "abc…"


def test_final_output_preview_stops_after_character_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    output = PreviewContent(parts=[PreviewPart("abcdef"), ExplodingPreviewPart()])

    assert adapter.final_output_preview(output, max_chars=4) == "abc…"


def test_runner_skips_handoff_extraction_when_events_have_no_markers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    agent_module = importlib.import_module("kitaru.adapters.google_adk._agent")

    def fail_if_called(_events: list[dict[str, Any]]) -> list[Any]:
        raise AssertionError("handoff extraction should not run without markers")

    monkeypatch.setattr(agent_module, "extract_handoff_requests", fail_if_called)

    class StaticRunner:
        app_name = "plain-output-app"

        def run(self, **_kwargs: Any) -> list[dict[str, Any]]:
            return [{"text": "plain output"}]

    result = adapter.KitaruADKRunner(StaticRunner()).run_sync(
        adapter.ADKRunRequest(
            user_id="local-user",
            session_id="plain-session",
            message={"text": "hello"},
        )
    )

    assert result.status == "completed"
    assert result.handoffs == []
    assert result.final_output == "plain output"


def test_hitl_parser_accepts_synthetic_snake_case_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _modules(monkeypatch)
    hitl_module = importlib.import_module("kitaru.adapters.google_adk._hitl")

    handoffs = hitl_module.extract_handoff_requests(
        [
            {
                "id": "event-1",
                "invocation_id": "invocation-1",
                "author": "agent",
                "content": {
                    "parts": [
                        {
                            "function_call": {
                                "id": "request-1",
                                "name": "adk_request_confirmation",
                                "args": {
                                    "original_function_call": {
                                        "id": "tool-call-1",
                                        "name": "delete_thing",
                                        "args": {"thing": "draft"},
                                    },
                                    "tool_confirmation": {"hint": "Confirm?"},
                                },
                            }
                        }
                    ]
                },
            }
        ]
    )

    assert len(handoffs) == 1
    handoff = handoffs[0]
    assert handoff.kind == hitl_module._HANDOFF_KIND_TOOL_CONFIRMATION
    assert handoff.invocation_id == "invocation-1"
    assert handoff.function_call_id == "tool-call-1"
    assert handoff.request_function_call_id == "request-1"
    assert handoff.tool_name == "delete_thing"
    assert handoff.tool_args == {"thing": "draft"}
    assert handoff.message == "Confirm?"


def test_tool_confirmation_message_builder_creates_denial_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    handoff = _tool_confirmation_handoff(adapter)

    message = adapter.build_tool_confirmation_message(
        handoff,
        confirmed=False,
        payload={"reason": "too risky"},
    )

    assert message.role == "user"
    assert len(message.parts) == 1
    function_response = message.parts[0].function_response
    assert function_response.name == "adk_request_confirmation"
    assert function_response.id == "request-call-1"
    assert function_response.response == {
        "confirmed": False,
        "payload": {"reason": "too risky"},
    }


def test_tool_confirmation_request_builder_wraps_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    handoff = _tool_confirmation_handoff(adapter)

    request = adapter.build_tool_confirmation_request(
        handoff,
        True,
        user_id="local-user",
        session_id="local-session",
        run_kwargs={"streaming": False},
        metadata={"source": "test"},
    )

    assert isinstance(request, adapter.ADKRunRequest)
    assert request.user_id == "local-user"
    assert request.session_id == "local-session"
    assert request.run_kwargs == {"streaming": False}
    assert request.metadata == {"source": "test"}
    function_response = request.message.parts[0].function_response
    assert function_response.id == "request-call-1"
    assert function_response.response == {"confirmed": True}


@pytest.mark.parametrize("kind", ["credential_request", "human_input"])
def test_tool_confirmation_helpers_reject_unsupported_handoff_kinds(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    handoff = adapter.ADKHandoffRequest(
        kind=kind,
        function_call_id="original-call",
        request_function_call_id="request-call",
    )

    with pytest.raises(KitaruUsageError, match="kind='tool_confirmation'"):
        adapter.build_tool_confirmation_message(handoff, confirmed=True)
    with pytest.raises(KitaruUsageError, match="kind='tool_confirmation'"):
        adapter.build_tool_confirmation_request(
            handoff,
            confirmed=True,
            user_id="local-user",
            session_id="local-session",
        )


def test_tool_confirmation_helper_rejects_missing_response_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    handoff = adapter.ADKHandoffRequest(
        kind="tool_confirmation",
        function_call_id="tool-call-1",
    )

    with pytest.raises(KitaruUsageError, match="request_function_call_id"):
        adapter.build_tool_confirmation_message(handoff, confirmed=True)


def test_tool_confirmation_helper_import_error_is_clear(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    handoff = _tool_confirmation_handoff(adapter)
    monkeypatch.delitem(sys.modules, "google.genai", raising=False)
    monkeypatch.delitem(sys.modules, "google.genai.types", raising=False)
    monkeypatch.delattr(sys.modules["google"], "genai", raising=False)

    with pytest.raises(KitaruFeatureNotAvailableError, match=r"google\.genai\.types"):
        adapter.build_tool_confirmation_message(handoff, confirmed=True)


def test_wait_for_tool_confirmation_returns_resume_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    hitl_module = importlib.import_module("kitaru.adapters.google_adk._hitl")
    import kitaru

    monkeypatch.setattr(hitl_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(hitl_module, "is_inside_checkpoint", lambda: False)
    wait_calls: list[dict[str, Any]] = []

    def fake_wait(**kwargs: Any) -> bool:
        wait_calls.append(kwargs)
        return True

    monkeypatch.setattr(kitaru, "wait", fake_wait)
    result = adapter.ADKRunResult(
        status="requires_action",
        handoffs=[_tool_confirmation_handoff(adapter)],
    )

    request = adapter.wait_for_tool_confirmation(
        result,
        user_id="local-user",
        session_id="local-session",
        metadata={"ticket": "123"},
    )

    assert request.user_id == "local-user"
    assert request.session_id == "local-session"
    assert request.metadata == {"ticket": "123"}
    assert request.message.parts[0].function_response.response == {"confirmed": True}
    assert wait_calls[0]["schema"] is bool
    assert wait_calls[0]["question"] == "Confirm dangerous lookup?"
    assert wait_calls[0]["metadata"] == {
        "adapter": "google_adk",
        "source": "tool_confirmation_wait",
        "handoff_kind": "tool_confirmation",
        "tool_name": "dangerous_lookup",
        "function_call_id": "tool-call-1",
        "request_function_call_id": "request-call-1",
        "invocation_id": None,
        "user_metadata": {"ticket": "123"},
    }


def test_wait_for_tool_confirmation_rejects_invalid_scope_or_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    hitl_module = importlib.import_module("kitaru.adapters.google_adk._hitl")
    result = adapter.ADKRunResult(status="completed")

    monkeypatch.setattr(hitl_module, "is_inside_flow", lambda: False)
    monkeypatch.setattr(hitl_module, "is_inside_checkpoint", lambda: False)
    with pytest.raises(KitaruUsageError, match="inside a flow"):
        adapter.wait_for_tool_confirmation(
            result,
            user_id="local-user",
            session_id="local-session",
        )

    monkeypatch.setattr(hitl_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(hitl_module, "is_inside_checkpoint", lambda: True)
    with pytest.raises(KitaruUsageError, match="flow body"):
        adapter.wait_for_tool_confirmation(
            result,
            user_id="local-user",
            session_id="local-session",
        )

    monkeypatch.setattr(hitl_module, "is_inside_checkpoint", lambda: False)
    with pytest.raises(KitaruUsageError, match="requires_action"):
        adapter.wait_for_tool_confirmation(
            result,
            user_id="local-user",
            session_id="local-session",
        )


def test_wait_for_tool_confirmation_rejects_missing_or_ambiguous_handoffs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    hitl_module = importlib.import_module("kitaru.adapters.google_adk._hitl")
    monkeypatch.setattr(hitl_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(hitl_module, "is_inside_checkpoint", lambda: False)

    with pytest.raises(KitaruUsageError, match="no `tool_confirmation` handoffs"):
        adapter.wait_for_tool_confirmation(
            adapter.ADKRunResult(status="requires_action"),
            user_id="local-user",
            session_id="local-session",
        )

    with pytest.raises(KitaruUsageError, match="multiple `tool_confirmation`"):
        adapter.wait_for_tool_confirmation(
            adapter.ADKRunResult(
                status="requires_action",
                handoffs=[
                    _tool_confirmation_handoff(adapter),
                    _tool_confirmation_handoff(adapter),
                ],
            ),
            user_id="local-user",
            session_id="local-session",
        )


def test_wait_for_tool_confirmation_validates_handoff_before_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    hitl_module = importlib.import_module("kitaru.adapters.google_adk._hitl")
    import kitaru

    monkeypatch.setattr(hitl_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(hitl_module, "is_inside_checkpoint", lambda: False)
    wait_calls: list[dict[str, Any]] = []

    def fake_wait(**kwargs: Any) -> bool:
        wait_calls.append(kwargs)
        return True

    monkeypatch.setattr(kitaru, "wait", fake_wait)
    result = adapter.ADKRunResult(
        status="requires_action",
        handoffs=[
            adapter.ADKHandoffRequest(
                kind="tool_confirmation",
                function_call_id="tool-call-1",
            )
        ],
    )

    with pytest.raises(KitaruUsageError, match="request_function_call_id"):
        adapter.wait_for_tool_confirmation(
            result,
            user_id="local-user",
            session_id="local-session",
        )

    assert wait_calls == []


def test_model_wrapper_is_base_llm_and_delegates_supported_models(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    base_llm_module = importlib.import_module("google.adk.models.base_llm")
    BaseLlm = base_llm_module.BaseLlm

    wrapped = adapter.KitaruADKModel(FakeModel())

    assert isinstance(wrapped, BaseLlm)
    assert wrapped.supported_models() == ["gemini-fake", "gemini-fake-pro"]


def test_model_wrapper_without_supported_models_returns_empty_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    model_without_registry = object()

    wrapped = adapter.KitaruADKModel(model_without_registry)

    assert wrapped.supported_models() == []


def test_tool_wrapper_is_base_tool_and_accepts_sync_process_delegate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    base_tool_module = importlib.import_module("google.adk.tools.base_tool")
    BaseTool = base_tool_module.BaseTool

    tool = SyncProcessTool()
    wrapped = adapter.wrap_tool(tool)
    context = object()
    request = {"prompt": "hi"}

    assert isinstance(wrapped, BaseTool)
    asyncio.run(wrapped.process_llm_request(tool_context=context, llm_request=request))

    assert tool.processed == [(context, request)]


def test_tool_wrapper_process_llm_request_accepts_async_delegate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, _tool_module = _modules(monkeypatch)
    tool = AsyncProcessTool()
    wrapped = adapter.wrap_tool(tool)
    context = object()
    request = {"prompt": "hi"}

    asyncio.run(wrapped.process_llm_request(tool_context=context, llm_request=request))

    assert tool.processed == [(context, request)]


def test_model_wrapper_buffers_generate_content_inside_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, model_module, _tool_module = _modules(monkeypatch)
    checkpoint_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(model_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(model_module.runtime, "is_inside_checkpoint", lambda: False)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        checkpoint_calls.append(kwargs)
        return await kwargs["body"]()

    monkeypatch.setattr(model_module, "run_async_in_checkpoint", fake_checkpoint)

    wrapped = adapter.KitaruADKModel(FakeModel())
    responses = asyncio.run(_collect_model(wrapped, {"prompt": "hi"}))

    assert responses == [{"text": "response:hi", "stream": False}]
    assert (
        checkpoint_calls[0]["checkpoint_inputs"]["model_input"]["model"]
        == "gemini-fake"
    )


def test_tool_wrapper_runs_tool_inside_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, tool_module = _modules(monkeypatch)
    checkpoint_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(tool_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_checkpoint", lambda: False)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        checkpoint_calls.append(kwargs)
        return await kwargs["body"]()

    monkeypatch.setattr(tool_module, "run_async_in_checkpoint", fake_checkpoint)

    wrapped = adapter.wrap_tool(FakeTool())
    result = asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=object())
    )

    assert result["result"] == "cats"
    assert (
        checkpoint_calls[0]["checkpoint_inputs"]["tool_args"]["tool_name"] == "search"
    )


def test_tool_checkpoint_replay_restores_tool_context_state_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, tool_module = _modules(monkeypatch)
    checkpoint_cache: dict[str, Any] = {}

    monkeypatch.setattr(tool_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_checkpoint", lambda: False)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        cache_key = kwargs["cache_key"]
        if cache_key not in checkpoint_cache:
            checkpoint_cache[cache_key] = await kwargs["body"]()
        return checkpoint_cache[cache_key]

    monkeypatch.setattr(tool_module, "run_async_in_checkpoint", fake_checkpoint)

    tool = StatefulTool()
    wrapped = adapter.wrap_tool(tool)
    first_context = FakeToolContext({"seed": "same"})
    second_context = FakeToolContext({"seed": "same"})

    first_result = asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=first_context)
    )
    second_result = asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=second_context)
    )

    assert (
        first_result
        == second_result
        == {
            "answer": "local-cat-fact",
            "query": "cats",
        }
    )
    assert tool.calls == [{"query": "cats"}]
    assert first_context.state == {
        "seed": "same",
        "lookup_marker": "local-cat-fact",
    }
    assert second_context.state == {
        "seed": "same",
        "lookup_marker": "local-cat-fact",
    }


def test_replay_safe_mcp_backed_adk_tool_replays_exposed_tool_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, tool_module = _modules(monkeypatch)
    checkpoint_cache: dict[str, Any] = {}

    monkeypatch.setattr(tool_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_checkpoint", lambda: False)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        cache_key = kwargs["cache_key"]
        if cache_key not in checkpoint_cache:
            checkpoint_cache[cache_key] = await kwargs["body"]()
        return checkpoint_cache[cache_key]

    monkeypatch.setattr(tool_module, "run_async_in_checkpoint", fake_checkpoint)

    tool = ReplaySafeMCPBackedTool()
    wrapped = adapter.KitaruADKTool(tool)
    first_context = FakeToolContext({"seed": "same"})
    replayed_context = FakeToolContext({"seed": "same"})

    first_result = asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=first_context)
    )
    replayed_result = asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=replayed_context)
    )

    assert first_result == replayed_result == {"source": "mcp", "answer": "cats"}
    assert tool.calls == [{"query": "cats"}]
    assert first_context.state == {
        "seed": "same",
        "mcp_lookup_marker": "replay-safe-mcp-result",
    }
    assert replayed_context.state == first_context.state


def test_tool_checkpoint_runs_directly_with_non_mutable_adk_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, tool_module = _modules(monkeypatch)
    checkpoint_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(tool_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_checkpoint", lambda: False)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        checkpoint_calls.append(kwargs)
        return await kwargs["body"]()

    monkeypatch.setattr(tool_module, "run_async_in_checkpoint", fake_checkpoint)

    wrapped = adapter.KitaruADKTool(FakeTool())
    result = asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=ReadOnlyStateContext())
    )

    assert result == {"result": "cats", "context": "ReadOnlyStateContext"}
    assert checkpoint_calls == []


def test_tracker_scope_policy_allows_nested_model_and_tool_metadata_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, model_module, tool_module = _modules(monkeypatch)
    tracking_module = importlib.import_module("kitaru.adapters.google_adk._tracking")

    monkeypatch.setattr(model_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(model_module.runtime, "is_inside_checkpoint", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_checkpoint", lambda: True)

    policy = adapter.ADKCallCheckpointPolicy(nested_checkpoint_policy="metadata_only")
    tracker = tracking_module.EventTracker("active_policy")
    wrapped_model = adapter.KitaruADKModel(FakeModel())
    wrapped_tool = adapter.KitaruADKTool(FakeTool())

    with tracking_module.tracker_scope(
        tracker,
        capture=adapter.ADKCapturePolicy(),
        call_policy=policy,
    ):
        responses = asyncio.run(_collect_model(wrapped_model, {"prompt": "hi"}))
        result = asyncio.run(
            wrapped_tool.run_async(args={"query": "cats"}, tool_context=object())
        )

    assert responses == [{"text": "response:hi", "stream": False}]
    assert result == {"result": "cats", "context": "object"}


def test_tracker_scope_capture_controls_model_and_tool_checkpoint_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, model_module, tool_module = _modules(monkeypatch)
    tracking_module = importlib.import_module("kitaru.adapters.google_adk._tracking")
    model_include_raw: list[bool] = []
    tool_include_raw: list[bool] = []
    original_model_to_json_safe = model_module.to_json_safe
    original_tool_to_json_safe = tool_module.to_json_safe

    monkeypatch.setattr(model_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(model_module.runtime, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(tool_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_checkpoint", lambda: False)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        return await kwargs["body"]()

    def model_to_json_safe(value: Any, *, include_raw: bool = False) -> Any:
        model_include_raw.append(include_raw)
        return original_model_to_json_safe(value, include_raw=include_raw)

    def tool_to_json_safe(value: Any, *, include_raw: bool = False) -> Any:
        tool_include_raw.append(include_raw)
        return original_tool_to_json_safe(value, include_raw=include_raw)

    monkeypatch.setattr(model_module, "run_async_in_checkpoint", fake_checkpoint)
    monkeypatch.setattr(tool_module, "run_async_in_checkpoint", fake_checkpoint)
    monkeypatch.setattr(model_module, "to_json_safe", model_to_json_safe)
    monkeypatch.setattr(tool_module, "to_json_safe", tool_to_json_safe)

    policy = adapter.ADKCallCheckpointPolicy()
    capture = adapter.ADKCapturePolicy(capture_mode="full")
    tracker = tracking_module.EventTracker("active_capture")
    wrapped_model = adapter.KitaruADKModel(FakeModel())
    wrapped_tool = adapter.KitaruADKTool(FakeTool())

    with tracking_module.tracker_scope(tracker, capture=capture, call_policy=policy):
        responses = asyncio.run(_collect_model(wrapped_model, {"prompt": "hi"}))
        result = asyncio.run(
            wrapped_tool.run_async(args={"query": "cats"}, tool_context=object())
        )

    assert responses == [{"text": "response:hi", "stream": False}]
    assert result == {"result": "cats", "context": "object"}
    assert model_include_raw == [True]
    assert tool_include_raw == [True]


def test_tool_checkpoint_input_includes_starting_state_to_avoid_stale_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, tool_module = _modules(monkeypatch)
    checkpoint_cache: dict[str, Any] = {}
    checkpoint_inputs: list[dict[str, Any]] = []

    monkeypatch.setattr(tool_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_checkpoint", lambda: False)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        checkpoint_inputs.append(kwargs["checkpoint_inputs"]["tool_args"])
        cache_key = kwargs["cache_key"]
        if cache_key not in checkpoint_cache:
            checkpoint_cache[cache_key] = await kwargs["body"]()
        return checkpoint_cache[cache_key]

    monkeypatch.setattr(tool_module, "run_async_in_checkpoint", fake_checkpoint)

    tool = StatefulTool()
    wrapped = adapter.wrap_tool(tool)
    first_context = FakeToolContext({"seed": "first"})
    second_context = FakeToolContext({"seed": "second"})

    asyncio.run(wrapped.run_async(args={"query": "cats"}, tool_context=first_context))
    asyncio.run(wrapped.run_async(args={"query": "cats"}, tool_context=second_context))

    assert tool.calls == [{"query": "cats"}, {"query": "cats"}]
    assert len(checkpoint_cache) == 2
    assert [
        item["tool_context_state_before"]["value"]["seed"] for item in checkpoint_inputs
    ] == ["first", "second"]


def test_tool_checkpoint_cache_identity_keeps_redacted_state_values_distinct(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, tool_module = _modules(monkeypatch)
    checkpoint_cache: dict[str, Any] = {}

    monkeypatch.setattr(tool_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_checkpoint", lambda: False)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        cache_key = kwargs["cache_key"]
        if cache_key not in checkpoint_cache:
            checkpoint_cache[cache_key] = await kwargs["body"]()
        return checkpoint_cache[cache_key]

    monkeypatch.setattr(tool_module, "run_async_in_checkpoint", fake_checkpoint)

    tool = StatefulTool()
    wrapped = adapter.wrap_tool(tool)

    asyncio.run(
        wrapped.run_async(
            args={"query": "cats"},
            tool_context=FakeToolContext({"api_token": "first"}),
        )
    )
    asyncio.run(
        wrapped.run_async(
            args={"query": "cats"},
            tool_context=FakeToolContext({"api_token": "second"}),
        )
    )

    assert tool.calls == [{"query": "cats"}, {"query": "cats"}]
    assert len(checkpoint_cache) == 2


def test_tool_checkpoint_replay_copies_nested_state_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, tool_module = _modules(monkeypatch)
    checkpoint_cache: dict[str, Any] = {}

    monkeypatch.setattr(tool_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_checkpoint", lambda: False)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        cache_key = kwargs["cache_key"]
        if cache_key not in checkpoint_cache:
            checkpoint_cache[cache_key] = await kwargs["body"]()
        return checkpoint_cache[cache_key]

    monkeypatch.setattr(tool_module, "run_async_in_checkpoint", fake_checkpoint)

    tool = NestedStatefulTool()
    wrapped = adapter.wrap_tool(tool)

    asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=FakeToolContext())
    )
    replayed_context = FakeToolContext()
    asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=replayed_context)
    )
    replayed_context.state["nested"]["answer"] = "corrupted"
    later_replayed_context = FakeToolContext()
    asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=later_replayed_context)
    )

    assert tool.calls == [{"query": "cats"}]
    assert later_replayed_context.state["nested"] == {"answer": "local-cat-fact"}


def test_model_wrapper_nested_checkpoint_limitation_is_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, model_module, _tool_module = _modules(monkeypatch)

    monkeypatch.setattr(model_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(model_module.runtime, "is_inside_checkpoint", lambda: True)

    wrapped = adapter.KitaruADKModel(FakeModel())

    with pytest.raises(KitaruUsageError, match="inside a Kitaru checkpoint"):
        asyncio.run(_collect_model(wrapped, {"prompt": "hi"}))


def test_tool_wrapper_metadata_only_nested_policy_runs_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, _model_module, tool_module = _modules(monkeypatch)
    checkpoint_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(tool_module.runtime, "is_inside_flow", lambda: True)
    monkeypatch.setattr(tool_module.runtime, "is_inside_checkpoint", lambda: True)

    async def fake_checkpoint(**kwargs: Any) -> Any:
        checkpoint_calls.append(kwargs)
        return await kwargs["body"]()

    monkeypatch.setattr(tool_module, "run_async_in_checkpoint", fake_checkpoint)

    policy = adapter.ADKCallCheckpointPolicy(nested_checkpoint_policy="metadata_only")
    wrapped = adapter.wrap_tool(FakeTool(), call_policy=policy)
    result = asyncio.run(
        wrapped.run_async(args={"query": "dogs"}, tool_context=object())
    )

    assert result["result"] == "dogs"
    assert checkpoint_calls == []
