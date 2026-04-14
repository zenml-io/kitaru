"""Tests for `kitaru.llm()` runtime and normalization behavior."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from kitaru.analytics import AnalyticsEvent
from kitaru.config import ResolvedModelSelection
from kitaru.errors import (
    KitaruContextError,
    KitaruRuntimeError,
    KitaruUsageError,
)
from kitaru.llm import (
    LLMResponse,
    LLMToolCall,
    LLMToolDefinition,
    LLMUsage,
    _LLMUsage,
    _parse_anthropic_response,
    _parse_openai_compatible_response,
    _parse_provider_target,
    _ProviderCallResult,
    _resolve_credential_overlay,
    llm,
)
from kitaru.runtime import _checkpoint_scope, _flow_scope


def _flow_checkpoint_scope() -> tuple[str, str]:
    """Return valid execution/checkpoint IDs for scope setup."""
    return str(uuid4()), str(uuid4())


def _simple_selection(
    model: str, *, secret: str | None = None
) -> ResolvedModelSelection:
    """Build a selection where requested == resolved (no alias indirection)."""
    return ResolvedModelSelection(
        requested_model=model,
        alias=None,
        resolved_model=model,
        secret=secret,
    )


def _tracked_llm_metadata(track_mock: MagicMock) -> dict[str, object]:
    """Return the analytics metadata emitted by one LLM call."""
    track_mock.assert_called_once()
    event, metadata = track_mock.call_args.args
    assert event == AnalyticsEvent.LLM_CALLED
    assert isinstance(metadata, dict)
    return metadata


def _search_tool(name: str = "search_documents") -> dict[str, object]:
    """Return a small canonical OpenAI-style function tool definition."""
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": "Search documents by query.",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
            },
        },
    }


def _fake_response(
    content: str | None = "ok",
    *,
    usage: LLMUsage | None = None,
    tool_calls: list[LLMToolCall] | None = None,
    finish_reason: str = "completed",
    provider_finish_reason: str | None = "stop",
    resolved_model: str = "openai/gpt-4o-mini",
) -> LLMResponse:
    """Build a normalized response for tests."""
    return LLMResponse(
        content=content,
        tool_calls=tool_calls or [],
        finish_reason=finish_reason,  # type: ignore[arg-type]
        provider_finish_reason=provider_finish_reason,
        usage=usage or LLMUsage(),
        resolved_model=resolved_model,
    )


@contextmanager
def _llm_execution_scope(
    *,
    model_selection: ResolvedModelSelection,
    credential_overlay: tuple[dict[str, str], str] = ({}, "environment"),
):
    """Set up flow/checkpoint scope with mocked model resolution and credentials.

    Encapsulates the common 6-layer context-manager scaffolding that most
    ``llm()`` integration tests need.  Yields ``(mock_save, mock_log)``.
    """
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch("kitaru.llm.resolve_model_selection", return_value=model_selection),
        patch(
            "kitaru.llm._resolve_credential_overlay",
            return_value=credential_overlay,
        ),
        patch("kitaru.llm.save") as mock_save,
        patch("kitaru.llm.log") as mock_log,
    ):
        yield mock_save, mock_log


# ---------------------------------------------------------------------------
# Context guards (unchanged behavior)
# ---------------------------------------------------------------------------


def test_llm_raises_outside_flow() -> None:
    """`kitaru.llm()` should reject calls outside an active flow."""
    with pytest.raises(KitaruContextError, match=r"inside a @flow"):
        llm("hello")


def test_llm_uses_inline_execution_inside_checkpoint() -> None:
    """Inside checkpoints, llm should run inline without synthetic checkpoint calls."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch(
            "kitaru.llm._execute_llm_call",
            return_value=_fake_response("ok"),
        ) as mock_execute,
        patch("kitaru.llm._llm_checkpoint_call") as mock_synthetic,
    ):
        response = llm("hello", model="fast")

    assert response.content == "ok"
    mock_execute.assert_called_once()
    mock_synthetic.assert_not_called()


def test_llm_dispatches_through_synthetic_checkpoint_in_flow_scope() -> None:
    """In flow scope (outside checkpoints), llm should call the synthetic boundary."""
    with (
        _flow_scope(name="demo_flow", execution_id=str(uuid4())),
        patch(
            "kitaru.llm._llm_checkpoint_call",
            return_value=_fake_response("ok"),
        ) as mock_synthetic,
    ):
        response = llm("hello", model="fast", name="outline")

    assert response.content == "ok"
    mock_synthetic.assert_called_once()
    request = mock_synthetic.call_args.args[0]
    assert request.call_name == "outline"
    assert request.model == "fast"
    assert mock_synthetic.call_args.kwargs["id"] == "outline"


def test_llm_auto_names_calls_sequentially_within_flow_scope() -> None:
    """Unnamed calls should receive deterministic runtime-local names."""
    with (
        _flow_scope(name="demo_flow", execution_id=str(uuid4())),
        patch(
            "kitaru.llm._llm_checkpoint_call",
            return_value=_fake_response("ok"),
        ) as mock_synthetic,
    ):
        llm("first")
        llm("second")

    first_request = mock_synthetic.call_args_list[0].args[0]
    second_request = mock_synthetic.call_args_list[1].args[0]
    assert first_request.call_name == "llm_1"
    assert second_request.call_name == "llm_2"


# ---------------------------------------------------------------------------
# Provider routing
# ---------------------------------------------------------------------------


class TestParseProviderTarget:
    def test_openai_model(self) -> None:
        target = _parse_provider_target("openai/gpt-4o-mini")
        assert target.provider == "openai"
        assert target.provider_model == "gpt-4o-mini"
        assert target.resolved_model == "openai/gpt-4o-mini"

    def test_anthropic_model(self) -> None:
        target = _parse_provider_target("anthropic/claude-sonnet-4-20250514")
        assert target.provider == "anthropic"
        assert target.provider_model == "claude-sonnet-4-20250514"

    def test_ollama_model(self) -> None:
        target = _parse_provider_target("ollama/qwen3.5")
        assert target.provider == "ollama"
        assert target.provider_model == "qwen3.5"
        assert target.resolved_model == "ollama/qwen3.5"

    def test_openrouter_model_with_nested_provider(self) -> None:
        target = _parse_provider_target("openrouter/anthropic/claude-sonnet-4-20250514")
        assert target.provider == "openrouter"
        assert target.provider_model == "anthropic/claude-sonnet-4-20250514"
        assert target.resolved_model == "openrouter/anthropic/claude-sonnet-4-20250514"

    def test_providerless_model_raises(self) -> None:
        with pytest.raises(KitaruUsageError, match="provider prefix"):
            _parse_provider_target("gpt-4o-mini")

    def test_unsupported_provider_raises(self) -> None:
        with pytest.raises(KitaruUsageError, match="not supported"):
            _parse_provider_target("gemini/gemini-2.0-flash")

    def test_empty_model_name_raises(self) -> None:
        with pytest.raises(KitaruUsageError, match="empty model name"):
            _parse_provider_target("openai/")


# ---------------------------------------------------------------------------
# Rich LLM response model and parser fixtures
# ---------------------------------------------------------------------------


def test_llm_models_are_exported_from_public_package() -> None:
    """Rich LLM response models should be public package exports."""
    import kitaru

    assert kitaru.LLMFinishReason is not None
    assert kitaru.LLMResponse is LLMResponse
    assert kitaru.LLMToolCall is LLMToolCall
    assert kitaru.LLMToolDefinition is LLMToolDefinition
    assert kitaru.LLMUsage is LLMUsage


def test_llm_response_models_round_trip_and_expose_text_convenience() -> None:
    """The rich response models should be serializable Pydantic models."""
    response = LLMResponse(
        content="done",
        tool_calls=[
            LLMToolCall(
                id="call_1",
                name="search_documents",
                arguments_json='{"query":"cats"}',
                arguments={"query": "cats"},
            )
        ],
        finish_reason="tool_calls",
        provider_finish_reason="tool_calls",
        usage=LLMUsage(prompt_tokens=3, completion_tokens=5, total_tokens=8),
        requested_model="fast",
        alias="fast",
        resolved_model="openai/gpt-4o-mini",
    )

    restored = LLMResponse.model_validate(response.model_dump())

    assert restored.text == "done"
    assert str(restored) == "done"
    assert restored.has_tool_calls is True
    assert restored.tool_calls[0].arguments == {"query": "cats"}
    assert restored.usage.total_tokens == 8


def test_provider_call_result_response_text_requires_content() -> None:
    """Internal text compatibility should still fail for tool-only responses."""
    result = _ProviderCallResult(
        response=LLMResponse(
            tool_calls=[
                LLMToolCall(
                    id="call_1",
                    name="search_documents",
                    arguments_json='{"query":"cats"}',
                    arguments={"query": "cats"},
                )
            ],
            finish_reason="tool_calls",
            resolved_model="openai/gpt-4o-mini",
        )
    )

    assert result.response.has_tool_calls is True
    with pytest.raises(KitaruRuntimeError, match="no text content"):
        _ = result.response_text


def test_parse_openai_compatible_text_only_dict_response() -> None:
    """OpenAI-compatible dict responses should normalize text and usage."""
    raw_response = {
        "choices": [
            {
                "message": {"role": "assistant", "content": "hello world"},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": 7,
            "completion_tokens": 11,
            "total_tokens": 18,
        },
    }

    response = _parse_openai_compatible_response(
        raw_response,
        requested_model="fast",
        alias="fast",
        resolved_model="openai/gpt-4o-mini",
    )

    assert response.content == "hello world"
    assert response.tool_calls == []
    assert response.finish_reason == "completed"
    assert response.provider_finish_reason == "stop"
    assert response.usage.prompt_tokens == 7
    assert response.usage.completion_tokens == 11
    assert response.usage.total_tokens == 18
    assert response.requested_model == "fast"
    assert response.alias == "fast"
    assert response.resolved_model == "openai/gpt-4o-mini"


def test_parse_openai_compatible_mixed_multiple_object_tool_calls() -> None:
    """OpenAI-compatible object responses may contain text plus many tool calls."""
    raw_response = SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(
                    content=[{"type": "text", "text": "I need to check two places."}],
                    tool_calls=[
                        SimpleNamespace(
                            id="call_1",
                            function=SimpleNamespace(
                                name="search_documents",
                                arguments='{"query":"kitaru llm"}',
                            ),
                        ),
                        SimpleNamespace(
                            id="call_2",
                            function=SimpleNamespace(
                                name="lookup_record",
                                arguments={"record_id": 42},
                            ),
                        ),
                    ],
                ),
                finish_reason="tool_calls",
            )
        ],
        usage=SimpleNamespace(
            prompt_tokens="10",
            completion_tokens=20,
            total_tokens=30,
        ),
    )

    response = _parse_openai_compatible_response(
        raw_response,
        resolved_model="openai/gpt-4o-mini",
    )

    assert response.content == "I need to check two places."
    assert response.finish_reason == "tool_calls"
    assert response.provider_finish_reason == "tool_calls"
    assert response.has_tool_calls is True
    assert [call.name for call in response.tool_calls] == [
        "search_documents",
        "lookup_record",
    ]
    assert response.tool_calls[0].arguments_json == '{"query":"kitaru llm"}'
    assert response.tool_calls[0].arguments == {"query": "kitaru llm"}
    assert response.tool_calls[1].arguments_json == '{"record_id":42}'
    assert response.tool_calls[1].arguments == {"record_id": 42}
    assert response.usage.prompt_tokens == 10
    assert response.usage.total_tokens == 30


def test_parse_openai_compatible_tool_only_preserves_invalid_arguments() -> None:
    """Malformed OpenAI tool arguments should remain inspectable as raw JSON."""
    raw_response = {
        "choices": [
            {
                "message": {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "call_bad",
                            "type": "function",
                            "function": {
                                "name": "search_documents",
                                "arguments": '{"query":',
                            },
                        }
                    ],
                },
                "finish_reason": "tool_calls",
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
    }

    response = _parse_openai_compatible_response(
        raw_response,
        resolved_model="openai/gpt-4o-mini",
    )

    assert response.content is None
    assert response.finish_reason == "tool_calls"
    assert len(response.tool_calls) == 1
    tool_call = response.tool_calls[0]
    assert tool_call.name == "search_documents"
    assert tool_call.arguments_json == '{"query":'
    assert tool_call.arguments is None
    assert tool_call.arguments_parse_error is not None


def test_parse_anthropic_text_only_dict_response() -> None:
    """Anthropic dict responses should collect text blocks and map usage."""
    raw_response = {
        "content": [{"type": "text", "text": "hello from claude"}],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 4, "output_tokens": 6},
    }

    response = _parse_anthropic_response(
        raw_response,
        requested_model="claude",
        alias="claude",
        resolved_model="anthropic/claude-sonnet-4-20250514",
    )

    assert response.content == "hello from claude"
    assert response.tool_calls == []
    assert response.finish_reason == "completed"
    assert response.provider_finish_reason == "end_turn"
    assert response.usage.prompt_tokens == 4
    assert response.usage.completion_tokens == 6
    assert response.usage.total_tokens == 10
    assert response.requested_model == "claude"
    assert response.alias == "claude"


def test_parse_anthropic_tool_only_dict_response() -> None:
    """Anthropic tool-only responses should normalize tool_use blocks."""
    raw_response = {
        "content": [
            {
                "type": "tool_use",
                "id": "toolu_1",
                "name": "search_documents",
                "input": {"query": "cats"},
            }
        ],
        "stop_reason": "tool_use",
        "usage": {"input_tokens": 8, "output_tokens": 5},
    }

    response = _parse_anthropic_response(
        raw_response,
        resolved_model="anthropic/claude-sonnet-4-20250514",
    )

    assert response.content is None
    assert response.finish_reason == "tool_calls"
    assert response.provider_finish_reason == "tool_use"
    assert len(response.tool_calls) == 1
    assert response.tool_calls[0].id == "toolu_1"
    assert response.tool_calls[0].name == "search_documents"
    assert response.tool_calls[0].arguments_json == '{"query":"cats"}'
    assert response.tool_calls[0].arguments == {"query": "cats"}
    assert response.tool_calls[0].arguments_parse_error is None


def test_parse_anthropic_mixed_multiple_object_tool_calls() -> None:
    """Anthropic object responses may mix text and many tool_use blocks."""
    raw_response = SimpleNamespace(
        content=[
            SimpleNamespace(type="text", text="I will check a couple of things."),
            SimpleNamespace(
                type="tool_use",
                id="toolu_1",
                name="search_documents",
                input={"query": "kitaru"},
            ),
            SimpleNamespace(
                type="tool_use",
                id="toolu_2",
                name="lookup_record",
                input={"record_id": 7},
            ),
        ],
        stop_reason="pause_turn",
        usage=SimpleNamespace(input_tokens=9, output_tokens=12),
    )

    response = _parse_anthropic_response(
        raw_response,
        resolved_model="anthropic/claude-sonnet-4-20250514",
    )

    assert response.content == "I will check a couple of things."
    assert response.finish_reason == "pause"
    assert response.provider_finish_reason == "pause_turn"
    assert [call.name for call in response.tool_calls] == [
        "search_documents",
        "lookup_record",
    ]
    assert response.tool_calls[0].arguments == {"query": "kitaru"}
    assert response.tool_calls[1].arguments == {"record_id": 7}
    assert response.usage.total_tokens == 21


@pytest.mark.parametrize("raw_response", [{}, {"choices": []}])
def test_parse_openai_compatible_rejects_missing_choices(
    raw_response: dict[str, object],
) -> None:
    """OpenAI-compatible parser should fail loudly without choices."""
    with pytest.raises(KitaruRuntimeError, match="OpenAI returned no response choices"):
        _parse_openai_compatible_response(
            raw_response,
            resolved_model="openai/gpt-4o-mini",
        )


@pytest.mark.parametrize(
    "raw_response",
    [
        {},
        {"content": []},
        {"content": "not a block list"},
    ],
)
def test_parse_anthropic_rejects_missing_or_invalid_content(
    raw_response: dict[str, object],
) -> None:
    """Anthropic parser should fail loudly without usable content blocks."""
    with pytest.raises(
        KitaruRuntimeError,
        match="Anthropic returned no response content",
    ):
        _parse_anthropic_response(
            raw_response,
            resolved_model="anthropic/claude-sonnet-4-20250514",
        )


@pytest.mark.parametrize(
    ("raw_reason", "normalized_reason"),
    [
        ("stop", "completed"),
        ("length", "max_tokens"),
        ("tool_calls", "tool_calls"),
        ("content_filter", "content_filter"),
        ("something_new", "unknown"),
        (None, "unknown"),
    ],
)
def test_parse_openai_compatible_finish_reason_mapping(
    raw_reason: str | None,
    normalized_reason: str,
) -> None:
    """OpenAI-compatible finish reasons should follow the normalized taxonomy."""
    raw_response = {
        "choices": [
            {
                "message": {"role": "assistant", "content": "ok"},
                "finish_reason": raw_reason,
            }
        ]
    }

    response = _parse_openai_compatible_response(
        raw_response,
        resolved_model="openai/gpt-4o-mini",
    )

    assert response.finish_reason == normalized_reason
    assert response.provider_finish_reason == (
        None if raw_reason is None else raw_reason
    )


@pytest.mark.parametrize(
    ("raw_reason", "normalized_reason"),
    [
        ("end_turn", "completed"),
        ("max_tokens", "max_tokens"),
        ("tool_use", "tool_calls"),
        ("pause_turn", "pause"),
        ("something_new", "unknown"),
        (None, "unknown"),
    ],
)
def test_parse_anthropic_finish_reason_mapping(
    raw_reason: str | None,
    normalized_reason: str,
) -> None:
    """Anthropic stop reasons should follow the normalized taxonomy."""
    raw_response = {
        "content": [{"type": "text", "text": "ok"}],
        "stop_reason": raw_reason,
    }

    response = _parse_anthropic_response(
        raw_response,
        resolved_model="anthropic/claude-sonnet-4-20250514",
    )

    assert response.finish_reason == normalized_reason
    assert response.provider_finish_reason == (
        None if raw_reason is None else raw_reason
    )


# ---------------------------------------------------------------------------
# Tool request validation and provider translation
# ---------------------------------------------------------------------------


def test_llm_accepts_tools_and_tool_choice_on_public_signature() -> None:
    """Flow-body calls should carry tools/tool_choice into the internal request."""
    tool = _search_tool()
    with (
        _flow_scope(name="demo_flow", execution_id=str(uuid4())),
        patch(
            "kitaru.llm._llm_checkpoint_call",
            return_value=_fake_response("ok"),
        ) as mock_synthetic,
    ):
        output = llm(
            "hello",
            model="fast",
            tools=[tool],
            tool_choice="search_documents",
            name="tool_call",
        )

    assert output.content == "ok"
    request = mock_synthetic.call_args.args[0]
    assert request.tools == [tool]
    assert request.tool_choice == "search_documents"


def test_llm_normalizes_typed_tool_definition_and_named_choice_for_dispatch() -> None:
    """Typed tool helpers should normalize before provider dispatch."""
    fake_result = _ProviderCallResult(response_text="ok", usage=_LLMUsage())
    typed_tool = LLMToolDefinition(
        name="search_documents",
        description="Search documents by query.",
        parameters={
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        },
    )

    with (
        _llm_execution_scope(model_selection=_simple_selection("openai/gpt-4o-mini")),
        patch("kitaru.llm._call_openai", return_value=fake_result) as mock_call,
    ):
        output = llm(
            "hello",
            model="openai/gpt-4o-mini",
            tools=[typed_tool],
            tool_choice="search_documents",
            name="tools_call",
        )

    assert output.content == "ok"
    call_kwargs = mock_call.call_args.kwargs
    assert call_kwargs["tools"] == [_search_tool()]
    assert call_kwargs["tool_choice"] == {
        "type": "function",
        "function": {"name": "search_documents"},
    }


@pytest.mark.parametrize(
    ("tool_choice", "expected_choice"),
    [
        ({"type": "auto"}, "auto"),
        ({"type": "required"}, "required"),
        ({"type": "none"}, "none"),
    ],
)
def test_llm_normalizes_dict_tool_choice_modes_for_openai_dispatch(
    tool_choice: dict[str, str],
    expected_choice: str,
) -> None:
    """Dict mode choices should normalize to OpenAI-compatible strings."""
    fake_result = _ProviderCallResult(response_text="ok", usage=_LLMUsage())

    with (
        _llm_execution_scope(model_selection=_simple_selection("openai/gpt-4o-mini")),
        patch("kitaru.llm._call_openai", return_value=fake_result) as mock_call,
    ):
        output = llm(
            "hello",
            model="openai/gpt-4o-mini",
            tools=[_search_tool()],
            tool_choice=tool_choice,
            name="tools_call",
        )

    assert output.content == "ok"
    assert mock_call.call_args.kwargs["tool_choice"] == expected_choice


@pytest.mark.parametrize(
    ("tools", "tool_choice", "match"),
    [
        (
            [_search_tool("search_documents"), _search_tool("search_documents")],
            None,
            "Duplicate",
        ),
        ([{"type": "function", "function": {"name": ""}}], None, "non-empty name"),
        ([_search_tool("search_documents")], "lookup_record", "does not match"),
        (None, "search_documents", "does not match"),
        (None, "auto", "requires tools"),
        (None, "required", "requires tools"),
    ],
)
def test_llm_rejects_invalid_tools_before_provider_dispatch(
    tools: list[object] | None,
    tool_choice: str | None,
    match: str,
) -> None:
    """Tool validation failures should happen before provider SDK dispatch."""
    with (
        _llm_execution_scope(model_selection=_simple_selection("openai/gpt-4o-mini")),
        patch("kitaru.llm._dispatch_provider_call") as mock_dispatch,
        pytest.raises(KitaruUsageError, match=match),
    ):
        llm(
            "hello",
            model="openai/gpt-4o-mini",
            tools=tools,  # type: ignore[arg-type]
            tool_choice=tool_choice,
            name="invalid_tools",
        )

    mock_dispatch.assert_not_called()


@pytest.mark.parametrize(
    ("messages", "match"),
    [
        ([{"role": "developer", "content": "nope"}], "Unsupported message role"),
        ([{"role": "user"}], "user.*content"),
        ([{"role": "assistant"}], "Assistant messages require"),
        ([{"role": "assistant", "content": ""}], "non-empty"),
        ([{"role": "assistant", "content": "   "}], "non-empty"),
        ([{"role": "assistant", "tool_calls": "not a list"}], "tool_calls.*list"),
        (
            [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "type": "function",
                            "function": {
                                "name": "search_documents",
                                "arguments": "{}",
                            },
                        }
                    ],
                }
            ],
            "id",
        ),
        (
            [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "type": "function",
                            "function": {"arguments": "{}"},
                        }
                    ],
                }
            ],
            "name",
        ),
        (
            [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "type": "function",
                            "function": {"name": "search_documents"},
                        }
                    ],
                }
            ],
            "arguments",
        ),
        (
            [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "name": "search_documents",
                            "arguments_json": "not json",
                        }
                    ],
                }
            ],
            "valid JSON",
        ),
        ([{"role": "tool", "content": "result"}], "tool_call_id"),
    ],
)
def test_llm_rejects_invalid_messages_before_provider_dispatch(
    messages: list[dict[str, object]],
    match: str,
) -> None:
    """Canonical message validation should fail before provider SDK dispatch."""
    with (
        _llm_execution_scope(model_selection=_simple_selection("openai/gpt-4o-mini")),
        patch("kitaru.llm._dispatch_provider_call") as mock_dispatch,
        pytest.raises(KitaruUsageError, match=match),
    ):
        llm(
            messages,
            model="openai/gpt-4o-mini",
            name="invalid_messages",
        )

    mock_dispatch.assert_not_called()


def test_call_openai_passes_tools_tool_choice_and_canonical_messages() -> None:
    """OpenAI-compatible calls should receive tools, choice, and tool messages."""
    from kitaru.llm import _call_openai

    mock_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="done"))],
        usage=SimpleNamespace(prompt_tokens=5, completion_tokens=3, total_tokens=8),
    )
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_response
    mock_openai_cls = MagicMock(return_value=mock_client)
    tool = _search_tool()
    tool_choice = {"type": "function", "function": {"name": "search_documents"}}

    with patch.dict("sys.modules", {"openai": MagicMock(OpenAI=mock_openai_cls)}):
        result = _call_openai(
            model="gpt-4o-mini",
            messages=[
                {"role": "user", "content": "Need search"},
                {
                    "role": "assistant",
                    "tool_calls": [
                        LLMToolCall(
                            id="call_1",
                            name="search_documents",
                            arguments_json='{"query":"cats"}',
                            arguments={"query": "cats"},
                        ).model_dump()
                    ],
                },
                {"role": "tool", "tool_call_id": "call_1", "content": "result"},
            ],
            temperature=None,
            max_tokens=None,
            tools=[tool],
            tool_choice=tool_choice,
            env_overlay={},
        )

    assert result.response.content == "done"
    assert result.response_text == "done"
    mock_client.chat.completions.create.assert_called_once_with(
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": "Need search"},
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "call_1",
                        "type": "function",
                        "function": {
                            "name": "search_documents",
                            "arguments": '{"query":"cats"}',
                        },
                    }
                ],
            },
            {"role": "tool", "tool_call_id": "call_1", "content": "result"},
        ],
        tools=[tool],
        tool_choice=tool_choice,
    )


def test_call_anthropic_translates_tools_choice_and_tool_messages() -> None:
    """Anthropic calls should receive translated tool and tool-result shapes."""
    from kitaru.llm import _call_anthropic

    mock_response = SimpleNamespace(
        content=[SimpleNamespace(type="text", text="done")],
        usage=SimpleNamespace(input_tokens=8, output_tokens=4),
    )
    mock_client = MagicMock()
    mock_client.messages.create.return_value = mock_response
    mock_anthropic_cls = MagicMock(return_value=mock_client)
    tool = _search_tool()
    tool_choice = {"type": "function", "function": {"name": "search_documents"}}

    with patch.dict(
        "sys.modules", {"anthropic": MagicMock(Anthropic=mock_anthropic_cls)}
    ):
        result = _call_anthropic(
            model="claude-sonnet-4-20250514",
            messages=[
                {"role": "system", "content": "You help."},
                {"role": "user", "content": "Need search"},
                {
                    "role": "assistant",
                    "content": "I will search.",
                    "tool_calls": [
                        LLMToolCall(
                            id="call_1",
                            name="search_documents",
                            arguments_json='{"query":"cats"}',
                            arguments={"query": "cats"},
                        ).model_dump()
                    ],
                },
                {"role": "tool", "tool_call_id": "call_1", "content": "result"},
            ],
            temperature=None,
            max_tokens=None,
            tools=[tool],
            tool_choice=tool_choice,
            env_overlay={},
        )

    assert result.response.content == "done"
    assert result.response_text == "done"
    mock_client.messages.create.assert_called_once_with(
        model="claude-sonnet-4-20250514",
        messages=[
            {"role": "user", "content": "Need search"},
            {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "I will search."},
                    {
                        "type": "tool_use",
                        "id": "call_1",
                        "name": "search_documents",
                        "input": {"query": "cats"},
                    },
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "call_1",
                        "content": "result",
                    }
                ],
            },
        ],
        max_tokens=4096,
        system="You help.",
        tools=[
            {
                "name": "search_documents",
                "description": "Search documents by query.",
                "input_schema": {
                    "type": "object",
                    "properties": {"query": {"type": "string"}},
                    "required": ["query"],
                },
            }
        ],
        tool_choice={"type": "tool", "name": "search_documents"},
    )


def test_call_anthropic_translates_auto_tool_choice_mode() -> None:
    """Anthropic auto tool choice should use Anthropic's mode shape."""
    from kitaru.llm import _call_anthropic

    mock_response = SimpleNamespace(
        content=[SimpleNamespace(type="text", text="done")],
        usage=SimpleNamespace(input_tokens=8, output_tokens=4),
    )
    mock_client = MagicMock()
    mock_client.messages.create.return_value = mock_response
    mock_anthropic_cls = MagicMock(return_value=mock_client)

    with patch.dict(
        "sys.modules", {"anthropic": MagicMock(Anthropic=mock_anthropic_cls)}
    ):
        _call_anthropic(
            model="claude-sonnet-4-20250514",
            messages=[{"role": "user", "content": "Need search"}],
            temperature=None,
            max_tokens=None,
            tools=[_search_tool()],
            tool_choice="auto",
            env_overlay={},
        )

    assert mock_client.messages.create.call_args.kwargs["tool_choice"] == {
        "type": "auto"
    }


# ---------------------------------------------------------------------------
# OpenAI call path
# ---------------------------------------------------------------------------


def test_llm_executes_openai_with_normalized_messages_and_tracking() -> None:
    """OpenAI path: normalized prompts, artifacts, and metadata persisted."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    fake_result = _ProviderCallResult(
        response=_fake_response(
            "hello world",
            usage=LLMUsage(prompt_tokens=10, completion_tokens=20, total_tokens=30),
        )
    )

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch(
            "kitaru.llm.resolve_model_selection",
            return_value=ResolvedModelSelection(
                requested_model="fast",
                alias="fast",
                resolved_model="openai/gpt-4o-mini",
                secret=None,
            ),
        ) as mock_resolve_model,
        patch(
            "kitaru.llm._resolve_credential_overlay", return_value=({}, "environment")
        ),
        patch("kitaru.llm._call_openai", return_value=fake_result) as mock_call_openai,
        patch("kitaru.llm.save") as mock_save,
        patch("kitaru.llm.log") as mock_log,
    ):
        output = llm(
            "Summarize this",
            model="fast",
            system="You are concise.",
            temperature=0.1,
            max_tokens=200,
            name="summary_call",
        )

    assert output.content == "hello world"
    mock_resolve_model.assert_called_once_with("fast")
    mock_call_openai.assert_called_once()
    call_kwargs = mock_call_openai.call_args.kwargs
    assert call_kwargs["model"] == "gpt-4o-mini"
    assert call_kwargs["temperature"] == 0.1
    assert call_kwargs["max_tokens"] == 200

    mock_save.assert_any_call(
        "summary_call_prompt",
        {
            "messages": [
                {"role": "system", "content": "You are concise."},
                {"role": "user", "content": "Summarize this"},
            ],
            "tools": None,
            "tool_choice": None,
            "temperature": 0.1,
            "max_tokens": 200,
        },
        type="prompt",
    )
    mock_save.assert_any_call(
        "summary_call_response",
        output.model_dump(mode="json"),
        type="response",
    )
    mock_log.assert_called_once()
    logged_payload = mock_log.call_args.kwargs["llm_calls"]["summary_call"]
    assert logged_payload["resolved_model"] == "openai/gpt-4o-mini"
    assert logged_payload["tokens_input"] == 10
    assert logged_payload["tokens_output"] == 20
    assert logged_payload["total_tokens"] == 30
    assert logged_payload["api_mode"] == "response"
    assert logged_payload["finish_reason"] == "completed"
    assert logged_payload["provider_finish_reason"] == "stop"
    assert logged_payload["response_kind"] == "text_only"
    assert logged_payload["tool_call_count"] == 0
    assert logged_payload["tool_call_names"] == []
    assert logged_payload["has_content"] is True
    # cost_usd should be absent (not provided by direct SDK calls)
    assert "cost_usd" not in logged_payload


# ---------------------------------------------------------------------------
# Anthropic call path
# ---------------------------------------------------------------------------


def test_llm_executes_anthropic_with_system_separation_and_tracking() -> None:
    """Anthropic path: system separated, usage mapped, artifacts persisted."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    fake_result = _ProviderCallResult(
        response=_fake_response(
            "bonjour",
            usage=LLMUsage(prompt_tokens=5, completion_tokens=15, total_tokens=20),
            resolved_model="anthropic/claude-sonnet-4-20250514",
        )
    )

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch(
            "kitaru.llm.resolve_model_selection",
            return_value=ResolvedModelSelection(
                requested_model="claude",
                alias="claude",
                resolved_model="anthropic/claude-sonnet-4-20250514",
                secret=None,
            ),
        ),
        patch(
            "kitaru.llm._resolve_credential_overlay", return_value=({}, "environment")
        ),
        patch(
            "kitaru.llm._call_anthropic", return_value=fake_result
        ) as mock_call_anthropic,
        patch("kitaru.llm.save") as mock_save,
        patch("kitaru.llm.log") as mock_log,
    ):
        output = llm(
            "Translate hello",
            model="claude",
            system="You translate.",
            name="translate_call",
        )

    assert output.content == "bonjour"
    mock_call_anthropic.assert_called_once()
    call_kwargs = mock_call_anthropic.call_args.kwargs
    assert call_kwargs["model"] == "claude-sonnet-4-20250514"

    mock_save.assert_any_call(
        "translate_call_response",
        output.model_dump(mode="json"),
        type="response",
    )
    mock_log.assert_called_once()
    logged_payload = mock_log.call_args.kwargs["llm_calls"]["translate_call"]
    assert logged_payload["tokens_input"] == 5
    assert logged_payload["tokens_output"] == 15
    assert logged_payload["total_tokens"] == 20
    assert logged_payload["response_kind"] == "text_only"
    assert logged_payload["finish_reason"] == "completed"
    assert "cost_usd" not in logged_payload


# ---------------------------------------------------------------------------
# Ollama call path
# ---------------------------------------------------------------------------


def test_llm_executes_ollama_via_openai_compatible_path() -> None:
    """Ollama should route through _call_openai with base_url and dummy api_key."""
    fake_result = _ProviderCallResult(
        response_text="ollama response",
        usage=_LLMUsage(prompt_tokens=10, completion_tokens=20, total_tokens=30),
    )
    with (
        _llm_execution_scope(model_selection=_simple_selection("ollama/qwen3.5")),
        patch("kitaru.llm._call_openai", return_value=fake_result) as mock_call,
    ):
        output = llm("hello", model="ollama/qwen3.5", name="ollama_call")

    assert output.content == "ollama response"
    mock_call.assert_called_once()
    call_kwargs = mock_call.call_args.kwargs
    assert "localhost:11434/v1" in call_kwargs["base_url"]
    assert call_kwargs["api_key"] == "ollama"
    assert call_kwargs["provider_label"] == "ollama"
    assert call_kwargs["model"] == "qwen3.5"


def test_ollama_respects_custom_host_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OLLAMA_HOST should override the default base URL."""
    monkeypatch.setenv("OLLAMA_HOST", "http://remote-gpu:11434")
    fake_result = _ProviderCallResult(response_text="ok", usage=_LLMUsage())
    with (
        _llm_execution_scope(model_selection=_simple_selection("ollama/qwen3.5")),
        patch("kitaru.llm._call_openai", return_value=fake_result) as mock_call,
    ):
        llm("hello", model="ollama/qwen3.5", name="test")

    assert mock_call.call_args.kwargs["base_url"] == "http://remote-gpu:11434/v1"


# ---------------------------------------------------------------------------
# OpenRouter call path
# ---------------------------------------------------------------------------


def test_llm_executes_openrouter_via_openai_compatible_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OpenRouter should route through _call_openai with base_url and API key."""
    monkeypatch.setenv("OPENROUTER_API_KEY", "or-test-key")
    fake_result = _ProviderCallResult(
        response_text="openrouter response",
        usage=_LLMUsage(prompt_tokens=5, completion_tokens=10, total_tokens=15),
    )
    with (
        _llm_execution_scope(
            model_selection=_simple_selection(
                "openrouter/anthropic/claude-sonnet-4-20250514"
            ),
        ),
        patch("kitaru.llm._call_openai", return_value=fake_result) as mock_call,
    ):
        output = llm(
            "hello",
            model="openrouter/anthropic/claude-sonnet-4-20250514",
            name="or_call",
        )

    assert output.content == "openrouter response"
    mock_call.assert_called_once()
    call_kwargs = mock_call.call_args.kwargs
    assert call_kwargs["base_url"] == "https://openrouter.ai/api/v1"
    assert call_kwargs["api_key"] == "or-test-key"
    assert call_kwargs["provider_label"] == "openrouter"
    assert call_kwargs["model"] == "anthropic/claude-sonnet-4-20250514"


def test_openrouter_uses_api_key_from_secret_overlay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OpenRouter should use the API key from a secret overlay when env is unset."""
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    fake_result = _ProviderCallResult(response_text="ok", usage=_LLMUsage())
    overlay = {"OPENROUTER_API_KEY": "secret-or-key"}
    with (
        _llm_execution_scope(
            model_selection=_simple_selection("openrouter/openai/gpt-4o"),
            credential_overlay=(overlay, "secret"),
        ),
        patch("kitaru.llm._call_openai", return_value=fake_result) as mock_call,
    ):
        llm("hello", model="openrouter/openai/gpt-4o", name="test")

    assert mock_call.call_args.kwargs["api_key"] == "secret-or-key"


# ---------------------------------------------------------------------------
# Credential resolution for new providers
# ---------------------------------------------------------------------------


def test_resolve_credential_overlay_skips_credential_check_for_ollama(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ollama should not require any API key."""
    overlay, source = _resolve_credential_overlay(
        ResolvedModelSelection(
            requested_model="ollama/qwen3.5",
            alias=None,
            resolved_model="ollama/qwen3.5",
            secret=None,
        )
    )
    assert overlay == {}
    assert source == "environment"


def test_resolve_credential_overlay_requires_openrouter_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OpenRouter should fail if OPENROUTER_API_KEY is missing and no secret."""
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)

    with pytest.raises(KitaruRuntimeError, match="No provider credentials found"):
        _resolve_credential_overlay(
            ResolvedModelSelection(
                requested_model="openrouter/openai/gpt-4o",
                alias=None,
                resolved_model="openrouter/openai/gpt-4o",
                secret=None,
            )
        )


# ---------------------------------------------------------------------------
# Missing SDK import guard for Ollama/OpenRouter
# ---------------------------------------------------------------------------


def test_llm_raises_clear_error_when_openai_not_installed_for_ollama(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing openai package should give install guidance for ollama models."""
    monkeypatch.setitem(sys.modules, "openai", None)
    with (
        _llm_execution_scope(model_selection=_simple_selection("ollama/qwen3.5")),
        pytest.raises(KitaruUsageError, match=r"kitaru\[openai\]"),
    ):
        llm("hello", model="ollama/qwen3.5", name="test_call")


# ---------------------------------------------------------------------------
# Unsupported / providerless model errors
# ---------------------------------------------------------------------------


def test_llm_rejects_providerless_model_in_real_call() -> None:
    """A bare model like 'gpt-4o-mini' should fail at runtime routing."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch(
            "kitaru.llm.resolve_model_selection",
            return_value=ResolvedModelSelection(
                requested_model="gpt-4o-mini",
                alias=None,
                resolved_model="gpt-4o-mini",
                secret=None,
            ),
        ),
        patch(
            "kitaru.llm._resolve_credential_overlay", return_value=({}, "environment")
        ),
        pytest.raises(KitaruUsageError, match="provider prefix"),
    ):
        llm("hello", model="gpt-4o-mini", name="test_call")


def test_llm_rejects_unsupported_provider_in_real_call() -> None:
    """An unsupported provider like 'gemini/' should fail at runtime routing."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch(
            "kitaru.llm.resolve_model_selection",
            return_value=ResolvedModelSelection(
                requested_model="gemini/gemini-2.0-flash",
                alias=None,
                resolved_model="gemini/gemini-2.0-flash",
                secret=None,
            ),
        ),
        patch(
            "kitaru.llm._resolve_credential_overlay", return_value=({}, "environment")
        ),
        pytest.raises(KitaruUsageError, match="not supported"),
    ):
        llm("hello", model="gemini/gemini-2.0-flash", name="test_call")


# ---------------------------------------------------------------------------
# Mock short-circuit
# ---------------------------------------------------------------------------


def test_llm_mock_response_skips_provider_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """KITARU_LLM_MOCK_RESPONSE should short-circuit without calling any SDK."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    monkeypatch.setenv("KITARU_LLM_MOCK_RESPONSE", "mocked answer")

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch(
            "kitaru.llm.resolve_model_selection",
            return_value=ResolvedModelSelection(
                requested_model="fast",
                alias="fast",
                resolved_model="openai/gpt-4o-mini",
                secret=None,
            ),
        ),
        patch(
            "kitaru.llm._resolve_credential_overlay", return_value=({}, "environment")
        ),
        patch("kitaru.llm._call_openai") as mock_openai,
        patch("kitaru.llm._call_anthropic") as mock_anthropic,
        patch("kitaru.llm.save") as mock_save,
        patch("kitaru.llm.log") as mock_log,
    ):
        output = llm("hello", model="fast", name="mock_call")

    assert output.content == "mocked answer"
    mock_openai.assert_not_called()
    mock_anthropic.assert_not_called()
    # Artifacts and metadata should still be persisted
    mock_save.assert_called()
    mock_log.assert_called_once()


def test_llm_mock_response_works_with_unsupported_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mock mode should work even with providers that would fail in real calls."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    monkeypatch.setenv("KITARU_LLM_MOCK_RESPONSE", "mocked")

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch(
            "kitaru.llm.resolve_model_selection",
            return_value=ResolvedModelSelection(
                requested_model="gemini/gemini-2.0-flash",
                alias=None,
                resolved_model="gemini/gemini-2.0-flash",
                secret=None,
            ),
        ),
        patch(
            "kitaru.llm._resolve_credential_overlay", return_value=({}, "environment")
        ),
        patch("kitaru.llm.save"),
        patch("kitaru.llm.log"),
    ):
        output = llm("hello", model="gemini/gemini-2.0-flash", name="mock_call")

    assert output.content == "mocked"


@pytest.mark.parametrize(
    ("mock_value", "match"),
    [
        ("not-json", "valid JSON"),
        ("[]", "JSON object"),
        (json.dumps({"text": "hello"}), "unsupported keys"),
        (json.dumps({}), "requires `content`, `tool_calls`, or both"),
    ],
)
def test_llm_structured_mock_response_rejects_invalid_payloads(
    monkeypatch: pytest.MonkeyPatch,
    mock_value: str,
    match: str,
) -> None:
    """Structured mock mode should fail loudly for invalid response JSON."""
    monkeypatch.setenv("KITARU_LLM_MOCK_RESPONSE_JSON", mock_value)

    with (
        _llm_execution_scope(model_selection=_simple_selection("openai/gpt-4o-mini")),
        pytest.raises(KitaruUsageError, match=match),
    ):
        llm("hello", model="openai/gpt-4o-mini", name="bad_structured_mock")


def test_llm_structured_mock_response_can_return_tool_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Structured mock mode should produce a rich LLMResponse with tool calls."""
    tool_call_payload = {
        "id": "call_1",
        "name": "search_documents",
        "arguments_json": '{"query":"cats"}',
        "arguments": {"query": "cats"},
    }
    monkeypatch.setenv(
        "KITARU_LLM_MOCK_RESPONSE_JSON",
        json.dumps(
            {
                "content": "I need to search.",
                "tool_calls": [tool_call_payload],
                "finish_reason": "tool_calls",
                "provider_finish_reason": "mock_tool_calls",
                "usage": {
                    "prompt_tokens": 2,
                    "completion_tokens": 3,
                    "total_tokens": 5,
                },
            }
        ),
    )
    model_selection = ResolvedModelSelection(
        requested_model="fast",
        alias="fast",
        resolved_model="openai/gpt-4o-mini",
        secret=None,
    )

    with _llm_execution_scope(model_selection=model_selection) as (mock_save, mock_log):
        output = llm(
            "private prompt text",
            model="fast",
            tools=[_search_tool()],
            tool_choice="search_documents",
            name="structured_mock",
        )

    assert output.content == "I need to search."
    assert output.finish_reason == "tool_calls"
    assert output.tool_calls[0].name == "search_documents"
    assert output.tool_calls[0].arguments == {"query": "cats"}
    mock_save.assert_any_call(
        "structured_mock_prompt",
        {
            "messages": [{"role": "user", "content": "private prompt text"}],
            "tools": [_search_tool()],
            "tool_choice": {
                "type": "function",
                "function": {"name": "search_documents"},
            },
            "temperature": None,
            "max_tokens": None,
        },
        type="prompt",
    )
    mock_save.assert_any_call(
        "structured_mock_response",
        output.model_dump(mode="json"),
        type="response",
    )
    logged_payload = mock_log.call_args.kwargs["llm_calls"]["structured_mock"]
    assert logged_payload["response_kind"] == "mixed"
    assert logged_payload["tool_call_count"] == 1
    assert logged_payload["tool_call_names"] == ["search_documents"]
    assert "cats" not in logged_payload.values()


# ---------------------------------------------------------------------------
# Analytics metadata
# ---------------------------------------------------------------------------


def test_llm_analytics_includes_model_alias_for_explicit_model() -> None:
    """LLM analytics should expose `model` as an alias for `resolved_model`."""
    fake_result = _ProviderCallResult(response_text="ok", usage=_LLMUsage())
    with (
        _llm_execution_scope(model_selection=_simple_selection("openai/gpt-4o-mini")),
        patch("kitaru.llm._call_openai", return_value=fake_result),
        patch("kitaru.analytics.track", return_value=True) as track_mock,
    ):
        llm(
            "private prompt text",
            model="openai/gpt-4o-mini",
            system="private system text",
            name="private_call_name",
        )

    metadata = _tracked_llm_metadata(track_mock)
    assert metadata["resolved_model"] == "openai/gpt-4o-mini"
    assert metadata["model"] == "openai/gpt-4o-mini"
    assert metadata["model"] == metadata["resolved_model"]
    assert "prompt" not in metadata
    assert "system" not in metadata
    assert "call_name" not in metadata
    assert "name" not in metadata
    assert "private prompt text" not in metadata.values()
    assert "private system text" not in metadata.values()
    assert "private_call_name" not in metadata.values()


def test_llm_analytics_includes_model_alias_in_mock_alias_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mock/default-alias calls should still emit safe model metadata."""
    monkeypatch.setenv("KITARU_LLM_MOCK_RESPONSE", "mocked")
    model_selection = ResolvedModelSelection(
        requested_model="fast",
        alias="fast",
        resolved_model="openai/gpt-4o-mini",
        secret=None,
    )

    with (
        _llm_execution_scope(model_selection=model_selection),
        patch("kitaru.analytics.track", return_value=True) as track_mock,
    ):
        llm("private prompt text", name="private_call_name")

    metadata = _tracked_llm_metadata(track_mock)
    assert metadata["resolved_model"] == "openai/gpt-4o-mini"
    assert metadata["model"] == "openai/gpt-4o-mini"
    assert metadata["model"] == metadata["resolved_model"]
    assert metadata["mocked"] is True
    assert metadata["api_mode"] == "response"
    assert metadata["tools_supplied"] is False
    assert metadata["tool_count"] == 0
    assert metadata["tool_calls_returned"] is False
    assert metadata["tool_call_count"] == 0
    assert metadata["finish_reason"] == "completed"
    assert "prompt" not in metadata
    assert "system" not in metadata
    assert "call_name" not in metadata
    assert "name" not in metadata
    assert "private prompt text" not in metadata.values()
    assert "private_call_name" not in metadata.values()


def test_llm_analytics_excludes_tool_names_and_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Analytics flags should stay low-cardinality for tool-capable calls."""
    monkeypatch.setenv(
        "KITARU_LLM_MOCK_RESPONSE_JSON",
        json.dumps(
            {
                "tool_calls": [
                    {
                        "id": "call_1",
                        "name": "search_documents",
                        "arguments_json": '{"query":"cats"}',
                        "arguments": {"query": "cats"},
                    }
                ],
                "finish_reason": "tool_calls",
                "provider_finish_reason": "mock_tool_calls",
            }
        ),
    )

    with (
        _llm_execution_scope(model_selection=_simple_selection("openai/gpt-4o-mini")),
        patch("kitaru.analytics.track", return_value=True) as track_mock,
    ):
        llm(
            "private prompt text",
            model="openai/gpt-4o-mini",
            tools=[_search_tool()],
            tool_choice="search_documents",
            name="private_call_name",
        )

    metadata = _tracked_llm_metadata(track_mock)
    assert metadata["api_mode"] == "response"
    assert metadata["tools_supplied"] is True
    assert metadata["tool_count"] == 1
    assert metadata["tool_calls_returned"] is True
    assert metadata["tool_call_count"] == 1
    assert metadata["finish_reason"] == "tool_calls"
    assert "search_documents" not in metadata.values()
    assert "cats" not in metadata.values()
    assert "private prompt text" not in metadata.values()


# ---------------------------------------------------------------------------
# Missing SDK import guards
# ---------------------------------------------------------------------------


def test_llm_raises_clear_error_when_openai_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing openai package should give install guidance."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    monkeypatch.setitem(sys.modules, "openai", None)

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch(
            "kitaru.llm.resolve_model_selection",
            return_value=ResolvedModelSelection(
                requested_model="openai/gpt-4o-mini",
                alias=None,
                resolved_model="openai/gpt-4o-mini",
                secret=None,
            ),
        ),
        patch(
            "kitaru.llm._resolve_credential_overlay", return_value=({}, "environment")
        ),
        pytest.raises(KitaruUsageError, match=r"kitaru\[openai\]"),
    ):
        llm("hello", model="openai/gpt-4o-mini", name="test_call")


def test_llm_raises_clear_error_when_anthropic_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing anthropic package should give install guidance."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    monkeypatch.setitem(sys.modules, "anthropic", None)

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch(
            "kitaru.llm.resolve_model_selection",
            return_value=ResolvedModelSelection(
                requested_model="anthropic/claude-sonnet-4-20250514",
                alias=None,
                resolved_model="anthropic/claude-sonnet-4-20250514",
                secret=None,
            ),
        ),
        patch(
            "kitaru.llm._resolve_credential_overlay", return_value=({}, "environment")
        ),
        pytest.raises(KitaruUsageError, match=r"kitaru\[anthropic\]"),
    ):
        llm(
            "hello",
            model="anthropic/claude-sonnet-4-20250514",
            name="test_call",
        )


# ---------------------------------------------------------------------------
# Artifact fallback (unchanged behavior)
# ---------------------------------------------------------------------------


def test_llm_falls_back_to_blob_when_artifact_save_fails() -> None:
    """LLM tracking should fall back to blob artifacts when save serialization fails."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    fake_result = _ProviderCallResult(
        response=_fake_response(
            "hello world",
            usage=LLMUsage(prompt_tokens=10, completion_tokens=20, total_tokens=30),
        )
    )
    save_attempts: list[tuple[str, str, object]] = []

    def fake_save(name: str, value: object, *, type: str = "output") -> None:
        save_attempts.append((name, type, value))
        if type in {"prompt", "response"}:
            raise TypeError("cannot serialize")

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch(
            "kitaru.llm.resolve_model_selection",
            return_value=ResolvedModelSelection(
                requested_model="fast",
                alias="fast",
                resolved_model="openai/gpt-4o-mini",
                secret=None,
            ),
        ),
        patch(
            "kitaru.llm._resolve_credential_overlay", return_value=({}, "environment")
        ),
        patch("kitaru.llm._call_openai", return_value=fake_result),
        patch("kitaru.llm.save", side_effect=fake_save),
        patch("kitaru.llm.log") as mock_log,
    ):
        output = llm("Summarize this", model="fast", name="summary_call")

    assert output.content == "hello world"
    prompt_envelope = {
        "messages": [{"role": "user", "content": "Summarize this"}],
        "tools": None,
        "tool_choice": None,
        "temperature": None,
        "max_tokens": None,
    }
    response_payload = output.model_dump(mode="json")
    assert save_attempts == [
        ("summary_call_prompt", "prompt", prompt_envelope),
        (
            "summary_call_prompt",
            "blob",
            {"repr": repr(prompt_envelope), "python_type": "dict"},
        ),
        ("summary_call_response", "response", response_payload),
        (
            "summary_call_response",
            "blob",
            {"repr": repr(response_payload), "python_type": "dict"},
        ),
    ]
    mock_log.assert_called_once()


# ---------------------------------------------------------------------------
# Env default model (unchanged behavior)
# ---------------------------------------------------------------------------


def test_llm_uses_env_default_model_when_no_explicit_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The llm call path should honor KITARU_DEFAULT_MODEL."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    monkeypatch.setenv("KITARU_DEFAULT_MODEL", "fast")
    monkeypatch.setenv("KITARU_LLM_MOCK_RESPONSE", "mocked")

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch(
            "kitaru.llm.resolve_model_selection",
            return_value=ResolvedModelSelection(
                requested_model="fast",
                alias="fast",
                resolved_model="openai/gpt-4o-mini",
                secret=None,
            ),
        ) as mock_resolve_model,
        patch(
            "kitaru.llm._resolve_credential_overlay", return_value=({}, "environment")
        ),
        patch("kitaru.llm.save"),
        patch("kitaru.llm.log"),
    ):
        llm("Summarize this")

    mock_resolve_model.assert_called_once_with(None)


def test_llm_explicit_model_beats_env_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit model should still beat KITARU_DEFAULT_MODEL."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    monkeypatch.setenv("KITARU_DEFAULT_MODEL", "fast")
    monkeypatch.setenv("KITARU_LLM_MOCK_RESPONSE", "mocked")

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="demo_checkpoint",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
        patch(
            "kitaru.llm.resolve_model_selection",
            return_value=ResolvedModelSelection(
                requested_model="openai/gpt-4.1-mini",
                alias=None,
                resolved_model="openai/gpt-4.1-mini",
                secret=None,
            ),
        ) as mock_resolve_model,
        patch(
            "kitaru.llm._resolve_credential_overlay", return_value=({}, "environment")
        ),
        patch("kitaru.llm.save"),
        patch("kitaru.llm.log"),
    ):
        llm("Summarize this", model="openai/gpt-4.1-mini")

    mock_resolve_model.assert_called_once_with("openai/gpt-4.1-mini")


# ---------------------------------------------------------------------------
# Credential overlay (unchanged behavior)
# ---------------------------------------------------------------------------


def test_resolve_credential_overlay_prefers_environment_for_known_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Known providers should use env credentials before secret lookup."""
    monkeypatch.setenv("OPENAI_API_KEY", "env-key")

    overlay, source = _resolve_credential_overlay(
        ResolvedModelSelection(
            requested_model="fast",
            alias="fast",
            resolved_model="openai/gpt-4o-mini",
            secret="openai-creds",
        )
    )

    assert overlay == {}
    assert source == "environment"


def test_resolve_credential_overlay_uses_secret_when_env_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Known providers should fall back to configured secret values."""
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    with patch(
        "kitaru.llm._read_secret_values",
        return_value={"OPENAI_API_KEY": "secret-key"},
    ) as mock_read_secret:
        overlay, source = _resolve_credential_overlay(
            ResolvedModelSelection(
                requested_model="fast",
                alias="fast",
                resolved_model="openai/gpt-4o-mini",
                secret="openai-creds",
            )
        )

    mock_read_secret.assert_called_once_with("openai-creds")
    assert overlay == {"OPENAI_API_KEY": "secret-key"}
    assert source == "secret"


def test_resolve_credential_overlay_errors_without_known_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Known providers should fail with guidance if env and secret are absent."""
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    with pytest.raises(KitaruRuntimeError, match="No provider credentials found"):
        _resolve_credential_overlay(
            ResolvedModelSelection(
                requested_model="openai/gpt-4o-mini",
                alias=None,
                resolved_model="openai/gpt-4o-mini",
                secret=None,
            )
        )


# ---------------------------------------------------------------------------
# Direct provider SDK integration (verifies correct SDK invocation)
# ---------------------------------------------------------------------------


def test_call_openai_returns_rich_tool_only_response() -> None:
    """_call_openai should preserve provider tool-call-only responses."""
    from kitaru.llm import _call_openai

    mock_response = SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(
                    content=None,
                    tool_calls=[
                        SimpleNamespace(
                            id="call_1",
                            function=SimpleNamespace(
                                name="search_documents",
                                arguments='{"query":"cats"}',
                            ),
                        )
                    ],
                ),
                finish_reason="tool_calls",
            )
        ],
        usage=SimpleNamespace(prompt_tokens=5, completion_tokens=3, total_tokens=8),
    )
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_response
    mock_openai_cls = MagicMock(return_value=mock_client)

    with patch.dict("sys.modules", {"openai": MagicMock(OpenAI=mock_openai_cls)}):
        result = _call_openai(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "hi"}],
            temperature=None,
            max_tokens=None,
            env_overlay={},
        )

    assert result.response.content is None
    assert result.response.has_tool_calls is True
    assert result.response.finish_reason == "tool_calls"
    assert result.response.tool_calls[0].arguments == {"query": "cats"}
    with pytest.raises(KitaruRuntimeError, match="no text content"):
        _ = result.response_text


def test_call_anthropic_returns_rich_tool_only_response() -> None:
    """_call_anthropic should preserve provider tool-use-only responses."""
    from kitaru.llm import _call_anthropic

    mock_response = SimpleNamespace(
        content=[
            SimpleNamespace(
                type="tool_use",
                id="toolu_1",
                name="search_documents",
                input={"query": "cats"},
            )
        ],
        stop_reason="tool_use",
        usage=SimpleNamespace(input_tokens=8, output_tokens=4),
    )
    mock_client = MagicMock()
    mock_client.messages.create.return_value = mock_response
    mock_anthropic_cls = MagicMock(return_value=mock_client)

    with patch.dict(
        "sys.modules", {"anthropic": MagicMock(Anthropic=mock_anthropic_cls)}
    ):
        result = _call_anthropic(
            model="claude-sonnet-4-20250514",
            messages=[{"role": "user", "content": "hello"}],
            temperature=None,
            max_tokens=None,
            env_overlay={},
        )

    assert result.response.content is None
    assert result.response.has_tool_calls is True
    assert result.response.finish_reason == "tool_calls"
    assert result.response.tool_calls[0].arguments == {"query": "cats"}
    with pytest.raises(KitaruRuntimeError, match="no text content"):
        _ = result.response_text


def test_call_openai_passes_correct_parameters() -> None:
    """_call_openai should invoke OpenAI chat completions with correct args."""
    from kitaru.llm import _call_openai

    mock_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="hi"))],
        usage=SimpleNamespace(prompt_tokens=5, completion_tokens=3, total_tokens=8),
    )
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_response
    mock_openai_cls = MagicMock(return_value=mock_client)

    with patch.dict("sys.modules", {"openai": MagicMock(OpenAI=mock_openai_cls)}):
        result = _call_openai(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "hi"}],
            temperature=0.5,
            max_tokens=100,
            env_overlay={},
        )

    assert result.response.content == "hi"
    assert result.response_text == "hi"
    assert result.usage.prompt_tokens == 5
    assert result.usage.completion_tokens == 3
    mock_client.chat.completions.create.assert_called_once_with(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": "hi"}],
        temperature=0.5,
        max_tokens=100,
    )


def test_call_anthropic_separates_system_and_maps_usage() -> None:
    """_call_anthropic should extract system prompt and map usage fields."""
    from kitaru.llm import _call_anthropic

    mock_response = SimpleNamespace(
        content=[SimpleNamespace(type="text", text="bonjour")],
        usage=SimpleNamespace(input_tokens=8, output_tokens=4),
    )
    mock_client = MagicMock()
    mock_client.messages.create.return_value = mock_response
    mock_anthropic_cls = MagicMock(return_value=mock_client)

    with patch.dict(
        "sys.modules", {"anthropic": MagicMock(Anthropic=mock_anthropic_cls)}
    ):
        result = _call_anthropic(
            model="claude-sonnet-4-20250514",
            messages=[
                {"role": "system", "content": "You translate."},
                {"role": "user", "content": "hello"},
            ],
            temperature=None,
            max_tokens=None,
            env_overlay={},
        )

    assert result.response.content == "bonjour"
    assert result.response_text == "bonjour"
    assert result.usage.prompt_tokens == 8
    assert result.usage.completion_tokens == 4
    assert result.usage.total_tokens == 12
    mock_client.messages.create.assert_called_once_with(
        model="claude-sonnet-4-20250514",
        messages=[{"role": "user", "content": "hello"}],
        max_tokens=4096,  # default when caller omits max_tokens
        system="You translate.",
    )


def test_call_anthropic_rejects_interleaved_system_messages() -> None:
    """System messages after non-system messages should raise."""
    from kitaru.llm import _call_anthropic

    # Need a mock module so the lazy import doesn't fail before we test
    with (
        patch.dict("sys.modules", {"anthropic": MagicMock()}),
        pytest.raises(KitaruUsageError, match="System messages must appear"),
    ):
        _call_anthropic(
            model="claude-sonnet-4-20250514",
            messages=[
                {"role": "user", "content": "hello"},
                {"role": "system", "content": "late system"},
            ],
            temperature=None,
            max_tokens=None,
            env_overlay={},
        )
