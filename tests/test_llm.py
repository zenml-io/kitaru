"""Tests for `kitaru.llm()` runtime and normalization behavior."""

from __future__ import annotations

import sys
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from kitaru.config import ResolvedModelSelection
from kitaru.errors import (
    KitaruContextError,
    KitaruRuntimeError,
    KitaruUsageError,
)
from kitaru.llm import (
    _LLMUsage,
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
        patch("kitaru.llm._execute_llm_call", return_value="ok") as mock_execute,
        patch("kitaru.llm._llm_checkpoint_call") as mock_synthetic,
    ):
        response = llm("hello", model="fast")

    assert response == "ok"
    mock_execute.assert_called_once()
    mock_synthetic.assert_not_called()


def test_llm_dispatches_through_synthetic_checkpoint_in_flow_scope() -> None:
    """In flow scope (outside checkpoints), llm should call the synthetic boundary."""
    with (
        _flow_scope(name="demo_flow", execution_id=str(uuid4())),
        patch("kitaru.llm._llm_checkpoint_call", return_value="ok") as mock_synthetic,
    ):
        response = llm("hello", model="fast", name="outline")

    assert response == "ok"
    mock_synthetic.assert_called_once()
    request = mock_synthetic.call_args.args[0]
    assert request.call_name == "outline"
    assert request.model == "fast"
    assert mock_synthetic.call_args.kwargs["id"] == "outline"


def test_llm_auto_names_calls_sequentially_within_flow_scope() -> None:
    """Unnamed calls should receive deterministic runtime-local names."""
    with (
        _flow_scope(name="demo_flow", execution_id=str(uuid4())),
        patch("kitaru.llm._llm_checkpoint_call", return_value="ok") as mock_synthetic,
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
# OpenAI call path
# ---------------------------------------------------------------------------


def test_llm_executes_openai_with_normalized_messages_and_tracking() -> None:
    """OpenAI path: normalized prompts, artifacts, and metadata persisted."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    fake_result = _ProviderCallResult(
        response_text="hello world",
        usage=_LLMUsage(prompt_tokens=10, completion_tokens=20, total_tokens=30),
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

    assert output == "hello world"
    mock_resolve_model.assert_called_once_with("fast")
    mock_call_openai.assert_called_once()
    call_kwargs = mock_call_openai.call_args.kwargs
    assert call_kwargs["model"] == "gpt-4o-mini"
    assert call_kwargs["temperature"] == 0.1
    assert call_kwargs["max_tokens"] == 200

    mock_save.assert_any_call(
        "summary_call_prompt",
        [
            {"role": "system", "content": "You are concise."},
            {"role": "user", "content": "Summarize this"},
        ],
        type="prompt",
    )
    mock_save.assert_any_call("summary_call_response", "hello world", type="response")
    mock_log.assert_called_once()
    logged_payload = mock_log.call_args.kwargs["llm_calls"]["summary_call"]
    assert logged_payload["resolved_model"] == "openai/gpt-4o-mini"
    assert logged_payload["tokens_input"] == 10
    assert logged_payload["tokens_output"] == 20
    assert logged_payload["total_tokens"] == 30
    # cost_usd should be absent (not provided by direct SDK calls)
    assert "cost_usd" not in logged_payload


# ---------------------------------------------------------------------------
# Anthropic call path
# ---------------------------------------------------------------------------


def test_llm_executes_anthropic_with_system_separation_and_tracking() -> None:
    """Anthropic path: system separated, usage mapped, artifacts persisted."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    fake_result = _ProviderCallResult(
        response_text="bonjour",
        usage=_LLMUsage(prompt_tokens=5, completion_tokens=15, total_tokens=20),
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

    assert output == "bonjour"
    mock_call_anthropic.assert_called_once()
    call_kwargs = mock_call_anthropic.call_args.kwargs
    assert call_kwargs["model"] == "claude-sonnet-4-20250514"

    mock_save.assert_any_call("translate_call_response", "bonjour", type="response")
    mock_log.assert_called_once()
    logged_payload = mock_log.call_args.kwargs["llm_calls"]["translate_call"]
    assert logged_payload["tokens_input"] == 5
    assert logged_payload["tokens_output"] == 15
    assert logged_payload["total_tokens"] == 20
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

    assert output == "ollama response"
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

    assert output == "openrouter response"
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

    assert output == "mocked answer"
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

    assert output == "mocked"


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
        response_text="hello world",
        usage=_LLMUsage(prompt_tokens=10, completion_tokens=20, total_tokens=30),
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

    assert output == "hello world"
    assert save_attempts == [
        (
            "summary_call_prompt",
            "prompt",
            [{"role": "user", "content": "Summarize this"}],
        ),
        (
            "summary_call_prompt",
            "blob",
            {
                "repr": repr([{"role": "user", "content": "Summarize this"}]),
                "python_type": "list",
            },
        ),
        ("summary_call_response", "response", "hello world"),
        (
            "summary_call_response",
            "blob",
            {
                "repr": repr("hello world"),
                "python_type": "str",
            },
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
