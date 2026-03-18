"""Tests for `kitaru.llm()` runtime and normalization behavior."""

from __future__ import annotations

import logging
import sys
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import pytest

from kitaru.config import ResolvedModelSelection
from kitaru.errors import KitaruContextError, KitaruRuntimeError
from kitaru.llm import (
    _configure_litellm_terminal_logger,
    _resolve_credential_overlay,
    llm,
)
from kitaru.runtime import _checkpoint_scope, _flow_scope


def _flow_checkpoint_scope() -> tuple[str, str]:
    """Return valid execution/checkpoint IDs for scope setup."""
    return str(uuid4()), str(uuid4())


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


def test_configure_litellm_terminal_logger_removes_console_handlers() -> None:
    """LiteLLM should not keep its own stderr handler attached under Kitaru."""
    logger = logging.getLogger("LiteLLM")
    original_handlers = list(logger.handlers)
    original_propagate = logger.propagate
    stray_handler = logging.StreamHandler(sys.stderr)
    logger.handlers = [stray_handler]
    logger.propagate = False

    try:
        _configure_litellm_terminal_logger()

        assert logger.handlers == []
        assert logger.propagate is True
    finally:
        logger.handlers = original_handlers
        logger.propagate = original_propagate
        stray_handler.close()


def test_llm_executes_litellm_with_normalized_messages_and_tracking() -> None:
    """LLM execution should normalize prompts and persist artifacts/metadata."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    fake_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="hello world"))],
        usage=SimpleNamespace(prompt_tokens=10, completion_tokens=20, total_tokens=30),
        _hidden_params={"response_cost": 0.0025},
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
        patch("kitaru.llm._configure_litellm_terminal_logger") as mock_configure_logger,
        patch("kitaru.llm.completion", return_value=fake_response) as mock_completion,
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
    mock_configure_logger.assert_called_once_with()
    mock_completion.assert_called_once_with(
        model="openai/gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are concise."},
            {"role": "user", "content": "Summarize this"},
        ],
        temperature=0.1,
        max_tokens=200,
    )
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
    assert logged_payload["cost_usd"] == 0.0025


def test_llm_falls_back_to_blob_when_artifact_save_fails() -> None:
    """LLM tracking should fall back to blob artifacts when save serialization fails."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    fake_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="hello world"))],
        usage=SimpleNamespace(prompt_tokens=10, completion_tokens=20, total_tokens=30),
        _hidden_params={"response_cost": 0.0025},
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
        patch("kitaru.llm.completion", return_value=fake_response),
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


def test_llm_uses_env_default_model_when_no_explicit_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The llm call path should honor KITARU_DEFAULT_MODEL."""
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    fake_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="hello world"))],
        usage=SimpleNamespace(prompt_tokens=1, completion_tokens=1, total_tokens=2),
        _hidden_params={},
    )
    monkeypatch.setenv("KITARU_DEFAULT_MODEL", "fast")

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
        patch("kitaru.llm.completion", return_value=fake_response),
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
    fake_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="hello world"))],
        usage=SimpleNamespace(prompt_tokens=1, completion_tokens=1, total_tokens=2),
        _hidden_params={},
    )
    monkeypatch.setenv("KITARU_DEFAULT_MODEL", "fast")

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
        patch("kitaru.llm.completion", return_value=fake_response),
        patch("kitaru.llm.save"),
        patch("kitaru.llm.log"),
    ):
        llm("Summarize this", model="openai/gpt-4.1-mini")

    mock_resolve_model.assert_called_once_with("openai/gpt-4.1-mini")


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
