"""Tests for `kitaru.llm()` runtime and normalization behavior."""

from __future__ import annotations

import importlib
import sys
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from pydantic import ValidationError

from kitaru._llm_usage import (
    LLM_USAGE_METADATA_KEY,
    aggregate_usage_records_with_cost_completeness,
)
from kitaru.analytics import AnalyticsEvent
from kitaru.config import (
    ResolvedModelSelection,
    configure,
    register_model_alias,
    resolve_llm_estimated_cost_policy,
)
from kitaru.errors import (
    KitaruBackendError,
    KitaruContextError,
    KitaruRuntimeError,
    KitaruUsageError,
)
from kitaru.flow import flow
from kitaru.llm import (
    _estimate_direct_llm_cost,
    _extract_usage_anthropic,
    _extract_usage_openai,
    _LLMRequest,
    _LLMUsage,
    _openai_token_limit_param,
    _parse_provider_target,
    _ProviderCallResult,
    _resolve_credential_overlay,
    llm,
)
from kitaru.runtime import _checkpoint_scope, _flow_scope
from tests._checkpoint_handle_helpers import (
    assert_checkpoint_handle_error,
    checkpoint_output_handle,
)
from tests._diff_helpers import checkpoint_diff_from_usage_records


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


def _mock_openai_chat_client(
    *, response_text: str = "ok"
) -> tuple[MagicMock, MagicMock]:
    """Return a fake OpenAI chat client and OpenAI constructor."""
    mock_response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=response_text))],
        usage=SimpleNamespace(prompt_tokens=1, completion_tokens=2, total_tokens=3),
    )
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_response
    mock_openai_cls = MagicMock(return_value=mock_client)
    return mock_client, mock_openai_cls


def _assert_llm_attempted_call(
    track_mock: MagicMock, *, inside_checkpoint: bool
) -> None:
    """Assert the first analytics call is the shared LLM attempt event."""
    event, metadata = track_mock.call_args_list[0].args
    assert event == AnalyticsEvent.LLM_ATTEMPTED
    assert metadata == {
        "llm_path": "direct_llm",
        "inside_checkpoint": inside_checkpoint,
    }


def _tracked_llm_metadata(
    track_mock: MagicMock, *, inside_checkpoint: bool = True
) -> dict[str, object]:
    """Return the success analytics metadata emitted by one LLM call."""
    assert [call_args.args[0] for call_args in track_mock.call_args_list] == [
        AnalyticsEvent.LLM_ATTEMPTED,
        AnalyticsEvent.LLM_CALLED,
    ]
    _assert_llm_attempted_call(track_mock, inside_checkpoint=inside_checkpoint)
    metadata = track_mock.call_args_list[1].args[1]
    assert isinstance(metadata, dict)
    return metadata


def _tracked_llm_failure_metadata(
    track_mock: MagicMock, *, inside_checkpoint: bool = True
) -> dict[str, object]:
    """Return the failure analytics metadata emitted by one failed LLM call."""
    assert [call_args.args[0] for call_args in track_mock.call_args_list] == [
        AnalyticsEvent.LLM_ATTEMPTED,
        AnalyticsEvent.LLM_FAILED,
    ]
    _assert_llm_attempted_call(track_mock, inside_checkpoint=inside_checkpoint)
    metadata = track_mock.call_args_list[1].args[1]
    assert isinstance(metadata, dict)
    return metadata


def _assert_metadata_excludes(
    metadata: Mapping[str, object],
    *,
    keys: Sequence[str] = (),
    values: Sequence[object] = (),
) -> None:
    """Assert analytics metadata excludes privacy-sensitive keys and values."""
    for key in keys:
        assert key not in metadata
    for value in values:
        assert value not in metadata.values()


def _install_fake_genai_prices(
    monkeypatch: pytest.MonkeyPatch,
    *,
    total_price: float = 0.00123,
    extract_error: Exception | None = None,
    calc_error: Exception | None = None,
) -> MagicMock:
    """Install a fake genai_prices module for deterministic pricing tests."""

    class _FakeExtractedUsage:
        def calc_price(self) -> SimpleNamespace:
            if calc_error is not None:
                raise calc_error
            return SimpleNamespace(total_price=total_price)

    def _extract_usage(*_args: Any, **_kwargs: Any) -> _FakeExtractedUsage:
        if extract_error is not None:
            raise extract_error
        return _FakeExtractedUsage()

    extract_usage = MagicMock(side_effect=_extract_usage)
    monkeypatch.setitem(
        sys.modules,
        "genai_prices",
        SimpleNamespace(extract_usage=extract_usage),
    )
    return extract_usage


def _single_usage_record(mock_log: MagicMock) -> dict[str, Any]:
    """Return the only canonical usage record from one mocked log call."""
    usage_records = mock_log.call_args.kwargs[LLM_USAGE_METADATA_KEY]
    assert len(usage_records) == 1
    return next(iter(usage_records.values()))


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


def test_llm_request_rejects_checkpoint_handles() -> None:
    handle = checkpoint_output_handle()

    with pytest.raises(ValidationError) as prompt_exc:
        _LLMRequest(
            prompt=cast(str, handle),
            model="fast",
            call_name="demo",
        )
    assert_checkpoint_handle_error(
        prompt_exc,
        field_name="_LLMRequest.prompt",
    )

    with pytest.raises(ValidationError) as system_exc:
        _LLMRequest(
            prompt="hello",
            model="fast",
            system=cast(str, handle),
            call_name="demo",
        )
    assert_checkpoint_handle_error(
        system_exc,
        field_name="_LLMRequest.system",
    )

    with pytest.raises(ValidationError) as nested_exc:
        _LLMRequest(
            prompt=[{"role": "user", "content": [{"text": handle}]}],
            model="fast",
            call_name="demo",
        )
    assert_checkpoint_handle_error(
        nested_exc,
        field_name="_LLMRequest.prompt[0].content[0].text",
    )

    valid = _LLMRequest(
        prompt=[{"role": "user", "content": "hello"}],
        model="fast",
        system="You are helpful.",
        call_name="demo",
    )
    assert valid.prompt == [{"role": "user", "content": "hello"}]
    assert valid.system == "You are helpful."


def test_public_llm_rejects_checkpoint_handles_before_dispatch() -> None:
    handle = checkpoint_output_handle()

    with (
        _flow_scope(name="demo_flow", execution_id=str(uuid4())),
        patch("kitaru.llm._llm_checkpoint_call") as mock_synthetic,
        pytest.raises(ValidationError) as prompt_exc,
    ):
        llm(cast(str, handle), model="fast", name="demo")

    assert_checkpoint_handle_error(
        prompt_exc,
        field_name="_LLMRequest.prompt",
    )
    mock_synthetic.assert_not_called()

    with (
        _flow_scope(name="demo_flow", execution_id=str(uuid4())),
        patch("kitaru.llm._llm_checkpoint_call") as mock_synthetic,
        pytest.raises(ValidationError) as system_exc,
    ):
        llm("hello", model="fast", system=cast(str, handle), name="demo")

    assert_checkpoint_handle_error(
        system_exc,
        field_name="_LLMRequest.system",
    )
    mock_synthetic.assert_not_called()


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


def test_flow_body_llm_output_handle_string_conversion_is_helpful(
    monkeypatch: pytest.MonkeyPatch,
    primed_zenml: None,
) -> None:
    """Synthetic flow-body llm handles should share checkpoint handle display."""
    register_model_alias("fast", model="openai/gpt-4o-mini")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("KITARU_LLM_MOCK_RESPONSE", "Mocked LLM response.")
    observed: dict[str, str] = {}

    @flow
    def issue_127_llm_handle_display_flow():
        handle = cast(Any, llm("hello", model="fast", name="issue_127_llm"))
        observed.update(
            {
                "stringified": str(handle),
                "repr": repr(handle),
                "loaded": handle.load(),
            }
        )

    issue_127_llm_handle_display_flow.run(cache=False)

    assert observed["loaded"] == "Mocked LLM response."
    for rendered in (observed["stringified"], observed["repr"]):
        assert "Kitaru checkpoint output handle" in rendered
        assert ".load()" in rendered
        assert "ArtifactVersionResponse" not in rendered
        assert "ZenML" not in rendered


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


def test_openai_token_limit_param_only_matches_known_o_series_prefixes() -> None:
    """OpenAI names starting with plain `o` should not all use the newer field."""
    assert _openai_token_limit_param("gpt-5-nano") == "max_completion_tokens"
    assert _openai_token_limit_param("o1-preview") == "max_completion_tokens"
    assert _openai_token_limit_param("o3-mini") == "max_completion_tokens"
    assert _openai_token_limit_param("o4-mini") == "max_completion_tokens"
    assert _openai_token_limit_param("omni-moderation-latest") == "max_tokens"
    assert _openai_token_limit_param("gpt-4o-mini") == "max_tokens"


# ---------------------------------------------------------------------------
# OpenAI call path
# ---------------------------------------------------------------------------


def test_llm_executes_openai_with_normalized_messages_and_tracking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OpenAI path: normalized prompts, artifacts, and metadata persisted."""
    extract_usage = _install_fake_genai_prices(monkeypatch, total_price=0.00123)
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    fake_result = _ProviderCallResult(
        response_text="hello world",
        usage=_LLMUsage(
            prompt_tokens=10,
            completion_tokens=20,
            total_tokens=30,
            cached_input_tokens=3,
            reasoning_tokens=4,
            raw_usage={"prompt_tokens": 10, "completion_tokens": 20},
        ),
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
    assert call_kwargs["token_limit_param"] == "max_tokens"

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
    assert logged_payload["estimated_cost_usd"] == 0.00123
    assert "cost_usd" not in logged_payload
    usage_records = mock_log.call_args.kwargs[LLM_USAGE_METADATA_KEY]
    usage_key, usage_record = next(iter(usage_records.items()))
    assert usage_key.startswith("summary_call:")
    assert usage_record["record_id"] == usage_key
    assert usage_record["call_name"] == "summary_call"
    assert usage_record["adapter"] == "kitaru.llm"
    assert usage_record["usage"]["input_tokens"] == 10
    assert usage_record["usage"]["output_tokens"] == 20
    assert usage_record["usage"]["total_tokens"] == 30
    assert usage_record["usage"]["cached_input_tokens"] == 3
    assert usage_record["usage"]["reasoning_tokens"] == 4
    assert usage_record["usage"]["raw"] == {
        "prompt_tokens": 10,
        "completion_tokens": 20,
    }
    assert usage_record["cost"]["source"] == "calculator"
    assert usage_record["cost"]["estimated_cost_usd"] == 0.00123
    assert usage_record["cost"]["actual_cost_usd"] is None
    assert usage_record["cost"]["source_label"] == "genai-prices"
    assert usage_record["cost"]["pricing_version"].startswith("genai-prices:")
    assert usage_record["warnings"] == []
    checkpoint_diff = checkpoint_diff_from_usage_records(
        original_records=[],
        replay_records=[usage_record],
    )
    assert checkpoint_diff.token_delta == {
        "prompt_tokens": 10,
        "completion_tokens": 20,
        "total_tokens": 30,
    }
    assert checkpoint_diff.cost_delta_usd == 0.00123
    extract_usage.assert_called_once_with(
        {
            "model": "gpt-4o-mini",
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 20,
                "total_tokens": 30,
                "prompt_tokens_details": {"cached_tokens": 3},
                "completion_tokens_details": {"reasoning_tokens": 4},
            },
        },
        provider_id="openai",
        api_flavor="chat",
    )


def test_llm_routes_gpt5_alias_limit_to_max_completion_tokens() -> None:
    """Resolved GPT-5-family OpenAI models should use the newer limit field."""
    fake_result = _ProviderCallResult(response_text="ok", usage=_LLMUsage())
    model_selection = ResolvedModelSelection(
        requested_model="fast",
        alias="fast",
        resolved_model="openai/gpt-5-nano",
        secret=None,
    )

    with (
        _llm_execution_scope(model_selection=model_selection),
        patch("kitaru.llm._call_openai", return_value=fake_result) as mock_call,
    ):
        output = llm("hello", model="fast", max_tokens=64, name="gpt5_call")

    assert output == "ok"
    mock_call.assert_called_once()
    call_kwargs = mock_call.call_args.kwargs
    assert call_kwargs["model"] == "gpt-5-nano"
    assert call_kwargs["max_tokens"] == 64
    assert call_kwargs["token_limit_param"] == "max_completion_tokens"


def test_llm_routes_o_series_alias_limit_to_max_completion_tokens() -> None:
    """Resolved OpenAI o-series models should use max_completion_tokens."""
    fake_result = _ProviderCallResult(response_text="ok", usage=_LLMUsage())
    model_selection = ResolvedModelSelection(
        requested_model="reasoner",
        alias="reasoner",
        resolved_model="openai/o4-mini",
        secret=None,
    )

    with (
        _llm_execution_scope(model_selection=model_selection),
        patch("kitaru.llm._call_openai", return_value=fake_result) as mock_call,
    ):
        output = llm("hello", model="reasoner", max_tokens=64, name="o_call")

    assert output == "ok"
    mock_call.assert_called_once()
    call_kwargs = mock_call.call_args.kwargs
    assert call_kwargs["model"] == "o4-mini"
    assert call_kwargs["max_tokens"] == 64
    assert call_kwargs["token_limit_param"] == "max_completion_tokens"


def test_openai_gpt5_alias_request_uses_max_completion_tokens() -> None:
    """Alias-resolved GPT-5 OpenAI requests should send the newer field only."""
    mock_client, mock_openai_cls = _mock_openai_chat_client()
    model_selection = ResolvedModelSelection(
        requested_model="fast",
        alias="fast",
        resolved_model="openai/gpt-5-nano",
        secret=None,
    )

    with (
        _llm_execution_scope(model_selection=model_selection),
        patch.dict("sys.modules", {"openai": MagicMock(OpenAI=mock_openai_cls)}),
    ):
        output = llm("hello", model="fast", max_tokens=77, name="gpt5_tokens")

    assert output == "ok"
    request_kwargs = mock_client.chat.completions.create.call_args.kwargs
    assert request_kwargs["max_completion_tokens"] == 77
    assert "max_tokens" not in request_kwargs


def test_openai_default_o_series_request_uses_max_completion_tokens() -> None:
    """Default-model resolution should also affect the OpenAI request field."""
    mock_client, mock_openai_cls = _mock_openai_chat_client()
    model_selection = ResolvedModelSelection(
        requested_model="reasoner",
        alias="reasoner",
        resolved_model="openai/o3-mini",
        secret=None,
    )

    with (
        _llm_execution_scope(model_selection=model_selection),
        patch.dict("sys.modules", {"openai": MagicMock(OpenAI=mock_openai_cls)}),
    ):
        output = llm("hello", max_tokens=33, name="default_o_tokens")

    assert output == "ok"
    request_kwargs = mock_client.chat.completions.create.call_args.kwargs
    assert request_kwargs["max_completion_tokens"] == 33
    assert "max_tokens" not in request_kwargs


def test_llm_preserves_zero_openai_usage_tokens() -> None:
    """Direct OpenAI usage logging should keep provider-reported zero tokens."""
    fake_result = _ProviderCallResult(
        response_text="empty usage",
        usage=_LLMUsage(prompt_tokens=0, completion_tokens=0, total_tokens=0),
    )

    with (
        _llm_execution_scope(model_selection=_simple_selection("openai/gpt-4o-mini")),
        patch("kitaru.llm._call_openai", return_value=fake_result),
        patch("kitaru.llm.save"),
        patch("kitaru.llm.log") as mock_log,
    ):
        output = llm("Say nothing", name="zero_call")

    assert output == "empty usage"
    logged_payload = mock_log.call_args.kwargs["llm_calls"]["zero_call"]
    assert logged_payload["tokens_input"] == 0
    assert logged_payload["tokens_output"] == 0
    assert logged_payload["total_tokens"] == 0
    assert logged_payload["estimated_cost_usd"] == 0.0
    usage_records = mock_log.call_args.kwargs[LLM_USAGE_METADATA_KEY]
    usage_key, usage_record = next(iter(usage_records.items()))
    assert usage_key.startswith("zero_call:")
    assert usage_record["record_id"] == usage_key
    assert usage_record["call_name"] == "zero_call"
    assert usage_record["usage"]["input_tokens"] == 0
    assert usage_record["usage"]["output_tokens"] == 0
    assert usage_record["usage"]["total_tokens"] == 0
    assert usage_record["cost"]["source"] == "calculator"
    assert usage_record["cost"]["estimated_cost_usd"] == 0.0
    assert usage_record["cost"]["source_label"] == "genai-prices"
    assert usage_record["cost"]["pricing_version"].startswith("genai-prices:")
    assert usage_record["warnings"] == []


def test_openai_usage_extraction_omits_raw_usage_when_serialization_fails() -> None:
    """Provider success should survive unusual SDK usage objects."""

    class BadUsage:
        prompt_tokens = 3
        completion_tokens = 4
        total_tokens = 7

        def model_dump(self, *, mode: str) -> dict[str, Any]:
            raise TypeError(f"cannot dump as {mode}")

        @property
        def prompt_tokens_details(self) -> Any:
            raise RuntimeError("details unavailable")

        @property
        def completion_tokens_details(self) -> Any:
            raise RuntimeError("details unavailable")

    usage = _extract_usage_openai(SimpleNamespace(usage=BadUsage()))

    assert usage.prompt_tokens == 3
    assert usage.completion_tokens == 4
    assert usage.total_tokens == 7
    assert usage.cached_input_tokens is None
    assert usage.reasoning_tokens is None
    assert usage.raw_usage == {
        "prompt_tokens": 3,
        "completion_tokens": 4,
        "total_tokens": 7,
    }


def test_repeated_named_llm_calls_get_distinct_usage_record_ids() -> None:
    """Repeated display names should not overwrite canonical usage records."""
    first_result = _ProviderCallResult(
        response_text="first",
        usage=_LLMUsage(prompt_tokens=1, completion_tokens=2, total_tokens=3),
    )
    second_result = _ProviderCallResult(
        response_text="second",
        usage=_LLMUsage(prompt_tokens=4, completion_tokens=5, total_tokens=9),
    )

    with (
        _llm_execution_scope(
            model_selection=_simple_selection("openai/gpt-4o-mini")
        ) as (
            _mock_save,
            mock_log,
        ),
        patch("kitaru.llm._call_openai", side_effect=[first_result, second_result]),
    ):
        assert llm("Summarize this", name="summarize") == "first"
        assert llm("Summarize that", name="summarize") == "second"

    first_metadata = mock_log.call_args_list[0].kwargs[LLM_USAGE_METADATA_KEY]
    second_metadata = mock_log.call_args_list[1].kwargs[LLM_USAGE_METADATA_KEY]
    first_key, first_record = next(iter(first_metadata.items()))
    second_key, second_record = next(iter(second_metadata.items()))

    assert first_key != second_key
    assert first_key.startswith("summarize:")
    assert second_key.startswith("summarize:")
    assert first_record["record_id"] == first_key
    assert second_record["record_id"] == second_key
    assert first_record["call_name"] == "summarize"
    assert second_record["call_name"] == "summarize"
    assert first_record["usage"]["total_tokens"] == 3
    assert second_record["usage"]["total_tokens"] == 9


# ---------------------------------------------------------------------------
# Provider dispatch and parsing
# ---------------------------------------------------------------------------


def test_llm_executes_anthropic_with_system_separation_and_tracking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Anthropic path: system separated, usage mapped, artifacts persisted."""
    extract_usage = _install_fake_genai_prices(monkeypatch, total_price=0.00456)
    execution_id, checkpoint_id = _flow_checkpoint_scope()
    fake_result = _ProviderCallResult(
        response_text="bonjour",
        usage=_LLMUsage(
            prompt_tokens=5,
            completion_tokens=15,
            total_tokens=20,
            cache_creation_input_tokens=2,
            cache_read_input_tokens=3,
            raw_usage={"input_tokens": 5, "output_tokens": 15},
        ),
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
    assert logged_payload["estimated_cost_usd"] == 0.00456
    assert "cost_usd" not in logged_payload
    usage_records = mock_log.call_args.kwargs[LLM_USAGE_METADATA_KEY]
    usage_key, usage_record = next(iter(usage_records.items()))
    assert usage_key.startswith("translate_call:")
    assert usage_record["record_id"] == usage_key
    assert usage_record["call_name"] == "translate_call"
    assert usage_record["provider"] == "anthropic"
    assert usage_record["usage"]["input_tokens"] == 5
    assert usage_record["usage"]["output_tokens"] == 15
    assert usage_record["usage"]["raw"] == {"input_tokens": 5, "output_tokens": 15}
    assert usage_record["cost"]["source"] == "calculator"
    assert usage_record["cost"]["estimated_cost_usd"] == 0.00456
    assert usage_record["cost"]["actual_cost_usd"] is None
    assert usage_record["cost"]["source_label"] == "genai-prices"
    assert usage_record["cost"]["pricing_version"].startswith("genai-prices:")
    assert usage_record["warnings"] == []
    extract_usage.assert_called_once_with(
        {
            "model": "claude-sonnet-4-20250514",
            "usage": {
                "input_tokens": 5,
                "output_tokens": 15,
                "cache_creation_input_tokens": 2,
                "cache_read_input_tokens": 3,
            },
        },
        provider_id="anthropic",
    )


# ---------------------------------------------------------------------------
# Direct estimated-cost policy and failure behavior
# ---------------------------------------------------------------------------


def test_direct_openai_cost_uses_installed_genai_prices() -> None:
    """Direct OpenAI cost estimates should work with the real installed library."""
    warnings: list[str] = []

    estimate = _estimate_direct_llm_cost(
        resolved_model="openai/gpt-4o-mini",
        usage=_LLMUsage(prompt_tokens=1000, completion_tokens=500, total_tokens=1500),
        warnings=warnings,
    )

    assert estimate.estimated_cost_usd is not None
    assert estimate.estimated_cost_usd > 0
    assert estimate.cost_source_label == "genai-prices"
    assert estimate.pricing_version is not None
    assert estimate.pricing_version.startswith("genai-prices:")
    assert warnings == []


@pytest.mark.parametrize(
    ("resolved_model", "usage"),
    [
        (
            "openai/gpt-4o-mini",
            _LLMUsage(prompt_tokens=0, completion_tokens=0, total_tokens=0),
        ),
        (
            "anthropic/claude-3-5-haiku-latest",
            _LLMUsage(prompt_tokens=1000, completion_tokens=500, total_tokens=1500),
        ),
    ],
)
def test_direct_cost_uses_installed_genai_prices_for_zero_and_anthropic_usage(
    resolved_model: str,
    usage: _LLMUsage,
) -> None:
    """The real pricing helper should handle zero-token and Anthropic usage."""
    warnings: list[str] = []

    estimate = _estimate_direct_llm_cost(
        resolved_model=resolved_model,
        usage=usage,
        warnings=warnings,
    )

    assert estimate.estimated_cost_usd is not None
    assert estimate.estimated_cost_usd >= 0
    assert estimate.cost_source_label == "genai-prices"
    assert estimate.pricing_version is not None
    assert estimate.pricing_version.startswith("genai-prices:")
    assert warnings == []
    if usage.total_tokens == 0:
        assert estimate.estimated_cost_usd == 0.0


def test_llm_estimated_cost_policy_defaults_to_auto() -> None:
    assert resolve_llm_estimated_cost_policy() == "auto"


def test_llm_estimated_cost_policy_honors_env_opt_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KITARU_LLM_ESTIMATED_COSTS", "off")

    assert resolve_llm_estimated_cost_policy() == "off"


def test_llm_estimated_cost_runtime_setting_beats_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KITARU_LLM_ESTIMATED_COSTS", "off")
    configure(llm_estimated_costs="auto")

    assert resolve_llm_estimated_cost_policy() == "auto"


def test_llm_estimated_cost_invalid_env_is_non_fatal_after_provider_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KITARU_LLM_ESTIMATED_COSTS", "banana")
    extract_usage = _install_fake_genai_prices(monkeypatch, total_price=0.01)
    fake_result = _ProviderCallResult(
        response_text="ok",
        usage=_LLMUsage(prompt_tokens=10, completion_tokens=20, total_tokens=30),
    )

    with (
        _llm_execution_scope(
            model_selection=_simple_selection("openai/gpt-4o-mini")
        ) as (_mock_save, mock_log),
        patch("kitaru.llm._call_openai", return_value=fake_result),
    ):
        assert llm("hello", name="invalid_cost_policy") == "ok"

    extract_usage.assert_not_called()
    usage_record = _single_usage_record(mock_log)
    assert usage_record["cost"]["source"] == "none"
    assert usage_record["cost"]["estimated_cost_usd"] is None
    assert len(usage_record["warnings"]) == 1
    warning = usage_record["warnings"][0]
    assert "could not resolve the direct LLM estimated-cost policy" in warning
    assert "llm_estimated_costs must be 'auto' or 'off'" in warning


def test_llm_estimated_cost_env_off_skips_genai_prices(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KITARU_LLM_ESTIMATED_COSTS", "off")
    extract_usage = _install_fake_genai_prices(monkeypatch, total_price=0.01)
    fake_result = _ProviderCallResult(
        response_text="ok",
        usage=_LLMUsage(prompt_tokens=10, completion_tokens=20, total_tokens=30),
    )

    with (
        _llm_execution_scope(
            model_selection=_simple_selection("openai/gpt-4o-mini")
        ) as (_mock_save, mock_log),
        patch("kitaru.llm._call_openai", return_value=fake_result),
    ):
        assert llm("hello", name="env_off") == "ok"

    extract_usage.assert_not_called()
    usage_record = _single_usage_record(mock_log)
    assert usage_record["cost"]["source"] == "none"
    assert usage_record["cost"]["estimated_cost_usd"] is None
    assert usage_record["warnings"] == []


def test_llm_estimated_cost_missing_genai_prices_is_non_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "genai_prices", None)
    fake_result = _ProviderCallResult(
        response_text="ok",
        usage=_LLMUsage(prompt_tokens=10, completion_tokens=20, total_tokens=30),
    )

    with (
        _llm_execution_scope(
            model_selection=_simple_selection("openai/gpt-4o-mini")
        ) as (_mock_save, mock_log),
        patch("kitaru.llm._call_openai", return_value=fake_result),
    ):
        assert llm("hello", name="missing_prices") == "ok"

    usage_record = _single_usage_record(mock_log)
    assert usage_record["cost"]["source"] == "none"
    assert usage_record["cost"]["estimated_cost_usd"] is None
    assert usage_record["warnings"] == [
        "genai-prices is not installed; direct LLM tokens were recorded "
        "without an estimated cost."
    ]


def test_llm_estimated_cost_extract_failure_is_non_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_genai_prices(
        monkeypatch,
        extract_error=LookupError("no price for model"),
    )
    fake_result = _ProviderCallResult(
        response_text="ok",
        usage=_LLMUsage(prompt_tokens=10, completion_tokens=20, total_tokens=30),
    )

    with (
        _llm_execution_scope(
            model_selection=_simple_selection("openai/not-a-real-priced-model")
        ) as (_mock_save, mock_log),
        patch("kitaru.llm._call_openai", return_value=fake_result),
    ):
        assert llm("hello", name="unknown_model") == "ok"

    usage_record = _single_usage_record(mock_log)
    assert usage_record["cost"]["source"] == "none"
    assert usage_record["cost"]["estimated_cost_usd"] is None
    assert len(usage_record["warnings"]) == 1
    warning = usage_record["warnings"][0]
    assert "genai-prices could not estimate direct LLM cost" in warning
    assert "openai/not-a-real-priced-model" in warning
    assert "LookupError: no price for model" in warning


def test_llm_estimated_cost_calc_price_failure_is_non_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_genai_prices(
        monkeypatch,
        calc_error=RuntimeError("GitHub price data unavailable"),
    )
    fake_result = _ProviderCallResult(
        response_text="ok",
        usage=_LLMUsage(prompt_tokens=5, completion_tokens=7, total_tokens=12),
    )

    with (
        _llm_execution_scope(
            model_selection=_simple_selection("anthropic/claude-sonnet-4-20250514")
        ) as (_mock_save, mock_log),
        patch("kitaru.llm._call_anthropic", return_value=fake_result),
    ):
        assert llm("hello", name="calc_failure") == "ok"

    usage_record = _single_usage_record(mock_log)
    assert usage_record["cost"]["source"] == "none"
    assert usage_record["cost"]["estimated_cost_usd"] is None
    assert len(usage_record["warnings"]) == 1
    assert "RuntimeError: GitHub price data unavailable" in usage_record["warnings"][0]


def test_llm_estimated_cost_invalid_price_is_non_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_genai_prices(monkeypatch, total_price=-1.0)
    fake_result = _ProviderCallResult(
        response_text="ok",
        usage=_LLMUsage(prompt_tokens=5, completion_tokens=7, total_tokens=12),
    )

    with (
        _llm_execution_scope(
            model_selection=_simple_selection("openai/gpt-4o-mini")
        ) as (_mock_save, mock_log),
        patch("kitaru.llm._call_openai", return_value=fake_result),
    ):
        assert llm("hello", name="invalid_price") == "ok"

    usage_record = _single_usage_record(mock_log)
    assert usage_record["cost"]["source"] == "none"
    assert usage_record["cost"]["estimated_cost_usd"] is None
    assert usage_record["warnings"] == [
        "genai-prices returned an invalid direct LLM estimated cost for "
        "'openai/gpt-4o-mini'; tokens were recorded without an estimated cost."
    ]


def test_extract_usage_openai_preserves_cached_and_reasoning_tokens() -> None:
    raw_response = {
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 20,
            "total_tokens": 30,
            "prompt_tokens_details": {"cached_tokens": 4},
            "completion_tokens_details": {"reasoning_tokens": 6},
        }
    }

    usage = _extract_usage_openai(raw_response)

    assert usage.prompt_tokens == 10
    assert usage.completion_tokens == 20
    assert usage.total_tokens == 30
    assert usage.cached_input_tokens == 4
    assert usage.reasoning_tokens == 6
    assert usage.raw_usage == raw_response["usage"]


def test_extract_usage_anthropic_preserves_cache_token_fields() -> None:
    raw_response = SimpleNamespace(
        usage=SimpleNamespace(
            input_tokens=5,
            output_tokens=7,
            cache_creation_input_tokens=2,
            cache_read_input_tokens=3,
        )
    )

    usage = _extract_usage_anthropic(raw_response)

    assert usage.prompt_tokens == 5
    assert usage.completion_tokens == 7
    assert usage.total_tokens == 12
    assert usage.cache_creation_input_tokens == 2
    assert usage.cache_read_input_tokens == 3
    assert usage.raw_usage == {
        "input_tokens": 5,
        "output_tokens": 7,
        "cache_creation_input_tokens": 2,
        "cache_read_input_tokens": 3,
    }


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
    assert call_kwargs["token_limit_param"] == "max_tokens"


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


def test_ollama_openai_compatible_request_uses_max_tokens() -> None:
    """Ollama's OpenAI-compatible endpoint should keep max_tokens."""
    mock_client, mock_openai_cls = _mock_openai_chat_client()

    with (
        _llm_execution_scope(model_selection=_simple_selection("ollama/qwen3.5")),
        patch.dict("sys.modules", {"openai": MagicMock(OpenAI=mock_openai_cls)}),
    ):
        output = llm(
            "hello",
            model="ollama/qwen3.5",
            max_tokens=77,
            name="ollama_tokens",
        )

    assert output == "ok"
    request_kwargs = mock_client.chat.completions.create.call_args.kwargs
    assert request_kwargs["max_tokens"] == 77
    assert "max_completion_tokens" not in request_kwargs


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
    assert call_kwargs["token_limit_param"] == "max_tokens"


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


def test_openrouter_openai_compatible_request_uses_max_tokens() -> None:
    """OpenRouter should keep max_tokens even for a nested GPT-5 model string."""
    mock_client, mock_openai_cls = _mock_openai_chat_client()
    overlay = {"OPENROUTER_API_KEY": "secret-or-key"}

    with (
        _llm_execution_scope(
            model_selection=_simple_selection("openrouter/openai/gpt-5-nano"),
            credential_overlay=(overlay, "secret"),
        ),
        patch.dict("sys.modules", {"openai": MagicMock(OpenAI=mock_openai_cls)}),
    ):
        output = llm(
            "hello",
            model="openrouter/openai/gpt-5-nano",
            max_tokens=77,
            name="openrouter_tokens",
        )

    assert output == "ok"
    request_kwargs = mock_client.chat.completions.create.call_args.kwargs
    assert request_kwargs["max_tokens"] == 77
    assert "max_completion_tokens" not in request_kwargs


def test_llm_redacts_openrouter_key_through_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OpenRouter failures should redact keys after provider dispatch."""
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    secret = "or-test-dispatch-secret"
    provider_exc = RuntimeError(f"OpenRouter rejected API key {secret}")
    mock_client = MagicMock()
    mock_client.chat.completions.create.side_effect = provider_exc
    mock_openai_cls = MagicMock(return_value=mock_client)

    with (
        _llm_execution_scope(
            model_selection=_simple_selection("openrouter/openai/gpt-4o"),
            credential_overlay=({"OPENROUTER_API_KEY": secret}, "secret"),
        ),
        patch.dict("sys.modules", {"openai": MagicMock(OpenAI=mock_openai_cls)}),
        pytest.raises(KitaruBackendError) as raised,
    ):
        llm("hello", model="openrouter/openai/gpt-4o", name="test")

    message = str(raised.value)
    assert "openrouter" in message
    assert "[redacted]" in message
    assert secret not in message
    assert raised.value.__cause__ is provider_exc


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
    # Artifacts and metadata should still be persisted.
    mock_save.assert_called()
    mock_log.assert_called_once()
    usage_record = _single_usage_record(mock_log)
    assert usage_record["billing_effect"] == "unknown"
    summary, has_unpriced_incurred_record = (
        aggregate_usage_records_with_cost_completeness([usage_record])
    )
    assert summary["incurred_usage_record_count"] == 1
    assert summary["incurred_total_tokens"] == 0
    assert has_unpriced_incurred_record is True


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
    _assert_metadata_excludes(
        metadata,
        keys=("prompt", "system", "call_name", "name"),
        values=("private prompt text", "private system text", "private_call_name"),
    )


def test_llm_failed_analytics_is_not_duplicated_in_flow_body_path() -> None:
    """Flow-body failures should not emit a second outer LLM_FAILED event."""
    llm_module = importlib.import_module("kitaru.llm")

    model_selection = ResolvedModelSelection(
        requested_model="fast",
        alias="fast",
        resolved_model="openai/gpt-4o-mini",
        secret=None,
    )
    credential_error = KitaruRuntimeError(
        "No provider credentials found for private-secret-name and sk-private-value"
    )

    def _run_synthetic_checkpoint(request: _LLMRequest, *, id: str) -> str:
        assert id == "private_call_name"
        return llm_module._execute_llm_call(request)

    with (
        _flow_scope(name="demo_flow", execution_id=str(uuid4())),
        patch("kitaru.llm.resolve_model_selection", return_value=model_selection),
        patch("kitaru.llm._resolve_credential_overlay", side_effect=credential_error),
        patch("kitaru.llm._llm_checkpoint_call", side_effect=_run_synthetic_checkpoint),
        patch("kitaru.analytics.track", return_value=True) as track_mock,
        pytest.raises(KitaruRuntimeError, match="No provider credentials"),
    ):
        llm("private prompt text", model="fast", name="private_call_name")

    metadata = _tracked_llm_failure_metadata(track_mock, inside_checkpoint=False)
    assert metadata["error_type"] == "KitaruRuntimeError"
    assert metadata["resolved_model"] == "openai/gpt-4o-mini"
    _assert_metadata_excludes(
        metadata,
        values=("private-secret-name", "sk-private-value"),
    )


def test_llm_failed_analytics_for_missing_credentials_is_privacy_safe() -> None:
    """Credential failures should emit coarse failure metadata only."""
    model_selection = ResolvedModelSelection(
        requested_model="fast",
        alias="fast",
        resolved_model="openai/gpt-4o-mini",
        secret="private-secret-name",
    )
    credential_error = KitaruRuntimeError(
        "No provider credentials found for private-secret-name and sk-private-value"
    )

    with (
        _llm_execution_scope(model_selection=model_selection),
        patch("kitaru.llm._resolve_credential_overlay", side_effect=credential_error),
        patch("kitaru.analytics.track", return_value=True) as track_mock,
        pytest.raises(KitaruRuntimeError, match="No provider credentials"),
    ):
        llm(
            "private prompt text",
            model="fast",
            system="private system text",
            name="private_call_name",
        )

    metadata = _tracked_llm_failure_metadata(track_mock)
    assert metadata["llm_path"] == "direct_llm"
    assert metadata["error_type"] == "KitaruRuntimeError"
    assert metadata["resolved_model"] == "openai/gpt-4o-mini"
    assert metadata["model"] == "openai/gpt-4o-mini"
    _assert_metadata_excludes(
        metadata,
        keys=(
            "credential_source",
            "prompt",
            "system",
            "call_name",
            "name",
            "requested_model",
            "alias",
        ),
        values=(
            "private prompt text",
            "private system text",
            "private_call_name",
            "fast",
            "private-secret-name",
            "sk-private-value",
        ),
    )


def test_llm_failed_analytics_for_unsupported_provider_is_privacy_safe() -> None:
    """Unsupported providers should emit one coarse failure event."""
    model_selection = ResolvedModelSelection(
        requested_model="fast",
        alias="fast",
        resolved_model="gemini/gemini-2.0-flash",
        secret=None,
    )

    with (
        _llm_execution_scope(model_selection=model_selection),
        patch("kitaru.analytics.track", return_value=True) as track_mock,
        pytest.raises(KitaruUsageError, match="not supported"),
    ):
        llm("private prompt text", model="fast", name="private_call_name")

    metadata = _tracked_llm_failure_metadata(track_mock)
    assert metadata["error_type"] == "KitaruUsageError"
    assert metadata["resolved_model"] == "gemini/gemini-2.0-flash"
    assert metadata["model"] == "gemini/gemini-2.0-flash"
    assert metadata["credential_source"] == "environment"
    assert metadata["mocked"] is False
    _assert_metadata_excludes(
        metadata,
        values=("fast", "private prompt text", "private_call_name"),
    )


def test_llm_failed_analytics_for_missing_sdk_is_privacy_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing provider SDK failures should not put install text in analytics."""
    monkeypatch.setitem(sys.modules, "openai", None)

    with (
        _llm_execution_scope(model_selection=_simple_selection("openai/gpt-4o-mini")),
        patch("kitaru.analytics.track", return_value=True) as track_mock,
        pytest.raises(KitaruUsageError, match=r"kitaru\[openai\]"),
    ):
        llm("private prompt text", model="openai/gpt-4o-mini", name="private_call_name")

    metadata = _tracked_llm_failure_metadata(track_mock)
    assert metadata["error_type"] == "KitaruUsageError"
    assert metadata["resolved_model"] == "openai/gpt-4o-mini"
    assert metadata["credential_source"] == "environment"
    assert metadata["mocked"] is False
    _assert_metadata_excludes(
        metadata,
        values=("kitaru[openai]", "private prompt text", "private_call_name"),
    )


def test_llm_failed_analytics_for_provider_error_is_privacy_safe() -> None:
    """Provider errors should not put provider text or usage records in analytics."""
    provider_error = KitaruBackendError(
        "provider said authentication failed for sk-private-provider-value"
    )
    with (
        _llm_execution_scope(
            model_selection=_simple_selection("openai/gpt-4o-mini")
        ) as (_mock_save, mock_log),
        patch("kitaru.llm._call_openai", side_effect=provider_error),
        patch("kitaru.analytics.track", return_value=True) as track_mock,
        pytest.raises(KitaruBackendError, match="authentication failed"),
    ):
        llm(
            "private prompt text",
            model="openai/gpt-4o-mini",
            system="private system text",
            name="private_call_name",
        )

    mock_log.assert_not_called()
    metadata = _tracked_llm_failure_metadata(track_mock)
    assert metadata["error_type"] == "KitaruBackendError"
    assert metadata["resolved_model"] == "openai/gpt-4o-mini"
    assert metadata["credential_source"] == "environment"
    assert metadata["mocked"] is False
    _assert_metadata_excludes(
        metadata,
        values=(
            "sk-private-provider-value",
            "private prompt text",
            "private system text",
            "private_call_name",
        ),
    )


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
    _assert_metadata_excludes(
        metadata,
        keys=("prompt", "system", "call_name", "name"),
        values=("private prompt text", "private_call_name"),
    )


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


def test_call_openai_redacts_overlay_secrets_from_provider_errors() -> None:
    """OpenAI provider errors should not leak env-overlay credentials."""
    from kitaru.llm import _call_openai

    secret = "sk-test-overlay-value"
    mock_client = MagicMock()
    mock_client.chat.completions.create.side_effect = RuntimeError(
        f"Incorrect API key provided: {secret}"
    )
    mock_openai_cls = MagicMock(return_value=mock_client)

    with (
        patch.dict("sys.modules", {"openai": MagicMock(OpenAI=mock_openai_cls)}),
        pytest.raises(KitaruBackendError) as raised,
    ):
        _call_openai(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "hi"}],
            temperature=None,
            max_tokens=None,
            env_overlay={"OPENAI_API_KEY": secret},
        )

    message = str(raised.value)
    assert "[redacted]" in message
    assert secret not in message


def test_call_openai_redacts_client_construction_errors() -> None:
    """OpenAI client construction errors should use the same redaction path."""
    from kitaru.llm import _call_openai

    secret = "sk-test-client-construction"
    provider_exc = RuntimeError(f"Client rejected API key {secret}")
    mock_openai_cls = MagicMock(side_effect=provider_exc)

    with (
        patch.dict("sys.modules", {"openai": MagicMock(OpenAI=mock_openai_cls)}),
        pytest.raises(KitaruBackendError) as raised,
    ):
        _call_openai(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "hi"}],
            temperature=None,
            max_tokens=None,
            env_overlay={"OPENAI_API_KEY": secret},
        )

    message = str(raised.value)
    assert "[redacted]" in message
    assert secret not in message
    assert raised.value.__cause__ is provider_exc


def test_call_openai_redacts_process_env_secret_from_provider_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Provider errors should redact credentials read from os.environ."""
    from kitaru.llm import _call_openai

    secret = "sk-test-process-env-value"
    monkeypatch.setenv("OPENAI_API_KEY", secret)
    mock_client = MagicMock()
    mock_client.chat.completions.create.side_effect = RuntimeError(
        f"Incorrect API key provided: {secret}"
    )
    mock_openai_cls = MagicMock(return_value=mock_client)

    with (
        patch.dict("sys.modules", {"openai": MagicMock(OpenAI=mock_openai_cls)}),
        pytest.raises(KitaruBackendError) as raised,
    ):
        _call_openai(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "hi"}],
            temperature=None,
            max_tokens=None,
            env_overlay={},
        )

    message = str(raised.value)
    assert "[redacted]" in message
    assert secret not in message


def test_call_openai_redacts_explicit_api_key_from_provider_errors() -> None:
    """OpenAI-compatible provider errors should not leak explicit API keys."""
    from kitaru.llm import _call_openai

    secret = "sk-test-explicit-value"
    mock_client = MagicMock()
    mock_client.chat.completions.create.side_effect = RuntimeError(
        f"Incorrect API key provided: {secret}"
    )
    mock_openai_cls = MagicMock(return_value=mock_client)

    with (
        patch.dict("sys.modules", {"openai": MagicMock(OpenAI=mock_openai_cls)}),
        pytest.raises(KitaruBackendError) as raised,
    ):
        _call_openai(
            model="openai/gpt-4o",
            messages=[{"role": "user", "content": "hi"}],
            temperature=None,
            max_tokens=None,
            env_overlay={},
            base_url="https://openrouter.ai/api/v1",
            api_key=secret,
            provider_label="openrouter",
        )

    message = str(raised.value)
    assert "[redacted]" in message
    assert secret not in message


def test_call_anthropic_redacts_overlay_secrets_from_provider_errors() -> None:
    """Anthropic provider errors should not leak env-overlay credentials."""
    from kitaru.llm import _call_anthropic

    secret = "sk-test-anthropic-overlay"
    mock_client = MagicMock()
    mock_client.messages.create.side_effect = RuntimeError(
        f"Authentication failed for key {secret}"
    )
    mock_anthropic_cls = MagicMock(return_value=mock_client)

    with (
        patch.dict(
            "sys.modules", {"anthropic": MagicMock(Anthropic=mock_anthropic_cls)}
        ),
        pytest.raises(KitaruBackendError) as raised,
    ):
        _call_anthropic(
            model="claude-sonnet-4-20250514",
            messages=[{"role": "user", "content": "hello"}],
            temperature=None,
            max_tokens=None,
            env_overlay={"ANTHROPIC_API_KEY": secret},
        )

    message = str(raised.value)
    assert "[redacted]" in message
    assert secret not in message


def test_call_anthropic_redacts_client_construction_errors() -> None:
    """Anthropic client construction errors should use the same redaction path."""
    from kitaru.llm import _call_anthropic

    secret = "sk-test-anthropic-client"
    provider_exc = RuntimeError(f"Client rejected API key {secret}")
    mock_anthropic_cls = MagicMock(side_effect=provider_exc)

    with (
        patch.dict(
            "sys.modules", {"anthropic": MagicMock(Anthropic=mock_anthropic_cls)}
        ),
        pytest.raises(KitaruBackendError) as raised,
    ):
        _call_anthropic(
            model="claude-sonnet-4-20250514",
            messages=[{"role": "user", "content": "hello"}],
            temperature=None,
            max_tokens=None,
            env_overlay={"ANTHROPIC_API_KEY": secret},
        )

    message = str(raised.value)
    assert "[redacted]" in message
    assert secret not in message
    assert raised.value.__cause__ is provider_exc


def test_call_openai_leaves_non_secret_provider_error_text_unchanged() -> None:
    """Provider error text without known credentials should pass through."""
    from kitaru.llm import _call_openai

    provider_error = "provider timeout without credential details"
    mock_client = MagicMock()
    mock_client.chat.completions.create.side_effect = RuntimeError(provider_error)
    mock_openai_cls = MagicMock(return_value=mock_client)

    with (
        patch.dict("sys.modules", {"openai": MagicMock(OpenAI=mock_openai_cls)}),
        pytest.raises(KitaruBackendError) as raised,
    ):
        _call_openai(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "hi"}],
            temperature=None,
            max_tokens=None,
            env_overlay={},
        )

    message = str(raised.value)
    assert provider_error in message
    assert "[redacted]" not in message


def test_redact_provider_error_text_handles_empty_and_ambient_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The helper should be a no-op unless Kitaru knows the credential value."""
    from kitaru.llm import _redact_provider_error_text

    assert (
        _redact_provider_error_text(
            "provider timeout", env_overlay={}, extra_secrets=()
        )
        == "provider timeout"
    )

    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-ambient-value")

    assert (
        _redact_provider_error_text(
            "Incorrect API key provided: sk-test-ambient-value",
            env_overlay={"OPENAI_API_KEY": ""},
        )
        == "Incorrect API key provided: [redacted]"
    )
    assert (
        _redact_provider_error_text(
            "ollama server unavailable", env_overlay={}, extra_secrets=("ollama",)
        )
        == "ollama server unavailable"
    )


def test_redact_provider_error_text_skips_unrelated_and_short_values() -> None:
    """Benign overlay values should not erase useful provider diagnostics."""
    from kitaru.llm import _redact_provider_error_text

    assert (
        _redact_provider_error_text(
            "provider project alpha failed with key sk",
            env_overlay={"PROJECT_NAME": "alpha", "OPENAI_API_KEY": "sk"},
        )
        == "provider project alpha failed with key sk"
    )


def test_redact_provider_error_text_handles_overlapping_secrets() -> None:
    """Longer credential values should be redacted before shorter prefixes."""
    from kitaru.llm import _redact_provider_error_text

    message = "primary sk-test12345 fallback sk-test1"

    assert (
        _redact_provider_error_text(
            message,
            env_overlay={
                "OPENAI_API_KEY": "sk-test12345",
                "BACKUP_API_KEY": "sk-test1",
            },
        )
        == "primary [redacted] fallback [redacted]"
    )


def test_call_openai_redacts_message_but_preserves_original_cause() -> None:
    """Kitaru messages are redacted while exception chaining stays intact."""
    from kitaru.llm import _call_openai

    secret = "sk-test-cause-value"
    provider_exc = RuntimeError(f"Incorrect API key provided: {secret}")
    mock_client = MagicMock()
    mock_client.chat.completions.create.side_effect = provider_exc
    mock_openai_cls = MagicMock(return_value=mock_client)

    with (
        patch.dict("sys.modules", {"openai": MagicMock(OpenAI=mock_openai_cls)}),
        pytest.raises(KitaruBackendError) as raised,
    ):
        _call_openai(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "hi"}],
            temperature=None,
            max_tokens=None,
            env_overlay={"OPENAI_API_KEY": secret},
        )

    assert secret not in str(raised.value)
    assert "[redacted]" in str(raised.value)
    assert raised.value.__cause__ is provider_exc
    # Kitaru redacts its own message, but preserves the provider exception for
    # debug tracebacks; the original cause may still contain raw provider text.
    assert secret in str(raised.value.__cause__)


def test_call_openai_passes_correct_parameters() -> None:
    """_call_openai should use max_tokens by default."""
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


def test_call_openai_can_send_max_completion_tokens() -> None:
    """_call_openai should send only max_completion_tokens when requested."""
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
            model="gpt-5-nano",
            messages=[{"role": "user", "content": "hi"}],
            temperature=0.5,
            max_tokens=100,
            env_overlay={},
            token_limit_param="max_completion_tokens",
        )

    assert result.response_text == "hi"
    request_kwargs = mock_client.chat.completions.create.call_args.kwargs
    assert request_kwargs["max_completion_tokens"] == 100
    assert "max_tokens" not in request_kwargs


def test_call_openai_omits_token_limit_when_unset() -> None:
    """_call_openai should not send either limit field when max_tokens is None."""
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
            model="gpt-5-nano",
            messages=[{"role": "user", "content": "hi"}],
            temperature=0.5,
            max_tokens=None,
            env_overlay={},
            token_limit_param="max_completion_tokens",
        )

    assert result.response_text == "hi"
    request_kwargs = mock_client.chat.completions.create.call_args.kwargs
    assert "max_completion_tokens" not in request_kwargs
    assert "max_tokens" not in request_kwargs


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
