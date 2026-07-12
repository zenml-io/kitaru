"""Tests for canonical LLM usage metadata helpers."""

import math
from types import SimpleNamespace
from typing import Any, cast

import pytest

from kitaru._client._models import (
    CheckpointAttempt,
    CheckpointCall,
    Execution,
    ExecutionStatus,
)
from kitaru._llm_usage import (
    LLM_FLAT_ACTUAL_COST_USD_KEY,
    LLM_FLAT_DISPLAY_COST_USD_KEY,
    LLM_FLAT_ESTIMATED_COST_USD_KEY,
    LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY,
    LLM_FLAT_INCURRED_USAGE_RECORD_COUNT_KEY,
    LLM_FLAT_REUSED_TOTAL_TOKENS_KEY,
    LLM_FLAT_REUSED_USAGE_RECORD_COUNT_KEY,
    LLM_USAGE_METADATA_KEY,
    LLM_USAGE_SUMMARY_METADATA_KEY,
    _normalize_provider_id,
    _provider_model_ref,
    _usage_has_genai_pricing_tokens,
    add_optional_token_count,
    aggregate_usage_records,
    build_usage_record,
    calculated_or_genai_cost_metadata,
    estimate_genai_prices_cost,
    execution_metadata_from_records,
    flat_usage_metadata_from_records,
    log_usage_record_best_effort,
    metadata_matches_flat_usage_metadata,
    parse_usage_summary,
    usage_records_from_metadata,
    usage_reuse_classification,
)
from tests._genai_prices_helpers import install_fake_genai_calc_price


def test_usage_reuse_classification_marks_non_reused_records_executed() -> None:
    reused, cache_status = usage_reuse_classification(checkpoint_status="completed")

    assert reused is False
    assert cache_status == "executed"


def test_genai_prices_helper_estimates_gemini_with_normalized_usage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_fake_genai_calc_price(monkeypatch, total_price=0.007)
    warnings: list[str] = []

    metadata = estimate_genai_prices_cost(
        provider="google_gemini",
        model="gemini/gemini-1.5-flash",
        usage={"input_tokens": 1000, "output_tokens": 500, "thoughtsTokenCount": 100},
        warnings=warnings,
        adapter_name="Gemini Interactions",
    )

    assert metadata.estimated_cost_usd == 0.007
    assert metadata.cost_source == "calculator"
    assert metadata.cost_source_label == "genai-prices"
    assert metadata.pricing_version is not None
    assert metadata.pricing_version.startswith("genai-prices:")
    assert warnings == []
    assert calls == [
        {
            "usage": {
                "input_tokens": 1000,
                "output_tokens": 600,
                "cache_read_tokens": None,
                "cache_write_tokens": None,
            },
            "model_ref": "gemini-1.5-flash",
            "provider_id": "google",
        }
    ]


def test_genai_prices_helper_resolves_google_flash_latest_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_fake_genai_calc_price(monkeypatch, total_price=0.0195)
    warnings: list[str] = []

    metadata = estimate_genai_prices_cost(
        provider="google_gemini",
        model="gemini-flash-latest",
        usage={"input_tokens": 100, "output_tokens": 50},
        warnings=warnings,
        adapter_name="Google ADK",
    )

    assert metadata.estimated_cost_usd == 0.0195
    assert metadata.cost_source == "calculator"
    assert metadata.cost_source_label == "genai-prices"
    assert warnings == []
    assert calls == [
        {
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_read_tokens": None,
                "cache_write_tokens": None,
            },
            "model_ref": "gemini-3.5-flash",
            "provider_id": "google",
        }
    ]


def test_genai_prices_helper_includes_anthropic_cache_buckets_in_pricing_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_fake_genai_calc_price(monkeypatch, total_price=0.006)
    warnings: list[str] = []
    usage = {
        "input_tokens": 10,
        "output_tokens": 5,
        "cache_creation_input_tokens": 7,
        "cache_read_input_tokens": 11,
    }

    metadata = estimate_genai_prices_cost(
        provider="anthropic",
        model="anthropic/claude-3-5-haiku-latest",
        usage=usage,
        warnings=warnings,
        adapter_name="Claude Agent SDK",
    )
    record = build_usage_record(
        adapter="claude_agent_sdk",
        surface="agent_invocation",
        usage=usage,
    )

    assert metadata.estimated_cost_usd == 0.006
    assert warnings == []
    assert calls == [
        {
            "usage": {
                "input_tokens": 28,
                "output_tokens": 5,
                "cache_read_tokens": 11,
                "cache_write_tokens": 7,
            },
            "model_ref": "claude-3-5-haiku-latest",
            "provider_id": "anthropic",
        }
    ]
    assert record["usage"]["input_tokens"] == 10
    assert record["usage"]["cached_input_tokens"] == 11
    assert record["usage"]["raw"] == usage


def test_genai_prices_helper_keeps_openai_reasoning_in_completion_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_fake_genai_calc_price(monkeypatch, total_price=0.008)
    warnings: list[str] = []

    metadata = estimate_genai_prices_cost(
        provider="openai",
        model="openai/gpt-4o-mini",
        usage={
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "completion_tokens_details": {"reasoning_tokens": 10},
        },
        warnings=warnings,
        adapter_name="OpenAI Agents",
    )

    assert metadata.estimated_cost_usd == 0.008
    assert warnings == []
    assert calls == [
        {
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_read_tokens": None,
                "cache_write_tokens": None,
            },
            "model_ref": "gpt-4o-mini",
            "provider_id": "openai",
        }
    ]


@pytest.mark.parametrize(
    ("provider", "model", "expected_provider"),
    [
        (None, "gpt-4o-mini", "openai"),
        ("unknown", "gpt-4o-mini", None),
        (None, "unknown/gpt-4o-mini", None),
        ("unknown", "unknown/gpt-4o-mini", None),
    ],
)
def test_normalize_provider_id_handles_unknown_provider_and_model_prefix(
    provider: str | None, model: str, expected_provider: str | None
) -> None:
    assert _normalize_provider_id(provider, model) == expected_provider


@pytest.mark.parametrize("model", ["gpt-4o-mini", "gpt-4o-mini-2024-07-18"])
def test_genai_prices_helper_prices_bare_openai_model_names(
    model: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_fake_genai_calc_price(monkeypatch, total_price=0.011)
    warnings: list[str] = []

    metadata = estimate_genai_prices_cost(
        provider=None,
        model=model,
        usage={"request_tokens": 100, "tokens_output": 50},
        warnings=warnings,
        adapter_name="LangGraph",
    )

    assert metadata.estimated_cost_usd == 0.011
    assert metadata.cost_source == "calculator"
    assert warnings == []
    assert calls == [
        {
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_read_tokens": None,
                "cache_write_tokens": None,
            },
            "model_ref": model,
            "provider_id": "openai",
        }
    ]


@pytest.mark.parametrize(
    ("model", "expected_provider"),
    [
        ("claude-haiku-4-5", "anthropic"),
        ("gemini-2.5-flash", "google"),
        ("mistral-large-latest", "mistral"),
        ("codestral-latest", "mistral"),
        ("devstral-small-latest", "mistral"),
        ("magistral-medium-latest", "mistral"),
        ("ministral-8b-latest", "mistral"),
        ("deepseek-chat", "deepseek"),
        ("command-r-plus", "cohere"),
        ("grok-3-mini", "xai"),
    ],
)
def test_genai_prices_helper_prices_well_known_bare_non_openai_model_names(
    model: str,
    expected_provider: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_fake_genai_calc_price(monkeypatch, total_price=0.013)
    warnings: list[str] = []

    metadata = estimate_genai_prices_cost(
        provider=None,
        model=model,
        usage={"input_tokens": 100, "output_tokens": 50},
        warnings=warnings,
        adapter_name="LangGraph",
    )

    assert metadata.estimated_cost_usd == 0.013
    assert metadata.cost_source == "calculator"
    assert warnings == []
    assert calls == [
        {
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_read_tokens": None,
                "cache_write_tokens": None,
            },
            "model_ref": model,
            "provider_id": expected_provider,
        }
    ]


@pytest.mark.parametrize(
    ("model", "expected_provider", "expected_model_ref"),
    [
        ("groq/llama-3.1-8b-instant", "groq", "llama-3.1-8b-instant"),
        ("mistral/mistral-small-latest", "mistral", "mistral-small-latest"),
        ("deepseek/deepseek-chat", "deepseek", "deepseek-chat"),
        ("cohere/command-r", "cohere", "command-r"),
        ("openrouter/openai/gpt-4o-mini", "openrouter", "openai/gpt-4o-mini"),
        ("x-ai/grok-2", "xai", "grok-2"),
        ("xai/grok-2", "xai", "grok-2"),
        ("azure/gpt-4o-mini", "azure", "gpt-4o-mini"),
        ("aws/anthropic.claude-3-haiku", "aws", "anthropic.claude-3-haiku"),
        ("bedrock/anthropic.claude-3-haiku", "aws", "anthropic.claude-3-haiku"),
        ("together/meta-llama/Llama-3", "together", "meta-llama/Llama-3"),
        ("perplexity/sonar", "perplexity", "sonar"),
        (
            "fireworks/accounts/fireworks/models/llama-v3",
            "fireworks",
            "accounts/fireworks/models/llama-v3",
        ),
    ],
)
def test_genai_prices_helper_passes_through_supported_non_big_three_provider(
    model: str,
    expected_provider: str,
    expected_model_ref: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_fake_genai_calc_price(monkeypatch, total_price=0.022)
    warnings: list[str] = []

    metadata = estimate_genai_prices_cost(
        provider=None,
        model=model,
        usage={"input_token_count": 10, "output_token_count": 5},
        warnings=warnings,
        adapter_name="LangGraph",
    )

    assert metadata.estimated_cost_usd == 0.022
    assert metadata.cost_source == "calculator"
    assert warnings == []
    assert calls == [
        {
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "cache_read_tokens": None,
                "cache_write_tokens": None,
            },
            "model_ref": expected_model_ref,
            "provider_id": expected_provider,
        }
    ]


def test_genai_prices_helper_rejects_unknown_provider_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_fake_genai_calc_price(monkeypatch, total_price=0.022)
    warnings: list[str] = []

    metadata = estimate_genai_prices_cost(
        provider=None,
        model="some-random-prefix/model",
        usage={"input_token_count": 10, "output_token_count": 5},
        warnings=warnings,
        adapter_name="LangGraph",
    )

    assert metadata.estimated_cost_usd is None
    assert metadata.cost_source == "none"
    assert metadata.cost_source_label is None
    assert warnings == []
    assert calls == []


def test_genai_prices_helper_rejects_unknown_explicit_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_fake_genai_calc_price(monkeypatch, total_price=0.022)
    warnings: list[str] = []

    metadata = estimate_genai_prices_cost(
        provider="some-gateway",
        model="gpt-4o-mini",
        usage={"input_tokens": 10, "output_tokens": 5},
        warnings=warnings,
        adapter_name="LangGraph",
    )

    assert metadata.estimated_cost_usd is None
    assert metadata.cost_source == "none"
    assert metadata.cost_source_label is None
    assert warnings == []
    assert calls == []


@pytest.mark.parametrize(
    ("provider", "model"),
    [
        ("google_gemini", "models/gemini-1.5-flash"),
        ("google_vertex", "publishers/google/models/gemini-1.5-flash"),
        (
            "google_vertex",
            "projects/demo/locations/us-central1/publishers/google/models/gemini-1.5-flash",
        ),
    ],
)
def test_genai_prices_helper_normalizes_google_model_resource_names(
    provider: str,
    model: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_fake_genai_calc_price(monkeypatch, total_price=0.009)
    warnings: list[str] = []

    metadata = estimate_genai_prices_cost(
        provider=provider,
        model=model,
        usage={"input_tokens": 10, "output_tokens": 5},
        warnings=warnings,
        adapter_name="Gemini Interactions",
    )

    assert metadata.estimated_cost_usd == 0.009
    assert warnings == []
    assert calls == [
        {
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "cache_read_tokens": None,
                "cache_write_tokens": None,
            },
            "model_ref": "gemini-1.5-flash",
            "provider_id": "google",
        }
    ]


def test_genai_prices_helper_respects_estimated_cost_opt_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "kitaru.config.resolve_llm_estimated_cost_policy",
        lambda: "off",
    )
    calls = install_fake_genai_calc_price(monkeypatch)
    warnings: list[str] = []

    metadata = estimate_genai_prices_cost(
        provider="openai",
        model="gpt-4o-mini",
        usage={"input_tokens": 10, "output_tokens": 5},
        warnings=warnings,
        adapter_name="OpenAI Agents",
    )

    assert metadata.estimated_cost_usd is None
    assert metadata.cost_source == "none"
    assert metadata.cost_source_label is None
    assert warnings == []
    assert calls == []


def test_genai_prices_helper_failure_is_non_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_fake_genai_calc_price(
        monkeypatch,
        calc_error=LookupError("no price"),
    )
    warnings: list[str] = []

    metadata = estimate_genai_prices_cost(
        provider="openai",
        model="gpt-4o-mini",
        usage={"input_tokens": 10, "output_tokens": 5},
        warnings=warnings,
        adapter_name="OpenAI Agents",
    )

    assert metadata.estimated_cost_usd is None
    assert metadata.cost_source == "calculator_error"
    assert metadata.cost_source_label == "genai-prices"
    assert len(warnings) == 1
    assert "LookupError: no price" in warnings[0]


def test_provider_model_ref_preserves_unknown_prefix_without_provider() -> None:
    assert (
        _provider_model_ref("unknown-prefix/model-name", None)
        == "unknown-prefix/model-name"
    )
    assert _provider_model_ref("openai/gpt-4o-mini", "openai") == "gpt-4o-mini"


@pytest.mark.parametrize(
    "usage",
    [
        {"request_tokens": 1},
        {"tokens_output": 1},
        {"input_token_count": 1},
        {"candidatesTokenCount": 1},
        {"cachedContentTokenCount": 1},
        {"thoughts_token_count": 1},
        {"cache_write_tokens": 1},
    ],
)
def test_genai_pricing_token_gate_accepts_token_usage_aliases(
    usage: dict[str, int],
) -> None:
    assert _usage_has_genai_pricing_tokens(usage)


def test_genai_prices_helper_unknown_provider_records_tokens_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = install_fake_genai_calc_price(monkeypatch)
    warnings: list[str] = []

    metadata = estimate_genai_prices_cost(
        provider=None,
        model="unprefixed-model",
        usage={"input_tokens": 10, "output_tokens": 5},
        warnings=warnings,
        adapter_name="LangGraph",
    )

    assert metadata.estimated_cost_usd is None
    assert metadata.cost_source == "none"
    assert metadata.cost_source_label is None
    assert warnings == []
    assert calls == []


def test_calculated_or_genai_cost_metadata_treats_calculator_none_as_no_estimate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    genai_calls = install_fake_genai_calc_price(monkeypatch, total_price=0.99)
    warnings: list[str] = []

    def decline_cost(_usage: object) -> float | None:
        return None

    metadata = calculated_or_genai_cost_metadata(
        calculator=decline_cost,
        calculator_usage={"input_tokens": 10, "output_tokens": 5},
        genai_provider="openai",
        genai_model="gpt-4o-mini",
        genai_usage={"input_tokens": 10, "output_tokens": 5},
        warnings=warnings,
        adapter_name="OpenAI Agents",
        calculator_source_label="openai_agents.cost_calculator",
    )

    assert metadata.estimated_cost_usd is None
    assert metadata.cost_source == "none"
    assert metadata.cost_source_label is None
    assert genai_calls == []
    assert warnings == []


def test_calculated_or_genai_cost_metadata_does_not_fallback_after_calculator_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    genai_calls = install_fake_genai_calc_price(monkeypatch, total_price=0.99)
    warnings: list[str] = []

    def fail_cost(_usage: object) -> float:
        raise RuntimeError("boom")

    metadata = calculated_or_genai_cost_metadata(
        calculator=fail_cost,
        calculator_usage={"input_tokens": 10, "output_tokens": 5},
        genai_provider="openai",
        genai_model="gpt-4o-mini",
        genai_usage={"input_tokens": 10, "output_tokens": 5},
        warnings=warnings,
        adapter_name="OpenAI Agents",
        calculator_source_label="openai_agents.cost_calculator",
    )

    assert metadata.estimated_cost_usd is None
    assert metadata.cost_source == "calculator_error"
    assert metadata.cost_source_label == "openai_agents.cost_calculator"
    assert genai_calls == []
    assert len(warnings) == 1
    assert "RuntimeError: boom" in warnings[0]


def test_add_optional_token_count_preserves_missing_values() -> None:
    assert add_optional_token_count(None, None) is None
    assert add_optional_token_count(None, True) is None
    assert add_optional_token_count(None, 0) == 0
    assert add_optional_token_count(3, "4") == 7


def test_aggregate_prefers_actual_cost_for_display() -> None:
    actual_record = build_usage_record(
        adapter="claude_agent_sdk",
        surface="agent_invocation",
        record_id="actual",
        input_tokens=10,
        output_tokens=5,
        actual_cost_usd=0.25,
    )
    estimated_record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="estimated",
        input_tokens=7,
        output_tokens=3,
        estimated_cost_usd=0.04,
    )

    summary = aggregate_usage_records([actual_record, estimated_record])

    assert summary["usage_record_count"] == 2
    assert summary["incurred_usage_record_count"] == 2
    assert summary["total_tokens"] == 25
    assert summary["actual_cost_usd"] == 0.25
    assert summary["estimated_cost_usd"] == 0.04
    assert summary["display_cost_usd"] == 0.29


def test_aggregate_preserves_explicit_zero_total_tokens() -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="zero-total",
        input_tokens=10,
        output_tokens=5,
        total_tokens=0,
    )

    summary = aggregate_usage_records([record])

    assert summary["input_tokens"] == 10
    assert summary["output_tokens"] == 5
    assert summary["total_tokens"] == 0
    assert summary["incurred_total_tokens"] == 0


def test_reused_records_do_not_add_incurred_cost_or_tokens() -> None:
    record = build_usage_record(
        adapter="pydantic_ai",
        surface="model_call",
        record_id="cached",
        input_tokens=100,
        output_tokens=20,
        estimated_cost_usd=0.5,
        billing_effect="reused_not_incurred",
        cache_status="checkpoint_cache_hit",
    )

    summary = aggregate_usage_records([record])

    assert summary["usage_record_count"] == 1
    assert summary["incurred_usage_record_count"] == 0
    assert summary["reused_usage_record_count"] == 1
    assert summary["total_tokens"] == 120
    assert summary["reused_total_tokens"] == 120
    assert summary["incurred_total_tokens"] == 0
    assert summary["display_cost_usd"] == 0.0


def test_retry_attempts_with_same_record_id_are_counted_separately() -> None:
    first_attempt = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="same-call",
        total_tokens=11,
    )
    second_attempt = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="same-call",
        total_tokens=13,
    )

    records = [
        *usage_records_from_metadata(
            {LLM_USAGE_METADATA_KEY: {"same-call": first_attempt}},
            source_attempt_id="attempt-1",
        ),
        *usage_records_from_metadata(
            {LLM_USAGE_METADATA_KEY: {"same-call": second_attempt}},
            source_attempt_id="attempt-2",
        ),
    ]

    summary = aggregate_usage_records(records)

    assert summary["usage_record_count"] == 2
    assert summary["incurred_total_tokens"] == 24


def test_duplicate_record_from_same_attempt_is_counted_once() -> None:
    record = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="same-call",
        total_tokens=11,
    )
    records = usage_records_from_metadata(
        {LLM_USAGE_METADATA_KEY: {"first": record, "duplicate": record}},
        source_attempt_id="attempt-1",
    )

    summary = aggregate_usage_records(records)

    assert summary["usage_record_count"] == 1
    assert summary["total_tokens"] == 11


def test_zero_token_values_are_preserved_as_present_values() -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        usage={
            "input_tokens": 0,
            "prompt_tokens": 99,
            "output_tokens": 0,
            "completion_tokens": 88,
            "total_tokens": 0,
            "cached_input_tokens": 0,
            "details": {"cached_tokens": 77},
            "reasoning_tokens": 0,
            "output_tokens_details": {"reasoning_tokens": 66},
        },
    )

    assert record["usage"]["input_tokens"] == 0
    assert record["usage"]["output_tokens"] == 0
    assert record["usage"]["total_tokens"] == 0
    assert record["usage"]["cached_input_tokens"] == 0
    assert record["usage"]["reasoning_tokens"] == 0


def test_gemini_snake_case_usage_fields_are_normalized() -> None:
    record = build_usage_record(
        adapter="gemini_interactions",
        surface="gemini_interaction",
        usage={
            "prompt_token_count": 10,
            "candidates_token_count": 4,
            "total_token_count": 17,
            "cached_content_token_count": 3,
            "thoughts_token_count": 2,
            "tool_use_prompt_token_count": 1,
        },
    )

    assert record["usage"]["input_tokens"] == 10
    assert record["usage"]["output_tokens"] == 4
    assert record["usage"]["total_tokens"] == 17
    assert record["usage"]["cached_input_tokens"] == 3
    assert record["usage"]["reasoning_tokens"] == 2
    assert record["usage"]["raw"]["tool_use_prompt_token_count"] == 1


def test_gemini_camel_case_usage_fields_are_normalized() -> None:
    record = build_usage_record(
        adapter="gemini_interactions",
        surface="gemini_interaction",
        usage={
            "promptTokenCount": 12,
            "candidatesTokenCount": 5,
            "totalTokenCount": 20,
            "cachedContentTokenCount": 4,
            "thoughtsTokenCount": 3,
            "toolUsePromptTokenCount": 2,
        },
    )

    assert record["usage"]["input_tokens"] == 12
    assert record["usage"]["output_tokens"] == 5
    assert record["usage"]["total_tokens"] == 20
    assert record["usage"]["cached_input_tokens"] == 4
    assert record["usage"]["reasoning_tokens"] == 3
    assert record["usage"]["raw"]["toolUsePromptTokenCount"] == 2


def test_gemini_explicit_zero_usage_values_are_preserved() -> None:
    record = build_usage_record(
        adapter="gemini_interactions",
        surface="gemini_interaction",
        usage={
            "prompt_token_count": 0,
            "promptTokenCount": 99,
            "candidates_token_count": 0,
            "candidatesTokenCount": 88,
            "total_token_count": 0,
            "totalTokenCount": 77,
            "cached_content_token_count": 0,
            "cachedContentTokenCount": 66,
            "thoughts_token_count": 0,
            "thoughtsTokenCount": 55,
        },
    )

    assert record["usage"]["input_tokens"] == 0
    assert record["usage"]["output_tokens"] == 0
    assert record["usage"]["total_tokens"] == 0
    assert record["usage"]["cached_input_tokens"] == 0
    assert record["usage"]["reasoning_tokens"] == 0


def test_gemini_token_only_record_aggregates_without_cost() -> None:
    record = build_usage_record(
        adapter="gemini_interactions",
        surface="gemini_interaction",
        record_id="gemini-call",
        model="gemini-test",
        usage={
            "promptTokenCount": 8,
            "candidatesTokenCount": 6,
            "totalTokenCount": 14,
        },
        cost_source="none",
    )

    summary = aggregate_usage_records([record])

    assert summary["usage_record_count"] == 1
    assert summary["incurred_usage_record_count"] == 1
    assert summary["input_tokens"] == 8
    assert summary["output_tokens"] == 6
    assert summary["total_tokens"] == 14
    assert summary["actual_cost_usd"] == 0.0
    assert summary["estimated_cost_usd"] == 0.0
    assert summary["display_cost_usd"] == 0.0
    assert summary["records_without_cost_count"] == 1
    assert summary["adapters"] == ["gemini_interactions"]
    assert summary["models"] == ["gemini-test"]


@pytest.mark.parametrize("invalid_cost", [True, -0.01, math.nan, math.inf, "bad"])
def test_invalid_cost_values_are_omitted(invalid_cost: object) -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="bad-cost",
        total_tokens=3,
        actual_cost_usd=cast(float, invalid_cost),
        estimated_cost_usd=cast(float, invalid_cost),
    )

    assert record["cost"]["actual_cost_usd"] is None
    assert record["cost"]["estimated_cost_usd"] is None

    summary = aggregate_usage_records([record])
    assert summary["display_cost_usd"] == 0.0
    assert summary["records_without_cost_count"] == 1


def test_zero_cost_values_are_preserved() -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        actual_cost_usd=0,
        estimated_cost_usd=0,
    )

    assert record["cost"]["actual_cost_usd"] == 0.0
    assert record["cost"]["estimated_cost_usd"] == 0.0


def test_malformed_records_are_ignored_during_parsing() -> None:
    records = usage_records_from_metadata(
        {
            LLM_USAGE_METADATA_KEY: {
                "bad": {"schema_version": 1, "record_id": "missing-blocks"},
                "not-a-record": "hello",
            }
        }
    )

    assert records == []


def test_aggregate_skips_malformed_records_without_counting_them() -> None:
    record = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="valid",
        total_tokens=5,
    )

    summary = aggregate_usage_records(
        [record, {"schema_version": 1, "record_id": "missing-blocks"}]
    )

    assert summary["usage_record_count"] == 1
    assert "malformed_record_count" not in summary


def test_parsed_records_preserve_billing_and_cache_status() -> None:
    record = build_usage_record(
        adapter="pydantic_ai",
        surface="model_call",
        record_id="cached",
        billing_effect="reused_not_incurred",
        cache_status="checkpoint_cache_hit",
    )

    records = usage_records_from_metadata({LLM_USAGE_METADATA_KEY: {"cached": record}})

    assert records[0]["billing_effect"] == "reused_not_incurred"
    assert records[0]["cache_status"] == "checkpoint_cache_hit"


def test_log_usage_record_best_effort_suppresses_backend_errors(monkeypatch) -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="provider-call",
        total_tokens=5,
    )

    def fail_log(**metadata: object) -> None:
        raise RuntimeError("metadata backend down")

    monkeypatch.setattr("kitaru.logging.log", fail_log)

    log_usage_record_best_effort(record)


def test_execution_metadata_omits_empty_summary_by_default() -> None:
    assert execution_metadata_from_records([]) == {}
    assert LLM_USAGE_SUMMARY_METADATA_KEY in execution_metadata_from_records(
        [],
        include_empty_summary=True,
    )


def test_execution_metadata_is_deterministic_json_summary() -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="run",
        total_tokens=42,
        estimated_cost_usd=0.123,
    )

    first = execution_metadata_from_records([record])
    second = execution_metadata_from_records([record])

    assert first == second
    assert isinstance(first[LLM_USAGE_SUMMARY_METADATA_KEY], str)
    assert parse_usage_summary(
        first[LLM_USAGE_SUMMARY_METADATA_KEY]
    ) == parse_usage_summary(second[LLM_USAGE_SUMMARY_METADATA_KEY])


def test_execution_metadata_uses_json_summary_and_flat_numeric_keys() -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="run",
        total_tokens=42,
        estimated_cost_usd=0.123,
    )

    metadata = execution_metadata_from_records([record])
    summary = parse_usage_summary(metadata[LLM_USAGE_SUMMARY_METADATA_KEY])

    assert summary is not None
    assert summary["usage_record_count"] == 1
    assert metadata[LLM_FLAT_INCURRED_USAGE_RECORD_COUNT_KEY] == 1
    assert metadata[LLM_FLAT_REUSED_USAGE_RECORD_COUNT_KEY] == 0
    assert metadata[LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY] == 42
    assert metadata[LLM_FLAT_REUSED_TOTAL_TOKENS_KEY] == 0
    assert metadata[LLM_FLAT_ACTUAL_COST_USD_KEY] == 0.0
    assert metadata[LLM_FLAT_ESTIMATED_COST_USD_KEY] == 0.123
    assert metadata[LLM_FLAT_DISPLAY_COST_USD_KEY] == 0.123


def test_flat_usage_metadata_from_records_omits_summary_key() -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="checkpoint",
        total_tokens=42,
        estimated_cost_usd=0.123,
    )

    metadata = flat_usage_metadata_from_records([record])

    assert LLM_USAGE_SUMMARY_METADATA_KEY not in metadata
    assert metadata[LLM_FLAT_INCURRED_USAGE_RECORD_COUNT_KEY] == 1
    assert metadata[LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY] == 42
    assert metadata[LLM_FLAT_DISPLAY_COST_USD_KEY] == 0.123


def test_flat_usage_metadata_from_records_omits_empty_by_default() -> None:
    assert flat_usage_metadata_from_records([]) == {}
    assert (
        flat_usage_metadata_from_records([], include_empty_metadata=True)[
            LLM_FLAT_DISPLAY_COST_USD_KEY
        ]
        == 0.0
    )


def test_metadata_matches_flat_usage_metadata_requires_complete_matching_keys() -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="checkpoint",
        total_tokens=42,
        estimated_cost_usd=0.123,
    )
    metadata = flat_usage_metadata_from_records([record])
    partial = dict(metadata)
    partial.pop(LLM_FLAT_DISPLAY_COST_USD_KEY)
    stale = dict(metadata)
    stale[LLM_FLAT_DISPLAY_COST_USD_KEY] = 0.0

    assert metadata_matches_flat_usage_metadata(metadata, metadata) is True
    assert metadata_matches_flat_usage_metadata(partial, metadata) is False
    assert metadata_matches_flat_usage_metadata(stale, metadata) is False


def test_cached_checkpoint_attempt_public_records_are_marked_reused() -> None:
    from kitaru._client._mappers import _map_checkpoint_attempt

    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="cached-call",
        total_tokens=100,
        actual_cost_usd=1.25,
        billing_effect="incurred",
        cache_status="executed",
    )
    attempt = _map_checkpoint_attempt(
        cast(
            Any,
            SimpleNamespace(
                id="attempt-cached",
                name="cached_call",
                status="cached",
                start_time=None,
                end_time=None,
                run_metadata={LLM_USAGE_METADATA_KEY: {"cached-call": record}},
                exception_info=None,
            ),
        )
    )

    records = attempt.llm_usage_records

    assert attempt.status is ExecutionStatus.COMPLETED
    assert len(records) == 1
    assert records[0]["billing_effect"] == "reused_not_incurred"
    assert records[0]["cache_status"] == "checkpoint_cache_hit"
    assert records[0]["cost"]["actual_cost_usd"] == 1.25
    assert "_source_attempt_id" not in records[0]


def test_replay_like_mapped_attempt_preserves_persisted_incurred_record() -> None:
    from kitaru._client._mappers import _map_checkpoint_attempt

    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="mapped-replay-tail-call",
        total_tokens=37,
        billing_effect="incurred",
        cache_status="executed",
    )
    attempt = _map_checkpoint_attempt(
        cast(
            Any,
            SimpleNamespace(
                id="attempt-mapped-tail",
                name="write",
                status="replay_reused",
                start_time=None,
                end_time=None,
                run_metadata={
                    LLM_USAGE_METADATA_KEY: {"mapped-replay-tail-call": record}
                },
                exception_info=None,
            ),
        )
    )

    records = attempt.llm_usage_records

    assert attempt.status is ExecutionStatus.COMPLETED
    assert len(records) == 1
    assert records[0]["billing_effect"] == "incurred"
    assert records[0]["cache_status"] == "executed"


def test_mapped_execution_uses_replay_skip_metadata_for_public_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru._client._mappers import _map_execution
    from kitaru.replay import REPLAY_SKIPPED_STEPS_METADATA_KEY

    fetch_record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="fetch-call",
        total_tokens=13,
        billing_effect="incurred",
        cache_status="executed",
    )
    write_record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="write-call",
        total_tokens=37,
        billing_effect="incurred",
        cache_status="executed",
    )
    fetch_step = SimpleNamespace(
        id="fetch-attempt",
        name="fetch",
        version=1,
        status="replay_reused",
        start_time=None,
        end_time=None,
        run_metadata={LLM_USAGE_METADATA_KEY: {"fetch-call": fetch_record}},
        exception_info=None,
        spec=SimpleNamespace(invocation_id="fetch"),
        original_step_run_id=None,
        parent_step_ids=[],
        inputs={},
        outputs={},
        type=None,
    )
    write_step = SimpleNamespace(
        id="write-attempt",
        name="write",
        version=1,
        status="replay_reused",
        start_time=None,
        end_time=None,
        run_metadata={LLM_USAGE_METADATA_KEY: {"write-call": write_record}},
        exception_info=None,
        spec=SimpleNamespace(invocation_id="write"),
        original_step_run_id=None,
        parent_step_ids=[],
        inputs={},
        outputs={},
        type=None,
    )
    run = SimpleNamespace(
        id="replay-run",
        status="completed",
        status_reason=None,
        exception_info=None,
        run_metadata={REPLAY_SKIPPED_STEPS_METADATA_KEY: ["fetch"]},
        steps={"fetch": fetch_step, "write": write_step},
        pipeline=SimpleNamespace(id="flow-1", name="sample_flow"),
        original_run=None,
        stack=SimpleNamespace(name="local"),
        start_time=None,
        end_time=None,
    )
    monkeypatch.setattr(
        "kitaru._client._mappers._list_checkpoint_attempts_for_run",
        lambda *, run, client: {
            "fetch": [fetch_step],
            "write": [write_step],
        },
    )

    execution = _map_execution(
        run=cast(Any, run),
        client=cast(Any, SimpleNamespace()),
        include_details=True,
    )
    records_by_id = {
        record["record_id"]: record for record in execution.llm_usage_records
    }

    assert records_by_id["fetch-call"]["billing_effect"] == "reused_not_incurred"
    assert records_by_id["fetch-call"]["cache_status"] == "replay_reused"
    assert records_by_id["write-call"]["billing_effect"] == "incurred"
    assert records_by_id["write-call"]["cache_status"] == "executed"


def test_replay_like_raw_status_preserves_persisted_incurred_record() -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="replay-tail-call",
        total_tokens=37,
        billing_effect="incurred",
        cache_status="executed",
    )
    attempt = CheckpointAttempt(
        attempt_id="attempt-tail",
        status=ExecutionStatus.COMPLETED,
        started_at=None,
        ended_at=None,
        metadata={LLM_USAGE_METADATA_KEY: {"replay-tail-call": record}},
        failure=None,
        _raw_status="replay_reused",
    )

    records = attempt.llm_usage_records

    assert len(records) == 1
    assert records[0]["billing_effect"] == "incurred"
    assert records[0]["cache_status"] == "executed"


def test_replay_skipped_attempt_public_records_are_marked_reused() -> None:
    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="replay-upstream-call",
        total_tokens=37,
        billing_effect="incurred",
        cache_status="executed",
    )
    attempt = CheckpointAttempt(
        attempt_id="attempt-upstream",
        status=ExecutionStatus.COMPLETED,
        started_at=None,
        ended_at=None,
        metadata={LLM_USAGE_METADATA_KEY: {"replay-upstream-call": record}},
        failure=None,
        _raw_status="replay_reused",
        _replay_reused=True,
    )

    records = attempt.llm_usage_records

    assert len(records) == 1
    assert records[0]["billing_effect"] == "reused_not_incurred"
    assert records[0]["cache_status"] == "replay_reused"


def test_execution_llm_usage_records_include_unique_run_level_records() -> None:
    checkpoint_record = build_usage_record(
        adapter="pydantic_ai",
        surface="model_call",
        record_id="same-record",
        event_id="same-event",
        total_tokens=11,
    )
    run_level_record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="run-only",
        event_id="run-only-event",
        total_tokens=7,
    )
    duplicate_run_record = dict(checkpoint_record)
    attempt = CheckpointAttempt(
        attempt_id="attempt-1",
        status=ExecutionStatus.COMPLETED,
        started_at=None,
        ended_at=None,
        metadata={LLM_USAGE_METADATA_KEY: {"checkpoint": checkpoint_record}},
        failure=None,
    )
    checkpoint = CheckpointCall(
        call_id="call-1",
        name="model_call",
        status=ExecutionStatus.COMPLETED,
        started_at=None,
        ended_at=None,
        metadata={},
        original_call_id=None,
        parent_call_ids=[],
        failure=None,
        attempts=[attempt],
        artifacts=[],
    )
    execution = Execution(
        exec_id="exec-1",
        flow_id=None,
        flow_name=None,
        status=ExecutionStatus.COMPLETED,
        started_at=None,
        ended_at=None,
        stack_name=None,
        metadata={
            LLM_USAGE_METADATA_KEY: {
                "duplicate": duplicate_run_record,
                "run-only": run_level_record,
            }
        },
        status_reason=None,
        failure=None,
        pending_wait=None,
        frozen_execution_spec=None,
        original_exec_id=None,
        checkpoints=[checkpoint],
        artifacts=[],
        _client=cast(Any, None),
    )

    records = execution.llm_usage_records

    assert [record["record_id"] for record in records] == [
        "run-only",
        "same-record",
    ]
    assert all("_source_attempt_id" not in record for record in records)
