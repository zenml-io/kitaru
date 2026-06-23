"""Tests for OpenAI Agents usage normalization."""

from types import SimpleNamespace
from typing import Any, cast

import pytest

import kitaru.adapters.openai_agents._agent as agent_module
from kitaru._llm_usage import LLM_USAGE_METADATA_KEY, build_usage_record
from kitaru.adapters.openai_agents._agent import KitaruRunner
from kitaru.adapters.openai_agents._policy import OpenAICapturePolicy
from kitaru.adapters.openai_agents._types import OpenAIRunResult
from kitaru.adapters.openai_agents._usage import normalize_usage
from tests._genai_prices_helpers import install_fake_genai_calc_price


def test_normalize_usage_reads_standard_openai_dict_shape() -> None:
    usage = normalize_usage(
        {
            "prompt_tokens": 10,
            "completion_tokens": 4,
            "prompt_tokens_details": {"cached_tokens": 3},
            "completion_tokens_details": {"reasoning_tokens": 2},
        },
        model_name="gpt-5-mini",
    )

    assert usage.model_name == "gpt-5-mini"
    assert usage.input_tokens == 10
    assert usage.output_tokens == 4
    assert usage.total_tokens == 14
    assert usage.cached_input_tokens == 3
    assert usage.reasoning_tokens == 2


def test_normalize_usage_reads_agents_object_shape() -> None:
    raw_usage = SimpleNamespace(
        input_tokens=12,
        output_tokens=8,
        total_tokens=20,
        input_tokens_details=SimpleNamespace(cached_tokens=5),
        output_tokens_details=SimpleNamespace(reasoning_tokens=7),
    )

    usage = normalize_usage(raw_usage)

    assert usage.input_tokens == 12
    assert usage.output_tokens == 8
    assert usage.total_tokens == 20
    assert usage.cached_input_tokens == 5
    assert usage.reasoning_tokens == 7
    assert usage.raw is not None


def test_normalize_usage_preserves_nested_zero_values() -> None:
    usage = normalize_usage(
        {
            "input_tokens": 5,
            "output_tokens": 2,
            "cached_input_tokens": 0,
            "input_tokens_details": {"cached_tokens": 4},
            "reasoning_tokens": 0,
            "output_tokens_details": {"reasoning_tokens": 3},
        }
    )

    assert usage.cached_input_tokens == 0
    assert usage.reasoning_tokens == 0


def test_openai_usage_summary_populates_canonical_token_totals() -> None:
    usage = normalize_usage(
        {
            "input_tokens": 21,
            "output_tokens": 9,
            "input_tokens_details": {"cached_tokens": 6},
            "output_tokens_details": {"reasoning_tokens": 4},
        }
    )

    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        usage=usage.model_dump(mode="json"),
    )

    assert record["usage"]["input_tokens"] == 21
    assert record["usage"]["output_tokens"] == 9
    assert record["usage"]["total_tokens"] == 30
    assert record["usage"]["cached_input_tokens"] == 6
    assert record["usage"]["reasoning_tokens"] == 4


def test_finalize_run_result_keeps_successful_run_when_cost_calculator_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logged: list[dict[str, Any]] = []

    def fail_cost(_usage: object) -> float:
        raise RuntimeError("pricing service down")

    genai_calls = install_fake_genai_calc_price(monkeypatch, total_price=0.99)
    monkeypatch.setattr(agent_module, "log_usage_record", logged.append)
    runner = KitaruRunner(SimpleNamespace(name="agent"), cost_calculator=fail_cost)

    result = runner._finalize_run_result(
        OpenAIRunResult(
            status="completed",
            usage={
                "model_name": "gpt-4o-mini",
                "input_tokens": 1,
                "output_tokens": 2,
                "total_tokens": 3,
            },
        ),
        tracker=SimpleNamespace(
            run_label="run-1",
            event_log_artifact_name="events",
            run_summary_artifact_name="summary",
        ),
    )

    assert result.estimated_cost_usd is None
    assert any("cost calculator failed" in warning for warning in result.warnings)
    assert len(logged) == 1
    assert logged[0]["cost"]["estimated_cost_usd"] is None
    assert logged[0]["warnings"] == result.warnings
    assert genai_calls == []


@pytest.mark.parametrize("invalid_cost", [None, True, -1, float("nan"), "bad"])
def test_finalize_run_result_ignores_invalid_cost_calculator_return(
    invalid_cost: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(agent_module, "log_usage_record", lambda _record: None)
    runner = KitaruRunner(
        SimpleNamespace(name="agent"),
        cost_calculator=cast(Any, lambda _usage: invalid_cost),
    )

    result = runner._finalize_run_result(
        OpenAIRunResult(status="completed", usage={"total_tokens": 3}),
        tracker=SimpleNamespace(
            run_label="run-1",
            event_log_artifact_name="events",
            run_summary_artifact_name="summary",
        ),
    )

    assert result.estimated_cost_usd is None
    assert any("invalid estimated cost" in warning for warning in result.warnings)


def test_finalize_run_result_uses_genai_prices_when_no_user_calculator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logged: list[dict[str, Any]] = []
    genai_calls = install_fake_genai_calc_price(monkeypatch, total_price=0.99)
    monkeypatch.setattr(agent_module, "log_usage_record", logged.append)
    runner = KitaruRunner(SimpleNamespace(name="agent"))

    result = runner._finalize_run_result(
        OpenAIRunResult(
            status="completed",
            usage={
                "model_name": "gpt-4o-mini",
                "input_tokens": 10,
                "output_tokens": 6,
                "reasoning_tokens": 2,
                "raw": {"model": "gpt-4o-mini"},
            },
        ),
        tracker=SimpleNamespace(
            run_label="run-1",
            event_log_artifact_name="events",
            run_summary_artifact_name="summary",
        ),
    )

    assert result.estimated_cost_usd == 0.99
    assert logged[0]["cost"]["estimated_cost_usd"] == 0.99
    assert logged[0]["cost"]["source"] == "calculator"
    assert logged[0]["cost"]["source_label"] == "genai-prices"
    assert logged[0]["cost"]["pricing_version"].startswith("genai-prices:")
    assert genai_calls == [
        {
            "usage": {
                "input_tokens": 10,
                "output_tokens": 6,
                "cache_read_tokens": None,
                "cache_write_tokens": None,
            },
            "model_ref": "gpt-4o-mini",
            "provider_id": "openai",
        }
    ]


def test_finalize_run_result_skips_genai_prices_for_model_name_without_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logged: list[dict[str, Any]] = []
    genai_calls = install_fake_genai_calc_price(monkeypatch, total_price=0.99)
    monkeypatch.setattr(agent_module, "log_usage_record", logged.append)
    runner = KitaruRunner(SimpleNamespace(name="agent"))

    result = runner._finalize_run_result(
        OpenAIRunResult(
            status="completed",
            usage={
                "model_name": "gpt-4o-mini",
                "input_tokens": 10,
                "output_tokens": 6,
            },
        ),
        tracker=SimpleNamespace(
            run_label="run-1",
            event_log_artifact_name="events",
            run_summary_artifact_name="summary",
        ),
    )

    assert result.estimated_cost_usd is None
    assert genai_calls == []
    assert logged[0]["cost"]["estimated_cost_usd"] is None
    assert logged[0]["cost"]["source"] == "none"


def test_finalize_run_result_skips_genai_prices_for_multi_model_usage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logged: list[dict[str, Any]] = []
    genai_calls = install_fake_genai_calc_price(monkeypatch, total_price=0.99)
    monkeypatch.setattr(agent_module, "log_usage_record", logged.append)
    runner = KitaruRunner(SimpleNamespace(name="agent"))

    result = runner._finalize_run_result(
        OpenAIRunResult(
            status="completed",
            usage={
                "model_name": "gpt-4o-mini",
                "input_tokens": 10,
                "output_tokens": 6,
                "raw": {"model_names": ["gpt-4o-mini", "gpt-4.1-mini"]},
            },
        ),
        tracker=SimpleNamespace(
            run_label="run-1",
            event_log_artifact_name="events",
            run_summary_artifact_name="summary",
        ),
    )

    assert result.estimated_cost_usd is None
    assert genai_calls == []
    assert logged[0]["cost"]["estimated_cost_usd"] is None
    assert logged[0]["cost"]["source"] == "none"


def test_finalize_run_result_logs_one_record_without_usage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logged: list[dict[str, Any]] = []
    monkeypatch.setattr(agent_module, "log_usage_record", logged.append)
    runner = KitaruRunner(
        SimpleNamespace(name="agent"),
        capture=OpenAICapturePolicy(save_usage=True),
    )

    result = runner._finalize_run_result(
        OpenAIRunResult(status="completed", usage=None),
        tracker=SimpleNamespace(
            run_label="run-1",
            event_log_artifact_name="events",
            run_summary_artifact_name="summary",
        ),
    )

    assert result.usage is None
    assert len(logged) == 1
    assert logged[0]["record_id"] == "run-1"
    assert logged[0]["usage"]["total_tokens"] is None
    assert LLM_USAGE_METADATA_KEY == "llm_usage_v1"
