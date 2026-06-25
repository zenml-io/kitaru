"""Usage and cost tracking tests for the fast-agent calls strategy."""

from __future__ import annotations

import importlib
import sys
import types
from dataclasses import dataclass
from typing import Any, ClassVar

import pytest


def _purge_fast_agent_adapter_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.fast_agent"):
            monkeypatch.delitem(sys.modules, cached, raising=False)


@pytest.fixture
def fast_agent_adapter(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    _purge_fast_agent_adapter_modules(monkeypatch)
    monkeypatch.setitem(sys.modules, "fast_agent", types.ModuleType("fast_agent"))
    return importlib.import_module("kitaru.adapters.fast_agent")


@dataclass
class FakeTurnUsage:
    provider: str = "memory"
    model: str = "memory-model"
    input_tokens: int = 11
    output_tokens: int = 7
    total_tokens: int = 18
    reasoning_tokens: int | None = None
    raw_usage: dict[str, Any] | None = None

    @property
    def display_input_tokens(self) -> int:
        return self.input_tokens


@dataclass
class FakeUsageAccumulator:
    turns: list[FakeTurnUsage]


class OpenAIStyleResult:
    usage: ClassVar[dict[str, int]] = {
        "prompt_tokens": 3,
        "completion_tokens": 4,
        "total_tokens": 7,
    }


class GeminiStyleResult:
    usage_metadata: ClassVar[dict[str, int]] = {
        "promptTokenCount": 5,
        "candidatesTokenCount": 6,
    }


class BadUsageResult:
    @property
    def usage(self) -> dict[str, int]:
        raise RuntimeError("usage exploded")


def _model_call(fast_agent_adapter: types.ModuleType, **overrides: Any) -> Any:
    values = {
        "agent_name": "researcher",
        "kind": "model",
        "operation": "generate",
        "args": ("hello",),
        "kwargs": {},
        "model_name": "fallback-model",
        "provider": "fallback-provider",
        **overrides,
    }
    return fast_agent_adapter.FastAgentCall(**values)


def _tool_call(fast_agent_adapter: types.ModuleType, **overrides: Any) -> Any:
    return fast_agent_adapter.FastAgentCall(
        agent_name="researcher",
        kind="tool",
        operation="call_tool",
        args=("lookup", {"topic": "kitaru"}),
        kwargs={},
        tool_name="lookup",
        **overrides,
    )


def _run_with_inline_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    fast_agent_adapter: types.ModuleType,
    recorder: Any,
    call: Any,
    proceed: Any,
) -> tuple[Any, list[dict[str, Any]]]:
    from kitaru.adapters.fast_agent import _usage, _wrapping

    records: list[dict[str, Any]] = []

    def run_sync_in_checkpoint(**kwargs: Any) -> Any:
        return kwargs["body"]()

    monkeypatch.setattr(_wrapping, "is_inside_flow", lambda: True)
    monkeypatch.setattr(_wrapping, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(_wrapping, "run_sync_in_checkpoint", run_sync_in_checkpoint)
    monkeypatch.setattr(
        _usage,
        "log_usage_record",
        lambda record: records.append(record),
    )

    result = recorder(call, proceed)
    return result, records


def test_model_usage_tries_later_accumulator_when_first_is_unchanged(
    fast_agent_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_accumulator = FakeUsageAccumulator(turns=[])
    llm_accumulator = FakeUsageAccumulator(turns=[])
    recorder = fast_agent_adapter.KitaruFastAgentCallRecorder()
    call = _model_call(
        fast_agent_adapter,
        usage_sources=(
            types.SimpleNamespace(usage_accumulator=agent_accumulator),
            llm_accumulator,
        ),
    )

    def proceed() -> str:
        llm_accumulator.turns.append(
            FakeTurnUsage(
                provider="responses",
                model="gpt-5-nano",
                input_tokens=4,
                output_tokens=2,
                total_tokens=6,
            )
        )
        return "ok"

    result, records = _run_with_inline_checkpoint(
        monkeypatch,
        fast_agent_adapter,
        recorder,
        call,
        proceed,
    )

    assert result == "ok"
    assert len(records) == 1
    assert records[0]["provider"] == "responses"
    assert records[0]["model"] == "gpt-5-nano"
    assert records[0]["usage"]["total_tokens"] == 6


def test_model_usage_prefers_accumulator_last_turn(
    fast_agent_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    accumulator = FakeUsageAccumulator(turns=[])
    source = types.SimpleNamespace(usage_accumulator=accumulator)
    recorder = fast_agent_adapter.KitaruFastAgentCallRecorder()
    call = _model_call(fast_agent_adapter, usage_sources=(source,))

    def proceed() -> dict[str, Any]:
        accumulator.turns.append(
            FakeTurnUsage(
                provider="openai",
                model="gpt-5-nano",
                input_tokens=11,
                output_tokens=7,
                total_tokens=18,
                reasoning_tokens=2,
                raw_usage={"provider_payload": "kept"},
            )
        )
        return {"usage": {"prompt_tokens": 999, "completion_tokens": 999}}

    result, records = _run_with_inline_checkpoint(
        monkeypatch,
        fast_agent_adapter,
        recorder,
        call,
        proceed,
    )

    assert result == {"usage": {"prompt_tokens": 999, "completion_tokens": 999}}
    assert len(records) == 1
    record = records[0]
    assert record["adapter"] == "fast_agent"
    assert record["surface"] == "model_call"
    assert record["call_name"] == "researcher_generate_model_call"
    assert record["provider"] == "openai"
    assert record["model"] == "gpt-5-nano"
    assert record["usage"]["input_tokens"] == 11
    assert record["usage"]["output_tokens"] == 7
    assert record["usage"]["total_tokens"] == 18
    assert record["usage"]["reasoning_tokens"] == 2
    assert record["usage"]["raw"]["provider_payload"] == "kept"


def test_openai_style_fallback_usage_normalizes_token_names(
    fast_agent_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = fast_agent_adapter.KitaruFastAgentCallRecorder()
    call = _model_call(fast_agent_adapter, provider="openai", model_name="gpt-5-nano")

    result, records = _run_with_inline_checkpoint(
        monkeypatch,
        fast_agent_adapter,
        recorder,
        call,
        lambda: OpenAIStyleResult(),
    )

    assert isinstance(result, OpenAIStyleResult)
    assert len(records) == 1
    usage = records[0]["usage"]
    assert usage["input_tokens"] == 3
    assert usage["output_tokens"] == 4
    assert usage["total_tokens"] == 7


def test_gemini_style_fallback_usage_normalizes_camel_case_names(
    fast_agent_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = fast_agent_adapter.KitaruFastAgentCallRecorder()
    call = _model_call(
        fast_agent_adapter,
        provider="google_gemini",
        model_name="gemini-2.5-flash",
    )

    _, records = _run_with_inline_checkpoint(
        monkeypatch,
        fast_agent_adapter,
        recorder,
        call,
        lambda: GeminiStyleResult(),
    )

    assert len(records) == 1
    usage = records[0]["usage"]
    assert usage["input_tokens"] == 5
    assert usage["output_tokens"] == 6
    assert usage["total_tokens"] == 11


def test_custom_cost_calculator_receives_fast_agent_summary_and_wins(
    fast_agent_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    accumulator = FakeUsageAccumulator(turns=[])
    seen: list[Any] = []

    def calculate_cost(usage: Any) -> float:
        seen.append(usage)
        return 0.123

    def proceed() -> str:
        accumulator.turns.append(FakeTurnUsage(provider="openai", model="gpt-5-nano"))
        return "ok"

    recorder = fast_agent_adapter.KitaruFastAgentCallRecorder(
        cost_calculator=calculate_cost
    )
    call = _model_call(
        fast_agent_adapter,
        usage_sources=(types.SimpleNamespace(usage_accumulator=accumulator),),
    )

    _, records = _run_with_inline_checkpoint(
        monkeypatch,
        fast_agent_adapter,
        recorder,
        call,
        proceed,
    )

    assert len(seen) == 1
    assert isinstance(seen[0], fast_agent_adapter.FastAgentUsageSummary)
    assert seen[0].input_tokens == 11
    assert len(records) == 1
    assert records[0]["cost"]["estimated_cost_usd"] == 0.123
    assert records[0]["cost"]["source"] == "calculator"
    assert records[0]["cost"]["source_label"] == "fast_agent.cost_calculator"


def test_save_usage_false_keeps_checkpoint_behavior_but_logs_no_usage(
    fast_agent_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    accumulator = FakeUsageAccumulator(turns=[FakeTurnUsage()])
    recorder = fast_agent_adapter.KitaruFastAgentCallRecorder(save_usage=False)
    call = _model_call(
        fast_agent_adapter,
        usage_sources=(types.SimpleNamespace(usage_accumulator=accumulator),),
    )

    result, records = _run_with_inline_checkpoint(
        monkeypatch,
        fast_agent_adapter,
        recorder,
        call,
        lambda: "ok",
    )

    assert result == "ok"
    assert records == []


def test_tool_calls_do_not_log_llm_usage(
    fast_agent_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = fast_agent_adapter.KitaruFastAgentCallRecorder()
    call = _tool_call(fast_agent_adapter)

    result, records = _run_with_inline_checkpoint(
        monkeypatch,
        fast_agent_adapter,
        recorder,
        call,
        lambda: {"usage": {"prompt_tokens": 10, "completion_tokens": 5}},
    )

    assert result == {"usage": {"prompt_tokens": 10, "completion_tokens": 5}}
    assert records == []


def test_missing_or_malformed_usage_returns_result_without_empty_record(
    fast_agent_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = fast_agent_adapter.KitaruFastAgentCallRecorder()
    call = _model_call(fast_agent_adapter)

    result, records = _run_with_inline_checkpoint(
        monkeypatch,
        fast_agent_adapter,
        recorder,
        call,
        lambda: {"usage": {"not_tokens": "ignored"}},
    )

    assert result == {"usage": {"not_tokens": "ignored"}}
    assert records == []


@pytest.mark.anyio
async def test_async_model_call_logs_usage_inside_checkpoint_body(
    fast_agent_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kitaru.adapters.fast_agent import _usage, _wrapping

    accumulator = FakeUsageAccumulator(turns=[])
    records: list[dict[str, Any]] = []

    async def run_async_in_checkpoint(**kwargs: Any) -> Any:
        return await kwargs["body"]()

    async def proceed() -> str:
        accumulator.turns.append(
            FakeTurnUsage(
                provider="responses",
                model="gpt-5-nano",
                input_tokens=8,
                output_tokens=3,
                total_tokens=11,
            )
        )
        return "async ok"

    monkeypatch.setattr(_wrapping, "is_inside_flow", lambda: True)
    monkeypatch.setattr(_wrapping, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(_wrapping, "run_async_in_checkpoint", run_async_in_checkpoint)
    monkeypatch.setattr(
        _usage,
        "log_usage_record",
        lambda record: records.append(record),
    )

    recorder = fast_agent_adapter.KitaruFastAgentCallRecorder()
    call = _model_call(
        fast_agent_adapter,
        is_async=True,
        usage_sources=(types.SimpleNamespace(usage_accumulator=accumulator),),
    )

    result = await recorder(call, proceed)

    assert result == "async ok"
    assert len(records) == 1
    assert records[0]["provider"] == "responses"
    assert records[0]["model"] == "gpt-5-nano"
    assert records[0]["usage"]["total_tokens"] == 11


def test_usage_extraction_failure_returns_result_without_empty_record(
    fast_agent_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorder = fast_agent_adapter.KitaruFastAgentCallRecorder()
    call = _model_call(fast_agent_adapter)

    result, records = _run_with_inline_checkpoint(
        monkeypatch,
        fast_agent_adapter,
        recorder,
        call,
        BadUsageResult,
    )

    assert isinstance(result, BadUsageResult)
    assert records == []
