#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Tests for calling OpenAI-compatible endpoints."""

from decimal import Decimal
from typing import Any

import httpx
import pytest

from kitaru_model_swap import endpoint as endpoint_module
from kitaru_model_swap.endpoint import (
    MissingCredentialError,
    Usage,
    build_body,
    get_api_key,
    send,
)
from kitaru_model_swap.normalize import ToolCall
from kitaru_model_swap.params import Candidate, ModelSwapParams, Price
from kitaru_model_swap.pricing import ResolvedPrice, call_cost, resolve_price

from .helpers import MockOpenAIServer, completion

MESSAGES = [{"role": "user", "content": "hi"}]


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    async def instant(_: float) -> None:
        return None

    monkeypatch.setattr(endpoint_module.asyncio, "sleep", instant)


def candidate(**fields: Any) -> Candidate:
    return Candidate.model_validate(
        {"name": "c", "base_url": "https://mock/v1", "model": "m"} | fields
    )


async def call(server: MockOpenAIServer, item: Candidate, key: str | None = None):
    async with httpx.AsyncClient(transport=server.transport()) as client:
        return await send(client, item, api_key=key, messages=MESSAGES, tools=[])


async def test_native_tool_calls_and_usage_are_read() -> None:
    server = MockOpenAIServer(
        {
            "m": lambda _: completion(
                tool_calls=[("lookup_order", {"order_id": "1"})],
                prompt_tokens=10,
                completion_tokens=5,
                reasoning_tokens=3,
            )
        }
    )

    result = await call(server, candidate(), key="sk-test")

    assert result.failure is None
    assert result.tool_calls == (ToolCall("lookup_order", {"order_id": "1"}),)
    assert result.usage == Usage(input_tokens=10, output_tokens=5, reasoning_tokens=3)
    assert server.headers[0]["authorization"] == "Bearer sk-test"


async def test_endpoint_without_key_sends_no_authorization() -> None:
    server = MockOpenAIServer({"m": lambda _: completion(text="hi")})

    await call(server, candidate())

    assert "authorization" not in server.headers[0]


async def test_hermes_tool_calls_are_parsed_from_text() -> None:
    text = 'Sure.<tool_call>{"name": "refund", "arguments": {"amount": 5}}</tool_call>'
    server = MockOpenAIServer({"m": lambda _: completion(text=text)})

    result = await call(
        server, candidate(provider_settings={"tool_call_format": "hermes"})
    )

    assert result.tool_calls == (ToolCall("refund", {"amount": 5}),)
    assert result.text == "Sure."


async def test_json_content_tool_call_is_parsed() -> None:
    server = MockOpenAIServer(
        {"m": lambda _: completion(text='{"name": "refund", "arguments": {}}')}
    )

    result = await call(
        server, candidate(provider_settings={"tool_call_format": "json_content"})
    )

    assert result.tool_calls == (ToolCall("refund", {}),)


@pytest.mark.parametrize(
    ("response", "settings", "category"),
    [
        (
            lambda _: completion(tool_calls=[("refund", "{not json")]),
            {},
            "tool_call_parse_error",
        ),
        (
            lambda _: completion(text="<tool_call>{oops"),
            {"tool_call_format": "hermes"},
            "tool_call_parse_error",
        ),
        (lambda _: completion(text="  "), {}, "empty_reply"),
        (
            lambda _: httpx.Response(200, json={"choices": [], "usage": {}}),
            {},
            "empty_reply",
        ),
        (
            lambda _: httpx.Response(
                200,
                json={
                    "choices": [
                        {
                            "message": {"role": "assistant", "refusal": "no"},
                            "finish_reason": "stop",
                        }
                    ]
                },
            ),
            {},
            "refusal",
        ),
        (lambda _: httpx.Response(429), {"max_retries": 0}, "rate_limited"),
        (lambda _: httpx.Response(400), {}, "http_error"),
    ],
)
async def test_failures_are_categorized(
    response: Any, settings: dict[str, Any], category: str
) -> None:
    server = MockOpenAIServer({"m": response})

    result = await call(server, candidate(provider_settings=settings))

    assert result.failure == category


async def test_transient_errors_are_retried() -> None:
    replies = iter([httpx.Response(503), completion(text="ok")])
    server = MockOpenAIServer({"m": lambda _: next(replies)})

    result = await call(server, candidate(provider_settings={"max_retries": 1}))

    assert result.failure is None
    assert result.attempts == 2


async def test_client_errors_are_not_retried() -> None:
    server = MockOpenAIServer({"m": lambda _: httpx.Response(400)})

    result = await call(server, candidate(provider_settings={"max_retries": 3}))

    assert result.attempts == 1
    assert len(server.requests) == 1


async def test_error_messages_are_kept_but_not_the_body() -> None:
    body = {
        "error": {"message": "Function tools with reasoning_effort are not supported."},
        "echo": "secret request text",
    }
    server = MockOpenAIServer({"m": lambda _: httpx.Response(400, json=body)})

    result = await call(server, candidate())

    assert result.failure_detail == (
        "HTTP 400: Function tools with reasoning_effort are not supported."
    )


async def test_timeouts_are_categorized() -> None:
    def slow(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(slow)) as client:
        result = await send(
            client,
            candidate(provider_settings={"max_retries": 0}),
            api_key=None,
            messages=MESSAGES,
            tools=[],
        )

    assert result.failure == "timeout"


def test_body_carries_provider_settings() -> None:
    body = build_body(
        candidate(
            provider_settings={
                "reasoning_effort": "low",
                "max_tokens": 256,
                "max_tokens_field": "max_tokens",
                "temperature": 0,
                "extra_body": {"top_k": 20},
            }
        ),
        MESSAGES,
        [{"type": "function", "function": {"name": "x", "parameters": {}}}],
    )

    assert body["reasoning_effort"] == "low"
    assert body["max_tokens"] == 256
    assert body["temperature"] == 0
    assert body["top_k"] == 20
    assert body["tools"][0]["function"]["name"] == "x"


def test_api_key_is_read_from_named_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CANDIDATE_KEY", "sk-env")

    assert get_api_key(candidate(api_key_env="CANDIDATE_KEY")) == "sk-env"


def test_missing_key_names_the_variable_not_a_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CANDIDATE_KEY", raising=False)

    with pytest.raises(MissingCredentialError, match="CANDIDATE_KEY"):
        get_api_key(candidate(api_key_env="CANDIDATE_KEY"))


def test_a_key_in_api_key_env_is_rejected() -> None:
    with pytest.raises(ValueError, match="environment variable"):
        candidate(api_key_env="sk-proj-abc123")


def test_candidate_names_must_be_unique() -> None:
    with pytest.raises(ValueError, match="unique"):
        ModelSwapParams.model_validate(
            {
                "candidates": [
                    {"name": "a", "base_url": "http://x", "model": "m"},
                    {"name": "a", "base_url": "http://y", "model": "n"},
                ]
            }
        )


def test_call_cost_splits_cached_input() -> None:
    price = Price(
        input_per_mtok=Decimal(1),
        output_per_mtok=Decimal(4),
        cached_input_per_mtok=Decimal("0.1"),
    )
    usage = Usage(
        input_tokens=1_000_000, cached_input_tokens=500_000, output_tokens=250_000
    )

    assert call_cost(ResolvedPrice(price, "params"), usage) == Decimal("1.55")


def test_cached_input_defaults_to_input_price() -> None:
    price = Price(input_per_mtok=Decimal(2), output_per_mtok=Decimal(0))

    assert call_cost(
        ResolvedPrice(price, "params"),
        Usage(input_tokens=1000, cached_input_tokens=1000),
    ) == Decimal("0.002")


def test_price_from_params_wins() -> None:
    resolved = resolve_price(
        candidate(price={"input_per_mtok": "0.1", "output_per_mtok": "0.2"})
    )

    assert resolved is not None
    assert resolved.source == "params"


def test_bundled_price_table_knows_the_recorded_model() -> None:
    resolved = resolve_price(candidate(model="gpt-5-nano", price_provider="openai"))

    assert resolved is not None
    assert resolved.price.input_per_mtok == Decimal("0.05")
    assert resolved.source.startswith("genai-prices")


def test_bundled_prices_follow_each_calls_tokens() -> None:
    resolved = resolve_price(candidate(model="gpt-5-nano", price_provider="openai"))
    assert resolved is not None

    cost = call_cost(resolved, Usage(input_tokens=1_000_000, output_tokens=1_000_000))

    assert cost == Decimal("0.45")


def test_unknown_model_has_no_price() -> None:
    assert (
        resolve_price(candidate(model="qwen3:8b", base_url="http://localhost:11434/v1"))
        is None
    )
