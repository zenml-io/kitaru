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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Token usage and cost tests for the PydanticAI adapter."""

from decimal import Decimal

from pydantic_ai.messages import ModelResponse, TextPart
from pydantic_ai.usage import RequestUsage

import kitaru_pydantic_ai.capability as capability_module
from kitaru.api_models.v1.session import TokenUsage
from kitaru_pydantic_ai import PydanticAIUsageSummary


def _get_response() -> ModelResponse:
    return ModelResponse(
        parts=[TextPart("resolved")],
        usage=RequestUsage(
            input_tokens=1000,
            output_tokens=500,
            cache_read_tokens=200,
            cache_write_tokens=300,
        ),
        model_name="claude-sonnet-4-5",
        provider_name="anthropic",
    )


def test_token_usage_records_cache_writes() -> None:
    assert capability_module._token_usage(_get_response()) == TokenUsage(
        input_tokens=1000,
        output_tokens=500,
        cached_input_tokens=200,
        cache_write_input_tokens=300,
    )


def test_bundled_pricing_charges_cache_writes() -> None:
    response = _get_response()

    cost, attributes = capability_module._calculate_cost(
        response, "claude-sonnet-4-5", response.timestamp, None, True
    )

    # 500 uncached, 200 cache-read, and 300 cache-write input tokens plus 500
    # output tokens at claude-sonnet-4-5's $3 / $0.30 / $3.75 / $15 per million.
    assert cost == Decimal("0.010185")
    assert attributes == {"cost": {"status": "estimated", "source": "genai-prices"}}


def test_cost_calculator_receives_cache_writes() -> None:
    observed: list[PydanticAIUsageSummary] = []

    def calculate_cost(usage: PydanticAIUsageSummary) -> Decimal:
        observed.append(usage)
        return Decimal("0.01")

    response = _get_response()
    capability_module._calculate_cost(
        response, "claude-sonnet-4-5", response.timestamp, calculate_cost, True
    )

    assert [usage.cache_write_input_tokens for usage in observed] == [300]
