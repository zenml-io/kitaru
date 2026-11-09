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
"""Token usage and cost tests for the OpenAI Agents adapter."""

from datetime import UTC, datetime
from decimal import Decimal

from agents import ModelResponse, Usage
from openai.types.responses.response_usage import (
    InputTokensDetails,
    OutputTokensDetails,
)

import kitaru_openai_agents.recording as recording_module
from kitaru.api_models.v1.session import TokenUsage
from kitaru_openai_agents.pricing import estimate_cost


def test_capture_usage_records_cache_writes() -> None:
    response = ModelResponse(
        output=[],
        usage=Usage(
            requests=1,
            input_tokens=1000,
            output_tokens=500,
            total_tokens=1500,
            input_tokens_details=InputTokensDetails(
                cached_tokens=200, cache_write_tokens=300
            ),
            output_tokens_details=OutputTokensDetails(reasoning_tokens=0),
        ),
        response_id="response-1",
    )

    assert recording_module._capture_usage(response) == TokenUsage(
        input_tokens=1000,
        output_tokens=500,
        cached_input_tokens=200,
        cache_write_input_tokens=300,
        reasoning_tokens=0,
    )


def test_estimate_cost_prices_cache_writes() -> None:
    tokens = TokenUsage(
        input_tokens=1000,
        output_tokens=500,
        cached_input_tokens=200,
        cache_write_input_tokens=300,
    )

    cost, attributes = estimate_cost(
        tokens, "claude-sonnet-4-5", "anthropic", datetime(2026, 10, 1, tzinfo=UTC)
    )

    # 500 uncached, 200 cache-read, and 300 cache-write input tokens plus 500
    # output tokens at claude-sonnet-4-5's $3 / $0.30 / $3.75 / $15 per million.
    assert cost == Decimal("0.010185")
    assert attributes == {"cost": {"status": "estimated", "source": "genai-prices"}}
