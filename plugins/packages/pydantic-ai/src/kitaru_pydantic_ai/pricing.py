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
"""Cost calculation for the PydanticAI adapter."""

from collections.abc import Callable
from datetime import datetime
from decimal import Decimal, InvalidOperation

from genai_prices import Usage, calc_price
from pydantic import BaseModel


class PydanticAIUsageSummary(BaseModel):
    """Describe one completed PydanticAI model request for cost calculation."""

    model: str
    provider: str | None
    input_tokens: int
    output_tokens: int
    cached_input_tokens: int
    cache_write_input_tokens: int = 0
    reasoning_tokens: int | None


CostCalculator = Callable[[PydanticAIUsageSummary], Decimal | float | int | None]


def normalize_cost(value: Decimal | float | int | None) -> Decimal | None:
    """Convert a calculator result into a valid non-negative decimal cost."""
    if value is None or isinstance(value, bool):
        return None
    try:
        cost = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not cost.is_finite() or cost < 0:
        return None
    return cost


def estimate_catalog_price(
    summary: PydanticAIUsageSummary, started_at: datetime
) -> Decimal:
    """Price one model request from the bundled genai-prices catalog.

    Args:
        summary: Completed request usage, whose input count includes cache
            reads and writes.
        started_at: Request start time, which selects historical prices.

    Returns:
        Catalog price in USD.
    """
    usage = Usage(
        input_tokens=summary.input_tokens,
        output_tokens=summary.output_tokens,
        cache_read_tokens=summary.cached_input_tokens,
        cache_write_tokens=summary.cache_write_input_tokens,
    )
    return calc_price(
        usage,
        summary.model,
        provider_id=summary.provider,
        genai_request_timestamp=started_at,
    ).total_price
