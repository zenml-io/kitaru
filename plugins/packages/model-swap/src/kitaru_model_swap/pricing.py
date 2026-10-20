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
"""Resolve token prices and compute call costs."""

from dataclasses import dataclass
from decimal import Decimal
from importlib.metadata import version

from genai_prices import Usage as PriceUsage
from genai_prices import calc_price
from genai_prices.types import PriceCalculation, TieredPrices

from kitaru_model_swap.endpoint import Usage
from kitaru_model_swap.params import Candidate, Price

_MILLION = Decimal(1_000_000)


@dataclass(frozen=True)
class ResolvedPrice:
    """A candidate's price, where it came from, and how to apply it.

    `price` shows the base per-MTok rates. Prices from the bundled table are
    applied through the table per call, so tiered rates follow each call's
    tokens.
    """

    price: Price
    source: str
    candidate: Candidate | None = None


def resolve_price(candidate: Candidate) -> ResolvedPrice | None:
    """Return the candidate's price, or None when no source knows it."""
    if candidate.price is not None:
        return ResolvedPrice(candidate.price, "params")
    try:
        result = _lookup(candidate, Usage(input_tokens=1, output_tokens=1))
    except LookupError:
        return None
    rates = result.model_price
    if rates.input_mtok is None or rates.output_mtok is None:
        return None
    cached = rates.cache_read_mtok
    return ResolvedPrice(
        Price(
            input_per_mtok=_base(rates.input_mtok),
            output_per_mtok=_base(rates.output_mtok),
            cached_input_per_mtok=None if cached is None else _base(cached),
        ),
        f"genai-prices {version('genai-prices')} ({result.provider.name} "
        f"{result.model.name})",
        candidate,
    )


def call_cost(price: ResolvedPrice, usage: Usage) -> Decimal:
    """Cost of one call; reasoning tokens are billed inside output tokens."""
    if price.candidate is not None:
        return _lookup(price.candidate, usage).total_price
    rates = price.price
    cached = min(usage.cached_input_tokens, usage.input_tokens)
    cached_rate = (
        rates.input_per_mtok
        if rates.cached_input_per_mtok is None
        else rates.cached_input_per_mtok
    )
    return (
        Decimal(usage.input_tokens - cached) * rates.input_per_mtok
        + Decimal(cached) * cached_rate
        + Decimal(usage.output_tokens) * rates.output_per_mtok
    ) / _MILLION


def _lookup(candidate: Candidate, usage: Usage) -> PriceCalculation:
    """Price a call with the bundled table.

    Raises:
        LookupError: The table does not know the model.
    """
    tokens = PriceUsage(
        input_tokens=usage.input_tokens,
        cache_read_tokens=usage.cached_input_tokens,
        output_tokens=usage.output_tokens,
    )
    if candidate.price_provider is not None:
        return calc_price(tokens, candidate.model, provider_id=candidate.price_provider)
    return calc_price(tokens, candidate.model, provider_api_url=candidate.base_url)


def _base(value: Decimal | TieredPrices) -> Decimal:
    """Read the base per-MTok rate of a flat or tiered price."""
    return value.base if isinstance(value, TieredPrices) else Decimal(value)
