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
from genai_prices.types import TieredPrices

from kitaru_model_swap.endpoint import Usage
from kitaru_model_swap.params import Candidate, Price

_MILLION = Decimal(1_000_000)


@dataclass(frozen=True)
class ResolvedPrice:
    """A price with the place it came from."""

    price: Price
    source: str


def resolve_price(candidate: Candidate) -> ResolvedPrice | None:
    """Return the candidate's price, or None when no source knows it."""
    if candidate.price is not None:
        return ResolvedPrice(candidate.price, "params")
    try:
        usage = PriceUsage(input_tokens=_MILLION, output_tokens=_MILLION)
        if candidate.price_provider is not None:
            result = calc_price(
                usage, candidate.model, provider_id=candidate.price_provider
            )
        else:
            result = calc_price(
                usage, candidate.model, provider_api_url=candidate.base_url
            )
    except LookupError:
        return None
    model_price = result.model_price
    cached = model_price.cache_read_mtok
    if model_price.input_mtok is None or model_price.output_mtok is None:
        return None
    return ResolvedPrice(
        Price(
            input_per_mtok=_flat(model_price.input_mtok),
            output_per_mtok=_flat(model_price.output_mtok),
            cached_input_per_mtok=None if cached is None else _flat(cached),
        ),
        f"genai-prices {version('genai-prices')} ({result.provider.name} "
        f"{result.model.name})",
    )


def _flat(value: Decimal | TieredPrices) -> Decimal:
    """Read a flat per-MTok price; tiered prices use their base tier."""
    return value.base if isinstance(value, TieredPrices) else Decimal(value)


def call_cost(price: Price, usage: Usage) -> Decimal:
    """Cost of one call; reasoning tokens are billed inside output tokens."""
    cached = min(usage.cached_input_tokens, usage.input_tokens)
    cached_price = (
        price.input_per_mtok
        if price.cached_input_per_mtok is None
        else price.cached_input_per_mtok
    )
    return (
        Decimal(usage.input_tokens - cached) * price.input_per_mtok
        + Decimal(cached) * cached_price
        + Decimal(usage.output_tokens) * price.output_per_mtok
    ) / _MILLION
