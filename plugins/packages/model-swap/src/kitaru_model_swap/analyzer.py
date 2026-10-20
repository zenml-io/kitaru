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
"""Analyzer entrypoint for model-swap comparisons."""

from decimal import Decimal
from typing import Any
from uuid import UUID

import httpx

from kitaru.api_models.v1.insight import InsightInput
from kitaru.client import KitaruAPIClient
from kitaru_model_swap.calls import Collection, sample_calls
from kitaru_model_swap.params import ModelSwapParams
from kitaru_model_swap.pricing import ResolvedPrice, resolve_price
from kitaru_model_swap.report import build_insights, build_report
from kitaru_model_swap.runner import Runner


async def analyze_model_swap(
    session_ids: list[UUID], **params: Any
) -> list[InsightInput]:
    """Replay recorded model calls against candidate models and compare them.

    Args:
        session_ids: IDs of the imported sessions to analyze.
        **params: Fields of `ModelSwapParams`.

    Returns:
        Insight inputs ready for the analyzer task to persist.
    """
    return await run_model_swap(session_ids, ModelSwapParams.model_validate(params))


async def run_model_swap(
    session_ids: list[UUID],
    params: ModelSwapParams,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> list[InsightInput]:
    """Run the analysis with validated parameters.

    Raises:
        ValueError: A spend cap is set but a candidate has no known price.
    """
    prices = get_prices(params)
    if params.max_spend_usd is not None:
        unpriced = [name for name, price in prices.items() if price is None]
        if unpriced:
            raise ValueError(
                "max_spend_usd needs a price for every model; set `price` on: "
                + ", ".join(unpriced)
            )
    collection = Collection()
    async with KitaruAPIClient() as client:
        for session_id in session_ids:
            session = await client.sessions.get_with_nodes(session_id)
            collection.add_session(session, params)
            del session
    sampled = sample_calls(collection.calls, params.max_calls)
    results = []
    spent = Decimal(0)
    stopped_early = None
    if not params.dry_run and sampled:
        runner = Runner(params, prices, transport=transport)
        results = await runner.run(sampled)
        spent = runner.budget.spent
        stopped_early = runner.stopped_early
    report = build_report(
        params,
        collection,
        sampled,
        results,
        prices,
        spent=spent,
        stopped_early=stopped_early,
    )
    return build_insights(report)


def get_prices(params: ModelSwapParams) -> dict[str, ResolvedPrice | None]:
    """Resolve the price of every candidate and of the judge."""
    prices = {c.name: resolve_price(c) for c in params.candidates}
    if params.judge is not None:
        prices[f"judge:{params.judge.name}"] = resolve_price(params.judge)
    return prices


__all__ = ["analyze_model_swap", "run_model_swap"]
