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

import asyncio
import time
from contextlib import aclosing
from decimal import Decimal
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

import httpx

from kitaru.api_models.v1.insight import InsightInput
from kitaru.client import KitaruAPIClient
from kitaru.client.dashboard_urls import get_dashboard_base_url
from kitaru.client.exceptions import KitaruClientError
from kitaru.task.importer import stream_bounded
from kitaru_model_swap.calls import (
    Collection,
    RecordedCall,
    reservation_usage,
    sample_calls,
)
from kitaru_model_swap.decision_plan import plan_decisions
from kitaru_model_swap.params import Candidate, ModelSwapParams
from kitaru_model_swap.pricing import ResolvedPrice, call_cost, resolve_price
from kitaru_model_swap.report import build_insights, build_report
from kitaru_model_swap.runner import Runner

# Sessions are fetched a few at a time so a large import neither waits on
# one request after another nor holds every session in memory at once.
_FETCH_CONCURRENCY = 8


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
    # The time budget covers fetching sessions too, so the analysis returns
    # before the task timeout however large the import is.
    deadline = time.monotonic() + params.time_budget_seconds
    prices = {c.name: resolve_price(c) for c in params.candidates} | {
        c.name: ResolvedPrice(c.price, "params") if c.price else None
        for c in params.decision_candidates
    }
    judge_price = _get_judge_price(params)
    if params.max_spend_usd is not None:
        unpriced = [name for name, price in prices.items() if price is None]
        if params.judge is not None and judge_price is None:
            unpriced.append("the judge")
        if unpriced:
            raise ValueError(
                "max_spend_usd needs a price for every model; set `price` on: "
                + ", ".join(unpriced)
            )
    collection = Collection(max_per_group=params.max_calls)
    async with KitaruAPIClient() as client:
        try:
            # Closing the stream on exit cancels fetches already started, and
            # the timeout covers a fetch that hangs.
            async with (
                asyncio.timeout(max(deadline - time.monotonic(), 0)),
                aclosing(
                    stream_bounded(
                        (client.sessions.get_with_nodes(i) for i in session_ids),
                        _FETCH_CONCURRENCY,
                    )
                ) as sessions,
            ):
                async for session in sessions:
                    collection.add_session(session, params)
        except TimeoutError:
            collection.sessions_not_fetched = len(session_ids) - collection.sessions
        dashboard_url = await _get_dashboard_url(client)
    sampled = sample_calls(collection.calls, params.max_calls)
    _check_cap_fits(params, prices, sampled)
    plans = [(call, plan_decisions(call.request)) for call in sampled]
    decision_results = []
    results = []
    spent = Decimal(0)
    stopped_early = None
    if not params.dry_run and sampled:
        runner = Runner(params, prices, judge_price=judge_price, transport=transport)
        # Decision requests are small and fast, so they go first and are not
        # starved by slower candidate calls sharing the same budgets.
        if params.decision_candidates:
            decision_results = await runner.run_decisions(
                [(call, plan) for call, plan in plans if plan.questions],
                deadline=deadline,
            )
        results = await runner.run(sampled, deadline=deadline)
        spent = runner.budget.spent
        stopped_early = runner.stopped_early
    if collection.sessions_not_fetched:
        stopped_early = stopped_early or "time_budget"
    report = build_report(
        params,
        collection,
        sampled,
        results,
        prices,
        spent=spent,
        stopped_early=stopped_early,
        plans=plans,
        decision_results=decision_results,
    )
    return build_insights(report, dashboard_url=dashboard_url)


def _get_judge_price(params: ModelSwapParams) -> ResolvedPrice | None:
    """Price the judge; decision services are not in the bundled table."""
    judge = params.judge
    if isinstance(judge, Candidate):
        return resolve_price(judge)
    if judge is not None and judge.price is not None:
        return ResolvedPrice(judge.price, "params")
    return None


def _check_cap_fits(
    params: ModelSwapParams,
    prices: dict[str, ResolvedPrice | None],
    sampled: list[RecordedCall],
) -> None:
    """Refuse a run whose spend cap cannot fit a single call.

    Raises:
        ValueError: One call's full reservation exceeds `max_spend_usd`.
    """
    cap = params.max_spend_usd
    if cap is None or not sampled:
        return
    for candidate in params.candidates:
        price = prices.get(candidate.name)
        max_tokens = candidate.provider_settings.max_tokens
        if price is None or max_tokens is None:
            continue
        largest = max(
            call_cost(price, reservation_usage(call, max_tokens, strict=True))
            for call in sampled
        )
        if largest > cap:
            raise ValueError(
                f"one call to {candidate.name} can cost up to ${largest:.6f} at "
                f"max_tokens={max_tokens}, more than max_spend_usd; lower "
                "max_tokens or raise the cap"
            )


async def _get_dashboard_url(client: KitaruAPIClient) -> str | None:
    """Resolve the dashboard base URL for session links, when there is one."""
    try:
        info = await client.info.get()
    # Links only decorate the report, so a failing or malformed info
    # response must not fail an analysis whose replays already ran.
    except (KitaruClientError, httpx.HTTPError, ValueError):
        return None
    url = get_dashboard_base_url(info, client.base_url)
    return url if url and urlsplit(url).scheme in {"http", "https"} else None


__all__ = ["analyze_model_swap", "run_model_swap"]
