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
"""Send sampled calls to every candidate within the spend and time budgets."""

import asyncio
import json
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

import httpx

from kitaru_model_swap.calls import RecordedCall, estimate_usage
from kitaru_model_swap.compare import Agreement, compare
from kitaru_model_swap.endpoint import CandidateReply, Usage, get_api_key, send
from kitaru_model_swap.normalize import RecordedOutcome, ToolCall
from kitaru_model_swap.params import Candidate, ModelSwapParams
from kitaru_model_swap.pricing import ResolvedPrice, call_cost

NotRunReason = Literal["max_spend", "time_budget"]

_JUDGED: frozenset[Agreement] = frozenset(
    {"same_tool_different_args", "different_text"}
)
_JUDGE_PROMPT = (
    "You compare two answers an AI agent could give at the same step. Reply "
    'with only a JSON object {"equivalent": true} or {"equivalent": false}. '
    "Answers are equivalent when they take the same action with the same "
    "facts and values; wording may differ."
)


@dataclass(frozen=True)
class CallResult:
    """What one candidate did with one recorded call."""

    call: RecordedCall
    candidate: str
    reply: CandidateReply | None = None
    not_run: NotRunReason | None = None
    agreement: Agreement | None = None
    cost: Decimal | None = None
    judge_equivalent: bool | None = None


class Budget:
    """Track reserved and actual spend against an optional cap."""

    def __init__(self, cap: Decimal | None) -> None:
        """Start with nothing spent."""
        self.cap = cap
        self.spent = Decimal(0)
        self._reserved = Decimal(0)

    def reserve(self, amount: Decimal) -> bool:
        """Reserve an estimated amount, or refuse when it would pass the cap."""
        if self.cap is not None and self.spent + self._reserved + amount > self.cap:
            return False
        self._reserved += amount
        return True

    def settle(self, reserved: Decimal, actual: Decimal) -> None:
        """Replace a reservation with the actual cost."""
        self._reserved -= reserved
        self.spent += actual


class Runner:
    """Send recorded calls to candidates and classify the replies."""

    def __init__(
        self,
        params: ModelSwapParams,
        prices: dict[str, ResolvedPrice | None],
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Prepare credentials, budgets and the shared HTTP client settings."""
        self._params = params
        self._prices = prices
        self._transport = transport
        self._keys = {c.name: get_api_key(c) for c in params.candidates}
        self._judge_key = get_api_key(params.judge) if params.judge else None
        self.budget = Budget(params.max_spend_usd)
        self.stopped_early: NotRunReason | None = None

    async def run(self, calls: list[RecordedCall]) -> list[CallResult]:
        """Send every call to every candidate, in call order."""
        deadline = time.monotonic() + self._params.time_budget_seconds
        semaphore = asyncio.Semaphore(self._params.concurrency)
        async with httpx.AsyncClient(transport=self._transport) as client:

            async def one(call: RecordedCall, candidate: Candidate) -> CallResult:
                async with semaphore:
                    return await self._run_one(client, call, candidate, deadline)

            return list(
                await asyncio.gather(
                    *(
                        one(call, candidate)
                        for call in calls
                        for candidate in self._params.candidates
                    )
                )
            )

    async def _run_one(
        self,
        client: httpx.AsyncClient,
        call: RecordedCall,
        candidate: Candidate,
        deadline: float,
    ) -> CallResult:
        """Send one call to one candidate unless a budget is exhausted."""
        if time.monotonic() >= deadline:
            self.stopped_early = self.stopped_early or "time_budget"
            return CallResult(call, candidate.name, not_run="time_budget")
        price = self._prices.get(candidate.name)
        estimate = (
            call_cost(
                price.price,
                estimate_usage(call, candidate.provider_settings.max_tokens),
            )
            if price
            else Decimal(0)
        )
        if not self.budget.reserve(estimate):
            self.stopped_early = "max_spend"
            return CallResult(call, candidate.name, not_run="max_spend")
        reply = await send(
            client,
            candidate,
            api_key=self._keys[candidate.name],
            messages=call.request.messages,
            tools=call.request.tools,
        )
        cost = call_cost(price.price, reply.usage) if price else None
        self.budget.settle(estimate, cost or Decimal(0))
        if reply.failure is not None:
            return CallResult(call, candidate.name, reply=reply, cost=cost)
        agreement = compare(call.request.recorded, reply)
        judged = None
        if self._params.judge is not None and agreement in _JUDGED:
            judged = await self._judge(client, call.request.recorded, reply)
        return CallResult(
            call,
            candidate.name,
            reply=reply,
            agreement=agreement,
            cost=cost,
            judge_equivalent=judged,
        )

    async def _judge(
        self,
        client: httpx.AsyncClient,
        recorded: RecordedOutcome,
        reply: CandidateReply,
    ) -> bool | None:
        """Ask the judge whether two answers are equivalent."""
        judge = self._params.judge
        assert judge is not None
        price = self._prices.get(f"judge:{judge.name}")
        estimate = Decimal(0)
        if price is not None:
            estimate = call_cost(price.price, estimate_usage_for_judge(recorded, reply))
        if not self.budget.reserve(estimate):
            self.stopped_early = "max_spend"
            return None
        verdict = await send(
            client,
            judge,
            api_key=self._judge_key,
            messages=[
                {"role": "system", "content": _JUDGE_PROMPT},
                {
                    "role": "user",
                    "content": (
                        "Recorded answer:\n"
                        + describe(recorded.tool_calls, recorded.text)
                        + "\n\nCandidate answer:\n"
                        + describe(reply.tool_calls, reply.text)
                    ),
                },
            ],
            tools=[],
        )
        cost = call_cost(price.price, verdict.usage) if price else Decimal(0)
        self.budget.settle(estimate, cost)
        if verdict.failure is not None or verdict.text is None:
            return None
        try:
            value = json.loads(verdict.text.strip().removeprefix("```json").strip("`"))
        except json.JSONDecodeError:
            return None
        equivalent = value.get("equivalent") if isinstance(value, dict) else None
        return equivalent if isinstance(equivalent, bool) else None


def estimate_usage_for_judge(recorded: RecordedOutcome, reply: CandidateReply) -> Usage:
    """Estimate a judge call's tokens from the two answers."""
    chars = len(describe(recorded.tool_calls, recorded.text)) + len(
        describe(reply.tool_calls, reply.text)
    )
    return Usage(input_tokens=chars // 4 + 100, output_tokens=50)


def describe(tool_calls: tuple[ToolCall, ...], text: str | None) -> str:
    """Render an answer as short readable text."""
    parts = [
        f"{call.name}({json.dumps(call.arguments, sort_keys=True, default=str)})"
        for call in tool_calls
    ]
    if text:
        parts.append(text)
    return "\n".join(parts) or "(empty)"
