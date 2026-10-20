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

import json
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

import httpx

from kitaru.task.importer import gather_bounded
from kitaru_model_swap.calls import RecordedCall, estimate_usage
from kitaru_model_swap.compare import Agreement, compare
from kitaru_model_swap.endpoint import CandidateReply, Usage, get_api_key, send
from kitaru_model_swap.normalize import RecordedOutcome
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
_JUDGE_OUTPUT_TOKENS = 50


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
        judge_price: ResolvedPrice | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Read credentials and set up the budget."""
        self._params = params
        self._prices = prices
        self._judge_price = judge_price
        self._transport = transport
        self._keys = {c.name: get_api_key(c) for c in params.candidates}
        self._judge_key = get_api_key(params.judge) if params.judge else None
        self.budget = Budget(params.max_spend_usd)
        self.stopped_early: NotRunReason | None = None

    async def run(self, calls: list[RecordedCall]) -> list[CallResult]:
        """Send every call to every candidate, in call order."""
        deadline = time.monotonic() + self._params.time_budget_seconds
        async with httpx.AsyncClient(transport=self._transport) as client:
            return await gather_bounded(
                (
                    self._run_one(client, call, candidate, deadline)
                    for call in calls
                    for candidate in self._params.candidates
                ),
                self._params.concurrency,
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
        sent = await self._send_within_budget(
            client,
            candidate,
            price=self._prices.get(candidate.name),
            api_key=self._keys[candidate.name],
            estimate=estimate_usage(call, candidate.provider_settings.max_tokens),
            messages=call.request.messages,
            tools=call.request.tools,
        )
        if sent is None:
            return CallResult(call, candidate.name, not_run="max_spend")
        reply, cost = sent
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
        assert self._params.judge is not None
        prompt = (
            f"Recorded answer:\n{describe(recorded)}\n\n"
            f"Candidate answer:\n{describe(reply)}"
        )
        sent = await self._send_within_budget(
            client,
            self._params.judge,
            price=self._judge_price,
            api_key=self._judge_key,
            estimate=Usage(
                input_tokens=(len(_JUDGE_PROMPT) + len(prompt)) // 4,
                output_tokens=_JUDGE_OUTPUT_TOKENS,
            ),
            messages=[
                {"role": "system", "content": _JUDGE_PROMPT},
                {"role": "user", "content": prompt},
            ],
            tools=[],
        )
        if sent is None or sent[0].failure is not None or sent[0].text is None:
            return None
        try:
            value = json.loads(sent[0].text.strip().removeprefix("```json").strip("`"))
        except json.JSONDecodeError:
            return None
        equivalent = value.get("equivalent") if isinstance(value, dict) else None
        return equivalent if isinstance(equivalent, bool) else None

    async def _send_within_budget(
        self,
        client: httpx.AsyncClient,
        candidate: Candidate,
        *,
        price: ResolvedPrice | None,
        api_key: str | None,
        estimate: Usage,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
    ) -> tuple[CandidateReply, Decimal | None] | None:
        """Reserve the estimated cost, send, and settle the actual cost.

        Returns None without sending when the reservation would pass the cap.
        """
        reserved = call_cost(price, estimate) if price else Decimal(0)
        if not self.budget.reserve(reserved):
            self.stopped_early = "max_spend"
            return None
        reply = await send(
            client, candidate, api_key=api_key, messages=messages, tools=tools
        )
        cost = call_cost(price, reply.usage) if price else None
        self.budget.settle(reserved, cost or Decimal(0))
        return reply, cost


def describe(answer: RecordedOutcome | CandidateReply) -> str:
    """Render an answer as short readable text."""
    parts = [
        f"{call.name}({json.dumps(call.arguments, sort_keys=True, default=str)})"
        for call in answer.tool_calls
    ]
    if answer.text:
        parts.append(answer.text)
    return "\n".join(parts) or "(empty)"
