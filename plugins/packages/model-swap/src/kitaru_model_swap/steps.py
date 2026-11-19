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
"""Group recorded calls by kind of answer and by the step a router sees."""

from collections import defaultdict
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from kitaru_model_swap.calls import RecordedCall
from kitaru_model_swap.compare import AGREEING, field_matches
from kitaru_model_swap.runner import CallResult
from kitaru_model_swap.stats import (
    compute_rate,
    compute_weighted_mean,
    format_usd,
)

TEXT_GROUP = "text answer"


SAME_TOOL = frozenset({"same_tool_same_args", "same_tool_different_args"})


_TEXT_REPLY = frozenset({"same_text", "different_text"})


def get_step_results(attempts: list[CallResult]) -> list[CallResult]:
    """First attempts on calls with known tool schemas, answered or not."""
    return [
        r
        for r in attempts
        if r.attempt == 0 and r.call.request.schema_source != "inferred"
    ]


def summarize_by_group(
    results: list[CallResult],
    key: Callable[[RecordedCall], str],
    weight: Callable[[RecordedCall], Decimal] | None = None,
) -> dict[str, Any]:
    """Agreement, reliability and per-argument agreement grouped by `key`.

    `calls` counts answered calls, which the rates are computed on;
    `attempted` counts every call, of which `failed` got a provider failure
    and `not_run` was never sent because a budget ran out. `failure_rate` is
    failures among the calls that were sent.

    `match_rate` asks whether the candidate took the same kind of action: the
    same tools, or a text answer where the recording answered in text. Exact
    wording almost never matches, so it would make every text step look
    unmovable, or, against a reference that does not match itself, movable to
    anything. It is not semantic agreement: any two text answers match.

    `cost_per_call_usd` weights each call by `weight`, the recorded traffic
    it stands for, so a group's cost is not skewed by balanced sampling.
    """
    groups: dict[str, dict[str, Any]] = {}
    for result in results:
        group = groups.setdefault(
            key(result.call),
            {
                "attempted": 0,
                "failed": 0,
                "not_run": 0,
                "calls": 0,
                "agreeing": 0,
                "same_tool": 0,
                "matching": 0,
                "costs": [],
                "fields": defaultdict(lambda: [0, 0]),
            },
        )
        group["attempted"] += 1
        if result.reply is None:
            group["not_run"] += 1
            continue
        if result.agreement is None:
            group["failed"] += result.reply.failure is not None
            continue
        group["calls"] += 1
        group["agreeing"] += result.agreement in AGREEING
        group["same_tool"] += result.agreement in SAME_TOOL
        group["matching"] += result.agreement in (
            SAME_TOOL if result.call.request.recorded.tool_calls else _TEXT_REPLY
        )
        if result.cost is not None:
            group["costs"].append(
                (result.cost, weight(result.call) if weight else Decimal(1))
            )
        for tool, fields in field_matches(
            result.call.request.recorded, result.reply
        ).items():
            for name, matched in fields.items():
                counts = group["fields"][f"{tool}.{name}"]
                counts[0] += matched
                counts[1] += 1
    return {
        name: {
            "attempted": group["attempted"],
            "calls": group["calls"],
            "failed": group["failed"],
            "not_run": group["not_run"],
            "failure_rate": compute_rate(
                group["failed"], group["calls"] + group["failed"]
            ),
            "agreement_rate": compute_rate(group["agreeing"], group["calls"]),
            "tool_choice_rate": (
                None
                if name == TEXT_GROUP
                else compute_rate(group["same_tool"], group["calls"])
            ),
            "match_rate": compute_rate(group["matching"], group["calls"]),
            "cost_per_call_usd": format_usd(compute_weighted_mean(group["costs"])),
            "fields": {
                field: compute_rate(matched, total)
                for field, (matched, total) in sorted(group["fields"].items())
            },
        }
        for name, group in sorted(groups.items())
    }


def get_group(call: RecordedCall) -> str:
    """Group a call by the tools it recorded, or as a text answer."""
    names = sorted({c.name for c in call.request.recorded.tool_calls})
    return " + ".join(names) if names else TEXT_GROUP


def get_route_key(sampled: list[RecordedCall]) -> Callable[[RecordedCall], str]:
    """Key calls by the step they answer, named by what a router sees first.

    The node name is added when the sample holds more than one.
    """
    several = len({call.node_name for call in sampled}) > 1
    return lambda call: (f"{call.node_name}: " if several else "") + _route(call)


def _route(call: RecordedCall) -> str:
    """Name a call by what came just before it.

    The tool the recorded model ends up calling is only known after the call,
    so it cannot route one; the request's last messages are known before it.
    """
    messages = [
        m for m in call.request.messages if m.get("role") not in {"system", "developer"}
    ]
    if not any(m.get("role") == "assistant" for m in messages):
        return "first turn"
    last = messages[-1]
    if last.get("role") == "tool":
        for message in reversed(messages):
            calls = (
                message.get("tool_calls")
                if message.get("role") == "assistant"
                else None
            )
            if calls:
                names = sorted(
                    {str(_as_dict(c.get("function")).get("name")) for c in calls}
                )
                return "after " + " + ".join(names)
    if last.get("role") == "user":
        return "after a user message"
    return "after an assistant message"


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
