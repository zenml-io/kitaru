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
"""The best mix: which steps can move to a cheaper candidate."""

import hashlib
from collections import Counter, defaultdict
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from kitaru_model_swap.calls import RecordedCall
from kitaru_model_swap.picks import (
    COMPARABLE_POINTS,
    MAX_FAILURE_RATE,
    NOISY_REFERENCE,
)
from kitaru_model_swap.runner import CallResult
from kitaru_model_swap.stats import (
    compute_weighted_mean,
    format_pct,
    format_usd,
)
from kitaru_model_swap.steps import (
    get_route_key,
    get_step_results,
    summarize_by_group,
)
from kitaru_model_swap.volume import (
    Volume,
    project_cost,
)


def is_learning_session(call: RecordedCall) -> bool:
    """Put about half of the sessions in the learning set, by session id."""
    return int(hashlib.sha256(str(call.session_id).encode()).hexdigest(), 16) % 2 == 0


def split_routes(
    by_candidate: dict[str, list[CallResult]],
    sampled: list[RecordedCall],
    weight: Callable[[RecordedCall], Decimal],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Per-step results of each candidate on the learning and held-out sessions.

    Sessions, not calls, are split, so calls from one conversation never land
    on both sides.
    """
    key = get_route_key(sampled)
    learning: dict[str, dict[str, Any]] = {}
    held_out: dict[str, dict[str, Any]] = {}
    for name, results in by_candidate.items():
        halves: tuple[list[CallResult], list[CallResult]] = ([], [])
        for r in get_step_results(results):
            halves[0 if is_learning_session(r.call) else 1].append(r)
        learning[name] = summarize_by_group(halves[0], key, weight)
        held_out[name] = summarize_by_group(halves[1], key, weight)
    return learning, held_out


def _is_unreliable(values: dict[str, Any] | None) -> bool:
    """Tell whether a candidate failed too often on one step.

    Agreement among the replies that came back says nothing about how often
    the provider failed to reply, so a step is judged on its own failures,
    not on the candidate's overall rate, which other steps can dilute.
    """
    rate = None if values is None else values["failure_rate"]
    return rate is not None and rate > MAX_FAILURE_RATE


def get_best_mix(
    candidates: list[dict[str, Any]],
    sampled: list[RecordedCall],
    volume: Volume,
    min_calls: int,
    split: tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]],
    weight: Callable[[RecordedCall], Decimal],
) -> dict[str, Any] | None:
    """Pick, per step, the cheapest candidate that behaves well enough.

    Steps are named by what comes just before a call (`_route`), which a
    router can see. Picking the best of many candidates on many steps finds
    some by luck, so each step is chosen on the learning half of the sessions
    and checked on the held-out half. A step moves to a candidate only when
    the candidate answered at least `min_calls` of the step's calls, failed on
    no more than a fifth of the step's calls it was sent, and its rate on the
    learning half is within 10 points of the reference's rate there (or of the
    best rate when there is no reference). A pick whose held-out rate, on
    enough held-out calls, falls more than 10 points below the reference's
    held-out rate (or the learning bar), or that fails too often on the
    held-out calls, is dropped (`held_out: failed`); with too few held-out
    calls it stays but is marked `too_few`. Otherwise the step stays on
    today's model, as does a step where the reference itself takes the
    recorded action less than half the time. "Rate" is whether the candidate
    takes the same kind of action (the same tools, or a text answer), not
    exact wording or meaning. Today's cost is the reference's replay cost
    where the reference answered the step, since recorded calls often hit a
    warm prompt cache that replayed single calls do not, and the recorded
    cost elsewhere; each step says which (`today_source`). Shares and costs
    weight each sampled call by the recorded traffic it stands for, so a
    mix that picks one candidate everywhere costs what that candidate costs.
    Excluded, failing and unpriced candidates are never picked.
    """
    key = get_route_key(sampled)
    population: dict[str, Decimal] = defaultdict(Decimal)
    counts = Counter(key(call) for call in sampled)
    recorded_costs: dict[str, list[tuple[Decimal, Decimal]]] = defaultdict(list)
    for call in sampled:
        population[key(call)] += weight(call)
        if call.cost is not None:
            recorded_costs[key(call)].append((call.cost, weight(call)))
    total_population = sum(population.values(), Decimal(0))
    # The reference is today's model run again, so it sets the bar but is not
    # somewhere to move calls to.
    usable = [
        c
        for c in candidates
        if not c["excluded"]
        and not c["reference"]
        and (c["failure_rate"] is None or c["failure_rate"] <= MAX_FAILURE_RATE)
    ]
    reference = next((c for c in candidates if c["reference"]), None)
    groups: list[dict[str, Any]] = []
    # (share, cost per call with the mix, cost per call today) per step
    prices: list[tuple[Decimal, Decimal | None, Decimal | None]] = []
    learning, held_out = split
    sessions = {
        "learning": len({c.session_id for c in sampled if is_learning_session(c)}),
        "held_out": len({c.session_id for c in sampled if not is_learning_session(c)}),
    }
    for group, count in sorted(counts.items()):
        ref_all = reference["by_route"].get(group) if reference else None
        # A step seen in only one half of the sessions cannot be split; it is
        # judged on all its calls and its pick marked as not checked.
        ref_values = (
            learning.get(reference["name"], {}).get(group) or ref_all
            if reference
            else None
        )
        ref_held = held_out.get(reference["name"], {}).get(group) if reference else None
        if ref_all and ref_all["cost_per_call_usd"] is not None:
            today: Decimal | None = Decimal(ref_all["cost_per_call_usd"])
            today_source: str | None = "reference replay"
        else:
            today = compute_weighted_mean(recorded_costs[group])
            today_source = None if today is None else "recording"
        rates: dict[str, float] = {}
        costs: dict[str, Decimal] = {}
        unsplit: set[str] = set()
        unreliable: list[str] = []
        too_few = False
        for c in usable:
            values = learning.get(c["name"], {}).get(group)
            overall = c["by_route"].get(group)
            if _is_unreliable(overall):
                unreliable.append(c["name"])
                continue
            if not overall or overall["cost_per_call_usd"] is None:
                continue
            if overall["calls"] < min_calls:
                too_few = True
                continue
            if not values or not values["calls"]:
                unsplit.add(c["name"])
                values = overall
            rates[c["name"]] = values["match_rate"]
            costs[c["name"]] = Decimal(overall["cost_per_call_usd"])
        bar = ref_values["match_rate"] if ref_values else None
        if bar is None and rates:
            bar = max(rates.values())
        # When today's model does not take the same action as its own
        # recording, agreement cannot tell candidates apart on this step.
        noisy = bar is not None and bar < NOISY_REFERENCE
        if noisy:
            bar = None
        fits = (
            [name for name, rate in rates.items() if rate >= bar - COMPARABLE_POINTS]
            if bar is not None
            else []
        )
        choice = min(fits, key=lambda name: costs[name], default=None)
        # Keep today's model when it is cheaper than every candidate that fits.
        if choice is not None and today is not None and costs[choice] > today:
            choice = None
        held_rate, held_calls, held_status = None, 0, None
        if choice is not None and bar is not None:
            held = held_out.get(choice, {}).get(group)
            held_bar = ref_held["match_rate"] if ref_held else bar
            held_calls = held["calls"] if held else 0
            held_rate = held["match_rate"] if held else None
            if choice in unsplit:
                held_status = "no_split"
            elif _is_unreliable(held):
                choice, held_status = None, "failed"
            elif held_rate is None or held_calls < max(1, min_calls // 4):
                held_status = "too_few"
            elif held_rate < held_bar - COMPARABLE_POINTS:
                choice, held_status = None, "failed"
            else:
                held_status = "held"
        cost = costs[choice] if choice else today
        share = population[group] / total_population if total_population else None
        groups.append(
            {
                "group": group,
                "share": None if share is None else round(float(share), 4),
                "calls": count,
                "candidate": choice,
                "rate": None if choice is None else rates[choice],
                "cost_per_call_usd": format_usd(cost),
                "too_few_calls": choice is None and too_few and not rates,
                "noisy_reference": noisy,
                "unreliable": sorted(unreliable),
                "held_out": held_status,
                "held_out_rate": held_rate,
                "held_out_calls": held_calls,
                "today_source": today_source,
            }
        )
        prices.append((share or Decimal(0), cost, today))
    # A pick that failed on held-out sessions is a result worth showing.
    if not any(item["candidate"] or item["held_out"] == "failed" for item in groups):
        return None
    summary = {
        "groups": groups,
        "min_calls": min_calls,
        "today_source": _describe_sources(groups),
        "sessions": sessions,
        "cost": None,
        "recorded_cost": None,
    }
    if any(cost is None or today is None for _, cost, today in prices):
        return summary
    mixed = sum((share * (cost or 0) for share, cost, _ in prices), Decimal(0))
    recorded = sum((share * (today or 0) for share, _, today in prices), Decimal(0))
    return summary | {
        "cost": project_cost(mixed, volume),
        "recorded_cost": project_cost(recorded, volume),
    }


def _describe_sources(groups: list[dict[str, Any]]) -> str:
    """Say where today's cost comes from, naming a mix of sources."""
    sources = Counter(item["today_source"] for item in groups if item["today_source"])
    if len(sources) <= 1:
        return next(iter(sources), "recording")
    replayed = sources["reference replay"]
    return (
        f"reference replay on {replayed} of {sources.total()} steps and the "
        "recording on the rest, which is not like for like"
    )


def get_mix_rows(mix: dict[str, Any] | None) -> list[dict[str, str]]:
    """Summarize the best mix as one "best for" row."""
    if mix is None:
        return []
    moved = [item for item in mix["groups"] if item["candidate"]]
    if not moved:
        return []
    targets = sorted({item["candidate"] for item in moved})
    why = (
        f"Moves {len(moved)} of {len(mix['groups'])} kinds of call "
        f"({format_pct(sum(item['share'] for item in moved))} of calls) to "
        f"{', '.join(targets)} and keeps the rest on today's model"
    )
    if mix["cost"] is not None and mix["recorded_cost"] is not None:
        why += (
            f": ${mix['cost']['per_call_usd']} per call against "
            f"${mix['recorded_cost']['per_call_usd']} today"
        )
    return [{"goal": "Best mix", "candidate": " + ".join(targets), "why": why + "."}]
