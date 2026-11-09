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
"""Per-candidate summaries and the Best for picks."""

import math
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from kitaru_model_swap.compare import AGREEING
from kitaru_model_swap.runner import CallResult
from kitaru_model_swap.stats import (
    compute_rate,
    format_pct,
)
from kitaru_model_swap.steps import SAME_TOOL

# A candidate failing more often than this is a setup problem, not a choice.
MAX_FAILURE_RATE = 0.2


# Candidates this close to the best match count as behaving the same.
COMPARABLE_POINTS = 0.1


# A candidate must answer this share of its calls to be ranked.
_MIN_COVERAGE = 0.8


NOISY_REFERENCE = 0.5


# A candidate matching today's model less than this share as often as the
# closest candidate does is a clear failure, not worth a full replay.
_CLEAR_FAILURE_RATIO = 0.5


def get_primary_rate(candidate: dict[str, Any]) -> tuple[str, float | None]:
    """Pick the comparison that is meaningful for this candidate's calls.

    Tool arguments often hold free text that never matches exactly, so calls
    that use tools are judged on tool choice; text-only calls on answers.
    """
    if candidate["tool_choice_rate"] is not None:
        return "picks the same tool", candidate["tool_choice_rate"]
    return "gives the same answer", candidate["agreement_rate"]


def is_measured(answered: int, attempted: int) -> bool:
    """Tell whether a candidate answered enough of its calls to be ranked.

    A candidate the budget stopped early answered only the first calls, so a
    rate over them neither covers the sample nor can be compared with a
    candidate that answered all of it.
    """
    return attempted > 0 and answered >= math.ceil(_MIN_COVERAGE * attempted)


def _hits(result: CallResult, tool_choice: bool) -> bool:
    """Whether one answer counts as a match under the candidate's headline rate."""
    if tool_choice:
        return result.agreement in SAME_TOOL
    return result.agreement in AGREEING


def add_paired_rates(
    candidates: list[dict[str, Any]], by_candidate: dict[str, list[CallResult]]
) -> None:
    """Compare each candidate with the reference on the calls both answered.

    Rates over different calls are not comparable when one candidate answered
    only some of them, so equivalence is judged on the common calls.
    """
    reference = next((c for c in candidates if c["reference"]), None)
    if reference is None:
        return
    ref_answers = {
        r.call.node_id: r for r in get_headline(by_candidate[reference["name"]])
    }
    for candidate in candidates:
        if candidate["reference"]:
            continue
        tool_choice = candidate["tool_choice_rate"] is not None
        pairs = [
            (r, ref_answers[r.call.node_id])
            for r in get_headline(by_candidate[candidate["name"]])
            if r.call.node_id in ref_answers
            and (not tool_choice or r.call.request.recorded.tool_calls)
        ]
        candidate["paired"] = {
            "calls": len(pairs),
            "rate": compute_rate(
                sum(_hits(r, tool_choice) for r, _ in pairs), len(pairs)
            ),
            "reference_rate": compute_rate(
                sum(_hits(ref, tool_choice) for _, ref in pairs), len(pairs)
            ),
        }


def add_summaries(candidates: list[dict[str, Any]]) -> None:
    """Attach a plain-language, factual summary to each candidate."""
    reference = next((c for c in candidates if c["reference"]), None)
    noise = get_primary_rate(reference)[1] if reference else None
    for candidate in candidates:
        what, rate = get_primary_rate(candidate)
        if candidate["failure_rate"] is not None and candidate["failure_rate"] > (
            MAX_FAILURE_RATE
        ):
            top = next(iter(candidate["failures"]), "unknown")
            text = (
                f"Fix the provider setup first: {candidate['failure_rate']:.0%} "
                f"of calls failed, mostly {top}. Failures are not counted as "
                "disagreement."
            )
        elif rate is None:
            text = "No answered calls to compare."
        elif candidate["reference"]:
            text = (
                f"Your current model, run again: {what} as its own recording "
                f"{rate:.0%} of the time. This is how much answers vary from run "
                "to run."
            )
        else:
            text = f"{what.capitalize()} as your current model {rate:.0%} of the time."
            if noise is not None and noise < NOISY_REFERENCE:
                text += (
                    f" Your current model matches itself only {noise:.0%} of the "
                    "time, so read per-argument agreement and your verdicts "
                    "rather than this rate."
                )
        candidate["summary"] = text


def get_best_for(candidates: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Name the best candidate for each goal, by simple, stated rules.

    Quality here is closeness to the current model's recorded answers; the
    user's verdicts on the disagreements decide which answers are right. The
    current model run again (the reference) sets the bar: a candidate counts
    as comparable when its rate is within 10 points of the reference's own
    rate, both measured on the calls the two answered. Without a reference,
    the bar is the closest candidate's rate. The reference is the baseline and
    never a pick; a goal no other candidate qualifies for gets a row with no
    candidate that says why. Only candidates that answered most of their
    calls are ranked; the rest are named as needing more measurement.
    """
    usable = [
        c
        for c in candidates
        if not c["excluded"]
        and get_primary_rate(c)[1] is not None
        and (c["failure_rate"] is None or c["failure_rate"] <= MAX_FAILURE_RATE)
    ]
    reference = next((c for c in usable if c["reference"]), None)
    others = [c for c in usable if not c["reference"] and c["measured"]]
    # Candidates failing too often get "fix the provider setup" instead.
    unmeasured = [
        c
        for c in candidates
        if not c["excluded"]
        and not c["reference"]
        and not c["measured"]
        and (c["failure_rate"] is None or c["failure_rate"] <= MAX_FAILURE_RATE)
    ]
    choices = _unmeasured_row(unmeasured)
    if not others:
        return choices

    def rate(candidate: dict[str, Any]) -> float:
        return get_primary_rate(candidate)[1] or 0

    closest = max(others, key=rate)
    bar = rate(reference) if reference is not None else rate(closest)

    def banded(candidate: dict[str, Any]) -> tuple[float, float]:
        """The candidate's rate and the bar it is held to, on the same calls."""
        paired = candidate.get("paired")
        if reference is None or not paired or paired["rate"] is None:
            return rate(candidate), bar
        return paired["rate"], paired["reference_rate"]

    def is_comparable(candidate: dict[str, Any]) -> bool:
        own, against = banded(candidate)
        return own >= against - COMPARABLE_POINTS

    # The row text names the same bar is_comparable uses, so the stated rule
    # and the applied rule cannot drift apart.
    def versus(candidate: dict[str, Any]) -> str:
        own, against = banded(candidate)
        if reference is None:
            closest = format_pct(against)
            return f"{format_pct(own)} vs the closest candidate's {closest}"
        return (
            f"{format_pct(own)} vs today's model's own {format_pct(against)} "
            "on the calls both answered"
        )

    band = f"within {COMPARABLE_POINTS * 100:.0f} points of " + (
        "today's model" if reference is not None else "the closest candidate"
    )

    # When today's model does not take the same action as its own recording,
    # agreement cannot tell an equivalent candidate from a different one; and
    # when it answered too few calls, it sets no bar at all.
    unbarred = reference is not None and not reference["measured"]
    noisy = reference is not None and (unbarred or bar < NOISY_REFERENCE)
    comparable = [] if noisy else [c for c in others if is_comparable(c)]
    if unbarred:
        no_band = (
            "Your current model run again answered too few of its calls to set a "
            "bar, so step agreement cannot show which candidate is equivalent."
        )
    elif noisy:
        no_band = (
            f"Your current model matches its own recording only {format_pct(bar)} of "
            "the time, so step agreement cannot show which candidate is "
            "equivalent."
        )
    elif not comparable:
        no_band = (
            f"No candidate is {band} (closest: {closest['name']} "
            f"at {versus(closest)}), so step agreement cannot name an equivalent."
        )
    else:
        no_band = ""
    choices = [
        {
            "goal": "Closest to today",
            "candidate": closest["name"],
            "why": f"{get_primary_rate(closest)[0].capitalize()} as your current model "
            f"{format_pct(rate(closest))} of the time{_answered(closest)}"
            + (
                f"; your current model run again does {format_pct(bar)}."
                if reference is not None
                else "."
            ),
        },
        *choices,
    ]
    choices.append(_cheapest_row(comparable, reference, others, versus, band, no_band))
    choices.append(_fastest_row(comparable, reference, versus, band, no_band))
    open_models = [c for c in others if c["open_weights"]]
    if open_models:
        best_open = max(open_models, key=rate)
        choices.append(
            {
                "goal": "Best open model",
                "candidate": best_open["name"],
                "why": f"{get_primary_rate(best_open)[0].capitalize()} as your current "
                f"model {format_pct(rate(best_open))} of the time"
                f"{_answered(best_open)}, and you can run it anywhere.",
            }
        )
    else:
        screened = any(
            c["open_weights"] and not c["reference"] and not c["excluded"]
            for c in candidates
        )
        why = (
            "Open-weight candidates were screened, but none can be ranked: each "
            f"failed on more than {MAX_FAILURE_RATE:.0%} of its calls or answered "
            "too few of them."
            if screened
            else "No open-weight candidate was screened."
        )
        if reference is not None and reference["open_weights"]:
            why += " Today's model has open weights, but it is the baseline."
        choices.append({"goal": "Best open model", "candidate": "", "why": why})
    # Step agreement ranks a cheap model low when it reaches the same outcome by
    # a different route, so only clear failures are dropped and the cheapest
    # candidate is always kept.
    cutoff = rate(closest) * _CLEAR_FAILURE_RATIO
    finalists = sorted((c for c in others if rate(c) >= cutoff), key=rate, reverse=True)
    priced_others = [
        c
        for c in others
        if c["cost"]["per_call_usd"] is not None and c["cost_complete"]
    ]
    cheapest_overall = min(
        priced_others,
        key=lambda c: Decimal(c["cost"]["per_call_usd"]),
        default=None,
    )
    if cheapest_overall is not None and cheapest_overall not in finalists:
        finalists.append(cheapest_overall)
    dropped = [c["name"] for c in others if c not in finalists]
    reason = no_band or "Step agreement ranks candidates but does not show outcomes."
    left_out = (
        f" Left out as clear failures, matching today's model less than half as "
        f"often as the closest candidate: {', '.join(dropped)}."
        if dropped
        else ""
    )
    choices.append(
        {
            "goal": "Worth a full replay",
            "candidate": " + ".join(c["name"] for c in finalists),
            "why": f"{reason} Replay these end to end with your evaluator before "
            "switching: every candidate except clear failures, plus the "
            "cheapest, which step agreement often ranks low even when it keeps "
            f"outcomes.{left_out}",
        }
    )
    return choices


def _cheapest_row(
    comparable: list[dict[str, Any]],
    reference: dict[str, Any] | None,
    others: list[dict[str, Any]],
    versus: Callable[[dict[str, Any]], str],
    band: str,
    no_band: str,
) -> dict[str, str]:
    """Name the cheapest comparable candidate that costs less than today's model.

    A candidate that costs as much as today's model or more is no reason to
    switch, so the row then names no candidate and says what was cheaper.
    """
    goal = "Cheapest"
    decide = " Decide with full replays."
    if no_band:
        return {"goal": goal, "candidate": "", "why": no_band + decide}

    priced = [(price, c) for c in comparable if (price := _get_price(c)) is not None]
    if not priced:
        return {
            "goal": goal,
            "candidate": "",
            "why": f"No candidate {band} has a complete price for every call it "
            "answered, so none can be ranked on cost." + decide,
        }
    price, cheapest = min(priced, key=lambda pair: pair[0])
    today_usd = None if reference is None else reference["cost"]["per_call_usd"]
    today = None if today_usd is None else Decimal(today_usd)
    if today is None or price < today:
        against = "" if today_usd is None else f" against ${today_usd} today"
        return {
            "goal": goal,
            "candidate": cheapest["name"],
            "why": f"${cheapest['cost']['per_call_usd']} per call{against}"
            f"{_answered(cheapest)}, {band}: {versus(cheapest)}.",
        }
    cheaper = [
        (price, c)
        for c in others
        if (price := _get_price(c)) is not None and price < today
    ]
    outside = ""
    if cheaper:
        _, cheapest_outside = min(cheaper, key=lambda pair: pair[0])
        outside = (
            " Cheaper candidates fall outside that band (cheapest: "
            f"{cheapest_outside['name']} at "
            f"${cheapest_outside['cost']['per_call_usd']} per call, "
            f"{versus(cheapest_outside)})."
        )
    return {
        "goal": goal,
        "candidate": "",
        "why": f"No candidate {band} costs less than it "
        f"(${today_usd} per call).{outside}" + decide,
    }


def _get_price(candidate: dict[str, Any]) -> Decimal | None:
    """Return a candidate's cost per call when every answered call was priced."""
    if candidate["cost"]["per_call_usd"] is None or not candidate["cost_complete"]:
        return None
    return Decimal(candidate["cost"]["per_call_usd"])


def _fastest_row(
    comparable: list[dict[str, Any]],
    reference: dict[str, Any] | None,
    versus: Callable[[dict[str, Any]], str],
    band: str,
    no_band: str,
) -> dict[str, str]:
    """Name the fastest comparable candidate that is faster than today's model."""
    goal = "Fastest"
    decide = " Decide with full replays."
    if no_band:
        return {"goal": goal, "candidate": "", "why": no_band + decide}
    timed = [c for c in comparable if c["latency_seconds"]["p50"] is not None]
    if not timed:
        return {
            "goal": goal,
            "candidate": "",
            "why": f"No candidate {band} has a measured latency." + decide,
        }
    fastest = min(timed, key=lambda c: c["latency_seconds"]["p50"])
    today = None if reference is None else reference["latency_seconds"]["p50"]
    p50 = fastest["latency_seconds"]["p50"]
    if today is not None and p50 >= today:
        return {
            "goal": goal,
            "candidate": "",
            "why": f"No candidate {band} is faster than today's model "
            f"({today:.2f}s median latency; the fastest of them, "
            f"{fastest['name']}, takes {p50:.2f}s)." + decide,
        }
    against = "" if today is None else f" against {today:.2f}s today"
    return {
        "goal": goal,
        "candidate": fastest["name"],
        "why": f"{p50:.2f}s median latency{against}{_answered(fastest)}, "
        f"{band}: {versus(fastest)}.",
    }


def _answered(candidate: dict[str, Any]) -> str:
    """Say how many of its calls a candidate answered."""
    coverage = candidate["coverage"]
    return f" (answered {coverage['answered']} of {coverage['attempted']} calls)"


def _unmeasured_row(unmeasured: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Name candidates that answered too few calls to be ranked or ruled out."""
    if not unmeasured:
        return []
    return [
        {
            "goal": "Needs more measurement",
            "candidate": " + ".join(c["name"] for c in unmeasured),
            "why": "Answered too few of their calls to be ranked or ruled out: "
            + ", ".join(
                f"{c['name']} {c['coverage']['answered']} of "
                f"{c['coverage']['attempted']}"
                for c in unmeasured
            )
            + ". Raise the spend cap or time budget and run again before "
            "keeping or dropping them.",
        }
    ]


def get_headline(attempts: list[CallResult]) -> list[CallResult]:
    """First attempts that were answered, on calls with known tool schemas."""
    return [
        r
        for r in attempts
        if r.attempt == 0
        and r.agreement is not None
        and r.call.request.schema_source != "inferred"
    ]
