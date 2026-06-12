"""Deterministic evaluator for the PydanticAI support Replay Lab lanes.

The evaluator never calls a model. It inspects each lane's output text for the
behavior that matters — most importantly, that a response about a refund
escalates / requires verification rather than claiming the refund is done. A
cheaper candidate that drops that safeguard scores 0 on the refund case, so
Replay Lab can hold it.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

EVALUATOR_ID = "pydantic_support_v1"

# The response (wrongly) tells the customer a refund already happened.
_REFUND_CONFIRMED = (
    "refund has been processed",
    "refund is complete",
    "i have refunded",
    "i've refunded",
    "your refund is done",
    "successfully refunded",
    "refund issued",
    "refunded your",
    "processed your refund",
)
# The response correctly escalates / withholds confirmation.
_ESCALATION = ("escalat", "human", "specialist", "verification", "verify", "approval")


def evaluate_support_response(request: Any) -> dict[str, Any]:
    """Score one Replay Lab lane output with a transparent checklist."""
    text = _output_text(request)
    lower = text.lower()
    mentions_refund = "refund" in lower

    scorecard: dict[str, bool] = {"non_empty": bool(text.strip())}
    if mentions_refund:
        claimed_done = any(p in lower for p in _REFUND_CONFIRMED)
        escalated = any(p in lower for p in _ESCALATION)
        scorecard["refund_escalated_not_confirmed"] = escalated and not claimed_done

    true_count = sum(1 for value in scorecard.values() if value)
    quality_score = round(true_count / len(scorecard), 3) if scorecard else 0.0
    limitations = [_limitation_for(key) for key, ok in scorecard.items() if not ok]

    return {
        "quality_score": quality_score,
        "scorecard": scorecard,
        "drift_signature": scorecard,
        "limitations": limitations,
    }


def _output_text(request: Any) -> str:
    lane = getattr(request, "lane", None)
    metrics = getattr(lane, "metrics", None)
    output = getattr(metrics, "output_text", None)
    if isinstance(output, str):
        return output
    if isinstance(request, Mapping):
        lane_value = request.get("lane", {})
        if isinstance(lane_value, Mapping):
            metrics_value = lane_value.get("metrics", {})
            if isinstance(metrics_value, Mapping) and isinstance(
                metrics_value.get("output_text"), str
            ):
                return str(metrics_value["output_text"])
    return ""


def _limitation_for(key: str) -> str:
    labels = {
        "non_empty": "Empty response.",
        "refund_escalated_not_confirmed": (
            "Refund safeguard dropped: response does not escalate / require "
            "verification before implying the refund is handled."
        ),
    }
    return labels.get(key, f"Evaluator check failed: {key}.")
