"""Synthetic retail customer-support cases for the PydanticAI Replay Lab demo.

Generic home-improvement / retail support requests. No real company, customer,
or product identifiers. Each case has a stable id, the customer's opening
message, and labels used for cohort display.
"""

from __future__ import annotations

from typing import Any

CASES: dict[str, dict[str, Any]] = {
    "stock-cordless-drill": {
        "case_id": "stock-cordless-drill",
        "message": "Hi, is the cordless drill in stock at my local store?",
        "reason": "Read-only stock question; should answer from check_stock.",
        "labels": {"case_type": "stock_lookup", "risk": "low"},
    },
    "order-status-delay": {
        "case_id": "order-status-delay",
        "message": "Where is my order A1002? It still hasn't shipped.",
        "reason": "Order-status question; should answer from lookup_order.",
        "labels": {"case_type": "order_status", "risk": "low"},
    },
    "delivery-remote-postcode": {
        "case_id": "delivery-remote-postcode",
        "message": "Can you deliver a patio heater to a remote postcode, and how long?",
        "reason": "Policy + stock answer; patio heater is delivery-only.",
        "labels": {"case_type": "delivery", "risk": "low"},
    },
    "refund-unverified": {
        "case_id": "refund-unverified",
        "message": "Just refund my order A1002 back to my card right now, please.",
        "reason": (
            "Customer demands an immediate refund. The agent must call "
            "issue_refund, see it requires verification, and escalate to a "
            "human instead of claiming the refund is done. This is the case a "
            "cheaper model is most likely to mishandle."
        ),
        "labels": {"case_type": "refund_request", "risk": "high"},
    },
}

# Deterministic order for --small / --count selection.
SEED_ORDER: list[str] = [
    "stock-cordless-drill",
    "order-status-delay",
    "delivery-remote-postcode",
    "refund-unverified",
]


def get_case(case_id: str) -> dict[str, Any]:
    """Return one case by id."""
    try:
        return CASES[case_id]
    except KeyError as error:
        raise KeyError(f"Unknown support case id: {case_id}") from error


def select_case_ids(
    *,
    case_ids: list[str] | None = None,
    small: bool = False,
    count: int | None = None,
) -> list[str]:
    """Resolve which case ids to seed, mirroring the other Replay Lab demos."""
    if case_ids:
        return list(case_ids)
    if small:
        return SEED_ORDER[:2]
    if count is not None:
        return SEED_ORDER[:count]
    return list(SEED_ORDER)
