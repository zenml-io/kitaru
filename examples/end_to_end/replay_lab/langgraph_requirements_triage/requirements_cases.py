"""Synthetic requirements-triage cases for the live Replay Lab demo."""

from __future__ import annotations

from typing import Any

SCENARIO_VERSION = "requirements_triage_v1"

REQUIREMENTS_CASES: list[dict[str, Any]] = [
    {
        "case_id": "onboarding-workflow-access",
        "title": "Role-based access for an onboarding workflow",
        "request": (
            "The product team wants a new onboarding workflow. Admins should "
            "invite users, managers should approve access, and auditors should "
            "be able to review changes later."
        ),
        "known_requirements": [
            "admins can invite users",
            "managers approve access",
            "auditors can review changes",
        ],
        "missing_information": [
            "approval SLA",
            "audit retention period",
            "fallback owner when a manager is unavailable",
        ],
        "risks": [
            "unclear permissions can grant too much access",
            "missing audit retention can break compliance expectations",
        ],
        "recommended_next_action": (
            "Confirm the approval SLA and audit retention period before writing "
            "implementation tickets."
        ),
        "reason": "Access-control request with compliance-sensitive missing details.",
        "labels": {"tier": "regulated", "trigger": "missing permissions"},
    },
    {
        "case_id": "export-format-change",
        "title": "Customer export needs a new spreadsheet format",
        "request": (
            "A customer success team needs exports to include both a spreadsheet "
            "file and a summary PDF. They also mentioned that old integrations "
            "must keep working."
        ),
        "known_requirements": [
            "spreadsheet export",
            "summary PDF export",
            "existing integrations keep working",
        ],
        "missing_information": [
            "exact spreadsheet columns",
            "PDF layout requirements",
            "backward-compatibility test cases",
        ],
        "risks": [
            "breaking existing integrations",
            "shipping an export format that omits required columns",
        ],
        "recommended_next_action": (
            "Collect a sample spreadsheet and define backward-compatibility tests."
        ),
        "reason": "Format-change request where compatibility risk is easy to miss.",
        "labels": {"tier": "standard", "trigger": "compatibility risk"},
    },
    {
        "case_id": "notification-throttle-request",
        "title": "Reduce noisy notifications without hiding urgent alerts",
        "request": (
            "Users say the notification stream is too noisy. The team wants to "
            "batch low-priority updates but keep urgent alerts immediate."
        ),
        "known_requirements": [
            "batch low-priority updates",
            "urgent alerts stay immediate",
            "users can understand what changed",
        ],
        "missing_information": [
            "definition of urgent",
            "batching interval",
            "user override behavior",
        ],
        "risks": [
            "urgent alerts could be delayed",
            "users may miss important state changes",
        ],
        "recommended_next_action": (
            "Define urgency rules and run examples through the proposed batching "
            "policy."
        ),
        "reason": (
            "Product-quality request where a cheaper model might sound confident "
            "but miss safety trade-offs."
        ),
        "labels": {"tier": "standard", "trigger": "risk triage"},
    },
]


def list_cases() -> list[dict[str, Any]]:
    """Return all committed synthetic requirements cases."""
    return [dict(case) for case in REQUIREMENTS_CASES]


def list_case_ids() -> list[str]:
    """Return case IDs in deterministic seed order."""
    return [case["case_id"] for case in REQUIREMENTS_CASES]


def get_case(case_id: str) -> dict[str, Any]:
    """Return one requirements case by stable ID."""
    for case in REQUIREMENTS_CASES:
        if case["case_id"] == case_id:
            return dict(case)
    known = ", ".join(list_case_ids())
    raise ValueError(f"Unknown requirements case `{case_id}`. Known cases: {known}.")


def select_case_ids(
    *,
    case_ids: list[str] | None,
    small: bool,
    count: int | None,
) -> list[str]:
    """Select case IDs for live seeding.

    The default live path seeds three cases. ``--small`` keeps the first-run path
    short with two cases, and ``--count N`` caps the deterministic order.
    """
    if count is not None and count < 1:
        raise ValueError("count must be at least 1.")
    if case_ids:
        return _validate_case_ids(case_ids)

    selected = list_case_ids()[:2] if small else list_case_ids()
    if count is not None:
        selected = selected[:count]
    return selected


def build_triage_prompt(case: dict[str, Any]) -> str:
    """Build the user prompt for the requirements-triage graph."""
    return (
        "Triage this requirements request. Use exactly these section headings:\n"
        "Summary\nKnown requirements\nMissing information\nRisks\n"
        "Recommended next action\n\n"
        f"Case ID: {case['case_id']}\n"
        f"Title: {case['title']}\n"
        f"Request: {case['request']}\n"
    )


def _validate_case_ids(case_ids: list[str]) -> list[str]:
    for case_id in case_ids:
        get_case(case_id)
    return list(case_ids)
