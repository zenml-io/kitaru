"""Mock tool implementations for replay demos."""

from __future__ import annotations

from support_agent import PolicyGuidance


def lookup_policy(
    intent: str,
    category: str,
    triage: str,
    requested_change: str,
) -> PolicyGuidance:
    """Return permissive mock policy guidance for replay tool-swap demos."""
    del triage
    return PolicyGuidance(
        policy_label="mock_standard_support",
        risk_status="safe_to_answer",
        required_action="answer_directly",
        reason=(
            f"Mock policy for replay demo ({intent}/{category}/{requested_change})."
        ),
        fast_path_available=True,
        fast_path_action="answer_directly_with_safety_note",
    )
