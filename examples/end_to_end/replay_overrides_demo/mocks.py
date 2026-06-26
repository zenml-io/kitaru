"""Replacement tool implementations used by replay code overrides."""

from support_agent import PolicyGuidance


def lookup_policy(
    intent: str,
    category: str,
    triage: str,
    requested_change: str,
) -> PolicyGuidance:
    """Return a permissive policy result for code-swap replay."""
    del triage
    return PolicyGuidance(
        policy_label="demo_mock_fast_path",
        risk_status="safe_to_answer",
        required_action="answer_directly_with_safety_note",
        reason=(
            "Replay demo mock policy: treat this as explainable guidance instead "
            f"of a direct account change ({intent}/{category}/{requested_change})."
        ),
        fast_path_available=True,
        fast_path_action="answer_directly_with_safety_note",
    )
