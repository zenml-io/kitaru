"""Deterministic evaluator for requirements-triage Replay Lab lanes."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

EVALUATOR_ID = "requirements_triage_v1"
SECTION_PATTERNS = {
    "has_summary": r"(?im)^#{0,3}\s*summary\s*:?",
    "lists_known_requirements": r"(?im)^#{0,3}\s*known requirements\s*:?",
    "lists_missing_information": r"(?im)^#{0,3}\s*missing information\s*:?",
    "lists_risks": r"(?im)^#{0,3}\s*risks\s*:?",
    "gives_next_action": r"(?im)^#{0,3}\s*recommended next action\s*:?",
}
FALSE_CERTAINTY_PATTERNS = (
    r"\bguaranteed\b",
    r"\bdefinitely\b",
    r"\bno risk\b",
    r"\bcomplete requirements\b",
    r"\bnothing is missing\b",
)


def evaluate_requirements_triage(request: Any) -> dict[str, Any]:
    """Score one Replay Lab lane output with a transparent section checklist."""
    output_text = _output_text(request)
    scorecard = {
        key: bool(re.search(pattern, output_text))
        for key, pattern in SECTION_PATTERNS.items()
    }
    scorecard["avoids_false_certainty"] = not any(
        re.search(pattern, output_text, flags=re.IGNORECASE)
        for pattern in FALSE_CERTAINTY_PATTERNS
    )

    true_count = sum(1 for value in scorecard.values() if value)
    quality_score = round(true_count / len(scorecard), 3)
    limitations = [
        _limitation_for(key) for key, value in scorecard.items() if not value
    ]

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
        "has_summary": "Missing Summary section.",
        "lists_known_requirements": "Missing Known requirements section.",
        "lists_missing_information": "Missing Missing information section.",
        "lists_risks": "Missing Risks section.",
        "gives_next_action": "Missing Recommended next action section.",
        "avoids_false_certainty": "Output uses false certainty language.",
    }
    return labels.get(key, f"Evaluator check failed: {key}.")
