# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Evaluate structured document extraction at the field level."""

import json
from typing import Any

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.task.evaluator import SessionView

EXPECTED = {
    "nist-ai-rmf-1.0": {
        "title": "Artificial Intelligence Risk Management Framework (AI RMF 1.0)",
        "publication_id": "NIST AI 100-1",
        "publication_month": "2023-01",
        "framework_functions": ["GOVERN", "MAP", "MEASURE", "MANAGE"],
    },
    "nist-genai-profile": {
        "title": (
            "Artificial Intelligence Risk Management Framework: "
            "Generative Artificial Intelligence Profile"
        ),
        "publication_id": "NIST AI 600-1",
        "publication_month": "2024-07",
        "framework_functions": ["GOVERN", "MAP", "MEASURE", "MANAGE"],
    },
    "nist-csf-2.0": {
        "title": "The NIST Cybersecurity Framework (CSF) 2.0",
        "publication_id": "NIST CSWP 29",
        "publication_month": "2024-02",
        "framework_functions": [
            "GOVERN",
            "IDENTIFY",
            "PROTECT",
            "DETECT",
            "RESPOND",
            "RECOVER",
        ],
    },
}


def _get_document_id(inputs: Any) -> str:
    """Read a document id from a native or imported session input."""
    if isinstance(inputs, dict) and isinstance(inputs.get("turns"), list):
        turns = inputs["turns"]
        inputs = turns[-1].get("inputs") if turns else None
    if not isinstance(inputs, dict) or not isinstance(inputs.get("document_id"), str):
        raise ValueError("Session inputs do not contain a document_id.")
    return inputs["document_id"]


def _get_output(value: Any) -> dict[str, Any]:
    """Normalize a structured or JSON-encoded extractor output."""
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("Session output is not a document record.")
    return value


def evaluate(session: SessionView) -> EvaluationResult:
    """Score the fraction of labeled fields with an exact match."""
    document_id = _get_document_id(session.session.inputs)
    expected = EXPECTED[document_id]
    actual = _get_output(session.session.outputs)
    matched = [name for name, value in expected.items() if actual.get(name) == value]
    score = len(matched) / len(expected)
    missing = sorted(set(expected) - set(matched))
    return EvaluationResult(
        name="field_accuracy",
        score=score,
        passed=score == 1.0,
        explanation=(
            "All labeled document fields match."
            if not missing
            else f"Mismatched fields: {', '.join(missing)}."
        ),
    )
