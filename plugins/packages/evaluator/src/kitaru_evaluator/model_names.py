#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Conservative comparisons of recorded requested and served model names."""

import json
import uuid
from typing import Any

from kitaru.api_models.v1.session_node import SessionNodeResponse

# Google documents bare model IDs and models/{model} resource names:
# https://ai.google.dev/api/models and https://ai.google.dev/gemini-api/docs/models
# Keep supported pairs explicit; versions and arbitrary prefixes can be meaningful.
_EQUIVALENT_MODEL_NAMES = {
    (
        "google",
        frozenset({"gemini-3-flash-preview", "models/gemini-3-flash-preview"}),
    ),
}


def find_model_name_differences(
    calls: list[SessionNodeResponse], positions: dict[uuid.UUID, int]
) -> list[dict[str, Any]]:
    """Find comparable model names whose equivalence is unresolved.

    Args:
        calls: Recorded LLM calls in session order.
        positions: Node positions in the materialized session.

    Returns:
        Original requested and served identifiers with their node positions.
    """
    differences = []
    for node in calls:
        requested, served = node.requested_model, node.model
        if (
            requested is None
            or served is None
            or requested == served
            or (node.model_provider, frozenset({requested, served}))
            in _EQUIVALENT_MODEL_NAMES
        ):
            continue
        differences.append(
            {"index": positions[node.id], "requested": requested, "served": served}
        )
    return differences


def describe_model_name_differences(differences: list[dict[str, Any]]) -> str:
    """Describe unresolved name differences with bounded recorded evidence.

    Args:
        differences: Original identifiers and node positions to describe.

    Returns:
        An explanation with at most five examples and 120 characters per identifier.
    """
    if not differences:
        return "No unresolved recorded-name differences among comparable LLM calls."
    evidence = [
        {
            key: value[:120] if isinstance(value, str) else value
            for key, value in finding.items()
        }
        for finding in differences[:5]
    ]
    return (
        "Unresolved recorded-name differences; model equivalence is unknown. "
        f"Evidence ({len(evidence)} of {len(differences)} calls; "
        "identifiers limited to 120 characters): "
        + json.dumps(evidence, ensure_ascii=False)
    )
