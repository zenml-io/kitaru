#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Bounded explanation formatting for localized numeric findings."""

import json
from typing import Any

_EVIDENCE_LIMIT = 20
_EVIDENCE_MAX_BYTES = 8_000
_STRING_LIMIT = 160
_LIST_LIMIT = 5
_LOCATOR_FIELDS = {"index", "indexes", "start_index", "end_index", "scope"}


def _summarize_value(value: Any) -> Any:
    """Bound the scalar and list details in a localized finding."""
    if isinstance(value, str) and len(value) > _STRING_LIMIT:
        return value[:_STRING_LIMIT] + "... (truncated)"
    if isinstance(value, list):
        items = [_summarize_value(item) for item in value[:_LIST_LIMIT]]
        if len(value) > _LIST_LIMIT:
            items.append(f"... ({len(value) - _LIST_LIMIT} more)")
        return items
    return value


def _encode_finding(finding: dict[str, Any]) -> str:
    """Encode a finding with stable keys and bounded detail values."""
    return json.dumps(
        {key: _summarize_value(value) for key, value in finding.items()},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def format_finding_evidence(findings: list[dict[str, Any]], explanation: str) -> str:
    """Add a bounded sample of findings to a numeric result's explanation."""
    if not findings:
        return explanation
    evidence: list[str] = []
    evidence_bytes = 2
    for finding in findings[:_EVIDENCE_LIMIT]:
        encoded = _encode_finding(finding)
        if (
            evidence_bytes + len(encoded.encode("utf-8")) + bool(evidence)
            > _EVIDENCE_MAX_BYTES
        ):
            encoded = _encode_finding(
                {
                    **{
                        key: value
                        for key, value in finding.items()
                        if key in _LOCATOR_FIELDS
                    },
                    "details": "details omitted",
                }
            )
        added_bytes = len(encoded.encode("utf-8")) + bool(evidence)
        if evidence_bytes + added_bytes > _EVIDENCE_MAX_BYTES:
            continue
        evidence.append(encoded)
        evidence_bytes += added_bytes
    explanation += " Evidence (zero-based node indexes): [" + ",".join(evidence) + "]."
    if len(findings) > len(evidence):
        explanation += f" {len(findings) - len(evidence)} additional findings omitted."
    return explanation
