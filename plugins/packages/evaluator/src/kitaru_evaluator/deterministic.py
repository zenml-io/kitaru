#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Offline deterministic evaluators for materialized Kitaru sessions.

This module is uploaded as one script-plugin source file. Keep every
score-affecting helper in this file and limit imports to the standard library
and Kitaru's evaluator contract models.
"""

import hashlib
import heapq
import json
import math
import uuid
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from itertools import pairwise
from typing import Any

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType, SessionNodeResponse
from kitaru.task.evaluator import SessionView

_EVIDENCE_LIMIT = 20
_RESULT_VALUE_MAX_BYTES = 64_000
_UNAVAILABLE = {"$kitaru": "unavailable"}
_UNSET = object()
_JSON_TYPES = {"null", "boolean", "number", "integer", "string", "array", "object"}
_WORKFLOW_MODES = {"exact_order", "in_order", "contains_all", "exact_set"}


def _format_decimal(value: Decimal) -> str:
    """Format a finite decimal without exponent or insignificant zeroes."""
    if not value.is_finite():
        raise ValueError("number must be finite")
    formatted = format(value, "f")
    if "." in formatted:
        formatted = formatted.rstrip("0").rstrip(".")
    if value == 0:
        return "0"
    return formatted


def _format_datetime(value: datetime) -> str | dict[str, str]:
    """Format an aware datetime in UTC, or mark a naive value unavailable."""
    if value.tzinfo is None or value.utcoffset() is None:
        return _UNAVAILABLE
    return (
        value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


def _normalize_observed(value: Any) -> tuple[Any, bool]:
    """Normalize observed evidence without failing on provider-specific values."""
    if value is None or isinstance(value, (str, bool, int)):
        return value, True
    if isinstance(value, float):
        if math.isfinite(value):
            return value, True
        return _UNAVAILABLE, False
    if isinstance(value, Decimal):
        if value.is_finite():
            return _format_decimal(value), True
        return _UNAVAILABLE, False
    if isinstance(value, datetime):
        normalized = _format_datetime(value)
        return normalized, normalized is not _UNAVAILABLE
    if isinstance(value, uuid.UUID):
        return str(value), True
    if isinstance(value, Enum):
        return _normalize_observed(value.value)
    if isinstance(value, (list, tuple)):
        normalized_items: list[Any] = []
        complete = True
        for item in value:
            normalized, available = _normalize_observed(item)
            normalized_items.append(normalized)
            complete = complete and available
        return normalized_items, complete
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            return _UNAVAILABLE, False
        normalized_dict: dict[str, Any] = {}
        complete = True
        for key in sorted(value):
            normalized, available = _normalize_observed(value[key])
            normalized_dict[key] = normalized
            complete = complete and available
        return normalized_dict, complete
    return _UNAVAILABLE, False


def _canonical_json(value: Any) -> str:
    """Serialize normalized JSON with the revision-1 encoding."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    """Hash a normalized value using UTF-8 encoded canonical JSON."""
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _token_fields(tokens: Any) -> Any:
    """Select the token fields used by the evaluator bundles."""
    if tokens is None:
        return None
    return {
        "cached_input_tokens": tokens.cached_input_tokens,
        "input_tokens": tokens.input_tokens,
        "output_tokens": tokens.output_tokens,
        "reasoning_tokens": tokens.reasoning_tokens,
    }


def _json_result(
    name: str,
    value: Any,
    explanation: str,
    *,
    passed: bool | None = None,
) -> EvaluationResult:
    """Create a deterministic structured-string result."""
    normalized, _ = _normalize_observed(value)
    encoded = _canonical_json(normalized)
    encoded_bytes = encoded.encode("utf-8")
    if len(encoded_bytes) > _RESULT_VALUE_MAX_BYTES:
        encoded = _canonical_json(
            {
                "$kitaru": "result_truncated",
                "sha256": hashlib.sha256(encoded_bytes).hexdigest(),
                "utf8_bytes": len(encoded_bytes),
            }
        )
    return EvaluationResult(
        name=name,
        value=encoded,
        explanation=explanation,
        passed=passed,
    )


def _finding_result(
    name: str,
    findings: list[dict[str, Any]],
    explanation: str,
    *,
    limit: int = _EVIDENCE_LIMIT,
) -> EvaluationResult:
    """Encode bounded localized evidence and retain its unbounded count."""
    return _json_result(
        name,
        {
            "evidence": findings[:limit],
            "total": len(findings),
            "truncated": len(findings) > limit,
        },
        explanation,
    )


def _analysis_nodes(view: SessionView) -> list[SessionNodeResponse]:
    """Sort nodes for stable analysis and evidence presentation."""
    return sorted(view.nodes, key=lambda node: (node.index, str(node.id)))


def _is_terminal(view: SessionView) -> bool:
    """Return whether the session materialization is terminal."""
    return view.session.status in {SessionStatus.COMPLETED, SessionStatus.FAILED}


def _validate_string_list(
    value: list[str] | None, name: str, *, allow_empty: bool = False
) -> list[str] | None:
    """Validate an optional JSON string set represented as a list."""
    if value is None:
        return None
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise ValueError(f"{name} must be a list of non-empty strings")
    if not allow_empty and not value:
        raise ValueError(f"{name} must not be empty")
    if len(set(value)) != len(value):
        raise ValueError(f"{name} contains a duplicate value")
    return sorted(value)


def _validate_limit(value: int) -> int:
    """Validate a bounded evidence retention limit."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("evidence_limit must be an integer")
    if not 1 <= value <= 100:
        raise ValueError("evidence_limit must be between 1 and 100")
    return value


def _validate_ceiling(value: int | float | None, name: str) -> Decimal | None:
    """Validate a non-negative finite numeric ceiling."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError(f"{name} must be a number")
    try:
        ceiling = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"{name} must be a number") from exc
    if not ceiling.is_finite():
        raise ValueError(f"{name} must be finite")
    if ceiling < 0:
        raise ValueError(f"{name} must be non-negative")
    return ceiling


def _duration(started_at: datetime | None, ended_at: datetime | None) -> Decimal | None:
    """Return a non-negative recorded duration, or None for invalid evidence."""
    if started_at is None or ended_at is None or ended_at < started_at:
        return None
    return Decimal(str((ended_at - started_at).total_seconds()))


def _is_nonnegative_decimal(value: Decimal) -> bool:
    """Return whether a recorded decimal is finite and non-negative."""
    return value.is_finite() and value >= 0


def _is_nonnegative_integer(value: Any) -> bool:
    """Return whether a recorded token count is a non-negative integer."""
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def sum_decimals(values: list[Decimal]) -> Decimal:
    """Sum finite decimals exactly without using the ambient decimal context."""
    if not values:
        return Decimal(0)
    parts: list[tuple[int, tuple[int, ...], int]] = []
    for value in values:
        decimal_tuple = value.as_tuple()
        if not isinstance(decimal_tuple.exponent, int):
            raise ValueError("number must be finite")
        parts.append((decimal_tuple.sign, decimal_tuple.digits, decimal_tuple.exponent))
    minimum_exponent = min(exponent for _, _, exponent in parts)
    total = 0
    for sign, digits, exponent in parts:
        coefficient = int("".join(str(digit) for digit in digits) or "0")
        if sign:
            coefficient = -coefficient
        total += coefficient * 10 ** (exponent - minimum_exponent)
    sign = 1 if total < 0 else 0
    digits = tuple(int(digit) for digit in str(abs(total))) if total else (0,)
    return Decimal((sign, digits, minimum_exponent))


def _resource_value(value: Decimal | int | None) -> str:
    """Encode a numeric resource value without conflating absence and zero."""
    if value is None:
        return "unavailable"
    if isinstance(value, int):
        return str(value)
    return _format_decimal(value)


def session_diagnostics(session: SessionView) -> list[EvaluationResult]:
    """Describe materialized session completeness and internal consistency."""
    nodes = _analysis_nodes(session)
    supplied_keys = [(node.index, str(node.id)) for node in session.nodes]
    ordered = supplied_keys == sorted(supplied_keys)
    index_counts = Counter(node.index for node in nodes)
    duplicate_indexes = sorted(
        index for index, count in index_counts.items() if count > 1
    )
    duplicate_ids = sorted(
        node_id
        for node_id, count in Counter(str(node.id) for node in nodes).items()
        if count > 1
    )

    indexes = {node.index for node in nodes}
    ids = {node.id for node in nodes}
    ids_by_index = {
        node.index: node.id for node in nodes if index_counts[node.index] == 1
    }
    parent_findings: list[dict[str, Any]] = []
    for node in nodes:
        missing = sorted(
            parent
            for parent in [node.parent_index, *node.secondary_parent_indexes]
            if parent is not None and parent not in indexes
        )
        nonpreceding = sorted(
            parent
            for parent in [node.parent_index, *node.secondary_parent_indexes]
            if parent is not None and parent >= node.index
        )
        id_mismatches: list[dict[str, Any]] = []
        if node.parent_index is None:
            if node.parent_id is not None:
                id_mismatches.append({"kind": "primary_without_index"})
        elif node.parent_id != ids_by_index.get(node.parent_index):
            id_mismatches.append({"kind": "primary_index_id_mismatch"})
        if len(node.secondary_parent_indexes) != len(node.secondary_parent_ids):
            id_mismatches.append({"kind": "secondary_cardinality_mismatch"})
        for parent_index, parent_id in zip(
            node.secondary_parent_indexes,
            node.secondary_parent_ids,
            strict=False,
        ):
            if parent_id != ids_by_index.get(parent_index):
                id_mismatches.append(
                    {
                        "kind": "secondary_index_id_mismatch",
                        "parent_index": parent_index,
                    }
                )
        missing_ids = sorted(
            str(parent_id)
            for parent_id in [node.parent_id, *node.secondary_parent_ids]
            if parent_id is not None and parent_id not in ids
        )
        if missing or nonpreceding or id_mismatches or missing_ids:
            parent_findings.append(
                {
                    "id_mismatches": id_mismatches,
                    "index": node.index,
                    "missing_ids": missing_ids,
                    "missing_indexes": missing,
                    "nonpreceding_indexes": nonpreceding,
                }
            )

    chronology: list[dict[str, Any]] = []
    for node in nodes:
        if (
            node.started_at is not None
            and node.ended_at is not None
            and node.ended_at < node.started_at
        ):
            chronology.append({"index": node.index, "kind": "negative_duration"})

    payload_complete = sum(
        node.inputs is not None and node.outputs is not None for node in nodes
    )
    tool_nodes = [node for node in nodes if node.node_type is NodeType.TOOL_CALL]
    llm_nodes = [node for node in nodes if node.node_type is NodeType.LLM_CALL]
    cost_nodes = [
        node
        for node in nodes
        if node.node_type in {NodeType.LLM_CALL, NodeType.TOOL_CALL}
    ]
    duration = _duration(session.session.started_at, session.session.ended_at)
    token_complete = sum(
        node.tokens is not None
        and node.tokens.input_tokens is not None
        and node.tokens.output_tokens is not None
        for node in llm_nodes
    )
    resource_findings: list[dict[str, Any]] = []
    if session.session.cost is not None and not _is_nonnegative_decimal(
        session.session.cost
    ):
        resource_findings.append({"field": "cost", "scope": "session"})
    if session.session.tokens is not None:
        for field, value in _token_fields(session.session.tokens).items():
            if value is not None and not _is_nonnegative_integer(value):
                resource_findings.append(
                    {"field": f"tokens.{field}", "scope": "session"}
                )
    for node in nodes:
        if node.cost is not None and not _is_nonnegative_decimal(node.cost):
            resource_findings.append(
                {"field": "cost", "index": node.index, "scope": "node"}
            )
        if node.tokens is not None:
            for field, value in _token_fields(node.tokens).items():
                if value is not None and not _is_nonnegative_integer(value):
                    resource_findings.append(
                        {
                            "field": f"tokens.{field}",
                            "index": node.index,
                            "scope": "node",
                        }
                    )
    results: list[EvaluationResult] = []
    results.extend(
        [
            _json_result(
                "terminality",
                {
                    "status": session.session.status.value,
                    "terminal": _is_terminal(session),
                },
                "Recorded session status and terminality.",
            ),
            _json_result(
                "node_order",
                {
                    "ordered": ordered,
                    "duplicate_ids": duplicate_ids,
                    "duplicate_indexes": duplicate_indexes,
                },
                "Supplied node ordering and identity diagnostics.",
            ),
            _finding_result(
                "parent_linkage",
                parent_findings,
                "Nodes with missing or nonpreceding parent indexes.",
            ),
            _finding_result(
                "chronology_findings",
                chronology,
                "Nodes whose recorded end precedes their start.",
            ),
            _json_result(
                "payload_coverage",
                {"complete": payload_complete, "total": len(nodes)},
                "Nodes with both input and output payloads present.",
            ),
            _json_result(
                "recorded_counts",
                {
                    "fetched_nodes": len(nodes),
                    "llm_nodes": len(llm_nodes),
                    "session_llm_calls": session.session.llm_call_count,
                    "session_tool_calls": session.session.tool_call_count,
                    "tool_nodes": len(tool_nodes),
                },
                "Fetched and session-level node and call counts.",
            ),
            EvaluationResult(
                name="duration_seconds",
                value=_resource_value(duration),
                explanation="Terminal wall-clock duration when valid timestamps exist.",
            ),
            _json_result(
                "cost_coverage",
                {
                    "node_recorded": sum(node.cost is not None for node in cost_nodes),
                    "node_total": len(cost_nodes),
                    "session": _resource_value(session.session.cost),
                },
                "Availability of session and call-level cost evidence.",
            ),
            _json_result(
                "token_coverage",
                {
                    "node_complete": token_complete,
                    "node_total": len(llm_nodes),
                    "session_complete": session.session.tokens is not None
                    and session.session.tokens.input_tokens is not None
                    and session.session.tokens.output_tokens is not None,
                },
                "Availability of input and output token evidence.",
            ),
            _finding_result(
                "resource_integrity",
                resource_findings,
                "Recorded negative or non-finite resource values.",
            ),
        ]
    )
    return results


def _decode_pointer_part(raw_part: str) -> str:
    """Decode one RFC 6901 reference token and reject invalid escapes."""
    decoded: list[str] = []
    position = 0
    while position < len(raw_part):
        character = raw_part[position]
        if character != "~":
            decoded.append(character)
            position += 1
            continue
        if position + 1 >= len(raw_part) or raw_part[position + 1] not in {"0", "1"}:
            raise ValueError("JSON Pointer '~' escapes must be '~0' or '~1'")
        decoded.append("~" if raw_part[position + 1] == "0" else "/")
        position += 2
    return "".join(decoded)


def _decode_pointer_parts(pointer: str) -> list[str]:
    """Validate and decode every token in an RFC 6901 JSON Pointer."""
    if pointer == "":
        return []
    if not pointer.startswith("/"):
        raise ValueError("JSON Pointer paths must be empty or start with '/'")
    return [_decode_pointer_part(part) for part in pointer[1:].split("/")]


def _resolve_pointer(document: Any, pointer: str) -> tuple[bool, Any]:
    """Resolve an RFC 6901 JSON Pointer against normalized JSON evidence."""
    current = document
    for part in _decode_pointer_parts(pointer):
        if isinstance(current, dict):
            if part not in current:
                return False, None
            current = current[part]
        elif isinstance(current, list):
            if (
                not part.isascii()
                or not part.isdigit()
                or (len(part) > 1 and part.startswith("0"))
            ):
                return False, None
            index = int(part)
            if index >= len(current):
                return False, None
            current = current[index]
        else:
            return False, None
    return True, current


def _validate_pointer_list(value: list[str] | None) -> list[str] | None:
    """Validate optional RFC 6901 pointers, including the empty root pointer."""
    if value is None:
        return None
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(pointer, str) for pointer in value)
    ):
        raise ValueError("required_paths must be a non-empty list of strings")
    if len(set(value)) != len(value):
        raise ValueError("required_paths contains a duplicate value")
    for pointer in value:
        _resolve_pointer({}, pointer)
    return sorted(value)


def _json_type(value: Any) -> str:
    """Return the JSON type name, keeping booleans separate from integers."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def output_contract(
    session: SessionView,
    expected: Any = _UNSET,
    required_paths: list[str] | None = None,
    type_requirements: dict[str, str] | None = None,
) -> list[EvaluationResult]:
    """Check exact output, JSON Pointer presence, and JSON value types."""
    if expected is _UNSET and required_paths is None and type_requirements is None:
        raise ValueError("at least one output contract rule is required")
    paths = _validate_pointer_list(required_paths)
    if type_requirements is not None:
        if not isinstance(type_requirements, dict) or not type_requirements:
            raise ValueError("type_requirements must be a non-empty object")
        if any(
            not isinstance(path, str)
            or not isinstance(kind, str)
            or kind not in _JSON_TYPES
            for path, kind in type_requirements.items()
        ):
            raise ValueError("type_requirements contains an invalid JSON type")
        for path in type_requirements:
            _resolve_pointer({}, path)

    normalized_output, output_complete = _normalize_observed(session.session.outputs)
    normalized_expected: Any = None
    if expected is not _UNSET:
        normalized_expected, expected_complete = _normalize_observed(expected)
        if not expected_complete:
            raise ValueError("expected must contain only finite JSON values")
    results: list[EvaluationResult] = []
    output_available = session.session.outputs is not None and output_complete
    terminal = _is_terminal(session)
    results.append(
        EvaluationResult(
            name="output_availability",
            value="available" if output_available else "unavailable",
            explanation="Whether a non-null output can be compared safely.",
        )
    )
    if expected is not _UNSET:
        passed = None
        if output_available:
            passed = _canonical_json(normalized_output) == _canonical_json(
                normalized_expected
            )
            if passed and not terminal:
                passed = None
        results.append(
            _json_result(
                "exact_output",
                {
                    "expected_sha256": _hash(normalized_expected),
                    "observed_sha256": (
                        _hash(normalized_output) if output_available else None
                    ),
                },
                "Exact canonical JSON comparison of recorded and expected output.",
                passed=passed,
            )
        )
    if paths is not None:
        missing = []
        if output_available:
            missing = [
                path
                for path in paths
                if not _resolve_pointer(normalized_output, path)[0]
            ]
        results.append(
            _json_result(
                "required_paths",
                {"missing": missing, "required": paths},
                "Required RFC 6901 JSON Pointer paths.",
                passed=(False if missing else True if terminal else None)
                if output_available
                else None,
            )
        )
    if type_requirements is not None:
        mismatches: list[dict[str, str]] = []
        if output_available:
            for path, expected_type in sorted(type_requirements.items()):
                found, value = _resolve_pointer(normalized_output, path)
                observed_type = _json_type(value) if found else "missing"
                matches = observed_type == expected_type or (
                    expected_type == "number" and observed_type == "integer"
                )
                if not matches:
                    mismatches.append(
                        {
                            "expected": expected_type,
                            "observed": observed_type,
                            "path": path,
                        }
                    )
        results.append(
            _json_result(
                "type_requirements",
                {
                    "mismatches": mismatches,
                    "required": dict(sorted(type_requirements.items())),
                },
                "Expected JSON types at RFC 6901 paths.",
                passed=(False if mismatches else True if terminal else None)
                if output_available
                else None,
            )
        )
    return results


def _tool_calls(session: SessionView) -> list[SessionNodeResponse]:
    """Project tool calls in stable index and id order."""
    return [
        node
        for node in _analysis_nodes(session)
        if node.node_type is NodeType.TOOL_CALL
    ]


def _tool_identity(node: SessionNodeResponse) -> tuple[str, str] | None:
    """Return exact tool name and canonical input identity when available."""
    if node.tool_name is None or node.inputs is None:
        return None
    normalized, complete = _normalize_observed(node.inputs)
    if not complete:
        return None
    return node.tool_name, _canonical_json(normalized)


def _cycle_findings(
    calls: list[SessionNodeResponse],
    identities: list[tuple[str, str] | None],
) -> list[dict[str, Any]]:
    """Detect left-maximal call cycles of period 2-5 repeated at least 3 times."""
    candidates: list[dict[str, Any]] = []
    names = [call.tool_name for call in calls]
    for period in range(2, 6):
        match_start: int | None = None
        for position in range(len(identities) - period + 1):
            matches = (
                position < len(identities) - period
                and identities[position] is not None
                and identities[position] == identities[position + period]
            )
            if matches and match_start is None:
                match_start = position
            if matches:
                continue
            if match_start is None:
                continue
            end = position + period
            if position - match_start >= 2 * period:
                candidates.append(
                    {
                        "end_index": calls[end - 1].index,
                        "period": period,
                        "repetitions": (end - match_start) // period,
                        "start_index": calls[match_start].index,
                        "tools": names[match_start : match_start + period],
                    }
                )
            match_start = None
    candidates.sort(
        key=lambda finding: (
            finding["start_index"],
            -(finding["end_index"] - finding["start_index"]),
            finding["period"],
        )
    )
    retained: list[dict[str, Any]] = []
    maximum_end: int | None = None
    for candidate in candidates:
        if maximum_end is not None and candidate["end_index"] <= maximum_end:
            continue
        retained.append(candidate)
        maximum_end = candidate["end_index"]
    return retained


def trajectory_signals(session: SessionView) -> list[EvaluationResult]:
    """Describe exact retries, adjacent repetition, and bounded short cycles."""
    calls = _tool_calls(session)
    identities = [_tool_identity(call) for call in calls]
    repeats: list[dict[str, Any]] = []
    retries: list[dict[str, Any]] = []
    for first, second, first_identity, second_identity in zip(
        calls, calls[1:], identities, identities[1:], strict=False
    ):
        if first_identity is None or first_identity != second_identity:
            continue
        evidence = {
            "indexes": [first.index, second.index],
            "tool_name": first.tool_name,
        }
        repeats.append(evidence)
        if first.status is NodeStatus.FAILED:
            retries.append(evidence)
    results: list[EvaluationResult] = []
    results.extend(
        [
            _json_result(
                "tool_identity_coverage",
                {
                    "complete": sum(identity is not None for identity in identities),
                    "total": len(calls),
                    "unknown_indexes": [
                        call.index
                        for call, identity in zip(calls, identities, strict=True)
                        if identity is None
                    ],
                },
                "Tool calls with exact name and canonically encodable inputs.",
            ),
            _finding_result(
                "adjacent_identical_calls",
                repeats,
                "Adjacent exact tool-call identities.",
            ),
            _finding_result(
                "failed_identical_retries",
                retries,
                "Adjacent exact calls whose first call is recorded failed.",
            ),
            _finding_result(
                "short_cycles",
                _cycle_findings(calls, identities),
                "Repeated exact tool-call cycles.",
            ),
            _json_result(
                "cycle_detector_bounds",
                {"max_period": 5, "min_period": 2, "min_repetitions": 3},
                "Revision-1 short-cycle detector bounds.",
            ),
        ]
    )
    return results


def _is_empty(value: Any) -> bool:
    """Return whether a non-null recorded result is an empty string or container."""
    return isinstance(value, (str, list, dict)) and len(value) == 0


def tool_health(session: SessionView) -> list[EvaluationResult]:
    """Describe recorded tool failures and result-payload health."""
    calls = _tool_calls(session)
    failed = [
        {"index": node.index, "tool_name": node.tool_name}
        for node in calls
        if node.status is NodeStatus.FAILED
    ]
    null = [
        {"index": node.index, "tool_name": node.tool_name}
        for node in calls
        if node.outputs is None
    ]
    empty = [
        {"index": node.index, "tool_name": node.tool_name}
        for node in calls
        if node.outputs is not None and _is_empty(node.outputs)
    ]
    inconsistent = [
        {"index": node.index, "tool_name": node.tool_name}
        for node in calls
        if (node.status is NodeStatus.FAILED and node.error is None)
        or (node.status is not NodeStatus.FAILED and node.error is not None)
    ]
    repeated_failures: list[dict[str, Any]] = []
    for first, second in pairwise(calls):
        if (
            first.status is NodeStatus.FAILED
            and second.status is NodeStatus.FAILED
            and first.tool_name is not None
            and first.tool_name == second.tool_name
        ):
            repeated_failures.append(
                {"indexes": [first.index, second.index], "tool_name": first.tool_name}
            )
    results: list[EvaluationResult] = []
    results.extend(
        [
            _finding_result("failed_calls", failed, "Tool calls recorded as failed."),
            _finding_result(
                "null_results", null, "Tool calls with a null recorded result."
            ),
            _finding_result(
                "empty_results",
                empty,
                "Tool calls with an empty string or container result.",
            ),
            _finding_result(
                "error_status_inconsistencies",
                inconsistent,
                "Tool calls whose error field disagrees with recorded status.",
            ),
            _finding_result(
                "adjacent_repeated_failures",
                repeated_failures,
                "Adjacent failed calls to the same named tool.",
            ),
        ]
    )
    return results


def timing_profile(
    session: SessionView, evidence_limit: int = _EVIDENCE_LIMIT
) -> list[EvaluationResult]:
    """Describe wall-clock and node timing evidence without outlier labels."""
    limit = _validate_limit(evidence_limit)
    nodes = _analysis_nodes(session)
    intervals: list[tuple[datetime, datetime, SessionNodeResponse, Decimal]] = []
    invalid: list[dict[str, Any]] = []
    for node in nodes:
        if node.started_at is None or node.ended_at is None:
            continue
        duration = _duration(node.started_at, node.ended_at)
        if duration is None:
            invalid.append({"index": node.index})
            continue
        intervals.append((node.started_at, node.ended_at, node, duration))
    slowest = [
        {"duration_seconds": _format_decimal(duration), "index": node.index}
        for _, _, node, duration in heapq.nsmallest(
            limit,
            intervals,
            key=lambda item: (-item[3], item[2].index, str(item[2].id)),
        )
    ]

    overlaps: list[dict[str, Any]] = []
    overlap_total = 0
    active_heap: list[tuple[datetime, str]] = []
    active: dict[str, SessionNodeResponse] = {}
    for started_at, ended_at, node, _ in sorted(
        intervals, key=lambda item: (item[0], item[2].index, str(item[2].id))
    ):
        while active_heap and active_heap[0][0] <= started_at:
            _, node_id = heapq.heappop(active_heap)
            active.pop(node_id, None)
        overlap_total += len(active)
        if len(overlaps) < limit:
            for other in sorted(
                active.values(), key=lambda item: (item.index, str(item.id))
            ):
                if len(overlaps) == limit:
                    break
                overlaps.append({"indexes": [other.index, node.index]})
        node_id = str(node.id)
        active[node_id] = node
        heapq.heappush(active_heap, (ended_at, node_id))
    wall_clock = _duration(session.session.started_at, session.session.ended_at)
    results: list[EvaluationResult] = []
    results.extend(
        [
            EvaluationResult(
                name="wall_clock_duration_seconds",
                value=_resource_value(wall_clock),
                explanation="Valid recorded session wall-clock duration.",
            ),
            _json_result(
                "node_duration_coverage",
                {
                    "complete": len(intervals) + len(invalid),
                    "total": len(nodes),
                    "valid": len(intervals),
                },
                "Node records with both timing endpoints and valid intervals.",
            ),
            _json_result(
                "slowest_nodes",
                {
                    "evidence": slowest,
                    "total": len(intervals),
                    "truncated": len(intervals) > limit,
                },
                "Longest valid node intervals in descending duration order.",
            ),
            _json_result(
                "overlapping_intervals",
                {
                    "evidence": overlaps,
                    "total": overlap_total,
                    "truncated": overlap_total > len(overlaps),
                },
                "Pairs of node intervals that overlap in recorded time.",
            ),
            _finding_result(
                "invalid_intervals",
                invalid,
                "Node intervals whose end precedes their start.",
                limit=limit,
            ),
        ]
    )
    return results


def _sum_cost(nodes: list[SessionNodeResponse]) -> tuple[Decimal | None, bool]:
    """Sum costs and report whether every relevant node recorded one."""
    relevant = [
        node
        for node in nodes
        if node.node_type is NodeType.LLM_CALL or node.cost is not None
    ]
    complete = all(
        node.cost is not None and _is_nonnegative_decimal(node.cost)
        for node in relevant
    )
    if not complete:
        return None, False
    return sum_decimals([node.cost for node in relevant if node.cost is not None]), True


def _sum_tokens(nodes: list[SessionNodeResponse]) -> tuple[int | None, bool]:
    """Sum input plus output tokens without double-counting components."""
    relevant = [
        node
        for node in nodes
        if node.node_type is NodeType.LLM_CALL or node.tokens is not None
    ]
    complete = all(
        node.tokens is not None
        and _is_nonnegative_integer(node.tokens.input_tokens)
        and _is_nonnegative_integer(node.tokens.output_tokens)
        for node in relevant
    )
    if not complete:
        return None, False
    total = sum(
        node.tokens.input_tokens + node.tokens.output_tokens
        for node in relevant
        if node.tokens is not None
        and node.tokens.input_tokens is not None
        and node.tokens.output_tokens is not None
    )
    return total, True


def _budget_result(
    name: str,
    ceiling: Decimal,
    observations: Mapping[str, Decimal | int | None],
    *,
    complete: bool,
    explanation: str,
) -> EvaluationResult:
    """Apply a ceiling with decisive failure and conservative pass semantics."""
    available = [Decimal(value) for value in observations.values() if value is not None]
    passed: bool | None
    if any(value > ceiling for value in available):
        passed = False
    elif complete:
        passed = True
    else:
        passed = None
    return _json_result(
        name,
        {
            "ceiling": _format_decimal(ceiling),
            "observed": {
                key: _resource_value(value) for key, value in observations.items()
            },
        },
        explanation,
        passed=passed,
    )


def resource_budget(
    session: SessionView,
    max_duration_seconds: int | float | None = None,
    max_cost: int | float | None = None,
    max_total_tokens: int | float | None = None,
    max_nodes: int | float | None = None,
    max_llm_calls: int | float | None = None,
    max_tool_calls: int | float | None = None,
) -> list[EvaluationResult]:
    """Apply explicit inclusive resource ceilings to recorded evidence."""
    raw = {
        "max_cost": max_cost,
        "max_duration_seconds": max_duration_seconds,
        "max_llm_calls": max_llm_calls,
        "max_nodes": max_nodes,
        "max_tool_calls": max_tool_calls,
        "max_total_tokens": max_total_tokens,
    }
    ceilings = {name: _validate_ceiling(value, name) for name, value in raw.items()}
    for name in ("max_nodes", "max_llm_calls", "max_tool_calls"):
        ceiling = ceilings[name]
        if ceiling is not None and ceiling != ceiling.to_integral_value():
            raise ValueError(f"{name} must be an integer")
    if all(value is None for value in ceilings.values()):
        raise ValueError("at least one resource ceiling is required")
    results: list[EvaluationResult] = []
    terminal = _is_terminal(session)
    nodes = _analysis_nodes(session)
    tool_count = sum(node.node_type is NodeType.TOOL_CALL for node in nodes)
    llm_count = sum(node.node_type is NodeType.LLM_CALL for node in nodes)
    node_cost, node_cost_complete = _sum_cost(nodes)
    node_tokens, node_tokens_complete = _sum_tokens(nodes)
    session_tokens = None
    session_tokens_complete = (
        session.session.tokens is not None
        and _is_nonnegative_integer(session.session.tokens.input_tokens)
        and _is_nonnegative_integer(session.session.tokens.output_tokens)
    )
    if session_tokens_complete:
        assert session.session.tokens is not None
        assert session.session.tokens.input_tokens is not None
        assert session.session.tokens.output_tokens is not None
        session_tokens = (
            session.session.tokens.input_tokens + session.session.tokens.output_tokens
        )

    if (ceiling := ceilings["max_duration_seconds"]) is not None:
        duration = _duration(session.session.started_at, session.session.ended_at)
        results.append(
            _budget_result(
                "duration_budget",
                ceiling,
                {"session": duration},
                complete=terminal and duration is not None,
                explanation=(
                    "Inclusive ceiling for recorded terminal wall-clock duration."
                ),
            )
        )
    if (ceiling := ceilings["max_cost"]) is not None:
        session_cost = session.session.cost
        if session_cost is not None and not _is_nonnegative_decimal(session_cost):
            session_cost = None
        results.append(
            _budget_result(
                "cost_budget",
                ceiling,
                {
                    "nodes": node_cost if node_cost_complete else None,
                    "session": session_cost,
                },
                complete=terminal
                and session_cost is not None
                and node_cost_complete
                and session_cost == node_cost,
                explanation=(
                    "Inclusive ceiling reconciled across session and call costs."
                ),
            )
        )
    if (ceiling := ceilings["max_total_tokens"]) is not None:
        results.append(
            _budget_result(
                "total_tokens_budget",
                ceiling,
                {
                    "nodes": node_tokens if node_tokens_complete else None,
                    "session": session_tokens,
                },
                complete=terminal
                and session_tokens_complete
                and node_tokens_complete
                and session_tokens == node_tokens,
                explanation="Inclusive ceiling for input plus output tokens.",
            )
        )
    count_rules = [
        (
            "max_nodes",
            "node_count_budget",
            len(nodes),
            len(nodes),
            "Inclusive fetched-node ceiling.",
        ),
        (
            "max_llm_calls",
            "llm_call_count_budget",
            session.session.llm_call_count,
            llm_count,
            "Inclusive LLM-call ceiling reconciled with fetched nodes.",
        ),
        (
            "max_tool_calls",
            "tool_call_count_budget",
            session.session.tool_call_count,
            tool_count,
            "Inclusive tool-call ceiling reconciled with fetched nodes.",
        ),
    ]
    for config_name, result_name, rollup, derived, explanation in count_rules:
        ceiling = ceilings[config_name]
        if ceiling is None:
            continue
        observations = {"nodes": derived}
        complete = terminal
        if config_name != "max_nodes":
            observations["session"] = rollup
            complete = complete and rollup == derived
        results.append(
            _budget_result(
                result_name,
                ceiling,
                observations,
                complete=complete,
                explanation=explanation,
            )
        )
    return results


def tool_policy(
    session: SessionView,
    required_tools: list[str] | None = None,
    forbidden_tools: list[str] | None = None,
    max_calls_per_tool: dict[str, int] | None = None,
) -> list[EvaluationResult]:
    """Apply exact case-sensitive rules to recorded tool names."""
    required = _validate_string_list(required_tools, "required_tools")
    forbidden = _validate_string_list(forbidden_tools, "forbidden_tools")
    if (
        required is not None
        and forbidden is not None
        and set(required) & set(forbidden)
    ):
        raise ValueError("required_tools and forbidden_tools conflict")
    if max_calls_per_tool is not None:
        if not isinstance(max_calls_per_tool, dict) or not max_calls_per_tool:
            raise ValueError("max_calls_per_tool must be a non-empty object")
        for name, maximum in max_calls_per_tool.items():
            if not isinstance(name, str) or not name:
                raise ValueError("max_calls_per_tool keys must be non-empty strings")
            if isinstance(maximum, bool) or not isinstance(maximum, int):
                raise ValueError("max_calls_per_tool values must be integers")
            if maximum < 0:
                raise ValueError("max_calls_per_tool values must be non-negative")
        if required is not None and any(
            max_calls_per_tool.get(name) == 0 for name in required
        ):
            raise ValueError("a required tool cannot have a maximum of zero calls")
    if required is None and forbidden is None and max_calls_per_tool is None:
        raise ValueError("at least one tool policy rule is required")
    maximums = dict(sorted((max_calls_per_tool or {}).items()))
    results: list[EvaluationResult] = []
    calls = _tool_calls(session)
    known, name_coverage = _get_tool_name_coverage(calls)
    complete = _is_terminal(session) and len(known) == len(calls)
    counts = Counter(known)
    results.append(name_coverage)
    if required is not None:
        missing = [name for name in required if counts[name] == 0]
        results.append(
            _json_result(
                "required_tools",
                {"missing": missing, "required": required},
                "Required exact tool names.",
                passed=(not missing) if complete else None,
            )
        )
    if forbidden is not None:
        violations = [name for name in forbidden if counts[name] > 0]
        passed = False if violations else True if complete else None
        results.append(
            _json_result(
                "forbidden_tools",
                {"forbidden": forbidden, "violations": violations},
                "Forbidden exact tool names.",
                passed=passed,
            )
        )
    if max_calls_per_tool is not None:
        violations = [
            {"count": counts[name], "maximum": maximum, "tool_name": name}
            for name, maximum in maximums.items()
            if counts[name] > maximum
        ]
        passed = False if violations else True if complete else None
        results.append(
            _json_result(
                "per_tool_maximums",
                {"maximums": maximums, "violations": violations},
                "Maximum recorded calls per exact tool name.",
                passed=passed,
            )
        )
    return results


def _llm_calls(session: SessionView) -> list[SessionNodeResponse]:
    """Project LLM calls in stable index and id order."""
    return [
        node for node in _analysis_nodes(session) if node.node_type is NodeType.LLM_CALL
    ]


def _get_tool_name_coverage(
    calls: list[SessionNodeResponse],
) -> tuple[list[str], EvaluationResult]:
    """Return known tool names and their coverage result."""
    known = [call.tool_name for call in calls if call.tool_name is not None]
    return known, _json_result(
        "tool_name_coverage",
        {
            "complete": len(known),
            "total": len(calls),
            "unknown_indexes": [call.index for call in calls if call.tool_name is None],
        },
        "Tool calls with exact recorded names.",
    )


def llm_call_signals(session: SessionView) -> list[EvaluationResult]:
    """Describe exact LLM call repetition, failures, and metadata coverage."""
    calls = _llm_calls(session)
    failed = [
        {"index": node.index} for node in calls if node.status is NodeStatus.FAILED
    ]
    empty = [
        {"index": node.index}
        for node in calls
        if node.outputs is None or _is_empty(node.outputs)
    ]
    repeated: list[dict[str, Any]] = []
    previous: tuple[SessionNodeResponse, Any, bool] | None = None
    for call in calls:
        normalized, complete = _normalize_observed(call.inputs)
        if (
            previous is not None
            and previous[2]
            and complete
            and _canonical_json(previous[1]) == _canonical_json(normalized)
        ):
            repeated.append({"indexes": [previous[0].index, call.index]})
        previous = call, normalized, complete and call.inputs is not None
    mismatches = [
        {"index": node.index, "requested": node.requested_model, "served": node.model}
        for node in calls
        if node.requested_model is not None
        and node.model is not None
        and node.requested_model != node.model
    ]
    results: list[EvaluationResult] = []
    results.extend(
        [
            _finding_result("failed_calls", failed, "LLM calls recorded as failed."),
            _finding_result(
                "empty_results", empty, "LLM calls with null or empty recorded output."
            ),
            _finding_result(
                "adjacent_identical_inputs",
                repeated,
                "Adjacent LLM calls with exact canonical inputs.",
            ),
            _finding_result(
                "requested_model_mismatches",
                mismatches,
                "Calls whose requested and served model names differ.",
            ),
            _json_result(
                "metadata_coverage",
                {
                    "complete_model": sum(node.model is not None for node in calls),
                    "complete_provider": sum(
                        node.model_provider is not None for node in calls
                    ),
                    "complete_requested_model": sum(
                        node.requested_model is not None for node in calls
                    ),
                    "complete_tokens": sum(
                        node.tokens is not None
                        and node.tokens.input_tokens is not None
                        and node.tokens.output_tokens is not None
                        for node in calls
                    ),
                    "total": len(calls),
                },
                "Coverage of LLM model, provider, and token metadata.",
            ),
        ]
    )
    return results


def model_policy(
    session: SessionView,
    allowed_models: list[str] | None = None,
    allowed_providers: list[str] | None = None,
    require_requested_model_match: bool = False,
) -> list[EvaluationResult]:
    """Apply exact model and provider policies to recorded LLM calls."""
    models = _validate_string_list(allowed_models, "allowed_models")
    providers = _validate_string_list(allowed_providers, "allowed_providers")
    if not isinstance(require_requested_model_match, bool):
        raise ValueError("require_requested_model_match must be a boolean")
    if models is None and providers is None and not require_requested_model_match:
        raise ValueError("at least one model policy rule is required")
    results: list[EvaluationResult] = []
    calls = _llm_calls(session)
    terminal = _is_terminal(session)
    can_pass = terminal and bool(calls)
    if models is not None:
        violations = [
            {"index": node.index, "model": node.model}
            for node in calls
            if node.model is not None and node.model not in models
        ]
        complete = can_pass and all(node.model is not None for node in calls)
        results.append(
            _json_result(
                "allowed_models",
                {"allowed": models, "violations": violations},
                "Allowed exact served model names.",
                passed=False if violations else True if complete else None,
            )
        )
    if providers is not None:
        violations = [
            {"index": node.index, "provider": node.model_provider}
            for node in calls
            if node.model_provider is not None and node.model_provider not in providers
        ]
        complete = can_pass and all(node.model_provider is not None for node in calls)
        results.append(
            _json_result(
                "allowed_providers",
                {"allowed": providers, "violations": violations},
                "Allowed exact provider names.",
                passed=False if violations else True if complete else None,
            )
        )
    if require_requested_model_match:
        mismatches = [
            {
                "index": node.index,
                "requested": node.requested_model,
                "served": node.model,
            }
            for node in calls
            if node.requested_model is not None
            and node.model is not None
            and node.requested_model != node.model
        ]
        complete = can_pass and all(
            node.requested_model is not None and node.model is not None
            for node in calls
        )
        results.append(
            _json_result(
                "requested_model_match",
                {"mismatches": mismatches},
                "Exact equality of requested and served model names.",
                passed=False if mismatches else True if complete else None,
            )
        )
    return results


def _is_subsequence(expected: list[str], observed: list[str]) -> bool:
    """Return whether expected occurs in observed in order."""
    position = 0
    for name in observed:
        if position < len(expected) and name == expected[position]:
            position += 1
    return position == len(expected)


def workflow_conformance(
    session: SessionView,
    expected_tools: list[str],
    mode: str = "exact_order",
) -> list[EvaluationResult]:
    """Compare the recorded tool-name sequence under an exact match mode."""
    if (
        not isinstance(expected_tools, list)
        or not expected_tools
        or any(not isinstance(item, str) or not item for item in expected_tools)
    ):
        raise ValueError("expected_tools must be a non-empty list of non-empty strings")
    if mode not in _WORKFLOW_MODES:
        raise ValueError(f"mode must be one of {sorted(_WORKFLOW_MODES)}")
    results: list[EvaluationResult] = []
    calls = _tool_calls(session)
    observed, name_coverage = _get_tool_name_coverage(calls)
    matched = False
    if mode == "exact_order":
        matched = observed == expected_tools
    elif mode == "in_order":
        matched = _is_subsequence(expected_tools, observed)
    elif mode == "contains_all":
        matched = set(expected_tools) <= set(observed)
    else:
        matched = set(expected_tools) == set(observed)
    name_coverage_complete = len(observed) == len(calls)
    complete = _is_terminal(session) and name_coverage_complete
    passed: bool | None = matched if complete else None
    if not complete and name_coverage_complete:
        if mode == "exact_order":
            is_valid_prefix = observed == expected_tools[: len(observed)]
            if len(observed) > len(expected_tools) or not is_valid_prefix:
                passed = False
        elif mode == "exact_set" and not set(observed) <= set(expected_tools):
            passed = False
    results.extend(
        [
            name_coverage,
            _json_result(
                "workflow_match",
                {"expected": expected_tools, "mode": mode, "observed": observed},
                "Exact recorded tool sequence comparison under the selected mode.",
                passed=passed,
            ),
        ]
    )
    return results
