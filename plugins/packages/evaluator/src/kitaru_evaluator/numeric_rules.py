#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Fixed numeric policies over recorded tool payloads."""

import re
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType, SessionNodeResponse
from kitaru.task.evaluator import SessionView
from kitaru_evaluator.evidence import (
    create_json_result,
    decode_pointer_parts,
    resolve_pointer,
)

_DECIMAL_PATTERN = re.compile(
    r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?"
)
_NUMBER_MAX_CHARACTERS = 1024
_EVIDENCE_LIMIT = 20
_Source = Literal["inputs", "outputs"]
_Operator = Literal["lt", "lte", "gt", "gte"]


def _parse_number(value: Any) -> Decimal:
    """Parse bounded finite numeric evidence without rounding or coercing bools."""
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise ValueError("value must be a finite number or decimal string")
    text = str(value)
    if len(text) > _NUMBER_MAX_CHARACTERS or not _DECIMAL_PATTERN.fullmatch(text):
        raise ValueError(
            "numeric text must be an ASCII decimal of at most 1024 characters"
        )
    try:
        number = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError("value must be a finite number") from exc
    if not number.is_finite():
        raise ValueError("value must be finite")
    return number


class _Condition(BaseModel):
    """Strict Boolean applicability condition on the same recorded tool call."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    source: _Source
    path: str
    equals: bool

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        decode_pointer_parts(value)
        return value


class _Rule(BaseModel):
    """Validated fixed comparison over one named tool's recorded payload."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    name: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_-]*$", max_length=64)
    tool_name: str = Field(min_length=1)
    source: _Source
    path: str
    operator: _Operator
    limit: Decimal
    when: _Condition | None = None

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        decode_pointer_parts(value)
        return value

    @field_validator("limit", mode="before")
    @classmethod
    def _validate_limit(cls, value: Any) -> Decimal:
        return _parse_number(value)


def _read_value(
    node: SessionNodeResponse, source: _Source, path: str
) -> tuple[str | None, Any]:
    """Read a payload value only when its recorded execution supports it."""
    if source == "outputs" and (
        node.status is not NodeStatus.COMPLETED or node.error is not None
    ):
        return "unavailable_outputs", None
    payload = node.inputs if source == "inputs" else node.outputs
    found, value = resolve_pointer(payload, path)
    return (None, value) if found else ("missing_path", None)


def _matches_comparison(value: Decimal, operator: _Operator, limit: Decimal) -> bool:
    """Compare exact finite decimals without consulting the rounding context."""
    match operator:
        case "lt":
            return value < limit
        case "lte":
            return value <= limit
        case "gt":
            return value > limit
        case "gte":
            return value >= limit


def _evaluate_rule(
    session: SessionView,
    calls: list[tuple[int, SessionNodeResponse]],
    unknown_tool_names: int,
    rule: _Rule,
) -> EvaluationResult:
    """Check every matching call with decisive failure and conservative passes."""
    matched = checked = excluded = unavailable = violations = 0
    evidence: list[dict[str, Any]] = []
    findings = 0
    for index, node in calls:
        if node.tool_name != rule.tool_name:
            continue
        matched += 1
        reason: str | None = None
        observed: Decimal | None = None
        if rule.when is not None:
            reason, condition = _read_value(node, rule.when.source, rule.when.path)
            if reason is None and not isinstance(condition, bool):
                reason = "invalid_condition"
            if reason is None and condition is not rule.when.equals:
                excluded += 1
                continue
        if reason is None:
            reason, value = _read_value(node, rule.source, rule.path)
            if reason is None:
                try:
                    observed = _parse_number(value)
                except ValueError:
                    reason = "invalid_number"
        if reason is not None:
            unavailable += 1
        else:
            assert observed is not None
            checked += 1
            if _matches_comparison(observed, rule.operator, rule.limit):
                continue
            violations += 1
            reason = "violation"
        findings += 1
        if len(evidence) < _EVIDENCE_LIMIT:
            finding: dict[str, Any] = {
                "index": index,
                "node_id": str(node.id),
                "reason": reason,
            }
            if observed is not None:
                finding["observed"] = str(observed)
            evidence.append(finding)
    terminal = session.session.status in {SessionStatus.COMPLETED, SessionStatus.FAILED}
    passed: bool | None
    if violations:
        passed = False
        explanation = "At least one applicable recorded call violates the numeric rule."
    elif matched and terminal and not unavailable and not unknown_tool_names:
        passed = True
        explanation = (
            "Every applicable recorded call satisfies the numeric rule."
            if checked
            else "All matching calls were explicitly excluded by the Boolean condition."
        )
    else:
        passed = None
        explanation = (
            "No matching tool calls were recorded."
            if not matched
            else "The session or required tool evidence is incomplete."
        )
    return create_json_result(
        f"numeric_rule.{rule.name}",
        {
            "rule": rule.model_dump(mode="json"),
            "matched_calls": matched,
            "checked_calls": checked,
            "excluded_calls": excluded,
            "unavailable_calls": unavailable,
            "unknown_tool_names": unknown_tool_names,
            "violation_count": violations,
            "evidence": evidence,
            "total": findings,
            "truncated": findings > len(evidence),
        },
        explanation,
        passed=passed,
    )


def evaluate_numeric_rules(
    session: SessionView, numeric_rules: list[dict[str, Any]]
) -> list[EvaluationResult]:
    """Validate configured numeric rules and evaluate their recorded evidence.

    Args:
        session: Materialized session with recorded node payloads.
        numeric_rules: Nonempty collection of uniquely named fixed comparisons.

    Raises:
        ValueError: Configuration contains an invalid or duplicate rule.

    Returns:
        One pass, fail, or inconclusive structured result per named rule.
    """
    if not isinstance(numeric_rules, list) or not numeric_rules:
        raise ValueError("numeric_rules must be a non-empty list")
    rules = [_Rule.model_validate(item) for item in numeric_rules]
    names = [rule.name for rule in rules]
    if len(set(names)) != len(names):
        raise ValueError("numeric_rules contains duplicate rule names")
    calls = [
        (index, node)
        for index, node in enumerate(session.nodes)
        if node.node_type is NodeType.TOOL_CALL
    ]
    unknown_tool_names = sum(node.tool_name is None for _, node in calls)
    return [_evaluate_rule(session, calls, unknown_tool_names, rule) for rule in rules]
