#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Configured numeric predicates over recorded tool calls."""

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal, localcontext
from typing import Any

import pytest

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session import (
    SessionDetailResponse,
    SessionOrigin,
    SessionStatus,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
)
from kitaru.task.evaluator import SessionView, call_evaluator
from kitaru.task.plugins import load_source_ref
from kitaru_evaluator import deterministic as evaluators

NOW = datetime(2026, 11, 2, tzinfo=UTC)
SESSION_ID = uuid.UUID(int=1)


def _call(
    ordinal: int = 0,
    *,
    tool_name: str | None = "issue_refund",
    inputs: Any = None,
    outputs: Any = None,
    status: NodeStatus = NodeStatus.COMPLETED,
    error: str | None = None,
    node_type: NodeType = NodeType.TOOL_CALL,
) -> SessionNodeResponse:
    return SessionNodeResponse(
        id=uuid.UUID(int=ordinal + 100),
        session_id=SESSION_ID,
        external_id=f"call-{ordinal}",
        node_type=node_type,
        name=tool_name or "unnamed",
        status=status,
        error=error,
        links=[],
        inputs=inputs,
        outputs=outputs,
        tool_name=tool_name,
        metadata={},
    )


def _view(
    calls: list[SessionNodeResponse],
    status: SessionStatus = SessionStatus.COMPLETED,
) -> SessionView:
    return SessionView(
        session=SessionDetailResponse(
            id=SESSION_ID,
            owner_id=uuid.UUID(int=2),
            agent_id=uuid.UUID(int=3),
            number=1,
            origin=SessionOrigin.IMPORTED,
            status=status,
            inputs={},
            outputs={},
            llm_call_count=sum(call.node_type is NodeType.LLM_CALL for call in calls),
            tool_call_count=sum(call.node_type is NodeType.TOOL_CALL for call in calls),
            metadata={},
            created=NOW,
            updated=NOW,
        ),
        nodes=calls,
    )


def _rule(**overrides: Any) -> dict[str, Any]:
    return {
        "name": "refund_ceiling",
        "tool_name": "issue_refund",
        "source": "outputs",
        "path": "/amount",
        "operator": "lte",
        "limit": "200.00",
        **overrides,
    }


def _evaluate(
    calls: list[SessionNodeResponse],
    rule: dict[str, Any] | None = None,
    *,
    status: SessionStatus = SessionStatus.COMPLETED,
) -> EvaluationResult:
    results = evaluators.tool_policy(
        _view(calls, status), numeric_rules=[_rule() if rule is None else rule]
    )
    return next(result for result in results if result.name.startswith("numeric_rule."))


def _payload(result: EvaluationResult) -> dict[str, Any]:
    assert result.value is not None
    return json.loads(result.value)


def test_accepted_refund_above_approval_limit_fails() -> None:
    rule = _rule(when={"source": "outputs", "path": "/accepted", "equals": True})
    result = _evaluate([_call(outputs={"accepted": True, "amount": "280.00"})], rule)

    assert result.name == "numeric_rule.refund_ceiling"
    assert result.passed is False
    assert _payload(result)["violation_count"] == 1


@pytest.mark.parametrize(
    ("operator", "expected"),
    [
        ("lt", [True, False, False]),
        ("lte", [True, True, False]),
        ("gt", [False, False, True]),
        ("gte", [False, True, True]),
    ],
)
def test_comparison_boundaries(operator: str, expected: list[bool]) -> None:
    observed = ["199.99", "200.00", "200.01"]
    actual = [
        _evaluate([_call(outputs={"amount": amount})], _rule(operator=operator)).passed
        for amount in observed
    ]
    assert actual == expected


@pytest.mark.parametrize(
    "amount", [200, 200.0, "200", "200.000", "+2.0e2", Decimal("200")]
)
@pytest.mark.parametrize("limit", [200, 200.0, "200.00"])
def test_numeric_encodings_compare_equally(amount: Any, limit: Any) -> None:
    result = _evaluate([_call(outputs={"amount": amount})], _rule(limit=limit))
    assert result.passed is True
    assert _payload(result)["checked_calls"] == 1


def test_decimal_comparison_preserves_digits_beyond_float_precision() -> None:
    with localcontext() as context:
        context.prec = 2
        result = _evaluate(
            [_call(outputs={"amount": "200.0000000000000000000000000001"})]
        )
    assert result.passed is False
    assert "200.0000000000000000000000000001" in (result.value or "")


def test_extreme_exponents_do_not_expand_result_evidence() -> None:
    result = _evaluate(
        [_call(outputs={"amount": "1e1000000000"})],
        _rule(limit="1e1000000001"),
    )
    assert result.passed is True
    assert len((result.value or "").encode()) < 2_000


def test_negative_values_need_an_explicit_lower_bound() -> None:
    call = _call(outputs={"amount": "-0.5"})
    assert _evaluate([call]).passed is True
    assert _evaluate([call], _rule(operator="gt", limit="0")).passed is False


@pytest.mark.parametrize(
    "amount",
    [
        None,
        True,
        False,
        [],
        {},
        "",
        " ",
        " 200",
        "200\n",
        "$200",
        "1,000",
        "2_00",
        "٢٠٠",
        "NaN",
        "Infinity",
        float("nan"),
        float("inf"),
        Decimal("NaN"),
        "1" * 1_025,
    ],
)
def test_unusable_numeric_evidence_holds(amount: Any) -> None:
    result = _evaluate([_call(outputs={"amount": amount})])
    assert result.passed is None
    assert _payload(result)["unavailable_calls"] == 1


@pytest.mark.parametrize("outputs", [None, {}, {"different": 100}, '{"amount":100}'])
def test_absent_payload_or_pointer_is_unavailable(outputs: Any) -> None:
    result = _evaluate([_call(outputs=outputs)])
    assert result.passed is None
    assert _payload(result)["checked_calls"] == 0


@pytest.mark.parametrize(
    ("outputs", "path"),
    [
        ("200.00", ""),
        ({"a/b": {"~amount": "200.00"}}, "/a~1b/~0amount"),
        ({"receipts": [{"amount": "200.00"}]}, "/receipts/0/amount"),
        ({"": "200.00"}, "/"),
    ],
)
def test_json_pointers_select_root_escaped_keys_and_array_items(
    outputs: Any, path: str
) -> None:
    assert _evaluate([_call(outputs=outputs)], _rule(path=path)).passed is True


@pytest.mark.parametrize(
    "path",
    [
        "/amount/0",
        "/receipts/01/amount",
        "/receipts/-/amount",
        "/receipts/" + "9" * 5_000 + "/amount",
    ],
)
def test_unresolvable_valid_pointers_hold(path: str) -> None:
    result = _evaluate(
        [_call(outputs={"amount": "200", "receipts": [{"amount": "200"}]})],
        _rule(path=path),
    )
    assert result.passed is None


@pytest.mark.parametrize("status", [SessionStatus.COMPLETED, SessionStatus.IN_PROGRESS])
def test_repeated_calls_keep_early_violation_despite_later_safe_and_missing_values(
    status: SessionStatus,
) -> None:
    result = _evaluate(
        [
            _call(0, outputs={"amount": "280"}),
            _call(1, outputs={"amount": "50"}),
            _call(2, outputs={}),
        ],
        status=status,
    )
    assert result.passed is False
    payload = _payload(result)
    assert payload["matched_calls"] == 3
    assert payload["checked_calls"] == 2
    assert payload["unavailable_calls"] == 1
    assert payload["violation_count"] == 1


def test_rejected_refund_is_excluded_from_accepted_only_rule() -> None:
    rule = _rule(when={"source": "outputs", "path": "/accepted", "equals": True})
    result = _evaluate([_call(outputs={"accepted": False})], rule)
    assert result.passed is True
    payload = _payload(result)
    assert payload["matched_calls"] == 1
    assert payload["checked_calls"] == 0
    assert payload["excluded_calls"] == 1
    assert payload["unavailable_calls"] == 0


def test_attempted_refund_uses_inputs_even_when_tool_rejects_or_fails() -> None:
    result = _evaluate(
        [
            _call(
                inputs={"amount": "280"},
                outputs={"accepted": False},
                status=NodeStatus.FAILED,
                error="Refund was rejected",
            )
        ],
        _rule(source="inputs"),
    )
    assert result.passed is False


@pytest.mark.parametrize("accepted", [None, 1, 0, "true", "false", {}, []])
def test_non_boolean_acceptance_is_unknown(accepted: Any) -> None:
    rule = _rule(when={"source": "outputs", "path": "/accepted", "equals": True})
    result = _evaluate([_call(outputs={"accepted": accepted, "amount": "280"})], rule)
    assert result.passed is None
    assert _payload(result)["unavailable_calls"] == 1


def test_missing_acceptance_is_not_a_rejected_action() -> None:
    rule = _rule(when={"source": "outputs", "path": "/accepted", "equals": True})
    assert _evaluate([_call(outputs={"amount": "280"})], rule).passed is None


def test_false_boolean_condition_and_input_selector_are_supported() -> None:
    rule = _rule(when={"source": "inputs", "path": "/human_approved", "equals": False})
    result = _evaluate(
        [_call(inputs={"human_approved": False}, outputs={"amount": "280"})], rule
    )
    assert result.passed is False


def test_condition_reads_the_same_call_instead_of_another_call_result() -> None:
    rule = _rule(when={"source": "outputs", "path": "/accepted", "equals": True})
    result = _evaluate(
        [
            _call(0, outputs={"accepted": True, "amount": "100"}),
            _call(1, outputs={"amount": "280"}),
        ],
        rule,
    )
    assert result.passed is None
    assert _payload(result)["checked_calls"] == 1


@pytest.mark.parametrize("status", [NodeStatus.IN_PROGRESS, NodeStatus.FAILED])
@pytest.mark.parametrize("use_condition", [False, True])
def test_unfinished_or_failed_call_output_cannot_prove_acceptance_or_amount(
    status: NodeStatus, use_condition: bool
) -> None:
    rule = _rule()
    if use_condition:
        rule["when"] = {"source": "outputs", "path": "/accepted", "equals": True}
    result = _evaluate(
        [_call(outputs={"accepted": True, "amount": "280"}, status=status)], rule
    )
    assert result.passed is None


def test_completed_output_with_execution_error_is_unknown() -> None:
    result = _evaluate([_call(outputs={"amount": "280"}, error="partial result")])
    assert result.passed is None


@pytest.mark.parametrize("status", [SessionStatus.COMPLETED, SessionStatus.FAILED])
def test_safe_complete_numeric_evidence_can_pass_terminal_sessions(
    status: SessionStatus,
) -> None:
    assert _evaluate([_call(outputs={"amount": "100"})], status=status).passed is True


def test_nonterminal_session_holds_safe_or_explicitly_excluded_calls() -> None:
    assert (
        _evaluate(
            [_call(outputs={"amount": "100"})], status=SessionStatus.IN_PROGRESS
        ).passed
        is None
    )
    rule = _rule(when={"source": "outputs", "path": "/accepted", "equals": True})
    assert (
        _evaluate(
            [_call(outputs={"accepted": False})], rule, status=SessionStatus.IN_PROGRESS
        ).passed
        is None
    )


@pytest.mark.parametrize(
    "calls",
    [
        [],
        [_call(tool_name="Issue_Refund", outputs={"amount": "280"})],
        [_call(tool_name="other", outputs={"amount": "280"})],
        [_call(node_type=NodeType.SPAN, outputs={"amount": "280"})],
    ],
)
def test_no_matching_exact_tool_calls_holds(calls: list[SessionNodeResponse]) -> None:
    result = _evaluate(calls)
    assert result.passed is None
    assert _payload(result)["matched_calls"] == 0


def test_unnamed_call_can_hide_another_matching_tool_and_prevents_pass() -> None:
    result = _evaluate(
        [_call(0, outputs={"amount": "100"}), _call(1, tool_name=None, outputs={})]
    )
    assert result.passed is None


def test_evidence_is_bounded_after_all_calls_determine_verdict() -> None:
    result = _evaluate([_call(index, outputs={"amount": "280"}) for index in range(35)])
    payload = _payload(result)
    assert result.passed is False
    assert payload["matched_calls"] == 35
    assert payload["violation_count"] == 35
    assert len(payload["evidence"]) <= 20
    assert payload["truncated"] is True
    assert len((result.value or "").encode()) <= 64_000


@pytest.mark.parametrize(
    "overrides",
    [
        {"name": ""},
        {"name": "bad.name"},
        {"name": "x" * 65},
        {"tool_name": ""},
        {"tool_name": 1},
        {"source": "attributes"},
        {"path": "amount"},
        {"path": "/a~2b"},
        {"path": 1},
        {"operator": "eq"},
        {"limit": None},
        {"limit": True},
        {"limit": "NaN"},
        {"limit": float("inf")},
        {"limit": " 200"},
        {"limit": "2_00"},
        {"limit": "1" * 1_025},
        {"limit": {}},
        {"unexpected": 1},
        {"when": {}},
        {"when": {"source": "outputs", "path": "/accepted", "equals": 1}},
        {"when": {"source": "outputs", "path": "/accepted", "equals": "true"}},
        {
            "when": {
                "source": "outputs",
                "path": "/accepted",
                "equals": True,
                "extra": 0,
            }
        },
        {"when": {"source": "outputs", "path": "accepted", "equals": True}},
    ],
)
def test_invalid_rule_configuration_raises(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        _evaluate([_call(outputs={"amount": "100"})], _rule(**overrides))


@pytest.mark.parametrize("rules", [[], {}, "rules", [1], [_rule(), _rule()]])
def test_invalid_rule_collections_raise(rules: Any) -> None:
    with pytest.raises(ValueError):
        evaluators.tool_policy(_view([]), numeric_rules=rules)


def test_multiple_rules_and_legacy_tool_rules_keep_independent_results() -> None:
    results = evaluators.tool_policy(
        _view([_call(outputs={"amount": "-1"})]),
        required_tools=["issue_refund"],
        numeric_rules=[
            _rule(),
            _rule(name="positive_amount", operator="gt", limit="0"),
        ],
    )
    by_name = {result.name: result for result in results}
    assert list(by_name) == [
        "tool_name_coverage",
        "required_tools",
        "numeric_rule.refund_ceiling",
        "numeric_rule.positive_amount",
    ]
    assert by_name["required_tools"].passed is True
    assert by_name["numeric_rule.refund_ceiling"].passed is True
    assert by_name["numeric_rule.positive_amount"].passed is False


def test_omitted_numeric_rules_preserve_legacy_results() -> None:
    view = _view([_call()])
    params: dict[str, Any] = {
        "required_tools": ["issue_refund"],
        "forbidden_tools": ["delete_account"],
        "max_calls_per_tool": {"issue_refund": 1},
    }
    implicit = evaluators.tool_policy(view, **params)
    explicit = evaluators.tool_policy(view, numeric_rules=None, **params)
    assert [result.model_dump() for result in implicit] == [
        result.model_dump() for result in explicit
    ]
    assert [result.name for result in implicit] == [
        "tool_name_coverage",
        "required_tools",
        "forbidden_tools",
        "per_tool_maximums",
    ]


@pytest.mark.parametrize(
    ("amount", "passed"), [("150", True), ("280", False), (None, None)]
)
async def test_package_entrypoint_receives_parameters_and_serializes_verdicts(
    amount: Any, passed: bool | None
) -> None:
    entrypoint = load_source_ref(
        "kitaru_evaluator.deterministic:tool_policy", "Evaluator"
    )
    results = await call_evaluator(
        "kitaru/tool-policy",
        entrypoint,
        _view([_call(outputs={"amount": amount})]),
        {"numeric_rules": [_rule()]},
    )
    result = next(
        item for item in results if item.name == "numeric_rule.refund_ceiling"
    )
    restored = EvaluationResult.model_validate_json(result.model_dump_json())
    assert restored.passed is passed
    assert _payload(restored)["matched_calls"] == 1
