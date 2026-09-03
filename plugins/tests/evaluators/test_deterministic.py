#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Tests for the repository deterministic evaluator bundles."""

import inspect
import json
import math
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext
from typing import Any

import pytest

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session import (
    SessionDetailResponse,
    SessionOrigin,
    SessionStatus,
    TokenUsage,
)
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
)
from kitaru.task.evaluator import SessionView
from kitaru.task.plugins import load_source_ref
from kitaru_evaluator import deterministic as evaluators

NOW = datetime(2026, 8, 6, 12, tzinfo=UTC)
SESSION_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
_DEFAULT_OUTPUT = object()


def _node(
    index: int,
    *,
    node_type: NodeType = NodeType.TOOL_CALL,
    status: NodeStatus = NodeStatus.COMPLETED,
    parent_index: int | None = None,
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
    inputs: Any = None,
    outputs: Any = None,
    error: str | None = None,
    tool_name: str | None = None,
    requested_model: str | None = None,
    model: str | None = None,
    model_provider: str | None = None,
    tokens: TokenUsage | None = None,
    cost: Decimal | None = None,
) -> SessionNodeResponse:
    """Build a deterministic session node fixture."""
    return SessionNodeResponse(
        id=uuid.UUID(int=index + 100),
        session_id=SESSION_ID,
        index=index,
        parent_index=parent_index,
        parent_id=(
            uuid.UUID(int=parent_index + 100) if parent_index is not None else None
        ),
        secondary_parent_indexes=[],
        secondary_parent_ids=[],
        node_type=node_type,
        name=tool_name or model or f"node-{index}",
        status=status,
        error=error,
        started_at=started_at,
        ended_at=ended_at,
        inputs=inputs,
        outputs=outputs,
        requested_model=requested_model,
        model=model,
        model_provider=model_provider,
        tokens=tokens,
        cost=cost,
        tool_name=tool_name,
        metadata={},
    )


def _view(
    nodes: list[SessionNodeResponse] | None = None,
    *,
    status: SessionStatus = SessionStatus.COMPLETED,
    outputs: Any = _DEFAULT_OUTPUT,
    started_at: datetime | None = NOW,
    ended_at: datetime | None = NOW + timedelta(seconds=10),
    cost: Decimal | None = None,
    tokens: TokenUsage | None = None,
    llm_call_count: int | None = None,
    tool_call_count: int | None = None,
) -> SessionView:
    """Build a deterministic materialized session view."""
    materialized_nodes = nodes or []
    if outputs is _DEFAULT_OUTPUT:
        outputs = {"answer": 42}
    llm_count = sum(n.node_type is NodeType.LLM_CALL for n in materialized_nodes)
    tool_count = sum(n.node_type is NodeType.TOOL_CALL for n in materialized_nodes)
    session = SessionDetailResponse(
        id=SESSION_ID,
        owner_id=uuid.UUID(int=2),
        agent_id=uuid.UUID(int=3),
        number=1,
        origin=SessionOrigin.IMPORTED,
        status=status,
        inputs={"prompt": "hello"},
        outputs=outputs,
        metadata={},
        imported_from="fixture",
        started_at=started_at,
        ended_at=ended_at,
        cost=cost,
        tokens=tokens,
        llm_call_count=llm_count if llm_call_count is None else llm_call_count,
        tool_call_count=tool_count if tool_call_count is None else tool_call_count,
        created=NOW,
        updated=NOW,
    )
    return SessionView(session=session, nodes=materialized_nodes)


def _by_name(results: list[EvaluationResult]) -> dict[str, EvaluationResult]:
    """Index results by their stable leaf name."""
    return {result.name: result for result in results}


def _payload(result: EvaluationResult) -> Any:
    """Decode a structured result value."""
    assert result.value is not None
    return json.loads(result.value)


def test_public_entrypoints_have_exact_signatures() -> None:
    """Keep all ten callables discoverable and reject unknown keywords."""
    names = [
        "session_diagnostics",
        "output_contract",
        "trajectory_signals",
        "tool_health",
        "timing_profile",
        "resource_budget",
        "tool_policy",
        "llm_call_signals",
        "model_policy",
        "workflow_conformance",
    ]
    for name in names:
        function = getattr(evaluators, name)
        assert not any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in inspect.signature(function).parameters.values()
        )
        with pytest.raises(TypeError, match="unexpected keyword"):
            function(_view(), unknown=True)


def test_entrypoint_loads_through_package_plugin_contract() -> None:
    """Load a deterministic evaluator through its package source reference."""
    entrypoint = load_source_ref(
        "kitaru_evaluator.deterministic:session_diagnostics", "Evaluator"
    )

    first = entrypoint(_view())
    repeated = entrypoint(_view())

    assert [result.model_dump(mode="json") for result in first] == [
        result.model_dump(mode="json") for result in repeated
    ]
    assert {result.name for result in first} >= {"terminality"}
    assert {result.name for result in first}.isdisjoint(
        {"input_sha256", "config_sha256"}
    )


def test_ordered_result_name_contracts() -> None:
    """Freeze the ordered leaf names emitted by every deterministic bundle."""
    view = _view()
    calls = {
        "session_diagnostics": evaluators.session_diagnostics(view),
        "output_contract": evaluators.output_contract(
            view,
            expected={"answer": 42},
            required_paths=["/answer"],
            type_requirements={"/answer": "integer"},
        ),
        "trajectory_signals": evaluators.trajectory_signals(view),
        "tool_health": evaluators.tool_health(view),
        "timing_profile": evaluators.timing_profile(view),
        "resource_budget": evaluators.resource_budget(
            view,
            max_duration_seconds=10,
            max_cost=0,
            max_total_tokens=0,
            max_nodes=0,
            max_llm_calls=0,
            max_tool_calls=0,
        ),
        "tool_policy": evaluators.tool_policy(
            view,
            required_tools=["search"],
            forbidden_tools=["blocked"],
            max_calls_per_tool={"search": 1},
        ),
        "llm_call_signals": evaluators.llm_call_signals(view),
        "model_policy": evaluators.model_policy(
            view,
            allowed_models=["model"],
            allowed_providers=["provider"],
            require_requested_model_match=True,
        ),
        "workflow_conformance": evaluators.workflow_conformance(
            view, expected_tools=["search"]
        ),
    }
    expected = {
        "session_diagnostics": [
            "terminality",
            "node_order",
            "parent_linkage",
            "chronology_findings",
            "payload_coverage",
            "recorded_counts",
            "duration_seconds",
            "cost_coverage",
            "token_coverage",
            "resource_integrity",
        ],
        "output_contract": [
            "output_availability",
            "exact_output",
            "required_paths",
            "type_requirements",
        ],
        "trajectory_signals": [
            "tool_identity_coverage",
            "adjacent_identical_calls",
            "failed_identical_retries",
            "short_cycles",
            "cycle_detector_bounds",
        ],
        "tool_health": [
            "failed_calls",
            "null_results",
            "empty_results",
            "error_status_inconsistencies",
            "adjacent_repeated_failures",
        ],
        "timing_profile": [
            "wall_clock_duration_seconds",
            "node_duration_coverage",
            "slowest_nodes",
            "overlapping_intervals",
            "invalid_intervals",
        ],
        "resource_budget": [
            "duration_budget",
            "cost_budget",
            "total_tokens_budget",
            "node_count_budget",
            "llm_call_count_budget",
            "tool_call_count_budget",
        ],
        "tool_policy": [
            "tool_name_coverage",
            "required_tools",
            "forbidden_tools",
            "per_tool_maximums",
        ],
        "llm_call_signals": [
            "failed_calls",
            "empty_results",
            "adjacent_identical_inputs",
            "requested_model_mismatches",
            "metadata_coverage",
        ],
        "model_policy": [
            "allowed_models",
            "allowed_providers",
            "requested_model_match",
        ],
        "workflow_conformance": [
            "tool_name_coverage",
            "workflow_match",
        ],
    }
    assert {
        name: [result.name for result in results] for name, results in calls.items()
    } == expected
    structured_or_resource_names = {
        "cycle_detector_bounds",
        "duration_seconds",
        "recorded_counts",
        "slowest_nodes",
        "wall_clock_duration_seconds",
    }
    for results in calls.values():
        for result in results:
            if result.name in structured_or_resource_names:
                continue
            assert result.value is None
            assert isinstance(result.score, float)
            assert result.min_score == 0


def test_decimal_encoding_ignores_ambient_precision() -> None:
    """Keep numeric receipts stable under a worker's decimal context."""
    view = _view(cost=Decimal("1.23456789"))
    ordinary = evaluators.session_diagnostics(view)
    with localcontext() as context:
        context.prec = 4
        constrained = evaluators.session_diagnostics(view)
    assert [result.model_dump_json() for result in ordinary] == [
        result.model_dump_json() for result in constrained
    ]


def test_session_diagnostics_distinguishes_missing_and_zero_resources() -> None:
    """Do not coerce unavailable resource evidence to zero."""
    missing = _by_name(
        evaluators.session_diagnostics(
            _view(started_at=None, ended_at=None, cost=None, tokens=None)
        )
    )
    zero = _by_name(
        evaluators.session_diagnostics(
            _view(
                started_at=NOW,
                ended_at=NOW,
                cost=Decimal("0"),
                tokens=TokenUsage(input_tokens=0, output_tokens=0),
            )
        )
    )
    assert missing["duration_seconds"].value == "unavailable"
    assert zero["duration_seconds"].value == "0"
    assert missing["cost_coverage"].score == 0
    assert missing["cost_coverage"].max_score == 1
    assert zero["cost_coverage"].score == 1
    assert zero["cost_coverage"].max_score == 1
    assert missing["token_coverage"].score == 0
    assert zero["token_coverage"].score == 1
    assert all(result.passed is None for result in missing.values())


def test_session_diagnostics_localizes_integrity_findings() -> None:
    """Report supplied ordering, invalid parents, chronology, and payload gaps."""
    nodes = [
        _node(
            2,
            parent_index=9,
            tool_name="later",
            started_at=NOW + timedelta(seconds=3),
            ended_at=NOW + timedelta(seconds=2),
        ),
        _node(1, tool_name="earlier", inputs={}, outputs={}),
    ]
    results = _by_name(evaluators.session_diagnostics(_view(nodes)))
    assert results["node_order"].score == 0
    assert results["parent_linkage"].score == 1
    assert results["parent_linkage"].max_score == 2
    assert results["chronology_findings"].score == 1
    assert results["payload_coverage"].score == 1
    assert results["payload_coverage"].max_score == 2


def test_session_diagnostics_checks_parent_ids_and_invalid_resources() -> None:
    """Localize inconsistent linkage and malformed resource evidence."""
    parent = _node(0, outputs={})
    child = _node(1, parent_index=0, outputs={}).model_copy(
        update={"parent_id": uuid.UUID(int=999), "cost": Decimal("-1")}
    )
    results = _by_name(
        evaluators.session_diagnostics(_view([parent, child], cost=Decimal("-1")))
    )
    assert results["parent_linkage"].score == 1
    assert results["resource_integrity"].score == 2
    assert results["resource_integrity"].max_score is None


def test_output_contract_handles_exact_pointer_and_type_rules() -> None:
    """Apply exact JSON, RFC 6901 paths, and JSON type checks offline."""
    view = _view(outputs={"a/b": {"~key": [1, "two"]}, "answer": 42})
    results = _by_name(
        evaluators.output_contract(
            view,
            expected={"answer": 42, "a/b": {"~key": [1, "two"]}},
            required_paths=["/a~1b/~0key/1"],
            type_requirements={"/answer": "integer", "/a~1b/~0key": "array"},
        )
    )
    assert results["exact_output"].passed is True
    assert results["exact_output"].score == 1
    assert results["required_paths"].passed is True
    assert results["required_paths"].score == 0
    assert results["required_paths"].max_score == 1
    assert results["type_requirements"].passed is True
    assert results["type_requirements"].score == 0
    assert results["type_requirements"].max_score == 2
    unknown = _by_name(evaluators.output_contract(_view(outputs=None), expected=None))
    assert unknown["exact_output"].passed is None
    assert unknown["output_availability"].score == 0
    assert unknown["output_availability"].max_score == 1
    root = _by_name(evaluators.output_contract(view, required_paths=[""]))
    assert root["required_paths"].passed is True


def test_output_contract_scores_large_exact_result_without_echoing_it() -> None:
    """Score a large exact match without echoing it into the task result."""
    output = {"content": "x" * 600_000}
    results = evaluators.output_contract(_view(outputs=output), expected=output)
    exact = _by_name(results)["exact_output"]
    assert exact.passed is True
    assert exact.score == 1
    assert exact.max_score == 1
    assert exact.value is None
    assert len(exact.model_dump_json()) < 1_000


@pytest.mark.parametrize(
    ("observed", "expected"),
    [(True, 1), (False, 0), (1, 1.0)],
)
def test_output_contract_uses_json_encoding_equality(
    observed: Any, expected: Any
) -> None:
    """Keep distinct JSON encodings distinct during exact comparison."""
    result = _by_name(evaluators.output_contract(_view(outputs=observed), expected))[
        "exact_output"
    ]
    assert result.passed is False


@pytest.mark.parametrize(
    ("call", "match"),
    [
        (lambda: evaluators.output_contract(_view()), "at least one"),
        (
            lambda: evaluators.output_contract(
                _view(), required_paths=["/answer", "/answer"]
            ),
            "duplicate",
        ),
        (
            lambda: evaluators.output_contract(_view(), required_paths=["/a~2b"]),
            "escapes",
        ),
        (
            lambda: evaluators.output_contract(_view(), required_paths=["/missing/~2"]),
            "escapes",
        ),
        (
            lambda: evaluators.output_contract(
                _view(), type_requirements={"/missing/~2": "string"}
            ),
            "escapes",
        ),
        (
            lambda: evaluators.timing_profile(_view(), evidence_limit=True),
            "integer",
        ),
        (
            lambda: evaluators.timing_profile(_view(), evidence_limit=0),
            "between",
        ),
        (
            lambda: evaluators.timing_profile(_view(), evidence_limit=101),
            "between",
        ),
        (
            lambda: evaluators.resource_budget(_view(), max_nodes=True),
            "number",
        ),
        (
            lambda: evaluators.resource_budget(_view(), max_cost=math.inf),
            "finite",
        ),
        (
            lambda: evaluators.resource_budget(_view(), max_nodes=1.5),
            "integer",
        ),
        (
            lambda: evaluators.tool_policy(
                _view(), required_tools=["search"], forbidden_tools=["search"]
            ),
            "conflict",
        ),
        (
            lambda: evaluators.tool_policy(
                _view(),
                required_tools=["search"],
                max_calls_per_tool={"search": 0},
            ),
            "maximum of zero",
        ),
        (lambda: evaluators.model_policy(_view()), "at least one"),
        (
            lambda: evaluators.workflow_conformance(_view(), expected_tools=[]),
            "non-empty",
        ),
        (
            lambda: evaluators.workflow_conformance(
                _view(), expected_tools=["search"], mode="unknown"
            ),
            "mode",
        ),
    ],
)
def test_config_validation_rejects_ambiguous_policies(call: Any, match: str) -> None:
    """Reject invalid policy configuration instead of producing a verdict."""
    with pytest.raises(ValueError, match=match):
        call()


def test_trajectory_signals_separate_repeats_retries_and_cycles() -> None:
    """Detect exact adjacent calls, failed retries, and bounded short cycles."""
    nodes = [
        _node(
            0,
            tool_name="a",
            inputs={"x": 1, "y": 2},
            outputs={},
            status=NodeStatus.FAILED,
        ),
        _node(1, tool_name="a", inputs={"y": 2, "x": 1}, outputs={}),
        _node(2, tool_name="b", inputs={}, outputs={}),
        _node(3, tool_name="a", inputs={"x": 1, "y": 2}, outputs={}),
        _node(4, tool_name="b", inputs={}, outputs={}),
        _node(5, tool_name="a", inputs={"y": 2, "x": 1}, outputs={}),
        _node(6, tool_name="b", inputs={}, outputs={}),
    ]
    results = _by_name(evaluators.trajectory_signals(_view(nodes)))
    assert results["adjacent_identical_calls"].score == 1
    assert results["adjacent_identical_calls"].max_score == 6
    assert results["failed_identical_retries"].score == 1
    assert results["short_cycles"].score == 1
    assert results["short_cycles"].max_score is None
    assert _payload(results["cycle_detector_bounds"]) == {
        "max_period": 5,
        "min_period": 2,
        "min_repetitions": 3,
    }


def test_trajectory_signals_do_not_cycle_distinct_calls_to_the_same_tool() -> None:
    """Do not classify different commands as a repeated tool-call cycle."""
    nodes = [
        _node(
            index,
            tool_name="command_execution",
            inputs={"command": command},
            outputs={},
        )
        for index, command in enumerate(
            ["pwd", "ls", "git status", "pytest", "ruff check", "git diff"]
        )
    ]

    cycles = _by_name(evaluators.trajectory_signals(_view(nodes)))["short_cycles"]

    assert cycles.score == 0


def test_trajectory_signals_emit_one_maximal_odd_cycle() -> None:
    """Canonicalize phase-shifted windows into one maximal cycle."""
    nodes = [
        _node(index, tool_name=name, inputs={}, outputs={})
        for index, name in enumerate(["a", "b", "a", "b", "a", "b", "a"])
    ]
    cycles = _by_name(evaluators.trajectory_signals(_view(nodes)))["short_cycles"]
    assert cycles.score == 1


def test_tool_health_preserves_null_empty_and_falsey_values() -> None:
    """Treat zero and false as data while separating null and empty results."""
    nodes = [
        _node(0, tool_name="a", status=NodeStatus.FAILED, error=None, outputs=None),
        _node(1, tool_name="a", status=NodeStatus.COMPLETED, error="boom", outputs=""),
        _node(2, tool_name="b", outputs=[]),
        _node(3, tool_name="c", outputs=0),
        _node(4, tool_name="d", outputs=False),
    ]
    results = _by_name(evaluators.tool_health(_view(nodes)))
    assert results["failed_calls"].score == 1
    assert results["failed_calls"].max_score == 5
    assert results["null_results"].score == 1
    assert results["empty_results"].score == 2
    assert results["error_status_inconsistencies"].score == 2


def test_timing_profile_reports_slow_overlapping_and_invalid_spans() -> None:
    """Describe timing evidence without corpus-relative outlier labels."""
    nodes = [
        _node(0, started_at=NOW, ended_at=NOW + timedelta(seconds=4)),
        _node(
            1,
            started_at=NOW + timedelta(seconds=1),
            ended_at=NOW + timedelta(seconds=2),
        ),
        _node(
            2,
            started_at=NOW + timedelta(seconds=5),
            ended_at=NOW + timedelta(seconds=4),
        ),
        _node(3),
    ]
    results = _by_name(evaluators.timing_profile(_view(nodes), evidence_limit=1))
    assert results["wall_clock_duration_seconds"].value == "10"
    assert results["node_duration_coverage"].score == 2
    assert results["node_duration_coverage"].max_score == 4
    assert _payload(results["slowest_nodes"])["evidence"][0]["index"] == 0
    assert results["overlapping_intervals"].score == 1
    assert results["overlapping_intervals"].max_score == 1
    assert results["invalid_intervals"].score == 1
    assert results["invalid_intervals"].max_score == 4


def test_resource_budget_reconciles_rollups_and_uses_inclusive_ceilings() -> None:
    """Pass at equality, fail on excess, and withhold pass on disagreement."""
    llm = _node(
        0,
        node_type=NodeType.LLM_CALL,
        inputs={"prompt": "x"},
        outputs={"text": "y"},
        tokens=TokenUsage(
            input_tokens=2,
            output_tokens=3,
            cached_input_tokens=99,
            reasoning_tokens=99,
        ),
        cost=Decimal("1.25"),
        model="m",
        model_provider="p",
    )
    view = _view(
        [llm], cost=Decimal("1.25"), tokens=TokenUsage(input_tokens=2, output_tokens=3)
    )
    results = _by_name(
        evaluators.resource_budget(
            view,
            max_duration_seconds=10,
            max_cost=1.25,
            max_total_tokens=5,
            max_nodes=1,
            max_llm_calls=1,
            max_tool_calls=0,
        )
    )
    assert all(
        results[name].passed is True
        for name in (
            "duration_budget",
            "cost_budget",
            "total_tokens_budget",
            "node_count_budget",
            "llm_call_count_budget",
            "tool_call_count_budget",
        )
    )
    assert results["duration_budget"].score == 10
    assert results["duration_budget"].min_score == 0
    assert results["duration_budget"].max_score == 10
    assert results["cost_budget"].score == 1.25
    assert results["cost_budget"].max_score == 1.25
    assert results["total_tokens_budget"].score == 5
    assert results["total_tokens_budget"].max_score == 5
    assert (
        _by_name(evaluators.resource_budget(view, max_total_tokens=4))[
            "total_tokens_budget"
        ].passed
        is False
    )
    mismatched = _view([llm], cost=Decimal("1.00"))
    assert (
        _by_name(evaluators.resource_budget(mismatched, max_cost=2))[
            "cost_budget"
        ].passed
        is None
    )


def test_resource_budget_does_not_require_cost_on_tool_calls() -> None:
    """Reconcile cost when tool calls structurally omit cost metadata."""
    llm = _node(
        0,
        node_type=NodeType.LLM_CALL,
        outputs={},
        cost=Decimal("1.25"),
    )
    tool = _node(1, tool_name="get_weather", outputs={})
    result = _by_name(
        evaluators.resource_budget(
            _view([llm, tool], cost=Decimal("1.25")),
            max_cost=2,
        )
    )["cost_budget"]

    assert result.passed is True


def test_resource_budget_matches_all_node_rollups_and_decimal_context() -> None:
    """Include recorded span resources and sum costs without ambient rounding."""
    llm = _node(
        0,
        node_type=NodeType.LLM_CALL,
        outputs={},
        cost=Decimal("0.123456789"),
        tokens=TokenUsage(input_tokens=2, output_tokens=3),
    )
    span = _node(
        1,
        node_type=NodeType.SPAN,
        outputs={},
        cost=Decimal("0.000000006"),
        tokens=TokenUsage(input_tokens=1, output_tokens=1),
    )
    view = _view(
        [llm, span],
        cost=Decimal("0.123456795"),
        tokens=TokenUsage(input_tokens=3, output_tokens=4),
    )
    ordinary = evaluators.resource_budget(view, max_cost=0.12348, max_total_tokens=7)
    with localcontext() as context:
        context.prec = 4
        constrained = evaluators.resource_budget(
            view, max_cost=0.12348, max_total_tokens=7
        )
    assert [result.model_dump_json() for result in ordinary] == [
        result.model_dump_json() for result in constrained
    ]
    assert _by_name(ordinary)["cost_budget"].passed is True
    assert _by_name(ordinary)["total_tokens_budget"].passed is True


def test_resource_budget_holds_on_negative_recorded_values() -> None:
    """Never turn malformed negative resource evidence into a policy pass."""
    llm = _node(
        0,
        node_type=NodeType.LLM_CALL,
        outputs={},
        cost=Decimal("-1"),
        tokens=TokenUsage(input_tokens=-1, output_tokens=0),
    )
    results = _by_name(
        evaluators.resource_budget(
            _view(
                [llm],
                cost=Decimal("-1"),
                tokens=TokenUsage(input_tokens=-1, output_tokens=0),
            ),
            max_cost=0,
            max_total_tokens=0,
        )
    )
    assert results["cost_budget"].passed is None
    assert results["total_tokens_budget"].passed is None


def test_tool_policy_uses_conservative_name_coverage() -> None:
    """Allow decisive failures but withhold passes when a tool is unnamed."""
    nodes = [
        _node(0, tool_name="blocked", outputs={}),
        _node(1, tool_name=None, outputs={}),
    ]
    results = _by_name(
        evaluators.tool_policy(
            _view(nodes),
            required_tools=["search"],
            forbidden_tools=["blocked"],
            max_calls_per_tool={"search": 1},
        )
    )
    assert results["forbidden_tools"].passed is False
    assert results["forbidden_tools"].score == 1
    assert results["forbidden_tools"].max_score == 1
    assert results["required_tools"].passed is None
    assert results["required_tools"].score == 1
    assert results["required_tools"].max_score == 1
    assert results["per_tool_maximums"].passed is None


def test_llm_signals_and_model_policy_use_recorded_metadata() -> None:
    """Report LLM call findings and enforce exact model metadata policies."""
    nodes = [
        _node(
            0,
            node_type=NodeType.LLM_CALL,
            status=NodeStatus.FAILED,
            inputs={"p": 1},
            outputs="",
            requested_model="requested",
            model="served",
            model_provider="acme",
            tokens=TokenUsage(input_tokens=1, output_tokens=0),
        ),
        _node(
            1,
            node_type=NodeType.LLM_CALL,
            inputs={"p": 1},
            outputs=None,
            requested_model="served",
            model="served",
            model_provider=None,
            tokens=None,
        ),
    ]
    signals = _by_name(evaluators.llm_call_signals(_view(nodes)))
    assert signals["failed_calls"].score == 1
    assert signals["failed_calls"].max_score == 2
    assert signals["empty_results"].score == 2
    assert signals["adjacent_identical_inputs"].score == 1
    assert signals["requested_model_mismatches"].score == 1
    assert signals["metadata_coverage"].score == 6
    assert signals["metadata_coverage"].max_score == 8

    policy = _by_name(
        evaluators.model_policy(
            _view(nodes),
            allowed_models=["served"],
            allowed_providers=["good"],
            require_requested_model_match=True,
        )
    )
    assert policy["allowed_models"].passed is True
    assert policy["allowed_models"].score == 0
    assert policy["allowed_models"].max_score == 2
    assert policy["allowed_providers"].passed is False
    assert policy["allowed_providers"].score == 1
    assert policy["requested_model_match"].passed is False
    assert policy["requested_model_match"].score == 1


def test_model_policy_holds_without_llm_calls() -> None:
    """Do not treat missing LLM evidence as proof of policy compliance."""
    policy = _by_name(
        evaluators.model_policy(
            _view([]),
            allowed_models=["model"],
            allowed_providers=["provider"],
            require_requested_model_match=True,
        )
    )

    assert policy["allowed_models"].passed is None
    assert policy["allowed_providers"].passed is None
    assert policy["requested_model_match"].passed is None


@pytest.mark.parametrize(
    ("first", "second"),
    [(True, 1), (False, 0), (1, 1.0)],
)
def test_llm_signals_use_json_encoding_equality(first: Any, second: Any) -> None:
    """Do not classify distinct JSON encodings as identical LLM inputs."""
    nodes = [
        _node(0, node_type=NodeType.LLM_CALL, inputs=first, outputs="first"),
        _node(1, node_type=NodeType.LLM_CALL, inputs=second, outputs="second"),
    ]
    signals = _by_name(evaluators.llm_call_signals(_view(nodes)))
    assert signals["adjacent_identical_inputs"].score == 0
    assert signals["adjacent_identical_inputs"].max_score == 1


@pytest.mark.parametrize(
    ("mode", "passed"),
    [
        ("exact_order", False),
        ("in_order", True),
        ("contains_all", True),
        ("exact_set", False),
    ],
)
def test_workflow_conformance_modes(mode: str, passed: bool) -> None:
    """Keep the four exact workflow match modes distinct."""
    names = ["search", "read", "read", "answer"]
    nodes = [
        _node(index, tool_name=name, outputs={}) for index, name in enumerate(names)
    ]
    results = _by_name(
        evaluators.workflow_conformance(
            _view(nodes), expected_tools=["search", "answer"], mode=mode
        )
    )
    assert results["workflow_match"].passed is passed


@pytest.mark.parametrize(
    ("names", "expected", "mode", "passed"),
    [
        (["read"], ["search"], "exact_order", False),
        (["search", "read"], ["search"], "exact_order", False),
        (["unexpected"], ["search"], "exact_set", False),
        (["search"], ["search", "answer"], "exact_order", None),
        (["search"], ["search", "answer"], "exact_set", None),
        (["search"], ["search"], "exact_order", None),
        (["search"], ["search"], "exact_set", None),
    ],
)
def test_nonterminal_workflow_conformance_reports_decisive_failures(
    names: list[str], expected: list[str], mode: str, passed: bool | None
) -> None:
    """Fail irreversible mismatches but hold incomplete matching traces."""
    nodes = [
        _node(index, tool_name=name, outputs={}) for index, name in enumerate(names)
    ]
    result = _by_name(
        evaluators.workflow_conformance(
            _view(nodes, status=SessionStatus.IN_PROGRESS),
            expected_tools=expected,
            mode=mode,
        )
    )["workflow_match"]
    assert result.passed is passed


def test_long_workflows_return_a_bounded_numeric_result() -> None:
    """Return a small numeric result for a long workflow."""
    names = [f"tool-{index}-{'x' * 100}" for index in range(1_000)]
    nodes = [
        _node(index, tool_name=name, outputs={}) for index, name in enumerate(names)
    ]
    result = _by_name(
        evaluators.workflow_conformance(_view(nodes), expected_tools=names)
    )["workflow_match"]
    assert result.passed is True
    assert result.score == 1
    assert result.max_score == 1
    assert result.value is None
    assert len(result.model_dump_json()) < 1_000


def test_nonterminal_sessions_can_fail_but_cannot_pass() -> None:
    """Withhold completeness-dependent passes on a changing session."""
    view = _view(
        [_node(0, tool_name="blocked", outputs={})],
        status=SessionStatus.IN_PROGRESS,
    )
    assert (
        _by_name(evaluators.tool_policy(view, forbidden_tools=["blocked"]))[
            "forbidden_tools"
        ].passed
        is False
    )
    assert (
        _by_name(evaluators.resource_budget(view, max_nodes=10))[
            "node_count_budget"
        ].passed
        is None
    )
    matching_output = _by_name(
        evaluators.output_contract(view, expected={"answer": 42})
    )
    mismatching_output = _by_name(
        evaluators.output_contract(view, expected={"answer": 0})
    )
    assert matching_output["exact_output"].passed is None
    assert mismatching_output["exact_output"].passed is False
