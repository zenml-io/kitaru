#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""Bounded numeric properties for deterministic evaluators."""

import uuid
from datetime import UTC, datetime, timedelta
from decimal import (
    ROUND_CEILING,
    ROUND_DOWN,
    ROUND_FLOOR,
    ROUND_HALF_EVEN,
    ROUND_UP,
    Context,
    Decimal,
    DecimalException,
    getcontext,
    localcontext,
)
from fractions import Fraction

import pytest
from hypothesis import given
from hypothesis import strategies as st

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
from kitaru_evaluator import deterministic as evaluators

NOW = datetime(2026, 9, 12, 12, tzinfo=UTC)
SESSION_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
ROUNDING_MODES = [
    ROUND_CEILING,
    ROUND_DOWN,
    ROUND_FLOOR,
    ROUND_HALF_EVEN,
    ROUND_UP,
]


def _decimal(coefficient: int, exponent: int) -> Decimal:
    """Construct a finite decimal without consulting the active context."""
    return Decimal(f"{coefficient}e{exponent}")


@st.composite
def _finite_decimals(draw: st.DrawFn) -> Decimal:
    """Generate finite decimals whose exact common-denominator sum stays small."""
    coefficient = draw(st.integers(-999_999_999_999, 999_999_999_999))
    exponent = draw(st.integers(-12, 12))
    return _decimal(coefficient, exponent)


def _node(
    index: int,
    node_type: NodeType,
    cost: Decimal,
    input_tokens: int,
    output_tokens: int,
) -> SessionNodeResponse:
    """Build a node containing complete numeric resource evidence."""
    return SessionNodeResponse(
        id=uuid.UUID(int=index + 100),
        session_id=SESSION_ID,
        index=index,
        parent_index=None,
        parent_id=None,
        secondary_parent_indexes=[],
        secondary_parent_ids=[],
        node_type=node_type,
        name=f"node-{index}",
        status=NodeStatus.COMPLETED,
        error=None,
        started_at=NOW,
        ended_at=NOW + timedelta(seconds=1),
        inputs={},
        outputs={},
        tokens=TokenUsage(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        ),
        cost=cost,
        metadata={},
    )


def _view(nodes: list[SessionNodeResponse], cost: Decimal) -> SessionView:
    """Build a terminal session whose rollups agree with its nodes."""
    input_tokens = 0
    output_tokens = 0
    for node in nodes:
        assert node.tokens is not None
        assert node.tokens.input_tokens is not None
        assert node.tokens.output_tokens is not None
        input_tokens += node.tokens.input_tokens
        output_tokens += node.tokens.output_tokens
    llm_call_count = sum(node.node_type is NodeType.LLM_CALL for node in nodes)
    tool_call_count = sum(node.node_type is NodeType.TOOL_CALL for node in nodes)
    session = SessionDetailResponse(
        id=SESSION_ID,
        owner_id=uuid.UUID(int=2),
        agent_id=uuid.UUID(int=3),
        number=1,
        origin=SessionOrigin.IMPORTED,
        status=SessionStatus.COMPLETED,
        inputs={},
        outputs={},
        metadata={},
        imported_from="numeric-property",
        started_at=NOW,
        ended_at=NOW + timedelta(seconds=1),
        cost=cost,
        tokens=TokenUsage(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        ),
        llm_call_count=llm_call_count,
        tool_call_count=tool_call_count,
        created=NOW,
        updated=NOW,
    )
    return SessionView(session=session, nodes=nodes)


def _by_name(results: list[EvaluationResult]) -> dict[str, EvaluationResult]:
    """Index evaluator results by their public names."""
    return {result.name: result for result in results}


def _context_state(
    context: Context,
) -> tuple[
    int,
    str,
    int,
    int,
    int,
    int,
    frozenset[type[DecimalException]],
    frozenset[type[DecimalException]],
]:
    """Snapshot every mutable Decimal context setting and signal state."""
    return (
        context.prec,
        context.rounding,
        context.Emin,
        context.Emax,
        context.capitals,
        context.clamp,
        frozenset(signal for signal, active in context.flags.items() if active),
        frozenset(signal for signal, active in context.traps.items() if active),
    )


@given(
    values=st.lists(_finite_decimals(), max_size=32),
    partition_seed=st.integers(0, 32),
    precision=st.integers(1, 28),
    rounding=st.sampled_from(ROUNDING_MODES),
)
def test_sum_decimals_matches_fraction_oracle_under_any_context(
    values: list[Decimal], partition_seed: int, precision: int, rounding: str
) -> None:
    """Sum finite decimals exactly without reading or changing ambient context."""
    expected = sum((Fraction(value) for value in values), start=Fraction())
    outer_context = getcontext()
    outer_state = _context_state(outer_context)
    ordinary = evaluators.sum_decimals(values)
    assert _context_state(outer_context) == outer_state
    partition = partition_seed % (len(values) + 1)

    with localcontext() as constrained_context:
        constrained_context.prec = precision
        constrained_context.rounding = rounding
        for signal in constrained_context.traps:
            constrained_context.traps[signal] = True
        constrained_state = _context_state(constrained_context)
        constrained = evaluators.sum_decimals(values)
        reversed_sum = evaluators.sum_decimals(list(reversed(values)))
        permuted_sum = evaluators.sum_decimals(values[::2] + values[1::2])
        with_zero = evaluators.sum_decimals([*values, Decimal(0)])
        recombined = evaluators.sum_decimals(
            [
                evaluators.sum_decimals(values[:partition]),
                evaluators.sum_decimals(values[partition:]),
            ]
        )
        assert _context_state(constrained_context) == constrained_state

    assert getcontext() is outer_context
    assert _context_state(outer_context) == outer_state
    assert constrained.as_tuple() == ordinary.as_tuple()
    for result in (constrained, reversed_sum, permuted_sum, with_zero, recombined):
        assert Fraction(result) == expected


@pytest.mark.parametrize(
    "value",
    [Decimal("NaN"), Decimal("sNaN"), Decimal("Infinity"), Decimal("-Infinity")],
)
def test_sum_decimals_rejects_nonfinite_values(value: Decimal) -> None:
    """Reject every non-finite Decimal spelling instead of producing a total."""
    with pytest.raises(ValueError, match="number must be finite"):
        evaluators.sum_decimals([Decimal(1), value])


RESOURCE_RECORDS = st.lists(
    st.tuples(
        st.sampled_from([NodeType.LLM_CALL, NodeType.TOOL_CALL, NodeType.SPAN]),
        st.integers(0, 1_000),
        st.integers(0, 100),
        st.integers(0, 100),
    ),
    max_size=8,
)


@given(
    records=RESOURCE_RECORDS,
    cost_scale=st.integers(0, 4),
    cost_ceiling=st.integers(0, 9_000),
    token_ceiling=st.integers(0, 2_000),
    node_ceiling=st.integers(0, 10),
    llm_ceiling=st.integers(0, 10),
    tool_ceiling=st.integers(0, 10),
    precision=st.integers(1, 12),
    rounding=st.sampled_from(ROUNDING_MODES),
)
def test_resource_budget_reports_reconciled_numeric_contracts(
    records: list[tuple[NodeType, int, int, int]],
    cost_scale: int,
    cost_ceiling: int,
    token_ceiling: int,
    node_ceiling: int,
    llm_ceiling: int,
    tool_ceiling: int,
    precision: int,
    rounding: str,
) -> None:
    """Publish exact-derived scores and inclusive verdicts for complete evidence."""
    exponent = -cost_scale
    nodes = [
        _node(index, node_type, _decimal(cost, exponent), input_tokens, output_tokens)
        for index, (node_type, cost, input_tokens, output_tokens) in enumerate(records)
    ]
    total_coefficient = sum(cost for _, cost, _, _ in records)
    view = _view(nodes, _decimal(total_coefficient, exponent))
    expected_cost = sum(
        (Fraction(_decimal(cost, exponent)) for _, cost, _, _ in records),
        start=Fraction(),
    )
    expected_tokens = sum(
        input_tokens + output_tokens for _, _, input_tokens, output_tokens in records
    )
    expected_counts = {
        "node_count_budget": len(records),
        "llm_call_count_budget": sum(
            node_type is NodeType.LLM_CALL for node_type, _, _, _ in records
        ),
        "tool_call_count_budget": sum(
            node_type is NodeType.TOOL_CALL for node_type, _, _, _ in records
        ),
    }
    ceilings = {
        "node_count_budget": node_ceiling,
        "llm_call_count_budget": llm_ceiling,
        "tool_call_count_budget": tool_ceiling,
    }
    kwargs = {
        "max_cost": cost_ceiling,
        "max_total_tokens": token_ceiling,
        "max_nodes": node_ceiling,
        "max_llm_calls": llm_ceiling,
        "max_tool_calls": tool_ceiling,
    }
    outer_context = getcontext()
    outer_state = _context_state(outer_context)
    ordinary = evaluators.resource_budget(view, **kwargs)
    assert _context_state(outer_context) == outer_state

    with localcontext() as constrained_context:
        constrained_context.prec = precision
        constrained_context.rounding = rounding
        for signal in constrained_context.traps:
            constrained_context.traps[signal] = True
        constrained_state = _context_state(constrained_context)
        constrained = evaluators.resource_budget(view, **kwargs)
        assert _context_state(constrained_context) == constrained_state

    assert getcontext() is outer_context
    assert _context_state(outer_context) == outer_state
    assert [result.model_dump_json() for result in constrained] == [
        result.model_dump_json() for result in ordinary
    ]

    results = _by_name(ordinary)
    cost_result = results["cost_budget"]
    assert cost_result.score == float(expected_cost)
    assert cost_result.min_score == 0
    assert cost_result.max_score == float(cost_ceiling)
    assert cost_result.passed is (expected_cost <= Fraction(cost_ceiling))

    token_result = results["total_tokens_budget"]
    assert token_result.score == expected_tokens
    assert token_result.min_score == 0
    assert token_result.max_score == token_ceiling
    assert token_result.passed is (expected_tokens <= token_ceiling)

    for name, observed in expected_counts.items():
        result = results[name]
        ceiling = ceilings[name]
        assert result.score == observed
        assert result.min_score == 0
        assert result.max_score == ceiling
        assert result.passed is (observed <= ceiling)


def test_resource_budget_distinguishes_missing_zero_and_boolean_tokens() -> None:
    """Reserve passing zero for complete integer token evidence."""
    zero_node = _node(0, NodeType.LLM_CALL, Decimal(0), 0, 0)
    zero_view = _view([zero_node], Decimal(0))

    missing_node = zero_node.model_copy(update={"cost": None, "tokens": None})
    missing_view = SessionView(
        session=zero_view.session.model_copy(update={"cost": None, "tokens": None}),
        nodes=[missing_node],
    )
    boolean_tokens = TokenUsage.model_construct(input_tokens=True, output_tokens=0)
    boolean_view = SessionView(
        session=zero_view.session.model_copy(update={"tokens": boolean_tokens}),
        nodes=[zero_node.model_copy(update={"tokens": boolean_tokens})],
    )

    zero = _by_name(
        evaluators.resource_budget(zero_view, max_cost=0, max_total_tokens=0)
    )
    missing = _by_name(
        evaluators.resource_budget(missing_view, max_cost=0, max_total_tokens=0)
    )
    boolean = _by_name(evaluators.resource_budget(boolean_view, max_total_tokens=0))

    assert zero["cost_budget"].score == missing["cost_budget"].score == 0
    assert zero["cost_budget"].passed is True
    assert missing["cost_budget"].passed is None
    assert zero["total_tokens_budget"].score == 0
    assert zero["total_tokens_budget"].passed is True
    assert missing["total_tokens_budget"].score == 0
    assert missing["total_tokens_budget"].passed is None
    assert boolean["total_tokens_budget"].score == 0
    assert boolean["total_tokens_budget"].passed is None
