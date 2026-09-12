#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""PostgreSQL-backed properties for recursive JSON filters.

The independent oracle covers id equality, nullable name equality and literal
string operators, tag EQ/IN through independent EXISTS predicates, recursive
AND/OR/NOT, and UUIDv7 cursor pagination in both directions. Exhaustive fixed
queries cover scalar cohort and agent relationships on experiment runs.
"""

import asyncio
import os
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from conftest import pg_session_with_engine, postgres_available
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.cohort_version_repository import (
    SQLCohortVersionRepository,
)
from kitaru.server.adapters.db.repositories.experiment_repository import (
    SQLExperimentRepository,
)
from kitaru.server.adapters.db.repositories.experiment_run_repository import (
    SQLExperimentRunRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.tag_repository import SQLTagRepository
from kitaru.server.application.models.experiment_run import ExperimentRunFilter
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.cohort_version import CohortVersion
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.experiment_run import ExperimentRun
from kitaru.server.domain.replay_config import (
    PassthroughConfig,
    ReplayConfig,
    ToolPolicy,
)
from kitaru.server.domain.session import Session
from kitaru.server.domain.tag import Tag, TagLink
from kitaru.server.filtering import (
    AndExpression,
    FilterCondition,
    FilterExpression,
    NotExpression,
    OrExpression,
)

_FUZZ_POSTGRES = os.environ.get("KITARU_FUZZ_POSTGRES") == "1"
pytestmark = pytest.mark.skipif(
    not _FUZZ_POSTGRES,
    reason="PostgreSQL filter fuzzing is opt-in; run just fuzz-filters-pg",
)


def _get_max_examples(enabled: bool) -> int:
    """Read a positive generated-example budget when the suite is enabled."""
    if not enabled:
        return 25
    raw_value = os.environ.get("KITARU_FUZZ_PG_MAX_EXAMPLES", "25")
    try:
        value = int(raw_value)
    except ValueError as error:
        raise pytest.UsageError(
            "KITARU_FUZZ_PG_MAX_EXAMPLES must be a positive integer"
        ) from error
    if value < 1:
        raise pytest.UsageError(
            "KITARU_FUZZ_PG_MAX_EXAMPLES must be a positive integer"
        )
    return value


_MAX_EXAMPLES = _get_max_examples(_FUZZ_POSTGRES)
_TAG_NAMES = ("red", "blue", "green")
_MISSING_TAG = "missing"
_SPECIAL_TEXT = st.one_of(
    st.sampled_from(("", "%", "_", "\\", "a%b_c\\d", "café中🙂", "cafe\u0301")),
    st.text(
        alphabet=[*list("abXYZ019 %_\\-"), "é", "中", "🙂"],
        max_size=12,
    ),
)


def _build_uuid7(value: int) -> uuid.UUID:
    """Build a shrinkable UUIDv7 without requiring Python 3.14's constructor."""
    random_b_mask = (1 << 62) - 1
    random_a_mask = (1 << 12) - 1
    timestamp_mask = (1 << 48) - 1
    random_b = value & random_b_mask
    random_a = (value >> 62) & random_a_mask
    timestamp = (value >> 74) & timestamp_mask
    version_bits = 0x7 << 76
    rfc_variant_bits = 0x2 << 62
    return uuid.UUID(
        int=(
            (timestamp << 80)
            | version_bits
            | (random_a << 64)
            | rfc_variant_bits
            | random_b
        )
    )


_UUID7_PAYLOAD = st.integers(min_value=0, max_value=2**122 - 1)


class Truth(Enum):
    """SQL three-valued truth result."""

    TRUE = "true"
    FALSE = "false"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class Row:
    """Reference row used by the independent filter oracle."""

    id: uuid.UUID
    name: str | None
    tags: frozenset[str]


@dataclass(frozen=True)
class Case:
    """Generated database rows, expression, and pagination size."""

    rows: tuple[Row, ...]
    expression: FilterExpression
    page_size: int


def _evaluate_and(values: list[Truth]) -> Truth:
    """Evaluate SQL AND over reference truth values."""
    if Truth.FALSE in values:
        return Truth.FALSE
    if Truth.UNKNOWN in values:
        return Truth.UNKNOWN
    return Truth.TRUE


def _evaluate_or(values: list[Truth]) -> Truth:
    """Evaluate SQL OR over reference truth values."""
    if Truth.TRUE in values:
        return Truth.TRUE
    if Truth.UNKNOWN in values:
        return Truth.UNKNOWN
    return Truth.FALSE


def _evaluate_condition(row: Row, condition: FilterCondition) -> Truth:
    """Evaluate one supported condition without using SQL implementation code."""
    if condition.field == "tag":
        if condition.op is FilterOp.EQ:
            matches = condition.value in row.tags
        elif condition.op is FilterOp.IN:
            matches = any(value in row.tags for value in condition.value)
        else:
            raise AssertionError(f"Unsupported tag operator: {condition.op}")
        return Truth.TRUE if matches else Truth.FALSE

    if condition.field == "id":
        actual: Any = row.id
    elif condition.field == "name":
        actual = row.name
    else:
        raise AssertionError(f"Unsupported generated field: {condition.field}")
    if condition.op is FilterOp.IS_NULL:
        return Truth.TRUE if actual is None else Truth.FALSE
    if actual is None:
        return Truth.UNKNOWN

    match condition.op:
        case FilterOp.EQ:
            matches = actual == condition.value
        case FilterOp.NE:
            matches = actual != condition.value
        case FilterOp.IN:
            matches = actual in condition.value
        case FilterOp.STARTSWITH:
            matches = actual.startswith(condition.value)
        case FilterOp.ENDSWITH:
            matches = actual.endswith(condition.value)
        case FilterOp.CONTAINS:
            matches = condition.value in actual
        case _:
            raise AssertionError(f"Unsupported generated operator: {condition.op}")
    return Truth.TRUE if matches else Truth.FALSE


def _evaluate_expression(row: Row, expression: FilterExpression) -> Truth:
    """Evaluate a supported expression with SQL three-valued semantics."""
    if isinstance(expression, AndExpression):
        return _evaluate_and(
            [_evaluate_expression(row, operand) for operand in expression.operands]
        )
    if isinstance(expression, OrExpression):
        return _evaluate_or(
            [_evaluate_expression(row, operand) for operand in expression.operands]
        )
    if isinstance(expression, NotExpression):
        result = _evaluate_expression(row, expression.operand)
        if result is Truth.UNKNOWN:
            return Truth.UNKNOWN
        return Truth.FALSE if result is Truth.TRUE else Truth.TRUE
    return _evaluate_condition(row, expression)


def _build_expression_strategy(
    rows: tuple[Row, ...],
) -> st.SearchStrategy[FilterExpression]:
    """Build expressions whose operands frequently occur in the generated rows."""
    used_ids = {row.id for row in rows}
    missing_id = next(
        candidate
        for value in range(len(rows) + 1)
        if (candidate := _build_uuid7(value)) not in used_ids
    )
    ids = (*tuple(row.id for row in rows), missing_id)
    names = tuple(row.name for row in rows if row.name is not None) or ("",)
    tag_names = (*_TAG_NAMES, _MISSING_TAG)

    id_condition = st.one_of(
        st.builds(
            FilterCondition,
            field=st.just("id"),
            op=st.sampled_from((FilterOp.EQ, FilterOp.NE)),
            value=st.sampled_from(ids),
        ),
        st.builds(
            FilterCondition,
            field=st.just("id"),
            op=st.just(FilterOp.IN),
            value=st.lists(st.sampled_from(ids), min_size=1, max_size=3, unique=True),
        ),
    )
    name_condition = st.one_of(
        st.builds(
            FilterCondition,
            field=st.just("name"),
            op=st.sampled_from(
                (
                    FilterOp.EQ,
                    FilterOp.NE,
                    FilterOp.STARTSWITH,
                    FilterOp.ENDSWITH,
                    FilterOp.CONTAINS,
                )
            ),
            value=st.one_of(st.sampled_from(names), _SPECIAL_TEXT),
        ),
        st.builds(
            FilterCondition,
            field=st.just("name"),
            op=st.just(FilterOp.IN),
            value=st.lists(
                st.one_of(st.sampled_from(names), _SPECIAL_TEXT),
                min_size=1,
                max_size=3,
                unique=True,
            ),
        ),
        st.just(FilterCondition(field="name", op=FilterOp.IS_NULL)),
    )
    tag_condition = st.one_of(
        st.builds(
            FilterCondition,
            field=st.just("tag"),
            op=st.just(FilterOp.EQ),
            value=st.sampled_from(tag_names),
        ),
        st.builds(
            FilterCondition,
            field=st.just("tag"),
            op=st.just(FilterOp.IN),
            value=st.lists(
                st.sampled_from(tag_names), min_size=1, max_size=3, unique=True
            ),
        ),
    )
    leaf = st.one_of(id_condition, name_condition, tag_condition)

    def build(depth: int) -> st.SearchStrategy[FilterExpression]:
        if depth == 4:
            return leaf
        operand = build(depth + 1)
        return st.one_of(
            leaf,
            st.builds(
                AndExpression,
                operands=st.lists(operand, min_size=1, max_size=2).map(tuple),
            ),
            st.builds(
                OrExpression,
                operands=st.lists(operand, min_size=1, max_size=2).map(tuple),
            ),
            st.builds(NotExpression, operand=operand),
        )

    return build(0)


@st.composite
def _build_case_strategy(draw: st.DrawFn) -> Case:
    """Build a bounded session dataset and query over its values."""
    ids = tuple(
        _build_uuid7(value)
        for value in draw(st.lists(_UUID7_PAYLOAD, min_size=2, max_size=6, unique=True))
    )
    rows = tuple(
        Row(
            id=id_,
            name=draw(st.one_of(st.none(), _SPECIAL_TEXT)),
            tags=frozenset(
                draw(st.sets(st.sampled_from(_TAG_NAMES), max_size=len(_TAG_NAMES)))
            ),
        )
        for id_ in ids
    )
    return Case(
        rows=rows,
        expression=draw(_build_expression_strategy(rows)),
        page_size=draw(st.integers(min_value=1, max_value=3)),
    )


_FIXED_ROWS = (
    Row(uuid.UUID("00000000-0000-7000-8000-000000000001"), None, frozenset()),
    Row(
        uuid.UUID("00000000-0000-7000-8000-000000000002"),
        "a%b_c\\d",
        frozenset({"red", "blue"}),
    ),
    Row(
        uuid.UUID("00000000-0000-7000-8000-000000000003"),
        "café中🙂",
        frozenset({"red"}),
    ),
    Row(
        uuid.UUID("00000000-0000-7000-8000-000000000004"),
        "cafe\u0301",
        frozenset({"blue"}),
    ),
)


def _build_fixed_case(expression: FilterExpression) -> Case:
    """Build an explicit regression case over the fixed edge-value rows."""
    return Case(rows=_FIXED_ROWS, expression=expression, page_size=1)


def _build_literal_operator_case(op: FilterOp, value: str) -> Case:
    """Build a case that distinguishes literal matching from SQL wildcards."""
    if op is FilterOp.STARTSWITH:
        match_name = f"{value}literal"
        distractor_name = "Xliteral"
    else:
        match_name = f"literal{value}"
        distractor_name = "literalX"
    rows = (
        Row(_build_uuid7(100), match_name, frozenset()),
        Row(_build_uuid7(101), distractor_name, frozenset()),
    )
    return Case(
        rows=rows,
        expression=FilterCondition(field="name", op=op, value=value),
        page_size=1,
    )


async def _assert_postgres_case(case: Case) -> None:
    """Compare one generated oracle result with both PostgreSQL cursor orders."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    async with pg_session_with_engine() as (db, engine):
        owner = await SQLAccountRepository(db).create(Account(name="owner"))
        agent = await SQLAgentRepository(db).create(
            Agent(owner_id=owner.id, name="assistant")
        )
        sessions = SQLSessionRepository(db, engine)
        tags = SQLTagRepository(db)
        for number, row in enumerate(case.rows, start=1):
            await sessions.create(
                Session(
                    id=row.id,
                    owner_id=owner.id,
                    agent_id=agent.id,
                    number=number,
                    origin=SessionOrigin.RECORDED,
                    name=row.name,
                )
            )

        stored_tags = {
            name: await tags.create(Tag(owner_id=owner.id, name=name))
            for name in _TAG_NAMES
            if any(name in row.tags for row in case.rows)
        }
        for row in case.rows:
            for name in row.tags:
                await tags.create_link(
                    TagLink(
                        tag_id=stored_tags[name].id,
                        resource_type=TagResourceType.SESSION,
                        resource_id=row.id,
                    )
                )

        expression = SessionFilter(expression=case.expression).expression
        assert expression is not None
        expected_ids = [
            row.id
            for row in case.rows
            if _evaluate_expression(row, expression) is Truth.TRUE
        ]
        for sort in ("created:asc", "created:desc"):
            expected = sorted(
                expected_ids,
                reverse=sort.endswith(":desc"),
            )
            collected: list[uuid.UUID] = []
            cursor = None
            for _ in range(len(case.rows) + 1):
                page, next_cursor = await sessions.query(
                    SessionFilter(
                        expression=expression,
                        sort=sort,
                        size=case.page_size,
                        cursor=cursor,
                    ),
                    include_payloads=False,
                )
                assert len(page) <= case.page_size
                collected.extend(session.id for session in page)
                assert collected == expected[: len(collected)]
                if len(collected) < len(expected):
                    assert page
                    assert next_cursor is not None
                else:
                    assert next_cursor is None
                if next_cursor is None:
                    break
                cursor = next_cursor
            else:
                pytest.fail("Cursor walk did not terminate")

            assert collected == expected


async def _assert_scalar_relationship_filters() -> None:
    """Check exhaustive scalar scope combinations through correlated EXISTS."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    async with pg_session_with_engine() as (db, _):
        owner = await SQLAccountRepository(db).create(Account(name="owner"))
        agents_repository = SQLAgentRepository(db)
        agents = [
            await agents_repository.create(
                Agent(owner_id=owner.id, name=f"agent-{index}")
            )
            for index in range(2)
        ]
        agent_versions_repository = SQLAgentVersionRepository(db)
        agent_versions = [
            await agent_versions_repository.create(
                AgentVersion(owner_id=owner.id, agent_id=agent.id)
            )
            for agent in agents
        ]
        cohorts_repository = SQLCohortRepository(db)
        cohorts = [
            await cohorts_repository.create(
                Cohort(
                    owner_id=owner.id,
                    agent_id=agent.id,
                    name=f"cohort-{index}",
                )
            )
            for index, agent in enumerate(agents)
        ]
        cohort_versions_repository = SQLCohortVersionRepository(db)
        cohort_versions = [
            await cohort_versions_repository.create(
                CohortVersion(
                    owner_id=owner.id,
                    cohort_id=cohort.id,
                    session_count=0,
                ),
                [],
            )
            for cohort in cohorts
        ]

        experiments_repository = SQLExperimentRepository(db)
        replay_config = await experiments_repository.create_replay_config(
            ReplayConfig(
                owner_id=owner.id,
                tool_policy=ToolPolicy(default=PassthroughConfig()),
                evaluators=[],
            )
        )
        experiments = [
            await experiments_repository.create(
                Experiment(
                    owner_id=owner.id,
                    name=f"experiment-{index}",
                    agent_id=agent.id,
                    replay_config_id=replay_config.id,
                )
            )
            for index, agent in enumerate(agents)
        ]
        runs_repository = SQLExperimentRunRepository(db)
        runs = {
            (index, index): await runs_repository.create(
                ExperimentRun(
                    owner_id=owner.id,
                    experiment_id=experiments[index].id,
                    number=1,
                    cohort_version_id=cohort_versions[index].id,
                    agent_version_id=agent_versions[index].id,
                )
            )
            for index in range(2)
        }

        missing_id = uuid.uuid4()
        cohort_targets = [*(cohort.id for cohort in cohorts), missing_id]
        agent_targets = [*(agent.id for agent in agents), missing_id]
        for cohort_index, cohort_id in enumerate(cohort_targets):
            for agent_index, agent_id in enumerate(agent_targets):
                matching, next_cursor = await runs_repository.query(
                    ExperimentRunFilter(
                        expression=AndExpression(
                            operands=(
                                FilterCondition(
                                    field="cohort_id",
                                    op=FilterOp.EQ,
                                    value=cohort_id,
                                ),
                                FilterCondition(
                                    field="agent_id",
                                    op=FilterOp.EQ,
                                    value=agent_id,
                                ),
                            )
                        )
                    )
                )
                expected = runs.get((cohort_index, agent_index))
                assert [run.id for run in matching] == (
                    [expected.id] if expected is not None else []
                )
                assert next_cursor is None


@pytest.mark.parametrize(
    ("row", "expression", "expected"),
    [
        (
            _FIXED_ROWS[0],
            FilterCondition(field="name", op=FilterOp.NE, value="x"),
            Truth.UNKNOWN,
        ),
        (
            _FIXED_ROWS[0],
            NotExpression(
                operand=FilterCondition(field="name", op=FilterOp.EQ, value="x")
            ),
            Truth.UNKNOWN,
        ),
        (
            _FIXED_ROWS[0],
            OrExpression(
                operands=(
                    FilterCondition(field="name", op=FilterOp.EQ, value="x"),
                    FilterCondition(field="name", op=FilterOp.NE, value="x"),
                )
            ),
            Truth.UNKNOWN,
        ),
        (
            _FIXED_ROWS[0],
            AndExpression(
                operands=(
                    FilterCondition(field="name", op=FilterOp.EQ, value="x"),
                    FilterCondition(field="tag", op=FilterOp.EQ, value="red"),
                )
            ),
            Truth.FALSE,
        ),
        (
            _FIXED_ROWS[1],
            AndExpression(
                operands=(
                    FilterCondition(field="tag", op=FilterOp.EQ, value="red"),
                    FilterCondition(field="tag", op=FilterOp.EQ, value="blue"),
                )
            ),
            Truth.TRUE,
        ),
        (
            _FIXED_ROWS[0],
            NotExpression(
                operand=FilterCondition(field="tag", op=FilterOp.EQ, value="red")
            ),
            Truth.TRUE,
        ),
    ],
)
def test_reference_oracle_retains_sql_unknown(
    row: Row, expression: FilterExpression, expected: Truth
) -> None:
    """Validate fixed SQL truth-table cases before generated comparisons."""
    assert _evaluate_expression(row, expression) is expected


def test_build_uuid7_sets_version_and_variant() -> None:
    """Build UUIDv7 values on every supported Python version."""
    identifier = _build_uuid7(42)
    assert identifier.version == 7
    assert identifier.variant == uuid.RFC_4122
    assert _build_uuid7(0) < _build_uuid7(1 << 74)


@pytest.mark.parametrize("raw_value", ("bogus", "0", "-1"))
def test_max_examples_rejects_invalid_enabled_values(
    monkeypatch: pytest.MonkeyPatch, raw_value: str
) -> None:
    """Reject malformed and nonpositive explicit database budgets clearly."""
    monkeypatch.setenv("KITARU_FUZZ_PG_MAX_EXAMPLES", raw_value)
    with pytest.raises(
        pytest.UsageError,
        match="KITARU_FUZZ_PG_MAX_EXAMPLES must be a positive integer",
    ):
        _get_max_examples(enabled=True)


def test_max_examples_ignores_stale_value_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ignore a stale database budget while PostgreSQL fuzzing is disabled."""
    monkeypatch.setenv("KITARU_FUZZ_PG_MAX_EXAMPLES", "bogus")
    assert _get_max_examples(enabled=False) == 25


async def test_scalar_relationship_filters_match_one_related_row() -> None:
    """Keep correlated scalar scope predicates on their referenced rows."""
    await _assert_scalar_relationship_filters()


@settings(max_examples=_MAX_EXAMPLES, deadline=None)
@example(
    case=_build_fixed_case(
        FilterCondition(field="name", op=FilterOp.CONTAINS, value="%")
    )
)
@example(
    case=_build_fixed_case(
        FilterCondition(field="name", op=FilterOp.CONTAINS, value="_")
    )
)
@example(
    case=_build_fixed_case(
        FilterCondition(field="name", op=FilterOp.CONTAINS, value="\\")
    )
)
@example(case=_build_literal_operator_case(FilterOp.STARTSWITH, "%"))
@example(case=_build_literal_operator_case(FilterOp.STARTSWITH, "_"))
@example(case=_build_literal_operator_case(FilterOp.STARTSWITH, "\\"))
@example(case=_build_literal_operator_case(FilterOp.ENDSWITH, "%"))
@example(case=_build_literal_operator_case(FilterOp.ENDSWITH, "_"))
@example(case=_build_literal_operator_case(FilterOp.ENDSWITH, "\\"))
@example(
    case=_build_fixed_case(
        OrExpression(
            operands=(
                FilterCondition(field="name", op=FilterOp.EQ, value="café中🙂"),
                FilterCondition(field="name", op=FilterOp.EQ, value="cafe\u0301"),
            )
        )
    )
)
@example(
    case=_build_fixed_case(
        AndExpression(
            operands=(
                FilterCondition(field="tag", op=FilterOp.EQ, value="red"),
                FilterCondition(field="tag", op=FilterOp.EQ, value="blue"),
            )
        )
    )
)
@example(
    case=_build_fixed_case(
        NotExpression(operand=FilterCondition(field="tag", op=FilterOp.EQ, value="red"))
    )
)
@given(case=_build_case_strategy())
def test_generated_filters_match_postgres(case: Case) -> None:
    """Match recursive filter membership and pagination against PostgreSQL."""
    asyncio.run(_assert_postgres_case(case))
