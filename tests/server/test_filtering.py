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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Tests for filter expression validation."""

import uuid
from collections.abc import Mapping
from decimal import Decimal
from enum import StrEnum
from typing import ClassVar

import pytest
from pydantic import AwareDatetime

from kitaru.api_models.v1.filter import FilterOp
from kitaru.server.adapters.db.filtering import FilterBinding
from kitaru.server.adapters.db.repositories.account_repository import (
    ACCOUNT_FILTER_BINDINGS,
)
from kitaru.server.adapters.db.repositories.agent_repository import (
    AGENT_FILTER_BINDINGS,
)
from kitaru.server.adapters.db.repositories.api_key_repository import (
    API_KEY_FILTER_BINDINGS,
)
from kitaru.server.adapters.db.repositories.cohort_repository import (
    COHORT_FILTER_BINDINGS,
)
from kitaru.server.adapters.db.repositories.device_repository import (
    DEVICE_FILTER_BINDINGS,
)
from kitaru.server.adapters.db.repositories.evaluation_repository import (
    EVALUATION_FILTER_BINDINGS,
)
from kitaru.server.adapters.db.repositories.experiment_repository import (
    EXPERIMENT_FILTER_BINDINGS,
)
from kitaru.server.adapters.db.repositories.experiment_run_repository import (
    EXPERIMENT_RUN_FILTER_BINDINGS,
)
from kitaru.server.adapters.db.repositories.job_repository import (
    JOB_FILTER_BINDINGS,
)
from kitaru.server.adapters.db.repositories.plugin_repository import (
    PLUGIN_FILTER_BINDINGS,
)
from kitaru.server.adapters.db.repositories.replay_repository import (
    REPLAY_FILTER_BINDINGS,
)
from kitaru.server.adapters.db.repositories.secret_repository import (
    SECRET_FILTER_BINDINGS,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SESSION_FILTER_BINDINGS,
)
from kitaru.server.adapters.db.repositories.tag_repository import (
    TAG_FILTER_BINDINGS,
)
from kitaru.server.adapters.db.repositories.task_repository import (
    TASK_FILTER_BINDINGS,
)
from kitaru.server.adapters.db.repositories.worker_repository import (
    WORKER_FILTER_BINDINGS,
)
from kitaru.server.application.models.account import AccountFilter
from kitaru.server.application.models.agent import AgentFilter
from kitaru.server.application.models.api_key import ApiKeyFilter
from kitaru.server.application.models.cohort import CohortFilter
from kitaru.server.application.models.device import DeviceFilter
from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.application.models.experiment import ExperimentFilter
from kitaru.server.application.models.experiment_run import ExperimentRunFilter
from kitaru.server.application.models.job import JobFilter
from kitaru.server.application.models.plugin import EvaluatorFilter, ImporterFilter
from kitaru.server.application.models.replay import ReplayFilter
from kitaru.server.application.models.secret import SecretFilter
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.application.models.tag import TagFilter
from kitaru.server.application.models.task import JobTasksFilter, TaskFilter
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.base import ListFilter
from kitaru.server.domain.base import ValidationError
from kitaru.server.filtering import (
    EQUALITY_OPS,
    NULLABLE_OPS,
    ORDERED_OPS,
    STRING_OPS,
    AndExpression,
    FilterCondition,
    FilterExpression,
    FilterField,
    OrExpression,
    validate_filter_expression,
)


class ProbeStatus(StrEnum):
    """Probe status."""

    ACTIVE = "active"
    DONE = "done"


PROBE_FILTERABLE_FIELDS: Mapping[str, FilterField] = {
    "id": FilterField(value_type=uuid.UUID, ops=EQUALITY_OPS),
    "status": FilterField(value_type=ProbeStatus, ops=EQUALITY_OPS),
    "name": FilterField(value_type=str, ops=STRING_OPS | NULLABLE_OPS),
    "created": FilterField(value_type=AwareDatetime, ops=ORDERED_OPS),
    "cost": FilterField(value_type=Decimal, ops=ORDERED_OPS),
}


class ProbeFilter(ListFilter):
    """Probe list filter."""

    filterable_fields: ClassVar[Mapping[str, FilterField]] = PROBE_FILTERABLE_FIELDS


def _and(n: int) -> FilterExpression:
    """Build an and expression with n equality conditions on id."""
    return AndExpression(
        operands=tuple(
            FilterCondition(field="id", op=FilterOp.EQ, value=str(uuid.uuid4()))
            for _ in range(n)
        )
    )


def _nested_and(levels: int) -> FilterExpression:
    """Build a chain of and wrappers around a single condition, levels deep."""
    expr: FilterExpression = FilterCondition(
        field="id", op=FilterOp.EQ, value=str(uuid.uuid4())
    )
    for _ in range(levels - 1):
        expr = AndExpression(operands=(expr,))
    return expr


def _validate(condition: FilterCondition) -> FilterExpression:
    return validate_filter_expression(condition, PROBE_FILTERABLE_FIELDS)


def test_coerces_uuid_string() -> None:
    """Coerce a uuid string value into a UUID."""
    value = uuid.uuid4()
    validated = _validate(FilterCondition(field="id", op=FilterOp.EQ, value=str(value)))
    assert isinstance(validated, FilterCondition)
    assert validated.value == value


def test_coerces_iso_string_to_aware_datetime() -> None:
    """Coerce an ISO string value into an aware datetime."""
    validated = _validate(
        FilterCondition(field="created", op=FilterOp.GE, value="2026-01-01T00:00:00Z")
    )
    assert isinstance(validated, FilterCondition)
    assert validated.value.tzinfo is not None
    assert validated.value.year == 2026


def test_coerces_enum_string_to_member() -> None:
    """Coerce an enum string value into the enum member."""
    validated = _validate(
        FilterCondition(field="status", op=FilterOp.EQ, value="active")
    )
    assert isinstance(validated, FilterCondition)
    assert validated.value is ProbeStatus.ACTIVE


def test_coerces_number_to_decimal() -> None:
    """Coerce a number value into a Decimal."""
    validated = _validate(FilterCondition(field="cost", op=FilterOp.GE, value=1.5))
    assert isinstance(validated, FilterCondition)
    assert validated.value == Decimal("1.5")


def test_eq_null_value_rejected() -> None:
    """Reject a null value for eq."""
    with pytest.raises(ValidationError, match="Invalid filter value for field 'name'"):
        _validate(FilterCondition(field="name", op=FilterOp.EQ, value=None))


def test_ne_null_value_rejected() -> None:
    """Reject a null value for ne."""
    with pytest.raises(ValidationError, match="Invalid filter value for field 'name'"):
        _validate(FilterCondition(field="name", op=FilterOp.NE, value=None))


def test_is_null_accepted_with_omitted_value() -> None:
    """Accept is_null with an omitted value."""
    validated = _validate(FilterCondition(field="name", op=FilterOp.IS_NULL))
    assert isinstance(validated, FilterCondition)
    assert validated.value is None


def test_is_null_with_value_rejected() -> None:
    """Reject is_null with a value set."""
    with pytest.raises(
        ValidationError, match="Filter 'is_null' on field 'name' takes no value"
    ):
        _validate(FilterCondition(field="name", op=FilterOp.IS_NULL, value="x"))


def test_is_null_on_field_without_nullable_ops_rejected() -> None:
    """Reject is_null on a field that does not support it."""
    with pytest.raises(
        ValidationError, match="Operator 'is_null' is not supported for field 'cost'"
    ):
        _validate(FilterCondition(field="cost", op=FilterOp.IS_NULL))


def test_rejects_unfilterable_field() -> None:
    """Reject a field outside the filterable allowlist."""
    with pytest.raises(ValidationError, match="Field 'bogus' is not filterable"):
        _validate(FilterCondition(field="bogus", op=FilterOp.EQ, value=1))


def test_rejects_unsupported_operator() -> None:
    """Reject an operator not in the field's allowed op set."""
    with pytest.raises(
        ValidationError,
        match="Operator 'startswith' is not supported for field 'cost'",
    ):
        _validate(FilterCondition(field="cost", op=FilterOp.STARTSWITH, value="1"))


def test_rejects_invalid_value() -> None:
    """Reject a value that fails to coerce to the field's value type."""
    with pytest.raises(ValidationError, match="Invalid filter value for field 'cost'"):
        _validate(FilterCondition(field="cost", op=FilterOp.GE, value="not-a-number"))


def test_rejects_naive_datetime() -> None:
    """Reject a naive datetime value for an aware datetime field."""
    with pytest.raises(
        ValidationError, match="Invalid filter value for field 'created'"
    ):
        _validate(
            FilterCondition(
                field="created", op=FilterOp.GE, value="2026-01-01T00:00:00"
            )
        )


def test_rejects_in_value_that_is_not_a_list() -> None:
    """Reject an in value that is not a list."""
    with pytest.raises(
        ValidationError, match="Filter value for 'in' on field 'id' must be a list"
    ):
        _validate(FilterCondition(field="id", op=FilterOp.IN, value="not-a-list"))


def test_rejects_empty_in_list() -> None:
    """Reject an in value with zero items."""
    with pytest.raises(
        ValidationError,
        match="Filter value for 'in' on field 'id' must have between 1 and 100 items",
    ):
        _validate(FilterCondition(field="id", op=FilterOp.IN, value=[]))


def test_rejects_in_list_over_100_items() -> None:
    """Reject an in value with 101 items."""
    values = [str(uuid.uuid4()) for _ in range(101)]
    with pytest.raises(
        ValidationError,
        match="Filter value for 'in' on field 'id' must have between 1 and 100 items",
    ):
        _validate(FilterCondition(field="id", op=FilterOp.IN, value=values))


def test_accepts_nesting_depth_of_5() -> None:
    """Accept a filter nested exactly 5 levels deep."""
    validated = validate_filter_expression(_nested_and(5), PROBE_FILTERABLE_FIELDS)
    assert isinstance(validated, AndExpression)


def test_rejects_nesting_depth_of_6() -> None:
    """Reject a filter nested 6 levels deep."""
    with pytest.raises(ValidationError, match="Filter is nested deeper than 5 levels"):
        validate_filter_expression(_nested_and(6), PROBE_FILTERABLE_FIELDS)


def test_accepts_30_conditions() -> None:
    """Accept a filter with exactly 30 conditions."""
    validated = validate_filter_expression(_and(30), PROBE_FILTERABLE_FIELDS)
    assert isinstance(validated, AndExpression)
    assert len(validated.operands) == 30


def test_rejects_31_conditions() -> None:
    """Reject a filter with 31 conditions."""
    with pytest.raises(ValidationError, match="Filter has more than 30 conditions"):
        validate_filter_expression(_and(31), PROBE_FILTERABLE_FIELDS)


def test_rejects_and_with_no_operands() -> None:
    """Reject an and expression with no operands."""
    with pytest.raises(
        ValidationError, match="Filter 'and' and 'or' need at least one operand"
    ):
        validate_filter_expression(AndExpression(operands=()), PROBE_FILTERABLE_FIELDS)


def test_rejects_or_with_no_operands() -> None:
    """Reject an or expression with no operands."""
    with pytest.raises(
        ValidationError, match="Filter 'and' and 'or' need at least one operand"
    ):
        validate_filter_expression(OrExpression(operands=()), PROBE_FILTERABLE_FIELDS)


def test_list_filter_where_validates_and_coerces() -> None:
    """Validate and coerce the filter expression on construction."""
    probe = ProbeFilter(
        expression=FilterCondition(field="status", op=FilterOp.EQ, value="active")
    )
    assert isinstance(probe.expression, FilterCondition)
    assert probe.expression.value is ProbeStatus.ACTIVE


def test_list_filter_where_rejects_invalid_field() -> None:
    """Reject a where expression naming a field outside the allowlist."""
    with pytest.raises(ValidationError, match="Field 'bogus' is not filterable"):
        ProbeFilter(expression=FilterCondition(field="bogus", op=FilterOp.EQ, value=1))


def test_compute_filter_hash_equal_for_identical_filters() -> None:
    """Hash equally for two identically constructed filters."""
    first = ProbeFilter(
        expression=FilterCondition(field="status", op=FilterOp.EQ, value="active")
    )
    second = ProbeFilter(
        expression=FilterCondition(field="status", op=FilterOp.EQ, value="active")
    )
    assert first.compute_filter_hash() == second.compute_filter_hash()


def test_compute_filter_hash_differs_when_where_differs() -> None:
    """Hash differently when the where expression differs."""
    active = ProbeFilter(
        expression=FilterCondition(field="status", op=FilterOp.EQ, value="active")
    )
    done = ProbeFilter(
        expression=FilterCondition(field="status", op=FilterOp.EQ, value="done")
    )
    assert active.compute_filter_hash() != done.compute_filter_hash()


def test_compute_filter_hash_differs_from_no_where() -> None:
    """Hash differently from a filter without a where expression."""
    filtered = ProbeFilter(
        expression=FilterCondition(field="status", op=FilterOp.EQ, value="active")
    )
    unfiltered = ProbeFilter()
    assert filtered.compute_filter_hash() != unfiltered.compute_filter_hash()


_EXACT_BINDING_PAIRS = [
    pytest.param(AccountFilter, ACCOUNT_FILTER_BINDINGS, id="account"),
    pytest.param(AgentFilter, AGENT_FILTER_BINDINGS, id="agent"),
    pytest.param(ApiKeyFilter, API_KEY_FILTER_BINDINGS, id="api_key"),
    pytest.param(CohortFilter, COHORT_FILTER_BINDINGS, id="cohort"),
    pytest.param(DeviceFilter, DEVICE_FILTER_BINDINGS, id="device"),
    pytest.param(EvaluationFilter, EVALUATION_FILTER_BINDINGS, id="evaluation"),
    pytest.param(ExperimentFilter, EXPERIMENT_FILTER_BINDINGS, id="experiment"),
    pytest.param(
        ExperimentRunFilter, EXPERIMENT_RUN_FILTER_BINDINGS, id="experiment_run"
    ),
    pytest.param(JobFilter, JOB_FILTER_BINDINGS, id="job"),
    pytest.param(ReplayFilter, REPLAY_FILTER_BINDINGS, id="replay"),
    pytest.param(SecretFilter, SECRET_FILTER_BINDINGS, id="secret"),
    pytest.param(SessionFilter, SESSION_FILTER_BINDINGS, id="session"),
    pytest.param(TagFilter, TAG_FILTER_BINDINGS, id="tag"),
    pytest.param(TaskFilter, TASK_FILTER_BINDINGS, id="task"),
    pytest.param(WorkerFilter, WORKER_FILTER_BINDINGS, id="worker"),
]

_SUBSET_BINDING_PAIRS = [
    pytest.param(EvaluatorFilter, PLUGIN_FILTER_BINDINGS, id="evaluator"),
    pytest.param(ImporterFilter, PLUGIN_FILTER_BINDINGS, id="importer"),
    pytest.param(JobTasksFilter, TASK_FILTER_BINDINGS, id="job_tasks"),
]


@pytest.mark.parametrize(("filter_class", "bindings"), _EXACT_BINDING_PAIRS)
def test_bindings_match_filterable_fields(
    filter_class: type[ListFilter], bindings: Mapping[str, FilterBinding]
) -> None:
    """Keep every repository's filter bindings in sync with its filter model."""
    assert set(bindings) == set(filter_class.filterable_fields)


@pytest.mark.parametrize(("filter_class", "bindings"), _SUBSET_BINDING_PAIRS)
def test_narrowed_bindings_cover_filterable_fields(
    filter_class: type[ListFilter], bindings: Mapping[str, FilterBinding]
) -> None:
    """Keep every narrowing filter's fields within its repository's bindings."""
    assert set(filter_class.filterable_fields) <= set(bindings)
