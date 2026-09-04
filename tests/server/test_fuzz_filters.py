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
"""Property tests for the JSON filter query grammar."""

import json
import warnings
from contextlib import suppress
from typing import Any

from hypothesis import given
from hypothesis import strategies as st
from pydantic import TypeAdapter, ValidationError
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from kitaru.api_models.v1.agent import AgentListParams
from kitaru.api_models.v1.filter import FilterParam
from kitaru.server.adapters.db.filtering import compile_filter_expression
from kitaru.server.adapters.db.orm.agent import AgentORM
from kitaru.server.adapters.db.repositories.agent_repository import (
    AGENT_FILTER_BINDINGS,
)
from kitaru.server.adapters.rest.mapping.agents import agent_list_params_to_filter

_FILTER_PARAM_ADAPTER: TypeAdapter[FilterParam] = TypeAdapter(FilterParam)
_MAX_BOOLEAN_DEPTH = 4
_NAME_VALUES = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",)), max_size=64
)


def _condition_strategy() -> st.SearchStrategy[dict[str, Any]]:
    """Build a valid leaf condition for the agent list endpoint."""
    identifier = st.uuids().map(str)
    identifier_condition = st.one_of(
        st.fixed_dictionaries(
            {
                "field": st.just("id"),
                "op": st.sampled_from(("eq", "ne")),
                "value": identifier,
            }
        ),
        st.fixed_dictionaries(
            {
                "field": st.just("id"),
                "op": st.just("in"),
                "value": st.lists(identifier, min_size=1, max_size=5),
            }
        ),
    )
    name_condition = st.one_of(
        st.fixed_dictionaries(
            {
                "field": st.just("name"),
                "op": st.sampled_from(
                    ("eq", "ne", "startswith", "endswith", "contains")
                ),
                "value": _NAME_VALUES,
            }
        ),
        st.fixed_dictionaries(
            {
                "field": st.just("name"),
                "op": st.just("in"),
                "value": st.lists(_NAME_VALUES, min_size=1, max_size=5),
            }
        ),
    )
    return st.one_of(identifier_condition, name_condition)


def _agent_filter_strategy(depth: int = 0) -> st.SearchStrategy[dict[str, Any]]:
    """Build a valid agent filter within the server's complexity caps."""
    condition = _condition_strategy()
    if depth == _MAX_BOOLEAN_DEPTH:
        return condition
    operand = _agent_filter_strategy(depth + 1)
    return st.one_of(
        condition,
        st.fixed_dictionaries({"and": st.lists(operand, min_size=1, max_size=2)}),
        st.fixed_dictionaries({"or": st.lists(operand, min_size=1, max_size=2)}),
        st.fixed_dictionaries({"not": operand}),
    )


_JSON_VALUES = st.recursive(
    st.one_of(st.none(), st.booleans(), st.integers(), _NAME_VALUES),
    lambda children: st.one_of(
        st.lists(children, max_size=3),
        st.dictionaries(_NAME_VALUES, children, max_size=3),
    ),
    max_leaves=30,
)


@given(_agent_filter_strategy())
def test_valid_agent_filter_query_compiles_to_sql(payload: dict[str, Any]) -> None:
    """Compile every generated valid agent filter through the request boundary."""
    params = AgentListParams.model_validate({"filter": json.dumps(payload)})
    agent_filter = agent_list_params_to_filter(params)
    assert agent_filter.expression is not None

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        statement = select(AgentORM).where(
            compile_filter_expression(agent_filter.expression, AGENT_FILTER_BINDINGS)
        )
        compiled = str(statement.compile(dialect=postgresql.dialect()))

    assert not caught
    assert "FROM agent" in compiled


@given(_JSON_VALUES)
def test_filter_param_rejects_hostile_json_cleanly(payload: Any) -> None:
    """Handle hostile filter shapes with validation errors rather than crashes."""
    with suppress(ValidationError):
        _FILTER_PARAM_ADAPTER.validate_python(json.dumps(payload))
