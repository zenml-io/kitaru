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
"""Property tests for the importer `parse()` contract."""

import json
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.task.importer import ImportedSession, flatten_nodes

from .fuzz_strategies import (
    IMPORTERS,
    encode_records,
    garbage_bytes,
    importer_params,
    invalid_params,
    mutated_seed_lines,
    records_for,
)

IMPORTER_NAMES = sorted(IMPORTERS)


def _assert_contract(name: str, content: bytes, params: dict[str, Any]) -> None:
    """Assert the documented `parse()` contract for one input."""
    module = IMPORTERS[name]
    try:
        items = list(module.parse(content, params))
    except module.InvalidImport:
        return
    for item in items:
        assert isinstance(item, (ImportedSession, ImportFailure)), type(item)
        json.loads(item.model_dump_json())


@pytest.mark.parametrize("name", IMPORTER_NAMES)
@given(content=garbage_bytes(), params=importer_params())
def test_parse_contract_on_garbage(
    name: str, content: bytes, params: dict[str, Any]
) -> None:
    _assert_contract(name, content, params)


@pytest.mark.parametrize("name", IMPORTER_NAMES)
@given(data=st.data())
def test_parse_contract_on_records(name: str, data: st.DataObject) -> None:
    records = data.draw(records_for(name))
    params = data.draw(importer_params())
    _assert_contract(name, encode_records(name, records), params)


@pytest.mark.parametrize("name", IMPORTER_NAMES)
@given(data=st.data())
def test_parse_contract_on_invalid_params(name: str, data: st.DataObject) -> None:
    """Cover the parameter-validation branches `importer_params()` avoids."""
    records = data.draw(records_for(name))
    params = data.draw(invalid_params())
    _assert_contract(name, encode_records(name, records), params)


@given(content=mutated_seed_lines(), params=importer_params())
def test_langfuse_contract_on_mutated_seed(
    content: bytes, params: dict[str, Any]
) -> None:
    _assert_contract("langfuse", content, params)


def _parse_outcomes(
    name: str, content: bytes, params: dict[str, Any]
) -> tuple[list[str], list[str | None]] | None:
    module = IMPORTERS[name]
    try:
        items = list(module.parse(content, params))
    except module.InvalidImport:
        return None
    return (
        sorted(item.external_id for item in items if isinstance(item, ImportedSession)),
        sorted(
            (item.external_id for item in items if isinstance(item, ImportFailure)),
            key=lambda external_id: (external_id is not None, external_id or ""),
        ),
    )


@pytest.mark.parametrize("name", [n for n in IMPORTER_NAMES if n != "jsonl"])
@given(data=st.data())
def test_grouping_is_order_independent(name: str, data: st.DataObject) -> None:
    """Which records form a session must not depend on record order."""
    records = data.draw(records_for(name))
    params = data.draw(importer_params())
    # A full st.permutations() draw costs entropy proportional to the record
    # list and trips Hypothesis's data_too_large health check under the
    # derandomized "ci" profile. Reversing and rotating changes which record
    # the importer reads first, which is what order dependence turns on, for
    # the price of one small integer.
    rotation = data.draw(st.integers(0, max(0, len(records) - 1)))
    reordered = list(reversed(records))
    reordered = reordered[rotation:] + reordered[:rotation]
    assert _parse_outcomes(
        name, encode_records(name, records), params
    ) == _parse_outcomes(name, encode_records(name, reordered), params)


def _assert_order_independent(name: str, rows: list[dict[str, Any]]) -> None:
    assert _parse_outcomes(name, encode_records(name, rows), {}) == _parse_outcomes(
        name, encode_records(name, rows[::-1]), {}
    )


def test_conflicting_project_identity_is_order_independent() -> None:
    _assert_order_independent(
        "braintrust",
        [
            {"span_id": "s0", "root_span_id": "t1", "project_id": "proj-a"},
            {"span_id": "s1", "root_span_id": "t1", "project_id": "proj-b"},
        ],
    )


def test_duplicate_run_id_grouping_is_order_independent() -> None:
    _assert_order_independent(
        "langsmith",
        [
            {"id": "r0", "trace_id": "t1", "parent_run_id": "r0"},
            {
                "id": "r0",
                "trace_id": "t1",
                "parent_run_id": None,
                "session_id": "proj-a",
            },
        ],
    )


# Every importer below needs a project identity before it will build a
# session; the records themselves are about cost and chain shape, not identity.
_PROJECT_PARAMS = {"source_instance": "proj"}


def _linear_chain(n: int) -> bytes:
    records = [
        {
            "id": f"s{i}",
            "traceId": "t1",
            "type": "SPAN",
            "name": f"n{i}",
            "parentObservationId": f"s{i - 1}" if i else None,
            "startTime": "2026-01-01T00:00:00Z",
        }
        for i in range(n)
    ]
    return encode_records("langfuse", records)


def test_large_trace_with_bounded_depth_is_serializable() -> None:
    # A rejected deep chain cannot establish performance for accepted traces.
    # Keep 20,000 observations at depth 64, with branches sharing ancestors.
    records = [
        {
            "id": f"s{i}",
            "traceId": "t1",
            "type": "SPAN",
            "name": f"n{i}",
            "parentObservationId": f"s{i - 1}"
            if 0 < i < 63
            else ("s62" if i >= 63 else None),
        }
        for i in range(20_000)
    ]
    content = encode_records("langfuse", records)
    items = list(IMPORTERS["langfuse"].parse(content, _PROJECT_PARAMS))
    assert len(items) == 1
    assert isinstance(items[0], ImportedSession)
    json.loads(items[0].model_dump_json())
    assert len(flatten_nodes(items[0].nodes)) == len(records)


def test_deep_chain_yields_serializable_session_or_failure() -> None:
    _assert_contract("langfuse", _linear_chain(1_200), _PROJECT_PARAMS)


# One good record and one whose cost field holds "NaN", in the record shape and
# under the cost key each importer actually reads.
_NON_FINITE_COST_RECORDS: dict[str, list[dict[str, Any]]] = {
    "langfuse": [
        {
            "id": "a",
            "traceId": "t1",
            "type": "GENERATION",
            "name": "ok",
            "startTime": "2026-01-01T00:00:00Z",
        },
        {
            "id": "b",
            "traceId": "t2",
            "type": "GENERATION",
            "name": "poison",
            "startTime": "2026-01-01T00:00:00Z",
            "totalCost": "NaN",
        },
    ],
    "braintrust": [
        {
            "span_id": "a",
            "root_span_id": "t1",
            "created": "2026-01-01T00:00:00Z",
            "span_attributes": {"type": "llm"},
        },
        {
            "span_id": "b",
            "root_span_id": "t2",
            "created": "2026-01-01T00:00:00Z",
            "span_attributes": {"type": "llm"},
            "metrics": {"estimated_cost": "NaN"},
        },
    ],
    "langsmith": [
        {
            "id": "a",
            "trace_id": "t1",
            "run_type": "llm",
            "name": "ok",
            "start_time": "2026-01-01T00:00:00Z",
        },
        {
            "id": "b",
            "trace_id": "t2",
            "run_type": "llm",
            "name": "poison",
            "start_time": "2026-01-01T00:00:00Z",
            "total_cost": "NaN",
        },
    ],
    "logfire": [
        {
            "span_id": "a",
            "trace_id": "t1",
            "span_name": "ok",
            "start_timestamp": "2026-01-01T00:00:00Z",
            "attributes": {"gen_ai.operation.name": "chat"},
        },
        {
            "span_id": "b",
            "trace_id": "t2",
            "span_name": "poison",
            "start_timestamp": "2026-01-01T00:00:00Z",
            "attributes": {
                "gen_ai.operation.name": "chat",
                "gen_ai.usage.cost": "NaN",
            },
        },
    ],
    "phoenix": [
        {
            "context": {"trace_id": "t1", "span_id": "a"},
            "name": "ok",
            "span_kind": "LLM",
            "start_time": "2026-01-01T00:00:00Z",
            "attributes": {},
        },
        {
            "context": {"trace_id": "t2", "span_id": "b"},
            "name": "poison",
            "span_kind": "LLM",
            "start_time": "2026-01-01T00:00:00Z",
            "attributes": {"gen_ai.usage.cost": "NaN"},
        },
    ],
}


@pytest.mark.parametrize("name", sorted(_NON_FINITE_COST_RECORDS))
def test_non_finite_cost_fails_only_its_record(name: str) -> None:
    records = _NON_FINITE_COST_RECORDS[name]
    items = list(IMPORTERS[name].parse(encode_records(name, records), _PROJECT_PARAMS))
    sessions = [item for item in items if isinstance(item, ImportedSession)]
    failures = [item for item in items if isinstance(item, ImportFailure)]
    assert len(sessions) == len(failures) == 1
    assert sessions[0].external_id.endswith("t1")
    for item in items:
        json.loads(item.model_dump_json())


def test_phoenix_superscript_index_does_not_escape() -> None:
    span = {
        "context": {"trace_id": "t1", "span_id": "s1"},
        "name": "llm",
        "span_kind": "LLM",
        "start_time": "2026-01-01T00:00:00Z",
        "attributes": {"llm.input_messages.\u00b2.role": "user"},
    }
    _assert_contract("phoenix", json.dumps(span).encode(), _PROJECT_PARAMS)


def test_lone_surrogate_yields_serializable_session() -> None:
    """A lone UTF-16 surrogate must not reach an unserializable session."""
    content = json.dumps([{"span_id": "\ud800"}]).encode("utf-8", "surrogatepass")
    _assert_contract("braintrust", content, {"source_instance": "0"})


def test_non_list_span_parents_is_contained() -> None:
    """A truthy non-list `span_parents` must not raise `TypeError` out of `parse()`."""
    records = [{"project_id": [], "id": None, "span_parents": True}]
    _assert_contract("braintrust", encode_records("braintrust", records), {})


def test_non_string_model_field_is_contained() -> None:
    """A non-string langfuse `model` must not escape `parse()`."""
    records = [{"id": "id0", "traceId": "trace0", "model": []}]
    _assert_contract(
        "langfuse", encode_records("langfuse", records), {"source_instance": "p"}
    )


def test_non_string_model_metadata_is_contained() -> None:
    """Non-string braintrust model metadata must not escape `parse()`."""
    records = [
        {
            "span_id": "s0",
            "root_span_id": "t1",
            "project_id": "p",
            "metadata": {"provider": False},
        }
    ]
    _assert_contract("braintrust", encode_records("braintrust", records), {})
