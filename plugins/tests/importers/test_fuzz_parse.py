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

import copy
import json
from decimal import Decimal
from pathlib import Path
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
    mutate_mastra_structure,
    mutated_seed_lines,
    records_for,
)

IMPORTER_NAMES = sorted(IMPORTERS)


def test_all_importers_have_fuzz_strategies() -> None:
    """Keep the shared fuzz registry aligned with importer packages."""
    packages = {
        path.name.removesuffix("-importer")
        for path in (Path(__file__).parents[2] / "packages").glob("*-importer")
    }
    assert set(IMPORTERS) == packages


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
@given(content=garbage_bytes(), data=st.data())
def test_parse_contract_on_garbage(
    name: str, content: bytes, data: st.DataObject
) -> None:
    params = data.draw(importer_params(name))
    _assert_contract(name, content, params)


@pytest.mark.parametrize("name", IMPORTER_NAMES)
@given(data=st.data())
def test_parse_contract_on_records(name: str, data: st.DataObject) -> None:
    records = data.draw(records_for(name))
    params = data.draw(importer_params(name))
    _assert_contract(name, encode_records(name, records), params)


@pytest.mark.parametrize("name", IMPORTER_NAMES)
@given(data=st.data())
def test_parse_contract_on_invalid_params(name: str, data: st.DataObject) -> None:
    """Cover the parameter-validation branches `importer_params()` avoids."""
    params = data.draw(invalid_params(name))
    if name == "mastra":
        with pytest.raises(IMPORTERS[name].InvalidImport):
            list(IMPORTERS[name].parse(b"{}", params))
        return
    records = data.draw(records_for(name))
    _assert_contract(name, encode_records(name, records), params)


@given(content=mutated_seed_lines(), params=importer_params("langfuse"))
def test_langfuse_contract_on_mutated_seed(
    content: bytes, params: dict[str, Any]
) -> None:
    _assert_contract("langfuse", content, params)


def _parse_outcomes(
    name: str, content: bytes, params: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[tuple[str | None, str]]] | None:
    module = IMPORTERS[name]
    try:
        items = list(module.parse(content, params))
    except module.InvalidImport:
        return None
    return (
        sorted(
            (
                item.model_dump(mode="json")
                for item in items
                if isinstance(item, ImportedSession)
            ),
            key=lambda session: (
                session["external_id"],
                json.dumps(session, sort_keys=True),
            ),
        ),
        sorted(
            (
                (item.external_id, type(item).__name__)
                for item in items
                if isinstance(item, ImportFailure)
            ),
            key=lambda failure: (failure[0] is not None, failure[0] or "", failure[1]),
        ),
    )


@pytest.mark.parametrize("name", [n for n in IMPORTER_NAMES if n != "jsonl"])
@given(data=st.data())
def test_grouping_is_order_independent(name: str, data: st.DataObject) -> None:
    """Which records form a session must not depend on record order."""
    records = data.draw(records_for(name))
    params = data.draw(importer_params(name))
    # A full st.permutations() draw costs entropy proportional to the record
    # list and trips Hypothesis's data_too_large health check under the
    # derandomized "ci" profile. Reversing and rotating changes which record
    # the importer reads first, which is what order dependence turns on, for
    # the price of one small integer.
    rotation = data.draw(st.integers(0, max(0, len(records) - 1)))
    reordered = list(reversed(records))
    reordered = reordered[rotation:] + reordered[:rotation]
    if name == "mastra":
        reordered = copy.deepcopy(reordered)
        for trace in reordered:
            trace["spans"].reverse()
    assert _parse_outcomes(
        name, encode_records(name, records), params
    ) == _parse_outcomes(name, encode_records(name, reordered), params)


@given(data=st.data())
def test_mastra_valid_records_reach_normalization(data: st.DataObject) -> None:
    """Generated Mastra data must exercise normalization, not only rejection."""
    records = data.draw(records_for("mastra"))
    params = data.draw(importer_params("mastra"))
    items = list(IMPORTERS["mastra"].parse(encode_records("mastra", records), params))
    assert len(items) == len(records)
    assert all(isinstance(item, ImportedSession) for item in items)
    sessions = {
        item.metadata["mastra"]["trace_id"]: item
        for item in items
        if isinstance(item, ImportedSession)
    }
    for trace in records:
        session = sessions[trace["traceId"]]
        source = {span["spanId"]: span for span in trace["spans"]}
        nodes = flatten_nodes(session.nodes)
        assert {node.external_id for node in nodes} == set(source)
        by_index = {node.index: node.external_id for node in nodes}
        for node in nodes:
            raw = source[node.external_id]
            assert node.trace_id == trace["traceId"]
            assert by_index.get(node.parent_index) == raw["parentSpanId"]
            assert node.inputs == raw["input"]
            assert node.outputs == raw["output"]
            assert node.attributes == raw["attributes"]


@given(case=mutate_mastra_structure(), params=importer_params("mastra"))
def test_mastra_structural_failure_is_contained(
    case: tuple[list[dict[str, Any]], str],
    params: dict[str, Any],
) -> None:
    """Reject one malformed trace while preserving every valid neighbor."""
    records, expected_error = case
    items = list(IMPORTERS["mastra"].parse(encode_records("mastra", records), params))
    failures = [item for item in items if isinstance(item, ImportFailure)]
    sessions = [item for item in items if isinstance(item, ImportedSession)]
    assert len(failures) == 1
    assert failures[0].external_id == records[0]["traceId"]
    assert expected_error in failures[0].error
    assert {session.metadata["mastra"]["trace_id"] for session in sessions} == {
        trace["traceId"] for trace in records[1:]
    }


@given(data=st.data())
def test_mastra_duplicate_trace_contract(data: st.DataObject) -> None:
    """Deduplicate exact traces and suppress a conflicting trace ID."""
    records = data.draw(records_for("mastra"))
    params = data.draw(importer_params("mastra"))
    original = records[0]
    exact_items = list(
        IMPORTERS["mastra"].parse(
            encode_records("mastra", [original, copy.deepcopy(original)]), params
        )
    )
    assert [
        item.metadata["mastra"]["trace_id"]
        for item in exact_items
        if isinstance(item, ImportedSession)
    ] == [original["traceId"]]

    conflicting = copy.deepcopy(original)
    conflicting["spans"][0]["name"] += "-conflicting"
    items = list(
        IMPORTERS["mastra"].parse(
            encode_records("mastra", [original, conflicting, *records[1:]]), params
        )
    )
    failures = [item for item in items if isinstance(item, ImportFailure)]
    assert len(failures) == 1
    assert failures[0].external_id == original["traceId"]
    assert "conflicting duplicate traceId" in failures[0].error
    assert {
        item.metadata["mastra"]["trace_id"]
        for item in items
        if isinstance(item, ImportedSession)
    } == {trace["traceId"] for trace in records[1:]}


_TOKEN_FIELDS = {
    "input_tokens": ("inputTokens", None),
    "output_tokens": ("outputTokens", None),
    "cached_input_tokens": ("cacheRead", "inputDetails"),
    "reasoning_tokens": ("reasoning", "outputDetails"),
}
_TOKEN_COUNTS = st.dictionaries(
    st.sampled_from(sorted(_TOKEN_FIELDS)),
    st.integers(min_value=0, max_value=10_000),
    max_size=len(_TOKEN_FIELDS),
)
_COST = st.none() | st.decimals(
    min_value=0, max_value=100, allow_nan=False, allow_infinity=False, places=4
)


def _build_mastra_usage(counts: dict[str, int]) -> dict[str, Any]:
    usage: dict[str, Any] = {}
    for normalized, (source, detail) in _TOKEN_FIELDS.items():
        if normalized not in counts:
            continue
        target = usage.setdefault(detail, {}) if detail else usage
        target[source] = counts[normalized]
    return usage


def _build_usage_attributes(
    counts: dict[str, int], cost: Decimal | None
) -> dict[str, Any]:
    attributes: dict[str, Any] = {"usage": _build_mastra_usage(counts)}
    if cost is not None:
        attributes["costContext"] = {
            "estimatedCost": str(cost),
            "costUnit": "USD",
        }
    return attributes


@given(
    generation=_TOKEN_COUNTS,
    step=_TOKEN_COUNTS,
    inference=_TOKEN_COUNTS,
    generation_cost=_COST,
    step_cost=_COST,
    inference_cost=_COST,
)
def test_mastra_usage_uses_nearest_aggregate_once(
    generation: dict[str, int],
    step: dict[str, int],
    inference: dict[str, int],
    generation_cost: Decimal | None,
    step_cost: Decimal | None,
    inference_cost: Decimal | None,
) -> None:
    """Aggregate each token and cost field once, with field-level fallback."""
    trace_id = "usage-trace"
    span_levels = (
        ("agent_run", {}, None),
        ("model_generation", generation, generation_cost),
        ("model_step", step, step_cost),
        ("model_inference", inference, inference_cost),
    )
    spans = []
    for index, (span_type, counts, cost) in enumerate(span_levels):
        spans.append(
            {
                "traceId": trace_id,
                "spanId": f"span-{index}",
                "parentSpanId": f"span-{index - 1}" if index else None,
                "name": span_type,
                "spanType": span_type,
                "startedAt": "2026-01-01T00:00:00Z",
                "endedAt": "2026-01-01T00:00:01Z",
                "input": {},
                "output": {},
                "attributes": _build_usage_attributes(counts, cost),
            }
        )
    session = next(
        item
        for item in IMPORTERS["mastra"].parse(
            encode_records("mastra", [{"traceId": trace_id, "spans": spans}]), {}
        )
        if isinstance(item, ImportedSession)
    )
    for field in _TOKEN_FIELDS:
        expected = next(
            (level[field] for level in (generation, step, inference) if field in level),
            0,
        )
        actual = sum(
            getattr(node.tokens, field) or 0
            for node in session.nodes
            if node.tokens is not None
        )
        assert actual == expected
    expected_cost = next(
        (
            cost
            for cost in (generation_cost, step_cost, inference_cost)
            if cost is not None
        ),
        Decimal(0),
    )
    assert sum((node.cost or Decimal(0)) for node in session.nodes) == expected_cost


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
