"""Conservation and isolation properties for importer normalization."""

import copy
import json
from collections import Counter
from collections.abc import Callable
from decimal import Decimal
from types import ModuleType
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

import kitaru_braintrust_importer.importer as braintrust
import kitaru_jsonl_importer.importer as kitaru_jsonl
import kitaru_langfuse_importer.importer as langfuse
import kitaru_langsmith_importer.importer as langsmith
import kitaru_logfire_importer.importer as logfire
import kitaru_mastra_importer.importer as mastra
import kitaru_phoenix_importer.importer as phoenix
from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session_node import SessionNodeCreateRequest
from kitaru.task.importer import ImportedSession, flatten_nodes

from .normalization_strategies import (
    LogicalNode,
    LogicalTrace,
    generate_logical_forest,
    generate_logical_trace,
    reorder_records,
)

_SOURCE = "property-project"
_TIME = "2026-01-01T00:00:00Z"


def _usage_fields(node: LogicalNode) -> dict[str, Any]:
    return {
        "input": node.input_tokens,
        "output": node.output_tokens,
    }


def _langfuse_record(trace: LogicalTrace, node: LogicalNode) -> dict[str, Any]:
    return {
        "id": node.node_id,
        "traceId": trace.trace_id,
        "parentObservationId": node.parent_id,
        "name": node.node_id,
        "type": "GENERATION",
        "startTime": _TIME,
        "endTime": _TIME,
        "usageDetails": _usage_fields(node),
        "totalCost": node.cost,
    }


def _braintrust_record(trace: LogicalTrace, node: LogicalNode) -> dict[str, Any]:
    metrics = {
        "prompt_tokens": node.input_tokens,
        "completion_tokens": node.output_tokens,
        "estimated_cost": node.cost,
    }
    return {
        "id": node.node_id,
        "span_id": node.node_id,
        "root_span_id": trace.trace_id,
        "span_parents": [node.parent_id] if node.parent_id else [],
        "span_attributes": {"name": node.node_id, "type": "llm"},
        "created": _TIME,
        "metrics": metrics,
    }


def _langsmith_record(trace: LogicalTrace, node: LogicalNode) -> dict[str, Any]:
    return {
        "id": node.node_id,
        "trace_id": trace.trace_id,
        "parent_run_id": node.parent_id,
        "name": node.node_id,
        "run_type": "llm",
        "start_time": _TIME,
        "end_time": _TIME,
        "prompt_tokens": node.input_tokens,
        "completion_tokens": node.output_tokens,
        "total_cost": node.cost,
    }


def _logfire_record(trace: LogicalTrace, node: LogicalNode) -> dict[str, Any]:
    attributes = {
        "gen_ai.usage.input_tokens": node.input_tokens,
        "gen_ai.usage.output_tokens": node.output_tokens,
        "gen_ai.usage.cost": node.cost,
    }
    return {
        "trace_id": trace.trace_id,
        "span_id": node.node_id,
        "parent_span_id": node.parent_id,
        "span_name": node.node_id,
        "kind": "span",
        "start_timestamp": _TIME,
        "end_timestamp": _TIME,
        "attributes": attributes,
    }


def _phoenix_record(trace: LogicalTrace, node: LogicalNode) -> dict[str, Any]:
    attributes = {
        "gen_ai.usage.input_tokens": node.input_tokens,
        "gen_ai.usage.output_tokens": node.output_tokens,
        "gen_ai.usage.cost": node.cost,
    }
    return {
        "context": {"trace_id": trace.trace_id, "span_id": node.node_id},
        "parent_id": node.parent_id,
        "name": node.node_id,
        "span_kind": "LLM",
        "start_time": _TIME,
        "end_time": _TIME,
        "attributes": attributes,
    }


_PROVIDERS: dict[
    str, tuple[ModuleType, Callable[[LogicalTrace, LogicalNode], dict[str, Any]]]
] = {
    "braintrust": (braintrust, _braintrust_record),
    "langfuse": (langfuse, _langfuse_record),
    "langsmith": (langsmith, _langsmith_record),
    "logfire": (logfire, _logfire_record),
    "phoenix": (phoenix, _phoenix_record),
}


def _encode_provider(
    encoder: Callable[[LogicalTrace, LogicalNode], dict[str, Any]],
    traces: tuple[LogicalTrace, ...],
) -> list[dict[str, Any]]:
    return [encoder(trace, node) for trace in traces for node in trace.nodes]


def _session_nodes(
    items: list[ImportedSession | ImportFailure],
) -> dict[str, SessionNodeCreateRequest]:
    sessions = [item for item in items if isinstance(item, ImportedSession)]
    assert len(sessions) > 0, items
    assert not [item for item in items if isinstance(item, ImportFailure)], items
    nodes = [node for session in sessions for node in flatten_nodes(session.nodes)]
    identities = [node.external_id for node in nodes]
    assert all(identity is not None for identity in identities)
    assert len(identities) == len(set(identities))
    return {node.external_id: node for node in nodes if node.external_id is not None}


def _parent_identities(
    nodes: dict[str, SessionNodeCreateRequest],
) -> dict[str, str | None]:
    by_index = {
        (node.trace_id, node.index): identity for identity, node in nodes.items()
    }
    return {
        identity: by_index.get((node.trace_id, node.parent_index))
        for identity, node in nodes.items()
    }


@pytest.mark.parametrize("provider", sorted(_PROVIDERS))
@given(data=st.data(), traces=generate_logical_forest())
def test_accepted_provider_forests_conserve_nodes_and_usage(
    provider: str, data: st.DataObject, traces: tuple[LogicalTrace, ...]
) -> None:
    """Preserve every accepted node once, including zero and missing usage."""
    module, encoder = _PROVIDERS[provider]
    records = _encode_provider(encoder, traces)
    reordered = data.draw(reorder_records(records))

    items = list(
        module.parse(json.dumps(reordered).encode(), {"source_instance": _SOURCE})
    )

    actual = _session_nodes(items)
    expected: dict[str, LogicalNode] = {
        f"{trace.trace_id}:{node.node_id}": node
        for trace in traces
        for node in trace.nodes
    }
    assert set(actual) == set(expected)
    assert {
        item.external_id for item in items if isinstance(item, ImportedSession)
    } == {f"{_SOURCE}:{trace.trace_id}" for trace in traces}
    expected_parents = {
        f"{trace.trace_id}:{node.node_id}": (
            f"{trace.trace_id}:{node.parent_id}" if node.parent_id is not None else None
        )
        for trace in traces
        for node in trace.nodes
    }
    assert _parent_identities(actual) == expected_parents
    for identity, source in expected.items():
        node = actual[identity]
        expected_tokens = (
            None
            if source.input_tokens is None and source.output_tokens is None
            else (source.input_tokens, source.output_tokens)
        )
        actual_tokens = (
            None
            if node.tokens is None
            else (node.tokens.input_tokens, node.tokens.output_tokens)
        )
        assert actual_tokens == expected_tokens
        assert node.cost == (Decimal(source.cost) if source.cost is not None else None)
        if node.cost is not None:
            assert node.cost.is_finite() and node.cost >= 0


@given(
    data=st.data(),
    traces=generate_logical_forest(min_size=2),
)
def test_braintrust_joined_traces_preserve_nodes_and_roots(
    data: st.DataObject, traces: tuple[LogicalTrace, ...]
) -> None:
    """Keep every trace tree intact when roots join one Braintrust session."""
    records = _encode_provider(_braintrust_record, traces)
    for record in records:
        if record["span_id"] == record["root_span_id"]:
            record["metadata"] = {"session_id": "joined-property-session"}
    records = data.draw(reorder_records(records))

    items = list(
        braintrust.parse(json.dumps(records).encode(), {"source_instance": _SOURCE})
    )

    assert not [item for item in items if isinstance(item, ImportFailure)], items
    [session] = [item for item in items if isinstance(item, ImportedSession)]
    assert session.external_id == f"{_SOURCE}:joined-property-session"
    nodes = flatten_nodes(session.nodes)
    expected_ids = [
        f"{trace.trace_id}:{node.node_id}" for trace in traces for node in trace.nodes
    ]
    assert Counter(node.external_id for node in nodes) == Counter(expected_ids)
    expected_roots = {
        f"{trace.trace_id}:{node.node_id}"
        for trace in traces
        for node in trace.nodes
        if node.parent_id is None
    }
    assert {node.external_id for node in session.nodes} == expected_roots
    assert all(node.parent_index is None for node in session.nodes)


def _flat_session(trace: LogicalTrace) -> dict[str, Any]:
    indexes = {node.node_id: index for index, node in enumerate(trace.nodes)}
    return {
        "status": "completed",
        "name": trace.trace_id,
        "inputs": {},
        "outputs": {},
        "external_id": trace.trace_id,
        "metadata": {},
        "nodes": [
            {
                "index": index,
                "parent_index": (
                    indexes[node.parent_id] if node.parent_id is not None else None
                ),
                "external_id": node.node_id,
                "trace_id": trace.trace_id,
                "node_type": "span",
                "name": node.node_id,
                "status": "completed",
                "inputs": {},
                "outputs": {},
                "tokens": (
                    None
                    if node.input_tokens is None and node.output_tokens is None
                    else {
                        "input_tokens": node.input_tokens,
                        "output_tokens": node.output_tokens,
                    }
                ),
                "cost": node.cost,
                "attributes": {},
                "metadata": {},
            }
            for index, node in enumerate(trace.nodes)
        ],
    }


@given(
    traces=st.lists(
        generate_logical_trace(prefix="jsonl"),
        min_size=1,
        max_size=3,
        unique_by=lambda trace: trace.trace_id,
    )
)
def test_jsonl_sessions_conserve_flat_indexed_membership(
    traces: list[LogicalTrace],
) -> None:
    """Use each record's indexed membership instead of source line count."""
    payload = b"\n".join(json.dumps(_flat_session(trace)).encode() for trace in traces)

    items = list(kitaru_jsonl.parse(payload, {}))

    actual = _session_nodes(items)
    assert set(actual) == {node.node_id for trace in traces for node in trace.nodes}
    assert {
        item.external_id for item in items if isinstance(item, ImportedSession)
    } == {trace.trace_id for trace in traces}
    assert _parent_identities(actual) == {
        node.node_id: node.parent_id for trace in traces for node in trace.nodes
    }


def _mastra_trace(trace: LogicalTrace) -> dict[str, Any]:
    return {
        "traceId": trace.trace_id,
        "spans": [
            {
                "traceId": trace.trace_id,
                "spanId": node.node_id,
                "parentSpanId": node.parent_id,
                "name": node.node_id,
                "spanType": "model_inference",
                "isEvent": False,
                "startedAt": _TIME,
                "endedAt": _TIME,
                "input": {},
                "output": {},
                "attributes": {
                    "usage": (
                        None
                        if node.input_tokens is None and node.output_tokens is None
                        else {
                            "inputTokens": node.input_tokens,
                            "outputTokens": node.output_tokens,
                        }
                    ),
                    "costContext": (
                        None
                        if node.cost is None
                        else {"estimatedCost": node.cost, "costUnit": "USD"}
                    ),
                },
                "metadata": {},
            }
            for node in trace.nodes
        ],
    }


@given(data=st.data(), trace=generate_logical_trace(prefix="mastra"))
def test_mastra_conserves_reordered_spans_and_collapses_identical_traces(
    data: st.DataObject, trace: LogicalTrace
) -> None:
    """Preserve accepted spans while collapsing an identical trace export."""
    record = _mastra_trace(trace)
    record["spans"] = data.draw(st.permutations(record["spans"]).map(list))

    items = list(mastra.parse(json.dumps([record, copy.deepcopy(record)]).encode(), {}))

    actual = _session_nodes(items)
    assert set(actual) == {node.node_id for node in trace.nodes}
    [session] = [item for item in items if isinstance(item, ImportedSession)]
    assert session.external_id == trace.trace_id
    assert _parent_identities(actual) == {
        node.node_id: node.parent_id for node in trace.nodes
    }
    for source in trace.nodes:
        node = actual[source.node_id]
        assert node.cost == (Decimal(source.cost) if source.cost is not None else None)
        assert (node.tokens is None) == (
            source.input_tokens is None and source.output_tokens is None
        )


@given(data=st.data(), trace=generate_logical_trace(prefix="mastra-reversed"))
def test_mastra_reversed_timestamp_preserves_neighboring_traces(
    data: st.DataObject, trace: LogicalTrace
) -> None:
    """Reject one reversed span interval without discarding valid traces."""
    malformed = _mastra_trace(trace)
    target_index = data.draw(
        st.integers(min_value=0, max_value=len(malformed["spans"]) - 1)
    )
    malformed["spans"][target_index]["endedAt"] = "2025-12-31T23:59:59Z"
    neighbors = [
        LogicalTrace(name, (LogicalNode(name, None, None, None, None),))
        for name in ("before", "after")
    ]

    items = list(
        mastra.parse(
            json.dumps(
                [_mastra_trace(neighbors[0]), malformed, _mastra_trace(neighbors[1])]
            ).encode(),
            {},
        )
    )

    [failure] = [item for item in items if isinstance(item, ImportFailure)]
    assert failure.external_id == trace.trace_id
    assert failure.error == "span endedAt precedes startedAt"
    sessions = [item for item in items if isinstance(item, ImportedSession)]
    assert {session.external_id for session in sessions} == {"before", "after"}
    assert Counter(
        node.external_id
        for session in sessions
        for node in flatten_nodes(session.nodes)
    ) == Counter({"before": 1, "after": 1})


def _root_record(provider: str, trace_id: str) -> dict[str, Any]:
    trace = LogicalTrace(
        trace_id=trace_id,
        nodes=(LogicalNode(trace_id, None, 0, 0, "0"),),
    )
    return _PROVIDERS[provider][1](trace, trace.nodes[0])


def _graph_case_records(provider: str, case: str) -> list[dict[str, Any]]:
    encoder = _PROVIDERS[provider][1]
    if case == "cycle":
        trace = LogicalTrace(
            "bad",
            (
                LogicalNode("bad-a", "bad-b", 0, 0, "0"),
                LogicalNode("bad-b", "bad-a", 0, 0, "0"),
            ),
        )
    elif case == "self_parent":
        trace = LogicalTrace("bad", (LogicalNode("bad", "bad", 0, 0, "0"),))
    elif case == "duplicate_identity":
        trace = LogicalTrace(
            "bad",
            (
                LogicalNode("bad", None, 0, 0, "0"),
                LogicalNode("bad", None, 0, 0, "0"),
            ),
        )
    elif case == "dangling_parent":
        trace = LogicalTrace("bad", (LogicalNode("bad", "not-exported", 0, 0, "0"),))
    elif case == "disconnected_cycle":
        trace = LogicalTrace(
            "bad",
            (
                LogicalNode("bad", None, 0, 0, "0"),
                LogicalNode("bad-a", "bad-b", 0, 0, "0"),
                LogicalNode("bad-b", "bad-a", 0, 0, "0"),
            ),
        )
    else:
        raise AssertionError(f"Unknown graph case: {case}")
    return [encoder(trace, node) for node in trace.nodes]


@pytest.mark.parametrize("provider", sorted(_PROVIDERS))
def test_provider_cycle_failure_preserves_neighboring_sessions(provider: str) -> None:
    """Reject a cyclic group without discarding valid groups around it."""
    module, _ = _PROVIDERS[provider]
    records = [
        _root_record(provider, "before"),
        *_graph_case_records(provider, "cycle"),
        _root_record(provider, "after"),
    ]

    items = list(
        module.parse(json.dumps(records).encode(), {"source_instance": _SOURCE})
    )

    assert {
        item.external_id for item in items if isinstance(item, ImportedSession)
    } == {f"{_SOURCE}:before", f"{_SOURCE}:after"}
    [failure] = [item for item in items if isinstance(item, ImportFailure)]
    assert failure.external_id == "bad"
    assert "cycle" in failure.error.lower() or "root" in failure.error.lower()


_MISSING_GRAPH_POLICIES = [
    *(
        pytest.param(provider, "self_parent", False, id=f"{provider}-self-parent")
        for provider in ("braintrust", "langfuse", "logfire", "phoenix")
    ),
    *(
        pytest.param(
            provider,
            "duplicate_identity",
            False,
            id=f"{provider}-duplicate-identity",
        )
        for provider in ("braintrust", "langfuse", "logfire")
    ),
    *(
        pytest.param(provider, "dangling_parent", True, id=f"{provider}-dangling")
        for provider in ("braintrust", "langfuse", "langsmith")
    ),
    *(
        pytest.param(
            provider,
            "disconnected_cycle",
            False,
            id=f"{provider}-disconnected-cycle",
        )
        for provider in sorted(_PROVIDERS)
    ),
]


@pytest.mark.parametrize("provider,case,accepted", _MISSING_GRAPH_POLICIES)
def test_provider_graph_policy_preserves_neighboring_sessions(
    provider: str, case: str, accepted: bool
) -> None:
    """Apply each provider's repair or rejection policy within one group."""
    module, _ = _PROVIDERS[provider]
    items = list(
        module.parse(
            json.dumps(
                [
                    _root_record(provider, "before"),
                    *_graph_case_records(provider, case),
                    _root_record(provider, "after"),
                ]
            ).encode(),
            {"source_instance": _SOURCE},
        )
    )

    sessions = [item for item in items if isinstance(item, ImportedSession)]
    session_ids = {session.external_id for session in sessions}
    failures = [item for item in items if isinstance(item, ImportFailure)]
    neighbor_ids = {f"{_SOURCE}:before", f"{_SOURCE}:after"}
    if accepted:
        assert session_ids == {*neighbor_ids, f"{_SOURCE}:bad"}
        assert failures == []
        repaired = next(
            session for session in sessions if session.external_id == f"{_SOURCE}:bad"
        )
        assert (
            "missing parent"
            in " ".join(repaired.metadata["normalization_warnings"]).lower()
        )
        [node] = flatten_nodes(repaired.nodes)
        assert node.parent_index is None
    else:
        assert session_ids == neighbor_ids
        [failure] = failures
        assert failure.external_id == "bad"


def _set_cost(record: dict[str, Any], provider: str, value: Any) -> None:
    if provider == "langfuse":
        record["totalCost"] = value
    elif provider == "braintrust":
        record["metrics"]["estimated_cost"] = value
    elif provider == "langsmith":
        record["total_cost"] = value
    elif provider in {"logfire", "phoenix"}:
        record["attributes"]["gen_ai.usage.cost"] = value
    else:
        raise AssertionError(f"Unknown provider: {provider}")


@pytest.mark.parametrize("provider", sorted(_PROVIDERS))
@given(invalid_cost=st.sampled_from(["NaN", "Infinity", "-0.001", -1]))
def test_provider_invalid_cost_preserves_neighboring_sessions(
    provider: str, invalid_cost: Any
) -> None:
    """Reject one unsafe accounting group without losing valid neighbors."""
    module, _ = _PROVIDERS[provider]
    bad = _root_record(provider, "bad")
    _set_cost(bad, provider, invalid_cost)
    records = [
        _root_record(provider, "before"),
        bad,
        _root_record(provider, "after"),
    ]

    items = list(
        module.parse(json.dumps(records).encode(), {"source_instance": _SOURCE})
    )

    assert {
        item.external_id for item in items if isinstance(item, ImportedSession)
    } == {f"{_SOURCE}:before", f"{_SOURCE}:after"}
    [failure] = [item for item in items if isinstance(item, ImportFailure)]
    assert failure.external_id == "bad"
    assert "finite" in failure.error.lower() or "nonnegative" in failure.error.lower()


@given(invalid_cost=st.sampled_from(["NaN", "Infinity", "-1", -1]))
def test_jsonl_invalid_usage_isolated_between_valid_sessions(invalid_cost: Any) -> None:
    """Keep JSONL line isolation for unsafe accounting values."""
    before = LogicalTrace("before", (LogicalNode("before", None, 0, 0, "0"),))
    after = LogicalTrace("after", (LogicalNode("after", None, 0, 0, "0"),))
    bad = _flat_session(before)
    bad["external_id"] = "bad"
    bad["nodes"][0]["cost"] = invalid_cost
    payload = b"\n".join(
        json.dumps(record).encode()
        for record in (_flat_session(before), bad, _flat_session(after))
    )

    items = list(kitaru_jsonl.parse(payload, {}))

    assert [type(item) for item in items] == [
        ImportedSession,
        ImportFailure,
        ImportedSession,
    ]
    assert [items[0].external_id, items[2].external_id] == ["before", "after"]


@given(trace=generate_logical_trace(prefix="mastra-bad"))
def test_mastra_conflicting_duplicate_drops_only_that_trace(
    trace: LogicalTrace,
) -> None:
    """Drop both versions of a conflicting trace while preserving neighbors."""
    original = _mastra_trace(trace)
    conflicting = copy.deepcopy(original)
    conflicting["spans"][0]["output"] = {"different": True}
    neighbor_trace = LogicalTrace(
        "neighbor", (LogicalNode("neighbor", None, None, None, None),)
    )

    items = list(
        mastra.parse(
            json.dumps([original, conflicting, _mastra_trace(neighbor_trace)]).encode(),
            {},
        )
    )

    [failure] = [item for item in items if isinstance(item, ImportFailure)]
    assert failure.external_id == trace.trace_id
    assert "conflicting duplicate traceId" in failure.error
    [session] = [item for item in items if isinstance(item, ImportedSession)]
    assert session.external_id == "neighbor"


def test_mastra_self_parent_failure_preserves_neighboring_traces() -> None:
    """Reject a self-parented trace without discarding independent traces."""
    bad = LogicalTrace("bad", (LogicalNode("bad", "bad", None, None, None),))
    neighbors = [
        LogicalTrace(name, (LogicalNode(name, None, None, None, None),))
        for name in ("before", "after")
    ]

    items = list(
        mastra.parse(
            json.dumps(
                [
                    _mastra_trace(neighbors[0]),
                    _mastra_trace(bad),
                    _mastra_trace(neighbors[1]),
                ]
            ).encode(),
            {},
        )
    )

    assert {
        item.external_id for item in items if isinstance(item, ImportedSession)
    } == {"before", "after"}
    [failure] = [item for item in items if isinstance(item, ImportFailure)]
    assert failure.external_id == "bad"
    assert "root span" in failure.error
