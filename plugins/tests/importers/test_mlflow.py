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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""MLflow trace importer plugin tests."""

import base64
import copy
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ImportedNode, ImportedSession, flatten_nodes
from kitaru_mlflow_importer.importer import InvalidImport, MlflowTraceImporter, importer
from kitaru_mlflow_importer.importer import parse as unified_parse

FIXTURE = Path(__file__).parent / "fixtures" / "mlflow" / "3.16.1" / "traces.json"
START_NS = 1_790_000_000_000_000_000


def load_export() -> dict[str, Any]:
    """Load the recorded ``mlflow traces search --output json`` page."""
    return json.loads(FIXTURE.read_text())


def parse(
    content: bytes | dict[str, Any] | list[Any], params: dict[str, Any] | None = None
) -> list[ImportedSession | ImportFailure]:
    """Parse one payload, encoding JSON values first."""
    payload = content if isinstance(content, bytes) else json.dumps(content).encode()
    return list(MlflowTraceImporter().parse(payload, params or {}))


def sessions_by_id(
    items: list[ImportedSession | ImportFailure],
) -> dict[str, ImportedSession]:
    """Index parsed sessions by external id, asserting there are no failures."""
    assert not [item for item in items if isinstance(item, ImportFailure)], items
    return {
        item.external_id: item for item in items if isinstance(item, ImportedSession)
    }


def flatten(nodes: list[ImportedNode]) -> list[ImportedNode]:
    """Flatten imported nodes depth-first."""
    return [node for root in nodes for node in (root, *flatten(root.children))]


def encoded(value: Any) -> str:
    """Encode an attribute value the way MLflow stores it."""
    return json.dumps(value)


def span(
    span_id: str,
    *,
    parent: str | None = None,
    span_type: str = "UNKNOWN",
    name: str | None = None,
    offset: int = 0,
    ended: bool = True,
    error: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one MLflow 3 span with JSON-encoded attributes."""
    return {
        "trace_id": "ignored",
        "span_id": span_id,
        "parent_span_id": parent or "",
        "name": name or span_id,
        "start_time_unix_nano": START_NS + offset,
        "end_time_unix_nano": START_NS + offset + 1_000 if ended else None,
        "status": {
            "code": "STATUS_CODE_ERROR" if error else "STATUS_CODE_OK",
            "message": error or "",
        },
        "events": [],
        "attributes": {
            "mlflow.spanType": encoded(span_type),
            **{key: encoded(value) for key, value in (attributes or {}).items()},
        },
    }


def trace(
    trace_id: str,
    spans: list[dict[str, Any]],
    *,
    metadata: dict[str, str] | None = None,
    experiment_id: Any = "7",
    state: str = "OK",
) -> dict[str, Any]:
    """Build one MLflow 3 trace dictionary."""
    return {
        "info": {
            "trace_id": trace_id,
            "trace_location": {
                "type": "MLFLOW_EXPERIMENT",
                "mlflow_experiment": {"experiment_id": experiment_id},
            },
            "request_time": "2026-09-29T12:00:00Z",
            "state": state,
            "trace_metadata": metadata or {},
            "tags": {},
        },
        "data": {"spans": spans},
    }


def llm_span(span_id: str, tokens: int, **kwargs: Any) -> dict[str, Any]:
    """Build an LLM span carrying usage and cost."""
    return span(
        span_id,
        span_type="CHAT_MODEL",
        attributes={
            "mlflow.chat.tokenUsage": {
                "input_tokens": tokens,
                "output_tokens": 1,
                "total_tokens": tokens + 1,
            },
            "mlflow.llm.cost": {"total_cost": 0.5},
        },
        **kwargs,
    )


def test_importer_instance_parse_matches_module_parse() -> None:
    content = FIXTURE.read_bytes()
    assert list(importer.parse(content, {})) == list(unified_parse(content, {}))


def test_reimport_is_stable() -> None:
    """Produce identical sessions, and so identical dedup keys, on every import."""
    content = FIXTURE.read_bytes()
    assert parse(content) == parse(content)


def test_recorded_export_groups_sessions_by_mlflow_session_metadata() -> None:
    sessions = sessions_by_id(parse(FIXTURE.read_bytes()))

    weather = sessions["1:session-weather"]
    assert len(sessions) == 4
    assert weather.name == "weather_agent"
    assert weather.status == SessionStatus.COMPLETED
    assert [turn["inputs"]["question"] for turn in weather.inputs["turns"]] == [
        "What's the weather in Delft?",
        "And tomorrow?",
    ]
    assert weather.outputs == "It is 14C and cloudy in Delft."
    assert weather.metadata["mlflow.session_id"] == "session-weather"
    assert weather.metadata["mlflow.users"] == ["user-42"]
    assert weather.metadata["mlflow.tags"] == {"env": "test"}
    assert weather.metadata["mlflow.join_paths"] == [
        "info.trace_metadata.mlflow.trace.session"
    ]
    [assessment] = weather.metadata["mlflow.assessments"]
    assert assessment["name"] == "helpfulness"
    assert assessment["value"] is True
    assert assessment["source"]["source_id"] == "reviewer@example.com"
    assert weather.started_at is not None and weather.ended_at is not None
    assert weather.started_at < weather.ended_at


def test_recorded_export_falls_back_to_trace_id_without_session() -> None:
    sessions = sessions_by_id(parse(FIXTURE.read_bytes()))
    [refund] = [s for s in sessions.values() if s.name == "refund_agent"]

    assert refund.external_id == f"1:{refund.metadata['mlflow.trace_ids'][0]}"
    assert refund.metadata["mlflow.session_id"] is None
    assert refund.metadata["normalization_warnings"] == [
        "No mlflow.trace.session metadata; grouped by trace id"
    ]


def test_recorded_session_totals_match_mlflow_trace_totals() -> None:
    """Count each model request once, as MLflow's own trace aggregation does."""
    export = load_export()
    sessions = sessions_by_id(parse(export))
    totals_by_trace = {
        item["info"]["trace_id"]: (
            json.loads(item["info"]["trace_metadata"]["mlflow.trace.tokenUsage"]),
            json.loads(item["info"]["trace_metadata"]["mlflow.trace.cost"]),
        )
        for item in export["traces"]
        if "mlflow.trace.tokenUsage" in item["info"]["trace_metadata"]
    }

    for session in sessions.values():
        trace_ids = session.metadata["mlflow.trace_ids"]
        if not all(trace_id in totals_by_trace for trace_id in trace_ids):
            continue
        nodes = flatten(session.nodes)
        assert sum(
            node.tokens.input_tokens or 0 for node in nodes if node.tokens
        ) == sum(totals_by_trace[t][0]["input_tokens"] for t in trace_ids)
        assert sum(node.cost for node in nodes if node.cost) == pytest.approx(
            Decimal(str(sum(totals_by_trace[t][1]["total_cost"] for t in trace_ids)))
        )


def test_recorded_openai_agent_maps_model_and_tool_calls() -> None:
    session = sessions_by_id(parse(FIXTURE.read_bytes()))["1:session-weather"]
    [first_turn_root, _] = session.nodes
    first_call, tool, second_call = first_turn_root.children

    assert first_turn_root.node_type == NodeType.SPAN
    assert first_turn_root.metadata["mlflow.span_type"] == "AGENT"
    assert first_call.node_type == NodeType.LLM_CALL
    assert first_call.requested_model == "gpt-4o-mini"
    assert first_call.model == "gpt-4o-mini-2024-07-18"
    assert first_call.tokens is not None
    assert first_call.tokens.model_dump(exclude_none=True) == {
        "input_tokens": 42,
        "output_tokens": 11,
        "cached_input_tokens": 8,
    }
    assert first_call.cost is not None and first_call.cost > 0
    assert first_call.metadata["mlflow.message_format"] == "openai"
    assert first_call.system_prompt_selector == "/messages/0/content"
    assert first_call.input_text_selector == "/messages/1/content"
    assert first_call.output_text_selector is None
    assert tool.node_type == NodeType.TOOL_CALL
    assert tool.tool_name == "get_weather"
    assert tool.inputs == {"city": "Delft"}
    assert tool.outputs == {"city": "Delft", "temp_c": 14, "sky": "cloudy"}
    assert second_call.output_text_selector == "/choices/0/message/content"
    assert "mlflow.spanInputs" not in first_call.attributes["mlflow.attributes"]


def test_recorded_anthropic_call_maps_provider_payload_selectors() -> None:
    sessions = sessions_by_id(parse(FIXTURE.read_bytes()))
    [refund] = [s for s in sessions.values() if s.name == "refund_agent"]
    [call] = refund.nodes[0].children

    assert call.node_type == NodeType.LLM_CALL
    assert call.model == "claude-sonnet-5-5"
    assert call.model_provider == "anthropic"
    assert call.system_prompt_selector == "/system"
    assert call.input_text_selector == "/messages/0/content"
    assert call.output_text_selector == "/content/0/text"


def test_recorded_langchain_wrapper_counts_the_request_once() -> None:
    sessions = sessions_by_id(parse(FIXTURE.read_bytes()))
    [chat] = [s for s in sessions.values() if s.name == "ChatOpenAI"]
    [wrapper] = chat.nodes
    [provider_call] = wrapper.children

    assert wrapper.node_type == NodeType.SPAN
    assert wrapper.tokens is None and wrapper.cost is None
    assert wrapper.model == "gpt-4o-mini"
    assert wrapper.model_params is not None
    assert wrapper.model_params["model_name"] == "gpt-4o-mini"
    assert wrapper.metadata["mlflow.usage_counted_on_descendants"] is True
    assert "mlflow.chat.tokenUsage" in wrapper.attributes["mlflow.attributes"]
    assert provider_call.node_type == NodeType.LLM_CALL
    assert provider_call.tokens is not None
    assert provider_call.tokens.input_tokens == 42


def test_recorded_failed_tool_fails_the_session() -> None:
    session = sessions_by_id(parse(FIXTURE.read_bytes()))["1:session-orders"]
    [root] = session.nodes
    [tool] = root.children

    assert session.status == SessionStatus.FAILED
    assert session.error == "RuntimeError: Order A-1001 not found"
    assert tool.status == NodeStatus.FAILED
    assert tool.error == "RuntimeError: Order A-1001 not found"
    assert tool.attributes["mlflow.events"][0]["name"] == "exception"


def test_accepts_single_trace_list_and_jsonl_shapes() -> None:
    traces = load_export()["traces"]
    expected = sessions_by_id(parse({"traces": traces}))

    single = sessions_by_id(parse(json.dumps(traces[0]).encode()))
    listed = sessions_by_id(parse(traces))
    lines = sessions_by_id(parse(b"\n".join(json.dumps(t).encode() for t in traces)))
    pages = sessions_by_id(
        parse(
            b"\n".join(
                json.dumps({"traces": [t], "next_page_token": None}).encode()
                for t in traces
            )
        )
    )

    assert set(single) < set(expected)
    assert listed == lines == pages == expected


def test_isolates_malformed_jsonl_lines() -> None:
    good = json.dumps(trace("tr-a", [span("0000000000000001")])).encode()

    items = parse(good + b"\n{not json\n" + good.replace(b"tr-a", b"tr-b"))

    assert {i.external_id for i in items if isinstance(i, ImportedSession)} == {
        "7:tr-a",
        "7:tr-b",
    }
    [failure] = [item for item in items if isinstance(item, ImportFailure)]
    assert failure.line == 2


@pytest.mark.parametrize("content", [b"", b"   ", b"\xff\xfe", b"{not json", b"[]"])
def test_rejects_payloads_without_traces(content: bytes) -> None:
    with pytest.raises(InvalidImport):
        parse(content)


def test_isolates_traces_without_spans() -> None:
    items = parse([trace("tr-empty", []), trace("tr-ok", [span("0000000000000001")])])

    [failure] = [item for item in items if isinstance(item, ImportFailure)]
    assert "export traces with spans" in failure.error
    assert [i.external_id for i in items if isinstance(i, ImportedSession)] == [
        "7:tr-ok"
    ]


def test_join_on_groups_by_a_configured_path() -> None:
    items = parse(
        [
            trace("tr-a", [span("0000000000000001")], metadata={"user": "u1"}),
            trace("tr-b", [span("0000000000000002")], metadata={"user": "u1"}),
        ],
        {"join_on": "/info/trace_metadata/user"},
    )

    [session] = sessions_by_id(items).values()
    assert session.external_id == "7:u1"
    assert session.metadata["mlflow.trace_ids"] == ["tr-a", "tr-b"]
    assert session.metadata["mlflow.join_paths"] == ["/info/trace_metadata/user"]


@pytest.mark.parametrize(
    ("join_on", "error"),
    [
        ("info.trace_metadata.absent", "no value at join path"),
        ("info.trace_location", "non-scalar value"),
        (42, "join_on must be"),
        ("/bad~2escape", "invalid JSON Pointer escape"),
    ],
)
def test_join_on_failures_isolate_each_trace(join_on: Any, error: str) -> None:
    items = parse([trace("tr-a", [span("0000000000000001")])], {"join_on": join_on})

    [failure] = items
    assert isinstance(failure, ImportFailure)
    assert error in failure.error


def test_identical_repeated_traces_collapse() -> None:
    """Import overlapping export pages without losing the session."""
    duplicate = trace("tr-a", [span("0000000000000001")])

    [session] = parse([duplicate, copy.deepcopy(duplicate)])

    assert isinstance(session, ImportedSession)
    assert session.metadata["mlflow.trace_ids"] == ["tr-a"]
    assert len(flatten(session.nodes)) == 1


def test_conflicting_repeated_traces_fail_their_session() -> None:
    original = trace("tr-a", [span("0000000000000001")])
    changed = trace("tr-a", [span("0000000000000001", name="changed")])

    [failure] = parse([original, changed])

    assert isinstance(failure, ImportFailure)
    assert "conflicting copies of trace 'tr-a'" in failure.error


@pytest.mark.parametrize(
    "info",
    [
        {"timestamp_ms": 1_790_000_000_000, "execution_duration_ms": 10**18},
        {"request_time": "9999-12-31T23:59:59Z", "execution_duration_ms": 10**6},
        {"assessments": 5},
        {"assessments": "not-a-list"},
    ],
)
def test_malformed_trace_info_never_aborts_the_import(info: dict[str, Any]) -> None:
    """Keep every trace importing when trace info holds out-of-range values."""
    bad = trace("tr-bad", [span("0000000000000001", ended=False)])
    bad["info"].update(info)
    if "timestamp_ms" in info:
        del bad["info"]["request_time"]

    items = parse([bad, trace("tr-ok", [span("0000000000000001")])])

    assert {i.external_id for i in items if isinstance(i, ImportedSession)} == {
        "7:tr-bad",
        "7:tr-ok",
    }


def test_explicit_source_instance_wins_and_is_trimmed() -> None:
    [session] = parse(
        [trace("tr-a", [span("0000000000000001")])],
        {"source_instance": " explicit ", "experiment_id": "alias"},
    )
    assert isinstance(session, ImportedSession)
    assert session.external_id == "explicit:tr-a"


def test_experiment_id_alias_wins_over_embedded_experiment() -> None:
    [session] = parse(
        [trace("tr-a", [span("0000000000000001")])],
        {"source_instance": " ", "experiment_id": " alias "},
    )
    assert isinstance(session, ImportedSession)
    assert session.external_id == "alias:tr-a"


def test_missing_experiment_requires_source_instance() -> None:
    uc_trace = trace("trace:/catalog.schema/abc", [span("0000000000000001")])
    uc_trace["info"]["trace_location"] = {
        "type": "UC_SCHEMA",
        "uc_schema": {"catalog_name": "catalog", "schema_name": "schema"},
    }

    [failure] = parse([uc_trace])
    [session] = parse([uc_trace], {"source_instance": "uc"})

    assert isinstance(failure, ImportFailure)
    assert "--params" in failure.error and "source_instance" in failure.error
    assert isinstance(session, ImportedSession)
    assert session.external_id == "uc:trace:/catalog.schema/abc"


@pytest.mark.parametrize("invalid", [0, False, [], {}, 42])
@pytest.mark.parametrize("location", ["source_instance", "experiment_id", "embedded"])
def test_nonstring_identity_is_rejected_even_with_an_override(
    location: str, invalid: Any
) -> None:
    params: dict[str, Any] = {"source_instance": "explicit", "experiment_id": "alias"}
    embedded: Any = "7"
    if location == "embedded":
        embedded = invalid
    else:
        params[location] = invalid

    [failure] = parse(
        [trace("tr-a", [span("0000000000000001")], experiment_id=embedded)], params
    )

    assert isinstance(failure, ImportFailure)
    assert "must be a string" in failure.error


def test_normalizes_span_id_encodings_to_the_same_node_ids() -> None:
    raw = bytes.fromhex("00000000000000aa")
    parent = bytes.fromhex("00000000000000bb")
    encodings = [
        (base64.b64encode(raw).decode(), base64.b64encode(parent).decode()),
        ("00000000000000AA", "00000000000000BB"),
        ("0x00000000000000aa", "0x00000000000000bb"),
    ]

    for child_id, parent_id in encodings:
        items = parse(
            [trace("tr-a", [span(parent_id), span(child_id, parent=parent_id)])]
        )
        [session] = sessions_by_id(items).values()
        assert [node.external_id for node in flatten(session.nodes)] == [
            "tr-a:00000000000000bb",
            "tr-a:00000000000000aa",
        ]


def test_parses_the_mlflow_2_trace_schema() -> None:
    """Read a trace in the pre-3.0 schema, built from MLflow's 2.x deserializer."""
    legacy = {
        "info": {
            "request_id": "tr-legacy",
            "experiment_id": "3",
            "timestamp_ms": 1_790_000_000_000,
            "execution_time_ms": 25,
            "status": "ERROR",
            "request_metadata": {"mlflow.trace.session": "legacy-session"},
            "tags": {"mlflow.traceName": "legacy_agent"},
        },
        "data": {
            "spans": [
                {
                    "name": "legacy_agent",
                    "context": {"trace_id": "0x01", "span_id": "0x00000000000000c1"},
                    "parent_id": None,
                    "start_time": START_NS,
                    "end_time": START_NS + 10,
                    "status_code": "ERROR",
                    "status_message": "boom",
                    "attributes": {"mlflow.spanType": '"AGENT"'},
                    "events": [],
                },
                {
                    "name": "llm",
                    "context": {"trace_id": "0x01", "span_id": "0x00000000000000c2"},
                    "parent_id": "0x00000000000000c1",
                    "start_time": START_NS + 1,
                    "end_time": START_NS + 5,
                    "status_code": "OK",
                    "status_message": "",
                    "attributes": {"mlflow.spanType": '"LLM"'},
                    "events": [],
                },
            ]
        },
    }

    [session] = parse([legacy])

    assert isinstance(session, ImportedSession)
    assert session.external_id == "3:legacy-session"
    assert session.name == "legacy_agent"
    assert session.status == SessionStatus.FAILED
    assert session.error == "boom"
    [root] = session.nodes
    assert [child.node_type for child in root.children] == [NodeType.LLM_CALL]


def test_missing_parent_becomes_a_root_with_a_warning() -> None:
    [session] = parse(
        [trace("tr-a", [span("0000000000000002", parent="00000000000000ff")])]
    )

    assert isinstance(session, ImportedSession)
    [node] = flatten_nodes(session.nodes)
    assert node.parent_external_id is None
    assert any(
        "missing parent" in warning
        for warning in session.metadata["normalization_warnings"]
    )


@pytest.mark.parametrize(
    ("spans", "error"),
    [
        (
            [
                span("0000000000000001", parent="0000000000000002"),
                span("0000000000000002", parent="0000000000000001"),
            ],
            "cycle",
        ),
        ([span("0000000000000001"), span("0000000000000001")], "duplicate span id"),
        ([span("not-an-id")], "not hex or base64"),
    ],
)
def test_graph_failures_preserve_neighboring_sessions(
    spans: list[dict[str, Any]], error: str
) -> None:
    items = parse(
        [
            trace("tr-before", [span("0000000000000001")]),
            trace("tr-bad", spans),
            trace("tr-after", [span("0000000000000001")]),
        ]
    )

    assert {i.external_id for i in items if isinstance(i, ImportedSession)} == {
        "7:tr-before",
        "7:tr-after",
    }
    [failure] = [item for item in items if isinstance(item, ImportFailure)]
    assert error in failure.error


def test_parent_depth_boundary() -> None:
    chain = [
        span(f"{index:016x}", parent=f"{index - 1:016x}" if index > 1 else None)
        for index in range(1, 66)
    ]

    [within] = parse([trace("tr-a", chain[:64])])
    [beyond] = parse([trace("tr-a", chain)])

    assert isinstance(within, ImportedSession)
    assert isinstance(beyond, ImportFailure)
    assert "64 parent levels" in beyond.error


@pytest.mark.parametrize(
    ("attribute", "value"),
    [
        ("mlflow.llm.cost", {"total_cost": "NaN"}),
        ("mlflow.llm.cost", {"total_cost": -1}),
        ("mlflow.chat.tokenUsage", {"input_tokens": -1}),
        ("mlflow.chat.tokenUsage", {"input_tokens": 1.5}),
        ("mlflow.chat.tokenUsage", {"output_tokens": True}),
    ],
)
def test_invalid_usage_isolates_its_session(attribute: str, value: Any) -> None:
    bad = span("0000000000000001", span_type="LLM", attributes={attribute: value})

    items = parse([trace("tr-bad", [bad]), trace("tr-ok", [span("0000000000000001")])])

    assert [i.external_id for i in items if isinstance(i, ImportedSession)] == [
        "7:tr-ok"
    ]
    [failure] = [item for item in items if isinstance(item, ImportFailure)]
    assert failure.external_id == "tr-bad"


def test_rollup_usage_on_an_agent_span_yields_to_per_call_usage() -> None:
    agent = span(
        "0000000000000001",
        span_type="AGENT",
        attributes={"mlflow.chat.tokenUsage": {"input_tokens": 30, "output_tokens": 2}},
    )
    calls = [
        llm_span("0000000000000002", 10, parent="0000000000000001", offset=1),
        llm_span("0000000000000003", 20, parent="0000000000000001", offset=2),
    ]

    [session] = parse([trace("tr-a", [agent, *calls])])

    assert isinstance(session, ImportedSession)
    root, first, second = flatten(session.nodes)
    assert root.tokens is None
    assert root.metadata["mlflow.usage_counted_on_descendants"] is True
    assert first.tokens is not None and first.tokens.input_tokens == 10
    assert second.tokens is not None and second.tokens.input_tokens == 20


def test_usage_stays_on_a_parent_when_no_descendant_carries_it() -> None:
    wrapper = llm_span("0000000000000001", 5)
    inner = span("0000000000000002", span_type="LLM", parent="0000000000000001")

    [session] = parse([trace("tr-a", [wrapper, inner])])

    assert isinstance(session, ImportedSession)
    outer, provider_call = flatten(session.nodes)
    assert outer.node_type == NodeType.SPAN
    assert outer.tokens is not None and outer.tokens.input_tokens == 5
    assert provider_call.node_type == NodeType.LLM_CALL
    assert provider_call.tokens is None


def test_in_progress_trace_is_marked_partial() -> None:
    [session] = parse(
        [trace("tr-a", [span("0000000000000001", ended=False)], state="IN_PROGRESS")]
    )

    assert isinstance(session, ImportedSession)
    [node] = session.nodes
    assert node.status == NodeStatus.IN_PROGRESS
    assert session.metadata["source_completeness"] == "partial"
    assert any(
        "still in progress" in warning
        for warning in session.metadata["normalization_warnings"]
    )


def test_invalidated_assessments_are_dropped() -> None:
    item = trace("tr-a", [span("0000000000000001")])
    item["info"]["assessments"] = [
        {"assessment_name": "old", "feedback": {"value": 1}, "valid": False},
        {"assessment_name": "new", "feedback": {"value": 2}, "valid": True},
        {"assessment_name": "truth", "expectation": {"value": "yes"}},
    ]

    [session] = parse([item])

    assert isinstance(session, ImportedSession)
    assert [
        (a["name"], a["kind"], a["value"])
        for a in session.metadata["mlflow.assessments"]
    ] == [("new", "feedback", 2), ("truth", "expectation", "yes")]


def test_turns_and_nodes_follow_trace_start_time_not_payload_order() -> None:
    later = trace(
        "tr-late",
        [span("0000000000000001", offset=5_000)],
        metadata={"mlflow.trace.session": "s"},
    )
    earlier = trace(
        "tr-early", [span("0000000000000001")], metadata={"mlflow.trace.session": "s"}
    )

    [session] = parse([later, earlier])

    assert isinstance(session, ImportedSession)
    assert session.metadata["mlflow.trace_ids"] == ["tr-early", "tr-late"]
    assert [node.trace_id for node in session.nodes] == ["tr-early", "tr-late"]
