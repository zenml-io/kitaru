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
"""LangSmith run-export importer plugin tests."""

import json
from decimal import Decimal
from typing import Any

import pytest

import importers.langsmith as langsmith_module
from importers.langsmith import InvalidImport, LangSmithRunImporter, parse
from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ParsedNode, ParsedSession


def jsonl(*records: dict[str, Any]) -> bytes:
    """Encode run records as JSONL."""
    return b"\n".join(json.dumps(record).encode() for record in records)


def run(
    run_id: str,
    trace_id: str,
    *,
    parent_run_id: str | None = None,
    run_type: str = "chain",
    name: str | None = None,
    thread_id: str | None = "thread-1",
    project_id: str | None = "project-1",
    start_time: str = "2026-08-05T10:00:00Z",
    inputs: Any = None,
    outputs: Any = None,
    **extra: Any,
) -> dict[str, Any]:
    """Build one representative LangSmith bulk-export row."""
    metadata = extra.pop("metadata", {})
    if thread_id is not None:
        metadata = {"thread_id": thread_id, **metadata}
    record: dict[str, Any] = {
        "id": run_id,
        "trace_id": trace_id,
        "parent_run_id": parent_run_id,
        "session_id": project_id,
        "name": name or run_id,
        "run_type": run_type,
        "start_time": start_time,
        "end_time": "2026-08-05T10:00:01Z",
        "status": "success",
        "inputs": inputs,
        "outputs": outputs,
        "extra": {"metadata": metadata},
        **extra,
    }
    return record


def sessions(
    content: bytes, params: dict[str, Any] | None = None
) -> list[ParsedSession]:
    """Return parsed sessions from one payload."""
    return [
        item
        for item in LangSmithRunImporter().parse(content, params or {})
        if isinstance(item, ParsedSession)
    ]


def failures(
    content: bytes, params: dict[str, Any] | None = None
) -> list[ImportFailure]:
    """Return isolated failures from one payload."""
    return [
        item
        for item in LangSmithRunImporter().parse(content, params or {})
        if isinstance(item, ImportFailure)
    ]


def flatten(nodes: list[ParsedNode]) -> list[ParsedNode]:
    """Flatten parsed nodes depth-first."""
    return [node for root in nodes for node in (root, *flatten(root.children))]


def test_groups_thread_traces_into_ordered_turns_and_nodes() -> None:
    """Map LangSmith projects, threads, traces, and run hierarchy."""
    content = jsonl(
        run(
            "tool-2",
            "trace-2",
            parent_run_id="llm-2",
            run_type="tool",
            name="lookup_policy",
            start_time="2026-08-05T10:01:00.200000Z",
            inputs={"policy": "returns"},
            outputs={"status": "active"},
            tags=["support"],
        ),
        run(
            "root-1",
            "trace-1",
            name="support-agent",
            start_time="2026-08-05T10:00:00Z",
            inputs={"message": "hello"},
            outputs={"answer": "hi"},
            metadata={"user_id": "user-7"},
        ),
        run(
            "root-2",
            "trace-2",
            name="support-agent",
            start_time="2026-08-05T10:01:00Z",
            inputs={"message": "return policy"},
            outputs={"answer": "30 days"},
        ),
        run(
            "llm-2",
            "trace-2",
            parent_run_id="root-2",
            run_type="llm",
            name="chat-model",
            start_time="2026-08-05T10:01:00.100000Z",
            inputs={"messages": [{"role": "user", "content": "return policy"}]},
            outputs={"message": "Checking policy"},
            prompt_tokens=12,
            completion_tokens=4,
            total_cost="0.004",
            extra={
                "metadata": {
                    "thread_id": "thread-1",
                    "ls_provider": "openai",
                    "ls_model_name": "gpt-5-mini",
                },
                "invocation_params": {"model": "gpt-5-mini", "temperature": 0},
            },
        ),
    )

    [session] = sessions(content)

    assert session.external_id == "project-1:thread-1"
    assert session.status is SessionStatus.COMPLETED
    assert [turn["source_trace_id"] for turn in session.inputs["turns"]] == [
        "trace-1",
        "trace-2",
    ]
    assert session.outputs == {"answer": "30 days"}
    assert session.metadata["langsmith.tags"] == ["support"]
    assert session.metadata["langsmith.user_ids"] == ["user-7"]
    assert session.metadata["langsmith.join_paths"] == ["extra.metadata.thread_id"]
    nodes = {node.external_id: node for node in flatten(session.nodes)}
    assert nodes["trace-2:llm-2"].node_type is NodeType.LLM_CALL
    assert nodes["trace-2:llm-2"].provider == "openai"
    assert nodes["trace-2:llm-2"].model == "gpt-5-mini"
    assert nodes["trace-2:llm-2"].tokens is not None
    assert nodes["trace-2:llm-2"].tokens.input_tokens == 12
    assert nodes["trace-2:llm-2"].cost == Decimal("0.004")
    assert nodes["trace-2:tool-2"].node_type is NodeType.TOOL_CALL
    assert nodes["trace-2:tool-2"].tool_name == "lookup_policy"
    assert nodes["trace-2:tool-2"] in nodes["trace-2:llm-2"].children


def test_uses_configurable_nested_join_path() -> None:
    """Group traces through a user-selected dotted JSON path."""
    first = run(
        "root-1",
        "trace-1",
        thread_id=None,
        inputs="one",
        metadata={"customer": {"case_id": "case-9"}},
    )
    second = run(
        "root-2",
        "trace-2",
        thread_id=None,
        start_time="2026-08-05T10:01:00Z",
        inputs="two",
        metadata={"customer": {"case_id": "case-9"}},
    )

    [session] = sessions(
        jsonl(first, second),
        {"join_on": "extra.metadata.customer.case_id"},
    )

    assert session.external_id == "project-1:case-9"
    assert session.metadata["source_trace_count"] == 2
    assert session.metadata["langsmith.join_paths"] == [
        "extra.metadata.customer.case_id"
    ]


def test_reads_configurable_join_path_from_trace_root() -> None:
    """Do not require root grouping metadata on every child run."""
    root = run(
        "root",
        "trace",
        thread_id=None,
        inputs="hello",
        metadata={"customer": {"case_id": "case-9"}},
    )
    child = run(
        "child",
        "trace",
        parent_run_id="root",
        thread_id=None,
        run_type="llm",
        inputs="hello",
        outputs="hi",
    )

    [session] = sessions(
        jsonl(root, child),
        {"join_on": "extra.metadata.customer.case_id"},
    )

    assert session.external_id == "project-1:case-9"


def test_accepts_json_pointer_join_path() -> None:
    """Accept JSON Pointer syntax for keys containing dots or slashes."""
    record = run(
        "root",
        "trace",
        thread_id=None,
        inputs="hello",
        metadata={"external/session.id": "session-7"},
    )

    [session] = sessions(
        jsonl(record),
        {"join_on": "/extra/metadata/external~1session.id"},
    )

    assert session.external_id == "project-1:session-7"


def test_falls_back_to_trace_id_with_warning() -> None:
    """Keep unthreaded traces separate and record the grouping assumption."""
    parsed = sessions(
        jsonl(
            run("root-1", "trace-1", thread_id=None, inputs="one"),
            run("root-2", "trace-2", thread_id=None, inputs="two"),
        )
    )

    assert [session.external_id for session in parsed] == [
        "project-1:trace-1",
        "project-1:trace-2",
    ]
    assert all(
        "grouped by trace id" in session.metadata["normalization_warnings"][0]
        for session in parsed
    )


def test_isolates_trace_missing_selected_join_value() -> None:
    """Preserve valid traces when another lacks the selected grouping field."""
    valid = run(
        "valid-root",
        "valid-trace",
        thread_id=None,
        inputs="valid",
        metadata={"case_id": "case-1"},
    )
    invalid = run(
        "invalid-root",
        "invalid-trace",
        thread_id=None,
        inputs="invalid",
    )
    parsed = LangSmithRunImporter().parse(
        jsonl(valid, invalid), {"join_on": "extra.metadata.case_id"}
    )

    assert [item.external_id for item in parsed if isinstance(item, ParsedSession)] == [
        "project-1:case-1"
    ]
    [failure] = [item for item in parsed if isinstance(item, ImportFailure)]
    assert failure.external_id == "invalid-trace"
    assert "join_on path" in failure.error


def test_marks_implicit_tool_activity_partial() -> None:
    """Avoid a ready result when tool calls have no explicit tool runs."""
    [session] = sessions(
        jsonl(
            run("root", "trace", inputs="hello"),
            run(
                "llm",
                "trace",
                parent_run_id="root",
                run_type="llm",
                inputs={"messages": []},
                outputs={"tool_calls": [{"name": "search", "args": {}}]},
            ),
        )
    )

    assert "no explicit tool runs" in session.metadata["normalization_warnings"][0]


def test_maps_failed_root_status_and_error() -> None:
    """Use the latest root outcome as the session outcome."""
    record = run("root", "trace", inputs="hello")
    record.update(status="error", error="model unavailable")

    [session] = sessions(jsonl(record))

    assert session.status is SessionStatus.FAILED
    assert session.error == "model unavailable"
    assert session.nodes[0].status is NodeStatus.FAILED


def test_parses_query_envelope_and_json_encoded_payloads() -> None:
    """Accept run-query envelopes and JSON-encoded input/output fields."""
    record = run(
        "root",
        "trace",
        inputs=json.dumps({"message": "hello"}),
        outputs=json.dumps({"answer": "hi"}),
    )

    [session] = sessions(json.dumps({"runs": [record]}).encode())

    assert session.inputs["turns"][0]["inputs"] == {"message": "hello"}
    assert session.outputs == {"answer": "hi"}


def test_uses_terminal_child_output_when_root_has_none() -> None:
    """Surface a model result when the trace wrapper omits its output."""
    [session] = sessions(
        jsonl(
            run("root", "trace", inputs={"message": "hello"}),
            run(
                "model",
                "trace",
                parent_run_id="root",
                run_type="llm",
                inputs={"messages": [{"role": "user", "content": "hello"}]},
                outputs={"role": "assistant", "content": "hi"},
            ),
        )
    )

    assert session.outputs == {"role": "assistant", "content": "hi"}


def test_node_order_is_stable_for_out_of_order_rows() -> None:
    """Produce the same node order regardless of export row order."""
    root = run("root", "trace", inputs="hello")
    child = run(
        "child",
        "trace",
        parent_run_id="root",
        run_type="tool",
        inputs={"query": "x"},
        outputs={"result": "y"},
    )

    forward = sessions(jsonl(root, child))[0]
    reverse = sessions(jsonl(child, root))[0]

    assert [node.external_id for node in forward.nodes] == [
        node.external_id for node in reverse.nodes
    ]


def test_source_instance_override_supports_exports_without_project_id() -> None:
    """Use an explicit source instance when the export omits project identity."""
    record = run("root", "trace", project_id=None, inputs="hello")

    [session] = sessions(jsonl(record), {"source_instance": "selected-project"})

    assert session.external_id == "selected-project:thread-1"


def test_unified_parse_yields_worker_contract_models() -> None:
    """Expose parsed sessions through the standard plugin entrypoint."""
    parsed = list(parse(jsonl(run("root", "trace", inputs="hello")), {}))

    assert len(parsed) == 1
    assert isinstance(parsed[0], ParsedSession)


def test_rejects_oversized_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """Enforce the upload limit before decoding."""
    monkeypatch.setattr(langsmith_module, "MAX_UPLOAD_BYTES", 3)

    with pytest.raises(InvalidImport, match="50 MiB upload limit"):
        LangSmithRunImporter().parse(b"1234", {})
