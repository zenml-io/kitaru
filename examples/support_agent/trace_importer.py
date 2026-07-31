# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Import the example support trace export."""

import json
from collections.abc import Iterator
from typing import Any

from kitaru.api_models.v1.session import SessionStatus, TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ParsedNode, ParsedSession


def parse(payload: bytes, params: dict[str, Any]) -> Iterator[ParsedSession]:
    """Parse one support session from each JSONL record.

    Args:
        payload: UTF-8 JSONL export.
        params: Import parameters. The example accepts a source label.

    Yields:
        Parsed support sessions.
    """
    source = str(params.get("source", "supportdesk"))
    for line_number, raw_line in enumerate(payload.decode().splitlines(), start=1):
        if not raw_line.strip():
            continue
        record = json.loads(raw_line)
        inputs = record["inputs"]
        output = record["outputs"]
        external_id = record["external_id"]
        yield ParsedSession(
            status=SessionStatus.COMPLETED,
            name=f"Support ticket {external_id}",
            inputs=inputs,
            outputs=output,
            expected=record["expected"],
            error=None,
            started_at=None,
            ended_at=None,
            external_id=external_id,
            metadata={
                **record.get("metadata", {}),
                "import_source": source,
                "source_line": line_number,
            },
            nodes=[
                ParsedNode(
                    external_id=f"{external_id}-run",
                    trace_id=external_id,
                    node_type=NodeType.SPAN,
                    name="support_agent.run",
                    status=NodeStatus.COMPLETED,
                    inputs=inputs,
                    outputs=output,
                    attributes={"environment": "production"},
                    children=[
                        ParsedNode(
                            external_id=f"{external_id}-llm",
                            trace_id=external_id,
                            node_type=NodeType.LLM_CALL,
                            name="respond",
                            status=NodeStatus.COMPLETED,
                            inputs=inputs["question"],
                            outputs=output,
                            requested_model="legacy-support-model",
                            model="legacy-support-model",
                            provider="example",
                            tokens=TokenUsage(input_tokens=24, output_tokens=12),
                            attributes={},
                        )
                    ],
                )
            ],
        )
