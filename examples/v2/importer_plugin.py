"""Deterministic JSON Lines importer plugin example."""

import json
from collections.abc import Iterator
from typing import Any

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ParsedNode, ParsedSession


def parse(
    payload: bytes,
    params: dict[str, Any],
) -> Iterator[ParsedSession | ImportFailure]:
    """Parse one session from each JSON Lines record."""
    name_prefix = str(params.get("name_prefix", "Imported"))
    for line_number, raw_line in enumerate(payload.splitlines(), start=1):
        try:
            record = json.loads(raw_line)
            external_id = str(record["id"])
            inputs = record["inputs"]
            outputs = record["outputs"]
        except (KeyError, TypeError, ValueError) as exc:
            yield ImportFailure(line=line_number, error=str(exc))
            continue

        yield ParsedSession(
            status=SessionStatus.COMPLETED,
            name=f"{name_prefix} {external_id}",
            inputs=inputs,
            outputs=outputs,
            expected=record.get("expected"),
            error=None,
            started_at=None,
            ended_at=None,
            external_id=external_id,
            metadata={"source_line": line_number},
            nodes=[
                ParsedNode(
                    node_type=NodeType.SPAN,
                    name="imported_record",
                    status=NodeStatus.COMPLETED,
                    inputs=inputs,
                    outputs=outputs,
                )
            ],
        )
