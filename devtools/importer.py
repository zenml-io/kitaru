"""JSONL session importer with configurable failure modes."""

import hashlib
import json
import time
from collections.abc import Iterator
from typing import Any

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.task.importer import ImportedSession, SessionImportError, flatten_nodes

MAX_UPLOAD_BYTES = 64 * 1024 * 1024


def _rate_hit(value: str, salt: str, rate: float) -> bool:
    """Decide an event deterministically from a value digest and a rate."""
    if rate <= 0:
        return False
    digest = hashlib.sha256(f"{value}:{salt}".encode()).hexdigest()
    return int(digest[:8], 16) % 10_000 < rate * 10_000


# A script plugin uploads as one file, so this repeats the flat index mapping
# of the built-in kitaru-jsonl importer instead of importing it. Keep the two
# in step, or files that import here fail on the built-in.
def resolve_indexed_nodes(nodes: Any) -> list[dict[str, Any]]:
    """Map the flat indexed node representation onto external ids.

    Args:
        nodes: Raw node records of one session.

    Raises:
        ValueError: A node lacks an index or nests children, an index repeats,
            or a parent index names no node.

    Returns:
        Node records carrying external ids and parent external ids.
    """
    if not isinstance(nodes, list) or not all(isinstance(node, dict) for node in nodes):
        raise ValueError("nodes must be a list of objects")
    if any(node.get("index") is None or node.get("children") for node in nodes):
        raise ValueError("nodes must use the flat indexed representation")
    external_ids: dict[Any, Any] = {}
    for node in nodes:
        index = node["index"]
        if index in external_ids:
            raise ValueError(f"node index {index} repeats within the session")
        external_id = node.get("external_id")
        external_ids[index] = f"node-{index}" if external_id is None else external_id
    resolved = []
    for node in nodes:
        parent_index = node.get("parent_index")
        if parent_index is not None and parent_index not in external_ids:
            raise ValueError(f"parent_index {parent_index} names no node")
        fields = {
            key: value
            for key, value in node.items()
            if key not in ("index", "parent_index")
        }
        fields["external_id"] = external_ids[node["index"]]
        fields["parent_external_id"] = (
            external_ids[parent_index] if parent_index is not None else None
        )
        resolved.append(fields)
    return resolved


def parse(
    content: bytes, params: dict[str, Any]
) -> Iterator[ImportedSession | ImportFailure]:
    """Parse one session per JSONL record, applying configured failure modes."""
    if params.get("crash"):
        raise RuntimeError("Simulated importer crash")
    fail_line_rate = float(params.get("fail_line_rate", 0.0))
    sleep_ms_per_line = float(params.get("sleep_ms_per_line", 0.0))

    if len(content) > MAX_UPLOAD_BYTES:
        raise ValueError("JSONL import exceeds the 64 MiB upload limit")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("JSONL import must be UTF-8") from exc
    if not text.strip():
        raise ValueError("JSONL import contains no records")

    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        if sleep_ms_per_line > 0:
            time.sleep(sleep_ms_per_line / 1000)
        external_id = None
        try:
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError("record must be a JSON object")
            raw_external_id = value.get("external_id")
            external_id = str(raw_external_id) if raw_external_id is not None else None
            if "nodes" in value:
                value = {**value, "nodes": resolve_indexed_nodes(value["nodes"])}
            session = ImportedSession.model_validate(value)
            flatten_nodes(session.nodes)
            if _rate_hit(session.external_id, "import-fail", fail_line_rate):
                yield ImportFailure(
                    line=line_number,
                    external_id=external_id,
                    error="Simulated import failure",
                )
                continue
            yield session
        except (SessionImportError, ValueError) as exc:
            yield ImportFailure(
                line=line_number,
                external_id=external_id,
                error=f"Invalid session: {exc}",
            )
