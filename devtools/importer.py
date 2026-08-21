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
            session = ImportedSession.model_validate(value)
            if any(node.index is None or node.children for node in session.nodes):
                raise ValueError("nodes must use the flat indexed representation")
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
