#!/usr/bin/env python3
"""Create a temporary Langfuse JSONL export with namespaced trace IDs."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


def _remap_id(value: str, namespace: str) -> str:
    """Return a stable hexadecimal ID with the original length."""
    digest = hashlib.sha256(f"{namespace}\0{value}".encode()).hexdigest()
    return digest[: len(value)]


def _remap_trace(record: dict[str, Any], namespace: str) -> dict[str, Any]:
    """Remap one trace and its observation references."""
    trace_id = record.get("id")
    if not isinstance(trace_id, str) or not trace_id:
        raise ValueError("Every trace must contain a non-empty string id.")
    record["id"] = _remap_id(trace_id, namespace)

    session_id = record.get("sessionId")
    if isinstance(session_id, str) and session_id:
        record["sessionId"] = f"{namespace}-{session_id}"

    observations = record.get("observations", [])
    if not isinstance(observations, list):
        raise ValueError("Trace observations must be a list.")
    for observation in observations:
        if not isinstance(observation, dict):
            raise ValueError("Every observation must be an object.")
        observation_trace_id = observation.get("traceId")
        if isinstance(observation_trace_id, str) and observation_trace_id:
            observation["traceId"] = _remap_id(observation_trace_id, namespace)
    return record


def remap_export(source: Path, output: Path, namespace: str) -> int:
    """Write a namespaced copy of a Langfuse JSONL export."""
    if source.resolve() == output.resolve():
        raise ValueError("Output must differ from the source export.")

    lines: list[str] = []
    for line_number, line in enumerate(source.read_text().splitlines(), start=1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"Line {line_number} must contain a JSON object.")
        lines.append(json.dumps(_remap_trace(value, namespace), separators=(",", ":")))

    output.write_text("\n".join(lines) + "\n")
    return len(lines)


def main() -> None:
    """Parse arguments and write the temporary export."""
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args()
    count = remap_export(args.source, args.output, args.namespace)
    print(f"Wrote {count} traces to {args.output}")


if __name__ == "__main__":
    main()
