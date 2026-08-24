"""Run the baseline resolver and export real Langfuse traces."""

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

from pydantic_ai import Agent

from returns_agent.agent import MODEL, build_agent, build_prompt
from returns_agent.fixtures import CASES
from returns_agent.store import MockCommerceStore

REQUEST_OPTIONS = {"timeout_in_seconds": 30, "max_retries": 3}
REDACTED_EXPORT_FIELDS = {
    "gen_ai.agent.call.id",
    "gen_ai.conversation.id",
    "gen_ai.response.id",
    "htmlPath",
    "modelId",
    "projectId",
    "public_key",
    "service.instance.id",
    "usagePricingTierId",
    "usagePricingTierName",
}


def _require_environment() -> None:
    """Raise a focused error when model or tracing credentials are absent."""
    required = (
        "OPENAI_API_KEY",
        "LANGFUSE_PUBLIC_KEY",
        "LANGFUSE_SECRET_KEY",
    )
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"Set {', '.join(missing)} in .env before continuing.")


def _sanitize_export(value: Any) -> Any:
    """Remove credential-shaped telemetry fields from an exported trace."""
    if isinstance(value, dict):
        return {
            key: _sanitize_export(item)
            for key, item in value.items()
            if key not in REDACTED_EXPORT_FIELDS
        }
    if isinstance(value, list):
        return [_sanitize_export(item) for item in value]
    return value


def _get_trace(client: Any, trace_id: str) -> Any:
    """Wait until one flushed trace has a complete observation graph."""
    deadline = time.monotonic() + 180
    while True:
        try:
            trace = client.api.trace.get(trace_id, request_options=REQUEST_OPTIONS)
            observation_ids = {item.id for item in trace.observations}
            root_count = sum(
                item.parent_observation_id is None for item in trace.observations
            )
            parents_available = all(
                item.parent_observation_id is None
                or item.parent_observation_id in observation_ids
                for item in trace.observations
            )
            if trace.observations and root_count == 1 and parents_available:
                return trace
        except Exception:
            if time.monotonic() >= deadline:
                raise
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"Langfuse trace {trace_id} was not queryable after 180 seconds."
            )
        time.sleep(2)


def _get_trace_id(result: Any) -> str:
    """Extract the Langfuse trace ID retained by an instrumented agent run."""
    traceparent = result._traceparent()  # noqa: SLF001
    parts = traceparent.split("-")
    if len(parts) != 4 or len(parts[1]) != 32:
        raise RuntimeError("PydanticAI returned an invalid trace context.")
    return parts[1]


async def generate_traces(export_path: Path) -> Path:
    """Run ten baseline tickets and write their Langfuse traces as JSONL."""
    _require_environment()
    from langfuse import Langfuse, propagate_attributes

    langfuse = Langfuse()
    Agent.instrument_all()
    trace_data: dict[str, tuple[str, dict[str, Any], dict[str, Any]]] = {}

    for ticket in CASES:
        trace_input = ticket.model_dump(mode="json")
        session_id = f"returns-{ticket.ticket_id}"
        with propagate_attributes(
            session_id=session_id,
            trace_name=f"Returns ticket: {ticket.ticket_id}",
            environment="canonical-example",
            version="baseline-v1",
            tags=["returns-resolution", "kitaru-example"],
            metadata={
                "ticket_id": ticket.ticket_id,
                "agent_release": "baseline-v1",
            },
        ):
            result = await build_agent(MockCommerceStore(), MODEL).run(
                build_prompt(ticket)
            )
            output = result.output.model_dump(mode="json")
            trace_data[session_id] = (_get_trace_id(result), trace_input, output)
        langfuse.flush()

    langfuse.flush()
    traces = []
    for session_id in sorted(trace_data):
        trace_id, trace_input, output = trace_data[session_id]
        trace = await asyncio.to_thread(_get_trace, langfuse, trace_id)
        document = trace.model_dump(mode="json", by_alias=True)
        document["input"] = trace_input
        document["output"] = output
        traces.append(document)
    export_path.parent.mkdir(parents=True, exist_ok=True)
    export_path.write_text(
        "\n".join(json.dumps(_sanitize_export(trace)) for trace in traces) + "\n",
        encoding="utf-8",
    )
    return export_path


def _get_args() -> argparse.Namespace:
    """Parse the trace export destination."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="Destination JSONL trace export.")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(generate_traces(_get_args().output))
