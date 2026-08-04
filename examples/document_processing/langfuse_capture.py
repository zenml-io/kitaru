"""Capture PydanticAI baselines in Langfuse and export their traces."""

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

from pydantic_ai import Agent

from examples.document_processing.corpus import CASES
from examples.document_processing.extractor import (
    BASELINE_INSTRUCTIONS,
    build_agent,
    build_prompt,
)

BASELINE_MODEL = os.environ.get("BASELINE_MODEL", "openai:gpt-5-nano")


def _require_langfuse_environment() -> None:
    """Raise a focused error when tracing credentials are absent."""
    required = ("LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        names = ", ".join(missing)
        raise RuntimeError(f"Set {names} before running the example.")


def _get_trace(client: Any, trace_id: str) -> Any:
    """Wait until one flushed trace is queryable through Langfuse."""
    deadline = time.monotonic() + 60
    while True:
        try:
            trace = client.api.trace.get(trace_id)
            if trace.observations:
                return trace
        except Exception:
            if time.monotonic() >= deadline:
                raise
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"Langfuse trace {trace_id} was not queryable after 60s."
            )
        time.sleep(2)


async def capture_baselines(export_path: Path) -> Path:
    """Run baseline extraction and write Langfuse trace JSONL."""
    _require_langfuse_environment()
    from langfuse import Langfuse, propagate_attributes

    langfuse = Langfuse()
    Agent.instrument_all()
    agent = build_agent(BASELINE_MODEL, BASELINE_INSTRUCTIONS)
    trace_ids: list[str] = []

    for case in CASES:
        trace_input = case.replay_input().model_dump(mode="json")
        with (
            propagate_attributes(
                session_id=case.document_id,
                trace_name="standards-document-extraction",
                environment="baseline",
                version="prompt-v1",
                tags=[
                    "control",
                    "document-processing",
                    "kitaru-example",
                    "replay-ready",
                ],
                metadata={
                    "document_id": case.document_id,
                    "source_url": case.url,
                    "source_sha256": case.sha256,
                },
            ),
            langfuse.start_as_current_observation(
                name="extract-document",
                as_type="agent",
                input=trace_input,
            ) as root,
        ):
            result = await agent.run(build_prompt(case.path))
            output = result.output.model_dump(mode="json")
            root.update(output=output)
            root.set_trace_io(input=trace_input, output=output)
            trace_id = langfuse.get_current_trace_id()
            if trace_id is None:
                raise RuntimeError("Langfuse did not create a trace id.")
            trace_ids.append(trace_id)

    langfuse.flush()
    traces = await asyncio.gather(
        *(asyncio.to_thread(_get_trace, langfuse, trace_id) for trace_id in trace_ids)
    )
    export_path.parent.mkdir(parents=True, exist_ok=True)
    export_path.write_text(
        "\n".join(
            json.dumps(trace.model_dump(mode="json", by_alias=True)) for trace in traces
        )
        + "\n"
    )
    return export_path


def _get_args() -> argparse.Namespace:
    """Parse the output path for optional trace generation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="Destination JSONL trace export.")
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(capture_baselines(_get_args().output))
