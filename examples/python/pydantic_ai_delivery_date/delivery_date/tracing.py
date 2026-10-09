"""Capture native PydanticAI traces with replayable delivery inputs."""

import asyncio
import json
import os
from typing import Any

from pydantic_ai import Agent

from .models import RunResult, Scenario
from .runner import run_scenario


def build_trace_input(
    result: RunResult, *, continue_after_boundary: bool
) -> dict[str, Any]:
    """Serialize the resolved execution inputs, including the frozen seed."""
    return {
        "schema_version": "delivery-date-run.v1",
        "scenario_snapshot": result.scenario.model_dump(mode="json"),
        "scenario_sha256": result.scenario_hash,
        "input_seed": result.input_seed,
        "variant": result.variant,
        "mode": result.mode,
        "backend": result.backend,
        "model_name": result.model_name,
        "continue_after_boundary": continue_after_boundary,
    }


def build_trace_output(result: RunResult) -> dict[str, Any]:
    """Serialize the actual run outcome and a readable conversation."""
    return result.model_dump(mode="json") | {
        "transcript_text": "\n".join(
            f"{message.role}: {message.content}"
            for message in result.visible_transcript
        )
    }


def require_langfuse_environment() -> None:
    """Require tracing credentials without exposing their values."""
    missing = [
        name
        for name in ("LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY")
        if not os.environ.get(name)
    ]
    if missing:
        raise ValueError(f"Set {', '.join(missing)} before using --source langfuse")


async def trace_scenario(
    scenario: Scenario,
    *,
    variant: str,
    backend: str = "openai",
    model: str = "gpt-6-luna",
    mode: str = "full",
    continue_after_boundary: bool = False,
    timeout: float = 180,
) -> tuple[RunResult, bytes, str | None]:
    """Execute, flush, and export a complete native Langfuse trace."""
    require_langfuse_environment()
    from kitaru_langfuse_importer.api import serialize_trace, wait_for_trace
    from langfuse import Langfuse, get_client, propagate_attributes
    from langfuse.types import TraceContext

    client = get_client()
    trace_id = Langfuse.create_trace_id()
    # PydanticAI emits agent, generation, and local tool spans under this root.
    Agent.instrument_all()
    with propagate_attributes(
        session_id=f"delivery-{trace_id}",
        tags=[
            "delivery-date",
            "scripted-fixture" if backend == "scripted" else "model-run",
        ],
        metadata={"demo": "delivery-date", "backend": backend},
    ):
        with client.start_as_current_observation(
            name=f"Delivery {scenario.name}: {variant}",
            trace_context=TraceContext(trace_id=trace_id),
        ) as root:
            result = await run_scenario(
                scenario,
                variant=variant,
                backend=backend,
                model=model,
                mode=mode,
                continue_after_boundary=continue_after_boundary,
            )
            inputs = build_trace_input(
                result, continue_after_boundary=continue_after_boundary
            )
            outputs = build_trace_output(result)
            root.update(input=inputs, output=outputs)
            # The existing importer reads legacy trace I/O as well as observations.
            client.set_current_trace_io(input=inputs, output=outputs)
    await asyncio.to_thread(client.flush)
    try:
        async with asyncio.timeout(timeout):
            while True:
                trace = await wait_for_trace(trace_id)
                payload = serialize_trace(trace)
                document = json.loads(payload)
                # Trace IO can arrive after the completed observation graph is visible.
                if (
                    document.get("input") == inputs
                    and document.get("output") == outputs
                ):
                    break
                await asyncio.sleep(2)
    except TimeoutError as exc:
        raise TimeoutError(
            f"Langfuse trace {trace_id} did not expose complete run inputs/output "
            f"within {timeout} seconds; recover the existing trace before running again"
        ) from exc
    return result, payload, client.get_trace_url(trace_id=trace_id)
