"""Phase 17 example: wrap a PydanticAI agent with Kitaru tracking.

This example uses `pydantic_ai.models.test.TestModel`, so it does not need API keys.

Run with:

    uv sync --extra local --extra pydantic-ai
    uv run python -m examples.pydantic_ai_adapter
"""

from typing import Any, cast

from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel
from zenml.client import Client

import kitaru
from kitaru.adapters import pydantic_ai as kp


def gather_context() -> str:
    """Simple tool used by the test model during the agent loop."""
    return "context-ready"


research_agent = kp.wrap(
    Agent(
        TestModel(),
        name="researcher",
        tools=[gather_context],
    ),
    tool_capture_config={"mode": "full"},
)


@kitaru.checkpoint(type="llm_call")
def run_research(topic: str) -> str:
    """Run one wrapped agent iteration inside an explicit checkpoint boundary."""
    result = research_agent.run_sync(f"Research {topic} and summarize findings.")
    return result.output


@kitaru.flow
def research_flow(topic: str) -> str:
    """Run the wrapped agent inside a durable flow."""
    return run_research(topic)


def run_workflow(
    topic: str = "kitaru",
) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
    """Run the workflow and return execution plus adapter diagnostics."""
    handle = research_flow.start(topic)
    raw_result = handle.wait()
    if not isinstance(raw_result, str):
        raise RuntimeError("Expected research_flow() to return a string result.")

    run = Client().get_pipeline_run(handle.exec_id, allow_name_prefix_match=False)
    hydrated_run = run.get_hydrated_version()

    child_events: dict[str, Any] = {}
    run_summaries: dict[str, Any] = {}

    for step in hydrated_run.steps.values():
        events_metadata = step.run_metadata.get("pydantic_ai_events")
        if isinstance(events_metadata, dict):
            child_events.update(cast(dict[str, Any], events_metadata))

        summaries_metadata = step.run_metadata.get("pydantic_ai_run_summaries")
        if isinstance(summaries_metadata, dict):
            run_summaries.update(cast(dict[str, Any], summaries_metadata))

    return handle.exec_id, raw_result, child_events, run_summaries


def main() -> None:
    """Run the example as a script."""
    execution_id, result, child_events, run_summaries = run_workflow()
    print(f"Execution ID: {execution_id}")
    print(f"Result: {result}")
    print(f"Tracked child events: {len(child_events)}")
    print(f"Tracked run summaries: {len(run_summaries)}")


if __name__ == "__main__":
    main()
