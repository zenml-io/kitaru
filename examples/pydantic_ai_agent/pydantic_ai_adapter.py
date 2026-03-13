"""Wrap a PydanticAI agent with Kitaru tracking.

This example uses ``pydantic_ai.models.test.TestModel``, so it does not
need API keys.

Run with:

    uv sync --extra local --extra pydantic-ai
    uv run python -m examples.pydantic_ai_agent.pydantic_ai_adapter
"""

from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel

from kitaru import checkpoint, flow
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


@checkpoint(type="llm_call")
def run_research(topic: str) -> str:
    """Run one wrapped agent iteration inside an explicit checkpoint boundary."""
    result = research_agent.run_sync(f"Research {topic} and summarize findings.")
    return result.output


@flow
def research_flow(topic: str) -> str:
    """Run the wrapped agent inside a durable flow."""
    return run_research(topic)


def run_workflow(topic: str = "kitaru") -> tuple[str, str]:
    """Run the workflow.

    Returns:
        Tuple of (execution_id, result).
    """
    handle = research_flow.run(topic)
    result = handle.wait()
    return handle.exec_id, result


def main() -> None:
    """Run the example as a script."""
    execution_id, result = run_workflow()
    print(f"Execution ID: {execution_id}")
    print(f"Result: {result}")


if __name__ == "__main__":
    main()
