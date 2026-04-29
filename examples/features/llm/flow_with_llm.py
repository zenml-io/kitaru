"""Tracked model calls with ``kitaru.llm()``.

Before running this example, register a model alias and provide credentials:

    kitaru secrets set openai-creds --OPENAI_API_KEY=sk-...
    kitaru model register fast --model openai/gpt-5-nano --secret openai-creds

For quick local testing you can also skip the linked secret and just export
the provider key:

    kitaru model register fast --model openai/gpt-5-nano
    export OPENAI_API_KEY=sk-...
"""

import kitaru
from kitaru import checkpoint, flow


@checkpoint
def write_draft(topic: str, outline: str) -> str:
    """Expand an outline into a short draft paragraph."""
    return kitaru.llm(
        f"Write a short paragraph about {topic} using this outline:\n{outline}",
        model="fast",
        name="draft_call",
    )


@flow
def llm_writer(topic: str) -> str:
    """Generate an outline and then a draft using tracked LLM calls."""
    outline = kitaru.llm(
        f"Create a 3-bullet outline about {topic}.",
        model="fast",
        name="outline_call",
    )
    return write_draft(topic, outline)


def run_workflow(topic: str = "kitaru") -> tuple[str, str]:
    """Run the LLM workflow.

    Returns:
        Tuple of (execution_id, result).
    """
    handle = llm_writer.run(topic)
    result = handle.wait()
    return handle.exec_id, result


def main() -> None:
    """Run the example as a script."""
    execution_id, result = run_workflow()
    print(f"Execution: {execution_id}")
    print(f"Result: {result}")


if __name__ == "__main__":
    main()
