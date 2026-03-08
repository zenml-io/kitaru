"""Phase 12 example: tracked model calls with `kitaru.llm()`.

Before running this example, register a model alias and set credentials:

    kitaru model register fast --model openai/gpt-4o-mini
    export OPENAI_API_KEY=sk-...
"""

from typing import Any, cast

from zenml.client import Client

import kitaru


@kitaru.checkpoint
def write_draft(topic: str, outline: str) -> str:
    """Expand an outline into a short draft paragraph."""
    return kitaru.llm(
        f"Write a short paragraph about {topic} using this outline:\n{outline}",
        model="fast",
        name="draft_call",
    )


@kitaru.flow
def llm_writer(topic: str) -> str:
    """Generate an outline and then a draft using tracked LLM calls."""
    outline = kitaru.llm(
        f"Create a 3-bullet outline about {topic}.",
        model="fast",
        name="outline_call",
    )
    return write_draft(topic, outline)


def run_workflow(topic: str = "kitaru") -> tuple[str, str, list[dict[str, Any]]]:
    """Run the LLM workflow and return execution diagnostics."""
    handle = llm_writer.start(topic)
    raw_result = handle.wait()
    if not isinstance(raw_result, str):
        raise RuntimeError(
            "The flow_with_llm example expected a string result from llm_writer()."
        )

    run = Client().get_pipeline_run(handle.exec_id, allow_name_prefix_match=False)
    hydrated_run = run.get_hydrated_version()
    step_llm_metadata = [
        cast(dict[str, Any], step.run_metadata["llm_calls"])
        for step in hydrated_run.steps.values()
        if "llm_calls" in step.run_metadata
    ]

    return handle.exec_id, raw_result, step_llm_metadata


def main() -> None:
    """Run the example as a script."""
    execution_id, result, step_llm_metadata = run_workflow()
    print(f"Execution: {execution_id}")
    print(f"Result: {result}")
    print(f"LLM metadata entries: {len(step_llm_metadata)}")


if __name__ == "__main__":
    main()
