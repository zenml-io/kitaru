"""Phase 7 example: structured metadata with `kitaru.log()`.

This example shows both logging scopes:
- flow-level metadata (execution-level)
- checkpoint-level metadata (step-level)
"""

import kitaru
from kitaru import checkpoint, flow


@checkpoint
def write_draft(topic: str) -> str:
    """Create a draft paragraph for a topic.

    Args:
        topic: Topic to write about.

    Returns:
        Draft text.
    """
    draft = f"Draft about {topic}."
    kitaru.log(draft_cost={"usd": 0.001})
    kitaru.log(draft_cost={"tokens": 42}, model="demo-model", latency_ms=120)
    return draft


@checkpoint
def polish_draft(draft: str) -> str:
    """Polish the generated draft.

    Args:
        draft: Draft text to refine.

    Returns:
        Refined text.
    """
    refined = f"{draft.upper()} [reviewed]"
    kitaru.log(quality_score=0.93)
    return refined


@flow
def writing_agent(topic: str) -> str:
    """Run the logging example workflow.

    Args:
        topic: Topic to write about.

    Returns:
        Final text output.
    """
    kitaru.log(topic=topic, stage="started")
    draft = write_draft(topic)
    result = polish_draft(draft)
    kitaru.log(stage="completed", output_kind="text")
    return result


def run_workflow(topic: str = "kitaru") -> str:
    """Execute the example workflow and return its output.

    Args:
        topic: Topic to write about.

    Returns:
        Workflow output.
    """
    return writing_agent.run(topic).wait()


def main() -> None:
    """Run the example as a script."""
    result = run_workflow()
    print(result)


if __name__ == "__main__":
    main()
