"""Wait for input and continue the same execution.

Demonstrates two wait patterns:
1. A boolean gate (approve/reject) using the default schema
2. A structured input wait using a Pydantic schema

When running locally, Kitaru prompts for input directly in the terminal.
When running remotely, use the CLI to provide input and resume:

    kitaru executions input <exec_id> --value true
    kitaru executions resume <exec_id>
"""

from pydantic import BaseModel

import kitaru
from kitaru import checkpoint, flow
from kitaru.runtime import _get_current_execution_id


class ReleaseDetails(BaseModel):
    """Structured input for the release details wait."""

    notes: str
    major_version: int


@checkpoint
def draft_release_note(topic: str) -> str:
    """Create a draft release note for the requested topic."""
    return f"Draft about {topic}."


@checkpoint
def publish_release_note(draft: str, details: ReleaseDetails) -> str:
    """Publish a previously approved draft release note."""
    return f"PUBLISHED v{details.major_version}: {draft}\nNotes: {details.notes}"


@flow
def wait_for_approval_flow(topic: str) -> str:
    """Gate publication behind a durable human-approval wait."""
    draft = draft_release_note(topic)

    exec_id = _get_current_execution_id()
    print("\nTo approve remotely, run in another terminal:")
    print(f"  kitaru executions input {exec_id} --value true")
    print(f"  kitaru executions resume {exec_id}")
    print("(Use --value false to reject.)\n")

    kitaru.wait(
        name="approve_release",
        question=f"Approve publishing release notes for {topic}?",
        timeout=3600,  # Compute is released after 1 hour; resume via CLI later
        metadata={"topic": topic},
    )  # if user approves flow continues, if not flow is suspended

    print("\nTo approve remotely, run in another terminal:")
    example_value = '\'{"notes": "Bug fixes", "major_version": 2}\''
    print(
        f"  kitaru executions input {exec_id}"
        f" --wait release_details --value {example_value}"
    )
    print(f"  kitaru executions resume {exec_id}")
    print("(Use --value false to reject.)\n")

    details = kitaru.wait(
        name="release_details",
        schema=ReleaseDetails,
        question="Provide the release notes and major version number:",
        timeout=60,  # Compute is released after 1 minute; resume via CLI later
        metadata={"topic": topic},
    )

    return publish_release_note(draft, details)


def run_workflow(topic: str = "v1.0") -> str:
    """Execute the wait/resume workflow and return its output."""
    return wait_for_approval_flow.run(topic)


def main() -> None:
    """Run the example as a script."""
    run_workflow()


if __name__ == "__main__":
    main()
