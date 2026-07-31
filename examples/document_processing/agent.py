"""Candidate PydanticAI document extractor run by Kitaru workers."""

import asyncio
import os
import uuid
from pathlib import Path
from typing import Any

from examples.document_processing.extractor import (
    CANDIDATE_INSTRUCTIONS,
    build_agent,
    build_prompt,
)
from examples.document_processing.models import DocumentInput
from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.task import get_task_inputs

REPOSITORY_ROOT = Path(__file__).parents[2]
CANDIDATE_MODEL = os.environ.get("CANDIDATE_MODEL", "openai:gpt-5-mini")


def get_document_input(value: Any) -> DocumentInput:
    """Unwrap the latest Langfuse turn into the candidate input."""
    if isinstance(value, dict) and isinstance(value.get("turns"), list):
        turns = value["turns"]
        if not turns:
            raise ValueError("The imported Langfuse session has no turns.")
        value = turns[-1].get("inputs")
    return DocumentInput.model_validate(value)


async def main() -> None:
    """Extract one replayed PDF and record the result in Kitaru."""
    document = get_document_input(get_task_inputs())
    pdf_path = REPOSITORY_ROOT / document.pdf_path
    pydantic_agent = build_agent(CANDIDATE_MODEL, CANDIDATE_INSTRUCTIONS)
    version_value = os.environ.get("KITARU_AGENT_VERSION_ID")
    agent = KitaruAgent(
        pydantic_agent,
        agent_id=uuid.UUID(os.environ["KITARU_AGENT_ID"]),
        agent_version_id=uuid.UUID(version_value) if version_value else None,
        session_name=f"Document extraction: {document.document_id}",
    )
    result = await agent.run(build_prompt(pdf_path))
    print(result.output.model_dump_json())


if __name__ == "__main__":
    asyncio.run(main())
