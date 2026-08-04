"""PydanticAI document extractor run by a Kitaru worker."""

import asyncio
import os
import uuid
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from pydantic_ai import Agent, BinaryContent

from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.task import get_task_inputs

REPOSITORY_ROOT = Path(__file__).parents[2]
MODEL = os.environ.get("CANDIDATE_MODEL", "openai:gpt-5-mini")
DEFAULT_PROMPT = "Extract the catalog record for this document."

INSTRUCTIONS = """You process standards documents into a catalog.
Read the complete PDF, including its cover and framework overview.

Follow these rules:
1. Copy the complete title from the cover without shortening it.
2. Preserve the publication series in publication_id, for example NIST AI or NIST CSWP.
3. Return publication_month as YYYY-MM. Use the stated publication date,
   not PDF file metadata.
4. Return only the named top-level framework functions, in document order
   and uppercase.
5. Do not return principles, characteristics, categories, subcategories,
   or lifecycle stages as functions.
"""


class DocumentRecord(BaseModel):
    """Structured fields extracted from one standards document."""

    title: str = Field(description="Full title printed on the document cover.")
    publication_id: str = Field(
        description="NIST publication identifier, including its series."
    )
    publication_month: str = Field(description="Publication month in YYYY-MM format.")
    framework_functions: list[str] = Field(
        description="Top-level framework function names in document order."
    )


class DocumentInput(BaseModel):
    """Replay-safe reference to one local PDF."""

    document_id: str
    pdf_path: str
    prompt: str = DEFAULT_PROMPT


def get_document_input(value: Any) -> DocumentInput:
    """Unwrap the latest imported turn into the document input."""
    if isinstance(value, dict) and isinstance(value.get("turns"), list):
        turns = value["turns"]
        if not turns:
            raise ValueError("The imported session has no turns.")
        value = turns[-1].get("inputs")
    return DocumentInput.model_validate(value)


def build_prompt(
    pdf_path: Path, prompt: str = DEFAULT_PROMPT
) -> list[str | BinaryContent]:
    """Build a multimodal prompt containing the PDF."""
    return [
        prompt,
        BinaryContent(
            data=pdf_path.read_bytes(),
            media_type="application/pdf",
            identifier=pdf_path.name,
        ),
    ]


def build_agent(model: str = MODEL) -> Agent[None, DocumentRecord]:
    """Build the typed document extraction agent."""
    return Agent(
        model,
        output_type=DocumentRecord,
        instructions=INSTRUCTIONS,
        retries=2,
    )


async def main() -> None:
    """Extract one replayed PDF and record the result in Kitaru."""
    document = get_document_input(get_task_inputs())
    pdf_path = REPOSITORY_ROOT / document.pdf_path
    pydantic_agent = build_agent()
    version_value = os.environ.get("KITARU_AGENT_VERSION_ID")
    agent = KitaruAgent(
        pydantic_agent,
        agent_id=uuid.UUID(os.environ["KITARU_AGENT_ID"]),
        agent_version_id=uuid.UUID(version_value) if version_value else None,
        session_name=f"Document extraction: {document.document_id}",
    )
    result = await agent.run(build_prompt(pdf_path, document.prompt))
    print(result.output.model_dump_json())


if __name__ == "__main__":
    asyncio.run(main())
