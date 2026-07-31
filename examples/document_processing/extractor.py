"""PydanticAI PDF extraction functions."""

from pathlib import Path

from pydantic_ai import Agent, BinaryContent

from examples.document_processing.models import DocumentRecord

BASELINE_INSTRUCTIONS = """Extract the main metadata and categories from the PDF.
Return the publication month as YYYY-MM and keep category names concise.
"""

CANDIDATE_INSTRUCTIONS = """You process standards documents into a catalog.
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


def build_prompt(pdf_path: Path) -> list[str | BinaryContent]:
    """Build a multimodal prompt containing the full PDF."""
    return [
        "Extract the catalog record for this document.",
        BinaryContent(
            data=pdf_path.read_bytes(),
            media_type="application/pdf",
            identifier=pdf_path.name,
        ),
    ]


def build_agent(model: str, instructions: str) -> Agent[None, DocumentRecord]:
    """Build a typed document extraction agent."""
    return Agent(
        model,
        output_type=DocumentRecord,
        instructions=instructions,
        retries=2,
    )
