"""Shared data models for the document processing example."""

from pydantic import BaseModel, Field


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
