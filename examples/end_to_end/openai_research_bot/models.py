"""Typed contracts shared by the research bot stages."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class WebSearchItem(BaseModel):
    """One planned web search."""

    model_config = ConfigDict(extra="forbid")

    reason: str = Field(description="Why this search helps answer the research query.")
    query: str = Field(description="The search term to send to the search stage.")


class WebSearchPlan(BaseModel):
    """Planner output: the searches needed to answer a query."""

    model_config = ConfigDict(extra="forbid")

    searches: list[WebSearchItem] = Field(
        description="A short list of web searches to perform."
    )


class SearchSummary(BaseModel):
    """Durable output from one parallel search checkpoint."""

    model_config = ConfigDict(extra="forbid")

    index: int
    query: str
    reason: str
    status: Literal["completed", "failed"]
    summary: str
    error_message: str | None = None


class ReportData(BaseModel):
    """Writer output shown to the user and saved as the final report."""

    model_config = ConfigDict(extra="forbid")

    short_summary: str = Field(description="A 2-3 sentence summary of the findings.")
    markdown_report: str = Field(description="The final report in Markdown.")
    follow_up_questions: list[str] = Field(
        description="Suggested topics the user may want to research next."
    )
