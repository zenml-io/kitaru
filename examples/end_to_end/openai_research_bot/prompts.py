"""Prompts and input builders for the OpenAI research bot example."""

try:  # Package import path used by tests.
    from .models import SearchSummary
except ImportError:  # Direct script path used by README commands.
    from models import SearchSummary


def planner_instructions(max_searches: int) -> str:
    """Build planner instructions with the current cost-control limit."""
    return (
        "You are a careful research planner. Given a user's research query, "
        "create a focused set of web searches that would best answer it. "
        f"Output between 1 and {max_searches} searches. "
        "Each search should have a concrete query string and a brief reason. "
        "Avoid duplicate searches, and prefer searches that will produce "
        "specific, evidence-rich information."
    )


SEARCH_INSTRUCTIONS = (
    "You are a research assistant. You receive one search term and the reason "
    "that search matters. You must call the search_web tool, then summarize the "
    "results in 2-3 concise paragraphs. Keep the summary under 300 words. "
    "Capture the main points, useful details, and any source/citation text the "
    "tool returned. Do not add extra commentary beyond the summary."
)


WRITER_INSTRUCTIONS = (
    "You are a senior researcher. Write a cohesive Markdown report for the "
    "original query using the supplied search summaries. Start by mentally "
    "planning the report structure, then return only the structured output. "
    "The markdown_report should be useful and detailed, with headings, concrete "
    "findings, caveats, and practical takeaways. If any searches failed, mention "
    "what evidence is missing rather than pretending the search succeeded."
)


def build_search_input(query: str, reason: str) -> str:
    """Format one planned search for the search agent."""
    return f"Search term: {query}\nReason for searching: {reason}"


def build_writer_input(original_query: str, summaries: list[SearchSummary]) -> str:
    """Format all search summaries for the writer agent."""
    lines = [
        f"Original query: {original_query}",
        "",
        "Summarized search results:",
    ]
    for summary in summaries:
        lines.extend(
            [
                "",
                f"Search {summary.index + 1}: {summary.query}",
                f"Reason: {summary.reason}",
                f"Status: {summary.status}",
                f"Summary: {summary.summary}",
            ]
        )
        if summary.error_message:
            lines.append(f"Error: {summary.error_message}")
    return "\n".join(lines)
