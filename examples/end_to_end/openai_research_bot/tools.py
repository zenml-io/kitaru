"""Local tools used by the OpenAI Agents SDK search agent."""

import os
from typing import Any

from agents import FunctionTool, function_tool

DEFAULT_SEARCH_TOOL_MODEL = "gpt-5-nano"
SEARCH_TOOL_MODEL_ENV = "OPENAI_RESEARCH_BOT_SEARCH_TOOL_MODEL"


def _search_tool_model(model: str | None = None) -> str:
    """Return the model used by the local web-search helper."""
    return model or os.getenv(SEARCH_TOOL_MODEL_ENV, DEFAULT_SEARCH_TOOL_MODEL)


class SearchWebError(RuntimeError):
    """Raised when the local web-search helper cannot complete safely."""


def _safe_error_message(error: BaseException) -> str:
    """Convert provider/network errors into a short, non-secret message."""
    message = str(error)
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        message = message.replace(api_key, "[redacted]")
    if len(message) > 500:
        message = f"{message[:497]}..."
    return f"{type(error).__name__}: {message}"


def _collect_source_urls(response: Any) -> list[str]:
    """Best-effort extraction of web-search sources from a Responses API result."""
    sources = getattr(response, "sources", None)
    if isinstance(sources, list):
        urls = [getattr(source, "url", None) for source in sources]
        return [url for url in urls if isinstance(url, str) and url]
    return []


def _run_web_search(query: str, *, model: str | None = None) -> str:
    """Search the web for a query and return concise source-grounded notes."""
    if not os.getenv("OPENAI_API_KEY"):
        raise SearchWebError(
            "OPENAI_API_KEY is not available in this runtime. For local runs, "
            "export OPENAI_API_KEY. For remote stacks, create a Kitaru secret "
            "and pass it through secret_environment_from."
        )

    try:
        from openai import OpenAI

        response = OpenAI().responses.create(
            model=_search_tool_model(model),
            tools=[{"type": "web_search"}],
            input=(
                "Search the web for the query below. Return concise, factual notes "
                "with enough detail for a later writer to synthesize a report. "
                "Keep any inline citations provided by the API visible.\n\n"
                f"Query: {query}"
            ),
        )
    except Exception as error:  # pragma: no cover - provider/network defensive path
        raise SearchWebError(_safe_error_message(error)) from error

    output_text = getattr(response, "output_text", "")
    if not isinstance(output_text, str) or not output_text.strip():
        output_text = str(response)

    source_urls = _collect_source_urls(response)
    if not source_urls:
        return output_text

    sources = "\n".join(f"- {url}" for url in source_urls[:10])
    return f"{output_text}\n\nSources consulted:\n{sources}"


def new_search_web_tool(*, model: str | None = None) -> FunctionTool:
    """Build a search_web tool bound to the requested Responses API model."""

    @function_tool(name_override="search_web", failure_error_function=None)
    def search_web(query: str) -> str:
        """Search the web for a query and return concise source-grounded notes."""
        return _run_web_search(query, model=model)

    return search_web


search_web = new_search_web_tool()
