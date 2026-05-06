"""OpenAI Agents SDK agent factories for the research bot."""

from agents import Agent, ModelSettings

try:  # Package import path used by tests.
    from .models import ReportData, WebSearchPlan
    from .prompts import SEARCH_INSTRUCTIONS, WRITER_INSTRUCTIONS, planner_instructions
    from .tools import new_search_web_tool
except ImportError:  # Direct script path used by README commands.
    from models import ReportData, WebSearchPlan
    from prompts import SEARCH_INSTRUCTIONS, WRITER_INSTRUCTIONS, planner_instructions
    from tools import new_search_web_tool

DEFAULT_MODEL = "gpt-5-nano"
SMARTER_MODEL = "gpt-5-mini"


def new_planner_agent(*, model: str, max_searches: int) -> Agent:
    """Create the planner agent that returns a structured search plan."""
    return Agent(
        name="research_planner",
        instructions=planner_instructions(max_searches),
        model=model,
        output_type=WebSearchPlan,
    )


def new_search_agent(*, model: str, search_tool_model: str) -> Agent:
    """Create the search agent that must call the local search_web tool."""
    return Agent(
        name="research_searcher",
        instructions=SEARCH_INSTRUCTIONS,
        model=model,
        tools=[new_search_web_tool(model=search_tool_model)],
        model_settings=ModelSettings(tool_choice="required"),
    )


def new_writer_agent(*, model: str) -> Agent:
    """Create the writer agent that returns the final structured report."""
    return Agent(
        name="research_writer",
        instructions=WRITER_INSTRUCTIONS,
        model=model,
        output_type=ReportData,
    )
