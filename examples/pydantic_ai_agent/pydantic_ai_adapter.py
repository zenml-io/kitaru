"""Durable research agent built on Pydantic AI and Kitaru.

A Pydantic AI `Agent` with a structured output, a dependency, and a tool,
wrapped in a one-line `KitaruAgent(...)` and driven from inside a
`@kitaru.flow` + `@kitaru.checkpoint` so the run is replayable, resumable,
and observable via `kitaru executions`.

The pydantic-ai side of this example is intentionally conventional —
the durable-execution layer adds durability without changing the agent.

Uses `pydantic_ai.models.test.TestModel` so no API keys are required;
swap the model for e.g. `'openai:gpt-4o-mini'` in production.

Run:
    cd examples/pydantic_ai_agent
    uv pip install 'kitaru[local,pydantic-ai]'
    python pydantic_ai_adapter.py
"""

from dataclasses import dataclass

from pydantic import BaseModel
from pydantic_ai import Agent, RunContext
from pydantic_ai.models.test import TestModel

from kitaru import checkpoint, flow
from kitaru.adapters.pydantic_ai import KitaruAgent, hitl_tool


@dataclass
class ResearchDeps:
    source_priority: list[str]


class ResearchBrief(BaseModel):
    topic: str
    headline: str
    sources: list[str]


# The `@hitl_tool` marker bridges tool invocations straight to
# `kitaru.wait(...)`. When the model calls this tool the flow pauses for a
# human decision (interactive terminal prompts in-line; remote stacks move
# the execution to `waiting` status). The tool body is skipped — the wait
# return value is what the model sees.
@hitl_tool(question="Approve publishing this brief?", schema=bool)
def publish_brief(headline: str, sources: list[str]) -> str:
    return f"published: {headline} ({len(sources)} sources)"


agent = Agent(
    TestModel(),
    name="researcher",
    deps_type=ResearchDeps,
    output_type=ResearchBrief,
    tools=[publish_brief],
    instructions=(
        "Research the given topic, cite at least one source, and call "
        "`publish_brief` once you are ready to share it. Respect the "
        "operator's source priority."
    ),
)


@agent.tool
def search_index(ctx: RunContext[ResearchDeps], query: str) -> list[str]:
    candidates = [
        "https://example.com/wiki/{query}",
        "https://example.com/news/{query}",
        "https://example.com/blog/{query}",
    ]
    priority = ctx.deps.source_priority

    def rank(url: str) -> int:
        return next((i for i, tag in enumerate(priority) if tag in url), len(priority))

    return [url.format(query=query) for url in sorted(candidates, key=rank)]


researcher = KitaruAgent(agent)


@checkpoint(type="llm_call")
def run_research(topic: str, deps: ResearchDeps) -> ResearchBrief:
    return researcher.run_sync(f"Research {topic!r}.", deps=deps).output


@flow
def research_flow(topic: str) -> ResearchBrief:
    deps = ResearchDeps(source_priority=["wiki", "news"])
    return run_research(topic, deps)


def main() -> None:
    handle = research_flow.run("kitaru")
    brief = handle.wait()
    print(f"exec_id: {handle.exec_id}")
    print(brief.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
