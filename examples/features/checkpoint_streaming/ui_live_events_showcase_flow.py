"""Real OpenAI Agents SDK showcase for Kitaru UI live events.

Run this when you want the local Kitaru UI Live Events panel to show a real
agent stream, not a simulated one. The flow emits a few normal Kitaru checkpoint
progress/custom events around an OpenAI Agents SDK streaming run.

Terminal:
    cd examples/features/checkpoint_streaming
    export OPENAI_API_KEY=sk-...
    CHECKPOINT_STREAMING_DELAY_SECONDS=1.5 \
    OPENAI_SHOWCASE_MIN_WORDS=320 \
        uv run python ui_live_events_showcase_flow.py

If the active Kitaru stack runs remotely, the remote runtime also needs access
to OPENAI_API_KEY. A key in your laptop shell is enough only when this code runs
locally.
"""

import os
import sys
import time
from typing import Any

from agents import Agent, RunConfig

import kitaru
from kitaru.adapters.openai_agents import KitaruRunner, OpenAIRunRequest

DEFAULT_DELAY_SECONDS = 1.25
DEFAULT_MIN_WORDS = 320
DEFAULT_TOPIC = "Aria and Blupus, two cats sharing a quiet afternoon adventure"


def delay_seconds() -> float:
    """Return the delay between visible live-event beats."""
    return float(
        os.environ.get("CHECKPOINT_STREAMING_DELAY_SECONDS", DEFAULT_DELAY_SECONDS)
    )


def min_words() -> int:
    """Return the requested minimum final-answer length."""
    return int(os.environ.get("OPENAI_SHOWCASE_MIN_WORDS", DEFAULT_MIN_WORDS))


def pause(multiplier: float = 1.0) -> None:
    """Sleep long enough that the UI can visibly update."""
    time.sleep(delay_seconds() * multiplier)


def require_openai_api_key() -> None:
    """Fail early when the local process clearly cannot call OpenAI."""
    if os.getenv("OPENAI_API_KEY"):
        return
    raise SystemExit(
        "Missing OPENAI_API_KEY.\n"
        "Set it first, then rerun:\n"
        "  export OPENAI_API_KEY='sk-...'"
    )


@kitaru.checkpoint
def scout_topic(topic: str) -> dict[str, Any]:
    """Gather a small prompt packet before asking the agent to write."""
    kitaru.progress("Opening the research notebook", percent=0.05, flush=True)
    pause()

    kitaru.progress("Collecting candidate angles", percent=0.25, angles=4, flush=True)
    pause()

    kitaru.events.publish(
        "demo.source.batch.ready",
        {"source_count": 5, "best_source": "field notes"},
        message="Five source snippets are ready",
        flush=True,
    )
    pause()

    kitaru.progress("Choosing the strongest story arc", percent=0.6, flush=True)
    pause()

    kitaru.progress("Research packet ready", percent=1.0, flush=True)
    return {
        "topic": topic,
        "angle": "practical field guide",
        "audience": "curious builders",
        "source_count": 5,
        "notes": [
            (
                "Aria is an older tortoiseshell cat: cautious, observant, "
                "secretly affectionate."
            ),
            (
                "Blupus is a ginger cat with big energy and a talent for "
                "demanding attention."
            ),
            (
                "Keep the story cozy and specific enough that the stream is "
                "fun to watch unfold."
            ),
        ],
    }


def build_showcase_agent(word_count: int) -> Agent:
    """Create the real OpenAI Agents SDK agent used by the demo."""
    model = os.environ.get("OPENAI_AGENTS_MODEL", "gpt-5-nano")
    return Agent(
        name="kitaru_live_events_writer",
        instructions=(
            "You write warm, vivid short stories for a live product demo. "
            f"Write at least {word_count} words. "
            "Use plain language, concrete images, and short paragraphs. "
            "Tell a gentle story with sensory detail, small moments of tension, "
            "and a satisfying ending. Do not use markdown tables. "
            "The point of the response is to make token streaming visible in a UI, "
            "so write enough continuous prose that the stream is easy to watch."
        ),
        model=model,
    )


def stream_agent_brief(topic: str) -> str:
    """Ask a real OpenAI agent to stream the final brief through Kitaru."""
    word_count = min_words()
    runner = KitaruRunner(
        build_showcase_agent(word_count),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    request = OpenAIRunRequest.start(
        "Write the showcase story now.\n\n"
        f"Topic: {topic}\n"
        f"Minimum length: {word_count} words\n"
        "Audience: people watching a Kitaru live-events UI demo\n"
        "Notes:\n"
        "- Aria is an older tortoiseshell cat: cautious, observant, "
        "secretly affectionate.\n"
        "- Blupus is a ginger cat with big energy and a talent for demanding "
        "attention.\n"
        "- Keep the story cozy, specific, and continuous enough that streaming "
        "is fun to watch."
    )
    result = runner.run_stream_sync(request)
    if result.status != "completed":
        raise RuntimeError(
            f"Expected completed OpenAI run, got status={result.status!r}."
        )
    return str(result.final_output)


@kitaru.checkpoint
def package_takeaways(agent_brief: str) -> str:
    """Add final custom events so the UI has varied event rows."""
    kitaru.progress("Extracting takeaways", percent=0.25, flush=True)
    pause()

    takeaways = [
        "Progress events explain what the checkpoint is doing.",
        "Custom events mark moments users may care about later.",
        "OpenAI stream events show the response as the model writes it.",
    ]
    for takeaway in takeaways:
        kitaru.events.publish(
            "demo.takeaway.ready",
            {"takeaway": takeaway},
            message=takeaway,
            flush=True,
        )
        pause(0.8)

    kitaru.progress("Showcase result packaged", percent=1.0, flush=True)
    return f"{agent_brief}\n\nTakeaways:\n- " + "\n- ".join(takeaways)


@kitaru.flow(cache=False)
def ui_live_events_showcase(topic: str) -> str:
    """Run a rich live-events demo for the Kitaru UI."""
    scout_topic(topic)
    agent_brief = stream_agent_brief(topic)
    return package_takeaways(agent_brief)


def run_workflow(topic: str = DEFAULT_TOPIC) -> tuple[str, str]:
    """Start the showcase flow and wait for the final result."""
    handle = ui_live_events_showcase.run(topic)
    print(f"Execution ID: {handle.exec_id}", flush=True)
    print(
        "Open this execution in the local UI and watch the Live events panel.",
        flush=True,
    )
    result = handle.wait()
    return handle.exec_id, result


def main(argv: list[str] | None = None) -> int:
    """Run the showcase from the command line."""
    require_openai_api_key()
    args = list(sys.argv[1:] if argv is None else argv)
    topic = args[0] if args else DEFAULT_TOPIC
    _execution_id, result = run_workflow(topic)
    print("\nFinal result:\n")
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
