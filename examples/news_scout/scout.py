"""News scout — agentic news monitor on Kitaru + PydanticAI (granular mode).

A PydanticAI agent with 4 tools autonomously searches news sources, investigates
articles, and judges what is worth surfacing. ``KitaruAgent`` runs in
``granular_checkpoints=True`` mode so every model and tool call becomes its own
Kitaru checkpoint — replayable, cached, visible in the dashboard.

The agent's final report is wrapped in a ``publish_report`` checkpoint so it
shows up as a named ``final_report`` artifact in the dashboard trace.

Usage::

    python scout.py --seed-profile       # one-time: seed the user profile
    python scout.py                       # run one agentic sweep
    python scout.py --interests ai,llms   # override interests for this run
"""

import argparse
import os
import sys
from typing import Annotated

from utils import load_dotenv

# Load .env BEFORE any provider SDK touches the environment.
load_dotenv()

from prompts import SYSTEM_PROMPT, build_user_prompt  # noqa: E402
from pydantic_ai import Agent  # noqa: E402
from pydantic_ai.usage import UsageLimits  # noqa: E402
from tools import fetch_url, investigate, search_news, search_twitter  # noqa: E402

from kitaru import ImageSettings, checkpoint, flow, memory  # noqa: E402
from kitaru.adapters.pydantic_ai import CapturePolicy, KitaruAgent  # noqa: E402

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

NAMESPACE = "news_scout"
MODEL = os.environ.get("KITARU_SCOUT_MODEL", "anthropic:claude-sonnet-4-6")
MAX_REQUESTS = 50

DEFAULT_INTERESTS: list[str] = [
    "artificial intelligence",
    "startups",
    "open source",
    "developer tools",
]

# Env vars to pick up from the local environment and bake into the container
# image when running on remote stacks. Only vars present locally at module load
# are forwarded — nothing secret leaves this file that isn't already in your
# shell / `.env`.
_ENV_VARS_TO_PROPAGATE = (
    "ANTHROPIC_API_KEY",
    "OPENAI_API_KEY",
    "XAI_API_KEY",
    "KITARU_SCOUT_MODEL",
    "KITARU_GROK_MODEL",
)


def _collect_env() -> dict[str, str]:
    """Pick up env vars set locally (after load_dotenv) for the remote image."""
    return {
        key: os.environ[key] for key in _ENV_VARS_TO_PROPAGATE if os.environ.get(key)
    }


# ---------------------------------------------------------------------------
# Agent — granular checkpoint mode: every model/tool call is its own checkpoint
# ---------------------------------------------------------------------------

scout_agent = KitaruAgent(
    Agent(
        MODEL,
        name="news_scout",
        tools=[search_news, search_twitter, investigate, fetch_url],
        system_prompt=SYSTEM_PROMPT,
    ),
    granular_checkpoints=True,
    model_checkpoint_config={"retries": 2},
    tool_checkpoint_config={"retries": 1},
    capture=CapturePolicy(tool_capture="full"),
)

# ---------------------------------------------------------------------------
# Image — declares container requirements + env vars for remote stacks
# ---------------------------------------------------------------------------

SCOUT_IMAGE = ImageSettings(
    requirements=[
        "pydantic-ai>=1.80",
        "openai>=1.0",
    ],
    environment=_collect_env(),
)


# ---------------------------------------------------------------------------
# Checkpoints
# ---------------------------------------------------------------------------


@checkpoint
def publish_report(report_text: str) -> Annotated[str, "final_report"]:
    """Save the agent's output as a named artifact readable in the dashboard.

    The agent's internal model/tool activity is captured as per-call
    checkpoints. This final checkpoint promotes the agent's text output to a
    first-class ``final_report`` artifact on the flow, so readers can pull up
    one run and see its summary without scrolling through every tool call.
    """
    print()
    print("=" * 72)
    print("News scout report")
    print("=" * 72)
    print(report_text)
    print()
    return report_text


# ---------------------------------------------------------------------------
# Flow — agent runs at flow scope so granular mode can open per-call checkpoints
# ---------------------------------------------------------------------------


@flow(image=SCOUT_IMAGE)
def news_scout(interests: list[str]) -> str:
    """Agentic news scout. Each tool call is its own Kitaru checkpoint; the
    final report is saved as the ``final_report`` artifact on this flow."""
    user_prompt = build_user_prompt(interests)
    result = scout_agent.run_sync(
        user_prompt,
        usage_limits=UsageLimits(request_limit=MAX_REQUESTS),
    )
    return publish_report(report_text=result.output)


# ---------------------------------------------------------------------------
# Profile seeding (detached — outside the flow, namespace-scoped memory)
# ---------------------------------------------------------------------------


def seed_profile(interests: list[str]) -> None:
    """Write interests into namespace memory. Runs detached (outside any flow)."""
    memory.configure(scope=NAMESPACE, scope_type="namespace")
    memory.set("interests", interests)
    print(f"Seeded {len(interests)} interests into namespace '{NAMESPACE}':")
    for interest in interests:
        print(f"  - {interest}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_interests(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Kitaru agentic news scout.")
    parser.add_argument(
        "--seed-profile",
        action="store_true",
        help="Write default interests into namespace memory and exit.",
    )
    parser.add_argument(
        "--interests",
        type=str,
        default=None,
        help="Comma-separated interests to override for this run.",
    )
    args = parser.parse_args(argv)

    override = _parse_interests(args.interests)

    if args.seed_profile:
        seed_profile(override or DEFAULT_INTERESTS)
        return 0

    memory.configure(scope=NAMESPACE, scope_type="namespace")
    interests_from_memory = memory.get("interests")
    interests = override or interests_from_memory or DEFAULT_INTERESTS

    news_scout.run(interests=interests)
    return 0


if __name__ == "__main__":
    sys.exit(main())
