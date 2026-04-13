"""News scout v2 — an agentic news monitor powered by PydanticAI + Kitaru.

A PydanticAI agent with 4 tools autonomously searches news sources, investigates
articles, and judges what is worth surfacing. Kitaru handles durable memory
(interests, seen fingerprints) and replay.

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

from models import ScoutContext  # noqa: E402
from prompts import SYSTEM_PROMPT, build_user_prompt  # noqa: E402
from pydantic_ai import Agent  # noqa: E402
from pydantic_ai.usage import UsageLimits  # noqa: E402
from tools import fetch_url, investigate, search_news, search_twitter  # noqa: E402

import kitaru  # noqa: E402
from kitaru import checkpoint, flow, memory  # noqa: E402
from kitaru.adapters import pydantic_ai as kp  # noqa: E402

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

NAMESPACE = "news_scout"
MODEL = os.environ.get("KITARU_SCOUT_MODEL", "anthropic:claude-sonnet-4-6")
MAX_REQUESTS = 50
SEEN_FINGERPRINT_WINDOW = 500

DEFAULT_INTERESTS: list[str] = [
    "artificial intelligence",
    "startups",
    "open source",
    "developer tools",
]

# ---------------------------------------------------------------------------
# Agent — wrapped once at module scope
# ---------------------------------------------------------------------------

scout_agent = kp.wrap(
    Agent(
        MODEL,
        tools=[search_news, search_twitter, investigate, fetch_url],
        system_prompt=SYSTEM_PROMPT,
    ),
    tool_capture_config={"mode": "full"},
)

# ---------------------------------------------------------------------------
# Checkpoints
# ---------------------------------------------------------------------------


@checkpoint
def resolve_context(
    interests_raw: list[str] | None,
    seen_raw: list[str] | None,
    override: list[str] | None,
) -> Annotated[ScoutContext, "scout_context"]:
    """Normalize memory artifact refs into a concrete ScoutContext."""
    interests = override or interests_raw or DEFAULT_INTERESTS
    seen = list(seen_raw) if seen_raw else []
    kitaru.log(
        event="resolve_context",
        interests_count=len(interests),
        seen_count=len(seen),
    )
    return ScoutContext(interests=list(interests), seen_fingerprints=seen)


@checkpoint(type="llm_call")
def run_scout(context: ScoutContext) -> Annotated[str, "scout_report"]:
    """Run the PydanticAI agent. This is the main replay boundary."""
    user_prompt = build_user_prompt(context.interests, context.seen_fingerprints)
    try:
        result = scout_agent.run_sync(
            user_prompt,
            usage_limits=UsageLimits(request_limit=MAX_REQUESTS),
        )
        output = result.output
    except Exception as exc:
        output = f"Agent stopped: {exc}"

    # Print report to console
    print()
    print("=" * 72)
    print("News scout report")
    print("=" * 72)
    print(output)
    print()

    return output


@checkpoint
def update_seen(
    context: ScoutContext,
    report: str,
) -> Annotated[list[str], "seen_fingerprints_out"]:
    """Extend the seen-fingerprint set. Extracts fingerprints mentioned in the report.

    Since the agent returns free text, we can't extract fingerprints from it.
    Instead, we just keep the existing seen set — the agent already skipped
    seen items via the prompt context. Future runs will re-check.
    """
    # For now, just preserve the existing set — the agent was told which
    # fingerprints to skip, so dedup happened at search time.
    kitaru.log(event="update_seen", total=len(context.seen_fingerprints))
    return context.seen_fingerprints


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow
def news_scout(interests_override: list[str] | None = None) -> None:
    """Agentic news scout with durable memory."""
    # --- Memory reads ---
    memory.configure(scope=NAMESPACE, scope_type="namespace")
    interests_raw = memory.get("interests")
    memory.configure(scope_type="flow")
    seen_raw = memory.get("seen_fingerprints")

    # --- Resolve context ---
    context = resolve_context(
        interests_raw=interests_raw,
        seen_raw=seen_raw,
        override=interests_override,
    )

    # --- Agent runs ---
    report = run_scout(context=context)

    # --- Memory write ---
    updated = update_seen(context=context, report=report)
    memory.set("seen_fingerprints", updated)


# ---------------------------------------------------------------------------
# Profile seeding (outside flow)
# ---------------------------------------------------------------------------


def seed_profile(interests: list[str]) -> None:
    """Write interests into namespace memory."""
    memory.configure(scope=NAMESPACE, scope_type="namespace")
    memory.set("interests", interests)
    print(f"Seeded {len(interests)} interests into namespace '{NAMESPACE}':")
    for interest in interests:
        print(f"  - {interest}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    load_dotenv()

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

    override = (
        [p.strip() for p in args.interests.split(",") if p.strip()]
        if args.interests
        else None
    )

    if args.seed_profile:
        seed_profile(override or DEFAULT_INTERESTS)
        return 0

    news_scout.run(interests_override=override)
    return 0


if __name__ == "__main__":
    sys.exit(main())
