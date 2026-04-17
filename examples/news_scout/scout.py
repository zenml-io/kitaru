"""News scout v2 — agentic news monitor on Kitaru + PydanticAI (granular mode).

A PydanticAI agent with 4 tools autonomously searches news sources, investigates
articles, and judges what is worth surfacing. Uses ``KitaruAgent`` with
``granular_checkpoints=True`` so every model and tool call becomes its own
Kitaru checkpoint — each one replayable, cached, and visible in the dashboard.

Granular mode requires the agent to run at flow scope (not inside a parent
checkpoint), so memory is read detached — outside the flow — and interests +
seen fingerprints are passed in as flow arguments with concrete values.

Usage::

    python scout.py --seed-profile       # one-time: seed the user profile
    python scout.py                       # run one agentic sweep
    python scout.py --interests ai,llms   # override interests for this run
"""

import argparse
import os
import sys

from utils import load_dotenv

# Load .env BEFORE any provider SDK touches the environment.
load_dotenv()

from prompts import SYSTEM_PROMPT, build_user_prompt  # noqa: E402
from pydantic_ai import Agent  # noqa: E402
from pydantic_ai.usage import UsageLimits  # noqa: E402
from tools import fetch_url, investigate, search_news, search_twitter  # noqa: E402

from kitaru import ImageSettings, flow, memory  # noqa: E402
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
# image when running on remote stacks (Kubernetes, Vertex, etc.). Only vars
# present locally at module load are forwarded — nothing secret leaves this
# file that isn't already in your shell / `.env`.
_ENV_VARS_TO_PROPAGATE = (
    "ANTHROPIC_API_KEY",
    "OPENAI_API_KEY",
    "XAI_API_KEY",
    "KITARU_SCOUT_MODEL",
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
# Flow — agent runs at flow scope so granular mode can open per-call checkpoints
# ---------------------------------------------------------------------------

SCOUT_IMAGE = ImageSettings(
    requirements=[
        "pydantic-ai>=1.80",
        "openai>=1.0",
    ],
    environment=_collect_env(),
)


@flow(image=SCOUT_IMAGE)
def news_scout(interests: list[str], seen_fingerprints: list[str]) -> None:
    """Agentic news scout. Agent runs at flow scope; each tool call is its own
    Kitaru checkpoint (replayable, cached, visible in the dashboard)."""
    user_prompt = build_user_prompt(interests, seen_fingerprints)
    result = scout_agent.run_sync(
        user_prompt,
        usage_limits=UsageLimits(request_limit=MAX_REQUESTS),
    )

    print()
    print("=" * 72)
    print("News scout report")
    print("=" * 72)
    print(result.output)
    print()


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

    # Detached memory reads — both keys live in the same namespace scope so
    # concrete values can be passed into the flow.
    memory.configure(scope=NAMESPACE, scope_type="namespace")
    interests_from_memory = memory.get("interests")
    seen_fingerprints = memory.get("seen_fingerprints") or []
    interests = override or interests_from_memory or DEFAULT_INTERESTS

    news_scout.run(interests=interests, seen_fingerprints=seen_fingerprints)
    return 0


if __name__ == "__main__":
    sys.exit(main())
