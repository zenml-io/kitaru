"""News scout — agentic news monitor on Kitaru + PydanticAI (granular mode).

A PydanticAI agent with 4 tools autonomously searches news sources, investigates
articles, and judges what is worth surfacing. ``KitaruAgent`` runs in
``granular_checkpoints=True`` mode so every model and tool call becomes its own
Kitaru checkpoint — replayable, cached, visible in the dashboard.

The agent's final report is wrapped in a ``publish_report`` checkpoint so it
shows up as a named ``final_report`` artifact in the dashboard trace.

Usage::

    python scout.py                       # run one agentic sweep with defaults
    python scout.py --interests ai,llms   # choose interests for this run
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
from rich.console import Console  # noqa: E402
from rich.markdown import Markdown  # noqa: E402
from rich.rule import Rule  # noqa: E402
from tools import fetch_url, investigate, search_news, search_twitter  # noqa: E402

from kitaru import ImageSettings, checkpoint, flow  # noqa: E402
from kitaru.adapters.pydantic_ai import CapturePolicy, KitaruAgent  # noqa: E402
from kitaru.config import classify_stack_deployment_type  # noqa: E402

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MODEL = os.environ.get("KITARU_SCOUT_MODEL", "anthropic:claude-sonnet-4-6")
MAX_REQUESTS = 50

DEFAULT_INTERESTS: list[str] = [
    "AI agents",
    "harness engineering",
    "durable execution",
    "agent frameworks",
    "developer tools",
]

# Provider API keys travel through a ZenML secret referenced by name only, so
# values are resolved at step runtime and never enter Docker image metadata,
# logs, or the frozen execution spec. Create the secret once with:
#   kitaru secrets set news-scout-keys \
#       --ANTHROPIC_API_KEY=sk-ant-... \
#       --XAI_API_KEY=xai-...          # optional, unlocks search_twitter
SECRET_NAME = "news-scout-keys"

# Non-secret config (model overrides) is forwarded via plain image env vars;
# only vars set locally at module load are propagated.
_NON_SECRET_ENV_VARS = (
    "KITARU_SCOUT_MODEL",
    "KITARU_GROK_MODEL",
)


def _collect_non_secret_env() -> dict[str, str]:
    """Forward local non-secret config (model overrides) into remote images."""
    return {key: os.environ[key] for key in _NON_SECRET_ENV_VARS if os.environ.get(key)}


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

# Pydantic-AI is pinned to <1.80 because 1.80+ bumped its opentelemetry-sdk
# floor to >=1.39, but ZenML (Kitaru's backend) hard-pins opentelemetry-sdk
# to 1.38.0. Using the `-slim` variant with explicit provider extras keeps
# the image small and avoids pulling unused provider clients.
SCOUT_IMAGE = ImageSettings(
    requirements=[
        "pydantic-ai-slim[anthropic,openai]>=1.75,<1.80",
    ],
    environment=_collect_non_secret_env(),
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
    # Construct Console lazily so TTY detection happens at call time (so
    # piped/captured output gets plain text, interactive terminals get styled).
    console = Console()
    console.print()
    console.print(Rule("News scout report"))
    console.print(Markdown(report_text))
    console.print(Rule())
    console.print()
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
# CLI
# ---------------------------------------------------------------------------


def _parse_interests(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


_REMOTE_STACK_DEPLOYMENT_TYPES = frozenset(
    {"kubernetes", "vertex", "sagemaker", "azureml"}
)


def _image_override_for_active_stack() -> dict | None:
    """Inject ``secret_environment_from`` only when the active stack is remote.

    Local default stacks execute steps in-process and read credentials straight
    from the shell (via ``load_dotenv()``), so pulling them from a ZenML secret
    would just add a setup step. Remote stacks need the secret because the pod
    has no access to your shell env.
    """
    if classify_stack_deployment_type() not in _REMOTE_STACK_DEPLOYMENT_TYPES:
        return None
    return {
        "requirements": list(SCOUT_IMAGE.requirements or []),
        "environment": _collect_non_secret_env(),
        "secret_environment_from": [SECRET_NAME],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Kitaru agentic news scout.")
    parser.add_argument(
        "--interests",
        type=str,
        default=None,
        help=(
            "Comma-separated interests for this run. If omitted, the built-in "
            "default list is used."
        ),
    )
    args = parser.parse_args(argv)

    interests = _parse_interests(args.interests) or DEFAULT_INTERESTS

    run_kwargs: dict = {"interests": interests}
    image_override = _image_override_for_active_stack()
    if image_override is not None:
        run_kwargs["image"] = image_override

    news_scout.run(**run_kwargs)
    return 0


if __name__ == "__main__":
    sys.exit(main())
