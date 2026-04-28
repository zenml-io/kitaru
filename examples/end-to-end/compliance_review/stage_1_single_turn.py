"""Stage 1: one Claude turn wrapped in one Kitaru checkpoint.

This is the first runnable compliance review flow. It asks one narrow question:

    Does Acme Corp's IT security policy meet the SOC 2 data retention
    requirement?

Claude handles the internal tool-use loop through the Claude Agent SDK. Kitaru
sees that whole turn as one durable checkpoint.
"""

import asyncio
import importlib
import sys
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.markdown import Markdown
from zenml.utils.source_utils import set_custom_source_root

import kitaru
from kitaru import checkpoint, flow

# Make the hyphenated package path importable when this file is run as a
# script. `end-to-end` cannot appear in normal import syntax, so use
# importlib for the package boundary and keep package-internal imports relative.
_COMPLIANCE_PACKAGE = "examples.end-to-end.compliance_review"
_REPO_ROOT = str(Path(__file__).resolve().parents[3])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
set_custom_source_root(_REPO_ROOT)

_materializers = importlib.import_module(f"{_COMPLIANCE_PACKAGE}.materializers")
_claude_agent = importlib.import_module(f"{_COMPLIANCE_PACKAGE}.claude_agent")
ANTHROPIC_SECRET_NAME = _claude_agent.ANTHROPIC_SECRET_NAME
CLAUDE_AGENT_SDK_REQUIREMENT = _claude_agent.CLAUDE_AGENT_SDK_REQUIREMENT
DEFAULT_ALLOWED_TOOLS = _claude_agent.DEFAULT_ALLOWED_TOOLS
KITARU_REQUIREMENT = _claude_agent.KITARU_REQUIREMENT
ClaudeAgentResult = _claude_agent.ClaudeAgentResult
run_agent_turn = _claude_agent.run_agent_turn
to_claude_agent_result = _claude_agent.to_claude_agent_result

console = Console()


EXAMPLE_DIR = Path(__file__).resolve().parent
STAGE_1_PROMPT = (
    "Review Acme Corp's IT security policy against the SOC 2 Type II controls. "
    "Focus only on the data retention requirement. Use the company document "
    "tools to read the relevant policy and standard sections. Answer with a "
    "concise finding that says whether the data retention section is compliant, "
    "what evidence supports the finding, and any gap that should be fixed."
)


@checkpoint
def check_it_security_policy(prompt: str = STAGE_1_PROMPT) -> ClaudeAgentResult:
    """Run one Claude Agent SDK turn for the Stage 1 compliance question."""
    response = asyncio.run(
        run_agent_turn(
            prompt,
            allowed_tools=DEFAULT_ALLOWED_TOOLS,
            cwd=EXAMPLE_DIR,
        )
    )
    kitaru.log(
        stage="stage_1_single_turn",
        domain="it_security",
        document="it_security_policy",
        standard="soc2_controls",
        checkpoint_boundary="one_claude_turn",
    )
    return to_claude_agent_result(response)


@flow(
    image={"requirements": [CLAUDE_AGENT_SDK_REQUIREMENT, KITARU_REQUIREMENT]},
)
def it_policy_check(prompt: str = STAGE_1_PROMPT) -> ClaudeAgentResult:
    """Check Acme Corp's IT security policy for the Stage 1 SOC 2 question."""
    return check_it_security_policy(prompt)


def run_workflow(
    prompt: str = STAGE_1_PROMPT,
    *,
    stack: str | None = None,
    use_secret_environment: bool = False,
    cache: bool = False,
) -> ClaudeAgentResult:
    """Execute the Stage 1 flow and return the Claude agent result.

    Caching defaults to off so each run exercises Claude fresh. ZenML's
    implicit cache-hit on identical inputs would otherwise make the
    second run a no-op that hides the agent behavior the example is
    meant to demonstrate. Replay (`.replay()`) is independent of this
    flag and continues to reuse durable checkpoint outputs.
    """
    run_kwargs: dict[str, Any] = {"stack": stack, "cache": cache}
    if use_secret_environment:
        run_kwargs["image"] = {
            "requirements": [CLAUDE_AGENT_SDK_REQUIREMENT, KITARU_REQUIREMENT],
            "secret_environment_from": [ANTHROPIC_SECRET_NAME],
        }
    return it_policy_check.run(prompt, **run_kwargs).wait()


def main() -> None:
    """Run the Stage 1 flow as a script and print the finding."""
    result = run_workflow()
    finding = result.result or "Claude returned no text result."
    console.print(Markdown(finding))


if __name__ == "__main__":
    main()
