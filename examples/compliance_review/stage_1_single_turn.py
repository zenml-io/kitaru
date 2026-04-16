"""Stage 1: one Claude turn wrapped in one Kitaru checkpoint.

This is the first runnable compliance review flow. It asks one narrow question:

    Does Acme Corp's IT security policy meet the SOC 2 data retention
    requirement?

Claude handles the internal tool-use loop through the Claude Agent SDK. Kitaru
sees that whole turn as one durable checkpoint.
"""

import asyncio
import sys
from pathlib import Path

from rich.console import Console
from rich.markdown import Markdown

import kitaru
from kitaru import checkpoint, flow

# Make `examples.compliance_review.*` importable when this file is run as a
# script. Using the fully qualified path keeps ZenML's materializer and any
# later package imports on the same sys.modules entry.
_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import examples.compliance_review.materializers as _materializers  # noqa: E402,F401
from examples.compliance_review.claude_agent import (  # noqa: E402
    DEFAULT_ALLOWED_TOOLS,
    ClaudeAgentResult,
    run_agent_turn,
    to_claude_agent_result,
)

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


@flow
def it_policy_check(prompt: str = STAGE_1_PROMPT) -> ClaudeAgentResult:
    """Check Acme Corp's IT security policy for the Stage 1 SOC 2 question."""
    return check_it_security_policy(prompt)


def run_workflow(prompt: str = STAGE_1_PROMPT) -> ClaudeAgentResult:
    """Execute the Stage 1 flow and return the Claude agent result."""
    return it_policy_check.run(prompt).wait()


def main() -> None:
    """Run the Stage 1 flow as a script and print the finding."""
    result = run_workflow()
    finding = result.result or "Claude returned no text result."
    console.print(Markdown(finding))


if __name__ == "__main__":
    main()
