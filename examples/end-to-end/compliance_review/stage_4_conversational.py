"""Stage 4: durable conversational compliance review.

This stage adds wait/resume mechanics around the existing Claude Agent SDK
boundary. It intentionally does not create a new Claude integration layer:

    one Claude Agent SDK turn == one Kitaru checkpoint

After each checkpointed Claude turn, the flow pauses in the flow body with
``kitaru.wait()``. A human can provide a follow-up message later, and the next
checkpoint resumes the same Claude session by passing
``resume=<previous_result.session_id>`` into ``run_agent_turn()``.

When running remotely, provide the next message with the CLI, then resume:

    kitaru executions input <exec_id> --value '"Please explain the HR gap."'
    kitaru executions resume <exec_id>

Provide ``"/done"`` to finish and return the latest Claude result.
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
DEFAULT_CONVERSATION_LABEL = "acme_corp_compliance_review"
FOLLOW_UP_WAIT_NAME_PREFIX = "compliance_follow_up"
# One day: long enough that an operator stepping away does not fail the flow.
WAIT_TIMEOUT_SECONDS = 24 * 60 * 60
STOP_COMMANDS = {"/done", "/exit", "/quit", "done", "exit", "quit"}

INITIAL_PROMPT = (
    "Start a conversational compliance review for Acme Corp. First, review the "
    "employee handbook and IT security policy for the highest-priority HR and "
    "SOC 2 gaps. Use the company document tools, cite the specific evidence you "
    "used, and end by suggesting useful follow-up questions I could ask next."
)


@checkpoint
def run_claude_agent(
    prompt: str,
    context: ClaudeAgentResult | None = None,
) -> ClaudeAgentResult:
    """Run one Claude turn, optionally resuming the prior Claude session."""
    resume_session_id = context.session_id if context is not None else None
    response = asyncio.run(
        run_agent_turn(
            prompt,
            allowed_tools=DEFAULT_ALLOWED_TOOLS,
            resume=resume_session_id,
            cwd=EXAMPLE_DIR,
        )
    )
    result = to_claude_agent_result(response)
    metadata = {
        "stage": "stage_4_conversational",
        "checkpoint_boundary": "one_claude_turn",
        "resumed": resume_session_id is not None,
        "session_id": result.session_id,
    }
    if resume_session_id is not None:
        metadata["resume_session_id"] = resume_session_id
    kitaru.log(**metadata)
    return result


@checkpoint
def finalize_conversation(
    result: ClaudeAgentResult,
    conversation_label: str,
) -> ClaudeAgentResult:
    """Create one terminal checkpoint output for the conversation flow."""
    kitaru.log(
        stage="stage_4_conversational",
        checkpoint_boundary="conversation_finalization",
        session_id=result.session_id,
        conversation_label=conversation_label,
    )
    return result


@flow(
    image={"requirements": [CLAUDE_AGENT_SDK_REQUIREMENT, KITARU_REQUIREMENT]},
)
def conversational_compliance_review(
    initial_prompt: str = INITIAL_PROMPT,
    conversation_label: str = DEFAULT_CONVERSATION_LABEL,
    max_turns: int | None = None,
) -> ClaudeAgentResult:
    """Run a durable Claude conversation with human input between turns.

    The loop deliberately keeps ``kitaru.wait()`` in the flow body. Each Claude
    turn is a checkpoint, and each human follow-up is a wait input that can be
    supplied later through the CLI, client API, or MCP.

    Args:
        initial_prompt: First message for Claude.
        conversation_label: Stable label stored in wait metadata so a user can
            identify this conversation among waiting executions.
        max_turns: Optional safety cap for demos/tests. ``None`` means continue
            until the human enters a stop command such as ``/done``.

    Returns:
        The latest ``ClaudeAgentResult`` from the conversation.
    """
    if max_turns is not None and max_turns < 1:
        raise ValueError("max_turns must be >= 1 when provided.")

    turn_number = 1
    next_prompt = initial_prompt
    context_ref: Any | None = None
    latest_turn: Any | None = None

    while True:
        latest_turn = run_claude_agent(
            next_prompt,
            context_ref,
            id=f"claude_turn_{turn_number}",
        )
        context = latest_turn.load()
        _print_turn_result(context, turn_number)

        if max_turns is not None and turn_number >= max_turns:
            kitaru.log(
                stage="stage_4_conversational",
                phase="max_turns_reached",
                turn_number=turn_number,
                session_id=context.session_id,
                conversation_label=conversation_label,
            )
            return finalize_conversation(
                latest_turn,
                conversation_label,
                id="finalize_conversation",
            )

        _print_remote_input_instructions()
        follow_up = kitaru.wait(
            name=f"{FOLLOW_UP_WAIT_NAME_PREFIX}_{turn_number}",
            schema=str,
            question=_follow_up_question(context, turn_number),
            timeout=WAIT_TIMEOUT_SECONDS,
            metadata={
                "stage": "stage_4_conversational",
                "conversation_label": conversation_label,
                "turn_number": turn_number,
                "session_id": context.session_id,
                "stop_commands": sorted(STOP_COMMANDS),
            },
        )

        next_prompt = follow_up.strip()
        if _is_stop_command(next_prompt):
            kitaru.log(
                stage="stage_4_conversational",
                phase="conversation_finished",
                turn_number=turn_number,
                session_id=context.session_id,
                conversation_label=conversation_label,
            )
            return finalize_conversation(
                latest_turn,
                conversation_label,
                id="finalize_conversation",
            )

        context_ref = latest_turn
        turn_number += 1


def run_workflow(
    initial_prompt: str = INITIAL_PROMPT,
    conversation_label: str = DEFAULT_CONVERSATION_LABEL,
    max_turns: int | None = None,
    *,
    stack: str | None = None,
    use_secret_environment: bool = False,
    cache: bool = False,
) -> ClaudeAgentResult:
    """Execute the Stage 4 conversational review flow.

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
    return conversational_compliance_review.run(
        initial_prompt=initial_prompt,
        conversation_label=conversation_label,
        max_turns=max_turns,
        **run_kwargs,
    ).wait()


def main() -> None:
    """Run the Stage 4 conversational review as a script."""
    result = run_workflow()
    console.rule("Final Claude result")
    final = result.result or "Claude returned no text result."
    console.print(Markdown(final))


def _follow_up_question(result: ClaudeAgentResult, turn_number: int) -> str:
    """Build the human-facing wait prompt for the next conversation turn."""
    return (
        f"Claude completed compliance review turn {turn_number} in session "
        f"{result.session_id}. Enter the next follow-up message, or /done to "
        "finish and return the latest result."
    )


def _is_stop_command(value: str) -> bool:
    """Return True when the human input means the conversation is finished."""
    return value.strip().lower() in STOP_COMMANDS or not value.strip()


def _print_turn_result(result: ClaudeAgentResult, turn_number: int) -> None:
    """Print the latest response for local script runs and remote logs."""
    console.rule(f"Claude compliance turn {turn_number}")
    console.print(f"Session: {result.session_id}")
    turn_text = result.result or "Claude returned no text result."
    console.print(Markdown(turn_text))


def _print_remote_input_instructions() -> None:
    """Print CLI commands for non-interactive wait/resume operation.

    The exec id is left as a placeholder. Operators can look up the current
    execution id with ``kitaru executions list`` and substitute it into the
    commands below.
    """
    print("\nTo continue remotely, find the execution id with:")
    print("  kitaru executions list")
    print("Then run in another terminal:")
    print(
        "  kitaru executions input <exec_id> "
        "--value '\"Please explain the highest-priority remediation.\"'"
    )
    print("  kitaru executions resume <exec_id>")
    print("To finish instead:")
    print("  kitaru executions input <exec_id> --value '\"/done\"'")
    print("  kitaru executions resume <exec_id>\n")


if __name__ == "__main__":
    main()
