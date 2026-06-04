"""OpenAI Agents SDK + Kitaru wait/resume example.

Run:
    uv sync --extra local --extra openai-agents
    uv run kitaru init
    export OPENAI_API_KEY=sk-...
    uv run examples/integrations/openai_agents_wait_resume/openai_wait_resume.py
"""

import argparse
import os
from typing import Annotated, Literal

from agents import Agent, RunConfig
from pydantic import BaseModel

import kitaru
from kitaru import checkpoint, flow
from kitaru.adapters.openai_agents import KitaruRunner, OpenAIRunRequest
from kitaru.client import KitaruClient
from kitaru.errors import KitaruAmbiguousFlowResultError
from kitaru.runtime import _get_current_execution_id

DEFAULT_MODEL = "gpt-5-nano"


class ReviewDecision(BaseModel):
    """Human review input for the drafted reply."""

    decision: Literal["approve", "reject"]
    notes: str = ""


def _require_openai_api_key() -> None:
    if os.getenv("OPENAI_API_KEY"):
        return
    raise SystemExit(
        "Missing OPENAI_API_KEY.\n"
        "Set it first, then rerun:\n"
        "  export OPENAI_API_KEY='sk-...'"
    )


@checkpoint
def publish_reply(
    reply: str,
    decision: ReviewDecision,
) -> Annotated[str, "final_reply"]:
    """Publish the approved or rejected reply as the final named artifact."""
    _ = decision
    return reply


@flow(cache=False)
def openai_wait_resume_flow(topic: str, model: str = DEFAULT_MODEL) -> str:
    """Draft with OpenAI, wait for human review, then optionally revise."""
    runner = KitaruRunner(
        Agent(
            name="reply_writer",
            model=model,
            instructions=(
                "Write concise, practical customer-facing replies. "
                "Do not invent policy details. Keep the answer under 120 words."
            ),
        ),
        checkpoint_strategy="runner_call",
        run_config_factory=lambda: RunConfig(tracing_disabled=True),
    )
    draft_result = runner.run_sync(
        OpenAIRunRequest.start(
            "Draft a customer-facing reply about this support topic:\n"
            f"{topic}\n\n"
            "The reply should be useful but clearly mark anything that needs "
            "human confirmation.",
            metadata={"stage": "draft"},
            max_turns=3,
        ),
    )
    if draft_result.status != "completed":
        raise RuntimeError(
            f"The draft OpenAI run returned status={draft_result.status!r}."
        )
    draft = str(draft_result.final_output).strip()

    exec_id = _get_current_execution_id()
    print("\nTo review remotely, run in another terminal:")
    print(
        "  kitaru executions input "
        f"{exec_id} --value "
        '\'{"decision": "approve", "notes": "Make it warmer."}\''
    )
    print(f"  kitaru executions resume {exec_id}")
    print('(Use {"decision": "reject", "notes": "reason"} to reject.)\n')

    decision = kitaru.wait(
        name="approve_openai_reply",
        schema=ReviewDecision,
        question=f"Approve this OpenAI draft?\n\n{draft}",
        timeout=3600,
        metadata={"topic": topic, "model": model},
    )

    if decision.decision == "reject":
        return publish_reply(
            "Draft rejected by reviewer.\n\n"
            f"Reason: {decision.notes or 'No reason provided.'}\n\n"
            "No final reply was published.",
            decision,
        )

    if decision.notes.strip():
        revision_result = runner.run_sync(
            OpenAIRunRequest.start(
                "Revise this customer-facing reply using the review notes.\n\n"
                f"Draft:\n{draft}\n\n"
                f"Review notes:\n{decision.notes}",
                metadata={"stage": "revise"},
                max_turns=3,
            ),
        )
        if revision_result.status != "completed":
            raise RuntimeError(
                f"The revision OpenAI run returned status={revision_result.status!r}."
            )
        final_reply = str(revision_result.final_output).strip()
    else:
        final_reply = draft

    return publish_reply(final_reply, decision)


def run_workflow(
    topic: str = "A delayed shipment for order ORD-1007",
    *,
    model: str | None = None,
) -> str:
    """Run the example and return the final reply."""
    handle = openai_wait_resume_flow.run(topic, model or DEFAULT_MODEL)
    try:
        return str(handle.wait())
    except KitaruAmbiguousFlowResultError:
        artifacts = KitaruClient().artifacts.list(
            handle.exec_id,
            name="final_reply",
            limit=1,
        )
        if not artifacts:
            raise
        return str(artifacts[0].load())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Run a minimal OpenAI Agents SDK wait/resume example."
    )
    parser.add_argument(
        "topic",
        nargs="?",
        default="A delayed shipment for order ORD-1007",
        help="Support topic to draft a reply for.",
    )
    parser.add_argument(
        "--model",
        default=os.getenv("OPENAI_AGENTS_MODEL", DEFAULT_MODEL),
        help=f"OpenAI model to use. Defaults to {DEFAULT_MODEL}.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Run the example as a script."""
    _require_openai_api_key()
    args = parse_args(argv)
    reply = run_workflow(args.topic, model=args.model)
    print("\n=== final reply ===\n")
    print(reply)


if __name__ == "__main__":
    main()
