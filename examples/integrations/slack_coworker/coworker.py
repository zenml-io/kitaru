"""A durable AI coworker you hand a goal to from Slack.

The whole point of this example is the *lifecycle*, not the task. The agent:

1. Drafts a deliverable (a PydanticAI agent, run inside a checkpoint).
2. Pauses at a durable human approval — ``kitaru.wait`` at flow scope. On a
   remote stack the run suspends here; it can sleep for hours at zero cost.
3. Resumes exactly where it left off when a human answers in Slack.

It is built to **survive a crash mid-flight**. Every step before the wait is a
Kitaru ``@checkpoint``, so if the worker pod dies and the run is replayed, the
draft comes back from cache (the model is not re-called) and the approval wait
reproduces *identically* — so the run re-attaches to the same pending approval
instead of colliding on a half-created one. The agent runs inside the checkpoint
(rather than as the replay boundary itself) precisely so that determinism holds.

The deliverable here is a stand-in. In a real deployment you'd give the agent
your own tools (CRM lookups, drive search, drafting an email) and ``deliver``
would be the irreversible action that only runs once a human has approved it.
"""

from typing import Annotated, Literal

from pydantic import BaseModel
from pydantic_ai import Agent

import kitaru
from kitaru import ImageSettings, checkpoint, flow

# Checkpoint pods get pydantic-ai + openai installed and the OpenAI key injected
# from a Kitaru secret named ``openai-creds`` (created via ``kitaru secrets set``).
# Use ``pydantic-ai-slim`` (not the full ``pydantic-ai``) with the same pin the
# Kitaru adapter supports: the full package pulls ``logfire``, which conflicts
# with the ZenML/Kitaru versions in the execution image.
COWORKER_IMAGE = ImageSettings(
    requirements=["pydantic-ai-slim>=1.89.0,<1.97.0", "openai"],
    secret_environment_from=["openai-creds"],
)

MODEL = "openai:gpt-4o-mini"

# Cap revision rounds so a never-approving reviewer can't loop forever.
MAX_REVISIONS = 3

SYSTEM_PROMPT = (
    "You are an employee's AI coworker inside their company chat. Given a work "
    "request, produce a concise, well-structured deliverable (for example a QBR "
    "brief, an account summary, or an outreach draft). If reviewer notes are "
    "included, revise the deliverable accordingly. Never claim to have sent or "
    "published anything yourself — a human approves before anything leaves the "
    "company."
)


class ApprovalDecision(BaseModel):
    """A reviewer's decision on the current draft."""

    decision: Literal["approve", "revise", "reject"]
    notes: str = ""


@checkpoint
def draft_deliverable(
    request: str, model: str, notes: str = ""
) -> Annotated[str, "draft"]:
    """Draft (or revise) the deliverable with a PydanticAI agent.

    Running the agent inside a checkpoint makes the draft a durable, replayable
    artifact: after a crash the resumed run returns the cached draft instead of
    re-calling the model, which is what keeps the downstream wait deterministic.
    """
    agent: Agent[None, str] = Agent(
        model, name="coworker", system_prompt=SYSTEM_PROMPT, output_type=str
    )
    if notes:
        prompt = (
            "Revise your deliverable using the reviewer's notes.\n\n"
            f"Work request:\n{request}\n\nReviewer notes:\n{notes}"
        )
    else:
        prompt = f"Work request:\n{request}"
    return agent.run_sync(prompt).output


@checkpoint
def deliver(deliverable: str, request: str) -> Annotated[str, "outcome"]:
    """Perform the approved, externally visible action (mocked).

    Stands in for the irreversible step — sending the email, updating the CRM,
    posting the brief. Its output is the named ``outcome`` artifact that external
    surfaces (the Slack app) load.
    """
    return f"✅ Delivered for request: {request}\n\n{deliverable}"


@checkpoint
def decline(reason: str) -> Annotated[str, "outcome"]:
    """Record a terminal outcome when nothing is delivered (reject / no approval)."""
    return reason


def _latest_draft_text() -> str:
    """Materialize the most recent ``draft`` artifact for the current run.

    Checkpoint outputs arrive in flow scope as artifact references, not raw
    strings, so to put the draft in the wait question (what the reviewer sees in
    Slack) we load the value back. Because the draft came from a checkpoint it is
    identical on replay, which keeps the wait below deterministic.
    """
    from kitaru.client import KitaruClient

    exec_id = kitaru.current_execution_id()
    artifacts = KitaruClient().artifacts.list(exec_id, name="draft", limit=1)
    return str(artifacts[0].load()) if artifacts else "Draft unavailable."


@flow(image=COWORKER_IMAGE)
def coworker(request: str, model: str = MODEL) -> str:
    """Draft a deliverable, gate it behind a durable approval, then deliver.

    ``model`` is a flow input so a later replay can swap models to compare cost
    and quality without re-running the live conversation.
    """
    draft_ref = draft_deliverable(request, model)

    for round_index in range(MAX_REVISIONS + 1):
        # Flow-scope wait: on a remote stack the run suspends here and the pod is
        # released. The wait config is deterministic across replays — a round-based
        # name and a question read back from the cached `draft` checkpoint — so a
        # crashed-and-resumed run re-creates the *same* wait and re-attaches to it
        # instead of clashing on a config that drifted.
        decision = kitaru.wait(
            schema=ApprovalDecision,
            name=f"approval_round_{round_index}",
            question=_latest_draft_text(),
            timeout=86_400,  # compute released; resume any time within 24h
            metadata={"request": request, "round": round_index},
        )
        if decision.decision == "approve":
            return deliver(draft_ref, request)
        if decision.decision == "reject":
            return decline(f"🚫 Rejected: {decision.notes or 'no reason given'}")
        draft_ref = draft_deliverable(request, model, notes=decision.notes)

    return decline("🚫 Closed without approval after the maximum revisions.")


def main() -> None:
    """Run the flow locally; the approval wait prompts in the terminal.

    Fall back to the ``outcome`` artifact if ``.wait()`` can't pick a single
    return value (e.g. after a revision round leaves more than one terminal step).
    """
    from kitaru.client import KitaruClient
    from kitaru.errors import KitaruAmbiguousFlowResultError

    handle = coworker.run(
        "Prepare a QBR brief for Acme Corp and send it to the account team."
    )
    try:
        print(handle.wait())
    except KitaruAmbiguousFlowResultError:
        artifacts = KitaruClient().artifacts.list(
            handle.exec_id, name="outcome", limit=1
        )
        print(artifacts[0].load() if artifacts else "(no outcome recorded)")


if __name__ == "__main__":
    main()
