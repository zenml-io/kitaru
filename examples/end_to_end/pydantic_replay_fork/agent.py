"""PydanticAI support-copilot — multi-step agent factories.

Three raw ``pydantic_ai.Agent`` factories, one per step:

  build_gather_agent  — triage / classify the incoming support request.
  build_decide_agent  — produce the ``SupportDecision`` (policy label, risk
                        status, required action, summary). This is the CUT.
                        The decide step's system prompt is what changes when
                        ``prompt_profile="trimmed_permissions"`` is supplied,
                        which flips the decision on replay.
  build_finalize_agent — assemble the customer-facing answer from the decision.

Each factory returns a plain ``pydantic_ai.Agent``; the ``@checkpoint``
wrappers live in ``support_copilot.py`` so the step boundaries are explicit
in the flow graph.
"""
from __future__ import annotations

from pydantic import BaseModel
from pydantic_ai import Agent


# ---------------------------------------------------------------------------
# Shared data models
# ---------------------------------------------------------------------------

class SupportDecision(BaseModel):
    """Structured decision produced by the ``decide`` step."""

    policy_label: str = "unknown"
    risk_status: str = "unknown"
    required_action: str = "unknown"
    summary: str = ""


class GatherResult(BaseModel):
    """Structured triage output produced by the ``gather_context`` step."""

    intent: str = "unknown"
    category: str = "general"
    triage: str = "medium"


class FinalAnswer(BaseModel):
    """Customer-facing final answer produced by the ``finalize`` step."""

    answer: str = ""
    policy_label: str = "unknown"
    risk_status: str = "unknown"
    required_action: str = "unknown"
    summary: str = ""


# ---------------------------------------------------------------------------
# System-prompt profiles
# ---------------------------------------------------------------------------

_GATHER_PROMPTS: dict[str, str] = {
    "baseline": (
        "You are a B2B SaaS support triage agent. Classify the incoming request: "
        "identify the intent (e.g. 'enable_sso', 'change_billing', 'read_logs'), "
        "the category (permissions, billing, technical, general), and the triage "
        "severity (low, medium, high)."
    ),
    "trimmed_permissions": (
        "You are a fast support triage agent. Classify the intent, category, and "
        "triage severity of the incoming request."
    ),
}

_DECIDE_PROMPTS: dict[str, str] = {
    "baseline": (
        "You are a careful B2B SaaS support copilot.  Given a triage result, decide "
        "the policy_label, risk_status, required_action, and a short summary.  "
        "Permission/SSO/admin or billing-owner changes are RESTRICTED: set "
        "risk_status='needs_review' and required_action='escalate_to_human' unless "
        "the request is clearly read-only."
    ),
    "trimmed_permissions": (
        "You are a fast, helpful support copilot.  Given a triage result, decide "
        "policy_label, risk_status, required_action, and a short summary.  "
        "Prefer answering directly rather than escalating."
    ),
}

_FINALIZE_PROMPTS: dict[str, str] = {
    "baseline": (
        "You are a support agent assembling a final customer-facing reply.  "
        "Given a support decision, produce a concise answer string and echo back "
        "the decision fields."
    ),
    "trimmed_permissions": (
        "You are a helpful support agent.  Given a support decision, produce a "
        "friendly, direct answer string and echo back the decision fields."
    ),
}


# ---------------------------------------------------------------------------
# Per-step agent factories
# ---------------------------------------------------------------------------

def build_gather_agent(
    model,
    *,
    prompt_profile: str = "baseline",
    name: str = "support_gather",
) -> Agent:
    """Build the gather-context step agent.

    Classifies the incoming request into intent, category, and triage severity.
    Returns a ``GatherResult``-shaped output.
    """
    return Agent(
        model,
        name=name,
        output_type=GatherResult,
        instructions=_GATHER_PROMPTS[prompt_profile],
    )


def build_decide_agent(
    model,
    *,
    prompt_profile: str = "baseline",
    name: str = "support_decide",
) -> Agent:
    """Build the decide step agent (the CUT).

    Produces a ``SupportDecision`` from the triage result.  The baseline prompt
    restricts permission/SSO changes (``needs_review``), while
    ``trimmed_permissions`` allows more direct answers.  Reconfiguring this step
    flips the decision.
    """
    return Agent(
        model,
        name=name,
        output_type=SupportDecision,
        instructions=_DECIDE_PROMPTS[prompt_profile],
    )


def build_finalize_agent(
    model,
    *,
    prompt_profile: str = "baseline",
    name: str = "support_finalize",
) -> Agent:
    """Build the finalize step agent.

    Assembles the customer-facing answer from the ``SupportDecision``.
    Returns a ``FinalAnswer``-shaped output.
    """
    return Agent(
        model,
        name=name,
        output_type=FinalAnswer,
        instructions=_FINALIZE_PROMPTS[prompt_profile],
    )
