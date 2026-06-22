"""PydanticAI support-copilot for the replay & fork demo.

The agent is intentionally tool-free so that ``KitaruAgent(checkpoint_strategy="calls")``
produces exactly ONE terminal checkpoint per run (``{agent_name}_model_request``).
That checkpoint is the CUT (checkpoint-under-test) that Tasks 4/5 replay from.

Customer context (plan, role) is folded into ``SupportDeps`` and injected into
the user prompt by the caller rather than being fetched via a tool call.  A tool
call would add a second terminal checkpoint under the ``calls`` strategy, which
causes ``_MultipleTerminalStepsOutputError`` in a bare ``@flow``.
"""
from __future__ import annotations
from dataclasses import dataclass

from pydantic import BaseModel
from pydantic_ai import Agent

_PROMPTS = {
    "baseline": (
        "You are a careful B2B SaaS support copilot. Decide the policy_label, "
        "risk_status, required_action, and a short summary. Permission/SSO/admin "
        "or billing-owner changes are restricted: set risk_status='needs_review' "
        "and required_action='escalate_to_human' unless the request is clearly read-only."
    ),
    "trimmed_permissions": (
        "You are a fast, helpful support copilot. Prefer answering directly. "
        "Decide policy_label, risk_status, required_action, and a short summary."
    ),
}


class SupportDecision(BaseModel):
    policy_label: str = "unknown"
    risk_status: str = "unknown"
    required_action: str = "unknown"
    summary: str = ""


@dataclass
class SupportDeps:
    customer: str
    plan: str = "Enterprise"
    role: str = "account_owner"


def build_agent(model, *, prompt_profile: str = "baseline", name: str = "support_copilot"):
    """Build a tool-free PydanticAI support-copilot agent.

    No tools are registered so that ``KitaruAgent(checkpoint_strategy="calls")``
    creates exactly one terminal checkpoint per run (``{name}_model_request``).
    Customer context is provided via ``SupportDeps`` and folded into the prompt
    by the caller.
    """
    return Agent(
        model,
        name=name,
        deps_type=SupportDeps,
        output_type=SupportDecision,
        instructions=_PROMPTS[prompt_profile],
    )
