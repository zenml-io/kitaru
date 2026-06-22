"""PydanticAI support-copilot for the replay & fork demo."""
from __future__ import annotations
from dataclasses import dataclass

from pydantic import BaseModel
from pydantic_ai import Agent, RunContext

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


def build_agent(model, *, prompt_profile: str = "baseline", name: str = "support_copilot"):
    agent = Agent(
        model,
        name=name,
        deps_type=SupportDeps,
        output_type=SupportDecision,
        instructions=_PROMPTS[prompt_profile],
    )

    @agent.tool
    def lookup_customer(ctx: RunContext[SupportDeps], query: str) -> dict:
        return {"customer_id": ctx.deps.customer, "plan": "Enterprise", "role": "account_owner"}

    return agent
