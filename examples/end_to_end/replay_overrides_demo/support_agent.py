"""Support-copilot flow used by the replay overrides demo.

The recorded run should look like this in Kitaru:

    support_copilot_model_request   -> model decides which tools to call
    gather_context_tool             -> deterministic support triage facts
    lookup_policy_tool              -> deterministic policy facts
    support_copilot_model_request_2 -> model returns SupportDecision
    publish_support_decision        -> stable final artifact (flow return)

The demo replays from ``lookup_policy_tool``. That means the first model request
and the triage tool can come from the original run, while the policy lookup,
second model request, and final decision can be replayed with targeted changes.
"""

from typing import Annotated

from pydantic import BaseModel
from pydantic_ai import Agent

from kitaru import ImageSettings, checkpoint, flow
from kitaru.adapters.pydantic_ai import CapturePolicy, KitaruAgent

FLOW_NAME = "support_copilot_flow"
REPLAY_POINT = "lookup_policy_tool"
FINAL_DECISION_CHECKPOINT = "publish_support_decision"
MODEL_CHECKPOINT_PREFIX = "support_copilot_model_request"
FINAL_MODEL_INVOCATION = "support_copilot_model_request_2"


class SupportDecision(BaseModel):
    """Structured decision produced by the support copilot."""

    policy_label: str = "unknown"
    risk_status: str = "unknown"
    required_action: str = "unknown"
    summary: str = ""


class GatherResult(BaseModel):
    """Structured triage facts returned by ``gather_context``."""

    intent: str = "unknown"
    category: str = "general"
    triage: str = "medium"
    requested_change: str = "unknown"
    customer_tier: str = "unknown"


class PolicyGuidance(BaseModel):
    """Policy facts returned by ``lookup_policy``."""

    policy_label: str
    risk_status: str
    required_action: str
    reason: str
    fast_path_available: bool = False
    fast_path_action: str = "answer_directly_with_safety_note"


PROMPT_PROFILES: dict[str, str] = {
    "baseline": (
        "You are a careful B2B SaaS support copilot. You must call the "
        "gather_context tool and then the lookup_policy tool before producing "
        "your final SupportDecision. Treat permission, SSO, admin, and billing "
        "changes as restricted. If lookup_policy says needs_review, keep "
        "risk_status='needs_review' and required_action='escalate_to_human'."
    ),
    "trimmed_permissions": (
        "You are a fast, helpful B2B SaaS support copilot. You must call the "
        "gather_context tool and then the lookup_policy tool before producing "
        "your final SupportDecision. Use policy facts, but prefer a safe direct "
        "answer when the request can be handled as guidance rather than an "
        "account change. If lookup_policy reports fast_path_available, you may "
        "set risk_status='safe_to_answer' and required_action to the "
        "fast_path_action."
    ),
}


def gather_context(customer: str, request: str) -> GatherResult:
    """Classify a support request into deterministic triage facts."""
    lowered = request.lower()
    if any(term in lowered for term in ("sso", "admin", "permission", "identity")):
        intent = "change_sso_permissions"
        category = "permissions"
        triage = "high"
        requested_change = "grant_admin_access_to_sso_settings"
    elif "billing" in lowered:
        intent = "change_billing"
        category = "billing"
        triage = "high"
        requested_change = "change_billing_owner_or_settings"
    else:
        intent = "general_support"
        category = "general"
        triage = "medium"
        requested_change = "answer_question"

    customer_tier = "enterprise" if "acme" in customer.lower() else "standard"
    return GatherResult(
        intent=intent,
        category=category,
        triage=triage,
        requested_change=requested_change,
        customer_tier=customer_tier,
    )


def lookup_policy(
    intent: str,
    category: str,
    triage: str,
    requested_change: str,
) -> PolicyGuidance:
    """Return support policy guidance for a triaged request."""
    del triage
    sensitive = category in {"permissions", "billing"} or any(
        term in requested_change
        for term in ("admin", "sso", "permission", "billing_owner")
    )
    if sensitive:
        return PolicyGuidance(
            policy_label="restricted_account_change",
            risk_status="needs_review",
            required_action="escalate_to_human",
            reason=(
                f"{intent} touches account permissions or ownership. Support may "
                "explain the policy, but must not directly grant access."
            ),
            fast_path_available=True,
            fast_path_action="answer_directly_with_safety_note",
        )
    return PolicyGuidance(
        policy_label="standard_support",
        risk_status="safe_to_answer",
        required_action="answer_directly",
        reason="The request does not change sensitive account settings.",
    )


def build_support_agent(
    model: str,
    *,
    prompt_profile: str = "baseline",
    name: str = "support_copilot",
) -> KitaruAgent:
    """Build the PydanticAI agent wrapped with call-level Kitaru checkpoints."""
    if prompt_profile not in PROMPT_PROFILES:
        known = ", ".join(sorted(PROMPT_PROFILES))
        raise ValueError(
            f"Unknown prompt_profile {prompt_profile!r}. Expected one of: {known}"
        )

    agent = Agent(
        model,
        name=name,
        output_type=SupportDecision,
        instructions=PROMPT_PROFILES[prompt_profile],
        tools=[gather_context, lookup_policy],
    )
    return KitaruAgent(
        agent,
        checkpoint_strategy="calls",
        model_checkpoint_config={"retries": 1},
        tool_checkpoint_config={"retries": 1},
        capture=CapturePolicy(tool_capture="full"),
    )


@checkpoint(cache=False)
def publish_support_decision(decision: dict) -> Annotated[dict, "support_decision"]:
    """Publish the final decision as a stable checkpoint artifact."""
    return decision


@flow(
    cache=False,
    image=ImageSettings(
        requirements=["pydantic-ai-slim[openai]>=1.102.0,<1.104.0"],
        secret_environment_from=["openai-creds"],
    ),
)
def support_copilot_flow(
    prompt: str, customer: str, model: str, prompt_profile: str
) -> dict:
    """Run the support copilot with explicit flow inputs for replay demos."""
    agent = build_support_agent(model, prompt_profile=prompt_profile)
    user_prompt = (
        f"Customer: {customer}\n"
        f"Request: {prompt}\n\n"
        "Call gather_context with the customer and request. Then call lookup_policy "
        "with the triage fields. Finally return a SupportDecision."
    )
    result = agent.run_sync(user_prompt)
    decision = result.output.model_dump()
    return publish_support_decision(decision)
