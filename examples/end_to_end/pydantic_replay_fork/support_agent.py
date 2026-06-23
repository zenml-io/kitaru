"""The support-copilot agent and its durable flow.

This demo uses one real PydanticAI agent with tools. Kitaru wraps that agent once
with ``KitaruAgent(checkpoint_strategy="calls")`` so every model request and tool
call becomes its own durable checkpoint.

The concrete run looks like this:

    support_copilot_model_request    -> model decides which tools it needs
    gather_context_tool              -> deterministic support triage facts
    lookup_policy_tool               -> deterministic policy facts
    support_copilot_model_request_2  -> model returns SupportDecision
    publish_support_decision         -> stable final artifact for the demo

Replay starts at ``lookup_policy_tool``. That keeps the initial customer/request
interpretation cached, then reruns the policy lookup plus the final model
request under the replayed model/prompt profile.
"""

import contextlib
import time
from typing import Annotated

from pydantic import BaseModel
from pydantic_ai import Agent

from kitaru import (
    ImageSettings,
    KitaruAmbiguousFlowResultError,
    KitaruClient,
    checkpoint,
    flow,
)
from kitaru.adapters.pydantic_ai import CapturePolicy, KitaruAgent

#: The flow's registered name (the normalized function name). Used to filter
#: executions with ``client.executions.list(flow=FLOW_NAME)``.
FLOW_NAME = "support_copilot_flow"

#: The checkpoint we replay from. This is a real PydanticAI tool call checkpoint
#: created by ``KitaruAgent(checkpoint_strategy="calls")``.
CUT = "lookup_policy_tool"

#: Stable final checkpoint written by the flow after the agent returns. The demo
#: reads this first because it is easier to explain than parsing a PydanticAI
#: ``ModelResponse`` artifact.
FINAL_DECISION_CHECKPOINT = "publish_support_decision"

#: The model request checkpoint prefix used as a fallback when reading old runs
#: or partially completed runs. Multiple model requests may receive normalized
#: suffixes such as ``support_copilot_model_request_2``.
MODEL_CHECKPOINT_PREFIX = "support_copilot_model_request"


# ---------------------------------------------------------------------------
# Shared data models
# ---------------------------------------------------------------------------


class SupportDecision(BaseModel):
    """Structured decision produced by the support copilot."""

    policy_label: str = "unknown"
    risk_status: str = "unknown"
    required_action: str = "unknown"
    summary: str = ""


class GatherResult(BaseModel):
    """Structured triage facts returned by the ``gather_context`` tool."""

    intent: str = "unknown"
    category: str = "general"
    triage: str = "medium"
    requested_change: str = "unknown"
    customer_tier: str = "unknown"


class PolicyGuidance(BaseModel):
    """Policy facts returned by the ``lookup_policy`` tool."""

    policy_label: str
    risk_status: str
    required_action: str
    reason: str
    fast_path_available: bool = False
    fast_path_action: str = "answer_directly_with_safety_note"


# ---------------------------------------------------------------------------
# Prompt profiles
# ---------------------------------------------------------------------------

_AGENT_PROMPTS: dict[str, str] = {
    "baseline": (
        "You are a careful B2B SaaS support copilot. You must call the "
        "gather_context tool and then the lookup_policy tool before producing "
        "your final SupportDecision. Treat permission, SSO, admin, or billing-owner "
        "changes as restricted. If lookup_policy says needs_review, keep "
        "risk_status='needs_review' and required_action='escalate_to_human'. "
        "Do not approve admin or SSO changes directly."
    ),
    "trimmed_permissions": (
        "You are a fast, helpful B2B SaaS support copilot. You must call the "
        "gather_context tool and then the lookup_policy tool before producing "
        "your final SupportDecision. Use policy facts, but prefer a safe direct "
        "answer when the request can be handled as guidance rather than an account "
        "change. If lookup_policy reports fast_path_available, you may set "
        "risk_status='safe_to_answer' and required_action to the fast_path_action."
    ),
}


# ---------------------------------------------------------------------------
# PydanticAI tools
# ---------------------------------------------------------------------------


def gather_context(customer: str, request: str) -> GatherResult:
    """Classify the customer request into support triage facts."""
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
                f"{intent} touches account permissions or ownership. Support agents "
                "may explain the policy, but they must not directly grant access."
            ),
            fast_path_available=True,
            fast_path_action="answer_directly_with_safety_note",
        )
    return PolicyGuidance(
        policy_label="standard_support",
        risk_status="safe_to_answer",
        required_action="answer_directly",
        reason=(
            "The request does not change sensitive account permissions or billing "
            "ownership."
        ),
    )


# ---------------------------------------------------------------------------
# The PydanticAI agent — one base agent wrapped once by Kitaru
# ---------------------------------------------------------------------------


def build_support_agent(
    model: str,
    *,
    prompt_profile: str = "baseline",
    name: str = "support_copilot",
) -> KitaruAgent:
    """Build the durable support copilot.

    The wrapped PydanticAI agent owns the tools. Kitaru's calls strategy records
    each model request and each tool call as its own checkpoint.
    """
    if prompt_profile not in _AGENT_PROMPTS:
        known = ", ".join(sorted(_AGENT_PROMPTS))
        raise ValueError(
            f"Unknown prompt_profile {prompt_profile!r}. Expected one of: {known}"
        )

    agent = Agent(
        model,
        name=name,
        output_type=SupportDecision,
        instructions=_AGENT_PROMPTS[prompt_profile],
        tools=[gather_context, lookup_policy],
    )
    return KitaruAgent(
        agent,
        checkpoint_strategy="calls",
        model_checkpoint_config={"retries": 1},
        tool_checkpoint_config={"retries": 1},
        capture=CapturePolicy(tool_capture="full"),
    )


# ---------------------------------------------------------------------------
# Final artifact checkpoint
# ---------------------------------------------------------------------------


@checkpoint(cache=False)
def publish_support_decision(
    decision: SupportDecision,
) -> Annotated[dict, "support_decision"]:
    """Store the final decision on a stable checkpoint for the demo UI/CLI."""
    return decision.model_dump()


# ---------------------------------------------------------------------------
# The durable flow
# ---------------------------------------------------------------------------


@flow(
    cache=False,
    image=ImageSettings(
        requirements=["pydantic-ai"],
        secret_environment_from=["openai-creds"],
    ),
)
def support_copilot_flow(
    prompt: str, customer: str, model: str, prompt_profile: str
) -> dict:
    """Run the tool-using support copilot under (model, prompt_profile)."""
    agent = build_support_agent(model, prompt_profile=prompt_profile)
    user_prompt = (
        f"Customer: {customer}\n"
        f"Request: {prompt}\n\n"
        "Call gather_context with the customer and request. Then call lookup_policy "
        "with the triage fields. Finally return a SupportDecision."
    )
    result = agent.run_sync(user_prompt)
    return publish_support_decision(result.output)


# ---------------------------------------------------------------------------
# Waiting on a run — calls strategy can produce several terminal adapter
# checkpoints, so the demo waits for terminal status and ignores ambiguous flow
# result extraction. The decision is read later from artifacts.
# ---------------------------------------------------------------------------


def wait_for_completion(handle, *, timeout_seconds: float = 300.0) -> str:
    """Block until the flow finishes; return its exec_id."""
    client = KitaruClient()
    deadline = time.monotonic() + timeout_seconds
    while True:
        run = client.executions.get(handle.exec_id)
        last_status = run.status
        if last_status.is_finished:
            break
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Timed out waiting for execution {handle.exec_id} after "
                f"{timeout_seconds:g}s. Last status: {last_status.value}."
            )
        time.sleep(1.0)

    # Surface failed executions through the normal handle path. Successful
    # calls-strategy runs may still be ambiguous because several adapter-created
    # checkpoints can be terminal result candidates, so suppress only that case.
    with contextlib.suppress(KitaruAmbiguousFlowResultError):
        handle.wait()
    return handle.exec_id


# ---------------------------------------------------------------------------
# Listing prior runs — a plain helper over the SDK client
# ---------------------------------------------------------------------------


def recent_exec_ids(client: KitaruClient, n: int) -> list[str]:
    """Return the ``n`` most recent ORIGINAL (non-replay) exec_ids, newest first.

    Replays show up in the list too; we keep only the originals (those with no
    ``original_exec_id``) so the cohort experiments against real production runs.
    """
    runs = client.executions.list(flow=FLOW_NAME, limit=n * 5)
    originals = [e for e in runs if e.original_exec_id is None]
    return [e.exec_id for e in originals[:n]]
