"""The support-copilot agent and its durable flow, in one place.

A three-step PydanticAI agent — gather_context -> decide -> finalize — made
durable with the Kitaru PydanticAI adapter. Each step is a ``pydantic_ai.Agent``
wrapped once in a ``KitaruAgent``; running it inside the flow turns every model
call into a durable checkpoint (with token usage / cost tracked), so the run can
be replayed.
"""

import contextlib

from pydantic import BaseModel
from pydantic_ai import Agent

from kitaru import (
    ImageSettings,
    KitaruAmbiguousFlowResultError,
    KitaruClient,
    flow,
)
from kitaru.adapters.pydantic_ai import KitaruAgent

#: The flow's registered name (the normalized function name). Used to filter
#: executions with ``client.executions.list(flow=FLOW_NAME)``.
FLOW_NAME = "support_copilot_flow"

#: The checkpoint we replay from — the intermediate "decide" agent. In the
#: adapter's default "calls" strategy, each model call is checkpointed as
#: ``<agent_name>_model_request``, so the decide step's checkpoint is named this.
CUT = "support_decide_model_request"

#: The finalize step's checkpoint name (same naming rule as CUT) — a fallback
#: source when reading the decision back from artifacts.
FINALIZE_CHECKPOINT = "support_finalize_model_request"


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
# Per-step agents — a pydantic_ai.Agent wrapped once in the Kitaru adapter
# ---------------------------------------------------------------------------


def build_gather_agent(
    model,
    *,
    prompt_profile: str = "baseline",
    name: str = "support_gather",
) -> KitaruAgent:
    """Build the gather-context step agent (``GatherResult`` output)."""
    return KitaruAgent(
        Agent(
            model,
            name=name,
            output_type=GatherResult,
            instructions=_GATHER_PROMPTS[prompt_profile],
        )
    )


def build_decide_agent(
    model,
    *,
    prompt_profile: str = "baseline",
    name: str = "support_decide",
) -> KitaruAgent:
    """Build the decide step agent — the CUT (``SupportDecision`` output).

    The baseline prompt restricts permission/SSO changes (``needs_review``);
    ``trimmed_permissions`` answers more directly. Reconfiguring this step is
    what flips the decision on replay.
    """
    return KitaruAgent(
        Agent(
            model,
            name=name,
            output_type=SupportDecision,
            instructions=_DECIDE_PROMPTS[prompt_profile],
        )
    )


def build_finalize_agent(
    model,
    *,
    prompt_profile: str = "baseline",
    name: str = "support_finalize",
) -> KitaruAgent:
    """Build the finalize step agent (``FinalAnswer`` output)."""
    return KitaruAgent(
        Agent(
            model,
            name=name,
            output_type=FinalAnswer,
            instructions=_FINALIZE_PROMPTS[prompt_profile],
        )
    )


# ---------------------------------------------------------------------------
# The durable flow: gather_context -> decide -> finalize.
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
    """gather_context -> decide -> finalize under (model, prompt_profile)."""
    gather = build_gather_agent(model, prompt_profile=prompt_profile)
    triage = gather.run_sync(f"Customer: {customer}\nRequest: {prompt}").output

    decide = build_decide_agent(model, prompt_profile=prompt_profile)
    decision = decide.run_sync(
        f"intent={triage.intent} category={triage.category} triage={triage.triage}"
    ).output

    finalize = build_finalize_agent(model, prompt_profile=prompt_profile)
    answer = finalize.run_sync(
        f"policy_label={decision.policy_label} risk_status={decision.risk_status} "
        f"required_action={decision.required_action} summary={decision.summary!r}"
    ).output

    out = answer.model_dump()
    for key in ("policy_label", "risk_status", "required_action", "summary"):
        if not out.get(key):
            out[key] = getattr(decision, key, "unknown")
    return out


# ---------------------------------------------------------------------------
# Waiting on a run — the adapter's "calls" strategy means a single run produces
# several terminal model-call checkpoints (support_gather_model_request,
# support_decide_model_request, support_finalize_model_request), so Kitaru can't
# auto-pick one flow return value. We don't need it: decisions are read back from
# the checkpoint artifacts. wait() still persists the token-usage roll-up before
# raising the ambiguity error, so we just block to completion and swallow it.
# ---------------------------------------------------------------------------


def wait_for_completion(handle) -> str:
    """Block until the flow finishes; return its exec_id (ignore multi-terminal)."""
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
