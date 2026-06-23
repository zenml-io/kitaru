"""The durable flow: a PydanticAI support-copilot, step by step.

This module defines *only* the flow — three ``@checkpoint`` steps wired together
under a ``@flow``. There is no demo wrapper here. The run/replay story is told
with plain Kitaru SDK primitives in ``demo.py``; this file is just the agent.

The two SDK calls the demo uses against this flow are:

    handle = support_copilot_flow.run(prompt, customer, model, prompt_profile)
    handle = support_copilot_flow.replay(
        exec_id, from_="decide", cache=False,
        model="openai:gpt-5-nano", prompt_profile="trimmed_permissions",
    )

The flow carries its config (``model`` + ``prompt_profile``) as ordinary flow
inputs. Each ``@checkpoint`` step builds its own ``pydantic_ai.Agent`` from those
inputs, so *any* process — the SDK or the ``kitaru executions replay`` CLI — can
rebuild the agents from the recorded execution. ``decide`` is the checkpoint you
replay from: replaying with a new ``model``/``prompt_profile`` re-runs ``decide``
+ ``finalize`` under the new config while the ``gather_context`` head is served
from cache.
"""

from agent import build_decide_agent, build_finalize_agent, build_gather_agent

from kitaru import ImageSettings, KitaruClient, flow
from kitaru.checkpoint import checkpoint

#: The flow's registered name (the normalized function name). Used to filter
#: executions with ``client.executions.list(flow=FLOW_NAME)``.
FLOW_NAME = "support_copilot_flow"

#: The checkpoint we replay from — the intermediate "decide" step.
CUT = "decide"

# ---------------------------------------------------------------------------
# The three-step flow: gather_context -> decide -> finalize.
# Config (model, prompt_profile) travels as flow inputs so the steps can rebuild
# their agents in any process (SDK run, SDK replay, or the kitaru CLI replay).
# ---------------------------------------------------------------------------


@checkpoint
def gather_context(prompt: str, customer: str, model: str, prompt_profile: str) -> dict:
    """Triage / classify the incoming support request."""
    agent = build_gather_agent(model, prompt_profile=prompt_profile)
    result = agent.run_sync(f"Customer: {customer}\nRequest: {prompt}")
    return result.output.model_dump()


@checkpoint
def decide(gather_out: dict, model: str, prompt_profile: str) -> dict:
    """Produce the SupportDecision from the triage result (the CUT)."""
    agent = build_decide_agent(model, prompt_profile=prompt_profile)
    triage = (
        f"intent={gather_out.get('intent', 'unknown')} "
        f"category={gather_out.get('category', 'general')} "
        f"triage={gather_out.get('triage', 'medium')}"
    )
    return agent.run_sync(triage).output.model_dump()


@checkpoint
def finalize(decide_out: dict, model: str, prompt_profile: str) -> dict:
    """Assemble the customer-facing answer (single terminal step)."""
    agent = build_finalize_agent(model, prompt_profile=prompt_profile)
    summary = (
        f"policy_label={decide_out.get('policy_label', 'unknown')} "
        f"risk_status={decide_out.get('risk_status', 'unknown')} "
        f"required_action={decide_out.get('required_action', 'unknown')} "
        f"summary={decide_out.get('summary', '')!r}"
    )
    out = agent.run_sync(summary).output.model_dump()
    # Carry the decision fields forward so the answer artifact is self-contained.
    for key in ("policy_label", "risk_status", "required_action", "summary"):
        if not out.get(key):
            out[key] = decide_out.get(key, "unknown")
    return out


# On a containerized stack (e.g. Kubernetes) the steps run in the image, so it
# needs pydantic-ai installed and OPENAI_API_KEY in the pod. The latter comes from
# the `openai-creds` secret (see the README); locally it comes from your env.
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
    gathered = gather_context(prompt, customer, model, prompt_profile)
    decided = decide(gathered, model, prompt_profile)
    return finalize(decided, model, prompt_profile)


# ---------------------------------------------------------------------------
# Listing prior runs — a plain helper over the SDK client, no wrapper.
# ---------------------------------------------------------------------------


def recent_exec_ids(client: KitaruClient, n: int) -> list[str]:
    """Return the ``n`` most recent ORIGINAL (non-replay) exec_ids, newest first.

    Replays show up in the list too; we keep only the originals (those with no
    ``original_exec_id``) so the cohort experiments against real production runs.
    """
    runs = client.executions.list(flow=FLOW_NAME, limit=n * 5)
    originals = [e for e in runs if e.original_exec_id is None]
    return [e.exec_id for e in originals[:n]]
