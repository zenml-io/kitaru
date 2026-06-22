"""PydanticAI support-copilot wrapped as a durable Kitaru flow.

    from support_copilot import KitaruAdapterPA
    from utils import cost, latency, quality_judge
    from cohort import cohort

    agent   = KitaruAdapterPA(model="openai:gpt-5-mini")
    exec_id = agent.run(prompt, customer)

    rerun  = agent.rerun(exec_id)                       # reproduce: cached head, live tail, no edits
    replay = agent.replay(exec_id, model="openai:gpt-5-nano",
                          prompt_profile="trimmed_permissions")
    replay.diff(rerun)                                  # did the change move the decision?

    report = cohort(agent.last_executions(10)).experiment(
        agent, variant=replay.recipe,
        metrics=[cost, latency, quality_judge], repeats=1)
    report.summary(); report.regressions()

The flow carries its config (``model`` spec + ``prompt_profile``) as ordinary
flow inputs.  Each ``@checkpoint`` step builds its own ``pydantic_ai.Agent`` from
those inputs, so *any* process — the SDK or the ``kitaru executions replay`` CLI —
can rebuild the agents from the recorded execution.  ``decide`` is the CUT: the
step you replay from.  Replaying with a new ``model``/``prompt_profile`` re-runs
``decide`` + ``finalize`` under the new config while the ``gather_context`` head
is served from cache.
"""
from __future__ import annotations

from typing import Any

from kitaru import KitaruClient, flow
from kitaru.checkpoint import checkpoint

from agent import build_decide_agent, build_finalize_agent, build_gather_agent
from utils import CUT, Recipe, decision_from_artifacts, decision_of, diff_decisions


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


@flow(cache=False)
def support_copilot_flow(prompt: str, customer: str, model: str, prompt_profile: str) -> dict:
    """gather_context -> decide -> finalize, each running under (model, prompt_profile)."""
    gathered = gather_context(prompt, customer, model, prompt_profile)
    decided = decide(gathered, model, prompt_profile)
    return finalize(decided, model, prompt_profile)


# ---------------------------------------------------------------------------
# RunHandle — the small result of rerun()/replay()
# ---------------------------------------------------------------------------

class RunHandle:
    """Result of a rerun or replay: the new exec_id, its decision, and the recipe."""

    def __init__(self, exec_id: str, decision: dict, recipe: Recipe, model: Any = None) -> None:
        self.exec_id = exec_id
        self.decision = decision
        self.recipe = recipe
        self.model = model

    def diff(self, other: "RunHandle"):
        """Compare this run's decision against ``other`` (the baseline)."""
        return diff_decisions(other.decision, self.decision)


# ---------------------------------------------------------------------------
# KitaruAdapterPA — wrap the flow; run, rerun (no edit), replay (with edit)
# ---------------------------------------------------------------------------

class KitaruAdapterPA:
    """Run the support-copilot flow durably, and rerun/replay recorded executions."""

    def __init__(self, *, model: str, prompt_profile: str = "baseline") -> None:
        self._model = model
        self._prompt_profile = prompt_profile
        self._client = KitaruClient()

    def run(self, prompt: str, customer: str) -> str:
        """Run the three-step flow and return the exec_id."""
        handle = support_copilot_flow.run(prompt, customer, self._model, self._prompt_profile)
        handle.wait()
        return handle.exec_id

    def cut_of(self, exec_id: str) -> str:
        """Return the CUT checkpoint name for ``exec_id`` (raises if absent)."""
        run = self._client.executions.get(exec_id)
        names = [c.name for c in run.checkpoints]
        if CUT not in names:
            raise RuntimeError(f"CUT {CUT!r} not found in {exec_id}; checkpoints: {names}.")
        return CUT

    def decision_of(self, exec_id: str) -> dict:
        """Return the SupportDecision dict recorded for ``exec_id``."""
        return decision_of(self._client, exec_id)

    def rerun(self, exec_id: str) -> RunHandle:
        """Re-execute from the CUT with NO config change (cached head, live tail)."""
        handle = support_copilot_flow.replay(exec_id, from_=self.cut_of(exec_id), cache=False)
        handle.wait()
        return RunHandle(
            exec_id=handle.exec_id,
            decision=decision_from_artifacts(self._client, handle.exec_id),
            recipe=Recipe(at=CUT),
            model=self._model,
        )

    def replay(
        self,
        exec_id: str,
        *,
        at: str = CUT,
        model: str | None = None,
        prompt_profile: str | None = None,
    ) -> RunHandle:
        """Re-execute from ``at`` WITH a config change (new model and/or prompt).

        The new config is passed as flow-input overrides, so ``decide`` + ``finalize``
        re-run under it while the ``gather_context`` head is served from cache.
        """
        new_model = model if model is not None else self._model
        new_profile = prompt_profile if prompt_profile is not None else self._prompt_profile
        handle = support_copilot_flow.replay(
            exec_id, from_=at, cache=False, model=new_model, prompt_profile=new_profile,
        )
        handle.wait()
        return RunHandle(
            exec_id=handle.exec_id,
            decision=decision_from_artifacts(self._client, handle.exec_id),
            recipe=Recipe(model=model, prompt_profile=prompt_profile, at=at),
            model=new_model,
        )

    def last_executions(self, n: int) -> list[str]:
        """Return the ``n`` most recent original (non-replay) exec_ids, newest first."""
        from kitaru._source_aliases import build_pipeline_registration_name, callable_name
        flow_name = build_pipeline_registration_name(callable_name(support_copilot_flow._func))
        runs = self._client.executions.list(flow=flow_name, limit=n * 5)
        originals = [e for e in runs if e.original_exec_id is None]
        return [e.exec_id for e in originals[:n]]
