"""Durable PydanticAI support-copilot with Kitaru checkpoints and replay.

    from support_copilot import KitaruAdapterPA
    from utils import cost, latency, quality_judge
    from cohort import cohort

    agent   = KitaruAdapterPA(model="openai:gpt-5-mini")
    exec_id = agent.run(prompt, customer)

    rerun  = agent.rerun(exec_id)
    replay = agent.replay(exec_id, at="decide",
                          model="openai:gpt-5-nano",
                          prompt_profile="trimmed_permissions")
    replay.diff(rerun)

    report = cohort(agent.last_executions(10)).experiment(
        agent, variant=replay.recipe,
        metrics=[cost, latency, quality_judge], repeats=1)
    report.summary()
    report.regressions()

Module structure:
- Module-level @checkpoint steps (gather_context / decide / finalize) and
  the @flow are registered once at import — never clobbered.
- ContextVar _active_agents holds the three step agents for the in-flight run;
  steps read them at execution time; kept ambient so they're never serialized
  as checkpoint inputs.
- RunHandle is the small return value of rerun/replay: .exec_id, .decision,
  .model, .recipe, .diff(other) -> DriftReport.
"""
from __future__ import annotations

import logging
from contextvars import ContextVar
from typing import Any

from pydantic_ai import Agent

from kitaru import flow, KitaruClient
from kitaru.checkpoint import checkpoint

from agent import (
    SupportDecision,
    build_decide_agent,
    build_finalize_agent,
    build_gather_agent,
)
from utils import CUT, DriftReport, Recipe, decision_of, diff_decisions, decision_from_artifacts

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Ambient agents — set by _activate() before each flow dispatch so that the
# module-level @checkpoint steps pick up the correct per-adapter agents without
# requiring them as serialized checkpoint inputs.
# ---------------------------------------------------------------------------

_active_agents: ContextVar[tuple[Agent, Agent, Agent]] = ContextVar("_active_agents")


# ---------------------------------------------------------------------------
# Module-level @checkpoint steps (registered once at import, never clobbered)
# ---------------------------------------------------------------------------

@checkpoint
def gather_context(prompt: str, customer: str) -> dict:  # type: ignore[return]
    """Triage / classify the incoming support request."""
    _gather_agent, _, _ = _active_agents.get()
    result = _gather_agent.run_sync(
        f"Customer: {customer}\nRequest: {prompt}"
    )
    return result.output.model_dump()


@checkpoint
def decide(gather_out: dict) -> dict:  # type: ignore[return]
    """Produce the SupportDecision from the triage result (the CUT)."""
    _, _decide_agent, _ = _active_agents.get()
    triage_summary = (
        f"intent={gather_out.get('intent', 'unknown')} "
        f"category={gather_out.get('category', 'general')} "
        f"triage={gather_out.get('triage', 'medium')}"
    )
    result = _decide_agent.run_sync(triage_summary)
    return result.output.model_dump()


@checkpoint
def finalize(decide_out: dict) -> dict:  # type: ignore[return]
    """Assemble the customer-facing answer (single terminal step)."""
    _, _, _finalize_agent = _active_agents.get()
    decision_summary = (
        f"policy_label={decide_out.get('policy_label', 'unknown')} "
        f"risk_status={decide_out.get('risk_status', 'unknown')} "
        f"required_action={decide_out.get('required_action', 'unknown')} "
        f"summary={decide_out.get('summary', '')!r}"
    )
    result = _finalize_agent.run_sync(decision_summary)
    out = result.output.model_dump()
    for key in ("policy_label", "risk_status", "required_action", "summary"):
        if key not in out or not out[key]:
            out[key] = decide_out.get(key, "unknown")
    return out


# ---------------------------------------------------------------------------
# Module-level @flow (registered once at import, never clobbered)
# ---------------------------------------------------------------------------

@flow(cache=False)
def support_copilot_flow(prompt: str, customer: str) -> dict:  # type: ignore[return]
    """Three-step support copilot flow: gather -> decide -> finalize."""
    gathered = gather_context(prompt, customer)
    decided = decide(gathered)
    return finalize(decided)


# ---------------------------------------------------------------------------
# RunHandle — returned by rerun() and replay()
# ---------------------------------------------------------------------------

class RunHandle:
    """Lightweight result handle returned by ``rerun`` and ``replay``.

    Attributes:
        exec_id:  The execution ID of the completed run.
        decision: The SupportDecision dict extracted from the execution.
        model:    The PydanticAI model used for this run (for judge metrics).
        recipe:   The Recipe that produced this run (identity for rerun;
                  model/prompt_profile/at for replay).
    """

    def __init__(
        self,
        exec_id: str,
        decision: dict,
        recipe: Recipe,
        model: Any = None,
    ) -> None:
        self.exec_id = exec_id
        self.decision = decision
        self.recipe = recipe
        self.model = model

    def diff(self, other: "RunHandle") -> DriftReport:
        """Compare this handle's decision against *other*'s decision."""
        return diff_decisions(self.decision, other.decision)


# ---------------------------------------------------------------------------
# KitaruAdapterPA
# ---------------------------------------------------------------------------

class KitaruAdapterPA:
    """Durable execution adapter for the three-step PydanticAI support-copilot.

    Args:
        model:          PydanticAI-compatible model (real or TestModel for tests).
        prompt_profile: System-prompt profile ("baseline" or "trimmed_permissions").
        name:           Stable name prefix for the step agents.

    Public surface:
        run(prompt, customer) -> str           execute and return exec_id
        rerun(exec_id) -> RunHandle            no edit: cached head, live tail
        replay(exec_id, ...) -> RunHandle      WITH edit: reconfigure decide + tail
        last_executions(n) -> list[str]        n most recent baseline exec_ids
    """

    def __init__(
        self,
        *,
        model: Any,
        prompt_profile: str = "baseline",
        name: str = "support_copilot",
    ) -> None:
        self.name = name
        self._model = model
        self._prompt_profile = prompt_profile
        self._client = KitaruClient()

        self._gather_agent = build_gather_agent(
            model, prompt_profile=prompt_profile, name=f"{name}_gather",
        )
        self._decide_agent = build_decide_agent(
            model, prompt_profile=prompt_profile, name=f"{name}_decide",
        )
        self._finalize_agent = build_finalize_agent(
            model, prompt_profile=prompt_profile, name=f"{name}_finalize",
        )

        self._flow = support_copilot_flow
        self._results: dict[str, dict] = {}

    def _activate(self) -> None:
        """Set the ContextVar to this adapter's agents before any dispatch."""
        _active_agents.set((self._gather_agent, self._decide_agent, self._finalize_agent))

    def run(self, prompt: str, customer: str) -> str:
        """Run the three-step flow and return the exec_id.

        Blocks until the execution finishes.  The flow result is cached by
        exec_id to enable a fast path in decision lookup.
        """
        self._activate()
        handle = self._flow.run(prompt, customer)
        result = handle.wait()
        exec_id = handle.exec_id
        if isinstance(result, dict) and "risk_status" in result:
            self._results[exec_id] = result
        return exec_id

    def cut_of(self, exec_id: str) -> str:
        """Return the CUT checkpoint name for *exec_id*.

        Raises:
            RuntimeError: If no ``"decide"`` checkpoint exists.
        """
        run = self._client.executions.get(exec_id)
        names = [c.name for c in run.checkpoints]
        if CUT not in names:
            raise RuntimeError(
                f"CUT checkpoint {CUT!r} not found in execution {exec_id}. "
                f"Checkpoints present: {names}."
            )
        return CUT

    def decision_of(self, exec_id: str) -> dict:
        """Return the SupportDecision dict for *exec_id*.

        Checks in-memory cache first, then artifact store.

        Raises:
            RuntimeError: If the decision cannot be found via any path.
        """
        return decision_of(self._client, exec_id, cache=self._results)

    def rerun(self, exec_id: str) -> RunHandle:
        """Re-execute from the CUT with NO config change (cached head, live tail).

        Returns:
            A RunHandle with the identity Recipe and the re-run decision.
        """
        self._activate()
        handle = self._flow.replay(exec_id, from_=self.cut_of(exec_id), cache=False)
        result = handle.wait()
        replay_id = handle.exec_id
        if isinstance(result, dict) and "risk_status" in result:
            self._results[replay_id] = result
        dec = decision_from_artifacts(self._client, replay_id)
        return RunHandle(
            exec_id=replay_id,
            decision=dec,
            recipe=Recipe(at=CUT),
            model=self._model,
        )

    def replay(
        self,
        exec_id: str,
        *,
        at: str = CUT,
        model: Any = None,
        prompt_profile: str | None = None,
    ) -> RunHandle:
        """Re-execute from *at* WITH a config change (new model and/or prompt).

        Builds a reconfigured adapter and replays from *at*, re-running the
        decide + finalize steps under the new configuration.  The
        gather_context head is served from cache.

        Args:
            exec_id: The baseline execution to replay.
            at: The checkpoint name to replay from (default: CUT = ``"decide"``).
            model: New PydanticAI model for the reconfigured adapter.
                Defaults to ``self``'s model when not supplied.
            prompt_profile: New system-prompt profile.  Defaults to ``self``'s
                profile when not supplied.

        Returns:
            A RunHandle capturing the Recipe and the replay decision.
        """
        resolved_model = model if model is not None else self._model
        resolved_profile = prompt_profile if prompt_profile is not None else self._prompt_profile
        reconfigured = KitaruAdapterPA(
            model=resolved_model,
            prompt_profile=resolved_profile,
            name=self.name,
        )
        reconfigured._activate()
        handle = reconfigured._flow.replay(exec_id, from_=at, cache=False)
        result = handle.wait()
        replay_id = handle.exec_id
        if isinstance(result, dict) and "risk_status" in result:
            self._results[replay_id] = result
        dec = decision_from_artifacts(self._client, replay_id)
        recipe = Recipe(model=model, prompt_profile=prompt_profile, at=at)
        return RunHandle(
            exec_id=replay_id,
            decision=dec,
            recipe=recipe,
            model=resolved_model,
        )

    def last_executions(self, n: int) -> list[str]:
        """Return the ``n`` most recent exec_ids for this adapter's flow, newest first.

        Excludes replay executions (those with ``original_exec_id`` set).
        """
        from kitaru._source_aliases import build_pipeline_registration_name, callable_name as _callable_name
        flow_name = build_pipeline_registration_name(_callable_name(self._flow._func))
        executions = self._client.executions.list(
            flow=flow_name,
            limit=n * 5,
        )
        originals = [e for e in executions if e.original_exec_id is None]
        return [e.exec_id for e in originals[:n]]
