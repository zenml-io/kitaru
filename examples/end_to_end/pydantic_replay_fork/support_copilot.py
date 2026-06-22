"""THE STORY SURFACE — PydanticAI support-copilot with Kitaru durable execution.

Usage (reads top-to-bottom like the promise)
---------------------------------------------
    from support_copilot import KitaruAdapterPA
    from utils import cost, latency, quality_judge
    from cohort import cohort

    agent  = KitaruAdapterPA(model="openai:gpt-5-mini")
    exec_id = agent.run(prompt, customer)          # production run (durable)

    rerun   = agent.rerun(exec_id)                 # no edit: cached head, live tail
    replay  = agent.replay(exec_id,                # WITH edit: reconfigure decide step
                           at="decide",
                           model="openai:gpt-5-nano",
                           prompt_profile="trimmed_permissions")
    replay.diff(rerun)                             # -> DriftReport

    report = cohort(agent.last_executions(10)).experiment(
        agent, variant=replay.recipe,
        metrics=[cost, latency, quality_judge], repeats=1)
    report.summary()
    report.regressions()

Module structure
----------------
- Module-level ``@checkpoint`` steps (``gather_context`` / ``decide`` /
  ``finalize``) and the ``@flow`` are defined ONCE at import so the module-level
  source aliases registered by Kitaru/ZenML are never clobbered.
- The ``ContextVar`` ``_active_agents`` + ``_activate()`` fix the alias-overwrite
  race (Finding 1): each adapter sets the ContextVar before dispatching the flow,
  so rerun-after-replay always uses the correct adapter's agents.
- ``RunHandle`` is the small return value of ``rerun``/``replay``: ``.exec_id``,
  ``.decision``, ``.recipe``, ``.diff(other) -> DriftReport``.
"""
from __future__ import annotations

import logging
from contextvars import ContextVar
from typing import Any

from pydantic_ai import Agent

from kitaru import flow, KitaruClient
from kitaru.checkpoint import checkpoint

from .agent import (
    SupportDecision,
    build_decide_agent,
    build_finalize_agent,
    build_gather_agent,
)
from .utils import CUT, Recipe, decision_of, _decision_from_artifacts

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ContextVar for ambient agents (alias-overwrite race fix — Finding 1)
# ---------------------------------------------------------------------------

# Holds the three pydantic_ai agents for the currently-dispatching adapter.
# KitaruAdapterPA._activate() sets this before any flow dispatch.
# The module-level @checkpoint steps read from it at execution time.
# NOT a checkpoint input — stays ambient/config, never serialized.
_active_agents: ContextVar[tuple[Agent, Agent, Agent]] = ContextVar("_active_agents")


# ---------------------------------------------------------------------------
# Module-level @checkpoint steps (registered ONCE at import, never clobbered)
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
    # Propagate SupportDecision fields so decision_of can read them from the
    # finalize artifact as a fallback.
    for key in ("policy_label", "risk_status", "required_action", "summary"):
        if key not in out or not out[key]:
            out[key] = decide_out.get(key, "unknown")
    return out


# ---------------------------------------------------------------------------
# Module-level @flow (registered ONCE at import, never clobbered)
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
        # Internal: stored so quality_judge metric can build a judge.
        self._model = model

    def diff(self, other: "RunHandle") -> "DriftReport":
        """Compare this handle's decision against *other*'s decision.

        Returns:
            A DriftReport (``has_fork_drift``, per-field comparison in ``.fork``).
        """
        from kitaru.adapters.langgraph.replay._drift import DriftReport as _DR
        from kitaru.adapters.langgraph.replay._drift import compare_decisions
        return _DR(reproduction=[], fork=compare_decisions(self.decision, other.decision))


# Re-export DriftReport so callers can type-annotate without reaching into kitaru internals.
try:
    from kitaru.adapters.langgraph.replay._drift import DriftReport  # noqa: F401
except ImportError:
    pass


# ---------------------------------------------------------------------------
# KitaruAdapterPA
# ---------------------------------------------------------------------------

class KitaruAdapterPA:
    """Durable execution adapter for the three-step PydanticAI support-copilot.

    Args:
        model: A PydanticAI-compatible model (real or ``TestModel`` for tests).
            The same model is used for all three steps.
        prompt_profile: System-prompt profile (``"baseline"`` or
            ``"trimmed_permissions"``).  Drives the ``decide`` step's behaviour.
        name: Stable name prefix for the step agents.

    Public surface (only):
        run(prompt, customer) -> str                exec_id
        rerun(exec_id) -> RunHandle                 no edit: cached head, live tail
        replay(exec_id, *, at, model, prompt_profile) -> RunHandle   WITH edit
        last_executions(n) -> list[str]
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

        # Build per-step raw pydantic_ai.Agent objects.
        # Raw agents are correct here — @checkpoint provides the Kitaru boundary.
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

        # In-memory decision cache populated at run() time for fast lookup.
        self._results: dict[str, dict] = {}

    def _activate(self) -> None:
        """Set the ContextVar to this adapter's agents before any dispatch.

        Must be called at the top of every method that dispatches the flow
        (``run``, ``rerun``, and the reconfigured adapter's dispatch in
        ``replay``).  This ensures that the module-level @checkpoint steps
        read from the correct adapter's agents.
        """
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

    def _cut_of(self, exec_id: str) -> str:
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

    # Keep cut_of as a public alias so existing tests that call adapter.cut_of(...)
    # continue to work during the migration period.
    def cut_of(self, exec_id: str) -> str:
        """Public alias for _cut_of (kept for test compatibility)."""
        return self._cut_of(exec_id)

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
        handle = self._flow.replay(exec_id, from_=self._cut_of(exec_id), cache=False)
        result = handle.wait()
        replay_id = handle.exec_id
        if isinstance(result, dict) and "risk_status" in result:
            self._results[replay_id] = result
        dec = _decision_from_artifacts(self._client, replay_id)
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
        dec = _decision_from_artifacts(self._client, replay_id)
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

    # -----------------------------------------------------------------------
    # Legacy verb aliases kept for test-migration compatibility
    # (removed once all callers are updated to rerun/replay)
    # -----------------------------------------------------------------------

    def reproduce(self, exec_id: str) -> str:
        """Legacy alias for rerun(); returns exec_id string."""
        handle = self.rerun(exec_id)
        return handle.exec_id

    def experiment(
        self,
        exec_id: str,
        *,
        model: Any = None,
        prompt_profile: str | None = None,
    ) -> str:
        """Legacy alias for replay(); returns exec_id string."""
        handle = self.replay(exec_id, model=model, prompt_profile=prompt_profile)
        return handle.exec_id

    def diff(self, baseline_exec: str, other_exec: str) -> "DriftReport":  # type: ignore[name-defined]
        """Legacy two-id diff; prefer RunHandle.diff(other)."""
        from kitaru.adapters.langgraph.replay._drift import DriftReport, compare_decisions
        base = self.decision_of(baseline_exec)
        other = self.decision_of(other_exec)
        return DriftReport(reproduction=[], fork=compare_decisions(base, other))

    def cohort(
        self,
        *,
        model: Any = None,
        prompt_profile: str | None = None,
        n: int = 10,
    ) -> Any:
        """Legacy cohort() method — returns a pipeline.CohortReport.

        Kept for backward compatibility with existing tests that import from
        ``pipeline`` and call ``adapter.cohort(...)``.  New code should use
        ``cohort(agent.last_executions(n)).experiment(agent, variant=..., ...)``
        from ``cohort.py`` and ``support_copilot.py`` directly.
        """
        import logging as _logging
        from .utils import build_judge as _build_judge, _extract_cost, _extract_latency_s, _decision_from_artifacts
        from .utils import CohortReport, CohortRow
        from kitaru.adapters.langgraph.replay._drift import DriftReport as _DR, compare_decisions as _cmp

        judge = _build_judge(self._model)
        exec_ids = self.last_executions(n)
        report = CohortReport()

        rows: list[CohortRow] = []
        for base_id in exec_ids:
            row = CohortRow(base_exec_id=base_id)
            try:
                self.cut_of(base_id)
            except Exception as exc:
                row.skipped = True
                row.skip_reason = str(exc)
                rows.append(row)
                report.skipped_count += 1
                continue

            try:
                repro_id = self.reproduce(base_id)
            except Exception as exc:
                row.skipped = True
                row.skip_reason = f"reproduce failed: {exc}"
                rows.append(row)
                report.skipped_count += 1
                continue

            row.repro_exec_id = repro_id
            rows.append(row)

        for row in rows:
            if row.skipped:
                report.rows.append(row)
                continue

            base_id = row.base_exec_id
            repro_id = row.repro_exec_id

            try:
                exp_id = self.experiment(base_id, model=model, prompt_profile=prompt_profile)
            except Exception as exc:
                row.skipped = True
                row.skip_reason = f"experiment failed: {exc}"
                report.rows.append(row)
                report.skipped_count += 1
                continue

            row.exp_exec_id = exp_id

            repro_decision = None
            exp_decision = None
            try:
                repro_decision = _decision_from_artifacts(self._client, repro_id)
                exp_decision = _decision_from_artifacts(self._client, exp_id)
                drift = _DR(reproduction=[], fork=_cmp(repro_decision, exp_decision))
                row.decision_changed = drift.has_fork_drift
            except Exception as exc:
                _logging.getLogger(__name__).warning("diff failed: %s", exc)
                row.decision_changed = None

            row.cost_baseline_usd = _extract_cost(self._client, repro_id)
            row.cost_experiment_usd = _extract_cost(self._client, exp_id)
            row.latency_baseline_s = _extract_latency_s(self._client, repro_id)
            row.latency_experiment_s = _extract_latency_s(self._client, exp_id)

            try:
                if repro_decision is not None:
                    repro_summary = (
                        f"policy={repro_decision.get('policy_label')} "
                        f"risk={repro_decision.get('risk_status')} "
                        f"action={repro_decision.get('required_action')} "
                        f"summary={repro_decision.get('summary', '')!r}"
                    )
                    row.judge_score_baseline = judge.run_sync(repro_summary).output.score
            except Exception as exc:
                _logging.getLogger(__name__).warning("Judge baseline failed: %s", exc)

            try:
                if exp_decision is not None:
                    exp_summary = (
                        f"policy={exp_decision.get('policy_label')} "
                        f"risk={exp_decision.get('risk_status')} "
                        f"action={exp_decision.get('required_action')} "
                        f"summary={exp_decision.get('summary', '')!r}"
                    )
                    row.judge_score_experiment = judge.run_sync(exp_summary).output.score
            except Exception as exc:
                _logging.getLogger(__name__).warning("Judge experiment failed: %s", exc)

            report.rows.append(row)

        return report
