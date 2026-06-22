"""Kitaru durable execution adapter for the PydanticAI support-copilot (multi-step).

R1–R4 of the REVISED TASK SEQUENCE — 2026-06-22.

Structure
---------
The flow ``support_copilot`` is composed of three explicit ``@checkpoint`` step
functions, each wrapping a raw ``pydantic_ai.Agent``:

  gather_context(prompt, customer) -> dict
      Triage / classify the incoming request.  Returns a ``GatherResult`` dict.

  decide(gather_out: dict) -> dict
      Produce the ``SupportDecision`` from the triage result.  This is the CUT
      (intermediate step).  The decide step's system prompt switches when
      ``prompt_profile="trimmed_permissions"`` is supplied — reconfiguring it
      flips the decision.

  finalize(decide_out: dict) -> dict
      Assemble the customer-facing answer from the decision.  This is the
      single terminal step in the ZenML DAG.

Chaining via artifacts
----------------------
``gather_context`` produces a ZenML artifact that is passed directly as the
argument to ``decide``.  ``decide`` produces an artifact that is passed to
``finalize``.  The ZenML DAG therefore has exactly one terminal (``finalize``),
so ``flow.run(...).wait()`` succeeds without ``_MultipleTerminalStepsOutputError``.

Each ``@checkpoint`` body uses raw ``pydantic_ai.Agent``  (not ``KitaruAgent``).
Inside an explicit ``@checkpoint``, ``KitaruAgent`` is a passthrough, so the raw
agent is correct and avoids the double-wrapping footgun.

CUT
---
``CUT = "decide"`` is the fixed checkpoint name for the intermediate step.  Use
``cut_of(exec_id)`` to resolve it per-run (validates that the checkpoint exists).

decision_of
-----------
Reads the ``SupportDecision`` dict from the ``decide`` (preferred) or
``finalize`` checkpoint artifact.  Raises loudly if not found (Task 3 loud-
failure behavior preserved).

reproduce / diff
----------------
Carry over Task 4 semantics: ``reproduce`` replays from CUT; ``diff`` uses
``_drift.compare_decisions``.

Alias-overwrite race fix (Finding 1)
--------------------------------------
The three ``@checkpoint`` step functions and the ``@flow`` are defined ONCE at
module scope so the module-level source aliases (used by Kitaru/ZenML for replay
dispatch) are registered exactly once at import time and never clobbered.

Each step reads its three agents from a module-level ``ContextVar``
(``_active_agents``).  ``KitaruAdapterPA._activate()`` sets the ContextVar to
the adapter's own agents before any flow dispatch.  This ensures that
``reproduce()`` after ``experiment()`` always runs under the adapter whose
``_activate()`` was called last — i.e. the adapter that issued the dispatch.
"""
from __future__ import annotations

import dataclasses
import logging
from contextvars import ContextVar
from typing import Any

from pydantic import BaseModel
from pydantic_ai import Agent

from kitaru import flow, KitaruClient
from kitaru.checkpoint import checkpoint

from .agent import (
    SupportDecision,
    build_decide_agent,
    build_finalize_agent,
    build_gather_agent,
)

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# R4 — LLM-judge factory
# ---------------------------------------------------------------------------

class QualityScore(BaseModel):
    """Typed output for the LLM quality judge (score 1–5)."""

    score: int = 3


def build_judge(model: Any) -> Agent:
    """Build a raw pydantic_ai.Agent that scores an answer 1–5.

    In tests, use ``TestModel(custom_output_args={"score": N})`` to make the
    judge deterministic.  In production, a real model evaluates the answer
    quality on a 5-point scale.

    Args:
        model: A PydanticAI-compatible model (real or ``TestModel``).

    Returns:
        A ``pydantic_ai.Agent`` with output_type ``QualityScore``.
    """
    return Agent(
        model,
        name="support_quality_judge",
        output_type=QualityScore,
        instructions=(
            "You are a quality evaluator for support-copilot answers. "
            "Given a support decision summary, rate the quality of the answer "
            "on a scale of 1 (poor) to 5 (excellent). "
            "Focus on clarity, correctness, and helpfulness."
        ),
    )


# ---------------------------------------------------------------------------
# R4 — CohortReport
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class CohortRow:
    """Per-run row in the cohort comparison.

    Fields:
        base_exec_id: The baseline execution ID (original production run).
        repro_exec_id: The reproduce execution ID (baseline replay from CUT).
        exp_exec_id: The experiment execution ID (reconfigured replay from CUT).
        decision_changed: Whether the decision drifted between repro and experiment.
        cost_baseline_usd: display_cost_usd from the reproduce run (0.0 if unavailable).
        cost_experiment_usd: display_cost_usd from the experiment run (0.0 if unavailable).
        latency_baseline_s: Wall-clock seconds for the reproduce run (None if timestamps absent).
        latency_experiment_s: Wall-clock seconds for the experiment run (None if timestamps absent).
        judge_score_baseline: QualityScore.score from the judge on the reproduce decision.
        judge_score_experiment: QualityScore.score from the judge on the experiment decision.
        skipped: True if this row was skipped (e.g. CUT not resolvable).
        skip_reason: Human-readable reason for the skip (None if not skipped).
    """

    base_exec_id: str
    repro_exec_id: str | None = None
    exp_exec_id: str | None = None
    decision_changed: bool | None = None
    cost_baseline_usd: float = 0.0
    cost_experiment_usd: float = 0.0
    latency_baseline_s: float | None = None
    latency_experiment_s: float | None = None
    judge_score_baseline: int | None = None
    judge_score_experiment: int | None = None
    skipped: bool = False
    skip_reason: str | None = None


@dataclasses.dataclass
class CohortReport:
    """Aggregate cohort comparison report.

    ``improvement`` is True when the experiment is simultaneously:
    - cheaper (mean cost <= baseline mean cost), AND
    - faster (mean latency <= baseline mean latency), AND
    - quality-not-worse (mean judge score >= baseline mean judge score
      within QUALITY_TOLERANCE).

    Under TestModel, cost will always be 0.0; in that case the cost
    criterion is treated as neutral (equal cost is not worse).
    """

    QUALITY_TOLERANCE: float = 0.1  # experiment >= baseline - tolerance

    rows: list[CohortRow] = dataclasses.field(default_factory=list)
    skipped_count: int = 0

    @property
    def decision_change_count(self) -> int:
        """Number of non-skipped rows where the decision changed."""
        return sum(1 for r in self.rows if r.decision_changed is True)

    @property
    def mean_cost_baseline_usd(self) -> float:
        """Mean display_cost_usd across non-skipped rows (baseline/reproduce runs)."""
        costs = [r.cost_baseline_usd for r in self.rows if not r.skipped]
        return sum(costs) / len(costs) if costs else 0.0

    @property
    def mean_cost_experiment_usd(self) -> float:
        """Mean display_cost_usd across non-skipped rows (experiment runs)."""
        costs = [r.cost_experiment_usd for r in self.rows if not r.skipped]
        return sum(costs) / len(costs) if costs else 0.0

    @property
    def mean_latency_baseline_s(self) -> float:
        """Mean wall-clock latency (seconds) across non-skipped rows (baseline runs)."""
        lats = [r.latency_baseline_s for r in self.rows if not r.skipped and r.latency_baseline_s is not None]
        return sum(lats) / len(lats) if lats else 0.0

    @property
    def mean_latency_experiment_s(self) -> float:
        """Mean wall-clock latency (seconds) across non-skipped rows (experiment runs)."""
        lats = [r.latency_experiment_s for r in self.rows if not r.skipped and r.latency_experiment_s is not None]
        return sum(lats) / len(lats) if lats else 0.0

    @property
    def mean_judge_score_baseline(self) -> float | None:
        """Mean judge score across non-skipped rows (baseline/reproduce decisions)."""
        scores = [r.judge_score_baseline for r in self.rows if not r.skipped and r.judge_score_baseline is not None]
        return sum(scores) / len(scores) if scores else None

    @property
    def mean_judge_score_experiment(self) -> float | None:
        """Mean judge score across non-skipped rows (experiment decisions)."""
        scores = [r.judge_score_experiment for r in self.rows if not r.skipped and r.judge_score_experiment is not None]
        return sum(scores) / len(scores) if scores else None

    @property
    def improvement(self) -> bool:
        """True iff experiment is cheaper, faster, and quality-not-worse than baseline.

        Cost criterion: experiment mean cost <= baseline mean cost.
        Latency criterion: experiment mean latency <= baseline mean latency.
        Quality criterion: experiment mean judge score >= baseline mean judge score
          minus QUALITY_TOLERANCE.

        All three must hold simultaneously.  Under TestModel, cost is always
        0.0 for both variants — the cost criterion evaluates to True (equal).
        """
        # Cost: cheaper-or-equal.
        cost_ok = self.mean_cost_experiment_usd <= self.mean_cost_baseline_usd

        # Latency: faster-or-equal.
        latency_ok = self.mean_latency_experiment_s <= self.mean_latency_baseline_s

        # Quality: not-worse (within tolerance).
        base_score = self.mean_judge_score_baseline
        exp_score = self.mean_judge_score_experiment
        if base_score is None or exp_score is None:
            quality_ok = True  # no judge data → treat as neutral
        else:
            quality_ok = exp_score >= (base_score - self.QUALITY_TOLERANCE)

        return cost_ok and latency_ok and quality_ok

    def __str__(self) -> str:
        lines = [
            "CohortReport",
            f"  rows: {len(self.rows)} | skipped: {self.skipped_count} | changed: {self.decision_change_count}",
            f"  cost     baseline={self.mean_cost_baseline_usd:.4f} usd  experiment={self.mean_cost_experiment_usd:.4f} usd",
            f"  latency  baseline={self.mean_latency_baseline_s:.3f}s  experiment={self.mean_latency_experiment_s:.3f}s",
            f"  quality  baseline={self.mean_judge_score_baseline}  experiment={self.mean_judge_score_experiment}",
            f"  improvement: {self.improvement}",
        ]
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Module-level CUT constant
# ---------------------------------------------------------------------------

#: The fixed checkpoint name for the intermediate decide step.
#: R1 spec: CUT = "decide".
CUT: str = "decide"


# ---------------------------------------------------------------------------
# R4 metric extraction helpers
# ---------------------------------------------------------------------------

def _extract_cost(client: KitaruClient, exec_id: str) -> float:
    """Return ``display_cost_usd`` from the execution's LLM usage summary.

    Falls back to 0.0 when:
    - the usage summary is absent (e.g. TestModel produces no token usage);
    - ``display_cost_usd`` is None or not numeric.

    This function is robust: it never raises on missing/zero cost.
    """
    try:
        run = client.executions.get(exec_id)
        summary = run.llm_usage_summary
        if summary is None:
            return 0.0
        cost = summary.get("display_cost_usd")
        if cost is None:
            return 0.0
        return float(cost)
    except Exception:  # noqa: BLE001
        return 0.0


def _extract_latency_s(client: KitaruClient, exec_id: str) -> float | None:
    """Return wall-clock latency in seconds for *exec_id*.

    Derived from ``execution.ended_at - execution.started_at``.  Returns None
    when either timestamp is absent (should not happen for completed runs, but
    is handled gracefully).
    """
    try:
        run = client.executions.get(exec_id)
        started = run.started_at
        ended = run.ended_at
        if started is None or ended is None:
            return None
        delta = ended - started
        return max(0.0, delta.total_seconds())
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# ContextVar for ambient agents (Finding 1 fix)
# ---------------------------------------------------------------------------

# Holds the three pydantic_ai agents for the currently-dispatching adapter.
# Each KitaruAdapterPA._activate() sets this before dispatching the flow.
# The module-level @checkpoint steps read from it at execution time.
# This is NOT a checkpoint input — it stays ambient/config, never serialized.
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
    # Ensure all SupportDecision fields are propagated so decision_of
    # can read them from the finalize artifact as a fallback.
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
# KitaruAdapterPA
# ---------------------------------------------------------------------------

class KitaruAdapterPA:
    """Durable execution adapter for the three-step PydanticAI support-copilot.

    Args:
        model: A PydanticAI-compatible model (real or ``TestModel`` for tests).
            The *same* model is used for all three steps.  Pass different models
            per step by extending this adapter.
        prompt_profile: System-prompt profile (``"baseline"`` or
            ``"trimmed_permissions"``).  Drives the ``decide`` step's behaviour.
        name: Stable name prefix for the step agents.  Stored on ``self.name``.
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
        # These are NOT decorated; the module-level @checkpoint steps are the
        # single registered step objects (Finding 1 fix).
        self._gather_agent = build_gather_agent(
            model,
            prompt_profile=prompt_profile,
            name=f"{name}_gather",
        )
        self._decide_agent = build_decide_agent(
            model,
            prompt_profile=prompt_profile,
            name=f"{name}_decide",
        )
        self._finalize_agent = build_finalize_agent(
            model,
            prompt_profile=prompt_profile,
            name=f"{name}_finalize",
        )

        # The @flow is the module-level support_copilot_flow.
        self._flow = support_copilot_flow

        # Cache for decisions read at run() time.
        self._results: dict[str, dict] = {}

    def _activate(self) -> None:
        """Set the module-level ContextVar to this adapter's agents.

        Must be called at the top of every method that dispatches the flow
        (``run``, ``reproduce``, and the reconfigured adapter's dispatch in
        ``experiment``).  This ensures that the module-level @checkpoint steps
        read from the correct adapter's agents when the flow executes.

        This is what makes reproduce-after-experiment correct: the adapter
        that calls ``_activate()`` immediately before dispatch wins.
        """
        _active_agents.set((self._gather_agent, self._decide_agent, self._finalize_agent))

    # -----------------------------------------------------------------------
    # Public interface
    # -----------------------------------------------------------------------

    def run(self, prompt: str, customer: str) -> str:
        """Run the three-step flow and return the exec_id.

        Blocks until the execution finishes.  The flow result (from the
        ``finalize`` terminal) is cached by exec_id to enable the fast path
        in ``decision_of``.
        """
        self._activate()
        handle = self._flow.run(prompt, customer)
        result = handle.wait()
        exec_id = handle.exec_id
        # The flow result is the finalize step's dict (contains SupportDecision
        # fields propagated by finalize).  Cache it for fast decision_of lookup.
        if isinstance(result, dict) and "risk_status" in result:
            self._results[exec_id] = result
        return exec_id

    def cut_of(self, exec_id: str) -> str:
        """Return the CUT checkpoint name for *exec_id*.

        In the multi-step flow, CUT is always ``"decide"`` (the intermediate
        checkpoint).  This method validates that the checkpoint exists and raises
        loudly if the flow structure has changed.

        Raises:
            RuntimeError: If no ``"decide"`` checkpoint exists.
        """
        run = self._client.executions.get(exec_id)
        names = [c.name for c in run.checkpoints]
        if CUT not in names:
            raise RuntimeError(
                f"CUT checkpoint {CUT!r} not found in execution {exec_id}. "
                f"Checkpoints present: {names}. "
                "The flow may not be using the multi-step @checkpoint structure."
            )
        return CUT

    def decision_of(self, exec_id: str) -> dict:
        """Return the ``SupportDecision`` dict for *exec_id*.

        Lookup order:
        1. In-memory cache (populated at ``run()`` time).
        2. ``decide`` checkpoint artifact (preferred; produced by the CUT step).
        3. ``finalize`` checkpoint artifact (fallback; contains propagated fields).

        Raises:
            RuntimeError: If the decision cannot be found via any path.
        """
        # Fast path: decision cached at run() time.
        cached = self._results.get(exec_id)
        if cached is not None:
            return cached

        run = self._client.executions.get(exec_id)

        # Helper: extract a SupportDecision dict from an artifact value.
        def _extract(val: Any) -> dict | None:
            if isinstance(val, dict) and "risk_status" in val:
                return val
            # SupportDecision object (from pydantic BaseModel).
            if isinstance(val, SupportDecision):
                return val.model_dump()
            model_dump = getattr(val, "model_dump", None)
            if callable(model_dump):
                dumped = model_dump()
                if isinstance(dumped, dict) and "risk_status" in dumped:
                    return dumped
            return None

        # Scan checkpoints in priority order: decide first, then finalize.
        priority = [CUT, "finalize"]
        cp_by_name = {c.name: c for c in run.checkpoints}
        for cp_name in priority:
            cp = cp_by_name.get(cp_name)
            if cp is None:
                continue
            for art in cp.artifacts:
                if getattr(art, "direction", None) not in (None, "output"):
                    continue
                try:
                    val = art.load()
                except Exception:  # noqa: BLE001
                    continue
                extracted = _extract(val)
                if extracted is not None:
                    return extracted

        raise RuntimeError(
            f"Could not extract a SupportDecision from execution {exec_id!r}. "
            f"Searched checkpoints: {priority}. "
            f"Checkpoints present: {[c.name for c in run.checkpoints]}. "
            "Ensure the flow completed successfully and the 'decide' checkpoint "
            "produced a serializable SupportDecision dict as its artifact."
        )

    def reproduce(self, exec_id: str) -> str:
        """Re-run from the CUT checkpoint without edits; return the replay exec_id.

        The ``gather_context`` head is served from cache; ``decide`` and
        ``finalize`` re-run under the same agent configuration.
        """
        self._activate()
        handle = self._flow.replay(exec_id, from_=self.cut_of(exec_id), cache=False)
        result = handle.wait()
        replay_id = handle.exec_id
        if isinstance(result, dict) and "risk_status" in result:
            self._results[replay_id] = result
        return replay_id

    def diff(self, baseline_exec: str, other_exec: str):
        """Return a DriftReport comparing the two executions' SupportDecision dicts."""
        from kitaru.adapters.langgraph.replay._drift import DriftReport, compare_decisions

        base = self.decision_of(baseline_exec)
        other = self.decision_of(other_exec)
        return DriftReport(reproduction=[], fork=compare_decisions(base, other))

    # -----------------------------------------------------------------------
    # R4: cohort layer
    # -----------------------------------------------------------------------

    def last_executions(self, n: int) -> list[str]:
        """Return the ``n`` most recent exec_ids for this adapter's flow, newest first.

        Filters ``client.executions.list`` to the ``support_copilot_flow``
        pipeline (the ``@flow``-decorated function's name).  Excludes replay
        executions (those with ``original_exec_id`` set) so the list represents
        independent production runs rather than reproduce/experiment runs.

        Args:
            n: Number of recent executions to return (1–100).

        Returns:
            List of exec_id strings, newest first.
        """
        # Derive the flow name from the flow object's function name (Finding 3a).
        # This avoids hardcoding the literal string and will track any rename.
        from kitaru._source_aliases import build_pipeline_registration_name, callable_name as _callable_name
        flow_name = build_pipeline_registration_name(_callable_name(self._flow._func))
        executions = self._client.executions.list(
            flow=flow_name,
            limit=n * 5,  # over-fetch to account for replays we'll filter out
        )
        # Exclude replays (reproduce/experiment runs) from the cohort seed.
        originals = [e for e in executions if e.original_exec_id is None]
        return [e.exec_id for e in originals[:n]]

    def cohort(
        self,
        *,
        model: Any = None,
        prompt_profile: str | None = None,
        n: int = 10,
    ) -> CohortReport:
        """Apply the same reconfiguration to the last ``n`` production runs.

        For each execution returned by ``last_executions(n)``:
        1. ``reproduce`` it (baseline no-edit replay from CUT).
        2. ``experiment`` it (reconfigured replay from CUT).
        3. Compute decision-changed, cost, latency, and LLM-judge quality score.

        Executions that cannot be reproduced (e.g. CUT not resolvable) are
        skipped and recorded in ``CohortReport.skipped_count`` — they are NOT
        silently dropped.

        A module-level ``build_judge`` factory creates a small PydanticAI agent
        that scores the support decision 1–5.  In tests, use a ``TestModel``
        with ``custom_output_args={"score": N}`` as the model for
        ``KitaruAdapterPA`` so the judge is deterministic.

        Args:
            model: Reconfiguration model (passed to ``experiment``).
            prompt_profile: Reconfiguration prompt profile (passed to ``experiment``).
            n: Number of recent baseline executions to include.

        Returns:
            A ``CohortReport`` with per-run rows and aggregate metrics.
        """
        # Build the judge BEFORE creating any reconfigured adapter.
        # Use self._model (baseline) to keep the judge deterministic under TestModel.
        judge = build_judge(self._model)

        exec_ids = self.last_executions(n)
        report = CohortReport()

        # -----------------------------------------------------------------------
        # With the ContextVar fix, the reproduce/experiment ordering hack is no
        # longer strictly required for correctness.  However, we keep Phase-1 /
        # Phase-2 separation as a belt-and-suspenders guard and for readability:
        # Phase 1 validates CUT and runs all baseline reproduces; Phase 2 runs
        # experiments.  Each dispatch calls _activate() on the correct adapter.
        # -----------------------------------------------------------------------

        rows: list[CohortRow] = []
        for base_id in exec_ids:
            row = CohortRow(base_exec_id=base_id)

            # Guard: validate CUT is resolvable.
            try:
                self.cut_of(base_id)
            except Exception as exc:
                row.skipped = True
                row.skip_reason = str(exc)
                rows.append(row)
                report.skipped_count += 1
                _log.warning("Skipping exec %s (CUT not resolvable): %s", base_id, exc)
                continue

            # Reproduce (baseline replay from CUT using self._flow / base model).
            try:
                repro_id = self.reproduce(base_id)
            except Exception as exc:
                row.skipped = True
                row.skip_reason = f"reproduce failed: {exc}"
                rows.append(row)
                report.skipped_count += 1
                _log.warning("Skipping exec %s (reproduce failed): %s", base_id, exc)
                continue

            row.repro_exec_id = repro_id
            rows.append(row)

        # Phase 2: run experiments for all non-skipped rows.
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
                _log.warning("Skipping exec %s (experiment failed): %s", base_id, exc)
                continue

            row.exp_exec_id = exp_id

            # Decision-changed: compare repro vs experiment decisions.
            repro_decision = None
            exp_decision = None
            try:
                repro_decision = self._decision_from_artifacts(repro_id)
                exp_decision = self._decision_from_artifacts(exp_id)
                from kitaru.adapters.langgraph.replay._drift import DriftReport, compare_decisions
                drift = DriftReport(reproduction=[], fork=compare_decisions(repro_decision, exp_decision))
                row.decision_changed = drift.has_fork_drift
            except Exception as exc:
                _log.warning("diff failed for %s vs %s: %s", repro_id, exp_id, exc)
                row.decision_changed = None

            # Cost and latency from execution metadata.
            row.cost_baseline_usd = _extract_cost(self._client, repro_id)
            row.cost_experiment_usd = _extract_cost(self._client, exp_id)
            row.latency_baseline_s = _extract_latency_s(self._client, repro_id)
            row.latency_experiment_s = _extract_latency_s(self._client, exp_id)

            # LLM-judge quality scores.
            # (Finding 3b: dead re-fetch block removed — repro_decision/exp_decision
            # are already set above from the try block or left None on failure.)
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
                _log.warning("Judge scoring failed for repro %s: %s", repro_id, exc)
                row.judge_score_baseline = None

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
                _log.warning("Judge scoring failed for exp %s: %s", exp_id, exc)
                row.judge_score_experiment = None

            report.rows.append(row)

        return report

    def _decision_from_artifacts(self, exec_id: str) -> dict:
        """Read the SupportDecision dict directly from the execution artifact store.

        Unlike ``decision_of()``, this method always reads from artifacts (never
        from the in-memory ``self._results`` cache).  This is important in
        ``cohort()`` where the cache may contain stale values written before the
        source-alias was overwritten by ``experiment()``'s reconfigured adapter.

        Raises:
            RuntimeError: If the decision cannot be found via artifact lookup.
        """
        run = self._client.executions.get(exec_id)

        def _extract(val: Any) -> dict | None:
            if isinstance(val, dict) and "risk_status" in val:
                return val
            if isinstance(val, SupportDecision):
                return val.model_dump()
            model_dump = getattr(val, "model_dump", None)
            if callable(model_dump):
                dumped = model_dump()
                if isinstance(dumped, dict) and "risk_status" in dumped:
                    return dumped
            return None

        priority = [CUT, "finalize"]
        cp_by_name = {c.name: c for c in run.checkpoints}
        for cp_name in priority:
            cp = cp_by_name.get(cp_name)
            if cp is None:
                continue
            for art in cp.artifacts:
                if getattr(art, "direction", None) not in (None, "output"):
                    continue
                try:
                    val = art.load()
                except Exception:  # noqa: BLE001
                    continue
                extracted = _extract(val)
                if extracted is not None:
                    return extracted

        raise RuntimeError(
            f"Could not extract a SupportDecision from execution {exec_id!r} "
            f"via artifact lookup. Checkpoints: {[c.name for c in run.checkpoints]}."
        )

    def experiment(
        self,
        exec_id: str,
        *,
        model: Any = None,
        prompt_profile: str | None = None,
    ) -> str:
        """Re-run decide+finalize under a reconfigured agent (kind #2 replay).

        Builds a new ``KitaruAdapterPA`` with the supplied ``model`` and/or
        ``prompt_profile`` (falling back to ``self``'s values when not supplied),
        then replays ``exec_id`` from the CUT (``"decide"``).  The
        ``gather_context`` head is served from cache; ``decide`` and ``finalize``
        re-run under the new configuration.

        This is kind #2 (re-run the step under a new config) — NOT an
        ``overrides=`` output substitution.  The reconfigured adapter uses the
        same ``@checkpoint`` step names (``gather_context``, ``decide``,
        ``finalize``) so the replay resolves correctly.

        Args:
            exec_id: The baseline execution to replay.
            model: A PydanticAI-compatible model for the reconfigured adapter.
                Defaults to ``self``'s model when not supplied (not useful on its
                own, but allows ``prompt_profile``-only reconfiguration).
            prompt_profile: System-prompt profile for the reconfigured adapter
                (``"baseline"`` or ``"trimmed_permissions"``).  Defaults to
                ``self``'s prompt profile when not supplied.

        Returns:
            The experiment execution's exec_id.
        """
        resolved_model = model if model is not None else self._model
        resolved_profile = prompt_profile if prompt_profile is not None else self._prompt_profile
        reconfigured = KitaruAdapterPA(
            model=resolved_model,
            prompt_profile=resolved_profile,
            name=self.name,
        )
        # Activate the reconfigured adapter's agents before dispatch so the
        # module-level @checkpoint steps run under the new config.
        reconfigured._activate()
        handle = reconfigured._flow.replay(
            exec_id,
            from_=self.cut_of(exec_id),
            cache=False,
        )
        result = handle.wait()
        replay_id = handle.exec_id
        if isinstance(result, dict) and "risk_status" in result:
            self._results[replay_id] = result
        return replay_id
