"""Analysis helpers for the PydanticAI support-copilot demo.

This module holds the demo's *analysis* code — the stuff that is genuinely the
example's own logic, not Kitaru's. None of it pretends to be the SDK:

ReplayRun     A plain record of one replayed execution (exec_id + decision + model).
MetricDelta   Named metric comparison: baseline vs variant value.
QualityScore  Typed output for the LLM quality judge (score 1-5).
build_judge   Build a raw pydantic_ai.Agent that scores an answer 1-5.
quality_judge BYO metric: score baseline and variant with the judge.
cost          BYO metric (lower_is_better=True): display_cost_usd delta.
latency       BYO metric (lower_is_better=True): wall-clock latency delta.
load_support_decision_from_execution
              Read the SupportDecision dict from checkpoint artifacts.
DriftReport   Decision-field drift report (.has_drift).
diff_decisions Compare two SupportDecision dicts and return a DriftReport.
"""

from __future__ import annotations

import dataclasses
import json
import logging
from typing import Any

from pydantic import BaseModel
from pydantic_ai import Agent
from support_agent import CUT, FINALIZE_CHECKPOINT

from kitaru import KitaruClient

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Drift comparison — self-contained, no framework dependency
# ---------------------------------------------------------------------------

#: The decision fields that define "did the decision move". ``policy_label`` and
#: ``summary`` are free-text the model rewords every call, so they are excluded —
#: a faithful no-edit rerun should not register drift on wording alone.
DECISION_FIELDS = ("risk_status", "required_action")


@dataclasses.dataclass
class FieldChange:
    """One decision field compared across two runs."""

    field: str
    baseline_value: Any
    comparison_value: Any

    @property
    def matches(self) -> bool:
        return self.baseline_value == self.comparison_value


@dataclasses.dataclass
class DriftReport:
    """The decision-field differences between a baseline run and another run."""

    changes: list[FieldChange]

    @property
    def has_drift(self) -> bool:
        """True when any decision field differs."""
        return any(not c.matches for c in self.changes)

    @property
    def has_fork_drift(self) -> bool:
        """Compatibility alias for older demo code."""
        return self.has_drift

    def __str__(self) -> str:
        diffs = [
            f"{c.field}: {c.baseline_value!r} -> {c.comparison_value!r}"
            for c in self.changes
            if not c.matches
        ]
        return (
            "decision changed — " + "; ".join(diffs) if diffs else "no decision drift"
        )


def diff_decisions(baseline: dict, other: dict) -> DriftReport:
    """Compare two SupportDecision dicts on the decision fields.

    Scoped to ``DECISION_FIELDS`` so drift means the decision changed, not that
    the model reworded a label or summary.
    """
    return DriftReport(
        [FieldChange(f, baseline.get(f), other.get(f)) for f in DECISION_FIELDS]
    )


# ---------------------------------------------------------------------------
# ReplayRun — a passive record of one replayed execution
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class ReplayRun:
    """The three things the demo's metrics and diff need about a finished replay.

    This is a plain data record, not a Kitaru type and not a wrapper: the demo
    code does the actual ``support_copilot_flow.replay(...)`` itself, then stashes
    the result here so ``cost``/``latency``/``quality_judge`` have something to
    compare. ``model`` is recorded so the quality judge can score with it.
    """

    exec_id: str
    decision: dict
    model: str


# ---------------------------------------------------------------------------
# MetricDelta — one metric comparison result
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class MetricDelta:
    """Result of applying one BYO metric to a baseline/variant pair.

    Args:
        name: Human-readable metric name (e.g. "cost", "latency", "quality").
        baseline_value: The metric value for the baseline (rerun) handle.
        variant_value: The metric value for the variant (replay) handle.
        lower_is_better: True for cost/latency; False for quality score.
    """

    name: str
    baseline_value: float | None
    variant_value: float | None
    lower_is_better: bool

    @property
    def is_worse(self) -> bool:
        """True when the variant regressed relative to the baseline.

        - lower_is_better metrics: worse if variant > baseline.
        - higher_is_better metrics: worse if variant < baseline.

        Returns False when either value is None (no data = not worse).
        """
        if self.baseline_value is None or self.variant_value is None:
            return False
        if self.lower_is_better:
            return self.variant_value > self.baseline_value
        else:
            return self.variant_value < self.baseline_value


# ---------------------------------------------------------------------------
# LLM-judge
# ---------------------------------------------------------------------------


class QualityScore(BaseModel):
    """Typed output for the LLM quality judge (score 1-5)."""

    score: int = 3


def build_judge(model: Any) -> Agent:
    """Build a raw pydantic_ai.Agent that scores an answer 1-5.

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
# Extraction helpers
# ---------------------------------------------------------------------------


def _extract_cost(client: KitaruClient, exec_id: str) -> float | None:
    """Return ``display_cost_usd`` from the execution's LLM usage summary.

    The PydanticAI adapter prices each model call (via genai-prices) into
    ``estimated_cost_usd``, which Kitaru rolls into ``display_cost_usd``. Returns
    None when no usage was recorded (e.g. TestModel) — None means "no data".
    """
    try:
        summary = client.executions.get(exec_id).llm_usage_summary
    except Exception:
        return None
    if not summary:
        return None
    return summary.get("display_cost_usd")


def _extract_latency_s(client: KitaruClient, exec_id: str) -> float | None:
    """Return wall-clock latency in seconds for *exec_id*.

    Returns None when either timestamp is absent.
    """
    try:
        run = client.executions.get(exec_id)
        started = run.started_at
        ended = run.ended_at
        if started is None or ended is None:
            return None
        delta = ended - started
        return max(0.0, delta.total_seconds())
    except Exception:
        return None


def load_support_decision_from_execution(client: KitaruClient, exec_id: str) -> dict:
    """Read the SupportDecision dict from the execution artifact store.

    Searches checkpoints in priority order: ``support_decide_model_request``
    (the CUT) first, then ``support_finalize_model_request``. Each is a
    KitaruAgent "calls" checkpoint whose artifact is a PydanticAI ModelResponse;
    the decision is its ``final_result`` tool-call args. Always reads from
    artifacts — never from an in-memory cache.

    Raises:
        RuntimeError: If the decision cannot be found via artifact lookup.
    """
    from support_agent import SupportDecision

    run = client.executions.get(exec_id)

    def _extract(val: Any) -> dict | None:
        if isinstance(val, dict) and "risk_status" in val:
            return val
        if isinstance(val, SupportDecision):
            return val.model_dump()
        # The KitaruAgent turn checkpoint persists the PydanticAI AgentRunResult;
        # the SupportDecision / FinalAnswer lives on its `.output`.
        output = getattr(val, "output", None)
        if output is not None and output is not val:
            extracted = _extract(output)
            if extracted is not None:
                return extracted
        # In the adapter's default "calls" strategy, each model-call checkpoint
        # persists a PydanticAI ModelResponse. The structured decision is the
        # `final_result` tool call's args (a JSON string of the output model).
        for part in getattr(val, "parts", None) or ():
            if getattr(part, "tool_name", None) != "final_result":
                continue
            args = getattr(part, "args", None)
            parsed = (
                json.loads(args)
                if isinstance(args, str)
                else args
                if isinstance(args, dict)
                else None
            )
            if isinstance(parsed, dict) and "risk_status" in parsed:
                return parsed
        model_dump = getattr(val, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump()
            if isinstance(dumped, dict) and "risk_status" in dumped:
                return dumped
        return None

    priority = [CUT, FINALIZE_CHECKPOINT]
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
            except Exception:
                continue
            extracted = _extract(val)
            if extracted is not None:
                return extracted

    raise RuntimeError(
        f"Could not extract a SupportDecision from execution {exec_id!r}. "
        f"Searched checkpoints: {priority}. "
        f"Checkpoints present: {[c.name for c in run.checkpoints]}. "
        "Ensure the flow completed successfully and the "
        "'support_decide_model_request' checkpoint produced a SupportDecision."
    )


# ---------------------------------------------------------------------------
# Built-in BYO metrics
# ---------------------------------------------------------------------------


def cost(baseline: ReplayRun, variant: ReplayRun) -> MetricDelta:
    """BYO metric: estimated USD cost (lower_is_better=True)."""
    client = KitaruClient()
    b = _extract_cost(client, baseline.exec_id)
    v = _extract_cost(client, variant.exec_id)
    return MetricDelta(
        name="cost", baseline_value=b, variant_value=v, lower_is_better=True
    )


def latency(baseline: ReplayRun, variant: ReplayRun) -> MetricDelta:
    """BYO metric: wall-clock latency in seconds (lower_is_better=True)."""
    client = KitaruClient()
    b = _extract_latency_s(client, baseline.exec_id)
    v = _extract_latency_s(client, variant.exec_id)
    return MetricDelta(
        name="latency", baseline_value=b, variant_value=v, lower_is_better=True
    )


def quality_judge(baseline: ReplayRun, variant: ReplayRun) -> MetricDelta:
    """BYO metric: LLM judge quality score (lower_is_better=False).

    Reads ``baseline.model`` — the model recorded on the ReplayRun. Returns an
    empty (None) delta when no model is available.
    """
    model = baseline.model
    if model is None:
        return MetricDelta(
            name="quality",
            baseline_value=None,
            variant_value=None,
            lower_is_better=False,
        )

    judge = build_judge(model)

    def _score(decision: dict) -> int | None:
        try:
            summary = (
                f"policy={decision.get('policy_label')} "
                f"risk={decision.get('risk_status')} "
                f"action={decision.get('required_action')} "
                f"summary={decision.get('summary', '')!r}"
            )
            return judge.run_sync(summary).output.score
        except Exception:
            return None

    b_score = _score(baseline.decision)
    v_score = _score(variant.decision)
    return MetricDelta(
        name="quality",
        baseline_value=b_score,
        variant_value=v_score,
        lower_is_better=False,
    )
